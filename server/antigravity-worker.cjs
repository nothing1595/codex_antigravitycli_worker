#!/usr/bin/env node
"use strict";

// Thin MCP wrapper: speaks stdio JSON-RPC 2.0 to Codex and relays every tool
// call to the singleton antigravity-broker over loopback TCP (127.0.0.1:19225).
// Multiple Codex subagents — each with its own wrapper process — funnel into the
// broker, which manages persistent agy sessions and turn concurrency safely.
// If no broker is reachable, one is spawned detached and connection retries converge.

const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");

const SERVER = { name: "codex-antigravity-worker", version: "0.3.0" };
const BROKER_PATH = path.join(__dirname, "antigravity-broker.cjs");
const BROKER_PORT = Number(process.env.AGY_BROKER_PORT || 19225);
const BROKER_START_ATTEMPTS = 40;
const BROKER_START_RETRY_MS = 250;
const BROKER_REQUEST_TIMEOUT_MS = 60_000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function send(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

function textResult(value, isError = false) {
  return {
    content: [
      {
        type: "text",
        text: typeof value === "string" ? value : JSON.stringify(value, null, 2),
      },
    ],
    isError,
  };
}

function brokerRequest(method, params) {
  return new Promise((resolve, reject) => {
    const socket = net.connect(BROKER_PORT, "127.0.0.1");
    let buffer = "";
    let settled = false;

    const fail = (error) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(error);
    };

    socket.setTimeout(BROKER_REQUEST_TIMEOUT_MS);
    socket.on("timeout", () => fail(Object.assign(new Error("broker request timed out"), { code: "ETIMEDOUT" })));
    socket.on("error", fail);

    socket.on("connect", () => {
      socket.write(`${JSON.stringify({ id: 1, method, params })}\n`);
    });

    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      const newline = buffer.indexOf("\n");
      if (newline < 0) return;
      if (settled) return;
      settled = true;
      socket.end();
      try {
        const message = JSON.parse(buffer.slice(0, newline));
        if (message.error) {
          reject(new Error(message.error.message || "broker error"));
        } else {
          resolve(message.result);
        }
      } catch (error) {
        reject(error);
      }
    });
  });
}

function spawnBroker() {
  const fs = require("node:fs");
  const os = require("node:os");
  const hostProfile = process.env.AGY_USER_PROFILE || (fs.existsSync("C:\\Users\\15869") ? "C:\\Users\\15869" : os.homedir());
  const child = spawn(process.execPath, [BROKER_PATH], {
    detached: true,
    stdio: "ignore",
    windowsHide: true,
    env: {
      ...process.env,
      USERPROFILE: hostProfile,
      HOME: hostProfile,
      APPDATA: path.join(hostProfile, "AppData", "Roaming"),
      LOCALAPPDATA: path.join(hostProfile, "AppData", "Local"),
    },
  });
  child.unref();
}

async function callBroker(method, params) {
  let lastError;
  for (let attempt = 0; attempt <= BROKER_START_ATTEMPTS; attempt += 1) {
    try {
      return await brokerRequest(method, params);
    } catch (error) {
      lastError = error;
      const connectFailure = ["ECONNREFUSED", "ECONNRESET", "EPIPE", "ETIMEDOUT"].includes(error.code);
      if (!connectFailure || attempt === BROKER_START_ATTEMPTS) break;
      if (attempt === 0) spawnBroker();
      await sleep(BROKER_START_RETRY_MS);
    }
  }
  throw new Error(`antigravity-broker unreachable on 127.0.0.1:${BROKER_PORT}: ${lastError?.message || "unknown error"}`);
}

const tools = [
  {
    name: "list_models",
    description: "List all available Antigravity models dynamically, grouped by model family, mapped to 'agy_XXX_worker' naming convention, and configured with their maximum available reasoning effort.",
    inputSchema: {
      type: "object",
      properties: {},
      additionalProperties: false,
    },
  },
  {
    name: "run_task",
    description: "Start a new Antigravity CLI session in the specified workspace. Creates a persistent agy process and returns immediately with job_id and session_id. Extra jobs queue when parallel limit is reached; poll job_id with get_status. Accepts model aliases such as 'agy_gemini3.8flash_worker', 'gemini3.8flash', or raw model slugs, automatically applying maximum reasoning effort.",
    inputSchema: {
      type: "object",
      properties: {
        workspace: {
          type: "string",
          description: "Absolute path to the shared workspace directory.",
        },
        task: {
          type: "string",
          description: "Complete instruction or delegated task for Antigravity.",
        },
        model: {
          type: "string",
          description: "Model slug to use (e.g. gemini-3.8-flash-high, gemini-3.1-pro-high). Default: gemini-3.8-flash-high.",
          default: "gemini-3.8-flash-high",
        },
        effort: {
          type: "string",
          enum: ["low", "medium", "high"],
          description: "Optional reasoning effort override.",
        },
        agent: {
          type: "string",
          description: "Optional agent name for the Antigravity session.",
        },
        permission_mode: {
          type: "string",
          enum: ["yolo", "safe"],
          default: "yolo",
          description: "Permission mode. 'yolo' passes --dangerously-skip-permissions to auto-approve tool execution. 'safe' prompts/enforces checks.",
        },
        timeout_minutes: {
          type: "number",
          description: "Optional task timeout in minutes (default: 240 min / 4 hours, bounded by AGY_TASK_TIMEOUT_MS). Pass lower value to restrict specific short-lived tasks.",
          default: 240,
        },
      },
      required: ["workspace", "task"],
      additionalProperties: false,
    },
  },
  {
    name: "continue_task",
    description: "Continue an existing Antigravity session with a follow-up instruction. Feeds the prompt into the running agy process stdin without restarting the CLI.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "The session_id returned from run_task.",
        },
        task: {
          type: "string",
          description: "Follow-up instruction for the ongoing session.",
        },
        timeout_minutes: {
          type: "number",
          description: "Optional turn timeout override in minutes for this continuation (default: session timeout, up to 4 hours).",
        },
      },
      required: ["session_id", "task"],
      additionalProperties: false,
    },
  },
  {
    name: "get_status",
    description: "Get status, progress, and output for an Antigravity job. Statuses: queued, running, completed, failed, cancelled. Supports server-side long polling via wait_ms (recommended: 40000) to avoid busy-polling.",
    inputSchema: {
      type: "object",
      properties: {
        job_id: {
          type: "string",
          description: "The job_id to check.",
        },
        wait_ms: {
          type: "number",
          description: "Hold response until status or progress changes, up to 45000 ms. Recommended: 40000.",
          default: 40000,
        },
      },
      required: ["job_id"],
      additionalProperties: false,
    },
  },
  {
    name: "cancel_task",
    description: "Cancel an active or queued Antigravity task. Gracefully terminates the running turn or halts the subprocess tree.",
    inputSchema: {
      type: "object",
      properties: {
        job_id: {
          type: "string",
          description: "The job_id to cancel.",
        },
      },
      required: ["job_id"],
      additionalProperties: false,
    },
  },
];

async function callTool(name, args) {
  if (!["list_models", "run_task", "continue_task", "get_status", "cancel_task"].includes(name)) {
    return textResult(`unknown tool: ${name}`, true);
  }
  const result = await callBroker(name, args || {});
  return textResult(result);
}

async function handle(request) {
  if (!request || request.jsonrpc !== "2.0") return;
  if (request.method === "notifications/initialized" || request.method === "notifications/cancelled") return;

  if (request.method === "initialize") {
    const protocolVersion = request.params?.protocolVersion || "2024-11-05";
    return send({
      jsonrpc: "2.0",
      id: request.id,
      result: {
        protocolVersion,
        capabilities: { tools: {} },
        serverInfo: SERVER,
      },
    });
  }

  if (request.method === "ping") {
    return send({ jsonrpc: "2.0", id: request.id, result: {} });
  }

  if (request.method === "tools/list") {
    return send({ jsonrpc: "2.0", id: request.id, result: { tools } });
  }

  if (request.method === "tools/call") {
    try {
      const result = await callTool(request.params?.name, request.params?.arguments || {});
      return send({ jsonrpc: "2.0", id: request.id, result });
    } catch (error) {
      return send({ jsonrpc: "2.0", id: request.id, result: textResult(error.stack || error.message, true) });
    }
  }

  if (request.id !== undefined) {
    send({
      jsonrpc: "2.0",
      id: request.id,
      error: { code: -32601, message: `Method not found: ${request.method}` },
    });
  }
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  for (;;) {
    const newline = buffer.indexOf("\n");
    if (newline < 0) break;
    const line = buffer.slice(0, newline).trim();
    buffer = buffer.slice(newline + 1);
    if (!line) continue;
    try {
      void handle(JSON.parse(line));
    } catch (error) {
      process.stderr.write(`Invalid MCP message: ${error.message}\n`);
    }
  }
});
