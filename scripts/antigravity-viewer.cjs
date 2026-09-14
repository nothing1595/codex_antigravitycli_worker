#!/usr/bin/env node
"use strict";

// Real-time CLI Monitor Window for Antigravity-Codex Bridge
// Displays live streaming reasoning, tool calls, status, and assistant responses.

const fs = require("node:fs");
const readline = require("node:readline");

const sessionId = process.argv[2] || "unknown_session";
const logPath = process.argv[3];

if (!logPath) {
  console.error("Usage: node antigravity-viewer.cjs <sessionId> <eventLogPath>");
  process.exit(1);
}

// ANSI Escape Codes
const c = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  italic: "\x1b[3m",
  underline: "\x1b[4m",
  cyan: "\x1b[36m",
  brightCyan: "\x1b[96m",
  green: "\x1b[32m",
  brightGreen: "\x1b[92m",
  yellow: "\x1b[33m",
  brightYellow: "\x1b[93m",
  red: "\x1b[31m",
  brightRed: "\x1b[91m",
  magenta: "\x1b[35m",
  brightMagenta: "\x1b[95m",
  blue: "\x1b[34m",
  white: "\x1b[37m",
  gray: "\x1b[90m",
};

// Set console window title
process.stdout.write(`\x1b]0;⚡ Antigravity CLI Monitor - ${sessionId}\x07`);

console.clear();
console.log(`${c.bold}${c.brightCyan}╔══════════════════════════════════════════════════════════════════════════════╗${c.reset}`);
console.log(`${c.bold}${c.brightCyan}║               ⚡ ANTIGRAVITY CLI REAL-TIME MONITOR (CODEX)                  ║${c.reset}`);
console.log(`${c.bold}${c.brightCyan}╚══════════════════════════════════════════════════════════════════════════════╝${c.reset}\n`);

let currentMode = null; // 'thought', 'text', 'tool'
let filePosition = 0;
let isFinished = false;

function formatTimestamp(ts) {
  const d = ts ? new Date(ts) : new Date();
  return d.toTimeString().split(" ")[0];
}

function renderEvent(event) {
  switch (event.type) {
    case "meta": {
      console.log(`${c.bold}${c.brightYellow}• Session ID :${c.reset} ${event.session_id}`);
      console.log(`${c.bold}${c.brightYellow}• Model      :${c.reset} ${event.model} (${c.cyan}Effort: ${event.effort || "default"}${c.reset})`);
      console.log(`${c.bold}${c.brightYellow}• Workspace  :${c.reset} ${event.workspace}`);
      console.log(`${c.bold}${c.brightYellow}• Started At :${c.reset} ${event.started_at || new Date().toISOString()}`);
      console.log(`${c.gray}──────────────────────────────────────────────────────────────────────────────${c.reset}`);
      break;
    }

    case "turn_start": {
      console.log(`\n${c.bold}${c.brightMagenta}▶ [${formatTimestamp()}] STARTING TURN: ${event.job_id}${c.reset}`);
      console.log(`${c.bold}${c.white}TASK PROMPT:${c.reset}`);
      const promptLines = (event.prompt || "").trim().split("\n");
      for (const line of promptLines) {
        console.log(`  ${c.dim}${line}${c.reset}`);
      }
      console.log(`${c.gray}──────────────────────────────────────────────────────────────────────────────${c.reset}`);
      currentMode = null;
      break;
    }

    case "thought_delta": {
      if (currentMode !== "thought") {
        process.stdout.write(`\n${c.bold}${c.brightCyan}💭 [THINKING]${c.reset} ${c.dim}${c.cyan}`);
        currentMode = "thought";
      }
      process.stdout.write(event.text);
      break;
    }

    case "text_delta": {
      if (currentMode !== "text") {
        process.stdout.write(`\n${c.bold}${c.brightGreen}💬 [OUTPUT]${c.reset}\n`);
        currentMode = "text";
      }
      process.stdout.write(`${c.white}${event.text}${c.reset}`);
      break;
    }

    case "tool_call": {
      currentMode = "tool";
      console.log(`\n${c.bold}${c.brightYellow}⚙️  [TOOL CALL]${c.reset} ${c.bold}${event.name}${c.reset}`);
      if (event.input) {
        const inputStr = typeof event.input === "string" ? event.input : JSON.stringify(event.input, null, 2);
        const lines = inputStr.split("\n");
        for (const l of lines.slice(0, 8)) {
          console.log(`   ${c.yellow}${l}${c.reset}`);
        }
        if (lines.length > 8) {
          console.log(`   ${c.dim}... (${lines.length - 8} more lines)${c.reset}`);
        }
      }
      break;
    }

    case "tool_result": {
      currentMode = "tool";
      console.log(`${c.bold}${c.yellow}📥 [TOOL RESULT]${c.reset} ${c.dim}${event.name}${c.reset}`);
      if (event.output) {
        const outputLines = String(event.output).trim().split("\n");
        for (const l of outputLines.slice(0, 4)) {
          console.log(`   ${c.gray}${l}${c.reset}`);
        }
        if (outputLines.length > 4) {
          console.log(`   ${c.dim}... (${outputLines.length - 4} more lines)${c.reset}`);
        }
      }
      break;
    }

    case "stderr": {
      console.log(`\n${c.red}${c.bold}[DIAGNOSTICS]${c.reset} ${c.red}${event.text}${c.reset}`);
      break;
    }

    case "step_progress": {
      const p = event.progress || {};
      const stepStr = p.step_index != null ? `Step: ${p.step_index}` : "";
      const stateStr = p.state ? `State: ${p.state}` : "";
      const tokensStr = p.total_tokens ? `Tokens: ${p.total_tokens}` : "";
      const parts = [stepStr, stateStr, tokensStr].filter(Boolean).join(" | ");
      if (parts) {
        process.stdout.write(`\r${c.gray}[${formatTimestamp()}] ⏳ ${parts}${c.reset}   `);
      }
      break;
    }

    case "turn_complete": {
      currentMode = null;
      console.log(`\n\n${c.gray}──────────────────────────────────────────────────────────────────────────────${c.reset}`);
      const isSuccess = event.status === "completed";
      const bannerColor = isSuccess ? c.brightGreen : c.brightRed;
      const statusIcon = isSuccess ? "✅" : "❌";
      console.log(`${bannerColor}${c.bold}==============================================================================${c.reset}`);
      console.log(`${bannerColor}${c.bold}  ${statusIcon} TURN FINISHED: ${event.status.toUpperCase()} (Exit Code: ${event.exit_code ?? 0})${c.reset}`);
      if (event.duration_s) {
        console.log(`${c.dim}  • Duration : ${event.duration_s}s${c.reset}`);
      }
      if (event.usage) {
        console.log(`${c.dim}  • Usage    : ${JSON.stringify(event.usage)}${c.reset}`);
      }
      console.log(`${bannerColor}${c.bold}==============================================================================${c.reset}\n`);

      if (event.session_ended) {
        isFinished = true;
        promptExit();
      }
      break;
    }

    default:
      break;
  }
}

function promptExit() {
  console.log(`${c.cyan}Execution session finished. Window will remain open for inspection.${c.reset}`);
  console.log(`${c.dim}Press Enter or close this window to exit.${c.reset}`);
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.on("line", () => process.exit(0));
  // Auto-close after 30 minutes if left untouched
  setTimeout(() => process.exit(0), 30 * 60_000).unref();
}

let buffer = "";
function pollLog() {
  try {
    if (!fs.existsSync(logPath)) return;
    const stat = fs.statSync(logPath);
    if (stat.size <= filePosition) return;

    const fd = fs.openSync(logPath, "r");
    const readBytes = stat.size - filePosition;
    const readBuf = Buffer.alloc(readBytes);
    fs.readSync(fd, readBuf, 0, readBytes, filePosition);
    fs.closeSync(fd);

    filePosition = stat.size;
    buffer += readBuf.toString("utf8");

    for (;;) {
      const idx = buffer.indexOf("\n");
      if (idx < 0) break;
      const line = buffer.slice(0, idx).trim();
      buffer = buffer.slice(idx + 1);
      if (!line) continue;
      try {
        const event = JSON.parse(line);
        renderEvent(event);
      } catch {
        // raw unformatted line
        console.log(line);
      }
    }
  } catch (err) {
    // best-effort read
  }
}

// Poll frequently for low-latency live streaming
const pollTimer = setInterval(pollLog, 80);

process.on("SIGINT", () => process.exit(0));
