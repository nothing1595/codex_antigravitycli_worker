#!/usr/bin/env node
"use strict";

// Singleton broker that manages persistent Antigravity CLI (agy) sessions,
// enforces parallel job concurrency (default 2), handles stream-json parsing,
// tracks live progress, manages dynamic model resolution with maximum reasoning effort,
// and executes graceful cancellations.

const net = require("node:net");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn, execFile } = require("node:child_process");
const { randomUUID } = require("node:crypto");

const SERVER = { name: "antigravity-broker", version: "0.3.0" };
const BROKER_PORT = Number(process.env.AGY_BROKER_PORT || 19225);
const MAX_PARALLEL_JOBS = Number(process.env.AGY_MAX_PARALLEL_JOBS || 2);
const IDLE_EXIT_MS = Number(process.env.AGY_BROKER_IDLE_MS || 10 * 60_000);
const JOB_TTL_MS = 60 * 60_000;
const TASK_IDLE_TIMEOUT_MS = Number(process.env.AGY_TASK_IDLE_TIMEOUT_MS || 10 * 60_000);
const TASK_HARD_TIMEOUT_MS = Number(process.env.AGY_TASK_TIMEOUT_MS || 4 * 60 * 60_000);
const DEFAULT_TIMEOUT_MINUTES = Math.round(TASK_HARD_TIMEOUT_MS / 60_000);
const MAX_OUTPUT_CHARS = 120_000;
const MODELS_CACHE_TTL_MS = 5 * 60_000;

function resolveHostUserProfile() {
  if (process.env.AGY_USER_PROFILE && fs.existsSync(process.env.AGY_USER_PROFILE)) {
    return process.env.AGY_USER_PROFILE;
  }
  const defaultProfile = "C:\\Users\\15869";
  if (fs.existsSync(defaultProfile)) return defaultProfile;
  return os.homedir();
}

const HOST_USER_PROFILE = resolveHostUserProfile();

function resolveAgyExe() {
  if (process.env.AGY_EXE && fs.existsSync(process.env.AGY_EXE)) {
    return process.env.AGY_EXE;
  }
  const localInHost = path.join(HOST_USER_PROFILE, "AppData", "Local", "agy", "bin", "agy.exe");
  if (fs.existsSync(localInHost)) return localInHost;
  const defaultLocal = path.join(os.homedir(), "AppData", "Local", "agy", "bin", "agy.exe");
  if (fs.existsSync(defaultLocal)) return defaultLocal;
  return "agy";
}

const AGY_EXE = resolveAgyExe();

function getAgyEnv() {
  const env = { ...process.env };
  env.USERPROFILE = HOST_USER_PROFILE;
  env.HOME = HOST_USER_PROFILE;
  const root = path.parse(HOST_USER_PROFILE).root || "C:\\";
  env.HOMEDRIVE = root.replace(/[\/\\]$/, "");
  env.HOMEPATH = HOST_USER_PROFILE.slice(env.HOMEDRIVE.length);
  env.APPDATA = path.join(HOST_USER_PROFILE, "AppData", "Roaming");
  env.LOCALAPPDATA = path.join(HOST_USER_PROFILE, "AppData", "Local");
  const agyBin = path.join(HOST_USER_PROFILE, "AppData", "Local", "agy", "bin");
  if (fs.existsSync(agyBin)) {
    const p = env.PATH || env.Path || "";
    if (!p.toLowerCase().includes(agyBin.toLowerCase())) {
      env.PATH = `${agyBin};${p}`;
      env.Path = `${agyBin};${p}`;
    }
  }
  return env;
}

const TERMINAL_STATUSES = new Set(["completed", "failed", "cancelled"]);

// In-memory data store
const sessions = new Map(); // sessionId -> Session
const jobs = new Map();     // jobId -> Job
const clients = new Set();
let activeSlots = 0;
const slotWaiters = [];
let lastActivity = Date.now();

// Session Event Stream Directory for Real-Time CLI Window Monitor
const SESSIONS_LOG_DIR = path.join(os.tmpdir(), "antigravity-sessions");
try { fs.mkdirSync(SESSIONS_LOG_DIR, { recursive: true }); } catch { /* ignore */ }

// Clean up any dangling agy models processes from previous deadlocks
function cleanupOrphanAgyModels() {
  if (process.platform === "win32") {
    execFile("powershell", [
      "-NoProfile",
      "-Command",
      "Get-CimInstance Win32_Process -Filter \"Name = 'agy.exe'\" | Where-Object { $_.CommandLine -match 'models' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
    ], () => {});
  }
}
cleanupOrphanAgyModels();

// Model Cache Defaults (pre-populated so list_models and resolution NEVER block empty)
const DEFAULT_MODEL_FAMILIES = [
  {
    worker_name: "agy_gemini3.8flash_worker",
    model_family: "Gemini 3.8 Flash",
    target_model: "gemini-3.8-flash-high",
    description: "Gemini 3.8 Flash (最高推理: High)",
    effort: "high",
  },
  {
    worker_name: "agy_gemini3.1pro_worker",
    model_family: "Gemini 3.1 Pro",
    target_model: "gemini-3.1-pro-high",
    description: "Gemini 3.1 Pro (最高推理: High)",
    effort: "high",
  },
  {
    worker_name: "agy_claudesonnet4.6_worker",
    model_family: "Claude Sonnet 4.6",
    target_model: "claude-sonnet-4.6-thinking",
    description: "Claude Sonnet 4.6 (最高推理: Thinking)",
    effort: "high",
  },
  {
    worker_name: "agy_claudeopus4.6_worker",
    model_family: "Claude Opus 4.6",
    target_model: "claude-opus-4.6-thinking",
    description: "Claude Opus 4.6 (最高推理: Thinking)",
    effort: "high",
  },
];
let cachedModels = [...DEFAULT_MODEL_FAMILIES];
let cachedModelsTime = 0;

// Fast-path model alias dictionary to completely skip `agy models` on dispatch
const KNOWN_MODEL_ALIASES = {
  "gemini-3.8-flash-high": { model: "gemini-3.8-flash-high", effort: "high" },
  "gemini-3.8-flash-medium": { model: "gemini-3.8-flash-medium", effort: "medium" },
  "gemini-3.8-flash-low": { model: "gemini-3.8-flash-low", effort: "low" },
  "gemini-3.8-flash": { model: "gemini-3.8-flash-high", effort: "high" },
  "gemini3.8flash": { model: "gemini-3.8-flash-high", effort: "high" },
  "agy_gemini3.8flash_worker": { model: "gemini-3.8-flash-high", effort: "high" },

  "gemini-3.1-pro-high": { model: "gemini-3.1-pro-high", effort: "high" },
  "gemini-3.1-pro-low": { model: "gemini-3.1-pro-low", effort: "low" },
  "gemini-3.1-pro": { model: "gemini-3.1-pro-high", effort: "high" },
  "gemini3.1pro": { model: "gemini-3.1-pro-high", effort: "high" },
  "agy_gemini3.1pro_worker": { model: "gemini-3.1-pro-high", effort: "high" },

  "gemini-3-flash-high": { model: "gemini-3-flash-high", effort: "high" },
  "gemini-3-flash": { model: "gemini-3-flash-high", effort: "high" },
  "gemini3flash": { model: "gemini-3-flash-high", effort: "high" },
  "agy_gemini3flash_worker": { model: "gemini-3-flash-high", effort: "high" },

  "claude-sonnet-4.6-thinking": { model: "claude-sonnet-4.6-thinking", effort: "high" },
  "claude-sonnet-4.6": { model: "claude-sonnet-4.6-thinking", effort: "high" },
  "claudesonnet4.6": { model: "claude-sonnet-4.6-thinking", effort: "high" },
  "agy_claudesonnet4.6_worker": { model: "claude-sonnet-4.6-thinking", effort: "high" },

  "claude-opus-4.6-thinking": { model: "claude-opus-4.6-thinking", effort: "high" },
  "claude-opus-4.6": { model: "claude-opus-4.6-thinking", effort: "high" },
  "claudeopus4.6": { model: "claude-opus-4.6-thinking", effort: "high" },
  "agy_claudeopus4.6_worker": { model: "claude-opus-4.6-thinking", effort: "high" },

  "gpt-oss-120b-medium": { model: "gpt-oss-120b-medium", effort: "medium" },
  "gpt-oss-120b": { model: "gpt-oss-120b-medium", effort: "medium" },
  "gptoss120b": { model: "gpt-oss-120b-medium", effort: "medium" },
  "agy_gptoss120b_worker": { model: "gpt-oss-120b-medium", effort: "medium" },
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const BROKER_LOG = process.env.AGY_BROKER_LOG || path.join(os.tmpdir(), "antigravity-broker.log");
function logEvent(message) {
  const line = `${new Date().toISOString()} ${message}\n`;
  process.stderr.write(line);
  try { fs.appendFileSync(BROKER_LOG, line); } catch { /* best-effort log */ }
}

function safeTail(value) {
  if (!value) return "";
  return value.length <= MAX_OUTPUT_CHARS ? value : `[output truncated]\n${value.slice(-MAX_OUTPUT_CHARS)}`;
}

function redactSensitive(value) {
  if (typeof value !== "string") return value;
  return value
    .replace(/\bBearer\s+[A-Za-z0-9._~+\/-]+/gi, "Bearer [REDACTED]")
    .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, "[REDACTED_JWT]")
    .replace(/(['"]?(?:authorization|x-api-key|api[_-]?key|token)['"]?\s*:\s*['"])[^'"\r\n]*(['"])/gi, "$1[REDACTED]$2");
}

function resolveWorkspace(input) {
  if (!input || typeof input !== "string") throw new Error("workspace is required");
  const workspace = path.resolve(input);
  if (!path.isAbsolute(workspace) || !fs.statSync(workspace).isDirectory()) {
    throw new Error(`workspace is not a directory: ${workspace}`);
  }
  return workspace;
}

// ---------------------------------------------------------------------------
// Model Discovery & Family Aggregation (Highest Reasoning Effort Selection)

function getEffortScore(slug, name) {
  const text = `${slug} ${name}`.toLowerCase();
  if (text.includes("high") || text.includes("thinking")) return 3;
  if (text.includes("medium")) return 2;
  if (text.includes("low")) return 1;
  return 0;
}

function getBaseFamilyName(name) {
  return name.replace(/\s*\((High|Medium|Low|Thinking)\)\s*$/i, "").trim();
}

function makeWorkerName(baseName) {
  const clean = baseName.toLowerCase().replace(/[^a-z0-9.]/g, "");
  return `agy_${clean}_worker`;
}

let fetchModelsInFlight = null;
function fetchRawModels() {
  if (fetchModelsInFlight) return fetchModelsInFlight;

  fetchModelsInFlight = new Promise((resolve) => {
    execFile(AGY_EXE, ["models"], {
      env: getAgyEnv(),
      windowsHide: true,
      timeout: 6000,
      stdio: ["ignore", "pipe", "pipe"],
    }, (err, stdout) => {
      fetchModelsInFlight = null;
      if (err || !stdout) {
        logEvent(`failed to query agy models: ${err?.message || "empty output"}`);
        return resolve([]);
      }
      const lines = stdout.split("\n");
      const models = [];
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("Fetching")) continue;
        const parts = trimmed.split("\t");
        if (parts.length >= 2) {
          models.push({ slug: parts[0].trim(), name: parts[1].trim() });
        }
      }
      resolve(models);
    });
  });

  return fetchModelsInFlight;
}

async function getAvailableModelFamilies() {
  const now = Date.now();
  if (cachedModels && now - cachedModelsTime < MODELS_CACHE_TTL_MS && cachedModelsTime > 0) {
    return cachedModels;
  }

  const rawModels = await fetchRawModels();
  const familyMap = new Map();

  for (const m of rawModels) {
    const family = getBaseFamilyName(m.name);
    const score = getEffortScore(m.slug, m.name);
    if (!familyMap.has(family) || familyMap.get(family).score < score) {
      const effortLabel = score === 3 ? (m.name.includes("Thinking") ? "Thinking" : "High") : score === 2 ? "Medium" : "Low";
      familyMap.set(family, {
        worker_name: makeWorkerName(family),
        model_family: family,
        target_model: m.slug,
        description: `${family} (最高推理: ${effortLabel})`,
        effort: score === 3 ? "high" : score === 2 ? "medium" : "low",
        score,
      });
    }
  }

  // Fallback defaults if agy models command fails
  if (familyMap.size === 0) {
    for (const d of DEFAULT_MODEL_FAMILIES) {
      familyMap.set(d.model_family, { ...d, score: 3 });
    }
  }

  cachedModels = Array.from(familyMap.values()).map(({ score, ...rest }) => rest);
  cachedModelsTime = now;
  return cachedModels;
}

async function resolveModelSelection(requestedModel) {
  if (!requestedModel || typeof requestedModel !== "string") {
    return { model: "gemini-3.8-flash-high", effort: "high" };
  }

  const normalized = requestedModel.trim().toLowerCase();

  // FAST-PATH: Known model alias mapping (resolves in 0ms without running `agy models`)
  if (KNOWN_MODEL_ALIASES[normalized]) {
    return { ...KNOWN_MODEL_ALIASES[normalized] };
  }

  const stripped = normalized.replace(/^agy_/, "").replace(/_worker$/, "");
  if (KNOWN_MODEL_ALIASES[stripped]) {
    return { ...KNOWN_MODEL_ALIASES[stripped] };
  }

  // FALLBACK: Query dynamic models
  const families = await getAvailableModelFamilies();

  // 1. Direct match with worker_name, e.g. agy_gemini3.8flash_worker
  const byWorker = families.find((f) => f.worker_name.toLowerCase() === normalized);
  if (byWorker) return { model: byWorker.target_model, effort: byWorker.effort };

  // 2. Match stripped format, e.g. gemini3.8flash
  const byStripped = families.find((f) => f.worker_name.toLowerCase().includes(stripped));
  if (byStripped) return { model: byStripped.target_model, effort: byStripped.effort };

  // 3. Match base family name, e.g. "gemini 3.8 flash"
  const byFamily = families.find((f) => f.model_family.toLowerCase() === normalized);
  if (byFamily) return { model: byFamily.target_model, effort: byFamily.effort };

  // 4. Exact slug match
  return { model: requestedModel, effort: null };
}

// ---------------------------------------------------------------------------
// Concurrency Control (Semaphore)

function acquireSlot(job) {
  return new Promise((resolve) => {
    slotWaiters.push({ job, resolve });
    pumpSlots();
  });
}

function pumpSlots() {
  while (activeSlots < MAX_PARALLEL_JOBS && slotWaiters.length > 0) {
    const { job, resolve } = slotWaiters.shift();
    if (TERMINAL_STATUSES.has(job.status)) {
      resolve(false);
      continue;
    }
    activeSlots += 1;
    job.slotHeld = true;
    resolve(true);
  }
}

function releaseSlot(job) {
  if (!job.slotHeld) return;
  job.slotHeld = false;
  activeSlots -= 1;
  pumpSlots();
}

function dropSlotWaiter(job) {
  for (let i = slotWaiters.length - 1; i >= 0; i -= 1) {
    if (slotWaiters[i].job === job) {
      slotWaiters[i].resolve(false);
      slotWaiters.splice(i, 1);
    }
  }
}

function hasActiveJobs() {
  for (const job of jobs.values()) {
    if (!TERMINAL_STATUSES.has(job.status)) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Process Tree Management & Cancellation

function killProcessTree(pid) {
  return new Promise((resolve) => {
    if (!pid) return resolve();
    if (process.platform === "win32") {
      execFile("taskkill", ["/pid", String(pid), "/T", "/F"], (err) => {
        if (err) logEvent(`taskkill pid=${pid} notice: ${err.message}`);
        resolve();
      });
    } else {
      try { process.kill(-pid, "SIGKILL"); } catch {
        try { process.kill(pid, "SIGKILL"); } catch { /* ignore */ }
      }
      resolve();
    }
  });
}

async function terminateSessionProcess(session, reason) {
  if (!session || !session.process || session.processExited) return;
  const child = session.process;
  const pid = child.pid;
  logEvent(`terminating session ${session.sessionId} process (pid=${pid}) due to: ${reason}`);

  session.processExited = true;

  try { child.stdin?.end(); } catch { /* ignore */ }
  try { child.kill("SIGINT"); } catch { /* ignore */ }

  const start = Date.now();
  while (Date.now() - start < 2500) {
    if (session.processExited && !session.process) break;
    await sleep(200);
  }

  if (pid) {
    await killProcessTree(pid);
  }

  session.process = null;
}

// ---------------------------------------------------------------------------
// Real-Time CLI Window Monitor & Session Event Stream

function writeSessionEvent(session, event) {
  if (!session || !session.logPath) return;
  try {
    fs.appendFileSync(session.logPath, `${JSON.stringify(event)}\n`);
  } catch { /* best-effort write */ }
}

const VIEWER_SCRIPT = path.join(__dirname, "..", "scripts", "antigravity-viewer.cjs");

function launchSessionViewer(session) {
  if (process.platform !== "win32") return;
  if (process.env.AGY_SHOW_WINDOW === "0") return;
  if (session.viewerLaunched) return;
  session.viewerLaunched = true;

  try {
    const title = `Antigravity CLI Monitor - [${session.model || "worker"}]`;
    const cmdArgs = [
      "/c",
      "start",
      title,
      process.execPath,
      VIEWER_SCRIPT,
      session.sessionId,
      session.logPath,
    ];
    const viewerProc = spawn("cmd.exe", cmdArgs, {
      detached: true,
      stdio: "ignore",
      windowsHide: false,
    });
    viewerProc.unref();
    logEvent(`launched visible CLI monitor window for session ${session.sessionId}`);
  } catch (err) {
    logEvent(`failed to launch CLI monitor window: ${err?.message || err}`);
  }
}

// ---------------------------------------------------------------------------
// Session & Subprocess Lifecycle

async function createSession({ workspace, model: rawModel, effort: rawEffort, agent, permissionMode, timeoutMinutes }) {
  const resolved = await resolveModelSelection(rawModel);
  const sessionId = `asess_${randomUUID()}`;
  const logPath = path.join(SESSIONS_LOG_DIR, `${sessionId}.jsonl`);
  const session = {
    sessionId,
    conversationId: null,
    workspace: resolveWorkspace(workspace),
    model: resolved.model,
    effort: rawEffort || resolved.effort,
    agent: agent || null,
    permissionMode: permissionMode === "safe" ? "safe" : "yolo",
    timeoutMinutes: Number(timeoutMinutes) || DEFAULT_TIMEOUT_MINUTES,
    process: null,
    processExited: false,
    activeJobId: null,
    logPath,
    viewerLaunched: false,
    createdAt: new Date().toISOString(),
    lastUsedAt: new Date().toISOString(),
  };
  sessions.set(sessionId, session);
  writeSessionEvent(session, {
    type: "meta",
    session_id: sessionId,
    model: session.model,
    effort: session.effort,
    workspace: session.workspace,
    started_at: session.createdAt,
  });
  return session;
}

function spawnAgyProcess(session) {
  const args = [
    "--input-format", "stream-json",
    "--output-format", "stream-json",
  ];

  if (session.permissionMode === "yolo") {
    args.push("--dangerously-skip-permissions");
    args.push("--mode", "accept-edits");
  }

  if (session.model) {
    args.push("--model", session.model);
  }

  if (session.effort) {
    args.push("--effort", session.effort);
  }

  if (session.agent) {
    args.push("--agent", session.agent);
  }

  if (session.conversationId) {
    args.push("--conversation", session.conversationId);
  }

  logEvent(`spawning agy in ${session.workspace} (model=${session.model}, effort=${session.effort || "default"}, resume=${session.conversationId || "none"})`);

  const child = spawn(AGY_EXE, args, {
    cwd: session.workspace,
    env: getAgyEnv(),
    windowsHide: true,
    stdio: ["pipe", "pipe", "pipe"],
  });

  session.process = child;
  session.processExited = false;
  let stdoutBuffer = "";

  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    stdoutBuffer += chunk;
    for (;;) {
      const newlineIndex = stdoutBuffer.indexOf("\n");
      if (newlineIndex < 0) break;
      const rawLine = stdoutBuffer.slice(0, newlineIndex).trim();
      stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1);
      if (!rawLine) continue;
      handleAgyEventLine(session, rawLine);
    }
  });

  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    const activeJob = session.activeJobId ? jobs.get(session.activeJobId) : null;
    if (activeJob) {
      activeJob.stderr = safeTail(activeJob.stderr + chunk);
      activeJob.lastActivityMs = Date.now();
    }
    writeSessionEvent(session, { type: "stderr", text: chunk });
  });

  child.on("error", (error) => {
    logEvent(`session ${session.sessionId} agy process error: ${error.message}`);
    session.processExited = true;
    const activeJob = session.activeJobId ? jobs.get(session.activeJobId) : null;
    if (activeJob && !TERMINAL_STATUSES.has(activeJob.status)) {
      activeJob.status = "failed";
      activeJob.stderr = safeTail(`${activeJob.stderr}\n${error.stack || error.message}`);
      settleJob(activeJob);
    }
  });

  child.on("close", (code, signal) => {
    logEvent(`session ${session.sessionId} agy process closed (code=${code}, signal=${signal})`);
    session.processExited = true;
    session.process = null;
    const activeJob = session.activeJobId ? jobs.get(session.activeJobId) : null;
    if (activeJob && !TERMINAL_STATUSES.has(activeJob.status)) {
      if (activeJob.status === "cancelling") {
        activeJob.status = "cancelled";
      } else {
        activeJob.status = "failed";
        const detail = signal ? `by signal ${signal}` : `with exit code ${code}`;
        const reason = code === 0
          ? `Antigravity CLI process exited cleanly (${detail}) before providing a stream-json 'result' event. Job marked as failed.`
          : `Antigravity CLI process terminated unexpectedly (${detail}).`;
        activeJob.stderr = safeTail(`${activeJob.stderr}\n${reason} Session conversation_id '${session.conversationId || "unknown"}' is preserved for lazy recovery via continue_task.`);
        activeJob.diagnostics = safeTail(`${activeJob.diagnostics || ""}\n${reason}`);
      }
      activeJob.exitCode = code;
      settleJob(activeJob);
    }
  });

  return child;
}

function handleAgyEventLine(session, rawLine) {
  let message;
  try {
    message = JSON.parse(rawLine);
  } catch (err) {
    logEvent(`session ${session.sessionId} received invalid JSON: ${rawLine.slice(0, 100)}`);
    return;
  }

  const activeJob = session.activeJobId ? jobs.get(session.activeJobId) : null;
  if (!activeJob) return;

  activeJob.lastActivityMs = Date.now();

  switch (message.event) {
    case "init": {
      if (message.conversation_id) {
        session.conversationId = message.conversation_id;
        activeJob.conversationId = message.conversation_id;
      }
      logEvent(`session ${session.sessionId} initialized conversation ${session.conversationId}`);
      writeSessionEvent(session, { type: "init", conversation_id: session.conversationId });
      break;
    }

    case "step_update": {
      const update = message.step_update;
      if (!update) break;

      if (!activeJob.conversationId && update.conversation_id) {
        activeJob.conversationId = update.conversation_id;
        session.conversationId = update.conversation_id;
      }

      if (update.text_delta) {
        activeJob.stdout = safeTail(activeJob.stdout + update.text_delta);
        writeSessionEvent(session, { type: "text_delta", text: update.text_delta });
      }

      if (update.thought_delta || update.reasoning_delta) {
        writeSessionEvent(session, { type: "thought_delta", text: update.thought_delta || update.reasoning_delta });
      }

      if (update.tool_call) {
        writeSessionEvent(session, { type: "tool_call", name: update.tool_call.name, input: update.tool_call.input });
      }

      if (update.tool_result) {
        writeSessionEvent(session, { type: "tool_result", name: update.tool_result.name, output: update.tool_result.output });
      }

      if (update.usage) {
        activeJob.usage = { ...activeJob.usage, ...update.usage };
      }

      activeJob.progress = {
        last_activity_age_s: 0,
        step_index: update.step_index ?? null,
        step_type: update.step_type ?? null,
        state: update.state ?? null,
        total_tokens: update.usage?.total_tokens ?? activeJob.usage?.total_tokens ?? null,
      };
      writeSessionEvent(session, { type: "step_progress", progress: activeJob.progress });
      break;
    }

    case "result": {
      const res = message.result || {};
      if (res.conversation_id) {
        session.conversationId = res.conversation_id;
        activeJob.conversationId = res.conversation_id;
      }
      if (res.response) {
        activeJob.response = res.response;
        activeJob.stdout = safeTail(res.response);
      }
      if (res.usage) {
        activeJob.usage = res.usage;
      }

      activeJob.exitCode = res.status === "SUCCESS" ? 0 : 1;
      activeJob.status = res.status === "SUCCESS" ? "completed" : "failed";
      if (res.status !== "SUCCESS" && !activeJob.stderr) {
        activeJob.stderr = `Task ended with status: ${res.status}`;
      }
      settleJob(activeJob);
      break;
    }

    default:
      logEvent(`session ${session.sessionId} received event: ${message.event}`);
      break;
  }
}

function settleJob(job) {
  if (job.settled) return;
  job.settled = true;
  job.completedAt = new Date().toISOString();
  logEvent(`job ${job.jobId} settled with status: ${job.status}`);

  const session = sessions.get(job.sessionId);
  if (session) {
    const duration_s = Math.max(0, Math.round((Date.now() - Date.parse(job.startedAt)) / 1000));
    writeSessionEvent(session, {
      type: "turn_complete",
      job_id: job.jobId,
      status: job.status,
      exit_code: job.exitCode,
      usage: job.usage,
      duration_s,
    });
    if (session.activeJobId === job.jobId) {
      session.activeJobId = null;
      session.lastUsedAt = new Date().toISOString();
    }
  }

  releaseSlot(job);
}

// ---------------------------------------------------------------------------
// Job Dispatch & Execution

async function startJob(args, isContinue = false) {
  let session;
  if (isContinue) {
    const sessionId = args.session_id;
    if (!sessionId) throw new Error("session_id is required for continue_task");
    session = sessions.get(sessionId);
    if (!session) throw new Error(`unknown session_id: ${sessionId}`);
    if (session.activeJobId) {
      throw new Error(`session ${sessionId} already has an active job (${session.activeJobId}); please wait for it to complete`);
    }
  } else {
    session = await createSession(args);
  }

  const task = args.task;
  if (!task || typeof task !== "string" || !task.trim()) {
    throw new Error("task is required");
  }

  const timeoutMinutes = Number(args.timeout_minutes) || session.timeoutMinutes || DEFAULT_TIMEOUT_MINUTES;
  const jobId = `ajob_${randomUUID()}`;
  const job = {
    jobId,
    sessionId: session.sessionId,
    conversationId: session.conversationId,
    workspace: session.workspace,
    model: session.model,
    prompt: task,
    timeoutMinutes,
    status: "queued",
    startedAt: new Date().toISOString(),
    completedAt: null,
    exitCode: null,
    stdout: "",
    stderr: "",
    response: null,
    usage: null,
    progress: null,
    lastActivityMs: Date.now(),
    slotHeld: false,
    settled: false,
    cancelRequested: false,
  };

  jobs.set(jobId, job);
  session.activeJobId = jobId;
  logEvent(`job ${jobId} created for session ${session.sessionId} (${isContinue ? "continue" : "new"}, timeout=${timeoutMinutes}m)`);

  void executeJob(job, session);
  return publicJob(job, false);
}

function wrapAutonomousPrompt(task) {
  return [
    "You are operating in autonomous execution mode in the shared workspace.",
    "Auto-approve and execute all required tool calls (edits, shell commands, inspections) directly without asking the user for interactive confirmation or pausing in plan mode.",
    "Complete the implementation and verification fully, then summarize the outcome and modified files.",
    "",
    "TASK:",
    task,
  ].join("\n");
}

async function executeJob(job, session) {
  const granted = await acquireSlot(job);
  if (!granted || job.status === "cancelled") {
    releaseSlot(job);
    return;
  }

  job.status = "running";
  job.lastActivityMs = Date.now();

  writeSessionEvent(session, {
    type: "turn_start",
    job_id: job.jobId,
    prompt: job.prompt,
  });
  launchSessionViewer(session);

  try {
    if (!session.process || session.processExited) {
      spawnAgyProcess(session);
    }

    await sleep(200);

    const userPromptContent = session.permissionMode === "yolo" ? wrapAutonomousPrompt(job.prompt) : job.prompt;

    const userEvent = {
      event: "user",
      message: {
        content: userPromptContent,
      },
    };

    session.process.stdin.write(`${JSON.stringify(userEvent)}\n`);
    logEvent(`job ${job.jobId} sent turn prompt to agy stdin`);

    const timeoutMs = (job.timeoutMinutes || DEFAULT_TIMEOUT_MINUTES) * 60_000;
    const hardDeadline = Date.now() + Math.min(timeoutMs, TASK_HARD_TIMEOUT_MS);
    while (!TERMINAL_STATUSES.has(job.status)) {
      await sleep(1000);

      if (Date.now() > hardDeadline) {
        throw new Error(`Task exceeded timeout limit (${job.timeoutMinutes || DEFAULT_TIMEOUT_MINUTES}m)`);
      }

      const silenceMs = Date.now() - job.lastActivityMs;
      if (silenceMs > TASK_IDLE_TIMEOUT_MS) {
        throw new Error(`agy CLI stalled: no activity for ${Math.round(silenceMs / 1000)}s`);
      }
    }
  } catch (error) {
    if (job.status !== "cancelled") {
      job.status = "failed";
      job.stderr = safeTail(`${job.stderr}\n${error.stack || error.message}`);
    }
    settleJob(job);
    // Proactively kill the agy process so it cannot continue mutating workspace in background
    await terminateSessionProcess(session, `job execution aborted: ${error.message}`);
  }
}

async function cancelJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) throw new Error(`unknown job_id: ${jobId}`);
  if (TERMINAL_STATUSES.has(job.status)) return publicJob(job);

  if (job.status === "queued") {
    job.status = "cancelled";
    job.completedAt = new Date().toISOString();
    dropSlotWaiter(job);
    settleJob(job);
    return publicJob(job);
  }

  job.status = "cancelling";
  job.cancelRequested = true;

  const session = sessions.get(job.sessionId);
  if (session) {
    await terminateSessionProcess(session, `job ${jobId} cancelled`);
  }

  job.status = "cancelled";
  settleJob(job);
  return publicJob(job);
}

function publicJob(job, includeOutput = true) {
  const result = {
    job_id: job.jobId,
    session_id: job.sessionId,
    conversation_id: job.conversationId || null,
    status: job.status,
    model: job.model,
    workspace: job.workspace,
    started_at: job.startedAt,
    completed_at: job.completedAt,
    exit_code: job.exitCode,
  };

  if (includeOutput) {
    result.output = redactSensitive(job.response || job.stdout.trim());
    result.diagnostics = redactSensitive(job.stderr.trim());
    if (job.usage) {
      result.usage = job.usage;
    }
    if (job.progress) {
      result.progress = {
        ...job.progress,
        last_activity_age_s: Math.max(0, Math.round((Date.now() - job.lastActivityMs) / 1000)),
      };
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// TCP Dispatcher & Server

async function dispatch(method, params) {
  switch (method) {
    case "health":
      return {
        ...SERVER,
        active_jobs: hasActiveJobs(),
        parallel_limit: MAX_PARALLEL_JOBS,
        sessions_count: sessions.size,
        port: BROKER_PORT,
      };

    case "list_models":
      return await getAvailableModelFamilies();

    case "run_task":
      return await startJob(params, false);

    case "continue_task":
      return await startJob(params, true);

    case "get_status": {
      const job = jobs.get(params.job_id);
      if (!job) throw new Error(`unknown job_id: ${params.job_id}`);

      const waitMs = Math.min(Number(params.wait_ms) || 0, 45_000);
      if (waitMs > 0 && !TERMINAL_STATUSES.has(job.status)) {
        const fingerprint = () => `${job.status}|${job.conversationId || ""}|${job.stdout.length}|${job.stderr.length}|${job.progress?.step_index || ""}`;
        const initial = fingerprint();
        const deadline = Date.now() + waitMs;
        while (Date.now() < deadline && !TERMINAL_STATUSES.has(job.status) && fingerprint() === initial) {
          await sleep(400);
        }
      }
      return publicJob(job);
    }

    case "cancel_task":
      return cancelJob(params.job_id);

    default:
      throw new Error(`unknown broker method: ${method}`);
  }
}

function writeMessage(socket, message) {
  if (!socket.destroyed) {
    socket.write(`${JSON.stringify(message)}\n`);
  }
}

async function handleLine(socket, line) {
  let request;
  try {
    request = JSON.parse(line);
  } catch (error) {
    writeMessage(socket, { id: null, error: { message: `invalid JSON line: ${error.message}` } });
    return;
  }
  const id = request?.id ?? null;
  try {
    const result = await dispatch(request?.method, request?.params || {});
    writeMessage(socket, { id, result });
  } catch (error) {
    writeMessage(socket, { id, error: { message: redactSensitive(error.message) } });
  }
}

const server = net.createServer((socket) => {
  clients.add(socket);
  lastActivity = Date.now();
  socket.setEncoding("utf8");
  let buffer = "";

  socket.on("data", (chunk) => {
    buffer += chunk;
    for (;;) {
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex < 0) break;
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (line) void handleLine(socket, line);
    }
  });

  socket.on("error", () => { /* client dropped */ });
  socket.on("close", () => {
    clients.delete(socket);
    lastActivity = Date.now();
  });
});

server.on("error", (error) => {
  if (error.code === "EADDRINUSE") {
    process.exit(0);
  }
  process.stderr.write(`antigravity-broker server error: ${error.stack || error.message}\n`);
  process.exit(1);
});

server.listen(BROKER_PORT, "127.0.0.1", () => {
  process.stderr.write(`antigravity-broker listening on 127.0.0.1:${BROKER_PORT} (max parallel jobs: ${MAX_PARALLEL_JOBS})\n`);
});

setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobs) {
    if (TERMINAL_STATUSES.has(job.status) && job.completedAt && now - Date.parse(job.completedAt) > JOB_TTL_MS) {
      jobs.delete(id);
    }
  }

  for (const [id, session] of sessions) {
    if (!session.activeJobId && session.processExited && now - Date.parse(session.lastUsedAt) > JOB_TTL_MS) {
      sessions.delete(id);
    }
  }

let isShuttingDown = false;
async function shutdownBroker(reason = "idle timeout") {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logEvent(`initiating graceful broker shutdown (${reason})...`);

  try {
    server.close();
  } catch { /* ignore */ }

  const terminations = [];
  for (const session of sessions.values()) {
    if (session.process && !session.processExited) {
      terminations.push(terminateSessionProcess(session, `broker shutdown (${reason})`));
    }
  }

  try {
    await Promise.allSettled(terminations);
    logEvent("all persistent agy sessions terminated cleanly");
  } catch (err) {
    logEvent(`error during shutdown termination: ${err?.message || err}`);
  }

  process.exit(0);
}

  if (clients.size === 0 && !hasActiveJobs() && now - lastActivity > IDLE_EXIT_MS) {
    logEvent("antigravity-broker idle timeout reached, triggering graceful shutdown");
    void shutdownBroker("idle timeout");
  }
}, 30_000).unref();

process.on("SIGINT", () => { void shutdownBroker("SIGINT"); });
process.on("SIGTERM", () => { void shutdownBroker("SIGTERM"); });

process.on("exit", () => {
  for (const session of sessions.values()) {
    if (session.process && !session.processExited && session.process.pid) {
      try {
        if (process.platform === "win32") {
          const { execFileSync } = require("node:child_process");
          execFileSync("taskkill", ["/pid", String(session.process.pid), "/T", "/F"], { stdio: "ignore" });
        } else {
          process.kill(session.process.pid, "SIGKILL");
        }
      } catch { /* ignore */ }
    }
  }
});

process.on("uncaughtException", (error) => {
  process.stderr.write(`antigravity-broker uncaught exception: ${error.stack || error.message}\n`);
});

process.on("unhandledRejection", (reason) => {
  process.stderr.write(`antigravity-broker unhandled rejection: ${reason?.stack || reason}\n`);
});
