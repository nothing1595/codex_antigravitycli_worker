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
const IDLE_EXIT_MS = Number(process.env.AGY_BROKER_IDLE_MS || 0); // 0 = persistent daemon (no auto-exit)
const JOB_TTL_MS = 60 * 60_000;
const TASK_IDLE_TIMEOUT_MS = Number(process.env.AGY_TASK_IDLE_TIMEOUT_MS || 10 * 60_000);
const TASK_HARD_TIMEOUT_MS = Number(process.env.AGY_TASK_TIMEOUT_MS || 4 * 60 * 60_000);
const configuredDefaultTimeoutMinutes = Number(process.env.AGY_DEFAULT_TIMEOUT_MINUTES || 240);
const DEFAULT_TIMEOUT_MINUTES = Number.isFinite(configuredDefaultTimeoutMinutes) && configuredDefaultTimeoutMinutes > 0
  ? configuredDefaultTimeoutMinutes
  : 240;
const MAX_OUTPUT_CHARS = 120_000;
const MODELS_CACHE_TTL_MS = Number(process.env.AGY_MODELS_CACHE_TTL_MS || 5 * 60_000);
const MODELS_QUERY_TIMEOUT_MS = Number(process.env.AGY_MODELS_TIMEOUT_MS || 12_000);
const MODELS_QUERY_RETRIES = Number(process.env.AGY_MODELS_RETRIES || 2);
const MODELS_QUERY_BACKOFF_MS = Number(process.env.AGY_MODELS_BACKOFF_MS || 800);

function resolveHostUserProfile() {
  if (process.env.AGY_USER_PROFILE && fs.existsSync(process.env.AGY_USER_PROFILE)) {
    return process.env.AGY_USER_PROFILE;
  }
  const defaultProfile = "C:\\Users\\15869";
  if (fs.existsSync(defaultProfile)) return defaultProfile;
  return os.homedir();
}

const HOST_USER_PROFILE = resolveHostUserProfile();

// Enforce that broker only runs under the interactive authenticated host user
function assertHostUserSecurity(options = {}) {
  const current = (options.testUsername || os.userInfo().username || "").toLowerCase();
  const host = path.basename(HOST_USER_PROFILE).toLowerCase();
  if (current.includes("sandbox") || (current !== host && current !== "system")) {
    const msg = `FATAL: antigravity-broker cannot run under sandbox user '${current}'. It must run under host user '${host}' to access Antigravity credentials and desktop display.`;
    if (options.throwInsteadOfExit) {
      const err = new Error(msg);
      err.code = "ESANDBOXUSER";
      err.exitCode = 42;
      throw err;
    }
    process.stderr.write(`${new Date().toISOString()} ${msg}\n`);
    process.exit(42);
  }
}

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

  // Network proxy preservation & auto-detection for Google API connectivity
  const defaultProxy = process.env.HTTPS_PROXY || process.env.HTTP_PROXY || "http://127.0.0.1:10808";
  if (!env.HTTPS_PROXY && defaultProxy) {
    env.HTTPS_PROXY = defaultProxy;
    env.https_proxy = defaultProxy;
  }
  if (!env.HTTP_PROXY && defaultProxy) {
    env.HTTP_PROXY = defaultProxy;
    env.http_proxy = defaultProxy;
  }
  if (!env.NO_PROXY) {
    env.NO_PROXY = "localhost,127.0.0.1,::1";
    env.no_proxy = "localhost,127.0.0.1,::1";
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
    worker_name: "agy_gemini3.7flash_worker",
    model_family: "Gemini 3.7 Flash",
    target_model: "gemini-3.7-flash-high",
    description: "Gemini 3.7 Flash (最高推理: High)",
    effort: "high",
  },
  {
    worker_name: "agy_gemini3.6flash_worker",
    model_family: "Gemini 3.6 Flash",
    target_model: "gemini-3.6-flash-high",
    description: "Gemini 3.6 Flash (最高推理: High)",
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
    target_model: "claude-sonnet-4-6",
    description: "Claude Sonnet 4.6 (最高推理: Thinking)",
    effort: "high",
  },
  {
    worker_name: "agy_claudeopus4.6_worker",
    model_family: "Claude Opus 4.6",
    target_model: "claude-opus-4-6-thinking",
    description: "Claude Opus 4.6 (最高推理: Thinking)",
    effort: "high",
  },
  {
    worker_name: "agy_gptoss120b_worker",
    model_family: "GPT-OSS 120B",
    target_model: "gpt-oss-120b-medium",
    description: "GPT-OSS 120B (最高推理: Medium)",
    effort: "medium",
  },
];

// Fast-path model alias dictionary to completely skip `agy models` on dispatch
const KNOWN_MODEL_ALIASES = {
  "gemini-3.8-flash-high": { model: "gemini-3.8-flash-high", effort: "high" },
  "gemini-3.8-flash-medium": { model: "gemini-3.8-flash-medium", effort: "medium" },
  "gemini-3.8-flash-low": { model: "gemini-3.8-flash-low", effort: "low" },
  "gemini-3.8-flash": { model: "gemini-3.8-flash-high", effort: "high" },
  "gemini3.8flash": { model: "gemini-3.8-flash-high", effort: "high" },
  "agy_gemini3.8flash_worker": { model: "gemini-3.8-flash-high", effort: "high" },

  "gemini-3.7-flash-high": { model: "gemini-3.7-flash-high", effort: "high" },
  "gemini-3.7-flash-medium": { model: "gemini-3.7-flash-medium", effort: "medium" },
  "gemini-3.7-flash-low": { model: "gemini-3.7-flash-low", effort: "low" },
  "gemini-3.7-flash": { model: "gemini-3.7-flash-high", effort: "high" },
  "gemini3.7flash": { model: "gemini-3.7-flash-high", effort: "high" },
  "agy_gemini3.7flash_worker": { model: "gemini-3.7-flash-high", effort: "high" },

  "gemini-3.6-flash-high": { model: "gemini-3.6-flash-high", effort: "high" },
  "gemini-3.6-flash-medium": { model: "gemini-3.6-flash-medium", effort: "medium" },
  "gemini-3.6-flash-low": { model: "gemini-3.6-flash-low", effort: "low" },
  "gemini-3.6-flash": { model: "gemini-3.6-flash-high", effort: "high" },
  "gemini3.6flash": { model: "gemini-3.6-flash-high", effort: "high" },
  "agy_gemini3.6flash_worker": { model: "gemini-3.6-flash-high", effort: "high" },

  "gemini-3.1-pro-high": { model: "gemini-3.1-pro-high", effort: "high" },
  "gemini-3.1-pro-low": { model: "gemini-3.1-pro-low", effort: "low" },
  "gemini-3.1-pro": { model: "gemini-3.1-pro-high", effort: "high" },
  "gemini3.1pro": { model: "gemini-3.1-pro-high", effort: "high" },
  "agy_gemini3.1pro_worker": { model: "gemini-3.1-pro-high", effort: "high" },

  "gemini-3-flash-high": { model: "gemini-3-flash-high", effort: "high" },
  "gemini-3-flash": { model: "gemini-3-flash-high", effort: "high" },
  "gemini3flash": { model: "gemini-3-flash-high", effort: "high" },
  "agy_gemini3flash_worker": { model: "gemini-3-flash-high", effort: "high" },

  "claude-sonnet-4.6-thinking": { model: "claude-sonnet-4-6", effort: "high" },
  "claude-sonnet-4-6": { model: "claude-sonnet-4-6", effort: "high" },
  "claude-sonnet-4.6": { model: "claude-sonnet-4-6", effort: "high" },
  "claudesonnet4.6": { model: "claude-sonnet-4-6", effort: "high" },
  "agy_claudesonnet4.6_worker": { model: "claude-sonnet-4-6", effort: "high" },

  "claude-opus-4.6-thinking": { model: "claude-opus-4-6-thinking", effort: "high" },
  "claude-opus-4-6-thinking": { model: "claude-opus-4-6-thinking", effort: "high" },
  "claude-opus-4.6": { model: "claude-opus-4-6-thinking", effort: "high" },
  "claude-opus-4-6": { model: "claude-opus-4-6-thinking", effort: "high" },
  "claudeopus4.6": { model: "claude-opus-4-6-thinking", effort: "high" },
  "agy_claudeopus4.6_worker": { model: "claude-opus-4-6-thinking", effort: "high" },

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

// Persistent Cache Paths & Helpers
const MODELS_CACHE_DIR = process.env.AGY_CACHE_DIR || path.join(HOST_USER_PROFILE, ".antigravity-codex-bridge");
const MODELS_CACHE_FILE = process.env.AGY_MODELS_CACHE_FILE || path.join(MODELS_CACHE_DIR, "models-cache.json");

function loadPersistentCache(cacheFile = MODELS_CACHE_FILE) {
  try {
    if (fs.existsSync(cacheFile)) {
      const raw = fs.readFileSync(cacheFile, "utf8");
      const data = JSON.parse(raw);
      if (data && Array.isArray(data.model_families) && data.model_families.length > 0) {
        return {
          timestamp: Number(data.timestamp) || 0,
          updated_at: data.updated_at || new Date(data.timestamp || 0).toISOString(),
          source: data.source || "file_cache",
          model_families: data.model_families,
          raw_count: data.raw_count || 0,
        };
      }
    }
  } catch (err) {
    logEvent(`failed to load persistent models cache from ${cacheFile}: ${err?.message || err}`);
  }
  return null;
}

function savePersistentCache(modelFamilies, rawCount = 0, cacheFile = MODELS_CACHE_FILE) {
  try {
    const dir = path.dirname(cacheFile);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    const payload = {
      version: 1,
      timestamp: Date.now(),
      updated_at: new Date().toISOString(),
      source: "agy_models_cli",
      raw_count: rawCount,
      families_count: modelFamilies.length,
      model_families: modelFamilies.map((m) => {
        const { stale, diagnostics, source, ...rest } = m;
        return rest;
      }),
    };
    const tmpFile = `${cacheFile}.${process.pid}.${Date.now()}.tmp`;
    fs.writeFileSync(tmpFile, JSON.stringify(payload, null, 2), "utf8");
    fs.renameSync(tmpFile, cacheFile);
    return true;
  } catch (err) {
    logEvent(`failed to save persistent models cache to ${cacheFile}: ${err?.message || err}`);
    return false;
  }
}

// Global in-memory cache and diagnostic tracking
let lastSuccessfulModels = null;
let lastSuccessfulTimestamp = 0;
let lastSuccessfulSource = "built_in_defaults";
let lastModelQueryDiagnostic = {
  timestamp: Date.now(),
  status: "initial",
  reason: null,
  error: null,
  source: "built_in_defaults",
};

// Attempt to load persistent cache on startup
const initialDiskCache = loadPersistentCache();
if (initialDiskCache) {
  lastSuccessfulModels = [...initialDiskCache.model_families];
  lastSuccessfulTimestamp = initialDiskCache.timestamp;
  lastSuccessfulSource = "file_cache";
  lastModelQueryDiagnostic = {
    timestamp: initialDiskCache.timestamp,
    status: "success",
    reason: null,
    error: null,
    source: "file_cache",
    count: initialDiskCache.model_families.length,
  };
  logEvent(`loaded ${initialDiskCache.model_families.length} model families from persistent cache (${MODELS_CACHE_FILE})`);
}

let cachedModels = lastSuccessfulModels ? [...lastSuccessfulModels] : [...DEFAULT_MODEL_FAMILIES];
let cachedModelsTime = lastSuccessfulTimestamp;

function categorizeModelQueryError(err, stdout = "", stderr = "") {
  const fullText = `${err?.message || ""} ${err?.stderr || ""} ${stderr} ${stdout}`.toLowerCase();
  if (
    err?.killed ||
    err?.signal === "SIGTERM" ||
    err?.code === "ETIMEDOUT" ||
    fullText.includes("timed out") ||
    fullText.includes("timeout")
  ) {
    return {
      reason: "timeout",
      message: `Dynamic query timed out after ${MODELS_QUERY_TIMEOUT_MS}ms`,
    };
  }
  if (
    fullText.includes("lock") ||
    fullText.includes("resource busy") ||
    fullText.includes("locked") ||
    fullText.includes("already in use") ||
    fullText.includes("ebusy")
  ) {
    return {
      reason: "lock_contention",
      message: `Lock contention detected: ${stderr.trim() || err?.message || ""}`,
    };
  }
  if (
    fullText.includes("sign in") ||
    fullText.includes("login") ||
    fullText.includes("unauthorized") ||
    fullText.includes("credentials") ||
    fullText.includes("permission denied") ||
    fullText.includes("auth")
  ) {
    return {
      reason: "auth_permission",
      message: `Authentication/permission error: ${stderr.trim() || err?.message || ""}`,
    };
  }
  if (err?.code === "ENOENT") {
    return {
      reason: "binary_not_found",
      message: `Antigravity executable not found: ${AGY_EXE}`,
    };
  }
  if (err?.code && typeof err.code === "number" && err.code !== 0) {
    return {
      reason: "exit_non_zero",
      message: `Process exited with code ${err.code}: ${stderr.trim() || err?.message || ""}`,
    };
  }
  if (!stdout || stdout.trim().length === 0) {
    return {
      reason: "empty_output",
      message: `Command returned empty output: ${stderr.trim() || err?.message || "no output"}`,
    };
  }
  return {
    reason: "unknown_error",
    message: stderr.trim() || err?.message || "Unknown error during agy models query",
  };
}

function validateRawModels(rawModels) {
  if (!Array.isArray(rawModels) || rawModels.length === 0) {
    return { valid: false, reason: "empty_output", message: "Model list is empty" };
  }

  const slugRegex = /^[a-z0-9][a-z0-9_.-]{1,64}$/i;
  const validModels = [];
  const seenSlugs = new Set();
  let duplicateCount = 0;

  for (const m of rawModels) {
    if (!m || typeof m.slug !== "string" || typeof m.name !== "string") continue;
    const slug = m.slug.trim();
    const name = m.name.trim();
    if (!slug || !name) continue;
    const lowerSlug = slug.toLowerCase();
    if (
      lowerSlug.startsWith("fetching") ||
      lowerSlug.startsWith("warning") ||
      lowerSlug.startsWith("error") ||
      lowerSlug.includes("failed") ||
      lowerSlug.includes("unauthorized")
    ) {
      continue;
    }
    if (!slugRegex.test(slug)) continue;

    if (seenSlugs.has(lowerSlug)) {
      duplicateCount++;
      continue;
    }
    seenSlugs.add(lowerSlug);
    validModels.push({ slug, name });
  }

  if (validModels.length === 0) {
    return {
      valid: false,
      reason: "malformed_output",
      message: "No valid model entries parsed from output (all lines invalid or malformed)",
    };
  }

  // Anomaly check: duplicate flood (e.g. corrupt stdout repeating identical lines)
  if (rawModels.length >= 8 && duplicateCount > validModels.length * 2) {
    return {
      valid: false,
      reason: "anomaly_duplicate_flood",
      message: `Excessive duplicate model entries detected (${duplicateCount} duplicates vs ${validModels.length} unique)`,
    };
  }

  return { valid: true, models: validModels, uniqueCount: validModels.length };
}

function validateFamilyQuality(newFamilies, previousFamilies) {
  if (!Array.isArray(newFamilies) || newFamilies.length === 0) {
    return {
      acceptable: false,
      reason: "empty_families",
      message: "No model families could be constructed from raw models",
    };
  }

  // Structural sanity on every family
  for (const f of newFamilies) {
    if (!f.worker_name || !f.model_family || !f.target_model || !f.effort) {
      return {
        acceptable: false,
        reason: "malformed_family_structure",
        message: `Model family missing required fields: ${JSON.stringify(f)}`,
      };
    }
  }

  // Relative completeness check against previous successful cache
  if (previousFamilies && Array.isArray(previousFamilies) && previousFamilies.length >= 3) {
    const minAcceptableCount = Math.max(2, Math.floor(previousFamilies.length * 0.5));
    if (newFamilies.length < minAcceptableCount) {
      return {
        acceptable: false,
        reason: "partial_result",
        message: `Suspected partial model discovery: returned ${newFamilies.length} families vs ${previousFamilies.length} previously cached (< 50% threshold: ${minAcceptableCount})`,
      };
    }
  }

  return { acceptable: true };
}

function fetchRawModelsOnce() {
  return new Promise((resolve) => {
    execFile(
      AGY_EXE,
      ["models"],
      {
        env: getAgyEnv(),
        windowsHide: true,
        timeout: MODELS_QUERY_TIMEOUT_MS,
        stdio: ["ignore", "pipe", "pipe"],
      },
      (err, stdout, stderr) => {
        if (err || !stdout) {
          const cat = categorizeModelQueryError(err, stdout, stderr);
          return resolve({ success: false, ...cat, models: [] });
        }
        const lines = stdout.split("\n");
        const rawParsed = [];
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || trimmed.startsWith("Fetching")) continue;
          const parts = trimmed.split("\t");
          if (parts.length >= 2) {
            rawParsed.push({ slug: parts[0].trim(), name: parts[1].trim() });
          }
        }
        if (rawParsed.length === 0) {
          const cat = categorizeModelQueryError(err || new Error("empty model list"), stdout, stderr);
          return resolve({ success: false, ...cat, models: [] });
        }
        const validation = validateRawModels(rawParsed);
        if (!validation.valid) {
          return resolve({
            success: false,
            reason: validation.reason,
            message: validation.message,
            models: [],
          });
        }
        resolve({ success: true, models: validation.models, rawCount: validation.uniqueCount });
      }
    );
  });
}

async function fetchRawModelsWithRetry() {
  const maxAttempts = 1 + Math.max(0, MODELS_QUERY_RETRIES);
  let lastFailure = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const result = await fetchRawModelsOnce();
    if (result.success && result.models.length > 0) {
      if (attempt > 1) {
        logEvent(`dynamic models query succeeded on retry attempt ${attempt}/${maxAttempts}`);
      }
      return result;
    }

    lastFailure = result;
    logEvent(`dynamic models query attempt ${attempt}/${maxAttempts} failed (${result.reason}): ${result.message}`);

    if (result.reason === "auth_permission" || result.reason === "binary_not_found") {
      break;
    }

    if (attempt < maxAttempts) {
      const delay = MODELS_QUERY_BACKOFF_MS * attempt;
      await sleep(delay);
    }
  }

  return lastFailure || { success: false, reason: "unknown", message: "All query attempts failed" };
}

let fetchModelsInFlight = null;
function fetchDynamicModelsSingleton() {
  if (fetchModelsInFlight) {
    return fetchModelsInFlight;
  }

  fetchModelsInFlight = (async () => {
    try {
      return await fetchRawModelsWithRetry();
    } finally {
      fetchModelsInFlight = null;
    }
  })();

  return fetchModelsInFlight;
}

function formatModelResponse(families, { stale, source, diagnostics, detailed }) {
  const modelsWithMeta = families.map((m) => ({
    ...m,
    stale: Boolean(stale),
    source: source || "unknown",
    ...(diagnostics ? { diagnostics } : {}),
  }));

  if (detailed) {
    return {
      models: modelsWithMeta,
      count: modelsWithMeta.length,
      stale: Boolean(stale),
      source: source || "unknown",
      diagnostics: diagnostics || null,
      timestamp: Date.now(),
    };
  }

  return modelsWithMeta;
}

async function getAvailableModelFamilies(params = {}) {
  const now = Date.now();
  const detailed = Boolean(params?.detailed);
  const force = Boolean(params?.force);

  // Return fresh memory cache if within TTL and not forcing refresh
  if (!force && lastSuccessfulModels && (now - lastSuccessfulTimestamp < MODELS_CACHE_TTL_MS) && lastSuccessfulTimestamp > 0) {
    return formatModelResponse(lastSuccessfulModels, {
      stale: false,
      source: lastSuccessfulSource,
      diagnostics: null,
      detailed,
    });
  }

  const queryResult = await fetchDynamicModelsSingleton();

  if (queryResult.success && queryResult.models && queryResult.models.length > 0) {
    const familyMap = new Map();
    for (const m of queryResult.models) {
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

    const families = Array.from(familyMap.values()).map(({ score, ...rest }) => rest);
    const qualityCheck = validateFamilyQuality(families, lastSuccessfulModels);

    if (!qualityCheck.acceptable) {
      logEvent(`model family quality check rejected result (${qualityCheck.reason}): ${qualityCheck.message}`);
      queryResult.success = false;
      queryResult.reason = qualityCheck.reason;
      queryResult.message = qualityCheck.message;
    } else {
      lastSuccessfulModels = families;
      lastSuccessfulTimestamp = now;
      lastSuccessfulSource = "agy_models_cli";
      cachedModels = families;
      cachedModelsTime = now;
      lastModelQueryDiagnostic = {
        timestamp: now,
        status: "success",
        reason: null,
        error: null,
        source: "agy_models_cli",
        count: families.length,
      };

      savePersistentCache(families, queryResult.rawCount);

      return formatModelResponse(families, {
        stale: false,
        source: "agy_models_cli",
        diagnostics: null,
        detailed,
      });
    }
  }

  // Dynamic discovery failed!
  const diagReason = queryResult.reason || "unknown_failure";
  const diagMsg = queryResult.message || queryResult.error || "Dynamic model query failed";
  logEvent(`dynamic model discovery failed (${diagReason}): ${diagMsg}`);

  // PREFER PREVIOUS SUCCESSFUL CACHE (memory or persistent file cache)
  if (lastSuccessfulModels && lastSuccessfulModels.length > 0) {
    const ageSeconds = Math.max(0, Math.round((now - lastSuccessfulTimestamp) / 1000));
    const diagNote = `Dynamic query failed (${diagReason}: ${diagMsg}). Serving cached model list (${lastSuccessfulModels.length} models, age: ${ageSeconds}s, source: ${lastSuccessfulSource}).`;
    lastModelQueryDiagnostic = {
      timestamp: now,
      status: "stale",
      reason: diagReason,
      error: diagMsg,
      source: lastSuccessfulSource,
      cached_timestamp: lastSuccessfulTimestamp,
      count: lastSuccessfulModels.length,
    };
    cachedModels = lastSuccessfulModels;
    return formatModelResponse(lastSuccessfulModels, {
      stale: true,
      source: lastSuccessfulSource,
      diagnostics: diagNote,
      detailed,
    });
  }

  // ONLY USE BUILT-IN FALLBACK IF NEVER HAD SUCCESSFUL QUERY
  const fallbackNote = `Dynamic query failed (${diagReason}: ${diagMsg}) and no persistent cache is available. Serving built-in default models.`;
  lastModelQueryDiagnostic = {
    timestamp: now,
    status: "fallback",
    reason: diagReason,
    error: diagMsg,
    source: "built_in_defaults",
    count: DEFAULT_MODEL_FAMILIES.length,
  };
  return formatModelResponse(DEFAULT_MODEL_FAMILIES, {
    stale: true,
    source: "built_in_defaults",
    diagnostics: fallbackNote,
    detailed,
  });
}

// Warm up dynamic models in background on startup so list_models gets full models instantly
if (require.main === module) {
  setTimeout(() => {
    getAvailableModelFamilies().then((families) => {
      const count = Array.isArray(families) ? families.length : families.count;
      logEvent(`warmup: discovered ${count} dynamic model families (source: ${lastSuccessfulSource})`);
    }).catch(() => {});
  }, 1500).unref();
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

let lastSpawnSlotPromise = Promise.resolve();

async function acquireSpawnSlot() {
  const previous = lastSpawnSlotPromise;
  let release;
  let resolved = false;
  lastSpawnSlotPromise = new Promise((r) => { release = r; });

  await previous;

  const timer = setTimeout(() => {
    if (!resolved) {
      resolved = true;
      release();
    }
  }, 4000);

  return () => {
    if (!resolved) {
      resolved = true;
      clearTimeout(timer);
      release();
    }
  };
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
  session.initPromise = new Promise((resolve, reject) => {
    session._initResolve = resolve;
    session._initReject = reject;
  });
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
    const isPreInit = typeof session._initReject === "function";
    if (isPreInit) {
      session._initReject(error);
      session._initResolve = null;
      session._initReject = null;
    }
    const activeJob = session.activeJobId ? jobs.get(session.activeJobId) : null;
    if (activeJob && !TERMINAL_STATUSES.has(activeJob.status)) {
      if (isPreInit) {
        logEvent(`pre-init error for job ${activeJob.jobId}, delegating recovery to executeJob`);
        return;
      }
      activeJob.status = "failed";
      activeJob.stderr = safeTail(`${activeJob.stderr}\n${error.stack || error.message}`);
      settleJob(activeJob);
    }
  });

  child.on("close", (code, signal) => {
    logEvent(`session ${session.sessionId} agy process closed (code=${code}, signal=${signal})`);
    session.processExited = true;
    session.process = null;
    const isPreInit = typeof session._initReject === "function";
    if (isPreInit) {
      session._initReject(new Error(`agy CLI closed with code ${code} before emitting 'init'`));
      session._initResolve = null;
      session._initReject = null;
    }
    const activeJob = session.activeJobId ? jobs.get(session.activeJobId) : null;
    if (activeJob && !TERMINAL_STATUSES.has(activeJob.status)) {
      if (isPreInit) {
        logEvent(`pre-init exit for job ${activeJob.jobId}, delegating recovery to executeJob`);
        return;
      }
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
      if (typeof session._initResolve === "function") {
        session._initResolve(message);
        session._initResolve = null;
        session._initReject = null;
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
    const MAX_START_ATTEMPTS = 3;
    let started = false;
    for (let attempt = 1; attempt <= MAX_START_ATTEMPTS; attempt++) {
      if (job.status === "cancelling" || job.status === "cancelled") {
        return;
      }
      let releaseSpawn = null;
      try {
        if (!session.process || session.processExited) {
          releaseSpawn = await acquireSpawnSlot();
          spawnAgyProcess(session);
        }

        if (session.initPromise) {
          logEvent(`job ${job.jobId} waiting for agy process initialization ('init' event, attempt ${attempt}/${MAX_START_ATTEMPTS})...`);
          const initTimeout = sleep(25000).then(() => {
            throw new Error("Timed out (25s) waiting for Antigravity CLI process to initialize (emit 'init' event)");
          });
          await Promise.race([session.initPromise, initTimeout]);
          session.initPromise = null;
        }
        started = true;
        break;
      } catch (startErr) {
        logEvent(`job ${job.jobId} process startup attempt ${attempt} failed: ${startErr.message}`);
        await terminateSessionProcess(session, `startup retry cleanup`);
        if (attempt < MAX_START_ATTEMPTS) {
          await sleep(1200 * attempt);
        } else {
          throw startErr;
        }
      } finally {
        if (releaseSpawn) {
          releaseSpawn();
          releaseSpawn = null;
        }
      }
    }

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
        user: os.userInfo().username,
        pid: process.pid,
        models_count: cachedModels ? cachedModels.length : 0,
        models_status: lastModelQueryDiagnostic,
        models_source: lastSuccessfulSource,
        models_cache_file: MODELS_CACHE_FILE,
      };

    case "list_models":
      return await getAvailableModelFamilies(params);

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

let server = null;

if (require.main === module) {
  assertHostUserSecurity();
  cleanupOrphanAgyModels();

  server = net.createServer((socket) => {
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

    if (IDLE_EXIT_MS > 0 && clients.size === 0 && !hasActiveJobs() && now - lastActivity > IDLE_EXIT_MS) {
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
}

let isShuttingDown = false;
async function shutdownBroker(reason = "idle timeout") {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logEvent(`initiating graceful broker shutdown (${reason})...`);

  try {
    if (server) server.close();
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

module.exports = {
  SERVER,
  BROKER_PORT,
  DEFAULT_TIMEOUT_MINUTES,
  DEFAULT_MODEL_FAMILIES,
  KNOWN_MODEL_ALIASES,
  MODELS_CACHE_FILE,
  MODELS_CACHE_DIR,
  getAgyEnv,
  getBaseFamilyName,
  getEffortScore,
  makeWorkerName,
  categorizeModelQueryError,
  loadPersistentCache,
  savePersistentCache,
  fetchRawModelsOnce,
  fetchRawModelsWithRetry,
  fetchDynamicModelsSingleton,
  getAvailableModelFamilies,
  resolveModelSelection,
  formatModelResponse,
  dispatch,
  assertHostUserSecurity,
  cleanupOrphanAgyModels,
  validateRawModels,
  validateFamilyQuality,
};
