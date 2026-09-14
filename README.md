# Codex → Antigravity CLI Worker Bridge

A high-performance local Model Context Protocol (MCP) bridge exposing Google Antigravity CLI (`agy`) as an MCP worker for OpenAI Codex and its subagent fleet.

## Overview

Instead of proxying or pretending Gemini is a native Codex model, this bridge wraps the native **Antigravity CLI (`agy`)** into standard local MCP tools. Codex maintains a single, unified gateway worker (`agy_worker`) that dynamically inspects all available models on your local machine, maps them to friendly `agy_XXX_worker` identifiers running at their **maximum reasoning effort**, and delegates heavy implementation, refactoring, repository-wide search, and complex multi-step coding tasks directly into the shared workspace.

```
                         Codex Main
                             │
                      Unified Custom Agent
                         (agy_worker)
                             │
                            MCP (stdio JSON-RPC 2.0)
                             │
                server/antigravity-worker.cjs (thin MCP relay)
                             │
                loopback TCP 127.0.0.1:19225 (line-delimited JSON)
                             │
                server/antigravity-broker.cjs (singleton daemon)
                  ├── Dynamic Model Discoverer (list_models)
                  ├── Session Manager (1 Custom Agent session = 1 persistent agy process)
                  ├── Job / Turn Queue (FIFO semaphore, default AGY_MAX_PARALLEL_JOBS = 2)
                  └── Stream-JSON Parser & Progress Tracker
                             │
                             │ stdin:  {"event":"user","message":{"content":"..."}}
                             │ stdout: init -> step_update* -> result
                             ▼
                  Persistent child_process `agy.exe`
                  --input-format stream-json --output-format stream-json
                             │
                  Antigravity Agent Harness (yolo or safe mode)
                             │
                  Selected Model (Automatic Highest Reasoning Effort)
                  (e.g., Gemini 3.8 Flash High, Gemini 3.1 Pro High, Claude Sonnet 4.6 Thinking)
                             │
                             ▼
                  Target Shared Workspace
```

---

## Single Gateway Worker (`agy_worker`) & Dynamic Model Selection

To keep the Codex custom agents list clean and clutter-free, only **one** agent is registered: **`agy_worker`**.

When assigned in Codex:
1. **Model Discovery & Feedback**:
   If the user has not specified a model or asks what models are available, `agy_worker` invokes `list_models` and presents the available model families formatted as `agy_XXX_worker`:
   - `agy_gemini3.8flash_worker` — Gemini 3.8 Flash (最高推理: High)
   - `agy_gemini3.7flash_worker` — Gemini 3.7 Flash (最高推理: High)
   - `agy_gemini3.6flash_worker` — Gemini 3.6 Flash (最高推理: High)
   - `agy_gemini3.1pro_worker` — Gemini 3.1 Pro (最高推理: High)
   - `agy_claudesonnet4.6_worker` — Claude Sonnet 4.6 (最高推理: Thinking)
   - `agy_claudeopus4.6_worker` — Claude Opus 4.6 (最高推理: Thinking)
   - `agy_gptoss120b_worker` — GPT-OSS 120B (最高推理: Medium)
2. **Automatic Maximum Reasoning Effort**:
   Regardless of whether you provide the alias (e.g. `agy_gemini3.8flash_worker` or `gemini3.8flash`) or the raw slug, the broker automatically resolves and applies the **maximum reasoning effort** available for that model (e.g. `gemini-3.8-flash-high`, `gemini-3.1-pro-high`, `claude-sonnet-4-6`).
3. **Execution**:
   Once chosen, `agy_worker` calls `run_task` with the chosen model to execute the implementation.

---

## Key Advantages over UI Bridges (e.g. ZCode Bridge)

1. **Persistent CLI Process (`--input-format stream-json`)**:
   `agy` natively supports bidirectional stream JSON. A single `agy` process stays alive across all turns in a Codex conversation. Follow-up turns are sent straight into the running process's `stdin`, eliminating process startup and reload latencies.
2. **Headless by Nature (No CDP / No Window Mutex)**:
   Does not require Chrome DevTools Protocol or physical window UI clicking. Tasks run truly headless in the background.
3. **Native Real-time Telemetry (`stream-json`)**:
   `init`, `step_update`, and `result` events provide immediate `conversation_id`, active tool calls, incremental tokens, and streaming text deltas without polling an external SQLite database.
4. **Native Permission Bypass & Autonomous Mode**:
   `--dangerously-skip-permissions` combined with `--mode accept-edits` and autonomous prompt framing enables fully unattended execution (the `yolo` semantic) without interactive plan/permission pauses.
5. **Session-Level Lazy Recovery (Fail-Fast & Auto-Resume)**:
   If the underlying `agy` process terminates or crashes during a turn, the current task fails safely without blind destructive re-execution. The `conversation_id` is preserved in the session, and the next `continue_task` call automatically revives the session via `agy --conversation <id>`.
6. **Active Process Tree Termination**:
   When a task exceeds its configured `timeout_minutes`, experiences extended stall silence, or is cancelled, the broker actively and recursively kills the entire underlying subprocess tree (`taskkill /T /F`), ensuring orphan CLI processes cannot continue mutating the workspace in the background.

---

## MCP Tools Reference

- **`list_models()`**: Queries `agy models`, aggregates models into families, and returns the list of `agy_XXX_worker` identifiers along with their maximum reasoning effort configurations.
- **`run_task(workspace, task, model?, effort?, agent?, permission_mode?, timeout_minutes?)`**: Starts a persistent Antigravity session in the workspace. Supports model aliases (e.g. `agy_gemini3.8flash_worker`, `agy_gemini3.1pro_worker`) and defaults to maximum reasoning effort.
- **`continue_task(session_id, task)`**: Sends a follow-up turn prompt directly to the running session's stdin.
- **`get_status(job_id, wait_ms?)`**: Long-polling status and live progress telemetry.
- **`cancel_task(job_id)`**: Gracefully stops the active turn and halts the subprocess tree.

---

## Installation

Run the PowerShell installer:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install-agents.ps1
```

The installer will:
1. Dynamically query `agy models` to verify local Antigravity models.
2. Clean up any obsolete agent files (such as `agy-pro-worker.toml`).
3. Render and install the unified `agy-worker.toml` into your Codex `agents/` directory.

Restart Codex, then ask it to assign **`agy_worker`**.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AGY_EXE` | Auto-detected | Absolute path to `agy.exe`. |
| `AGY_NODE_EXE` | Auto-detected | Path to Node.js binary. |
| `AGY_BROKER_PORT` | `19225` | Broker TCP daemon loopback port. |
| `AGY_MAX_PARALLEL_JOBS` | `2` | Maximum concurrent active turns. |
| `AGY_BROKER_IDLE_MS` | `600000` (10m) | Broker auto-exit timeout when idle. |
| `AGY_TASK_TIMEOUT_MS` | `14400000` (4h) | Hard deadline timeout for a single task. |
| `AGY_TASK_IDLE_TIMEOUT_MS` | `600000` (10m) | Stall detection: silence timeout before failing job. |

---

## Verification & Smoke Tests

Run dynamic model discovery, alias resolution, and multi-turn continuation test:
```powershell
& "E:\Node.js\node.exe" scripts\smoke-run.cjs . agy_gemini3.8flash_worker
```

Run parallel execution smoke test (2 concurrent jobs):
```powershell
& "E:\Node.js\node.exe" scripts\smoke-parallel.cjs . gemini-3.8-flash-high
```

Run graceful cancellation test:
```powershell
& "E:\Node.js\node.exe" scripts\smoke-parallel.cjs . gemini-3.8-flash-high --cancel
```
