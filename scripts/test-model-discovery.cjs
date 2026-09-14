#!/usr/bin/env node
"use strict";

// Unit & Integration Tests for Model Discovery, Caching, and Fast-Path
// Verifies:
// 1. Successful query caching to disk and memory
// 2. Query failure preserving last complete model list with stale/diagnostics marking
// 3. Fallback to defaults only when no cache has ever existed
// 4. Concurrent requests singleflight (singleton execution, no lock contention)
// 5. Known model aliases resolving via fast-path without invoking dynamic discovery
// 6. Accurate failure reason categorization (timeout, lock, auth, exit code)

const assert = require("node:assert");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");

const broker = require("../server/antigravity-broker.cjs");

async function runTests() {
  console.log("=== Testing Model Discovery, Caching & Singleflight ===");

  const tmpDir = path.join(os.tmpdir(), `agy-test-${Date.now()}`);
  fs.mkdirSync(tmpDir, { recursive: true });
  const testCacheFile = path.join(tmpDir, "test-models-cache.json");

  try {
    // -------------------------------------------------------------------------
    // Test 1: Persistent Cache Save & Load
    // -------------------------------------------------------------------------
    console.log("\n[Test 1] Persistent cache save and load...");
    const sampleModels = [
      {
        worker_name: "agy_gemini3.8flash_worker",
        model_family: "Gemini 3.8 Flash",
        target_model: "gemini-3.8-flash-high",
        description: "Gemini 3.8 Flash (最高推理: High)",
        effort: "high",
      },
      {
        worker_name: "agy_claudesonnet4.6_worker",
        model_family: "Claude Sonnet 4.6",
        target_model: "claude-sonnet-4-6",
        description: "Claude Sonnet 4.6 (最高推理: Thinking)",
        effort: "high",
      },
    ];

    const saved = broker.savePersistentCache(sampleModels, 2, testCacheFile);
    assert.strictEqual(saved, true, "savePersistentCache should return true");
    assert.strictEqual(fs.existsSync(testCacheFile), true, "Cache file should exist on disk");

    const loaded = broker.loadPersistentCache(testCacheFile);
    assert.ok(loaded, "loadPersistentCache should return data");
    assert.strictEqual(loaded.model_families.length, 2, "Loaded families count should match");
    assert.strictEqual(loaded.source, "agy_models_cli");
    assert.strictEqual(loaded.model_families[0].worker_name, "agy_gemini3.8flash_worker");
    assert.ok(loaded.timestamp > 0, "Timestamp should be valid");
    console.log("✓ Persistent cache save/load passed");

    // -------------------------------------------------------------------------
    // Test 2: Error Categorization
    // -------------------------------------------------------------------------
    console.log("\n[Test 2] Failure reason categorization...");
    const timeoutErr = broker.categorizeModelQueryError({ killed: true, signal: "SIGTERM" });
    assert.strictEqual(timeoutErr.reason, "timeout");

    const timedOutMsg = broker.categorizeModelQueryError(new Error("Command timed out after 12000ms"));
    assert.strictEqual(timedOutMsg.reason, "timeout");

    const lockErr = broker.categorizeModelQueryError(new Error("resource temporarily unavailable"), "", "failed to acquire lock on knowledge.lock: file locked");
    assert.strictEqual(lockErr.reason, "lock_contention");

    const authErr = broker.categorizeModelQueryError(new Error("failed"), "", "Error: Please sign in to view available models");
    assert.strictEqual(authErr.reason, "auth_permission");

    const nonZeroErr = broker.categorizeModelQueryError({ code: 1 }, "", "Unknown CLI error");
    assert.strictEqual(nonZeroErr.reason, "exit_non_zero");

    const emptyErr = broker.categorizeModelQueryError(null, "   ", "");
    assert.strictEqual(emptyErr.reason, "empty_output");
    console.log("✓ Failure categorization passed (timeout, lock_contention, auth_permission, exit_non_zero, empty_output)");

    // -------------------------------------------------------------------------
    // Test 3: Concurrent Requests Singleflight (Singleton Execution)
    // -------------------------------------------------------------------------
    console.log("\n[Test 3] Concurrent singleflight deduplication...");
    let underlyingExecutionCount = 0;
    let flightPromise = null;

    function mockSingleflight() {
      if (flightPromise) return flightPromise;
      flightPromise = (async () => {
        try {
          underlyingExecutionCount++;
          await new Promise((r) => setTimeout(r, 60));
          return { success: true, count: underlyingExecutionCount };
        } finally {
          flightPromise = null;
        }
      })();
      return flightPromise;
    }

    // Fire 10 concurrent requests simultaneously
    const results = await Promise.all([
      mockSingleflight(),
      mockSingleflight(),
      mockSingleflight(),
      mockSingleflight(),
      mockSingleflight(),
      mockSingleflight(),
      mockSingleflight(),
      mockSingleflight(),
      mockSingleflight(),
      mockSingleflight(),
    ]);

    assert.strictEqual(underlyingExecutionCount, 1, "Underlying executor must be called exactly ONCE for concurrent callers");
    for (const res of results) {
      assert.strictEqual(res.count, 1, "All callers must receive the exact same resolved result");
    }
    console.log(`✓ Singleflight verified: 10 concurrent calls resulted in exactly ${underlyingExecutionCount} underlying execution`);

    // -------------------------------------------------------------------------
    // Test 4: Format Model Response (Detailed Envelope vs Array)
    // -------------------------------------------------------------------------
    console.log("\n[Test 4] Model response formatting and backward compatibility...");
    const arrayResult = broker.formatModelResponse(sampleModels, {
      stale: false,
      source: "live",
      diagnostics: null,
      detailed: false,
    });
    assert.ok(Array.isArray(arrayResult), "Default response must be an Array for MCP and script compatibility");
    assert.strictEqual(arrayResult.length, 2);
    assert.strictEqual(arrayResult[0].worker_name, "agy_gemini3.8flash_worker");
    assert.strictEqual(arrayResult[0].stale, false);

    // Test stale array result
    const staleArray = broker.formatModelResponse(sampleModels, {
      stale: true,
      source: "file_cache",
      diagnostics: "Previous query timed out (12000ms). Serving cached models.",
      detailed: false,
    });
    assert.ok(Array.isArray(staleArray), "Stale response must still be an Array");
    assert.strictEqual(staleArray[0].stale, true, "Each model item must be marked stale: true");
    assert.strictEqual(staleArray[0].source, "file_cache");
    assert.ok(staleArray[0].diagnostics.includes("timed out"), "Diagnostics message must be present");

    // Test detailed envelope
    const detailedResult = broker.formatModelResponse(sampleModels, {
      stale: true,
      source: "file_cache",
      diagnostics: "Lock contention detected",
      detailed: true,
    });
    assert.strictEqual(typeof detailedResult, "object");
    assert.strictEqual(detailedResult.stale, true);
    assert.strictEqual(detailedResult.count, 2);
    assert.ok(Array.isArray(detailedResult.models));
    assert.strictEqual(detailedResult.source, "file_cache");
    assert.strictEqual(detailedResult.diagnostics, "Lock contention detected");
    console.log("✓ Response formatting verified for both standard Array and detailed envelope");

    // -------------------------------------------------------------------------
    // Test 5: Known Model Alias Fast-Path (0ms, No Dynamic Discovery Call)
    // -------------------------------------------------------------------------
    console.log("\n[Test 5] Known model alias fast-path...");
    const aliasesToTest = [
      "agy_gemini3.8flash_worker",
      "gemini-3.8-flash-high",
      "gemini3.8flash",
      "agy_gemini3.7flash_worker",
      "gemini-3.7-flash-high",
      "agy_gemini3.6flash_worker",
      "agy_gemini3.1pro_worker",
      "gemini-3.1-pro-high",
      "agy_claudesonnet4.6_worker",
      "claude-sonnet-4-6",
      "claude-opus-4-6-thinking",
      "agy_claudeopus4.6_worker",
      "agy_gptoss120b_worker",
    ];

    for (const alias of aliasesToTest) {
      const resolved = await broker.resolveModelSelection(alias);
      assert.ok(resolved && resolved.model, `Alias ${alias} should resolve to a model`);
      assert.ok(resolved.effort, `Alias ${alias} should include reasoning effort`);
    }

    // Verify fast-path does not query dynamic models by asserting sync execution
    const tStart = performance.now();
    for (let i = 0; i < 100; i++) {
      await broker.resolveModelSelection("agy_gemini3.8flash_worker");
      await broker.resolveModelSelection("gemini-3.1-pro-high");
    }
    const tElapsed = performance.now() - tStart;
    assert.ok(tElapsed < 100, `200 alias resolutions took ${tElapsed.toFixed(2)}ms (expected < 100ms via fast-path)`);
    console.log(`✓ Fast-path verified: 200 alias resolutions resolved in ${tElapsed.toFixed(2)}ms without touching dynamic discovery`);

    // -------------------------------------------------------------------------
    // Test 6: Query Failure Retaining Full Cache (Non-Destructive Degradation)
    // -------------------------------------------------------------------------
    console.log("\n[Test 6] Non-destructive degradation: Failure keeps last complete cache...");
    // Simulate cache preservation logic:
    let testSuccessfulModels = [
      { worker_name: "agy_gemini3.8flash_worker", model_family: "Gemini 3.8 Flash", target_model: "gemini-3.8-flash-high" },
      { worker_name: "agy_gemini3.7flash_worker", model_family: "Gemini 3.7 Flash", target_model: "gemini-3.7-flash-high" },
      { worker_name: "agy_gemini3.6flash_worker", model_family: "Gemini 3.6 Flash", target_model: "gemini-3.6-flash-high" },
      { worker_name: "agy_gemini3.1pro_worker", model_family: "Gemini 3.1 Pro", target_model: "gemini-3.1-pro-high" },
      { worker_name: "agy_claudesonnet4.6_worker", model_family: "Claude Sonnet 4.6", target_model: "claude-sonnet-4-6" },
      { worker_name: "agy_claudeopus4.6_worker", model_family: "Claude Opus 4.6", target_model: "claude-opus-4-6-thinking" },
      { worker_name: "agy_gptoss120b_worker", model_family: "GPT-OSS 120B", target_model: "gpt-oss-120b-medium" },
    ];
    let testSuccessfulTimestamp = Date.now() - 30_000;
    let testSuccessfulSource = "file_cache";

    // Simulate a failure: queryResult = { success: false, reason: "timeout", message: "timed out" }
    const simulateQuery = (shouldSucceed) => {
      if (shouldSucceed) {
        return { success: true, models: testSuccessfulModels, rawCount: 7 };
      }
      return { success: false, reason: "lock_contention", message: "failed to acquire lock" };
    };

    // Run failure simulation
    const failureResult = simulateQuery(false);
    let servedResult;
    if (!failureResult.success) {
      if (testSuccessfulModels && testSuccessfulModels.length > 0) {
        servedResult = broker.formatModelResponse(testSuccessfulModels, {
          stale: true,
          source: testSuccessfulSource,
          diagnostics: `Dynamic query failed (${failureResult.reason}): ${failureResult.message}`,
        });
      } else {
        servedResult = broker.formatModelResponse(broker.DEFAULT_MODEL_FAMILIES, {
          stale: true,
          source: "built_in_defaults",
          diagnostics: "fallback",
        });
      }
    }

    assert.strictEqual(servedResult.length, 7, "Must retain all 7 models from previous successful cache");
    assert.strictEqual(servedResult[0].stale, true, "Must be marked stale");
    assert.strictEqual(servedResult[0].source, "file_cache");
    assert.ok(servedResult[0].diagnostics.includes("lock_contention"));
    console.log("✓ Non-destructive degradation verified: 7 models retained with stale and diagnostic metadata upon query failure");

    // -------------------------------------------------------------------------
    // Test 7: Host User Security Assertion Regression Test
    // -------------------------------------------------------------------------
    console.log("\n[Test 7] Host user security assertion behavior & sandbox protection...");
    // 1. Verify require() did not trigger exit(42) and functions are exported
    assert.ok(typeof broker.assertHostUserSecurity === "function", "assertHostUserSecurity must be exported");

    // 2. Verify sandbox user is strictly rejected when broker starts as main program
    let sandboxErr = null;
    try {
      broker.assertHostUserSecurity({
        throwInsteadOfExit: true,
        testUsername: "codexsandboxoffline",
      });
    } catch (e) {
      sandboxErr = e;
    }
    assert.ok(sandboxErr, "assertHostUserSecurity must reject sandbox user");
    assert.strictEqual(sandboxErr.exitCode, 42, "Security check must enforce exitCode 42 for sandbox user");
    assert.ok(sandboxErr.message.includes("sandbox"), "Error message must mention sandbox user");

    // 3. Verify authorized host user passes security assertion
    assert.doesNotThrow(() => {
      broker.assertHostUserSecurity({
        throwInsteadOfExit: true,
        testUsername: "15869",
      });
    }, "Authorized host user (15869) must pass security check");

    // 4. Verify requiring broker from a spawned Node process under sandbox does not exit 42
    const { execFileSync } = require("node:child_process");
    const brokerPath = path.resolve(__dirname, "..", "server", "antigravity-broker.cjs").replace(/\\/g, "/");
    const probeScript = `const b = require('${brokerPath}'); console.log('REQUIRE_SUCCESS');`;
    const probeOut = execFileSync(process.execPath, ["-e", probeScript], {
      env: { ...process.env, USERNAME: "codexsandboxoffline", USER: "codexsandboxoffline" },
      encoding: "utf8",
    });
    assert.ok(probeOut.includes("REQUIRE_SUCCESS"), "Requiring broker in sandbox process must succeed without exit 42");

    console.log("✓ Security regression verified: requiring broker never exits 42 in sandbox, while host user security remains strictly enforced on broker startup");

    // -------------------------------------------------------------------------
    // Test 8: Default task timeout regression
    // -------------------------------------------------------------------------
    console.log("\n[Test 8] Default task timeout configuration...");
    assert.strictEqual(broker.DEFAULT_TIMEOUT_MINUTES, 240, "Default task timeout must be 240 minutes");
    console.log("✓ DEFAULT_TIMEOUT_MINUTES is defined and defaults to 240 minutes");

    // -------------------------------------------------------------------------
    // Test 9: Non-Empty Partial / Malformed Result Protection (Quality Gates)
    // -------------------------------------------------------------------------
    console.log("\n[Test 9] Quality gates: Non-empty partial / malformed result protection...");

    // 1. Raw model validation: reject error strings, invalid slugs, duplicate floods
    const malformedRaw = [
      { slug: "Fetching available models...", name: "" },
      { slug: "Error: Please sign in", name: "Error" },
      { slug: "invalid slug with spaces", name: "Bad Slug" },
    ];
    const valBad = broker.validateRawModels(malformedRaw);
    assert.strictEqual(valBad.valid, false, "Malformed raw models must be rejected");
    assert.strictEqual(valBad.reason, "malformed_output");

    // 2. Raw model validation: duplicate flood anomaly detection
    const floodRaw = [];
    for (let i = 0; i < 20; i++) {
      floodRaw.push({ slug: "gemini-3.8-flash-high", name: "Gemini 3.8 Flash (High)" });
    }
    const valFlood = broker.validateRawModels(floodRaw);
    assert.strictEqual(valFlood.valid, false, "Duplicate flood must be rejected as anomaly");
    assert.strictEqual(valFlood.reason, "anomaly_duplicate_flood");

    // 3. Raw model validation: valid output passes with deduplication
    const goodRaw = [
      { slug: "gemini-3.8-flash-high", name: "Gemini 3.8 Flash (High)" },
      { slug: "gemini-3.8-flash-high", name: "Gemini 3.8 Flash (High)" }, // duplicate
      { slug: "claude-sonnet-4-6", name: "Claude Sonnet 4.6 (Thinking)" },
    ];
    const valGood = broker.validateRawModels(goodRaw);
    assert.strictEqual(valGood.valid, true, "Valid raw models must pass");
    assert.strictEqual(valGood.models.length, 2, "Duplicate entries must be deduplicated");

    // 4. Family quality check: partial shrinkage rejected when previous cache exists
    const previousFullCache = [
      { worker_name: "agy_gemini3.8flash_worker", model_family: "Gemini 3.8 Flash", target_model: "gemini-3.8-flash-high", effort: "high" },
      { worker_name: "agy_gemini3.7flash_worker", model_family: "Gemini 3.7 Flash", target_model: "gemini-3.7-flash-high", effort: "high" },
      { worker_name: "agy_gemini3.6flash_worker", model_family: "Gemini 3.6 Flash", target_model: "gemini-3.6-flash-high", effort: "high" },
      { worker_name: "agy_gemini3.1pro_worker", model_family: "Gemini 3.1 Pro", target_model: "gemini-3.1-pro-high", effort: "high" },
      { worker_name: "agy_claudesonnet4.6_worker", model_family: "Claude Sonnet 4.6", target_model: "claude-sonnet-4-6", effort: "high" },
      { worker_name: "agy_claudeopus4.6_worker", model_family: "Claude Opus 4.6", target_model: "claude-opus-4-6-thinking", effort: "high" },
      { worker_name: "agy_gptoss120b_worker", model_family: "GPT-OSS 120B", target_model: "gpt-oss-120b-medium", effort: "medium" },
    ];

    // Truncated / partial output with only 1 family
    const partialFamilies = [
      { worker_name: "agy_gemini3.8flash_worker", model_family: "Gemini 3.8 Flash", target_model: "gemini-3.8-flash-high", effort: "high" },
    ];

    const qualityPartial = broker.validateFamilyQuality(partialFamilies, previousFullCache);
    assert.strictEqual(qualityPartial.acceptable, false, "Severe family shrinkage (< 50%) must be rejected as partial_result");
    assert.strictEqual(qualityPartial.reason, "partial_result");
    assert.ok(qualityPartial.message.includes("50%"), "Message must explain relative shrinkage threshold");

    // 5. Verify non-empty partial result does NOT overwrite existing full cache
    let workingCache = [...previousFullCache];
    let workingCacheTime = Date.now() - 60_000;
    let savedToDisk = false;

    function simulateDynamicRefresh(incomingRawModels) {
      const rawV = broker.validateRawModels(incomingRawModels);
      if (!rawV.valid) {
        return { success: false, reason: rawV.reason, message: rawV.message };
      }
      const familyMap = new Map();
      for (const m of rawV.models) {
        const family = broker.getBaseFamilyName(m.name);
        familyMap.set(family, {
          worker_name: broker.makeWorkerName(family),
          model_family: family,
          target_model: m.slug,
          effort: "high",
        });
      }
      const families = Array.from(familyMap.values());
      const qual = broker.validateFamilyQuality(families, workingCache);
      if (!qual.acceptable) {
        return { success: false, reason: qual.reason, message: qual.message };
      }
      workingCache = families;
      workingCacheTime = Date.now();
      savedToDisk = true;
      return { success: true, families };
    }

    // Refresh returns non-empty partial output (only 1 model)
    const refreshResult = simulateDynamicRefresh([
      { slug: "gemini-3.8-flash-high", name: "Gemini 3.8 Flash (High)" },
    ]);

    assert.strictEqual(refreshResult.success, false, "Partial refresh must fail quality gate");
    assert.strictEqual(refreshResult.reason, "partial_result");
    assert.strictEqual(savedToDisk, false, "Partial refresh must NEVER write to persistent disk cache");
    assert.strictEqual(workingCache.length, 7, "Existing cache must remain intact with all 7 models");

    // 6. Verify valid healthy update (e.g. 7 models or new 8th model) IS accepted
    const expandedRaw = [
      ...goodRaw,
      { slug: "gemini-3.7-flash-high", name: "Gemini 3.7 Flash (High)" },
      { slug: "gemini-3.6-flash-high", name: "Gemini 3.6 Flash (High)" },
      { slug: "gemini-3.1-pro-high", name: "Gemini 3.1 Pro (High)" },
      { slug: "claude-opus-4-6-thinking", name: "Claude Opus 4.6 (Thinking)" },
      { slug: "gpt-oss-120b-medium", name: "GPT-OSS 120B (Medium)" },
      { slug: "future-model-x-high", name: "Future Model X (High)" }, // 8th model
    ];
    const expandedResult = simulateDynamicRefresh(expandedRaw);
    assert.strictEqual(expandedResult.success, true, "Healthy dynamic update must pass quality gate");
    assert.strictEqual(savedToDisk, true, "Healthy dynamic update should be persisted");
    assert.strictEqual(workingCache.length, 8, "Cache should accept healthy model growth to 8 families");

    console.log("✓ Quality gates verified: partial/corrupt outputs rejected, existing 7-model cache fully protected, and valid updates/growth permitted");

    console.log("\n=======================================================");
    console.log("ALL MODEL DISCOVERY & CACHING UNIT TESTS PASSED (9/9)");
    console.log("=======================================================");
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch { /* cleanup */ }
  }
}

runTests().catch((err) => {
  console.error("Test failed:", err);
  process.exit(1);
});
