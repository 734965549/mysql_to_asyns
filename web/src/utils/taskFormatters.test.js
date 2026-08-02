import test from "node:test";
import assert from "node:assert/strict";

import {
  formatTime,
  calculateDuration,
  shouldShowFullSyncFailedReason,
} from "./taskFormatters.js";

test("formatTime rejects zero and pre-2000 timestamps", () => {
  assert.equal(formatTime(null), "-");
  assert.equal(formatTime(undefined), "-");
  assert.equal(formatTime(""), "-");
  assert.equal(formatTime("0001-01-01T00:00:00Z"), "-");
  assert.equal(formatTime("1999-01-01T00:00:00Z"), "-");
});

test("formatTime renders valid timestamps", () => {
  const formatted = formatTime("2026-08-02T05:30:00.000Z");
  assert.notEqual(formatted, "-");
  assert.match(formatted, /2026/);
});

test("calculateDuration ignores zero end_time and uses now", () => {
  const duration = calculateDuration("2026-08-02T05:00:00.000Z", "0001-01-01T00:00:00Z");
  assert.notEqual(duration, "-");
  assert.match(duration, /秒|分|小时/);
});

test("shouldShowFullSyncFailedReason hides stale reason when current error_stack exists", () => {
  assert.equal(
    shouldShowFullSyncFailedReason({
      status: "FAILED",
      sync_phase: "FULL_FAILED",
      full_sync_failed_reason: "previous round table copy failed",
      error_stack: "analyze table src.t1: metadata unavailable",
    }),
    false,
  );
});

test("shouldShowFullSyncFailedReason shows phase history when no current error_stack", () => {
  assert.equal(
    shouldShowFullSyncFailedReason({
      status: "FAILED",
      sync_phase: "FULL_FAILED",
      full_sync_failed_reason: "previous round table copy failed",
      error_stack: "",
    }),
    true,
  );
});

test("shouldShowFullSyncFailedReason requires FAILED + FULL_FAILED", () => {
  assert.equal(
    shouldShowFullSyncFailedReason({
      status: "RUNNING",
      sync_phase: "FULL_FAILED",
      full_sync_failed_reason: "still running",
    }),
    false,
  );
});
