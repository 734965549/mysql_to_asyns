import test from "node:test";
import assert from "node:assert/strict";
import {
  hasExplicitSinkConfigs,
  resolveMySQLSinkConnectionDisplay,
  resolveTaskTargetMySQLDisplay,
} from "./taskTargetDisplay.js";

test("hasExplicitSinkConfigs treats default MYSQL placeholder as implicit", () => {
  assert.equal(hasExplicitSinkConfigs([]), false);
  assert.equal(
    hasExplicitSinkConfigs([{ type: "MYSQL", options: {} }]),
    false,
  );
  assert.equal(
    hasExplicitSinkConfigs([
      { type: "MYSQL", options: { host: "10.0.0.2", port: 3306 } },
    ]),
    true,
  );
  assert.equal(
    hasExplicitSinkConfigs([{ type: "KAFKA", options: { topic: "events" } }]),
    true,
  );
});

test("resolveTaskTargetMySQLDisplay prefers task target_db over global config", () => {
  assert.deepEqual(
    resolveTaskTargetMySQLDisplay(
      {
        target_db: { host: "task-host", port: 3307, username: "task-user" },
      },
      { host: "global-host", port: 3306, username: "global-user" },
    ),
    { host: "task-host", port: 3307, username: "task-user" },
  );
  assert.deepEqual(
    resolveTaskTargetMySQLDisplay({}, { host: "global-host", port: 3306 }),
    { host: "global-host", port: 3306, username: "" },
  );
});

test("resolveMySQLSinkConnectionDisplay falls back to task/global target config", () => {
  assert.deepEqual(
    resolveMySQLSinkConnectionDisplay(
      { type: "MYSQL", options: {} },
      { target_db: { host: "task-host", port: 3306, username: "task-user" } },
      {},
    ),
    { host: "task-host", port: 3306, username: "task-user" },
  );
  assert.deepEqual(
    resolveMySQLSinkConnectionDisplay(
      { type: "MYSQL", options: { host: "sink-host" } },
      { target_db: { host: "task-host", port: 3306, username: "task-user" } },
      {},
    ),
    { host: "sink-host", port: 3306, username: "task-user" },
  );
});
