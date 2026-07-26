import test from "node:test";
import assert from "node:assert/strict";

import {
  buildTargetDatabasesPayload,
  getTaskDatabaseMappings,
  resolveTargetDatabaseName,
} from "./databaseMappings.js";

test("same-name mappings remain aligned instead of reusing the first target", () => {
  const mappings = [
    { source: "xk-order", target: "xk-order" },
    { source: "xk-contract", target: "xk-contract" },
    { source: "xk-fund-tcl", target: "xk-fund-tcl" },
  ];

  assert.deepEqual(buildTargetDatabasesPayload(mappings), [
    "xk-order",
    "xk-contract",
    "xk-fund-tcl",
  ]);
});

test("an empty mapping defaults to its own source database", () => {
  assert.equal(
    resolveTargetDatabaseName({ source: "xk-contract", target: "" }),
    "xk-contract",
  );
});

test("an explicit renamed target is preserved", () => {
  assert.equal(
    resolveTargetDatabaseName({
      source: "xk-contract",
      target: "xk-contract-archive",
    }),
    "xk-contract-archive",
  );
});

test("task details prefer every stored positional mapping, including same-name entries", () => {
  const task = {
    config: {
      source_databases: ["xk-order", "xk-contract", "xk-fund-tcl"],
      target_databases: ["xk-order", "xk-contract", "xk-fund-tcl"],
      target_database: "xk-order",
      target_schema: "xk-order",
    },
  };

  assert.deepEqual(getTaskDatabaseMappings(task), [
    { source: "xk-order", target: "xk-order" },
    { source: "xk-contract", target: "xk-contract" },
    { source: "xk-fund-tcl", target: "xk-fund-tcl" },
  ]);
});

test("a missing multi-database target defaults positionally to the source", () => {
  const task = {
    config: {
      source_databases: ["xk-order", "xk-contract"],
      target_databases: [],
      target_database: "xk-order",
      target_schema: "xk-order",
    },
  };

  assert.deepEqual(getTaskDatabaseMappings(task), [
    { source: "xk-order", target: "xk-order" },
    { source: "xk-contract", target: "xk-contract" },
  ]);
});
