function hasMySQLConnectionOptions(options) {
  const opts = options || {};
  return Boolean(
    String(opts.host || "").trim() ||
      String(opts.port || "").trim() ||
      String(opts.username || "").trim() ||
      String(opts.database || "").trim(),
  );
}

export function hasExplicitSinkConfigs(sinkConfigs) {
  if (!sinkConfigs || sinkConfigs.length === 0) {
    return false;
  }
  if (sinkConfigs.length === 1 && sinkConfigs[0].type === "MYSQL") {
    return hasMySQLConnectionOptions(sinkConfigs[0].options);
  }
  return true;
}

export function resolveTaskTargetMySQLDisplay(config, globalTargetConfig = {}) {
  const targetDb = config?.target_db || {};
  const globalTarget = globalTargetConfig || {};
  return {
    host: targetDb.host || globalTarget.host || "",
    port: targetDb.port || globalTarget.port || "",
    username: targetDb.username || globalTarget.username || "",
  };
}

export function resolveMySQLSinkConnectionDisplay(
  sink,
  config,
  globalTargetConfig = {},
) {
  const opts = sink?.options || {};
  const fallback = resolveTaskTargetMySQLDisplay(config, globalTargetConfig);
  return {
    host: opts.host || fallback.host,
    port: opts.port || fallback.port,
    username: opts.username || fallback.username,
  };
}
