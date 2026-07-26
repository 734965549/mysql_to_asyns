/**
 * 任务目标端 / MySQL Sink 展示解析工具。
 * 集中处理任务目标端与 sink_configs 的展示解析、密钥脱敏，以及"隐式占位 MYSQL sink"判定。
 */

// 后端回显密码时统一使用此掩码；前端编辑前必须清空（见 unmaskSecret），
// 否则会把掩码字符串当作真实密码提交。
export const SECRET_MASK = "******";

// 判断值是否为后端回显的密码掩码。
export function isMaskedSecret(value) {
  return String(value || "") === SECRET_MASK;
}

// 把掩码密码清空为空串，避免把 "******" 当真实密码提交；非掩码值原样返回。
export function unmaskSecret(value) {
  return isMaskedSecret(value) ? "" : String(value || "");
}

function hasMySQLConnectionOptions(options) {
  const opts = options || {};
  return Boolean(
    String(opts.host || "").trim() ||
      String(opts.port || "").trim() ||
      String(opts.username || "").trim() ||
      String(opts.database || "").trim(),
  );
}

// 判断单个 MYSQL sink 是否携带了显式连接参数（host/port/username/database 任一非空）。
export function isSingleExplicitMySQLSink(sinkConfigs) {
  return Boolean(
    sinkConfigs &&
      sinkConfigs.length === 1 &&
      sinkConfigs[0].type === "MYSQL" &&
      hasMySQLConnectionOptions(sinkConfigs[0].options),
  );
}

// 判断 sinkConfigs 是否包含"显式" sink 配置。
// 关键规则：单个 MYSQL sink 且无任何连接参数时视为"隐式占位目标端"（仅占位，实际使用任务 target_db），
// 不算显式 sink_configs；只有 2 个以上 sink 或单个带连接参数的 MYSQL sink 才算显式。
export function hasExplicitSinkConfigs(sinkConfigs) {
  if (!sinkConfigs || sinkConfigs.length === 0) {
    return false;
  }
  if (sinkConfigs.length === 1 && sinkConfigs[0].type === "MYSQL") {
    return hasMySQLConnectionOptions(sinkConfigs[0].options);
  }
  return true;
}

// 解析任务目标端 MySQL 展示信息。回退链：任务 config.target_db -> 全局 configForm.target。
export function resolveTaskTargetMySQLDisplay(config, globalTargetConfig = {}) {
  const targetDb = config?.target_db || {};
  const globalTarget = globalTargetConfig || {};
  return {
    host: targetDb.host || globalTarget.host || "",
    port: targetDb.port || globalTarget.port || "",
    username: targetDb.username || globalTarget.username || "",
  };
}

// 解析单个 MYSQL sink 的连接展示信息。回退链：sink.options -> 任务 target_db -> 全局 target。
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
