export function resolveTargetDatabaseName(mapping) {
  const source = String(mapping?.source || "").trim();
  const target = String(mapping?.target || "").trim();

  return target || source;
}

export function buildTargetDatabasesPayload(mappings) {
  return (mappings || []).map(resolveTargetDatabaseName);
}

export function getTaskDatabaseMappings(task) {
  if (!task?.config) {
    return [];
  }

  const config = task.config;
  const hasSourceDatabaseList = config.source_databases?.length > 0;
  const sourceDatabases = hasSourceDatabaseList
    ? config.source_databases
    : config.source_schema
      ? [config.source_schema]
      : [];
  const targetDatabases = config.target_databases || [];
  const singleDatabaseFallback =
    !hasSourceDatabaseList &&
    String(config.target_schema || config.target_database || "").trim();

  return sourceDatabases.map((source, index) => ({
    source,
    target:
      String(targetDatabases[index] || "").trim() ||
      singleDatabaseFallback ||
      source,
  }));
}
