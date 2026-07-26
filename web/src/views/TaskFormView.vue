<script setup>
import { ref, computed, watch, onMounted, onUnmounted } from "vue";
import { useRoute, useRouter } from "vue-router";
import { Message } from "@arco-design/web-vue";
import { API_BASE } from "../composables/useApi.js";
import { useDefaultConfig } from "../composables/useDefaultConfig.js";
import {
  setFormHeaderActions,
  clearFormHeaderActions,
} from "../composables/useFormHeaderActions.js";
import {
  buildTargetDatabasesPayload as buildDatabaseMappingsPayload,
} from "../utils/databaseMappings.js";
import {
  hasExplicitSinkConfigs,
  isSingleExplicitMySQLSink,
  unmaskSecret,
  isMaskedSecret,
} from "../utils/taskTargetDisplay.js";

const route = useRoute();
const router = useRouter();
const { configForm, ensureDefaultConfig } = useDefaultConfig();

const editMode = computed(
  () => route.meta?.mode === "edit" || route.name === "task-edit",
);

const databases = ref([]);
const tables = ref([]);
const loading = ref(false);

const databaseSearchText = ref("");
const tableSearchText = ref("");
const selectedSyncLevel = ref("database");
const selectedDatabases = ref([]);
const targetDatabaseMappings = ref([]);
const targetTableMappings = ref({});
const selectedTables = ref([]);
const activeTableSourceDatabase = ref("");
const tableSelectionsByDatabase = ref({});
const tablesByDatabase = ref({});
const loadingTablesByDatabase = ref({});
const tableFetchErrors = ref({});
const tableFetchGlobalError = ref(null);
const expandedTableDatabaseKeys = ref([]);
const expandedTargetTableDatabaseKeys = ref([]);

const editingTaskId = ref(null);
const cloneFromTaskId = ref(null);

const useCustomSourceDB = ref(false);
const useCustomTargetDB = ref(false);
const customSourceDB = ref({ host: "", port: 3306, database: "", username: "", password: "" });
const customTargetDB = ref({ host: "", port: 3306, database: "", username: "", password: "" });

const SINK_TYPES = [
  { value: "MYSQL", label: "MySQL 数据库" },
  { value: "KAFKA", label: "Kafka" },
  { value: "HTTP_WEBHOOK", label: "HTTP Webhook" },
];

function getDefaultSinkOptions(type) {
  if (type === "MYSQL") {
    return { host: "", port: 3306, username: "", password: "", database: "", target_schema: "", batch_size: 1000 };
  } else if (type === "KAFKA") {
    return {
      brokers: "", topic: "", routing_mode: "single_topic", topic_prefix: "", key_mode: "pk",
      batch_size: 1000, batch_timeout_ms: 500, required_acks: 1,
      security: { sasl_mechanism: "", sasl_username: "", sasl_password: "", tls_enabled: false, ca_cert_path: "", client_cert_path: "", client_key_path: "", insecure_skip_verify: false },
    };
  } else if (type === "HTTP_WEBHOOK") {
    return { url: "", method: "POST", headers: "", timeout_ms: 3000, retry_times: 3, retry_backoff_ms: 500 };
  }
  return {};
}

const targetType = ref("MYSQL");
const singleKafkaConfig = ref(getDefaultSinkOptions("KAFKA"));
const singleWebhookConfig = ref(getDefaultSinkOptions("HTTP_WEBHOOK"));
const sinkConfigs = ref([]);

function addSinkConfig() {
  sinkConfigs.value.push({ type: "MYSQL", options: getDefaultSinkOptions("MYSQL") });
}
function removeSinkConfig(index) {
  sinkConfigs.value.splice(index, 1);
}
function onSinkTypeChange(index) {
  sinkConfigs.value[index].options = getDefaultSinkOptions(sinkConfigs.value[index].type);
}
function getSinkTypeLabel(type) {
  if (type === "MULTI") return "高级: 多目标 (Multi-Sink)";
  return SINK_TYPES.find((t) => t.value === type)?.label || type;
}

function normalizeSinkOptionsForForm(type, options) {
  const opts = { ...(options || {}) };
  if (type === "MYSQL") {
    opts.password = unmaskSecret(opts.password);
  } else if (type === "KAFKA") {
    if (Array.isArray(opts.brokers)) opts.brokers = opts.brokers.join(", ");
    const security = { ...(opts.security || {}) };
    security.sasl_password = unmaskSecret(security.sasl_password);
    opts.security = security;
  } else if (type === "HTTP_WEBHOOK") {
    if (opts.headers && typeof opts.headers === "object") {
      opts.headers = Object.entries(opts.headers).map(([k, v]) => `${k}: ${isMaskedSecret(v) ? "" : v}`).join("\n");
    } else if (isMaskedSecret(opts.headers)) {
      opts.headers = "";
    }
  }
  return opts;
}

const taskForm = ref({
  name: "", source_schema: "", target_schema: "", target_database: "",
  tables: [], target_tables: [], mode: "FULL",
  batch_size: 1000, worker_count: 4, intra_table_worker_count: 0,
  enable_limit_one: false, optimize_index: false, enable_read_only: false,
  enable_drop_table_before_ddl: false, enable_skip_binlog: false,
  tx_commit_every_n_parallel: 0, index_restore_worker_count: 0,
  full_load_engine: "v1",
  full_load_read_workers: 0, full_load_write_workers: 0, full_load_buffer_mb: 0,
  full_load_batch_bytes_mb: 0, full_load_commit_rows: 0, full_load_commit_bytes_mb: 0,
  full_load_lock_wait_timeout_sec: 0, full_load_degrade_on_align_lock_fail: false,
  allow_nopk_all: false,
});

function applyFullLoadPreset(name) {
  const f = taskForm.value;
  f.full_load_engine = "v2";
  const presets = {
    balanced: [4, 4, 128, 4, 10000, 32, 10, true],
    speed:    [8, 8, 256, 8, 20000, 64, 10, true],
    low:      [2, 2, 64,  2, 5000,  16, 10, true],
    auto:     [0, 0, 0,   0, 0,     0,  0,  true],
  };
  const p = presets[name] || presets.auto;
  [f.full_load_read_workers, f.full_load_write_workers, f.full_load_buffer_mb,
   f.full_load_batch_bytes_mb, f.full_load_commit_rows, f.full_load_commit_bytes_mb,
   f.full_load_lock_wait_timeout_sec, f.full_load_degrade_on_align_lock_fail] = p;
}

const fullLoadEffective = computed(() => {
  const f = taskForm.value;
  const orDefault = (v, d) => (v && v > 0 ? v : d);
  const legacyCommitRows = f.tx_commit_every_n_parallel > 0
    ? orDefault(f.batch_size, 1000) * f.tx_commit_every_n_parallel
    : 10000;
  return {
    read: orDefault(f.full_load_read_workers, 4),
    write: orDefault(f.full_load_write_workers, 4),
    buffer: orDefault(f.full_load_buffer_mb, 128),
    commitRows: orDefault(f.full_load_commit_rows, legacyCommitRows),
    commitBytes: orDefault(f.full_load_commit_bytes_mb, 32),
  };
});

const refreshingDatabases = ref(false);
const refreshingTables = ref(false);

async function refreshDatabases() {
  refreshingDatabases.value = true;
  try {
    await fetch(`${API_BASE}/metadata/refresh`, { method: "POST" });
    const res = await fetch(`${API_BASE}/metadata/databases`);
    if (res.ok) databases.value = await res.json();
  } catch (e) {
    Message.error("刷新数据库列表失败");
  } finally {
    refreshingDatabases.value = false;
  }
}

async function fetchDatabases() {
  try {
    let dbConfig = null;
    if (useCustomSourceDB.value) {
      if (customSourceDB.value.host) {
        dbConfig = { host: customSourceDB.value.host, port: customSourceDB.value.port, username: customSourceDB.value.username, password: customSourceDB.value.password, database: customSourceDB.value.database || "mysql" };
      }
    } else if (configForm.value.datasource?.host) {
      dbConfig = { host: configForm.value.datasource.host, port: configForm.value.datasource.port, username: configForm.value.datasource.username, password: configForm.value.datasource.password, database: configForm.value.datasource.database || "mysql" };
    }
    const defaultRes = await fetch(`${API_BASE}/metadata/databases`);
    if (defaultRes.ok) { databases.value = await defaultRes.json(); tableFetchGlobalError.value = null; return; }
    if (dbConfig?.host) {
      const res = await fetch(`${API_BASE}/metadata/databases-with-config`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(dbConfig) });
      if (res.ok) { databases.value = await res.json(); tableFetchGlobalError.value = null; return; }
      const errData = await res.json();
      Message.warning("获取数据库列表失败: " + errData.error);
    } else {
      Message.info("请先在系统配置中配置源数据库连接信息，或在高级配置中指定自定义数据库连接");
    }
  } catch (e) {
    Message.error("获取数据库列表失败: " + e.message);
  }
}

async function testConnection(dbConfig, type) {
  try {
    const res = await fetch(`${API_BASE}/config/test-connection`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(dbConfig) });
    const data = await res.json();
    if (data.success) { Message.success(`${type}连接成功: ${data.message}`); } else { Message.error(`${type}连接失败: ${data.message}`); }
    return data.success;
  } catch (e) { Message.error(`${type}连接测试失败: ${e.message}`); return false; }
}
function testSourceConnection() { return testConnection(customSourceDB.value, "源数据库"); }
function testTargetConnection() { return testConnection(customTargetDB.value, "目标数据库"); }

async function saveConfig() {
  try {
    const res = await fetch(`${API_BASE}/config/update`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(configForm.value) });
    if (res.ok) { Message.success("配置已更新"); await ensureDefaultConfig(); } else { const t = await res.text(); Message.error("更新配置失败: " + t); }
  } catch (e) { Message.error("更新配置失败: " + e.message); }
}

async function saveSourceConfig() {
  if (!customSourceDB.value.host) { Message.warning("请先填写源数据库配置"); return; }
  Object.assign(configForm.value.datasource, { host: customSourceDB.value.host, port: customSourceDB.value.port, database: customSourceDB.value.database, username: customSourceDB.value.username, password: customSourceDB.value.password });
  await saveConfig();
  tableFetchGlobalError.value = null;
  tableFetchErrors.value = {};
  fetchDatabases();
}

async function saveTargetConfig() {
  if (!customTargetDB.value.host) { Message.warning("请先填写目标数据库配置"); return; }
  Object.assign(configForm.value.target, { host: customTargetDB.value.host, port: customTargetDB.value.port, database: customTargetDB.value.database, username: customTargetDB.value.username, password: customTargetDB.value.password });
  await saveConfig();
}

function isTableFetchConfigError(message) {
  if (!message) return false;
  const lower = String(message).toLowerCase();
  return lower.includes("database not connected") || lower.includes("please create a task with database configuration") || lower.includes("configure datasource");
}

async function fetchTablesForDatabase(database, { silent = false } = {}) {
  if (!database) return;
  if (loadingTablesByDatabase.value[database]) return;
  const cachedError = tableFetchErrors.value[database];
  if (cachedError && isTableFetchConfigError(cachedError)) return;
  if (tableFetchGlobalError.value && isTableFetchConfigError(tableFetchGlobalError.value)) return;
  loadingTablesByDatabase.value = { ...loadingTablesByDatabase.value, [database]: true };
  tableFetchErrors.value = { ...tableFetchErrors.value, [database]: null };
  try {
    let dbConfig = null;
    if (useCustomSourceDB.value && customSourceDB.value.host) {
      dbConfig = { host: customSourceDB.value.host, port: customSourceDB.value.port, username: customSourceDB.value.username, password: customSourceDB.value.password, database: customSourceDB.value.database || database };
    } else if (configForm.value.datasource?.host) {
      dbConfig = { host: configForm.value.datasource.host, port: configForm.value.datasource.port, username: configForm.value.datasource.username, password: configForm.value.datasource.password, database: configForm.value.datasource.database || database };
    }
    let res;
    if (dbConfig?.host) {
      res = await fetch(`${API_BASE}/metadata/tables-with-config`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ...dbConfig, schema: database }) });
    } else {
      res = await fetch(`${API_BASE}/metadata/tables?schema=${database}`);
    }
    if (res.ok) {
      const tableList = await res.json();
      tablesByDatabase.value = { ...tablesByDatabase.value, [database]: tableList };
      if (activeTableSourceDatabase.value === database) tables.value = tableList;
      if (!silent) tableFetchGlobalError.value = null;
    } else {
      const errText = await res.text();
      let errMessage = errText;
      try { const errData = JSON.parse(errText); errMessage = errData.error || errText; } catch { /* keep raw text */ }
      const prettyMessage = `获取表列表失败: ${errMessage}`;
      if (isTableFetchConfigError(errMessage)) {
        tableFetchGlobalError.value = prettyMessage;
      } else {
        tableFetchErrors.value = { ...tableFetchErrors.value, [database]: prettyMessage };
        if (!silent) Message.error(prettyMessage);
      }
    }
  } catch (e) {
    const prettyMessage = `获取表列表失败: ${e.message}`;
    tableFetchErrors.value = { ...tableFetchErrors.value, [database]: prettyMessage };
    if (!silent) Message.error(prettyMessage);
  } finally {
    loadingTablesByDatabase.value = { ...loadingTablesByDatabase.value, [database]: false };
  }
}

async function refreshTables() {
  const databasesToRefresh = selectedSyncLevel.value === "table" && selectedDatabases.value.length > 0
    ? (expandedTableDatabaseKeys.value.length > 0 ? expandedTableDatabaseKeys.value : selectedDatabases.value)
    : [activeTableSourceDatabase.value || taskForm.value.source_schema].filter(Boolean);
  if (databasesToRefresh.length === 0) { Message.warning("请先选择源数据库"); return; }
  refreshingTables.value = true;
  try { await fetch(`${API_BASE}/metadata/refresh`, { method: "POST" }); await Promise.all(databasesToRefresh.map((db) => fetchTablesForDatabase(db))); }
  catch { Message.error("刷新表列表失败"); }
  finally { refreshingTables.value = false; }
}

function resetForm() {
  taskForm.value = {
    name: "", source_schema: "", target_schema: "", target_database: "",
    tables: [], target_tables: [], mode: "FULL",
    batch_size: 1000, worker_count: 4, intra_table_worker_count: 0,
    enable_limit_one: false, optimize_index: false, index_restore_worker_count: 0,
    enable_read_only: false, enable_drop_table_before_ddl: false,
    enable_skip_binlog: false, tx_commit_every_n_parallel: 0,
    full_load_engine: "v1", full_load_read_workers: 0, full_load_write_workers: 0,
    full_load_buffer_mb: 0, full_load_batch_bytes_mb: 0, full_load_commit_rows: 0,
    full_load_commit_bytes_mb: 0, full_load_lock_wait_timeout_sec: 0,
    full_load_degrade_on_align_lock_fail: false,
    allow_nopk_all: false,
  };
  selectedSyncLevel.value = "database";
  selectedDatabases.value = [];
  targetDatabaseMappings.value = [];
  targetTableMappings.value = {};
  selectedTables.value = [];
  activeTableSourceDatabase.value = "";
  tableSelectionsByDatabase.value = {};
  tablesByDatabase.value = {};
  loadingTablesByDatabase.value = {};
  tableFetchErrors.value = {};
  tableFetchGlobalError.value = null;
  expandedTableDatabaseKeys.value = [];
  expandedTargetTableDatabaseKeys.value = [];
  editingTaskId.value = null;
  cloneFromTaskId.value = null;
  useCustomSourceDB.value = false;
  useCustomTargetDB.value = false;
  customSourceDB.value = { host: "", port: 3306, database: "", username: "", password: "" };
  customTargetDB.value = { host: "", port: 3306, database: "", username: "", password: "" };
  targetType.value = "";
  sinkConfigs.value = [];
  singleKafkaConfig.value = getDefaultSinkOptions("KAFKA");
  singleWebhookConfig.value = getDefaultSinkOptions("HTTP_WEBHOOK");
}

function getQualifiedTableName(database, tableName) { return `${database}.${tableName}`; }
function buildTargetDatabasesPayload() { return buildDatabaseMappingsPayload(targetDatabaseMappings.value); }

function parseQualifiedTableName(qualifiedName) {
  const parts = String(qualifiedName || "").split(".");
  if (parts.length < 2) return { database: "", table: String(qualifiedName || "") };
  return { database: parts.shift(), table: parts.join(".") };
}

function ensureTableSelectionBucket(database) {
  if (!database) return;
  if (!Array.isArray(tableSelectionsByDatabase.value[database])) tableSelectionsByDatabase.value[database] = [];
}

function updateDatabaseTableSelection(db, newValue) {
  ensureTableSelectionBucket(db);
  tableSelectionsByDatabase.value[db] = [...newValue];
  selectedTables.value = selectedDatabases.value.flatMap((database) => {
    return (tableSelectionsByDatabase.value[database] || []).map((t) => getQualifiedTableName(database, t));
  });
}

function getFilteredTablesForDatabase(database) {
  const list = tablesByDatabase.value[database] || [];
  if (!tableSearchText.value) return list;
  const s = tableSearchText.value.toLowerCase();
  return list.filter((t) => t.table_name.toLowerCase().includes(s));
}

function onTableDatabaseAccordionChange(activeKeys) {
  const keys = Array.isArray(activeKeys) ? activeKeys : [activeKeys];
  expandedTableDatabaseKeys.value = keys;
  keys.forEach((db) => { if (!tablesByDatabase.value[db]) fetchTablesForDatabase(db); });
}

function toggleAllTablesForDatabase(database) {
  if (!database) return;
  const filtered = getFilteredTablesForDatabase(database);
  const current = tableSelectionsByDatabase.value[database] || [];
  if (current.length === filtered.length && filtered.length > 0) {
    updateDatabaseTableSelection(database, []);
  } else {
    updateDatabaseTableSelection(database, filtered.map((t) => t.table_name));
  }
}

function onTableSourceDatabasesChange(dbs) {
  selectedDatabases.value = dbs;
  if (dbs.length === 0) { activeTableSourceDatabase.value = ""; expandedTableDatabaseKeys.value = []; tables.value = []; tableSearchText.value = ""; return; }
  if (!dbs.includes(activeTableSourceDatabase.value)) activeTableSourceDatabase.value = dbs[0];
  taskForm.value.source_schema = activeTableSourceDatabase.value || "";
  const expanded = expandedTableDatabaseKeys.value.filter((db) => dbs.includes(db));
  if (expanded.length === 0) { expandedTableDatabaseKeys.value = [dbs[0]]; fetchTablesForDatabase(dbs[0]); }
  else { expandedTableDatabaseKeys.value = expanded; expanded.forEach((db) => { if (!tablesByDatabase.value[db]) fetchTablesForDatabase(db); }); }
}

function onSyncLevelChange() {
  selectedDatabases.value = []; targetDatabaseMappings.value = [];
  selectedTables.value = []; activeTableSourceDatabase.value = "";
  tableSelectionsByDatabase.value = {}; tablesByDatabase.value = {};
  loadingTablesByDatabase.value = {}; tableFetchErrors.value = {};
  tableFetchGlobalError.value = null;
  expandedTableDatabaseKeys.value = [];
  expandedTargetTableDatabaseKeys.value = []; tableSearchText.value = "";
  taskForm.value.source_schema = ""; taskForm.value.target_schema = "";
}

function toggleSelectAllDatabases(checked) {
  selectedDatabases.value = checked ? [...filteredDatabases.value] : [];
  onTableSourceDatabasesChange(selectedDatabases.value);
}

function clearSelectedDatabases() {
  selectedDatabases.value = [];
  onTableSourceDatabasesChange([]);
}

const filteredDatabases = computed(() => {
  if (!databaseSearchText.value) return databases.value;
  const s = databaseSearchText.value.toLowerCase();
  return databases.value.filter((db) => db.toLowerCase().includes(s));
});

const totalSelectedTables = computed(() =>
  selectedDatabases.value.reduce((sum, db) => sum + (tableSelectionsByDatabase.value[db] || []).length, 0),
);

const tableTargetMappingsByDatabase = computed(() =>
  selectedDatabases.value
    .map((db) => {
      const sourceTables = tableSelectionsByDatabase.value[db] || [];
      const mapping = targetDatabaseMappings.value.find((m) => m.source === db);
      return {
        database: db,
        targetDatabase: mapping?.target || db,
        tables: sourceTables.map((tableName) => {
          const q = getQualifiedTableName(db, tableName);
          return { source: q, tableName, target: targetTableMappings.value[q] || tableName };
        }),
      };
    })
    .filter((g) => g.tables.length > 0),
);

function fillTaskFormFromTask(task) {
  taskForm.value = {
    name: task.config.name, source_schema: task.config.source_schema,
    target_schema: task.config.target_schema, target_database: task.config.target_database || "",
    tables: task.config.tables || [], target_tables: task.config.target_tables || [],
    mode: task.config.mode, batch_size: task.config.batch_size, worker_count: task.config.worker_count,
    intra_table_worker_count: task.config.intra_table_worker_count ?? 0,
    enable_limit_one: task.config.enable_limit_one,
    optimize_index: task.config.optimize_index || false,
    index_restore_worker_count: task.config.index_restore_worker_count ?? 0,
    enable_read_only: task.config.enable_read_only || false,
    enable_drop_table_before_ddl: task.config.enable_drop_table_before_ddl || false,
    enable_skip_binlog: task.config.enable_skip_binlog || false,
    tx_commit_every_n_parallel: task.config.tx_commit_every_n_parallel ?? 0,
    full_load_engine: task.config.full_load_engine || "v1",
    full_load_read_workers: task.config.full_load_read_workers ?? 0,
    full_load_write_workers: task.config.full_load_write_workers ?? 0,
    full_load_buffer_mb: task.config.full_load_buffer_mb ?? 0,
    full_load_batch_bytes_mb: task.config.full_load_batch_bytes_mb ?? 0,
    full_load_commit_rows: task.config.full_load_commit_rows ?? 0,
    full_load_commit_bytes_mb: task.config.full_load_commit_bytes_mb ?? 0,
    full_load_lock_wait_timeout_sec: task.config.full_load_lock_wait_timeout_sec ?? 0,
    full_load_degrade_on_align_lock_fail: false,
    allow_nopk_all: !!(task.config.allow_nopk_all || task.context?.nopk_all_risk_acknowledged_at),
  };

  if (task.config.sync_level === "DATABASE") {
    selectedSyncLevel.value = "database";
    const srcDbs = task.config.source_databases || [];
    const dstDbs = task.config.target_databases || [];
    selectedDatabases.value = srcDbs;
    targetDatabaseMappings.value = srcDbs.map((db, i) => ({ source: db, target: dstDbs[i] || db }));
  } else {
    selectedSyncLevel.value = "table";
    const sourceDatabases = task.config.source_databases?.length ? task.config.source_databases : task.config.source_schema ? [task.config.source_schema] : [];
    selectedDatabases.value = sourceDatabases;
    targetDatabaseMappings.value = sourceDatabases.map((db, i) => ({
      source: db,
      target: (task.config.target_databases?.[i]) || (sourceDatabases.length === 1 ? task.config.target_schema : "") || db,
    }));
    tableSelectionsByDatabase.value = {};
    for (const db of sourceDatabases) ensureTableSelectionBucket(db);
    const rawTables = task.config.tables || [];
    const rawTargetTables = task.config.target_tables || [];
    selectedTables.value = [...rawTables];
    for (let i = 0; i < rawTables.length; i++) {
      const parsed = parseQualifiedTableName(rawTables[i]);
      const targetTable = rawTargetTables[i] || parsed.table;
      if (parsed.database && sourceDatabases.includes(parsed.database)) {
        ensureTableSelectionBucket(parsed.database);
        if (!tableSelectionsByDatabase.value[parsed.database].includes(parsed.table)) tableSelectionsByDatabase.value[parsed.database].push(parsed.table);
        targetTableMappings.value[rawTables[i]] = targetTable;
      } else if (sourceDatabases.length > 0 && parsed.table) {
        const fallbackDb = sourceDatabases[0];
        ensureTableSelectionBucket(fallbackDb);
        if (!tableSelectionsByDatabase.value[fallbackDb].includes(parsed.table)) tableSelectionsByDatabase.value[fallbackDb].push(parsed.table);
        targetTableMappings.value[getQualifiedTableName(fallbackDb, parsed.table)] = targetTable;
      }
    }
    activeTableSourceDatabase.value = sourceDatabases[0] || "";
    taskForm.value.source_schema = activeTableSourceDatabase.value || "";
    const dbsWithTables = sourceDatabases.filter((db) => (tableSelectionsByDatabase.value[db] || []).length > 0);
    expandedTableDatabaseKeys.value = dbsWithTables.length > 0 ? dbsWithTables : sourceDatabases.slice(0, 1);
    expandedTargetTableDatabaseKeys.value = dbsWithTables.length ? dbsWithTables : sourceDatabases.slice(0, 1);
    expandedTableDatabaseKeys.value.forEach((db) => fetchTablesForDatabase(db));
  }

  if (task.config.source_db) {
    useCustomSourceDB.value = true;
    customSourceDB.value = { host: task.config.source_db.host, port: task.config.source_db.port, database: task.config.source_db.database, username: task.config.source_db.username, password: unmaskSecret(task.config.source_db.password) };
  }
  if (task.config.target_db) {
    useCustomTargetDB.value = true;
    customTargetDB.value = { host: task.config.target_db.host, port: task.config.target_db.port, database: task.config.target_db.database, username: task.config.target_db.username, password: unmaskSecret(task.config.target_db.password) };
  }

  if (hasExplicitSinkConfigs(task.config.sink_configs)) {
    if (isSingleExplicitMySQLSink(task.config.sink_configs)) {
      targetType.value = "MYSQL";
      sinkConfigs.value = [];
      if (!task.config.target_db) {
        const opts = task.config.sink_configs[0].options || {};
        useCustomTargetDB.value = true;
        customTargetDB.value = { host: opts.host || "", port: opts.port || 3306, database: opts.database || "", username: opts.username || "", password: unmaskSecret(opts.password) };
      }
    } else if (task.config.sink_configs.length === 1 && task.config.sink_configs[0].type !== "MYSQL") {
      const sc = task.config.sink_configs[0];
      targetType.value = sc.type;
      const opts = normalizeSinkOptionsForForm(sc.type, sc.options);
      if (sc.type === "KAFKA") singleKafkaConfig.value = opts;
      else if (sc.type === "HTTP_WEBHOOK") singleWebhookConfig.value = opts;
      sinkConfigs.value = [];
    } else {
      targetType.value = "MULTI";
      sinkConfigs.value = task.config.sink_configs.map((sc) => ({ type: sc.type, options: normalizeSinkOptionsForForm(sc.type, sc.options) }));
    }
  } else {
    targetType.value = "MYSQL";
    sinkConfigs.value = [];
  }
}

async function createTask() {
  if (!taskForm.value.name) { Message.warning("请输入任务名称"); return; }
  if (selectedSyncLevel.value === "database") {
    if (selectedDatabases.value.length === 0) { Message.warning("请至少选择一个源数据库"); return; }
  } else {
    if (selectedDatabases.value.length === 0) { Message.warning("请至少选择一个源数据库"); return; }
    if (totalSelectedTables.value === 0) { Message.warning("请至少选择一个表"); return; }
    for (const db of selectedDatabases.value) {
      const mapping = targetDatabaseMappings.value.find((m) => m.source === db);
      if (!String(mapping?.target || taskForm.value.target_database || "").trim()) { Message.warning(`请为源库 ${db} 配置目标库`); return; }
    }
  }

  let tablesPayload = [], targetTablesPayload = [], sourceDatabasesPayload = [], targetDatabasesPayload = [];
  let sourceSchemaPayload = taskForm.value.source_schema, targetSchemaPayload = taskForm.value.target_schema, targetDatabasePayload = taskForm.value.target_database;

  if (selectedSyncLevel.value === "database") {
    sourceDatabasesPayload = selectedDatabases.value;
    targetDatabasesPayload = buildTargetDatabasesPayload();
    sourceSchemaPayload = ""; targetSchemaPayload = "";
    targetDatabasePayload = taskForm.value.target_database || "";
  } else {
    sourceDatabasesPayload = selectedDatabases.value;
    targetDatabasesPayload = buildTargetDatabasesPayload();
    sourceSchemaPayload = selectedDatabases.value[0] || "";
    targetSchemaPayload = selectedDatabases.value.length === 1 ? targetDatabasesPayload[0] || "" : "";
    targetDatabasePayload = "";
    tablesPayload = selectedDatabases.value.flatMap((db) => (tableSelectionsByDatabase.value[db] || []).map((t) => getQualifiedTableName(db, t)));
    targetTablesPayload = selectedDatabases.value.flatMap((db) => (tableSelectionsByDatabase.value[db] || []).map((t) => { const q = getQualifiedTableName(db, t); return targetTableMappings.value[q] || t; }));
    selectedTables.value = [...tablesPayload];
  }

  let sinkConfigsPayload = null;
  if (targetType.value === "KAFKA") {
    const opts = { ...singleKafkaConfig.value };
    if (typeof opts.brokers === "string") opts.brokers = opts.brokers.split(",").map((s) => s.trim()).filter(Boolean);
    sinkConfigsPayload = [{ type: "KAFKA", options: opts }];
  } else if (targetType.value === "WEBHOOK") {
    const opts = { ...singleWebhookConfig.value };
    if (typeof opts.headers === "string") {
      const hm = {}; opts.headers.split("\n").forEach((line) => { const idx = line.indexOf(":"); if (idx > 0) hm[line.slice(0, idx).trim()] = line.slice(idx + 1).trim(); });
      opts.headers = Object.keys(hm).length > 0 ? hm : undefined;
    }
    sinkConfigsPayload = [{ type: "HTTP_WEBHOOK", options: opts }];
  } else if (targetType.value === "MULTI" && sinkConfigs.value.length > 0) {
    sinkConfigsPayload = sinkConfigs.value.map((sc) => {
      const opts = { ...sc.options };
      if (sc.type === "KAFKA" && typeof opts.brokers === "string") opts.brokers = opts.brokers.split(",").map((s) => s.trim()).filter(Boolean);
      if (sc.type === "HTTP_WEBHOOK" && typeof opts.headers === "string") {
        const hm = {}; opts.headers.split("\n").forEach((line) => { const idx = line.indexOf(":"); if (idx > 0) hm[line.slice(0, idx).trim()] = line.slice(idx + 1).trim(); });
        opts.headers = Object.keys(hm).length > 0 ? hm : undefined;
      }
      return { type: sc.type, options: opts };
    });
  }

  const payload = {
    ...taskForm.value,
    source_schema: sourceSchemaPayload, target_schema: targetSchemaPayload,
    sync_level: selectedSyncLevel.value, tables: tablesPayload,
    source_databases: sourceDatabasesPayload, target_databases: targetDatabasesPayload,
    target_database: targetDatabasePayload, target_tables: targetTablesPayload,
    source_db: useCustomSourceDB.value ? customSourceDB.value : null,
    target_db: useCustomTargetDB.value ? customTargetDB.value : null,
    sink_configs: sinkConfigsPayload,
    clone_from_task_id: !editMode.value && cloneFromTaskId.value ? cloneFromTaskId.value : undefined,
  };

  loading.value = true;
  try {
    const url = editMode.value ? `${API_BASE}/tasks/${editingTaskId.value}` : `${API_BASE}/tasks`;
    const method = editMode.value ? "PUT" : "POST";
    const res = await fetch(url, { method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
    if (res.ok) {
      Message.success(editMode.value ? "更新成功" : "创建成功");
      router.push("/tasks");
    } else {
      const text = await res.text();
      try { const err = JSON.parse(text); Message.error((editMode.value ? "更新" : "创建") + "失败: " + (err.error || text)); }
      catch { Message.error((editMode.value ? "更新" : "创建") + "失败: " + (text || "服务器返回空响应")); }
    }
  } catch (e) {
    Message.error((editMode.value ? "更新" : "创建") + "失败: " + e.message);
  } finally {
    loading.value = false;
  }
}

watch(useCustomSourceDB, () => {
  tableFetchGlobalError.value = null;
  tableFetchErrors.value = {};
  fetchDatabases();
});

watch(selectedDatabases, (newDbs) => {
  if (selectedSyncLevel.value === "table") {
    if (newDbs.length === 0) { activeTableSourceDatabase.value = ""; expandedTableDatabaseKeys.value = []; tables.value = []; }
    else if (!newDbs.includes(activeTableSourceDatabase.value)) {
      activeTableSourceDatabase.value = newDbs[0];
      if (expandedTableDatabaseKeys.value.length === 0) expandedTableDatabaseKeys.value = [newDbs[0]];
      fetchTablesForDatabase(newDbs[0]);
    }
    newDbs.forEach((db) => { if (expandedTableDatabaseKeys.value.includes(db) && !tablesByDatabase.value[db]) fetchTablesForDatabase(db); });
    const nextSelections = {};
    newDbs.forEach((db) => { nextSelections[db] = [...(tableSelectionsByDatabase.value[db] || [])]; });
    tableSelectionsByDatabase.value = nextSelections;
    selectedTables.value = newDbs.flatMap((db) => (tableSelectionsByDatabase.value[db] || []).map((t) => getQualifiedTableName(db, t)));
    taskForm.value.source_schema = activeTableSourceDatabase.value || "";
  }
  targetDatabaseMappings.value = newDbs.map((db) => {
    const existing = targetDatabaseMappings.value.find((m) => m.source === db);
    return existing || { source: db, target: db };
  });
}, { deep: true });

watch(tableTargetMappingsByDatabase, (groups) => {
  const dbKeys = groups.map((g) => g.database);
  if (dbKeys.length === 0) return;
  expandedTargetTableDatabaseKeys.value = [...new Set([...expandedTargetTableDatabaseKeys.value, ...dbKeys])];
}, { deep: true });

onMounted(async () => {
  await ensureDefaultConfig();
  fetchDatabases();

  if (editMode.value) {
    const taskId = route.params.id;
    editingTaskId.value = taskId;
    try {
      const res = await fetch(`${API_BASE}/tasks/${taskId}`);
      if (res.ok) { const task = await res.json(); fillTaskFormFromTask(task); }
      else { Message.error("加载任务失败"); router.push("/tasks"); return; }
    } catch (e) { Message.error("加载任务失败: " + e.message); router.push("/tasks"); return; }
  } else {
    const queryType = route.query.type;
    if (queryType) targetType.value = queryType;
    const cloneFrom = route.query.clone_from;
    if (cloneFrom) {
      cloneFromTaskId.value = cloneFrom;
      try {
        const res = await fetch(`${API_BASE}/tasks/${cloneFrom}`);
        if (res.ok) {
          const task = await res.json();
          fillTaskFormFromTask(task);
          const base = (task.config.name || "同步任务").trim();
          const suffix = "（副本）";
          taskForm.value.name = base.endsWith(suffix) ? `${base}_${Date.now()}` : `${base}${suffix}`;
          Message.success("已载入该任务配置，请检查后点击「创建」");
        }
      } catch { /* ignore */ }
    }
  }

  setFormHeaderActions({
    loading,
    editMode,
    submit: createTask,
    cancel: () => {
      clearFormHeaderActions();
      router.push("/tasks");
    },
  });
});

onUnmounted(() => {
  clearFormHeaderActions();
});
</script>

<template>
        <div
          class="task-form-full-page"
        >
          <a-form :model="taskForm" layout="vertical" class="task-create-form">
            <a-row :gutter="32" class="task-base-config-row">
              <a-col :span="24">
                <a-form-item label="任务名称" required>
                  <a-input
                    v-model="taskForm.name"
                    placeholder="请输入任务名称"
                  />
                </a-form-item>

                <a-form-item label="目标端类型" v-if="editMode">
                  <a-radio-group v-model="targetType" disabled>
                    <a-radio value="MYSQL">MySQL 数据库</a-radio>
                    <a-radio value="KAFKA">Kafka</a-radio>
                    <a-radio value="WEBHOOK">HTTP Webhook</a-radio>
                    <a-radio value="MULTI">高级: 多目标 (Multi-Sink)</a-radio>
                  </a-radio-group>
                </a-form-item>

                <a-form-item label="目标端类型" v-else>
                  <div style="display: flex; align-items: center; gap: 12px">
                    <a-tag color="blue" size="large">
                      <template #icon>
                        <icon-storage v-if="targetType === 'MYSQL'" />
                        <icon-send v-else-if="targetType === 'KAFKA'" />
                        <icon-link v-else-if="targetType === 'WEBHOOK'" />
                        <icon-branch v-else />
                      </template>
                      {{ getSinkTypeLabel(targetType) }}
                    </a-tag>
                    <a-button
                      type="text"
                      size="small"
                      @click="clearFormHeaderActions(); router.push('/tasks/new/select')"
                      >重新选择</a-button
                    >
                  </div>
                </a-form-item>

                <a-form-item label="同步级别">
                  <a-radio-group
                    v-model="selectedSyncLevel"
                    @change="onSyncLevelChange"
                  >
                    <a-radio value="database">
                      <a-space><icon-storage />库级别同步（全库）</a-space>
                    </a-radio>

                    <a-radio value="table">
                      <a-space><icon-file />表级别同步（指定表）</a-space>
                    </a-radio>
                  </a-radio-group>
                </a-form-item>

                <!-- 库级别：双栏选择器 (仿阿里云 DTS/DMS 布局) -->

                <div
                  v-if="selectedSyncLevel === 'database'"
                  class="db-transfer-container"
                >
                  <!-- 左侧：可选库 -->

                  <div class="transfer-pane">
                    <div class="transfer-header">
                      <span class="title">源数据库</span>

                      <a-button
                        type="text"
                        size="mini"
                        :loading="refreshingDatabases"
                        @click="refreshDatabases"
                      >
                        <template #icon><icon-refresh /></template>
                      </a-button>
                    </div>

                    <div class="transfer-search">
                      <a-input-search
                        v-model="databaseSearchText"
                        placeholder="搜索库名..."
                        size="small"
                        allow-clear
                      />
                    </div>

                    <div class="transfer-content">
                      <div class="transfer-list-header">
                        <a-checkbox
                          :model-value="
                            selectedDatabases.length ===
                              filteredDatabases.length &&
                            filteredDatabases.length > 0
                          "
                          :indeterminate="
                            selectedDatabases.length > 0 &&
                            selectedDatabases.length < filteredDatabases.length
                          "
                          @change="toggleSelectAllDatabases"
                        >
                          全选
                        </a-checkbox>

                        <span class="count"
                          >{{ filteredDatabases.length }} 个库</span
                        >
                      </div>

                      <div class="transfer-list">
                        <a-checkbox-group
                          v-model="selectedDatabases"
                          direction="vertical"
                          style="width: 100%"
                        >
                          <div
                            v-for="db in filteredDatabases"
                            :key="db"
                            class="transfer-list-item"
                          >
                            <a-checkbox :value="db">{{ db }}</a-checkbox>
                          </div>
                        </a-checkbox-group>

                        <a-empty
                          v-if="filteredDatabases.length === 0"
                          description="暂无数据"
                        />
                      </div>
                    </div>
                  </div>

                  <!-- 中间：箭头 (可选) -->

                  <div class="transfer-arrow">
                    <icon-arrow-right size="20" />
                  </div>

                  <!-- 右侧：已选库及映射 -->

                  <div class="transfer-pane">
                    <div class="transfer-header">
                      <span class="title"
                        >已选库 ({{ selectedDatabases.length }})</span
                      >

                      <a-button
                        type="text"
                        size="mini"
                        status="danger"
                        @click="clearSelectedDatabases"
                        :disabled="selectedDatabases.length === 0"
                      >
                        清空
                      </a-button>
                    </div>

                    <div class="transfer-header-tip">
                      <span>源库名</span>

                      <span>目标库名 (可修改)</span>
                    </div>

                    <div class="transfer-content bg-white">
                      <div class="transfer-list">
                        <div
                          v-for="(mapping, idx) in targetDatabaseMappings"
                          :key="mapping.source"
                          class="mapped-item"
                        >
                          <div class="source-name" :title="mapping.source">
                            <icon-storage
                              style="margin-right: 4px; color: #165dff"
                            />

                            {{ mapping.source }}
                          </div>

                          <div class="target-input">
                            <a-input
                              v-model="mapping.target"
                              size="small"
                              placeholder="目标库名"
                              :style="{
                                width: '100%',
                                borderColor: mapping.target ? '' : '#ff7d00',
                              }"
                            />
                          </div>

                          <a-button
                            type="text"
                            size="mini"
                            status="danger"
                            class="remove-btn"
                            @click="
                              selectedDatabases = selectedDatabases.filter(
                                (d) => d !== mapping.source,
                              )
                            "
                          >
                            <icon-close />
                          </a-button>
                        </div>

                        <a-empty
                          v-if="targetDatabaseMappings.length === 0"
                          description="请从左侧选择数据库"
                        />
                      </div>
                    </div>
                  </div>
                </div>

                <!-- 表级别：源库与目标库映射 -->

                <div
                  v-if="selectedSyncLevel === 'table'"
                  class="db-transfer-container table-source-transfer-container"
                >
                  <div class="transfer-pane">
                    <div class="transfer-header">
                      <span class="title">源数据库</span>

                      <a-button
                        type="text"
                        size="mini"
                        :loading="refreshingDatabases"
                        @click="refreshDatabases"
                      >
                        <template #icon><icon-refresh /></template>
                      </a-button>
                    </div>

                    <div class="transfer-search">
                      <a-input-search
                        v-model="databaseSearchText"
                        placeholder="搜索库名..."
                        size="small"
                        allow-clear
                      />
                    </div>

                    <div class="transfer-content">
                      <div class="transfer-list-header">
                        <a-checkbox
                          :model-value="
                            selectedDatabases.length ===
                              filteredDatabases.length &&
                            filteredDatabases.length > 0
                          "
                          :indeterminate="
                            selectedDatabases.length > 0 &&
                            selectedDatabases.length < filteredDatabases.length
                          "
                          @change="toggleSelectAllDatabases"
                        >
                          全选
                        </a-checkbox>

                        <span class="count"
                          >{{ filteredDatabases.length }} 个库</span
                        >
                      </div>

                      <div class="transfer-list">
                        <a-checkbox-group
                          v-model="selectedDatabases"
                          direction="vertical"
                          style="width: 100%"
                          @change="onTableSourceDatabasesChange"
                        >
                          <div
                            v-for="db in filteredDatabases"
                            :key="db"
                            class="transfer-list-item"
                          >
                            <a-checkbox :value="db">{{ db }}</a-checkbox>
                          </div>
                        </a-checkbox-group>

                        <a-empty
                          v-if="filteredDatabases.length === 0"
                          description="暂无数据"
                        />
                      </div>
                    </div>
                  </div>

                  <div class="transfer-arrow">
                    <icon-arrow-right size="20" />
                  </div>

                  <div class="transfer-pane">
                    <div class="transfer-header">
                      <span class="title">
                        源库到目标库映射（每个源库可指定不同的目标库）
                      </span>

                      <a-button
                        type="text"
                        size="mini"
                        status="danger"
                        @click="clearSelectedDatabases"
                        :disabled="selectedDatabases.length === 0"
                      >
                        清空
                      </a-button>
                    </div>

                    <div class="transfer-header-tip">
                      <span>源库名</span>

                      <span>目标库名 (可修改)</span>
                    </div>

                    <div class="transfer-content bg-white">
                      <div class="transfer-list">
                        <div
                          v-for="mapping in targetDatabaseMappings"
                          :key="mapping.source"
                          class="mapped-item"
                        >
                          <div class="source-name" :title="mapping.source">
                            <icon-storage
                              style="margin-right: 4px; color: var(--app-accent)"
                            />

                            {{ mapping.source }}
                          </div>

                          <div class="target-input">
                            <a-input
                              v-model="mapping.target"
                              size="small"
                              placeholder="目标库名"
                              :style="{
                                width: '100%',
                                borderColor: mapping.target ? '' : '#ff7d00',
                              }"
                            />
                          </div>

                          <a-button
                            type="text"
                            size="mini"
                            status="danger"
                            class="remove-btn"
                            @click="
                              selectedDatabases = selectedDatabases.filter(
                                (d) => d !== mapping.source,
                              )
                            "
                          >
                            <icon-close />
                          </a-button>
                        </div>

                        <a-empty
                          v-if="targetDatabaseMappings.length === 0"
                          description="请从左侧选择数据库"
                        />
                      </div>
                    </div>
                  </div>
                </div>

                <div
                  v-if="selectedSyncLevel === 'database'"
                  class="table-mapping-panel"
                >
                  <div class="table-mapping-title">
                    库级别同步目标库配置
                  </div>

                  <a-form-item label="默认目标库名">
                    <a-input
                      v-model="taskForm.target_database"
                      placeholder="请输入默认目标库名"
                    />
                  </a-form-item>

                  <div class="table-mapping-list">
                    <div
                      v-for="mapping in targetDatabaseMappings"
                      :key="mapping.source"
                      class="table-mapping-item"
                    >
                      <span class="table-mapping-source">{{
                        mapping.source
                      }}</span>

                      <icon-arrow-right style="color: #86909c" />

                      <a-input
                        v-model="mapping.target"
                        placeholder="目标库名"
                        style="width: 220px"
                      />
                    </div>

                    <a-empty
                      v-if="targetDatabaseMappings.length === 0"
                      description="请先选择源数据库"
                    />
                  </div>
                </div>

                <!-- 表级别：同步模式 -->

                <a-row :gutter="16">
                  <a-col
                    v-if="selectedSyncLevel === 'table'"
                    :span="24"
                  >
                    <a-form-item label="同步模式">
                      <a-select v-model="taskForm.mode">
                        <a-option value="FULL">全量同步</a-option>

                        <a-option value="INCREMENTAL">增量同步</a-option>

                        <a-option value="ALL">全量+增量</a-option>
                      </a-select>
                      <a-alert
                        v-if="taskForm.mode === 'FULL'"
                        type="warning"
                        style="margin-top: 8px"
                        show-icon
                      >
                        FULL 是一次性基线拷贝，不捕获执行期间的增量；同表不同分片可能读到不同时间点。
                        在线不停写迁移请选 ALL；需要严格静态副本请在 FULL 期间暂停源端写入。
                      </a-alert>
                      <div v-if="taskForm.mode === 'ALL'" style="margin-top: 8px">
                        <a-alert type="warning" show-icon>
                          ALL 会先做基线扫描再从 binlog 追平。若包含无主键/唯一键表，只能提供
                          best-effort 一致性（可能重复 INSERT，UPDATE/DELETE 依赖 before image）。
                        </a-alert>
                        <a-checkbox
                          v-model="taskForm.allow_nopk_all"
                          style="margin-top: 8px"
                        >
                          我已理解无主键/唯一键表无法保证严格一致性，仍继续 ALL
                        </a-checkbox>
                      </div>
                    </a-form-item>
                  </a-col>
                </a-row>

              </a-col>
            </a-row>

            <a-row
              v-if="selectedSyncLevel === 'table'"
              :gutter="24"
              class="table-config-row"
            >
              <a-col :span="12">
                <div class="table-mapping-panel table-target-mapping-panel">
                  <div class="table-mapping-title">表级别同步目标表配置</div>

                  <a-empty
                    v-if="tableTargetMappingsByDatabase.length === 0"
                    description="请先选择源库并勾选要同步的表"
                  />

                  <a-collapse
                    v-else
                    v-model:active-key="expandedTargetTableDatabaseKeys"
                    expand-icon-position="right"
                    class="table-db-collapse"
                  >
                    <a-collapse-item
                      v-for="group in tableTargetMappingsByDatabase"
                      :key="group.database"
                      :header="`${group.database} → ${group.targetDatabase}（${group.tables.length} 表）`"
                    >
                      <div class="table-mapping-list">
                        <div
                          v-for="item in group.tables"
                          :key="item.source"
                          class="table-mapping-item"
                        >
                          <span class="table-mapping-source">{{ item.source }}</span>

                          <icon-arrow-right style="color: #86909c" />

                          <a-input
                            v-model="targetTableMappings[item.source]"
                            placeholder="目标表名"
                            style="width: 220px"
                          />
                        </div>
                      </div>
                    </a-collapse-item>
                  </a-collapse>
                </div>
              </a-col>

              <a-col :span="12">
                <!-- 表级别：选择表 -->

                <div
                  v-if="selectedSyncLevel === 'table'"
                  class="table-selector-panel"
                >
                  <div class="table-mapping-title">选择要同步的表</div>

                  <a-alert
                    v-if="selectedDatabases.length === 0"
                    type="info"
                    style="margin-bottom: 8px"
                    show-icon
                  >
                    请先选择至少一个源数据库，再展开库名勾选表
                  </a-alert>

                  <a-alert
                    v-if="tableFetchGlobalError"
                    type="error"
                    style="margin-bottom: 8px"
                    show-icon
                    closable
                    @close="tableFetchGlobalError = null"
                  >
                    {{ tableFetchGlobalError }}
                  </a-alert>

                  <div class="table-toolbar">
                      <a-input
                        v-model="tableSearchText"
                        placeholder="搜索表名..."
                        allow-clear
                        class="table-search-input"
                        :disabled="selectedDatabases.length === 0"
                      >
                        <template #suffix><icon-search /></template>
                      </a-input>

                      <a-button
                        type="text"
                        size="small"
                        :disabled="selectedDatabases.length === 0"
                        :loading="refreshingTables"
                        @click="refreshTables"
                      >
                        <template #icon><icon-refresh /></template>
                      </a-button>
                    </div>

                    <div class="table-list-panel">
                      <a-collapse
                        v-if="selectedDatabases.length > 0"
                        :active-key="expandedTableDatabaseKeys"
                        expand-icon-position="right"
                        class="table-db-collapse"
                        @change="onTableDatabaseAccordionChange"
                      >
                        <a-collapse-item
                          v-for="db in selectedDatabases"
                          :key="db"
                        >
                          <template #header>
                            <a-space>
                              <icon-storage style="color: #165dff" />
                              <span>{{ db }}</span>
                              <a-tag size="small" color="arcoblue">
                                已选
                                {{
                                  (tableSelectionsByDatabase[db] || []).length
                                }}
                                表
                              </a-tag>
                            </a-space>
                          </template>

                          <div class="table-db-panel-toolbar">
                            <a-button
                              type="text"
                              size="small"
                              :loading="loadingTablesByDatabase[db]"
                              @click="fetchTablesForDatabase(db)"
                            >
                              <template #icon><icon-refresh /></template>
                              刷新
                            </a-button>

                            <a-button
                              type="text"
                              size="small"
                              @click="toggleAllTablesForDatabase(db)"
                            >
                              {{
                                (tableSelectionsByDatabase[db] || []).length ===
                                  getFilteredTablesForDatabase(db).length &&
                                getFilteredTablesForDatabase(db).length > 0
                                  ? "取消全选"
                                  : "全选"
                              }}
                            </a-button>
                          </div>

                          <a-spin
                            :loading="loadingTablesByDatabase[db]"
                            style="width: 100%"
                          >
                            <a-alert
                              v-if="tableFetchErrors[db]"
                              type="error"
                              style="margin-bottom: 8px"
                              show-icon
                              size="small"
                            >
                              {{ tableFetchErrors[db] }}
                            </a-alert>

                            <a-checkbox-group
                              v-if="getFilteredTablesForDatabase(db).length > 0"
                              :model-value="tableSelectionsByDatabase[db] || []"
                              class="table-checkbox-group"
                              @update:model-value="
                                (val) => updateDatabaseTableSelection(db, val)
                              "
                            >
                              <div class="table-list-grid">
                                <div
                                  class="table-list-item"
                                  v-for="table in getFilteredTablesForDatabase(db)"
                                  :key="`${db}.${table.table_name}`"
                                >
                                  <a-checkbox :value="table.table_name">
                                    <span class="table-name-text" :title="table.table_name">
                                      {{ table.table_name }}
                                    </span>

                                    <a-tag size="small" color="gray"
                                      >{{ table.table_row_count }} 行</a-tag
                                    >
                                  </a-checkbox>
                                </div>
                              </div>
                            </a-checkbox-group>

                            <a-empty
                              v-else-if="!tableFetchErrors[db]"
                              :description="
                                loadingTablesByDatabase[db]
                                  ? '加载中...'
                                  : '暂无匹配的表'
                              "
                              :style="{ padding: '12px 0' }"
                            />
                          </a-spin>
                        </a-collapse-item>
                      </a-collapse>

                      <a-empty
                        v-else
                        description="请先选择源数据库"
                        :style="{ padding: '20px 0' }"
                      />
                    </div>

                    <div class="table-selector-footer">
                      <a-typography-text type="secondary">
                        总计已选 {{ totalSelectedTables }} 个表
                      </a-typography-text>
                    </div>
                </div>
              </a-col>
            </a-row>

            <a-row :gutter="32" class="advanced-config-row">
              <a-col :span="24">
                <a-card class="advanced-config-card" :bordered="false">
                  <a-typography-title :heading="6" style="margin-bottom: 12px"
                    >高级配置</a-typography-title
                  >

                  <a-row :gutter="16">
                    <a-col :span="12">
                      <a-form-item label="批量大小">
                        <a-input-number
                          :model-value="taskForm.batch_size"
                          @change="(v) => (taskForm.batch_size = v)"
                          :min="1"
                          style="width: 100%"
                        />
                      </a-form-item>
                    </a-col>

                    <a-col :span="12">
                      <a-form-item label="表并发数">
                        <a-input-number
                          :model-value="taskForm.worker_count"
                          @change="(v) => (taskForm.worker_count = v)"
                          :min="1"
                          style="width: 100%"
                        />
                      </a-form-item>
                    </a-col>
                  </a-row>

                  <a-row :gutter="16">
                    <a-col :span="12">
                      <a-form-item label="单表内并发">
                        <a-input-number
                          :model-value="taskForm.intra_table_worker_count"
                          @change="
                            (v) => (taskForm.intra_table_worker_count = v ?? 0)
                          "
                          :min="0"
                          :max="1024"
                          style="width: 100%"
                        />

                        <a-typography-text
                          type="secondary"
                          style="
                            font-size: 12px;
                            display: block;
                            margin-top: 4px;
                          "
                        >
                          0 为默认（与表并发相同，单表封顶见服务端
                          intra_table_legacy_cap）；有主键且可并行时按此值拆范围读。实际并发还受连接池与
                          MySQL max_connections 限制，上限见 application.toml
                          [sync].intra_table_hard_max。
                        </a-typography-text>
                      </a-form-item>
                    </a-col>

                    <a-col :span="12">
                      <a-form-item label="并行事务提交间隔">
                        <a-input-number
                          :model-value="taskForm.tx_commit_every_n_parallel"
                          @change="
                            (v) => (taskForm.tx_commit_every_n_parallel = v ?? 0)
                          "
                          :min="0"
                          :max="1000"
                          style="width: 100%"
                        />

                        <a-typography-text
                          type="secondary"
                          style="
                            font-size: 12px;
                            display: block;
                            margin-top: 4px;
                          "
                        >
                          并行 worker 每 N 批提交一次事务；0 为默认值 5。减小可降低锁等待避免 lock wait
                          timeout，增大可减少 fsync 频率提高大表吞吐。
                        </a-typography-text>
                      </a-form-item>
                    </a-col>
                  </a-row>

                  <a-form-item v-if="targetType === 'MYSQL'" label="全量引擎">
                    <a-radio-group
                      v-model="taskForm.full_load_engine"
                      type="button"
                    >
                      <a-radio value="v1">V1（兼容，逐表调度）</a-radio>
                      <a-radio value="v2">V2（任务级流水线）</a-radio>
                    </a-radio-group>
                    <a-typography-text
                      type="secondary"
                      style="font-size: 12px; display: block; margin-top: 4px"
                    >
                      V2 使用任务级 chunk 调度与读写解耦流水线，统一控制源读取/目标写入并发，
                      并修复并行事务重试的正确性问题。默认 V1，建议先灰度验证后切换。
                    </a-typography-text>
                  </a-form-item>

                  <template
                    v-if="targetType === 'MYSQL' && taskForm.full_load_engine === 'v2'"
                  >
                    <a-form-item label="V2 预设">
                      <a-space wrap>
                        <a-button size="small" @click="applyFullLoadPreset('balanced')"
                          >4C8G 平衡</a-button
                        >
                        <a-button size="small" @click="applyFullLoadPreset('speed')"
                          >速度优先</a-button
                        >
                        <a-button size="small" @click="applyFullLoadPreset('low')"
                          >低目标负载</a-button
                        >
                        <a-button size="small" @click="applyFullLoadPreset('auto')"
                          >全部自动</a-button
                        >
                      </a-space>
                      <a-typography-text
                        type="secondary"
                        style="font-size: 12px; display: block; margin-top: 4px"
                      >
                        当前生效：读 {{ fullLoadEffective.read }} / 写
                        {{ fullLoadEffective.write }} worker，队列
                        {{ fullLoadEffective.buffer }} MiB，单事务
                        {{ fullLoadEffective.commitRows }} 行 /
                        {{ fullLoadEffective.commitBytes }} MiB。删除目标或跳过 binlog
                        仍需在下方单独确认，预设不会静默开启。
                      </a-typography-text>
                    </a-form-item>

                    <a-row :gutter="16">
                      <a-col :span="12">
                        <a-form-item label="源读取并发 (read workers)">
                          <a-input-number
                            :model-value="taskForm.full_load_read_workers"
                            @change="(v) => (taskForm.full_load_read_workers = v ?? 0)"
                            :min="0"
                            :max="64"
                            style="width: 100%"
                            placeholder="0=自动(4)"
                          />
                          <a-typography-text
                            type="secondary"
                            style="font-size: 12px"
                          >
                            1=显式单线程；&gt;1=并行读取。运行中失败不会自动降为单线程
                          </a-typography-text>
                        </a-form-item>
                      </a-col>
                      <a-col :span="12">
                        <a-form-item label="目标写入并发 (write workers)">
                          <a-input-number
                            :model-value="taskForm.full_load_write_workers"
                            @change="(v) => (taskForm.full_load_write_workers = v ?? 0)"
                            :min="0"
                            :max="64"
                            style="width: 100%"
                            placeholder="0=自动(4)"
                          />
                        </a-form-item>
                      </a-col>
                    </a-row>

                    <a-row :gutter="16">
                      <a-col :span="12">
                        <a-form-item label="数据队列上限 (MiB)">
                          <a-input-number
                            :model-value="taskForm.full_load_buffer_mb"
                            @change="(v) => (taskForm.full_load_buffer_mb = v ?? 0)"
                            :min="0"
                            :max="4096"
                            style="width: 100%"
                            placeholder="0=自动(128)"
                          />
                        </a-form-item>
                      </a-col>
                      <a-col :span="12">
                        <a-form-item label="单条 INSERT 字节上限 (MiB)">
                          <a-input-number
                            :model-value="taskForm.full_load_batch_bytes_mb"
                            @change="(v) => (taskForm.full_load_batch_bytes_mb = v ?? 0)"
                            :min="0"
                            :max="64"
                            style="width: 100%"
                            placeholder="0=自动(4)"
                          />
                        </a-form-item>
                      </a-col>
                    </a-row>

                    <a-row :gutter="16">
                      <a-col :span="12">
                        <a-form-item label="单事务行数上限">
                          <a-input-number
                            :model-value="taskForm.full_load_commit_rows"
                            @change="(v) => (taskForm.full_load_commit_rows = v ?? 0)"
                            :min="0"
                            :max="10000000"
                            style="width: 100%"
                            placeholder="0=自动(10000)"
                          />
                        </a-form-item>
                      </a-col>
                      <a-col :span="12">
                        <a-form-item label="单事务字节上限 (MiB)">
                          <a-input-number
                            :model-value="taskForm.full_load_commit_bytes_mb"
                            @change="(v) => (taskForm.full_load_commit_bytes_mb = v ?? 0)"
                            :min="0"
                            :max="4096"
                            style="width: 100%"
                            placeholder="0=自动(32)"
                          />
                        </a-form-item>
                      </a-col>
                    </a-row>

                  </template>

                  <a-form-item v-if="targetType === 'MYSQL'">
                    <a-checkbox v-model="taskForm.optimize_index">
                      <a-space direction="vertical" :size="4">
                        <span style="font-weight: 500">启用索引优化</span>

                        <a-typography-text
                          type="secondary"
                          style="font-size: 12px"
                        >
                          同步前删除非主键索引以提高写入性能，所有表数据同步完成后按顺序统一重建
                        </a-typography-text>
                      </a-space>
                    </a-checkbox>
                  </a-form-item>

                  <a-form-item
                    v-if="targetType === 'MYSQL' && taskForm.optimize_index"
                    field="index_restore_worker_count"
                    label="索引回放并发度"
                  >
                    <a-input-number
                      :model-value="taskForm.index_restore_worker_count"
                      @update:model-value="
                        (v) => (taskForm.index_restore_worker_count = v ?? 0)
                      "
                      :min="0"
                      :max="16"
                      :step="1"
                      :precision="0"
                      placeholder="0=自动"
                      allow-clear
                    />
                    <a-typography-text
                      type="secondary"
                      style="font-size: 12px; display: block; margin-top: 4px"
                    >
                      阶段3索引回放的表级并发度（不同表之间并行执行）。
                      0 表示按 min(worker_count, 4) 自动推导；建议 ≤
                      target_max_open_conns - 2。
                    </a-typography-text>
                  </a-form-item>

                  <a-form-item v-if="targetType === 'MYSQL'">
                    <a-checkbox v-model="taskForm.enable_read_only">
                      <a-space direction="vertical" :size="4">
                        <span style="font-weight: 500"
                          >同步期间启用目标库只读保护</span
                        >

                        <a-typography-text
                          type="secondary"
                          style="font-size: 12px"
                        >
                          勾选后设置 read_only=ON、super_read_only=OFF，阻止普通账号写入；同步账号需具备相应权限，同步结束后恢复原状态。不勾选则不修改目标实例的只读状态
                        </a-typography-text>
                      </a-space>
                    </a-checkbox>
                  </a-form-item>

                  <a-form-item v-if="targetType === 'MYSQL'">
                    <a-checkbox v-model="taskForm.enable_drop_table_before_ddl">
                      <a-space direction="vertical" :size="4">
                        <span style="font-weight: 500">{{
                          selectedSyncLevel === "database"
                            ? "同步前删除目标库"
                            : "同步 DDL 前删除目标表"
                        }}</span>

                        <a-typography-text
                          v-if="selectedSyncLevel === 'database'"
                          type="danger"
                          style="font-size: 12px"
                        >
                          破坏性操作：库级别同步开始前会先 DROP DATABASE IF EXISTS 再 CREATE DATABASE 重建整个目标库，目标库内所有表与数据将丢失。每个唯一目标库只重建一次，之后不再逐表删除。
                        </a-typography-text>

                        <a-typography-text
                          v-else
                          type="secondary"
                          style="font-size: 12px"
                        >
                          建表前执行 DROP TABLE IF EXISTS，仅作用于目标库/目标表；适用于需要重建表结构的场景
                        </a-typography-text>
                      </a-space>
                    </a-checkbox>
                  </a-form-item>

                  <a-form-item v-if="targetType === 'MYSQL'">
                    <a-checkbox v-model="taskForm.enable_skip_binlog">
                      <a-space direction="vertical" :size="4">
                        <span style="font-weight: 500">全量同步写入前关闭目标端 binlog</span>
                        <a-typography-text type="secondary" style="font-size: 12px">
                          勾选后在全量批量写入前执行 SET SESSION sql_log_bin=0，避免目标端 binlog 膨胀与级联复制回环；写入完成后自动恢复。需目标库账号具备 SUPER 权限
                        </a-typography-text>
                      </a-space>
                    </a-checkbox>
                  </a-form-item>

                  <a-collapse :default-active-key="[]">
                    <a-collapse-item key="source" header="自定义源数据库连接">
                      <template #extra
                        ><a-switch
                          v-model="useCustomSourceDB"
                          size="small"
                          @click.stop
                      /></template>

                      <div v-if="useCustomSourceDB">
                        <a-row :gutter="16">
                          <a-col :span="12"
                            ><a-form-item label="主机"
                              ><a-input
                                v-model="customSourceDB.host"
                                placeholder="如: 192.168.1.100" /></a-form-item
                          ></a-col>

                          <a-col :span="12"
                            ><a-form-item label="端口"
                              ><a-input-number
                                v-model="customSourceDB.port"
                                :min="1"
                                :max="65535" /></a-form-item
                          ></a-col>
                        </a-row>

                        <a-row :gutter="16">
                          <a-col :span="12"
                            ><a-form-item label="数据库"
                              ><a-input
                                v-model="
                                  customSourceDB.database
                                " /></a-form-item
                          ></a-col>

                          <a-col :span="12"
                            ><a-form-item label="用户名"
                              ><a-input
                                v-model="
                                  customSourceDB.username
                                " /></a-form-item
                          ></a-col>
                        </a-row>

                        <a-row :gutter="16">
                          <a-col :span="12"
                            ><a-form-item label="密码"
                              ><a-input-password
                                v-model="
                                  customSourceDB.password
                                " /></a-form-item
                          ></a-col>

                          <a-col :span="12">
                            <a-form-item label="操作">
                              <a-space>
                                <a-button
                                  type="outline"
                                  size="small"
                                  @click="testSourceConnection"
                                  ><template #icon><icon-check /></template
                                  >测试连接</a-button
                                >

                                <a-button
                                  type="primary"
                                  size="small"
                                  @click="saveSourceConfig"
                                  ><template #icon><icon-save /></template
                                  >保存配置</a-button
                                >
                              </a-space>
                            </a-form-item>
                          </a-col>
                        </a-row>
                      </div>
                    </a-collapse-item>

                    <a-collapse-item
                      v-if="targetType === 'MYSQL'"
                      key="target"
                      header="自定义目标数据库连接"
                    >
                      <template #extra
                        ><a-switch
                          v-model="useCustomTargetDB"
                          size="small"
                          @click.stop
                      /></template>

                      <div v-if="useCustomTargetDB">
                        <a-row :gutter="16">
                          <a-col :span="12"
                            ><a-form-item label="主机"
                              ><a-input
                                v-model="customTargetDB.host"
                                placeholder="如: 192.168.1.101" /></a-form-item
                          ></a-col>

                          <a-col :span="12"
                            ><a-form-item label="端口"
                              ><a-input-number
                                v-model="customTargetDB.port"
                                :min="1"
                                :max="65535" /></a-form-item
                          ></a-col>
                        </a-row>

                        <a-row :gutter="16">
                          <a-col :span="12"
                            ><a-form-item label="数据库"
                              ><a-input
                                v-model="
                                  customTargetDB.database
                                " /></a-form-item
                          ></a-col>

                          <a-col :span="12"
                            ><a-form-item label="用户名"
                              ><a-input
                                v-model="
                                  customTargetDB.username
                                " /></a-form-item
                          ></a-col>
                        </a-row>

                        <a-row :gutter="16">
                          <a-col :span="12"
                            ><a-form-item label="密码"
                              ><a-input-password
                                v-model="
                                  customTargetDB.password
                                " /></a-form-item
                          ></a-col>

                          <a-col :span="12">
                            <a-form-item label="操作">
                              <a-space>
                                <a-button
                                  type="outline"
                                  size="small"
                                  @click="testTargetConnection"
                                  ><template #icon><icon-check /></template
                                  >测试连接</a-button
                                >

                                <a-button
                                  type="primary"
                                  size="small"
                                  @click="saveTargetConfig"
                                  ><template #icon><icon-save /></template
                                  >保存配置</a-button
                                >
                              </a-space>
                            </a-form-item>
                          </a-col>
                        </a-row>
                      </div>
                    </a-collapse-item>
                  </a-collapse>

                  <!-- 目标端配置：非 MySQL 目标始终显示；MySQL 目标仅增量/全量+增量时显示 -->
                  <div
                    v-if="
                      targetType !== 'MYSQL' ||
                      taskForm.mode === 'INCREMENTAL' ||
                      taskForm.mode === 'ALL'
                    "
                    style="margin-top: 20px"
                  >
                    <a-typography-title :heading="6" style="margin-bottom: 12px"
                      >目标端配置</a-typography-title
                    >

                    <template v-if="targetType === 'MYSQL'">
                      <a-alert type="info" style="margin-bottom: 16px">
                        当前使用默认 MySQL
                        目标端，将同步到上方配置的目标数据库。
                      </a-alert>
                    </template>

                    <template v-if="targetType === 'KAFKA'">
                      <a-card :bordered="true" style="margin-bottom: 16px">
                        <a-row :gutter="16">
                          <a-col :span="12">
                            <a-form-item
                              label="Brokers (逗号分隔)"
                              style="margin-bottom: 16px"
                            >
                              <a-input
                                v-model="singleKafkaConfig.brokers"
                                placeholder="kafka1:9092, kafka2:9092"
                              />
                            </a-form-item>
                          </a-col>
                          <a-col :span="12">
                            <a-form-item
                              label="Topic"
                              style="margin-bottom: 16px"
                            >
                              <a-input
                                v-model="singleKafkaConfig.topic"
                                placeholder="mysql_cdc"
                              />
                            </a-form-item>
                          </a-col>
                        </a-row>
                        <a-row :gutter="16">
                          <a-col :span="8">
                            <a-form-item
                              label="路由模式"
                              style="margin-bottom: 16px"
                            >
                              <a-select
                                v-model="singleKafkaConfig.routing_mode"
                              >
                                <a-option value="single_topic"
                                  >单 Topic (single_topic)</a-option
                                >
                                <a-option value="per_table"
                                  >每表独立 (per_table)</a-option
                                >
                              </a-select>
                            </a-form-item>
                          </a-col>
                          <a-col :span="8">
                            <a-form-item
                              label="Topic 前缀"
                              style="margin-bottom: 16px"
                            >
                              <a-input
                                v-model="singleKafkaConfig.topic_prefix"
                                placeholder="cdc"
                                :disabled="
                                  singleKafkaConfig.routing_mode !==
                                  'per_table'
                                "
                              />
                            </a-form-item>
                          </a-col>
                          <a-col :span="8">
                            <a-form-item
                              label="Key 模式"
                              style="margin-bottom: 16px"
                            >
                              <a-select v-model="singleKafkaConfig.key_mode">
                                <a-option value="pk">主键 (pk)</a-option>
                                <a-option value="none">无 (none)</a-option>
                              </a-select>
                            </a-form-item>
                          </a-col>
                        </a-row>
                        <a-row :gutter="16">
                          <a-col :span="8">
                            <a-form-item
                              label="批量大小"
                              style="margin-bottom: 16px"
                            >
                              <a-input-number
                                v-model="singleKafkaConfig.batch_size"
                                :min="1"
                                style="width: 100%"
                              />
                            </a-form-item>
                          </a-col>
                          <a-col :span="8">
                            <a-form-item
                              label="批量超时 (ms)"
                              style="margin-bottom: 16px"
                            >
                              <a-input-number
                                v-model="singleKafkaConfig.batch_timeout_ms"
                                :min="0"
                                style="width: 100%"
                              />
                            </a-form-item>
                          </a-col>
                          <a-col :span="8">
                            <a-form-item
                              label="应答模式"
                              style="margin-bottom: 16px"
                            >
                              <a-select
                                v-model="singleKafkaConfig.required_acks"
                              >
                                <a-option :value="0">不等待 (0)</a-option>
                                <a-option :value="1">Leader确认 (1)</a-option>
                                <a-option :value="-1">全部ISR (-1)</a-option>
                              </a-select>
                            </a-form-item>
                          </a-col>
                        </a-row>
                        <a-collapse
                          :bordered="false"
                          style="margin-top: 8px"
                        >
                          <a-collapse-item
                            header="Security (SASL / SSL)"
                            key="security"
                          >
                            <a-row :gutter="16">
                              <a-col :span="8">
                                <a-form-item
                                  label="SASL 机制"
                                  style="margin-bottom: 12px"
                                >
                                  <a-select
                                    v-model="
                                      singleKafkaConfig.security
                                        .sasl_mechanism
                                    "
                                    placeholder="无"
                                    allow-clear
                                  >
                                    <a-option value="PLAIN">PLAIN</a-option>
                                    <a-option value="SCRAM-SHA-256"
                                      >SCRAM-SHA-256</a-option
                                    >
                                    <a-option value="SCRAM-SHA-512"
                                      >SCRAM-SHA-512</a-option
                                    >
                                  </a-select>
                                </a-form-item>
                              </a-col>
                              <a-col :span="8">
                                <a-form-item
                                  label="SASL 用户名"
                                  style="margin-bottom: 12px"
                                >
                                  <a-input
                                    v-model="
                                      singleKafkaConfig.security
                                        .sasl_username
                                    "
                                    placeholder="user"
                                  />
                                </a-form-item>
                              </a-col>
                              <a-col :span="8">
                                <a-form-item
                                  label="SASL 密码"
                                  style="margin-bottom: 12px"
                                >
                                  <a-input-password
                                    v-model="
                                      singleKafkaConfig.security
                                        .sasl_password
                                    "
                                    placeholder="******"
                                  />
                                </a-form-item>
                              </a-col>
                            </a-row>
                            <a-row :gutter="16">
                              <a-col :span="4">
                                <a-form-item
                                  label="启用 TLS"
                                  style="margin-bottom: 12px"
                                >
                                  <a-switch
                                    v-model="
                                      singleKafkaConfig.security.tls_enabled
                                    "
                                  />
                                </a-form-item>
                              </a-col>
                              <a-col :span="4">
                                <a-form-item
                                  label="跳过证书校验"
                                  style="margin-bottom: 12px"
                                >
                                  <a-switch
                                    v-model="
                                      singleKafkaConfig.security
                                        .insecure_skip_verify
                                    "
                                    :disabled="
                                      !singleKafkaConfig.security.tls_enabled
                                    "
                                  />
                                </a-form-item>
                              </a-col>
                              <a-col :span="8">
                                <a-form-item
                                  label="CA 证书路径"
                                  style="margin-bottom: 12px"
                                >
                                  <a-input
                                    v-model="
                                      singleKafkaConfig.security.ca_cert_path
                                    "
                                    placeholder="/etc/kafka/ca.pem"
                                    :disabled="
                                      !singleKafkaConfig.security.tls_enabled
                                    "
                                  />
                                </a-form-item>
                              </a-col>
                              <a-col :span="8">
                                <a-form-item
                                  label="客户端证书路径"
                                  style="margin-bottom: 12px"
                                >
                                  <a-input
                                    v-model="
                                      singleKafkaConfig.security
                                        .client_cert_path
                                    "
                                    placeholder="/etc/kafka/client.pem"
                                    :disabled="
                                      !singleKafkaConfig.security.tls_enabled
                                    "
                                  />
                                </a-form-item>
                              </a-col>
                            </a-row>
                            <a-row :gutter="16">
                              <a-col :span="8">
                                <a-form-item
                                  label="客户端密钥路径"
                                  style="margin-bottom: 12px"
                                >
                                  <a-input
                                    v-model="
                                      singleKafkaConfig.security
                                        .client_key_path
                                    "
                                    placeholder="/etc/kafka/client.key"
                                    :disabled="
                                      !singleKafkaConfig.security.tls_enabled
                                    "
                                  />
                                </a-form-item>
                              </a-col>
                            </a-row>
                          </a-collapse-item>
                        </a-collapse>
                      </a-card>
                    </template>

                    <template v-if="targetType === 'WEBHOOK'">
                      <a-card :bordered="true" style="margin-bottom: 16px">
                        <a-row :gutter="16">
                          <a-col :span="16">
                            <a-form-item
                              label="Webhook URL"
                              style="margin-bottom: 16px"
                            >
                              <a-input
                                v-model="singleWebhookConfig.url"
                                placeholder="https://api.example.com/webhook"
                              />
                            </a-form-item>
                          </a-col>
                          <a-col :span="8">
                            <a-form-item
                              label="HTTP 方法"
                              style="margin-bottom: 16px"
                            >
                              <a-select v-model="singleWebhookConfig.method">
                                <a-option value="POST">POST</a-option>
                                <a-option value="PUT">PUT</a-option>
                              </a-select>
                            </a-form-item>
                          </a-col>
                        </a-row>
                        <a-row :gutter="16">
                          <a-col :span="6">
                            <a-form-item
                              label="超时 (ms)"
                              style="margin-bottom: 16px"
                            >
                              <a-input-number
                                v-model="singleWebhookConfig.timeout_ms"
                                :min="100"
                                style="width: 100%"
                              />
                            </a-form-item>
                          </a-col>
                          <a-col :span="6">
                            <a-form-item
                              label="重试次数"
                              style="margin-bottom: 16px"
                            >
                              <a-input-number
                                v-model="singleWebhookConfig.retry_times"
                                :min="0"
                                :max="10"
                                style="width: 100%"
                              />
                            </a-form-item>
                          </a-col>
                          <a-col :span="6">
                            <a-form-item
                              label="重试退避 (ms)"
                              style="margin-bottom: 16px"
                            >
                              <a-input-number
                                v-model="singleWebhookConfig.retry_backoff_ms"
                                :min="0"
                                style="width: 100%"
                              />
                            </a-form-item>
                          </a-col>
                          <a-col :span="6">
                            <a-form-item
                              label="自定义 Headers"
                              style="margin-bottom: 16px"
                            >
                              <a-textarea
                                v-model="singleWebhookConfig.headers"
                                placeholder="Key: Value&#10;Authorization: Bearer xxx"
                                :auto-size="{ minRows: 2, maxRows: 4 }"
                              />
                            </a-form-item>
                          </a-col>
                        </a-row>
                      </a-card>
                    </template>

                    <!-- Multi-Sink 目标端配置 -->
                    <template v-if="targetType === 'MULTI'">
                      <div
                        style="
                          display: flex;
                          align-items: center;
                          justify-content: space-between;
                          margin-bottom: 12px;
                        "
                      >
                        <span style="color: #4e5969"
                          >配置多个目标端同时写入</span
                        >
                        <a-button
                          type="primary"
                          size="small"
                          @click="addSinkConfig"
                        >
                          <template #icon><icon-plus /></template>
                          添加目标端
                        </a-button>
                      </div>
                      <a-alert
                        v-if="sinkConfigs.length === 0"
                        type="info"
                        style="margin-bottom: 8px"
                      >
                        未配置目标端时，增量同步将使用默认 MySQL
                        目标库（上方已配置的目标数据库）。点击「添加目标端」可配置
                        Kafka、Webhook 等多目标。
                      </a-alert>

                      <div
                        v-for="(sc, idx) in sinkConfigs"
                        :key="idx"
                        class="sink-config-item"
                        style="
                          border: 1px solid #e5e6eb;
                          border-radius: 8px;
                          padding: 16px;
                          margin-bottom: 12px;
                          background: #fafafa;
                          position: relative;
                        "
                      >
                        <a-button
                          type="text"
                          size="mini"
                          status="danger"
                          style="position: absolute; top: 8px; right: 8px"
                          @click="removeSinkConfig(idx)"
                        >
                          <template #icon><icon-close /></template>
                        </a-button>
                        <a-row :gutter="16">
                          <a-col :span="8">
                            <a-form-item
                              label="目标端类型"
                              style="margin-bottom: 8px"
                            >
                              <a-select
                                v-model="sc.type"
                                @change="onSinkTypeChange(idx)"
                              >
                                <a-option
                                  v-for="st in SINK_TYPES"
                                  :key="st.value"
                                  :value="st.value"
                                  >{{ st.label }}</a-option
                                >
                              </a-select>
                            </a-form-item>
                          </a-col>
                          <a-col :span="16">
                            <a-tag
                              :color="
                                sc.type === 'MYSQL'
                                  ? 'blue'
                                  : sc.type === 'KAFKA'
                                    ? 'orange'
                                    : 'green'
                              "
                              style="margin-top: 28px"
                            >
                              {{ getSinkTypeLabel(sc.type) }}
                            </a-tag>
                          </a-col>
                        </a-row>

                        <!-- MySQL Options -->
                        <template v-if="sc.type === 'MYSQL'">
                          <a-row :gutter="16">
                            <a-col :span="8">
                              <a-form-item
                                label="主机地址"
                                style="margin-bottom: 8px"
                              >
                                <a-input
                                  v-model="sc.options.host"
                                  placeholder="192.168.1.100"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="4">
                              <a-form-item
                                label="端口"
                                style="margin-bottom: 8px"
                              >
                                <a-input-number
                                  v-model="sc.options.port"
                                  :min="1"
                                  :max="65535"
                                  style="width: 100%"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="6">
                              <a-form-item
                                label="用户名"
                                style="margin-bottom: 8px"
                              >
                                <a-input
                                  v-model="sc.options.username"
                                  placeholder="root"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="6">
                              <a-form-item
                                label="密码"
                                style="margin-bottom: 8px"
                              >
                                <a-input-password
                                  v-model="sc.options.password"
                                  placeholder="密码"
                                />
                              </a-form-item>
                            </a-col>
                          </a-row>
                          <a-row :gutter="16">
                            <a-col :span="8">
                              <a-form-item
                                label="数据库名"
                                style="margin-bottom: 8px"
                              >
                                <a-input
                                  v-model="sc.options.database"
                                  placeholder="目标数据库"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="8">
                              <a-form-item
                                label="目标 Schema"
                                style="margin-bottom: 8px"
                              >
                                <a-input
                                  v-model="sc.options.target_schema"
                                  placeholder="留空则使用源库名"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="8">
                              <a-form-item
                                label="批量大小"
                                style="margin-bottom: 8px"
                              >
                                <a-input-number
                                  v-model="sc.options.batch_size"
                                  :min="1"
                                  style="width: 100%"
                                />
                              </a-form-item>
                            </a-col>
                          </a-row>
                        </template>

                        <!-- Kafka Options -->
                        <template v-if="sc.type === 'KAFKA'">
                          <a-row :gutter="16">
                            <a-col :span="12">
                              <a-form-item
                                label="Brokers (逗号分隔)"
                                style="margin-bottom: 8px"
                              >
                                <a-input
                                  v-model="sc.options.brokers"
                                  placeholder="kafka1:9092, kafka2:9092"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="12">
                              <a-form-item
                                label="Topic"
                                style="margin-bottom: 8px"
                              >
                                <a-input
                                  v-model="sc.options.topic"
                                  placeholder="mysql_cdc"
                                />
                              </a-form-item>
                            </a-col>
                          </a-row>
                          <a-row :gutter="16">
                            <a-col :span="8">
                              <a-form-item
                                label="路由模式"
                                style="margin-bottom: 8px"
                              >
                                <a-select v-model="sc.options.routing_mode">
                                  <a-option value="single_topic"
                                    >单 Topic (single_topic)</a-option
                                  >
                                  <a-option value="per_table"
                                    >每表独立 (per_table)</a-option
                                  >
                                </a-select>
                              </a-form-item>
                            </a-col>
                            <a-col :span="8">
                              <a-form-item
                                label="Topic 前缀"
                                style="margin-bottom: 8px"
                              >
                                <a-input
                                  v-model="sc.options.topic_prefix"
                                  placeholder="cdc"
                                  :disabled="
                                    sc.options.routing_mode !== 'per_table'
                                  "
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="8">
                              <a-form-item
                                label="Key 模式"
                                style="margin-bottom: 8px"
                              >
                                <a-select v-model="sc.options.key_mode">
                                  <a-option value="pk">主键 (pk)</a-option>
                                  <a-option value="none">无 (none)</a-option>
                                </a-select>
                              </a-form-item>
                            </a-col>
                          </a-row>
                          <a-row :gutter="16">
                            <a-col :span="8">
                              <a-form-item
                                label="批量大小"
                                style="margin-bottom: 8px"
                              >
                                <a-input-number
                                  v-model="sc.options.batch_size"
                                  :min="1"
                                  style="width: 100%"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="8">
                              <a-form-item
                                label="批量超时 (ms)"
                                style="margin-bottom: 8px"
                              >
                                <a-input-number
                                  v-model="sc.options.batch_timeout_ms"
                                  :min="0"
                                  style="width: 100%"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="8">
                              <a-form-item
                                label="应答模式"
                                style="margin-bottom: 8px"
                              >
                                <a-select v-model="sc.options.required_acks">
                                  <a-option :value="0">不等待 (0)</a-option>
                                  <a-option :value="1">Leader确认 (1)</a-option>
                                  <a-option :value="-1">全部ISR (-1)</a-option>
                                </a-select>
                              </a-form-item>
                            </a-col>
                          </a-row>
                          <a-collapse
                            :bordered="false"
                            style="margin-top: 4px"
                          >
                            <a-collapse-item
                              header="Security (SASL / SSL)"
                              key="security"
                            >
                              <a-row :gutter="16">
                                <a-col :span="8">
                                  <a-form-item
                                    label="SASL 机制"
                                    style="margin-bottom: 8px"
                                  >
                                    <a-select
                                      v-model="
                                        sc.options.security.sasl_mechanism
                                      "
                                      placeholder="无"
                                      allow-clear
                                    >
                                      <a-option value="PLAIN">PLAIN</a-option>
                                      <a-option value="SCRAM-SHA-256"
                                        >SCRAM-SHA-256</a-option
                                      >
                                      <a-option value="SCRAM-SHA-512"
                                        >SCRAM-SHA-512</a-option
                                      >
                                    </a-select>
                                  </a-form-item>
                                </a-col>
                                <a-col :span="8">
                                  <a-form-item
                                    label="SASL 用户名"
                                    style="margin-bottom: 8px"
                                  >
                                    <a-input
                                      v-model="
                                        sc.options.security.sasl_username
                                      "
                                      placeholder="user"
                                    />
                                  </a-form-item>
                                </a-col>
                                <a-col :span="8">
                                  <a-form-item
                                    label="SASL 密码"
                                    style="margin-bottom: 8px"
                                  >
                                    <a-input-password
                                      v-model="
                                        sc.options.security.sasl_password
                                      "
                                      placeholder="******"
                                    />
                                  </a-form-item>
                                </a-col>
                              </a-row>
                              <a-row :gutter="16">
                                <a-col :span="4">
                                  <a-form-item
                                    label="启用 TLS"
                                    style="margin-bottom: 8px"
                                  >
                                    <a-switch
                                      v-model="
                                        sc.options.security.tls_enabled
                                      "
                                    />
                                  </a-form-item>
                                </a-col>
                                <a-col :span="4">
                                  <a-form-item
                                    label="跳过证书校验"
                                    style="margin-bottom: 8px"
                                  >
                                    <a-switch
                                      v-model="
                                        sc.options.security
                                          .insecure_skip_verify
                                      "
                                      :disabled="
                                        !sc.options.security.tls_enabled
                                      "
                                    />
                                  </a-form-item>
                                </a-col>
                                <a-col :span="8">
                                  <a-form-item
                                    label="CA 证书路径"
                                    style="margin-bottom: 8px"
                                  >
                                    <a-input
                                      v-model="
                                        sc.options.security.ca_cert_path
                                      "
                                      placeholder="/etc/kafka/ca.pem"
                                      :disabled="
                                        !sc.options.security.tls_enabled
                                      "
                                    />
                                  </a-form-item>
                                </a-col>
                                <a-col :span="8">
                                  <a-form-item
                                    label="客户端证书路径"
                                    style="margin-bottom: 8px"
                                  >
                                    <a-input
                                      v-model="
                                        sc.options.security.client_cert_path
                                      "
                                      placeholder="/etc/kafka/client.pem"
                                      :disabled="
                                        !sc.options.security.tls_enabled
                                      "
                                    />
                                  </a-form-item>
                                </a-col>
                              </a-row>
                              <a-row :gutter="16">
                                <a-col :span="8">
                                  <a-form-item
                                    label="客户端密钥路径"
                                    style="margin-bottom: 8px"
                                  >
                                    <a-input
                                      v-model="
                                        sc.options.security.client_key_path
                                      "
                                      placeholder="/etc/kafka/client.key"
                                      :disabled="
                                        !sc.options.security.tls_enabled
                                      "
                                    />
                                  </a-form-item>
                                </a-col>
                              </a-row>
                            </a-collapse-item>
                          </a-collapse>
                        </template>

                        <!-- HTTP Webhook Options -->
                        <template v-if="sc.type === 'HTTP_WEBHOOK'">
                          <a-row :gutter="16">
                            <a-col :span="16">
                              <a-form-item
                                label="Webhook URL"
                                style="margin-bottom: 8px"
                              >
                                <a-input
                                  v-model="sc.options.url"
                                  placeholder="https://api.example.com/webhook"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="8">
                              <a-form-item
                                label="HTTP 方法"
                                style="margin-bottom: 8px"
                              >
                                <a-select v-model="sc.options.method">
                                  <a-option value="POST">POST</a-option>
                                  <a-option value="PUT">PUT</a-option>
                                </a-select>
                              </a-form-item>
                            </a-col>
                          </a-row>
                          <a-row :gutter="16">
                            <a-col :span="6">
                              <a-form-item
                                label="超时 (ms)"
                                style="margin-bottom: 8px"
                              >
                                <a-input-number
                                  v-model="sc.options.timeout_ms"
                                  :min="100"
                                  style="width: 100%"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="6">
                              <a-form-item
                                label="重试次数"
                                style="margin-bottom: 8px"
                              >
                                <a-input-number
                                  v-model="sc.options.retry_times"
                                  :min="0"
                                  :max="10"
                                  style="width: 100%"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="6">
                              <a-form-item
                                label="重试退避 (ms)"
                                style="margin-bottom: 8px"
                              >
                                <a-input-number
                                  v-model="sc.options.retry_backoff_ms"
                                  :min="0"
                                  style="width: 100%"
                                />
                              </a-form-item>
                            </a-col>
                            <a-col :span="6">
                              <a-form-item
                                label="自定义 Headers"
                                style="margin-bottom: 8px"
                              >
                                <a-textarea
                                  v-model="sc.options.headers"
                                  placeholder="Key: Value&#10;Authorization: Bearer xxx"
                                  :auto-size="{ minRows: 2, maxRows: 4 }"
                                />
                              </a-form-item>
                            </a-col>
                          </a-row>
                        </template>
                      </div>
                    </template>
                  </div>
                </a-card>
              </a-col>
            </a-row>
          </a-form>
        </div>
</template>

<style scoped>
.task-form-full-page {
  max-width: 1100px;

  margin: 0 auto;

  padding: 8px 0 40px;
}
/* 双栏选择器样式 */

.db-transfer-container {
  display: flex;

  align-items: flex-start;

  gap: 16px;

  height: 400px;

  margin-bottom: 24px;
}
.transfer-pane {
  flex: 1;

  display: flex;

  flex-direction: column;

  border: 1px solid var(--app-border);

  border-radius: 4px;

  background: var(--app-surface);

  height: 100%;

  overflow: hidden;
}
.transfer-header {
  padding: 8px 12px;

  border-bottom: 1px solid var(--app-border-soft);

  background: var(--app-surface-soft);

  display: flex;

  justify-content: space-between;

  align-items: center;
}
.transfer-header .title {
  font-weight: 500;

  font-size: 14px;

  color: var(--app-text);
}
.transfer-header-tip {
  display: flex;

  padding: 6px 12px;

  background: var(--app-surface);

  border-bottom: 1px solid var(--app-border-soft);

  font-size: 12px;

  color: var(--app-muted);
}
.transfer-header-tip span {
  flex: 1;
}
.transfer-search {
  padding: 8px 12px;

  border-bottom: 1px solid var(--app-border-soft);
}
.advanced-config-row {
  margin-top: 16px;
}
.advanced-config-card {
  background: #fff;

  border-radius: 8px;
}
.transfer-content {
  flex: 1;

  overflow-y: auto;

  display: flex;

  flex-direction: column;
}
.transfer-content.bg-white {
  background: #fff;
}
.transfer-list-header {
  padding: 8px 12px;

  border-bottom: 1px solid #f2f3f5;

  display: flex;

  justify-content: space-between;

  align-items: center;

  font-size: 13px;

  color: #4e5969;
}
.transfer-list {
  padding: 4px 0;
}
.transfer-list-item {
  padding: 6px 12px;

  display: flex;

  align-items: center;
}
.transfer-list-item:hover {
  background: #f2f3f5;
}
.mapped-item {
  display: flex;

  align-items: center;

  padding: 8px 12px;

  border-bottom: 1px solid #f2f3f5;

  gap: 8px;
}
.mapped-item:hover {
  background: #f7f8fa;
}
.mapped-item .source-name {
  flex: 1;

  font-size: 13px;

  color: #1d2129;

  white-space: nowrap;

  overflow: hidden;

  text-overflow: ellipsis;

  display: flex;

  align-items: center;
}
.mapped-item .target-input {
  flex: 1;
}
.mapped-item .remove-btn {
  opacity: 0;

  transition: opacity 0.2s;
}
.mapped-item:hover .remove-btn {
  opacity: 1;
}
.transfer-arrow {
  display: flex;

  align-items: center;

  justify-content: center;

  height: 100%;

  color: var(--app-muted);

  padding-top: 40px; /* 稍微下移一点，对齐内容区 */
}
/* Layout refinements for task list filters and table-level sync configuration */
.task-base-config-row {
  align-items: flex-start;
}
/* Polished create task layout */
.task-form-full-page {
  max-width: 980px;
  padding: 24px 16px 56px;
}
.task-create-form {
  display: flex;
  flex-direction: column;
  gap: 18px;
}
.task-base-config-row,
.advanced-config-row {
  margin: 0;
}
.task-base-config-row {
  padding: 26px 28px 8px;
  border: 1px solid #e5e8ef;
  border-radius: 8px;
  background: #fff;
  box-shadow: 0 10px 28px rgba(29, 33, 41, 0.06);
}
.task-base-config-row > .arco-col,
.advanced-config-row > .arco-col {
  padding-left: 0 !important;
  padding-right: 0 !important;
}
.task-base-config-row :deep(.arco-form-item) {
  margin-bottom: 22px;
}
.task-base-config-row :deep(.arco-form-item-label-col) {
  margin-bottom: 8px;
}
.task-base-config-row :deep(.arco-form-item-label-col > label) {
  color: var(--app-text);
  font-size: 13px;
  font-weight: 600;
}
.task-base-config-row :deep(.arco-input-wrapper),
.task-base-config-row :deep(.arco-select-view) {
  min-height: 40px;
  border-color: var(--app-border-soft);
  border-radius: 6px;
  background: var(--app-surface-soft);
}
.task-base-config-row :deep(.arco-input-wrapper:hover),
.task-base-config-row :deep(.arco-select-view:hover) {
  border-color: var(--app-accent);
  background: var(--app-surface);
}
.task-base-config-row :deep(.arco-input-wrapper.arco-input-focus),
.task-base-config-row :deep(.arco-select-view-focus) {
  border-color: var(--app-accent);
  background: var(--app-surface);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--app-accent) 12%, transparent);
}
.task-base-config-row :deep(.arco-radio-group) {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}
.task-base-config-row :deep(.arco-radio) {
  min-height: 38px;
  margin-right: 0;
  padding: 8px 14px;
  border: 1px solid #e5e8ef;
  border-radius: 6px;
  background: #f7f9fc;
  transition: border-color 0.16s ease, background 0.16s ease, box-shadow 0.16s ease;
}
.task-base-config-row :deep(.arco-radio:hover),
.task-base-config-row :deep(.arco-radio-checked) {
  border-color: #4080ff;
  background: #eef6ff;
}
.task-base-config-row :deep(.arco-radio-checked) {
  box-shadow: 0 4px 12px rgba(64, 128, 255, 0.12);
}
.task-base-config-row :deep(.arco-tag) {
  min-height: 34px;
  padding: 0 12px;
  border-radius: 6px;
  font-weight: 600;
}
.db-transfer-container {
  margin-top: 6px;
  margin-bottom: 22px;
  padding: 16px;
  border: 1px solid #e5e8ef;
  border-radius: 8px;
  background: #f8fafc;
}
.transfer-pane {
  border-color: #e5e8ef;
  border-radius: 8px;
  background: #fff;
  box-shadow: 0 4px 14px rgba(29, 33, 41, 0.04);
}
.transfer-header {
  min-height: 44px;
  height: 44px;
  padding: 0 14px;
  border-bottom-color: #edf0f5;
  background: #fbfcff;
  display: flex;
  align-items: center;
}
.transfer-header .title {
  color: #1d2129;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.transfer-search {
  padding: 12px 14px;
}
.transfer-list-item,
.mapped-item {
  min-height: 40px;
  padding-left: 14px;
  padding-right: 14px;
}
.transfer-arrow {
  width: 42px;
  min-width: 42px;
  padding-top: 52px;
  color: var(--app-accent);
}
.advanced-config-card {
  border: 1px solid var(--app-border);
  border-radius: 8px;
  box-shadow: 0 8px 22px rgba(29, 33, 41, 0.05);
}
.advanced-config-card :deep(.arco-card-body) {
  padding: 22px 24px;
}
.advanced-config-card :deep(.arco-form-item) {
  margin-bottom: 16px;
}
.advanced-config-card :deep(.arco-form-item-label-col) {
  margin-bottom: 6px;
}
.advanced-config-card :deep(.arco-form-item-label-col > label) {
  color: var(--app-text);
  font-size: 13px;
  font-weight: 600;
}
.advanced-config-card :deep(.arco-form-item-content) {
  display: flex;
  flex-direction: column !important;
  align-items: stretch !important;
  gap: 4px;
}
.advanced-config-card :deep(.arco-form-item-content) .arco-input-wrapper,
.advanced-config-card :deep(.arco-form-item-content) .arco-select-view,
.advanced-config-card :deep(.arco-form-item-content) .arco-radio-group,
.advanced-config-card :deep(.arco-form-item-content) .arco-space {
  width: 100%;
}
.advanced-config-card :deep(.arco-typography-secondary) {
  line-height: 1.5;
  width: 100%;
}
.advanced-config-card :deep(.arco-row) {
  align-items: flex-start;
}
/* Corrected alignment pass */
.task-base-config-row :deep(.arco-select),
.task-base-config-row :deep(.arco-input-wrapper) {
  max-width: 100%;
}
.task-form-full-page {
  max-width: 1180px;
}
.task-create-form {
  gap: 20px;
}
.task-base-config-row,
.advanced-config-card,
.table-mapping-panel,
.table-selector-panel,
.transfer-pane {
  position: relative;
  overflow: hidden;
}
.task-base-config-row {
  padding: 28px 30px 10px;
}
.advanced-config-card,
.table-mapping-panel,
.table-selector-panel,
.transfer-pane {
  padding: 20px;
}
.task-base-config-row::before {
  content: "";
  position: absolute;
  inset: 0;
  pointer-events: none;
  background:
    linear-gradient(90deg, color-mix(in srgb, var(--app-accent) 22%, transparent), transparent 22%, transparent 72%, color-mix(in srgb, var(--app-accent-2) 18%, transparent)),
    linear-gradient(180deg, rgba(255, 255, 255, 0.05), transparent 18%);
  opacity: 0.85;
}
.task-base-config-row > .arco-col {
  position: relative;
  z-index: 1;
}

/* Unified panel styling */
.table-config-row {
  align-items: stretch;
  flex-wrap: nowrap;
}
.table-config-row > .arco-col {
  display: flex;
  flex: 0 0 50%;
  width: 50%;
  max-width: 50%;
}
.table-mapping-panel,
.table-selector-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  height: 520px;
  min-height: 520px;
  max-height: 520px;
  background: var(--app-surface);
  border: 1px solid var(--app-border);
  border-radius: 8px;
  padding: 16px;
  overflow: hidden;
}
.table-mapping-title {
  flex-shrink: 0;
  height: 22px;
  line-height: 22px;
  margin-bottom: 12px;
  color: var(--app-text);
  font-size: 14px;
  font-weight: 600;
}
.table-mapping-list {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-width: 0;
}
.table-mapping-item {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 16px minmax(160px, 220px);
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 6px;
  background: var(--app-surface-soft);
  min-height: 42px;
}
.table-mapping-source {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--app-text);
  font-weight: 500;
}
.table-mapping-item :deep(.arco-input-wrapper) {
  width: 100% !important;
  min-width: 0;
}
.table-db-collapse {
  flex: 1;
  overflow-y: auto;
  min-height: 0;
}
.table-db-collapse :deep(.arco-collapse-item-content) {
  padding: 8px 0;
}
.table-list-panel {
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  min-height: 0;
}
.table-list-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 8px;
}
.table-list-item {
  padding: 6px 8px;
  border-radius: 4px;
  background: var(--app-surface-soft);
}
.table-list-item:hover {
  background: var(--app-surface-hover);
}
.table-name-text {
  display: inline-block;
  max-width: 120px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  vertical-align: middle;
}
.table-db-panel-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
.table-db-panel-toolbar :deep(.arco-btn-text) {
  padding-left: 8px;
  padding-right: 8px;
}
.table-checkbox-group {
  width: 100%;
}
.table-selector-footer {
  flex-shrink: 0;
  height: 22px;
  line-height: 22px;
  margin-top: 8px;
  text-align: right;
}
.table-toolbar {
  flex-shrink: 0;
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}
.table-search-input {
  flex: 1;
}
</style>
