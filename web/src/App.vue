<script setup>
import { ref, onMounted, onUnmounted, watch, computed } from "vue";

import { Message, Modal } from "@arco-design/web-vue";

const API_BASE = "/api";
const TASK_SORT_OPTIONS_URL = "/api/tasks/sort-options";
const TASK_SORT_FALLBACK_OPTIONS = [
  {
    value: "created_at_desc",
    label: "创建时间（新 → 旧）",
    default: true,
  },
  {
    value: "created_at_asc",
    label: "创建时间（旧 → 新）",
  },
  {
    value: "name_asc",
    label: "任务名称（A → Z）",
  },
  {
    value: "name_desc",
    label: "任务名称（Z → A）",
  },
  {
    value: "status_asc",
    label: "状态优先（待执行 → 失败）",
  },
  {
    value: "status_desc",
    label: "状态优先（失败 → 待执行）",
  },
  {
    value: "progress_asc",
    label: "进度（低 → 高）",
  },
  {
    value: "progress_desc",
    label: "进度（高 → 低）",
  },
];

const taskSortOptions = ref([...TASK_SORT_FALLBACK_OPTIONS]);
const taskSortDefault = ref("created_at_desc");
const taskSortLabelMap = computed(() => Object.fromEntries(taskSortOptions.value.map((option) => [option.value, option.label])));

async function loadTaskSortOptions() {
  try {
    const res = await fetch(TASK_SORT_OPTIONS_URL);
    if (!res.ok) return;
    const data = await res.json();
    if (Array.isArray(data.options) && data.options.length > 0) {
      taskSortOptions.value = data.options;
      const defaultOption = data.options.find((option) => option.default) || data.options[0];
      if (defaultOption?.value) {
        taskSortDefault.value = defaultOption.value;
      }
    }
  } catch (e) {
    console.warn("加载任务排序选项失败，使用本地回退定义:", e);
  }
}

// 统一错误处理函数

async function handleApiError(response, defaultMsg = "操作失败") {
  try {
    const errData = await response.json();

    if (errData.error) {
      // 解析错误信息

      const errorMsg = errData.error;

      // 如果是详细错误信息，显示完整信息

      if (errorMsg.includes(":")) {
        return `${defaultMsg}: ${errorMsg}`;
      }

      return `${defaultMsg}: ${errorMsg}`;
    }

    return defaultMsg;
  } catch (e) {
    return defaultMsg;
  }
}

// 导航状态

const selectedKey = ref(["tasks"]);

const UI_THEME_STORAGE_KEY = "mysql_to_async_ui_theme";

const uiThemeOptions = [
  { value: "default", label: "默认白色", desc: "接近最初的浅色后台风格" },
  { value: "blue", label: "深蓝科技", desc: "当前青蓝深色主题" },
  { value: "gray", label: "高级灰", desc: "低饱和灰蓝工作台" },
  { value: "black", label: "纯黑", desc: "高对比黑色主题" },
  { value: "dark", label: "暗色", desc: "柔和暗色主题" },
];

const uiTheme = ref("default");
const appThemeClass = computed(() => `theme-${uiTheme.value}`);

function setUiTheme(theme) {
  uiTheme.value = theme;
  localStorage.setItem(UI_THEME_STORAGE_KEY, theme);
}

function syncUiThemeToDocument(theme) {
  if (typeof document !== "undefined") {
    document.documentElement.dataset.uiTheme = theme;
  }
}

// 状态

const tasks = ref([]);

const databases = ref([]);

const tables = ref([]);

const loading = ref(false);

const taskFormPage = ref("none"); // 'none' | 'select_type' | 'create' | 'edit'

// 搜索框状态

const databaseSearchText = ref("");

const tableSearchText = ref("");

const selectedSyncLevel = ref("database");

const selectedDatabases = ref([]); // 库级别同步时选中的源数据库列表

const targetDatabaseMappings = ref([]); // [{source, target}] 源->目标库映射

const targetTableMappings = ref({}); // {qualifiedSourceTable: targetTableName}

const selectedTables = ref([]);

const activeTableSourceDatabase = ref("");

const tableSelectionsByDatabase = ref({});

const tablesByDatabase = ref({});

const loadingTablesByDatabase = ref({});

const expandedTableDatabaseKeys = ref([]);

const expandedTargetTableDatabaseKeys = ref([]);

const editMode = ref(false);

const editingTaskId = ref(null);

// 任务详情抽屉

const detailDrawerVisible = ref(false);

const selectedTaskForDetail = ref(null);

// 任务详情独立标签页（在新标签页中全屏展示任务详情、进度、日志等）

const isTaskDetailPage = ref(false);

const detailPageTaskId = ref("");

const detailPageTask = ref(null);

const detailPageMetrics = ref({});

const detailPageProgress = ref(null); // 任务运行时进度（仅内存，任务运行时才有）

const detailPageLoading = ref(false);

const detailPageActiveTab = ref("runtime");

let detailPageRefreshInterval = null;

let detailPageProgressInterval = null;

// 自定义数据库配置开关

const useCustomSourceDB = ref(false);

const useCustomTargetDB = ref(false);

// 自定义数据库配置

const customSourceDB = ref({
  host: "",

  port: 3306,

  database: "",

  username: "",

  password: "",
});

const customTargetDB = ref({
  host: "",

  port: 3306,

  database: "",

  username: "",

  password: "",
});

const SINK_TYPES = [
  { value: "MYSQL", label: "MySQL 数据库" },
  { value: "KAFKA", label: "Kafka" },
  { value: "HTTP_WEBHOOK", label: "HTTP Webhook" },
];

function getDefaultSinkOptions(type) {
  if (type === "MYSQL") {
    return {
      host: "",
      port: 3306,
      username: "",
      password: "",
      database: "",
      target_schema: "",
      batch_size: 1000,
    };
  } else if (type === "KAFKA") {
    return {
      brokers: "",
      topic: "",
      key_mode: "pk",
      batch_size: 1000,
      required_acks: 1,
    };
  } else if (type === "HTTP_WEBHOOK") {
    return {
      url: "",
      method: "POST",
      headers: "",
      timeout_ms: 5000,
      retry_times: 3,
    };
  }
  return {};
}

const targetType = ref("MYSQL"); // 'MYSQL', 'KAFKA', 'WEBHOOK', 'MULTI'

const singleKafkaConfig = ref(getDefaultSinkOptions("KAFKA"));

const singleWebhookConfig = ref(getDefaultSinkOptions("HTTP_WEBHOOK"));

const sinkConfigs = ref([]);

function addSinkConfig() {
  sinkConfigs.value.push({
    type: "MYSQL",
    options: getDefaultSinkOptions("MYSQL"),
  });
}

function removeSinkConfig(index) {
  sinkConfigs.value.splice(index, 1);
}

function onSinkTypeChange(index) {
  const type = sinkConfigs.value[index].type;
  sinkConfigs.value[index].options = getDefaultSinkOptions(type);
}

function getSinkTypeLabel(type) {
  return SINK_TYPES.find((t) => t.value === type)?.label || type;
}

const taskForm = ref({
  name: "",

  source_schema: "",

  target_schema: "",

  target_database: "",

  tables: [],

  target_tables: [],

  mode: "FULL",

  batch_size: 1000,

  worker_count: 4,

  intra_table_worker_count: 0,

  enable_limit_one: false,

  optimize_index: false,

  enable_read_only: false,

  enable_drop_table_before_ddl: false,
  tx_commit_every_n_parallel: 0,
});

// 刷新状态

const refreshingDatabases = ref(false);

const refreshingTables = ref(false);

// 刷新数据库列表

async function refreshDatabases() {
  refreshingDatabases.value = true;

  try {
    await fetch(`${API_BASE}/metadata/refresh`, { method: "POST" });

    const res = await fetch(`${API_BASE}/metadata/databases`);

    if (res.ok) {
      databases.value = await res.json();
    }
  } catch (e) {
    Message.error("刷新数据库列表失败");

    console.error("刷新数据库列表失败:", e);
  } finally {
    refreshingDatabases.value = false;
  }
}

// 获取数据库列表

async function fetchDatabases() {
  try {
    // 确定使用哪个数据库配置

    let dbConfig = null;

    if (useCustomSourceDB.value) {
      // 开启自定义源数据库：使用自定义配置

      if (customSourceDB.value.host) {
        dbConfig = {
          host: customSourceDB.value.host,

          port: customSourceDB.value.port,

          username: customSourceDB.value.username,

          password: customSourceDB.value.password,

          database: customSourceDB.value.database || "mysql",
        };
      }
    } else {
      // 未开启自定义源数据库：使用配置文件中的源数据库配置

      if (configForm.value.datasource && configForm.value.datasource.host) {
        dbConfig = {
          host: configForm.value.datasource.host,

          port: configForm.value.datasource.port,

          username: configForm.value.datasource.username,

          password: configForm.value.datasource.password,

          database: configForm.value.datasource.database || "mysql",
        };
      }
    }

    // 首先尝试使用默认连接（后端可能在启动时已经根据配置文件建立了连接）

    const defaultRes = await fetch(`${API_BASE}/metadata/databases`);

    if (defaultRes.ok) {
      databases.value = await defaultRes.json();

      return;
    }

    // 如果默认连接失败，且有自定义配置，尝试使用自定义配置连接

    if (dbConfig && dbConfig.host) {
      const res = await fetch(`${API_BASE}/metadata/databases-with-config`, {
        method: "POST",

        headers: { "Content-Type": "application/json" },

        body: JSON.stringify(dbConfig),
      });

      if (res.ok) {
        databases.value = await res.json();

        return;
      } else {
        const errData = await res.json();

        console.error("获取数据库列表失败:", errData.error);

        Message.warning("获取数据库列表失败: " + errData.error);
      }
    } else {
      // 没有配置，提示用户

      const errData = await defaultRes.json();

      console.error("获取数据库列表失败:", errData.error);

      Message.info(
        "请先在系统配置中配置源数据库连接信息，或在高级配置中指定自定义数据库连接",
      );
    }
  } catch (e) {
    console.error("获取数据库列表失败:", e);

    Message.error("获取数据库列表失败: " + e.message);
  }
}

// 测试数据库连接

async function testConnection(dbConfig, type) {
  try {
    const res = await fetch(`${API_BASE}/config/test-connection`, {
      method: "POST",

      headers: { "Content-Type": "application/json" },

      body: JSON.stringify(dbConfig),
    });

    const data = await res.json();

    if (data.success) {
      Message.success(`${type}连接成功: ${data.message}`);
    } else {
      Message.error(`${type}连接失败: ${data.message}`);
    }

    return data.success;
  } catch (e) {
    Message.error(`${type}连接测试失败: ${e.message}`);

    return false;
  }
}

// 测试源数据库连接

async function testSourceConnection() {
  return await testConnection(customSourceDB.value, "源数据库");
}

// 测试目标数据库连接

async function testTargetConnection() {
  return await testConnection(customTargetDB.value, "目标数据库");
}

// 保存源数据库配置到配置文件

async function saveSourceConfig() {
  if (!customSourceDB.value.host) {
    Message.warning("请先填写源数据库配置");

    return;
  }

  configForm.value.datasource = {
    ...configForm.value.datasource,

    host: customSourceDB.value.host,

    port: customSourceDB.value.port,

    database: customSourceDB.value.database,

    username: customSourceDB.value.username,

    password: customSourceDB.value.password,
  };

  await saveConfig();

  // 重新获取数据库列表

  fetchDatabases();
}

// 保存目标数据库配置到配置文件

async function saveTargetConfig() {
  if (!customTargetDB.value.host) {
    Message.warning("请先填写目标数据库配置");

    return;
  }

  configForm.value.target = {
    ...configForm.value.target,

    host: customTargetDB.value.host,

    port: customTargetDB.value.port,

    database: customTargetDB.value.database,

    username: customTargetDB.value.username,

    password: customTargetDB.value.password,
  };

  await saveConfig();
}

// 刷新表列表

async function refreshTables() {
  const databasesToRefresh =
    selectedSyncLevel.value === "table" && selectedDatabases.value.length > 0
      ? expandedTableDatabaseKeys.value.length > 0
        ? expandedTableDatabaseKeys.value
        : selectedDatabases.value
      : [
          activeTableSourceDatabase.value || taskForm.value.source_schema,
        ].filter(Boolean);

  if (databasesToRefresh.length === 0) {
    Message.warning("请先选择源数据库");
    return;
  }

  refreshingTables.value = true;

  try {
    await fetch(`${API_BASE}/metadata/refresh`, { method: "POST" });

    await Promise.all(
      databasesToRefresh.map((database) => fetchTablesForDatabase(database)),
    );
  } catch (e) {
    Message.error("刷新表列表失败");
    console.error("刷新表列表失败:", e);
  } finally {
    refreshingTables.value = false;
  }
}

// 获取指定源库的表列表

async function fetchTablesForDatabase(database) {
  if (!database) {
    return;
  }

  loadingTablesByDatabase.value = {
    ...loadingTablesByDatabase.value,
    [database]: true,
  };

  try {
    let dbConfig = null;

    if (useCustomSourceDB.value) {
      if (customSourceDB.value.host) {
        dbConfig = {
          host: customSourceDB.value.host,
          port: customSourceDB.value.port,
          username: customSourceDB.value.username,
          password: customSourceDB.value.password,
          database: customSourceDB.value.database || database,
        };
      }
    } else if (configForm.value.datasource && configForm.value.datasource.host) {
      dbConfig = {
        host: configForm.value.datasource.host,
        port: configForm.value.datasource.port,
        username: configForm.value.datasource.username,
        password: configForm.value.datasource.password,
        database: configForm.value.datasource.database || database,
      };
    }

    let res;

    if (dbConfig && dbConfig.host) {
      res = await fetch(`${API_BASE}/metadata/tables-with-config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...dbConfig,
          schema: database,
        }),
      });
    } else {
      res = await fetch(`${API_BASE}/metadata/tables?schema=${database}`);
    }

    if (res.ok) {
      const tableList = await res.json();
      tablesByDatabase.value = {
        ...tablesByDatabase.value,
        [database]: tableList,
      };

      if (activeTableSourceDatabase.value === database) {
        tables.value = tableList;
      }
    } else {
      const errText = await res.text();

      try {
        const errData = JSON.parse(errText);
        Message.error(`获取表列表失败: ${errData.error || errText}`);
      } catch {
        Message.error(`获取表列表失败: ${errText}`);
      }
    }
  } catch (e) {
    console.error("获取表列表失败:", e);
    Message.error(`获取表列表失败: ${e.message}`);
  } finally {
    loadingTablesByDatabase.value = {
      ...loadingTablesByDatabase.value,
      [database]: false,
    };
  }
}

// 获取表列表

async function fetchTables() {
  const currentSourceSchema =
    activeTableSourceDatabase.value || taskForm.value.source_schema;

  if (!currentSourceSchema) {
    return;
  }

  activeTableSourceDatabase.value = currentSourceSchema;
  taskForm.value.source_schema = currentSourceSchema;
  await fetchTablesForDatabase(currentSourceSchema);
}

// 获取任务列表

let taskFetchSeq = 0;

async function fetchTasks(page = taskPagination.value.current, pageSize = taskPagination.value.pageSize) {
  const fetchSeq = ++taskFetchSeq;
  try {
    const params = new URLSearchParams({
      page: String(page),
      page_size: String(pageSize),
    });

    if (taskFilters.value.status) {
      params.set("status", taskFilters.value.status);
    }

    if (taskFilters.value.keyword) {
      params.set("keyword", taskFilters.value.keyword);
    }

    if (taskFilters.value.sort) {
      params.set("sort", taskFilters.value.sort);
    }

    const res = await fetch(`${API_BASE}/tasks?${params.toString()}`);

    if (res.ok) {
      const data = await res.json();
      if (fetchSeq !== taskFetchSeq) return;
      tasks.value = data.items || [];
      taskPagination.value.current = data.page || page;
      taskPagination.value.pageSize = data.page_size || pageSize;
      taskPagination.value.total = data.total || 0;
      syncTaskFiltersToUrl();
    }
  } catch (e) {
    console.error("获取任务列表失败:", e);
  }
}

// 关闭任务表单页

function closeTaskForm() {
  taskFormPage.value = "none";

  resetForm();

  window.history.pushState({}, "", "#/tasks");
}

// 打开创建页

function openCreateDialog() {
  resetForm();

  taskFormPage.value = "select_type";

  window.history.pushState(
    { taskForm: "select_type" },
    "",
    "#/tasks/new/select",
  );
}

// 从选择类型页进入具体的创建页
function proceedToCreateTask(type) {
  targetType.value = type;
  taskFormPage.value = "create";
  window.history.pushState({ taskForm: "create" }, "", "#/tasks/new/config");
}

// 重置表单

function resetForm() {
  taskForm.value = {
    name: "",

    source_schema: "",

    target_schema: "",

    target_database: "",

    tables: [],

    target_tables: [],

    mode: "FULL",

    batch_size: 1000,

    worker_count: 4,

    intra_table_worker_count: 0,

    enable_limit_one: false,

    optimize_index: false,

    index_restore_worker_count: 0,

    enable_read_only: false,

    enable_drop_table_before_ddl: false,

    tx_commit_every_n_parallel: 0,
  };

  selectedSyncLevel.value = "database";

  selectedDatabases.value = [];

  targetDatabaseMappings.value = [];

  targetTableMappings.value = {};

  selectedTables.value = [];

  taskForm.value.target_database = "";

  taskForm.value.target_tables = [];

  activeTableSourceDatabase.value = "";

  tableSelectionsByDatabase.value = {};

  tablesByDatabase.value = {};

  loadingTablesByDatabase.value = {};

  expandedTableDatabaseKeys.value = [];

  expandedTargetTableDatabaseKeys.value = [];

  editMode.value = false;

  editingTaskId.value = null;

  useCustomSourceDB.value = false;

  useCustomTargetDB.value = false;

  customSourceDB.value = {
    host: "",
    port: 3306,
    database: "",
    username: "",
    password: "",
  };

  customTargetDB.value = {
    host: "",
    port: 3306,
    database: "",
    username: "",
    password: "",
  };

  targetType.value = "";

  singleKafkaConfig.value = {};

  singleWebhookConfig.value = {};
}

function getQualifiedTableName(database, tableName) {
  return `${database}.${tableName}`;
}

function resolveTargetDatabaseName(mapping) {
  const mapped = String(mapping?.target || "").trim();
  const defaultTarget = String(taskForm.value.target_database || "").trim();

  if (mapped && mapped !== mapping.source) {
    return mapped;
  }

  if (defaultTarget) {
    return defaultTarget;
  }

  return mapped || mapping.source;
}

function buildTargetDatabasesPayload() {
  return targetDatabaseMappings.value.map((mapping) =>
    resolveTargetDatabaseName(mapping),
  );
}

function getTaskDatabaseMappings(task) {
  if (!task?.config) {
    return [];
  }

  const cfg = task.config;
  const srcDbs =
    cfg.source_databases?.length > 0
      ? cfg.source_databases
      : cfg.source_schema
        ? [cfg.source_schema]
        : [];
  const dstDbs = cfg.target_databases || [];
  const defaultTarget = cfg.target_database || cfg.target_schema || "";

  return srcDbs.map((src, i) => {
    const stored = dstDbs[i];
    if (stored && stored !== src) {
      return { source: src, target: stored };
    }
    if (defaultTarget && defaultTarget !== src) {
      return { source: src, target: defaultTarget };
    }
    return { source: src, target: stored || defaultTarget || src };
  });
}

function parseQualifiedTableName(qualifiedName) {
  const parts = String(qualifiedName || "").split(".");

  if (parts.length < 2) {
    return { database: "", table: String(qualifiedName || "") };
  }

  return {
    database: parts.shift(),

    table: parts.join("."),
  };
}

function ensureTableSelectionBucket(database) {
  if (!database) {
    return;
  }

  if (!Array.isArray(tableSelectionsByDatabase.value[database])) {
    tableSelectionsByDatabase.value[database] = [];
  }
}

function updateDatabaseTableSelection(db, newValue) {
  ensureTableSelectionBucket(db);
  tableSelectionsByDatabase.value[db] = [...newValue];

  selectedTables.value = selectedDatabases.value.flatMap((database) => {
    const tableNames = tableSelectionsByDatabase.value[database] || [];
    return tableNames.map((tableName) =>
      getQualifiedTableName(database, tableName),
    );
  });
}

function getFilteredTablesForDatabase(database) {
  const list = tablesByDatabase.value[database] || [];

  if (!tableSearchText.value) {
    return list;
  }

  const searchText = tableSearchText.value.toLowerCase();

  return list.filter((table) =>
    table.table_name.toLowerCase().includes(searchText),
  );
}

function onTableDatabaseAccordionChange(activeKeys) {
  const keys = Array.isArray(activeKeys) ? activeKeys : [activeKeys];
  expandedTableDatabaseKeys.value = keys;

  keys.forEach((db) => {
    if (!tablesByDatabase.value[db]) {
      fetchTablesForDatabase(db);
    }
  });
}

function toggleAllTablesForDatabase(database) {
  if (!database) {
    return;
  }

  const filtered = getFilteredTablesForDatabase(database);
  const currentSelection = tableSelectionsByDatabase.value[database] || [];

  if (
    currentSelection.length === filtered.length &&
    filtered.length > 0
  ) {
    updateDatabaseTableSelection(database, []);
  } else {
    updateDatabaseTableSelection(
      database,
      filtered.map((table) => table.table_name),
    );
  }
}

function onTableSourceDatabasesChange(databases) {
  selectedDatabases.value = databases;

  if (databases.length === 0) {
    activeTableSourceDatabase.value = "";
    expandedTableDatabaseKeys.value = [];
    tables.value = [];
    tableSearchText.value = "";
    return;
  }

  if (!databases.includes(activeTableSourceDatabase.value)) {
    activeTableSourceDatabase.value = databases[0];
  }

  taskForm.value.source_schema = activeTableSourceDatabase.value || "";

  const expanded = expandedTableDatabaseKeys.value.filter((db) =>
    databases.includes(db),
  );

  if (expanded.length === 0) {
    expandedTableDatabaseKeys.value = [databases[0]];
    fetchTablesForDatabase(databases[0]);
  } else {
    expandedTableDatabaseKeys.value = expanded;
    expanded.forEach((db) => {
      if (!tablesByDatabase.value[db]) {
        fetchTablesForDatabase(db);
      }
    });
  }
}

function onActiveTableSourceDatabaseChange() {
  taskForm.value.source_schema = activeTableSourceDatabase.value || "";

  tableSearchText.value = "";

  fetchTables();
}

// 全选/取消全选表

function toggleAllTables() {
  const currentDb = activeTableSourceDatabase.value;

  if (!currentDb) {
    return;
  }

  const currentSelection = currentDatabaseSelectedTables.value;

  if (currentSelection.length === filteredTables.value.length) {
    currentDatabaseSelectedTables.value = [];
  } else {
    currentDatabaseSelectedTables.value = filteredTables.value.map(
      (t) => t.table_name,
    );
  }
}

// 创建任务

async function createTask() {
  if (!taskForm.value.name) {
    Message.warning("请输入任务名称");

    return;
  }

  if (selectedSyncLevel.value === "database") {
    if (selectedDatabases.value.length === 0) {
      Message.warning("请至少选择一个源数据库");

      return;
    }
  } else {
    if (selectedDatabases.value.length === 0) {
      Message.warning("请至少选择一个源数据库");

      return;
    }

    if (totalSelectedTables.value === 0) {
      Message.warning("请至少选择一个表");

      return;
    }

    for (const db of selectedDatabases.value) {
      const mapping = targetDatabaseMappings.value.find(
        (item) => item.source === db,
      );

      if (!String(mapping?.target || taskForm.value.target_database || "").trim()) {
        Message.warning(`请为源库 ${db} 配置目标库`);

        return;
      }
    }
  }

  // 构建 payload

  let tablesPayload = [];

  let targetTablesPayload = [];

  let sourceDatabasesPayload = [];

  const sourceTableNames = [];

  let targetDatabasesPayload = [];

  let sourceSchemaPayload = taskForm.value.source_schema;

  let targetSchemaPayload = taskForm.value.target_schema;

  let targetDatabasePayload = taskForm.value.target_database;

  if (selectedSyncLevel.value === "database") {
    sourceDatabasesPayload = selectedDatabases.value;
    targetDatabasesPayload = buildTargetDatabasesPayload();

    sourceSchemaPayload = "";
    targetSchemaPayload = "";
    targetDatabasePayload = taskForm.value.target_database || "";
  } else {
    sourceDatabasesPayload = selectedDatabases.value;
    targetDatabasesPayload = buildTargetDatabasesPayload();

    sourceSchemaPayload = selectedDatabases.value[0] || "";
    targetSchemaPayload = targetDatabasesPayload[0] || "";
    targetDatabasePayload =
      targetDatabasesPayload[0] || taskForm.value.target_database || "";

    tablesPayload = selectedDatabases.value.flatMap((db) => {
      const tableNames = tableSelectionsByDatabase.value[db] || [];

      return tableNames.map((tableName) => getQualifiedTableName(db, tableName));
    });

    targetTablesPayload = selectedDatabases.value.flatMap((db) => {
      const tableNames = tableSelectionsByDatabase.value[db] || [];

      return tableNames.map((tableName) => {
        const sourceQualifiedName = getQualifiedTableName(db, tableName);
        return targetTableMappings.value[sourceQualifiedName] || tableName;
      });
    });

    selectedTables.value = [...tablesPayload];
  }

  // 构建 sink_configs payload
  let sinkConfigsPayload = null;
  if (targetType.value === "KAFKA") {
    const opts = { ...singleKafkaConfig.value };
    if (typeof opts.brokers === "string") {
      opts.brokers = opts.brokers
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    }
    sinkConfigsPayload = [{ type: "KAFKA", options: opts }];
  } else if (targetType.value === "WEBHOOK") {
    const opts = { ...singleWebhookConfig.value };
    if (typeof opts.headers === "string") {
      const headerMap = {};
      opts.headers.split("\n").forEach((line) => {
        const idx = line.indexOf(":");
        if (idx > 0) {
          headerMap[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
        }
      });
      opts.headers = Object.keys(headerMap).length > 0 ? headerMap : undefined;
    }
    sinkConfigsPayload = [{ type: "HTTP_WEBHOOK", options: opts }];
  } else if (targetType.value === "MULTI") {
    if (sinkConfigs.value.length > 0) {
      sinkConfigsPayload = sinkConfigs.value.map((sc) => {
        const opts = { ...sc.options };
        // Kafka: brokers 从逗号分隔字符串转为数组
        if (sc.type === "KAFKA" && typeof opts.brokers === "string") {
          opts.brokers = opts.brokers
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
        }
        // Webhook: headers 从 "Key:Value\nKey2:Value2" 转为对象
        if (sc.type === "HTTP_WEBHOOK" && typeof opts.headers === "string") {
          const headerMap = {};
          opts.headers.split("\n").forEach((line) => {
            const idx = line.indexOf(":");
            if (idx > 0) {
              headerMap[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
            }
          });
          opts.headers =
            Object.keys(headerMap).length > 0 ? headerMap : undefined;
        }
        return { type: sc.type, options: opts };
      });
    }
  }

  const payload = {
    ...taskForm.value,

    source_schema: sourceSchemaPayload,

    target_schema: targetSchemaPayload,

    sync_level: selectedSyncLevel.value,

    tables: tablesPayload,

    source_databases: sourceDatabasesPayload,

    target_databases: targetDatabasesPayload,

    target_database: targetDatabasePayload,

    target_tables: targetTablesPayload,

    source_db: useCustomSourceDB.value ? customSourceDB.value : null,

    target_db: useCustomTargetDB.value ? customTargetDB.value : null,

    sink_configs: sinkConfigsPayload,
  };

  loading.value = true;

  try {
    const url = editMode.value
      ? `${API_BASE}/tasks/${editingTaskId.value}`
      : `${API_BASE}/tasks`;

    const method = editMode.value ? "PUT" : "POST";

    const res = await fetch(url, {
      method,

      headers: { "Content-Type": "application/json" },

      body: JSON.stringify(payload),
    });

    if (res.ok) {
      closeTaskForm();

      fetchTasks();

      Message.success(editMode.value ? "更新成功" : "创建成功");
    } else {
      // 尝试解析错误信息

      try {
        const text = await res.text();

        if (text) {
          try {
            const err = JSON.parse(text);

            Message.error(
              (editMode.value ? "更新" : "创建") +
                "失败: " +
                (err.error || text),
            );
          } catch {
            // 不是JSON，直接显示文本

            Message.error((editMode.value ? "更新" : "创建") + "失败: " + text);
          }
        } else {
          Message.error(
            (editMode.value ? "更新" : "创建") + "失败: 服务器返回空响应",
          );
        }
      } catch (e) {
        Message.error(
          (editMode.value ? "更新" : "创建") + "失败: " + e.message,
        );
      }
    }
  } catch (e) {
    Message.error((editMode.value ? "更新" : "创建") + "失败: " + e.message);
  } finally {
    loading.value = false;
  }
}

// 启动任务

const startModalVisible = ref(false);
const startTaskId = ref("");
const startMode = ref("immediate");
const scheduleCron = ref("0 9 * * 1-5");
const scheduleTimezone = ref(Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Shanghai");

function openStartTaskModal(taskId, mode = "immediate") {
  startTaskId.value = taskId;
  startMode.value = mode;
  scheduleCron.value = "0 9 * * 1-5";
  scheduleTimezone.value = Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Shanghai";
  startModalVisible.value = true;
}

async function confirmStartTask() {
  try {
    let payload = {};
    let successMsg = "任务已启动";
    let failMsg = "启动失败";

    if (startMode.value === "cron") {
      const expr = String(scheduleCron.value || "").trim();
      if (!expr) {
        Message.error("请输入 cron 表达式");
        return;
      }
      payload = {
        scheduled_at: new Date().toISOString(),
        schedule_mode: "cron",
        cron_expression: expr,
        cron_timezone: String(scheduleTimezone.value || "").trim(),
      };
      successMsg = "定时启动已设置";
      failMsg = "设置定时启动失败";
    }

    const res = await fetch(`${API_BASE}/tasks/${startTaskId.value}/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: Object.keys(payload).length ? JSON.stringify(payload) : undefined,
    });

    if (res.ok) {
      fetchTasks();
      Message.success(successMsg);
      startModalVisible.value = false;
      refreshDetailPage();
    } else {
      const errorMsg = await handleApiError(res, failMsg);
      Message.error(errorMsg);
    }
  } catch (e) {
    Message.error((startMode.value === "cron" ? "设置定时启动失败" : "启动失败") + ": " + e.message);
  }
}

function formatScheduledTime(task) {
  if (task.context.scheduled_at) {
    const d = new Date(task.context.scheduled_at);
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
  return "";
}

// 取消定时启动

async function cancelSchedule(taskId) {
  try {
    const res = await fetch(`${API_BASE}/tasks/${taskId}/cancel-schedule`, {
      method: "POST",
    });

    if (res.ok) {
      fetchTasks();
      Message.success("已取消定时启动");
    } else {
      const errorMsg = await handleApiError(res, "取消定时失败");
      Message.error(errorMsg);
    }
  } catch (e) {
    Message.error("取消定时失败: " + e.message);
  }
}

// 暂停任务

async function pauseTask(taskId) {
  try {
    const res = await fetch(`${API_BASE}/tasks/${taskId}/pause`, {
      method: "POST",
    });

    if (res.ok) {
      fetchTasks();

      Message.success("任务已暂停");
    } else {
      const errorMsg = await handleApiError(res, "暂停失败");

      Message.error(errorMsg);
    }
  } catch (e) {
    Message.error("暂停失败: " + e.message);
  }
}

// 删除任务

async function deleteTask(taskId) {
  Modal.confirm({
    title: "确认删除",

    content: "确定要删除这个任务吗？",

    okText: "删除",

    cancelText: "取消",

    onOk: async () => {
      try {
        const res = await fetch(`${API_BASE}/tasks/${taskId}`, {
          method: "DELETE",
        });

        if (res.ok) {
          fetchTasks();

          Message.success("删除成功");
        } else {
          const errorMsg = await handleApiError(res, "删除失败");

          Message.error(errorMsg);
        }
      } catch (e) {
        Message.error("删除失败: " + e.message);
      }
    },
  });
}

// 显示任务详情：在新标签页中打开，展示更完整的进度/日志/表进度等信息

function showTaskDetail(task) {
  const url = new URL(window.location.href);

  url.search = "";

  url.hash = `#/task-detail/${task.config.id}`;

  window.open(url.toString(), "_blank");
}

// 同步阶段文案

function syncPhaseText(phase) {
  const map = {
    "": "未开始",

    FULL_STARTED: "全量进行中",

    FULL_COMPLETED: "全量已完成",

    FULL_FAILED: "全量失败",

    INCREMENTAL_STARTED: "增量进行中",
  };

  return map[phase] ?? (phase || "未开始");
}

// 历史全量断点表进度列表

function resumeTableList(task) {
  const resume = task?.context?.full_sync_resume;

  if (!resume || typeof resume !== "object") return [];

  return Object.entries(resume).map(([key, p]) => ({
    key,

    done: !!p.done,

    readPath: p.read_path || "-",

    processedRows: p.processed_rows || 0,

    intraWorkers: p.intra_workers || 0,
  }));
}

// 加载任务详情页数据（任务详情 + 指标）

async function fetchTaskDetailPage(taskId) {
  if (!taskId) return;

  detailPageLoading.value = true;

  try {
    const [taskRes, metricsRes] = await Promise.allSettled([
      fetch(`${API_BASE}/tasks/${taskId}`),

      fetch(`${API_BASE}/tasks/${taskId}/metrics`),
    ]);

    if (taskRes.status === "fulfilled") {
      if (taskRes.value.ok) {
        detailPageTask.value = await taskRes.value.json();
      } else if (taskRes.value.status === 404) {
        detailPageTask.value = null;
      }
    }

    if (metricsRes.status === "fulfilled" && metricsRes.value.ok) {
      detailPageMetrics.value = await metricsRes.value.json();
    }

    // 运行时进度接口仅任务运行时有效（404 表示任务未运行）
    await fetchTaskDetailProgress(taskId);
  } catch (e) {
    console.error("加载任务详情失败:", e);
  } finally {
    detailPageLoading.value = false;
  }
}

// 获取任务运行时进度（仅内存，任务运行时返回数据，否则 404）

async function fetchTaskDetailProgress(taskId) {
  if (!taskId) return;
  try {
    const res = await fetch(`${API_BASE}/tasks/${taskId}/progress`);
    if (res.ok) {
      detailPageProgress.value = await res.json();
    } else if (res.status === 404) {
      // 任务未运行，进度数据被清除
      detailPageProgress.value = null;
    }
  } catch (e) {
    // 进度接口失败时静默处理，不影响主详情展示
  }
}

// 刷新任务详情页

function refreshDetailPage() {
  if (isTaskDetailPage.value && detailPageTaskId.value) {
    fetchTaskDetailPage(detailPageTaskId.value);
  }
}

// 关闭任务详情标签页

function closeTaskDetailPage() {
  window.close();

  // 兜底：若 window.close() 无效（非脚本打开的标签页），回到主页

  const url = new URL(window.location.href);

  url.hash = "";

  url.search = "";

  window.location.href = url.toString();
}

// 详情页：暂停任务后刷新

async function detailPagePause() {
  await pauseTask(detailPageTaskId.value);

  refreshDetailPage();
}

// 详情页：取消定时后刷新

async function detailPageCancelSchedule() {
  await cancelSchedule(detailPageTaskId.value);

  refreshDetailPage();
}

// 将已有任务配置写入表单（创建副本与编辑共用）

function fillTaskFormFromTask(task) {
  taskForm.value = {
    name: task.config.name,

    source_schema: task.config.source_schema,

    target_schema: task.config.target_schema,

    target_database: task.config.target_database || "",

    tables: task.config.tables || [],

    target_tables: task.config.target_tables || [],

    mode: task.config.mode,

    batch_size: task.config.batch_size,

    worker_count: task.config.worker_count,

    intra_table_worker_count: task.config.intra_table_worker_count ?? 0,

    enable_limit_one: task.config.enable_limit_one,

    optimize_index: task.config.optimize_index || false,

    index_restore_worker_count: task.config.index_restore_worker_count ?? 0,

    enable_read_only: task.config.enable_read_only || false,

    enable_drop_table_before_ddl: task.config.enable_drop_table_before_ddl || false,
    tx_commit_every_n_parallel: task.config.tx_commit_every_n_parallel ?? 0,
  };

  if (task.config.sync_level === "DATABASE") {
    selectedSyncLevel.value = "database";

    const srcDbs = task.config.source_databases || [];

    const dstDbs = task.config.target_databases || [];

    selectedDatabases.value = srcDbs;

    targetDatabaseMappings.value = srcDbs.map((db, i) => ({
      source: db,

      target: dstDbs[i] || db,
    }));
  } else {
    selectedSyncLevel.value = "table";

    const sourceDatabases =
      task.config.source_databases && task.config.source_databases.length > 0
        ? task.config.source_databases
        : task.config.source_schema
          ? [task.config.source_schema]
          : [];

    selectedDatabases.value = sourceDatabases;

    targetDatabaseMappings.value = sourceDatabases.map((db, i) => ({
      source: db,

      target:
        (task.config.target_databases && task.config.target_databases[i]) ||
        task.config.target_schema ||
        db,
    }));

    tableSelectionsByDatabase.value = {};

    for (const db of sourceDatabases) {
      ensureTableSelectionBucket(db);
    }

    const rawTables = task.config.tables || [];
    const rawTargetTables = task.config.target_tables || [];

    selectedTables.value = [...rawTables];

    for (let i = 0; i < rawTables.length; i += 1) {
      const table = rawTables[i];
      const parsed = parseQualifiedTableName(table);
      const targetTable = rawTargetTables[i] || parsed.table;

      if (parsed.database && sourceDatabases.includes(parsed.database)) {
        ensureTableSelectionBucket(parsed.database);

        if (
          !tableSelectionsByDatabase.value[parsed.database].includes(
            parsed.table,
          )
        ) {
          tableSelectionsByDatabase.value[parsed.database].push(parsed.table);
        }
        targetTableMappings.value[table] = targetTable;
      } else if (sourceDatabases.length > 0 && parsed.table) {
        const fallbackDb = sourceDatabases[0];

        ensureTableSelectionBucket(fallbackDb);

        if (
          !tableSelectionsByDatabase.value[fallbackDb].includes(parsed.table)
        ) {
          tableSelectionsByDatabase.value[fallbackDb].push(parsed.table);
        }
        targetTableMappings.value[getQualifiedTableName(fallbackDb, parsed.table)] = targetTable;
      }
    }

    activeTableSourceDatabase.value = sourceDatabases[0] || "";

    taskForm.value.source_schema = activeTableSourceDatabase.value || "";

    const dbsWithTables = sourceDatabases.filter(
      (db) => (tableSelectionsByDatabase.value[db] || []).length > 0,
    );

    expandedTableDatabaseKeys.value =
      dbsWithTables.length > 0
        ? dbsWithTables
        : sourceDatabases.slice(0, 1);

    expandedTargetTableDatabaseKeys.value = dbsWithTables.length
      ? dbsWithTables
      : sourceDatabases.slice(0, 1);

    expandedTableDatabaseKeys.value.forEach((db) => {
      fetchTablesForDatabase(db);
    });
  }

  if (task.config.source_db) {
    useCustomSourceDB.value = true;

    customSourceDB.value = {
      host: task.config.source_db.host,

      port: task.config.source_db.port,

      database: task.config.source_db.database,

      username: task.config.source_db.username,

      password: task.config.source_db.password,
    };
  }

  if (task.config.target_db) {
    useCustomTargetDB.value = true;

    customTargetDB.value = {
      host: task.config.target_db.host,

      port: task.config.target_db.port,

      database: task.config.target_db.database,

      username: task.config.target_db.username,

      password: task.config.target_db.password,
    };
  }

  // 回填 sink_configs

  if (task.config.sink_configs && task.config.sink_configs.length > 0) {
    if (
      task.config.sink_configs.length === 1 &&
      task.config.sink_configs[0].type !== "MYSQL"
    ) {
      const sc = task.config.sink_configs[0];

      targetType.value = sc.type;

      const opts = { ...(sc.options || {}) };

      if (sc.type === "KAFKA") {
        if (Array.isArray(opts.brokers)) {
          opts.brokers = opts.brokers.join(", ");
        }

        singleKafkaConfig.value = opts;
      } else if (sc.type === "HTTP_WEBHOOK") {
        if (opts.headers && typeof opts.headers === "object") {
          opts.headers = Object.entries(opts.headers)
            .map(([k, v]) => `${k}: ${v}`)
            .join("\n");
        }

        singleWebhookConfig.value = opts;
      }

      sinkConfigs.value = [];
    } else {
      targetType.value = "MULTI";

      sinkConfigs.value = task.config.sink_configs.map((sc) => {
        const opts = { ...(sc.options || {}) };
        // Kafka: brokers 数组转为逗号分隔字符串供编辑
        if (sc.type === "KAFKA" && Array.isArray(opts.brokers)) {
          opts.brokers = opts.brokers.join(", ");
        }
        // Webhook: headers 对象转为 "Key: Value\n" 供编辑
        if (
          sc.type === "HTTP_WEBHOOK" &&
          opts.headers &&
          typeof opts.headers === "object"
        ) {
          opts.headers = Object.entries(opts.headers)
            .map(([k, v]) => `${k}: ${v}`)
            .join("\n");
        }
        return { type: sc.type, options: opts };
      });
    }
  } else {
    targetType.value = "MYSQL";
    sinkConfigs.value = [];
  }
}

// 打开编辑对话框

function openEditDialog(task) {
  resetForm();

  editMode.value = true;

  editingTaskId.value = task.config.id;

  fillTaskFormFromTask(task);

  taskFormPage.value = "edit";

  window.history.pushState(
    { taskForm: "edit" },
    "",
    `#/tasks/${editingTaskId.value}/edit`,
  );
}

// 从历史任务复制配置，新建一条任务（新 ID 由后端生成）

function openDuplicateFromTask(task) {
  resetForm();

  editMode.value = false;

  editingTaskId.value = null;

  fillTaskFormFromTask(task);

  const base = (task.config.name || "同步任务").trim();

  const suffix = "（副本）";

  taskForm.value.name = base.endsWith(suffix)
    ? `${base}_${Date.now()}`
    : `${base}${suffix}`;

  taskFormPage.value = "create";

  window.history.pushState({ taskForm: "create" }, "", "#/tasks/new");

  Message.success("已载入该任务配置，请检查后点击「创建」");
}

// 格式化时间

function formatTime(time) {
  if (!time) return "-";

  return new Date(time).toLocaleString("zh-CN");
}

// 计算运行时长

function calculateDuration(startTime, endTime) {
  if (!startTime) return "-";

  const start = new Date(startTime);

  if (isNaN(start.getTime()) || start.getFullYear() < 2000) return "-";

  // Go 零值 time.Time{} 序列化为 "0001-01-01T..."，year < 2000 时视为无效结束时间

  const endDate = endTime ? new Date(endTime) : null;

  const end = endDate && endDate.getFullYear() >= 2000 ? endDate : new Date();

  const diff = Math.floor((end - start) / 1000);

  if (diff < 0) return "-";

  if (diff < 60) return `${diff}秒`;

  if (diff < 3600) return `${Math.floor(diff / 60)}分${diff % 60}秒`;

  const hours = Math.floor(diff / 3600);

  const minutes = Math.floor((diff % 3600) / 60);

  return `${hours}小时${minutes}分`;
}

// 格式化同步速度（行/秒）

function formatSpeed(rowsPerSec) {
  if (rowsPerSec == null || rowsPerSec <= 0) return "0";
  if (rowsPerSec >= 10000) return `${(rowsPerSec / 1000).toFixed(1)}k`;
  if (rowsPerSec >= 1000) return `${rowsPerSec.toFixed(0)}`;
  return `${rowsPerSec.toFixed(1)}`;
}

// 格式化秒数为可读时长（-1 表示无法估算）

function formatSeconds(seconds) {
  if (seconds == null) return "-";
  if (seconds === -1) return "无法估算";
  if (seconds < 0) return "-";
  if (seconds < 60) return `${seconds.toFixed(0)}秒`;
  if (seconds < 3600)
    return `${Math.floor(seconds / 60)}分${Math.floor(seconds % 60)}秒`;
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return `${hours}小时${minutes}分`;
}

// 运行时表进度状态文案与颜色

function runtimeStatusColor(status) {
  const map = {
    pending: "gray",
    running: "blue",
    completed: "green",
    failed: "red",
  };
  return map[status] || "gray";
}

function runtimeStatusText(status) {
  const map = {
    pending: "待同步",
    running: "同步中",
    completed: "已完成",
    failed: "失败",
  };
  return map[status] || status;
}

// 获取状态颜色

function getStatusColor(status) {
  const colors = {
    PENDING: "gray",

    RUNNING: "blue",

    PAUSED: "orange",

    COMPLETED: "green",

    FAILED: "red",

    SCHEDULED: "arcoblue",
  };

  return colors[status] || "gray";
}

// 获取状态文本

function getStatusText(status) {
  const texts = {
    PENDING: "待执行",

    RUNNING: "执行中",

    PAUSED: "已暂停",

    COMPLETED: "已完成",

    FAILED: "失败",

    SCHEDULED: "定时中",
  };

  return texts[status] || status;
}

// 计算进度（0-100，供文案展示）

function getProgress(task) {
  if (!task?.context) return 0;

  const ctx = task.context;

  // 已完成任务以服务端进度为准，避免估算总行数与实处理行数不一致
  if (ctx.status === "COMPLETED") {
    return 100;
  }

  if (ctx.progress_percent != null && ctx.progress_percent >= 0) {
    const fromServer = Math.min(100, Math.max(0, ctx.progress_percent));
    if (!ctx.total_rows || ctx.total_rows <= 0) {
      return Number(fromServer.toFixed(2));
    }
  }

  // 防止除零错误
  if (!ctx.total_rows || ctx.total_rows <= 0) {
    return 0;
  }

  // 防止 processed_rows 为负数或异常值
  const processed = Math.max(0, ctx.processed_rows || 0);

  // 计算百分比，并限制在 0-100 之间，保留两位小数
  let percent = (processed / ctx.total_rows) * 100;
  percent = Math.min(100, Math.max(0, percent));

  return Number(percent.toFixed(2));
}

// Arco Progress 的 percent 取 0-1，与 getProgress 的 0-100 文案值分离
function getProgressRatio(value) {
  const pct = typeof value === "number" ? value : getProgress(value);
  return Math.min(1, Math.max(0, pct / 100));
}

// 已完成任务的总行数可能与估算值不一致，展示时以实处理行数为准
function getRowCounts(task) {
  const ctx = task?.context;
  if (!ctx) return { processed: 0, total: 0 };
  const processed = Math.max(0, ctx.processed_rows || 0);
  if (ctx.status === "COMPLETED" && processed > 0) {
    return { processed, total: processed };
  }
  return { processed, total: Math.max(0, ctx.total_rows || 0) };
}

// 监听同步级别变化

function onSyncLevelChange() {
  selectedDatabases.value = [];

  targetDatabaseMappings.value = [];

  selectedTables.value = [];

  activeTableSourceDatabase.value = "";

  tableSelectionsByDatabase.value = {};

  tablesByDatabase.value = {};

  loadingTablesByDatabase.value = {};

  expandedTableDatabaseKeys.value = [];

  expandedTargetTableDatabaseKeys.value = [];

  tableSearchText.value = "";

  taskForm.value.source_schema = "";

  taskForm.value.target_schema = "";

  // 注意：不在这里调用 fetchTables()，因为此时 source_schema 可能还没有值

  // 表列表会在用户选择源数据库后通过 onTableSourceDatabasesChange() 加载
}

// 系统配置状态

const configForm = ref({
  http: { host: "", port: 8080 },

  datasource: {
    host: "",
    port: 3306,
    database: "",
    username: "",
    password: "",
    debug: false,
  },

  target: { host: "", port: 3306, database: "", username: "", password: "" },

  redis: { host: "", port: 6379, password: "", db: 0 },

  storage: {
    mode: "file",
    data_dir: "data",
    host: "",
    port: 3306,
    database: "",
    username: "",
    password: "",
  },

  log: {
    level: "info",
    console: { enable: true, no_color: false },
    file: { enable: true },
  },
});

const configLoading = ref(false);

// 获取默认配置

async function fetchDefaultConfig() {
  try {
    const res = await fetch(`${API_BASE}/config/default`);

    if (res.ok) {
      const data = await res.json();

      // 深度合并配置，确保响应性且不丢失结构

      if (data.http) Object.assign(configForm.value.http, data.http);

      if (data.redis) Object.assign(configForm.value.redis, data.redis);

      if (data.log) {
        if (data.log.level) configForm.value.log.level = data.log.level;

        if (data.log.console)
          Object.assign(configForm.value.log.console, data.log.console);

        if (data.log.file)
          Object.assign(configForm.value.log.file, data.log.file);
      }

      if (data.datasource)
        Object.assign(configForm.value.datasource, data.datasource);

      if (data.target) Object.assign(configForm.value.target, data.target);

      if (data.storage) Object.assign(configForm.value.storage, data.storage);
    }
  } catch (e) {
    console.error("获取默认配置失败:", e);
  }
}

// 保存系统配置

async function saveConfig() {
  configLoading.value = true;

  try {
    const res = await fetch(`${API_BASE}/config/update`, {
      method: "POST",

      headers: { "Content-Type": "application/json" },

      body: JSON.stringify(configForm.value),
    });

    if (res.ok) {
      Message.success("系统配置已更新，配置文件已同步");

      // 重新获取最新配置以同步页面数据

      await fetchDefaultConfig();

      // 刷新元数据，因为数据库连接可能变了

      await refreshDatabases();
    } else {
      const text = await res.text();
      try {
        const err = JSON.parse(text);
        Message.error("更新配置失败: " + err.error);
      } catch {
        Message.error("更新配置失败: " + text);
      }
    }
  } catch (e) {
    Message.error("更新配置失败: " + e.message);
  } finally {
    configLoading.value = false;
  }
}

const logApplying = ref(false);

async function applyLogConfig() {
  logApplying.value = true;
  try {
    const res = await fetch(`${API_BASE}/config/log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        level: configForm.value.log.level,
        console: configForm.value.log.console,
        file: configForm.value.log.file,
      }),
    });
    if (res.ok) {
      const data = await res.json();
      Message.success(`日志配置已热加载生效 — 级别: ${data.level?.toUpperCase()}, 控制台: ${data.console ? '开' : '关'}, 文件: ${data.file ? '开' : '关'}`);
    } else {
      const text = await res.text();
      try {
        const err = JSON.parse(text);
        Message.error("日志热加载失败: " + err.error);
      } catch {
        Message.error("日志热加载失败: " + text);
      }
    }
  } catch (e) {
    Message.error("日志热加载失败: " + e.message);
  } finally {
    logApplying.value = false;
  }
}

// 处理浏览器返回按钒

function handlePopState() {
  if (taskFormPage.value !== "none") {
    taskFormPage.value = "none";

    resetForm();
  }

  loadTaskFiltersFromUrl();
  fetchTasks(taskPagination.value.current, taskPagination.value.pageSize);
}

let refreshInterval;

onMounted(async () => {
  const savedTheme = localStorage.getItem(UI_THEME_STORAGE_KEY);
  if (uiThemeOptions.some((option) => option.value === savedTheme)) {
    uiTheme.value = savedTheme;
  }

  // 任务详情独立标签页：仅加载单个任务并定时刷新，不加载任务列表等主界面资源
  const detailMatch = window.location.hash.match(/^#\/task-detail\/(.+)$/);

  if (detailMatch) {
    isTaskDetailPage.value = true;

    detailPageTaskId.value = decodeURIComponent(detailMatch[1]);

    document.title = "任务详情";

    await fetchTaskDetailPage(detailPageTaskId.value);

    detailPageRefreshInterval = setInterval(() => {
      fetchTaskDetailPage(detailPageTaskId.value);
    }, 3000);

    // 实时进度按 2 秒轮询，比整体刷新更频繁，保证进度条/速度的流畅度
    detailPageProgressInterval = setInterval(() => {
      fetchTaskDetailProgress(detailPageTaskId.value);
    }, 2000);

    return;
  }

  window.addEventListener("popstate", handlePopState);

  await fetchDefaultConfig();
  await loadTaskSortOptions();

  fetchDatabases();

  loadTaskFiltersFromUrl();
  if (!taskFilters.value.sort || !taskSortOptions.value.some((option) => option.value === taskFilters.value.sort)) {
    taskFilters.value.sort = taskSortDefault.value;
  }
  await fetchTasks(taskPagination.value.current, taskPagination.value.pageSize);

  refreshInterval = setInterval(() => {
    fetchTasks(taskPagination.value.current, taskPagination.value.pageSize);
  }, 3000);
});

onUnmounted(() => {
  window.removeEventListener("popstate", handlePopState);

  if (refreshInterval) clearInterval(refreshInterval);

  if (detailPageRefreshInterval) clearInterval(detailPageRefreshInterval);

  if (detailPageProgressInterval) clearInterval(detailPageProgressInterval);
});

// 菜单点击处理

function onMenuClick(key) {
  console.log("Menu item clicked:", key);

  const prevKey = selectedKey.value[0];

  // 如果当前处于任务编辑/创建表单页面，先退出表单页面再跳转
  if (taskFormPage.value !== "none") {
    taskFormPage.value = "none";
    resetForm();
    window.history.pushState({}, "", "#/" + key);
  }

  selectedKey.value = [key];

  if (key === "tasks") {
    fetchTasks(taskPagination.value.current, taskPagination.value.pageSize);
  } else if (key === "config") {
    // 只有在从非配置页面切换过来时才获取配置，避免在配置页面内操作时被覆盖

    if (prevKey !== "config") {
      fetchDefaultConfig();
    }
  }
}

// 使用计算属性来确保页面正确渲染

const currentPage = computed(() => selectedKey.value[0]);

const taskFilters = ref({
  status: "",
  keyword: "",
  sort: taskSortDefault.value,
});

function resetTaskFilters() {
  taskFilters.value = {
    status: "",
    keyword: "",
    sort: taskSortDefault.value,
  };
}

function getSortLabel(sortKey) {
  return taskSortLabelMap.value[sortKey] || sortKey;
}

function clearAllTaskFilters() {
  resetTaskFilters();
  fetchTasks(1, taskPagination.value.pageSize);
}

const activeTaskFilterChips = computed(() => {
  const chips = [];
  if (taskFilters.value.status) {
    chips.push({ key: 'status', label: `状态：${getStatusText(taskFilters.value.status)}` });
  }
  if (taskFilters.value.keyword) {
    chips.push({ key: 'keyword', label: `关键词：${taskFilters.value.keyword}` });
  }
  chips.push({ key: 'sort', label: `排序：${getSortLabel(taskFilters.value.sort)}` });
  return chips;
});

const hasActiveTaskFilters = computed(() => {
  return Boolean(taskFilters.value.status || taskFilters.value.keyword || taskFilters.value.sort !== taskSortDefault.value);
});

const syncUrlDebounceState = { timer: null };

function syncTaskFiltersToUrl() {
  if (syncUrlDebounceState.timer) {
    clearTimeout(syncUrlDebounceState.timer);
  }
  syncUrlDebounceState.timer = setTimeout(() => {
    const url = new URL(window.location.href);
    url.searchParams.delete("page");
    url.searchParams.delete("page_size");
    url.searchParams.delete("status");
    url.searchParams.delete("keyword");
    url.searchParams.delete("sort");

    if (taskPagination.value.current > 1) {
      url.searchParams.set("page", String(taskPagination.value.current));
    }
    if (taskPagination.value.pageSize !== 10) {
      url.searchParams.set("page_size", String(taskPagination.value.pageSize));
    }
    if (taskFilters.value.status) {
      url.searchParams.set("status", taskFilters.value.status);
    }
    if (taskFilters.value.keyword) {
      url.searchParams.set("keyword", taskFilters.value.keyword);
    }
    if (taskFilters.value.sort) {
      url.searchParams.set("sort", taskFilters.value.sort);
    }

    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  }, 0);
}

function loadTaskFiltersFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const page = Number.parseInt(params.get("page") || "1", 10);
  const pageSize = Number.parseInt(params.get("page_size") || "10", 10);

  taskPagination.value.current = Number.isFinite(page) && page > 0 ? page : 1;
  taskPagination.value.pageSize = Number.isFinite(pageSize) && pageSize > 0 ? pageSize : 10;
  taskFilters.value.status = params.get("status") || "";
  taskFilters.value.keyword = params.get("keyword") || "";
  taskFilters.value.sort = params.get("sort") || "created_at_desc";
}

const taskPagination = ref({
  current: 1,
  pageSize: 10,
  total: 0,
});

const filteredTasks = computed(() => tasks.value);

const paginatedTasks = computed(() => filteredTasks.value);

const currentDatabaseSelectedTables = computed({
  get() {
    const currentDb = activeTableSourceDatabase.value;

    if (!currentDb) {
      return [];
    }

    ensureTableSelectionBucket(currentDb);

    return tableSelectionsByDatabase.value[currentDb];
  },

  set(newValue) {
    const currentDb = activeTableSourceDatabase.value;

    if (!currentDb) {
      return;
    }

    tableSelectionsByDatabase.value[currentDb] = [...newValue];

    selectedTables.value = selectedDatabases.value.flatMap((db) => {
      const tableNames = tableSelectionsByDatabase.value[db] || [];

      return tableNames.map((tableName) =>
        getQualifiedTableName(db, tableName),
      );
    });
  },
});

const totalSelectedTables = computed(() => {
  return selectedDatabases.value.reduce((sum, db) => {
    return sum + (tableSelectionsByDatabase.value[db] || []).length;
  }, 0);
});

const tableTargetMappingsByDatabase = computed(() => {
  return selectedDatabases.value
    .map((db) => {
      const sourceTables = tableSelectionsByDatabase.value[db] || [];
      const mapping = targetDatabaseMappings.value.find(
        (item) => item.source === db,
      );

      return {
        database: db,
        targetDatabase:
          mapping?.target || taskForm.value.target_database || db,
        tables: sourceTables.map((tableName) => {
          const sourceQualifiedName = getQualifiedTableName(db, tableName);

          return {
            source: sourceQualifiedName,
            tableName,
            target:
              targetTableMappings.value[sourceQualifiedName] || tableName,
          };
        }),
      };
    })
    .filter((group) => group.tables.length > 0);
});

const currentDatabaseTargetTableMappings = computed(() => {
  const currentDb = activeTableSourceDatabase.value;

  if (!currentDb) {
    return [];
  }

  const sourceTables = tableSelectionsByDatabase.value[currentDb] || [];

  return sourceTables.map((tableName) => {
    const sourceQualifiedName = getQualifiedTableName(currentDb, tableName);

    return {
      source: sourceQualifiedName,
      target: targetTableMappings.value[sourceQualifiedName] || tableName,
    };
  });
});

// 计算属性：过滤后的数据库列表

const filteredDatabases = computed(() => {
  if (!databaseSearchText.value) {
    return databases.value;
  }

  const searchText = databaseSearchText.value.toLowerCase();

  return databases.value.filter((db) => db.toLowerCase().includes(searchText));
});

// 计算属性：过滤后的表列表

const filteredTables = computed(() => {
  if (!tableSearchText.value) {
    return tables.value;
  }

  const searchText = tableSearchText.value.toLowerCase();

  return tables.value.filter((table) =>
    table.table_name.toLowerCase().includes(searchText),
  );
});

// 监听自定义源数据库开关变化，自动刷新数据库列表

watch(useCustomSourceDB, (newVal) => {
  fetchDatabases();
});

// 监听选中的源数据库变化，自动同步目标数据库映射

watch(
  selectedDatabases,
  (newDbs) => {
    if (selectedSyncLevel.value === "table") {
      if (newDbs.length === 0) {
        activeTableSourceDatabase.value = "";
        expandedTableDatabaseKeys.value = [];
        tables.value = [];
      } else if (!newDbs.includes(activeTableSourceDatabase.value)) {
        activeTableSourceDatabase.value = newDbs[0];

        if (expandedTableDatabaseKeys.value.length === 0) {
          expandedTableDatabaseKeys.value = [newDbs[0]];
        }

        fetchTablesForDatabase(newDbs[0]);
      }

      newDbs.forEach((db) => {
        if (
          expandedTableDatabaseKeys.value.includes(db) &&
          !tablesByDatabase.value[db]
        ) {
          fetchTablesForDatabase(db);
        }
      });

      const nextSelections = {};

      newDbs.forEach((db) => {
        nextSelections[db] = [...(tableSelectionsByDatabase.value[db] || [])];
      });

      tableSelectionsByDatabase.value = nextSelections;

      selectedTables.value = newDbs.flatMap((db) => {
        const tableNames = tableSelectionsByDatabase.value[db] || [];

        return tableNames.map((tableName) =>
          getQualifiedTableName(db, tableName),
        );
      });

      taskForm.value.source_schema = activeTableSourceDatabase.value || "";
    }

    const newMappings = newDbs.map((db) => {
      const existing = targetDatabaseMappings.value.find(
        (m) => m.source === db,
      );

      return existing || { source: db, target: db };
    });

    targetDatabaseMappings.value = newMappings;
  },
  { deep: true },
);

watch(
  tableTargetMappingsByDatabase,
  (groups) => {
    const dbKeys = groups.map((group) => group.database);

    if (dbKeys.length === 0) {
      return;
    }

    expandedTargetTableDatabaseKeys.value = [
      ...new Set([...expandedTargetTableDatabaseKeys.value, ...dbKeys]),
    ];
  },
  { deep: true },
);

watch(uiTheme, (theme) => syncUiThemeToDocument(theme), { immediate: true });
</script>

<template>
  <!-- 任务详情独立标签页 -->
  <a-layout v-if="isTaskDetailPage" class="task-detail-page-layout" :class="appThemeClass">
    <a-layout-header class="detail-page-header">
      <div class="detail-header-left">
        <a-button type="text" @click="closeTaskDetailPage">
          <template #icon><icon-close /></template>
          关闭
        </a-button>
        <a-divider direction="vertical" />
        <icon-storage style="font-size: 18px; color: var(--color-text-2)" />
        <a-typography-text strong style="margin-left: 8px; font-size: 16px">
          {{ detailPageTask?.config?.name || detailPageTaskId }}
        </a-typography-text>
        <a-tag
          v-if="detailPageTask"
          :color="getStatusColor(detailPageTask.context.status)"
          style="margin-left: 12px"
        >
          {{ getStatusText(detailPageTask.context.status) }}
        </a-tag>
        <a-tag
          v-if="detailPageTask?.context?.sync_phase"
          color="cyan"
          style="margin-left: 8px"
        >
          {{ syncPhaseText(detailPageTask.context.sync_phase) }}
        </a-tag>
        <a-typography-text
          v-if="detailPageLoading"
          type="secondary"
          style="margin-left: 12px; font-size: 12px"
        >
          <icon-refresh /> 刷新中…
        </a-typography-text>
      </div>
      <div class="detail-header-right">
        <a-space>
          <a-button
            v-if="detailPageTask && ['PENDING', 'PAUSED', 'FAILED'].includes(detailPageTask.context.status)"
            type="primary"
            status="success"
            @click="openStartTaskModal(detailPageTaskId, 'immediate')"
          >
            <template #icon><icon-play-arrow /></template>
            启动
          </a-button>
          <a-button
            v-if="detailPageTask && ['PENDING', 'PAUSED', 'FAILED'].includes(detailPageTask.context.status)"
            @click="openStartTaskModal(detailPageTaskId, 'cron')"
          >
            <template #icon><icon-clock-circle /></template>
            定时启动
          </a-button>
          <a-button
            v-if="detailPageTask?.context?.status === 'SCHEDULED'"
            status="warning"
            @click="detailPageCancelSchedule"
          >
            <template #icon><icon-clock-circle /></template>
            取消定时
          </a-button>
          <a-button
            v-if="detailPageTask?.context?.status === 'RUNNING'"
            status="warning"
            @click="detailPagePause"
          >
            <template #icon><icon-pause /></template>
            暂停
          </a-button>
          <a-button type="text" @click="refreshDetailPage">
            <template #icon><icon-refresh /></template>
          </a-button>
        </a-space>
      </div>
    </a-layout-header>

    <a-layout-content class="detail-page-content">
      <a-spin
        :loading="detailPageLoading && !detailPageTask"
        tip="加载中…"
        style="width: 100%"
      >
        <div v-if="detailPageTask" class="detail-page-body">
          <!-- 概览卡片 -->
          <a-row :gutter="16" class="detail-overview-row">
            <a-col :xs="12" :md="6">
              <a-card class="overview-card">
                <div class="overview-label">同步进度</div>
                <div class="overview-value">{{ getProgress(detailPageTask) }}%</div>
                <a-progress
                  :percent="getProgressRatio(detailPageTask)"
                  :status="detailPageTask.context.status === 'FAILED' ? 'danger' : 'normal'"
                  :show-text="false"
                  style="margin-top: 8px"
                />
              </a-card>
            </a-col>
            <a-col :xs="12" :md="6">
              <a-card class="overview-card">
                <div class="overview-label">已处理 / 总行数</div>
                <div class="overview-value">{{ getRowCounts(detailPageTask).processed }}</div>
                <div class="overview-sub">
                  总行数：{{ getRowCounts(detailPageTask).total }}
                </div>
              </a-card>
            </a-col>
            <a-col :xs="12" :md="6">
              <a-card class="overview-card">
                <div class="overview-label">运行时长</div>
                <div class="overview-value overview-value-sm">
                  {{ calculateDuration(detailPageTask.context.start_time, detailPageTask.context.end_time) }}
                </div>
                <div class="overview-sub">
                  开始：{{ formatTime(detailPageTask.context.start_time) }}
                </div>
              </a-card>
            </a-col>
            <a-col :xs="12" :md="6">
              <a-card class="overview-card">
                <div class="overview-label">增量位点 / 延迟</div>
                <div class="overview-value overview-value-sm">
                  {{ detailPageMetrics.lag != null ? detailPageMetrics.lag + 's' : '-' }}
                </div>
                <div class="overview-sub">
                  {{
                    detailPageMetrics.binlog_file
                      ? `${detailPageMetrics.binlog_file}:${detailPageMetrics.binlog_pos}`
                      : (detailPageTask.context.current_position || '-')
                  }}
                </div>
              </a-card>
            </a-col>
          </a-row>

          <a-tabs v-model:active-key="detailPageActiveTab" class="detail-tabs">
            <!-- 实时进度 -->
            <a-tab-pane key="runtime" title="实时进度">
              <a-empty
                v-if="!detailPageProgress"
                description="任务未运行，暂无实时进度数据（进度数据仅在任务同步期间存在）"
                style="margin-top: 24px"
              />
              <template v-else>
                <!-- 实时概览卡片 -->
                <a-row :gutter="16" class="runtime-overview-row">
                  <a-col :xs="12" :md="6">
                    <a-card class="overview-card">
                      <div class="overview-label">当前同步表</div>
                      <div
                        class="overview-value overview-value-sm runtime-current-table"
                        :title="detailPageProgress.current_table || '-'"
                      >
                        {{ detailPageProgress.current_table || '-' }}
                      </div>
                      <div class="overview-sub">
                        阶段：
                        <a-tag
                          :color="detailPageProgress.phase === 'incremental' ? 'green' : 'blue'"
                          size="small"
                        >
                          {{ detailPageProgress.phase === 'incremental' ? '增量同步' : '全量同步' }}
                        </a-tag>
                      </div>
                    </a-card>
                  </a-col>
                  <a-col :xs="12" :md="6">
                    <a-card class="overview-card">
                      <div class="overview-label">整体速度</div>
                      <div class="overview-value">
                        {{ formatSpeed(detailPageProgress.overall_speed) }}
                        <span class="overview-unit">行/秒</span>
                      </div>
                      <div class="overview-sub">
                        最后更新：{{ formatTime(detailPageProgress.updated_at) }}
                      </div>
                    </a-card>
                  </a-col>
                  <a-col :xs="12" :md="6">
                    <a-card class="overview-card">
                      <div class="overview-label">已耗时</div>
                      <div class="overview-value overview-value-sm">
                        {{ formatSeconds(detailPageProgress.elapsed_seconds) }}
                      </div>
                      <div class="overview-sub">自任务开始同步起累计</div>
                    </a-card>
                  </a-col>
                  <a-col :xs="12" :md="6">
                    <a-card class="overview-card">
                      <div class="overview-label">预估剩余</div>
                      <div class="overview-value overview-value-sm">
                        {{ formatSeconds(detailPageProgress.estimated_remain) }}
                      </div>
                      <div class="overview-sub">
                        {{
                          detailPageProgress.estimated_remain === -1
                            ? '数据不足，暂无法估算'
                            : '基于当前速度估算'
                        }}
                      </div>
                    </a-card>
                  </a-col>
                </a-row>

                <!-- 表级实时进度 -->
                <div class="runtime-section-title">
                  <icon-storage />
                  <span>表级实时进度</span>
                  <a-tag color="arcoblue" size="small" style="margin-left: 8px">
                    共 {{ (detailPageProgress.tables || []).length }} 张表
                  </a-tag>
                </div>
                <a-table
                  :columns="[
                    { title: '表名', slotName: 'tableName', width: 240 },
                    { title: '状态', slotName: 'status', width: 110 },
                    { title: '进度', slotName: 'progress', width: 200 },
                    { title: '已处理 / 总行数', slotName: 'rows', width: 200 },
                    { title: '速度', slotName: 'speed', width: 130 },
                    { title: '时间', slotName: 'timeRange' },
                  ]"
                  :data="detailPageProgress.tables || []"
                  :pagination="false"
                  size="medium"
                  :scroll="{ y: 480 }"
                  row-key="table"
                >
                  <template #tableName="{ record }">
                    <a-space>
                      <icon-table />
                      <span class="runtime-table-name">{{ record.schema }}.{{ record.table }}</span>
                    </a-space>
                  </template>
                  <template #status="{ record }">
                    <a-tag :color="runtimeStatusColor(record.status)">
                      {{ runtimeStatusText(record.status) }}
                    </a-tag>
                  </template>
                  <template #progress="{ record }">
                    <a-progress
                      :percent="getProgressRatio(record.progress_pct)"
                      :status="
                        record.status === 'failed'
                          ? 'danger'
                          : record.status === 'completed'
                            ? 'success'
                            : 'normal'
                      "
                      :show-text="true"
                      size="mini"
                    />
                  </template>
                  <template #rows="{ record }">
                    <span class="runtime-rows">
                      {{ (record.processed_rows || 0).toLocaleString() }}
                      <span class="runtime-rows-total">/ {{ (record.total_rows || 0).toLocaleString() }}</span>
                    </span>
                  </template>
                  <template #speed="{ record }">
                    <span v-if="record.speed_rows_sec > 0" class="runtime-speed">
                      {{ formatSpeed(record.speed_rows_sec) }} <span class="overview-unit">行/秒</span>
                    </span>
                    <span v-else class="runtime-speed-muted">-</span>
                  </template>
                  <template #timeRange="{ record }">
                    <div class="runtime-time-cell">
                      <div>{{ formatTime(record.started_at) }}</div>
                      <div v-if="record.completed_at" class="runtime-time-end">
                        → {{ formatTime(record.completed_at) }}
                      </div>
                    </div>
                  </template>
                </a-table>
              </template>
            </a-tab-pane>

            <!-- 执行进度 -->
            <a-tab-pane key="progress" title="执行进度">
              <a-progress
                :percent="getProgressRatio(detailPageTask)"
                :status="detailPageTask.context.status === 'FAILED' ? 'danger' : 'normal'"
                style="margin-bottom: 20px"
              />
              <a-descriptions :column="3" bordered>
                <a-descriptions-item label="任务状态">
                  <a-tag :color="getStatusColor(detailPageTask.context.status)">
                    {{ getStatusText(detailPageTask.context.status) }}
                  </a-tag>
                  <span
                    v-if="detailPageTask.context.status === 'SCHEDULED' && detailPageTask.context.scheduled_at"
                    style="margin-left: 8px; color: #165dff; font-size: 13px"
                  >
                    <icon-clock-circle /> {{ formatScheduledTime(detailPageTask) }}
                  </span>
                </a-descriptions-item>
                <a-descriptions-item label="同步阶段">
                  {{ syncPhaseText(detailPageTask.context.sync_phase) }}
                </a-descriptions-item>
                <a-descriptions-item label="进度">
                  {{ getProgress(detailPageTask) }}%
                </a-descriptions-item>
                <a-descriptions-item label="已处理行数">
                  {{ getRowCounts(detailPageTask).processed }}
                </a-descriptions-item>
                <a-descriptions-item label="总行数">
                  {{ getRowCounts(detailPageTask).total }}
                </a-descriptions-item>
                <a-descriptions-item label="已完成表数">
                  {{ resumeTableList(detailPageTask).filter((t) => t.done).length }} /
                  {{ resumeTableList(detailPageTask).length || (detailPageMetrics.tables_total || 0) }}
                </a-descriptions-item>
                <a-descriptions-item label="当前位点">
                  {{ detailPageTask.context.current_position || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="运行时长">
                  {{ calculateDuration(detailPageTask.context.start_time, detailPageTask.context.end_time) }}
                </a-descriptions-item>
                <a-descriptions-item label="最后更新">
                  {{ formatTime(detailPageTask.context.last_update_time) }}
                </a-descriptions-item>
                <a-descriptions-item label="开始时间">
                  {{ formatTime(detailPageTask.context.start_time) }}
                </a-descriptions-item>
                <a-descriptions-item label="结束时间">
                  {{ formatTime(detailPageTask.context.end_time) }}
                </a-descriptions-item>
                <a-descriptions-item label="创建时间">
                  {{ formatTime(detailPageTask.context.created_at) }}
                </a-descriptions-item>
              </a-descriptions>

              <a-descriptions
                v-if="detailPageMetrics.binlog_file || detailPageMetrics.lag != null"
                title="增量同步指标"
                :column="3"
                bordered
                style="margin-top: 20px"
              >
                <a-descriptions-item label="Binlog 文件">
                  {{ detailPageMetrics.binlog_file || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="Binlog 位点">
                  {{ detailPageMetrics.binlog_pos || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="延迟">
                  {{ detailPageMetrics.lag != null ? detailPageMetrics.lag : '-' }}
                </a-descriptions-item>
              </a-descriptions>

              <a-descriptions
                v-if="detailPageTask.context.status === 'SCHEDULED' && (detailPageTask.context.schedule_mode || detailPageTask.context.cron_expression)"
                title="定时调度"
                :column="2"
                bordered
                style="margin-top: 20px"
              >
                <a-descriptions-item label="调度模式">
                  {{ detailPageTask.context.schedule_mode || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="Cron 表达式">
                  {{ detailPageTask.context.cron_expression || '-' }}
                </a-descriptions-item>
                <a-descriptions-item v-if="detailPageTask.context.scheduled_at" label="下次执行">
                  {{ formatScheduledTime(detailPageTask) }}
                </a-descriptions-item>
                <a-descriptions-item v-if="detailPageTask.context.repeat_remaining" label="剩余次数">
                  {{ detailPageTask.context.repeat_remaining }} /
                  {{ detailPageTask.context.repeat_count }}
                </a-descriptions-item>
              </a-descriptions>
            </a-tab-pane>

            <!-- 历史全量断点 -->
            <a-tab-pane key="tables" title="历史断点">
              <a-table
                :columns="[
                  { title: '表名', dataIndex: 'key' },
                  { title: '读取路径', dataIndex: 'readPath' },
                  { title: '状态', slotName: 'done' },
                  { title: '已处理行数', dataIndex: 'processedRows' },
                  { title: '表内并发', dataIndex: 'intraWorkers' },
                ]"
                :data="resumeTableList(detailPageTask)"
                :pagination="false"
                size="medium"
              >
                <template #done="{ record }">
                  <a-tag :color="record.done ? 'green' : 'orange'">
                    {{ record.done ? '已完成' : '进行中' }}
                  </a-tag>
                </template>
              </a-table>
              <a-empty
                v-if="resumeTableList(detailPageTask).length === 0"
                description="暂无历史全量断点数据"
                style="margin-top: 24px"
              />
            </a-tab-pane>

            <!-- 基本信息 -->
            <a-tab-pane key="basic" title="基本信息">
              <a-descriptions title="基本信息" :column="2" bordered>
                <a-descriptions-item label="任务ID">
                  {{ detailPageTask.config.id }}
                </a-descriptions-item>
                <a-descriptions-item label="任务名称">
                  {{ detailPageTask.config.name }}
                </a-descriptions-item>
                <a-descriptions-item label="同步级别">
                  {{ detailPageTask.config.sync_level === 'DATABASE' ? '库级别' : '表级别' }}
                </a-descriptions-item>
                <a-descriptions-item label="同步模式">
                  <a-tag v-if="detailPageTask.config.mode === 'FULL'" color="blue">全量同步</a-tag>
                  <a-tag v-else-if="detailPageTask.config.mode === 'INCREMENTAL'" color="green">增量同步</a-tag>
                  <a-tag v-else color="purple">全量+增量</a-tag>
                </a-descriptions-item>
                <a-descriptions-item label="批量大小">
                  {{ detailPageTask.config.batch_size }}
                </a-descriptions-item>
                <a-descriptions-item label="表并发数">
                  {{ detailPageTask.config.worker_count }}
                </a-descriptions-item>
                <a-descriptions-item label="单表内并发">
                  {{
                    detailPageTask.config.intra_table_worker_count > 0
                      ? detailPageTask.config.intra_table_worker_count
                      : '默认（≤16）'
                  }}
                </a-descriptions-item>
                <a-descriptions-item label="无主键 LIMIT 1">
                  {{ detailPageTask.config.enable_limit_one ? '开启' : '关闭' }}
                </a-descriptions-item>
                <a-descriptions-item label="并行事务提交间隔">
                  {{ detailPageTask.config.tx_commit_every_n_parallel > 0 ? detailPageTask.config.tx_commit_every_n_parallel : '默认（5批）' }}
                </a-descriptions-item>
                <a-descriptions-item label="启用索引优化">
                  {{ detailPageTask.config.optimize_index ? '开启' : '关闭' }}
                </a-descriptions-item>
                <a-descriptions-item v-if="detailPageTask.config.optimize_index" label="索引回放并发度">
                  {{
                    detailPageTask.config.index_restore_worker_count > 0
                      ? detailPageTask.config.index_restore_worker_count
                      : '自动（min(worker_count, 4)）'
                  }}
                </a-descriptions-item>
                <a-descriptions-item label="目标库只读保护">
                  {{ detailPageTask.config.enable_read_only ? '开启' : '关闭' }}
                </a-descriptions-item>
                <a-descriptions-item label="DDL前删除目标">
                  {{ detailPageTask.config.enable_drop_table_before_ddl ? '开启' : '关闭' }}
                </a-descriptions-item>
              </a-descriptions>

              <a-descriptions title="源数据库配置" :column="2" bordered style="margin-top: 20px">
                <a-descriptions-item label="主机地址">
                  {{ detailPageTask.config.source_db?.host || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="端口">
                  {{ detailPageTask.config.source_db?.port || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="用户名">
                  {{ detailPageTask.config.source_db?.username || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="数据库">
                  <a-space wrap>
                    <a-tag
                      v-for="db in (detailPageTask.config.source_databases || [])"
                      :key="db"
                      color="arcoblue"
                      >{{ db }}</a-tag
                    >
                    <span v-if="!(detailPageTask.config.source_databases || []).length">{{
                      detailPageTask.config.source_schema || '-'
                    }}</span>
                  </a-space>
                </a-descriptions-item>
              </a-descriptions>

              <a-descriptions title="目标数据库配置" :column="2" bordered style="margin-top: 20px">
                <template
                  v-if="!detailPageTask.config.sink_configs || detailPageTask.config.sink_configs.length === 0"
                >
                  <a-descriptions-item label="主机地址">
                    {{ detailPageTask.config.target_db?.host || '-' }}
                  </a-descriptions-item>
                  <a-descriptions-item label="端口">
                    {{ detailPageTask.config.target_db?.port || '-' }}
                  </a-descriptions-item>
                  <a-descriptions-item label="用户名">
                    {{ detailPageTask.config.target_db?.username || '-' }}
                  </a-descriptions-item>
                  <a-descriptions-item label="数据库映射">
                    <a-space wrap>
                      <span
                        v-for="mapping in getTaskDatabaseMappings(detailPageTask)"
                        :key="mapping.source"
                        style="display: inline-flex; align-items: center; margin-right: 12px"
                      >
                        <a-tag color="blue">{{ mapping.source }}</a-tag>
                        <span style="margin: 0 4px">→</span>
                        <a-tag color="green">{{ mapping.target }}</a-tag>
                      </span>
                    </a-space>
                  </a-descriptions-item>
                </template>
                <template v-else>
                  <a-descriptions-item
                    v-for="(sink, idx) in detailPageTask.config.sink_configs"
                    :key="idx"
                    :label="`目标端 ${sink.type}`"
                    :span="2"
                  >
                    <div v-if="sink.type === 'MYSQL'">
                      主机：{{ sink.options?.host || '-' }} 端口：{{ sink.options?.port || '-' }} 用户：{{
                        sink.options?.username || '-'
                      }}
                    </div>
                    <div v-else-if="sink.type === 'KAFKA'">
                      Brokers：{{
                        Array.isArray(sink.options?.brokers)
                          ? sink.options.brokers.join(', ')
                          : sink.options?.brokers || '-'
                      }}
                      Topic：{{ sink.options?.topic || '-' }}
                    </div>
                    <div v-else-if="sink.type === 'HTTP_WEBHOOK'">
                      URL：{{ sink.options?.url || '-' }} Method：{{ sink.options?.method || '-' }}
                    </div>
                  </a-descriptions-item>
                </template>
              </a-descriptions>

              <a-descriptions
                v-if="detailPageTask.config.sync_level !== 'DATABASE'"
                title="同步表"
                :column="1"
                bordered
                style="margin-top: 20px"
              >
                <a-descriptions-item label="表列表">
                  <a-space wrap>
                    <a-tag v-for="table in (detailPageTask.config.tables || [])" :key="table">{{
                      table
                    }}</a-tag>
                    <span
                      v-if="!detailPageTask.config.tables || detailPageTask.config.tables.length === 0"
                      >全库同步</span
                    >
                  </a-space>
                </a-descriptions-item>
              </a-descriptions>
            </a-tab-pane>

            <!-- 日志与错误 -->
            <a-tab-pane key="logs" title="日志与错误">
              <a-alert
                v-if="detailPageTask.context.error_stack"
                type="error"
                :show-icon="true"
                style="margin-bottom: 16px"
                title="任务错误堆栈"
              >
                <pre style="margin: 0; white-space: pre-wrap; word-break: break-word">{{
                  detailPageTask.context.error_stack
                }}</pre>
              </a-alert>

              <a-alert
                v-if="detailPageTask.context.full_sync_failed_reason"
                type="warning"
                :show-icon="true"
                style="margin-bottom: 16px"
                title="全量同步失败原因"
              >
                {{ detailPageTask.context.full_sync_failed_reason }}
              </a-alert>

              <a-descriptions title="同步阶段时间线" :column="2" bordered>
                <a-descriptions-item label="全量开始时间">
                  {{ formatTime(detailPageTask.context.full_sync_started_at) }}
                </a-descriptions-item>
                <a-descriptions-item label="全量完成时间">
                  {{ formatTime(detailPageTask.context.full_sync_completed_at) }}
                </a-descriptions-item>
                <a-descriptions-item label="全量起始位点" :span="2">
                  {{ detailPageTask.context.full_sync_start_position || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="最近增量位点" :span="2">
                  {{ detailPageTask.context.last_incremental_position || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="当前位点" :span="2">
                  {{ detailPageTask.context.current_position || '-' }}
                </a-descriptions-item>
              </a-descriptions>

              <a-alert
                v-if="!detailPageTask.context.error_stack && !detailPageTask.context.full_sync_failed_reason"
                type="info"
                :show-icon="true"
                style="margin-top: 16px"
                title="暂无错误日志"
                description="任务当前没有记录到错误堆栈或全量失败原因。"
              />
            </a-tab-pane>
          </a-tabs>
        </div>
        <a-empty
          v-else-if="!detailPageLoading"
          description="任务不存在或已被删除"
          style="margin-top: 80px"
        />
      </a-spin>
    </a-layout-content>
  </a-layout>

  <a-layout v-if="!isTaskDetailPage" class="layout-container" :class="appThemeClass">
    <!-- 左侧导航栏 -->

    <a-layout-sider :width="220" :collapsible="false" class="sider">
      <div class="logo">
        <div class="logo-icon">
          <icon-storage />
        </div>

        <span class="logo-text">MySQL 数据同步</span>
      </div>

      <a-menu
        v-model:selected-keys="selectedKey"
        :auto-open-selected="true"
        :collapsed="false"
        class="sider-menu"
        theme="dark"
        @menu-item-click="onMenuClick"
      >
        <a-menu-item key="tasks">
          <template #icon><icon-list /></template>

          任务管理
        </a-menu-item>

        <a-menu-item key="config">
          <template #icon><icon-settings /></template>

          系统配置
        </a-menu-item>
      </a-menu>

      <div class="sider-footer">
        <a-typography-text type="secondary">
          MySQL to Async v1.0
        </a-typography-text>
      </div>
    </a-layout-sider>

    <!-- 主内容区 -->

    <a-layout>
      <a-layout-header class="header">
        <div class="header-left">
          <a-button
            v-if="taskFormPage !== 'none'"
            type="text"
            style="margin-right: 8px"
            @click="closeTaskForm"
          >
            <template #icon><icon-arrow-left /></template>

            返回
          </a-button>

          <a-typography-title :heading="5" style="margin: 0">
            {{
              taskFormPage !== "none"
                ? editMode
                  ? "编辑任务"
                  : "创建同步任务"
                : selectedKey[0] === "tasks"
                  ? "任务管理"
                  : "系统配置"
            }}
          </a-typography-title>
        </div>

        <div
          class="header-right"
          v-if="selectedKey[0] === 'tasks' && taskFormPage === 'none'"
        >
          <a-button type="primary" @click="openCreateDialog">
            <template #icon><icon-plus /></template>

            创建同步任务
          </a-button>
        </div>

        <div class="header-right" v-if="taskFormPage !== 'none'">
          <a-space>
            <a-button @click="closeTaskForm">取消</a-button>

            <a-button type="primary" :loading="loading" @click="createTask">
              {{ editMode ? "更新" : "创建" }}
            </a-button>
          </a-space>
        </div>
      </a-layout-header>

      <a-layout-content class="content">
        <!-- 选择同步类型页 -->
        <div v-if="taskFormPage === 'select_type'" class="select-type-page">
          <div class="select-type-header">
            <a-typography-title :heading="3" style="margin-bottom: 8px"
              >选择目标端类型</a-typography-title
            >
            <a-typography-text type="secondary"
              >请选择数据同步的目标存储系统</a-typography-text
            >
          </div>

          <div class="type-cards-container">
            <a-card
              hoverable
              class="type-card"
              @click="proceedToCreateTask('MYSQL')"
            >
              <div class="type-icon mysql-icon">
                <icon-storage />
              </div>
              <div class="type-content">
                <a-typography-title :heading="5"
                  >MySQL 数据库</a-typography-title
                >
                <a-typography-text type="secondary"
                  >将数据实时同步到另一个 MySQL
                  或兼容数据库，支持全量+增量</a-typography-text
                >
              </div>
            </a-card>

            <a-card
              hoverable
              class="type-card"
              @click="proceedToCreateTask('KAFKA')"
            >
              <div class="type-icon kafka-icon">
                <icon-send />
              </div>
              <div class="type-content">
                <a-typography-title :heading="5"
                  >Kafka 消息队列</a-typography-title
                >
                <a-typography-text type="secondary"
                  >将变更流以 JSON 格式投递到 Kafka
                  Topic，适合下游流式处理</a-typography-text
                >
              </div>
            </a-card>

            <a-card
              hoverable
              class="type-card"
              @click="proceedToCreateTask('WEBHOOK')"
            >
              <div class="type-icon webhook-icon">
                <icon-link />
              </div>
              <div class="type-content">
                <a-typography-title :heading="5"
                  >HTTP Webhook</a-typography-title
                >
                <a-typography-text type="secondary"
                  >发生数据变更时主动回调指定的 HTTP
                  接口，灵活集成业务系统</a-typography-text
                >
              </div>
            </a-card>

            <a-card
              hoverable
              class="type-card"
              @click="proceedToCreateTask('MULTI')"
            >
              <div class="type-icon multi-icon">
                <icon-branch />
              </div>
              <div class="type-content">
                <a-typography-title :heading="5"
                  >高级: 多目标 (Multi-Sink)</a-typography-title
                >
                <a-typography-text type="secondary"
                  >同时将数据分发到多个不同类型的目标端</a-typography-text
                >
              </div>
            </a-card>
          </div>
        </div>

        <!-- 任务表单全屏页 -->
        <div
          v-if="taskFormPage === 'create' || taskFormPage === 'edit'"
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

                <a-form-item label="目标端类型" v-if="taskFormPage === 'edit'">
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
                      @click="taskFormPage = 'select_type'"
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
                          @change="
                            () => {
                              if (
                                selectedDatabases.length ===
                                filteredDatabases.length
                              ) {
                                selectedDatabases = [];
                              } else {
                                selectedDatabases = filteredDatabases;
                              }
                            }
                          "
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
                        @click="selectedDatabases = []"
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
                          @change="
                            () => {
                              if (
                                selectedDatabases.length ===
                                filteredDatabases.length
                              ) {
                                selectedDatabases = [];
                              } else {
                                selectedDatabases = filteredDatabases;
                              }
                              onTableSourceDatabasesChange();
                            }
                          "
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
                        @click="selectedDatabases = []"
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
                  <a-form-item
                    label="选择要同步的表"
                    class="table-selector-form-item"
                  >
                    <a-alert
                      v-if="selectedDatabases.length === 0"
                      type="info"
                      style="margin-bottom: 8px"
                      show-icon
                    >
                      请先选择至少一个源数据库，再展开库名勾选表
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
                              size="mini"
                              :loading="loadingTablesByDatabase[db]"
                              @click="fetchTablesForDatabase(db)"
                            >
                              <template #icon><icon-refresh /></template>
                              刷新
                            </a-button>

                            <a-button
                              type="text"
                              size="mini"
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
                              v-else
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

                    <div style="margin-top: 8px">
                      <a-typography-text type="secondary">
                        总计已选 {{ totalSelectedTables }} 个表
                      </a-typography-text>
                    </div>
                  </a-form-item>
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
                              label="Key 模式"
                              style="margin-bottom: 16px"
                            >
                              <a-select v-model="singleKafkaConfig.key_mode">
                                <a-option value="pk">主键 (pk)</a-option>
                                <a-option value="table">表名 (table)</a-option>
                              </a-select>
                            </a-form-item>
                          </a-col>
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
                          <a-col :span="8">
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
                          <a-col :span="8">
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
                          <a-col :span="8">
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
                                label="Key 模式"
                                style="margin-bottom: 8px"
                              >
                                <a-select v-model="sc.options.key_mode">
                                  <a-option value="pk">主键 (pk)</a-option>
                                  <a-option value="table"
                                    >表名 (table)</a-option
                                  >
                                </a-select>
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
                            <a-col :span="8">
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
                            <a-col :span="8">
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
                            <a-col :span="8">
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

        <!-- 任务管理页面 -->

        <div v-show="taskFormPage === 'none' && currentPage === 'tasks'">
          <!-- 统计卡片 -->

          <a-row :gutter="16" class="stat-cards">
            <a-col :span="6">
              <a-card class="stat-card">
                <a-statistic title="总任务数" :value="tasks.length">
                  <template #prefix>
                    <icon-branch class="stat-icon blue" />
                  </template>
                </a-statistic>
              </a-card>
            </a-col>

            <a-col :span="6">
              <a-card class="stat-card">
                <a-statistic
                  title="执行中"
                  :value="
                    tasks.filter((t) => t.context.status === 'RUNNING').length
                  "
                >
                  <template #prefix>
                    <icon-play-arrow class="stat-icon green" />
                  </template>
                </a-statistic>
              </a-card>
            </a-col>

            <a-col :span="6">
              <a-card class="stat-card">
                <a-statistic
                  title="已完成"
                  :value="
                    tasks.filter((t) => t.context.status === 'COMPLETED').length
                  "
                >
                  <template #prefix>
                    <icon-check class="stat-icon blue" />
                  </template>
                </a-statistic>
              </a-card>
            </a-col>

            <a-col :span="6">
              <a-card class="stat-card">
                <a-statistic
                  title="失败"
                  :value="
                    tasks.filter((t) => t.context.status === 'FAILED').length
                  "
                >
                  <template #prefix>
                    <icon-close class="stat-icon red" />
                  </template>
                </a-statistic>
              </a-card>
            </a-col>
          </a-row>

          <!-- 任务列表 -->

          <a-card class="task-list-card">
            <template #title>
              <div class="task-list-header">
                <div class="task-list-title-wrap">
                  <a-typography-title :heading="6" style="margin: 0">
                    任务列表
                  </a-typography-title>
                  <a-typography-text type="secondary">
                    统一筛选、排序与搜索，帮助快速定位任务
                  </a-typography-text>
                </div>

                <div class="task-list-toolbar">
                  <a-tag color="arcoblue" size="small" bordered>
                    共 {{ taskPagination.total }} 条
                  </a-tag>
                  <a-select
                    v-model="taskFilters.status"
                    placeholder="任务状态"
                    allow-clear
                    style="width: 150px"
                    @change="() => fetchTasks(1, taskPagination.pageSize)"
                  >
                    <a-option value="">全部</a-option>
                    <a-option value="PENDING">待执行</a-option>
                    <a-option value="RUNNING">运行中</a-option>
                    <a-option value="PAUSED">已暂停</a-option>
                    <a-option value="SCHEDULED">已计划</a-option>
                    <a-option value="COMPLETED">已完成</a-option>
                    <a-option value="FAILED">失败</a-option>
                  </a-select>
                  <a-select
                    v-model="taskFilters.sort"
                    placeholder="排序方式"
                    style="width: 160px"
                    @change="() => fetchTasks(1, taskPagination.pageSize)"
                  >
                    <a-option
                      v-for="option in taskSortOptions"
                      :key="option.value"
                      :value="option.value"
                    >
                      {{ option.label }}
                    </a-option>
                  </a-select>
                  <a-input-search
                    v-model="taskFilters.keyword"
                    placeholder="搜索任务名称 / ID / 表名"
                    style="width: 320px"
                    allow-clear
                    @search="() => fetchTasks(1, taskPagination.pageSize)"
                    @clear="() => fetchTasks(1, taskPagination.pageSize)"
                  />
                </div>
              </div>
            </template>

            <a-card class="task-filter-panel" :bordered="false">
              <template #title>
                <div class="task-filter-panel__header">
                  <div>
                    <div class="task-filter-panel__title">筛选面板</div>
                    <div class="task-filter-panel__desc">支持状态、排序与关键词组合筛选</div>
                  </div>
                  <div class="task-filter-panel__actions">
                    <a-tag color="arcoblue" bordered>当前页 {{ paginatedTasks.length }} 条</a-tag>
                    <a-button size="small" type="text" @click="clearAllTaskFilters">
                      一键清空
                    </a-button>
                  </div>
                </div>
              </template>

              <div class="task-filter-summary">
                <div class="task-filter-summary__title">已选筛选条件</div>
                <div v-if="activeTaskFilterChips.length > 0" class="task-filter-summary__chips">
                  <a-tag
                    v-for="chip in activeTaskFilterChips"
                    :key="chip.key + chip.label"
                    size="small"
                    color="arcoblue"
                    bordered
                    class="filter-chip"
                  >
                    {{ chip.label }}
                  </a-tag>
                </div>
                <div v-else class="task-filter-summary__empty">当前没有生效的筛选条件，将展示全部任务。</div>
              </div>

              <a-collapse class="advanced-filter-collapse" :default-active-key="['advanced']">
                <a-collapse-item key="advanced" header="高级筛选">
                  <a-row :gutter="12" class="task-filter-form-row">
                    <a-col :span="8">
                      <a-form-item label="快速筛选">
                        <a-select v-model="taskFilters.status" allow-clear placeholder="按状态筛选">
                          <a-option value="">全部</a-option>
                          <a-option value="PENDING">待执行</a-option>
                          <a-option value="RUNNING">运行中</a-option>
                          <a-option value="PAUSED">已暂停</a-option>
                          <a-option value="SCHEDULED">已计划</a-option>
                          <a-option value="COMPLETED">已完成</a-option>
                          <a-option value="FAILED">失败</a-option>
                        </a-select>
                      </a-form-item>
                    </a-col>
                    <a-col :span="8">
                      <a-form-item label="排序预设">
                        <a-select v-model="taskFilters.sort">
                          <a-option v-for="option in taskSortOptions" :key="option.value" :value="option.value">
                            {{ option.label }}
                          </a-option>
                        </a-select>
                      </a-form-item>
                    </a-col>
                    <a-col :span="8">
                      <a-form-item label="关键词搜索">
                        <a-input-search v-model="taskFilters.keyword" placeholder="任务名 / ID / 表名" allow-clear @search="() => fetchTasks(1, taskPagination.pageSize)" @clear="() => fetchTasks(1, taskPagination.pageSize)" />
                      </a-form-item>
                    </a-col>
                  </a-row>
                </a-collapse-item>
              </a-collapse>
            </a-card>

            <div v-if="filteredTasks.length === 0" class="empty-state empty-state--card">
              <a-empty description="暂无匹配的任务">
                <a-button type="primary" @click="openCreateDialog">
                  创建任务
                </a-button>
              </a-empty>
            </div>

            <a-list v-else :bordered="false" class="task-list">
              <a-list-item
                v-for="task in paginatedTasks"
                :key="task.config.id"
                class="task-item"
              >
                <a-card :bordered="false" class="task-card-inner">
                  <div class="task-card-grid">
                    <div class="task-card-main">
                      <div class="task-header">
                        <div class="task-title">
                          <a-typography-title :heading="6" style="margin: 0">
                            {{ task.config.name }}
                          </a-typography-title>

                          <a-tag
                            :color="getStatusColor(task.context.status)"
                            size="small"
                            bordered
                            class="task-status-tag"
                          >
                            {{ getStatusText(task.context.status) }}
                          </a-tag>
                          <a-tag
                            v-if="task.context.status === 'SCHEDULED' && task.context.scheduled_at"
                            color="arcoblue"
                            size="small"
                            bordered
                            class="task-status-tag"
                          >
                            <icon-clock-circle /> {{ formatScheduledTime(task) }}
                          </a-tag>
                          <a-tag
                            v-if="
                              task.config.sink_configs &&
                              task.config.sink_configs.length > 0
                            "
                            :color="
                              task.config.sink_configs.length > 1
                                ? 'purple'
                                : task.config.sink_configs[0].type === 'KAFKA'
                                  ? 'orange'
                                  : task.config.sink_configs[0].type ===
                                      'HTTP_WEBHOOK'
                                    ? 'green'
                                    : 'blue'
                            "
                            size="small"
                            bordered
                            class="task-status-tag"
                          >
                            {{
                              task.config.sink_configs.length > 1
                                ? 'MULTI-SINK'
                                : getSinkTypeLabel(task.config.sink_configs[0].type)
                            }}
                          </a-tag>
                          <a-tag v-else color="blue" size="small" bordered class="task-status-tag">MySQL 数据库</a-tag>
                        </div>
                      </div>

                      <div class="task-info-grid">
                        <div class="task-info-cell task-info-cell--level">
                          <span class="task-info-label">同步级别</span>
                          <span class="task-info-value">
                            {{
                              task.config.sync_level === "DATABASE"
                                ? "库级别"
                                : "表级别"
                            }}
                          </span>
                        </div>

                        <div class="task-info-cell task-info-cell--source">
                          <span class="task-info-label">源库</span>
                          <div class="task-info-value task-info-tags">
                            <template
                              v-if="task.config.source_databases?.length"
                            >
                              <a-tag
                                v-for="db in task.config.source_databases"
                                :key="db"
                                size="small"
                                color="arcoblue"
                                class="inline-tag"
                                bordered
                                >{{ db }}</a-tag
                              >
                            </template>

                            <template v-else>{{ task.config.source_schema || '-' }}</template>
                          </div>
                        </div>

                        <div class="task-info-cell task-info-cell--target">
                          <span class="task-info-label">目标端</span>
                          <div class="task-info-value task-info-tags">
                            <template
                              v-if="
                                task.config.sink_configs &&
                                task.config.sink_configs.length > 0
                              "
                            >
                              <template v-if="task.config.sink_configs.length > 1">
                                <a-tag size="small" color="purple" bordered>
                                  {{ task.config.sink_configs.length }} 个目标端
                                </a-tag>
                              </template>
                              <template v-else>
                                <template
                                  v-if="task.config.sink_configs[0].type === 'KAFKA'"
                                >
                                  <a-tag
                                    size="small"
                                    color="orange"
                                    class="text-ellipsis inline-tag"
                                    bordered
                                    :title="task.config.sink_configs[0].options?.topic"
                                  >
                                    Topic: {{ task.config.sink_configs[0].options?.topic || '-' }}
                                  </a-tag>
                                </template>
                                <template
                                  v-else-if="task.config.sink_configs[0].type === 'HTTP_WEBHOOK'"
                                >
                                  <a-tag
                                    size="small"
                                    color="green"
                                    class="text-ellipsis inline-tag"
                                    bordered
                                    :title="task.config.sink_configs[0].options?.url"
                                  >
                                    {{ task.config.sink_configs[0].options?.url || '-' }}
                                  </a-tag>
                                </template>
                                <template v-else>
                                  <a-tag
                                    v-for="mapping in getTaskDatabaseMappings(task)"
                                    :key="mapping.source"
                                    size="small"
                                    color="green"
                                    class="inline-tag"
                                    bordered
                                    :title="`${mapping.source} → ${mapping.target}`"
                                    >{{ mapping.target }}</a-tag
                                  >
                                </template>
                              </template>
                            </template>
                            <template v-else>
                              <a-tag
                                v-for="mapping in getTaskDatabaseMappings(task)"
                                :key="mapping.source"
                                size="small"
                                color="green"
                                class="inline-tag"
                                bordered
                                :title="`${mapping.source} → ${mapping.target}`"
                                >{{ mapping.target }}</a-tag
                              >
                            </template>
                          </div>
                        </div>

                        <div class="task-info-cell task-info-cell--count">
                          <span class="task-info-label">表数量</span>
                          <span class="task-info-value">
                            {{
                              task.config.sync_level === "DATABASE"
                                ? "全库"
                                : task.config.tables?.length || 0
                            }}
                          </span>
                        </div>
                      </div>

                      <div
                        v-if="task.context.status === 'RUNNING'"
                        class="task-progress"
                      >
                        <a-progress
                          :percent="getProgressRatio(task)"
                          :stroke-width="12"
                          status="normal"
                          :show-text="false"
                          style="flex: 1; margin: 0"
                          size="large"
                          color="var(--color-primary-light-4)"
                          track-color="var(--color-fill-2)"
                          animation
                        />

                        <div class="progress-details">
                          <span class="progress-text">
                            已处理: {{ task.context.processed_rows || 0 }} /
                            {{ task.context.total_rows || 0 }}
                          </span>
                          <span class="progress-percent-text">{{ getProgress(task) }}%</span>
                        </div>
                      </div>
                    </div>

                    <div class="task-card-actions">
                      <a-button size="small" @click="showTaskDetail(task)">
                        <template #icon><icon-eye /></template>
                        详情
                      </a-button>

                      <a-button size="small" @click="openDuplicateFromTask(task)">
                        <template #icon><icon-copy /></template>
                        复制新建
                      </a-button>

                      <a-button
                        v-if="task.context.status === 'PENDING' || task.context.status === 'PAUSED'"
                        size="small"
                        @click="openEditDialog(task)"
                      >
                        <template #icon><icon-edit /></template>
                        编辑
                      </a-button>

                      <a-button
                        v-if="task.context.status === 'PENDING' || task.context.status === 'PAUSED' || task.context.status === 'FAILED'"
                        type="primary"
                        size="small"
                        status="success"
                        @click="openStartTaskModal(task.config.id, 'immediate')"
                      >
                        <icon-play-arrow /> 启动
                      </a-button>

                      <a-button
                        v-if="task.context.status === 'PENDING' || task.context.status === 'PAUSED' || task.context.status === 'FAILED'"
                        size="small"
                        @click="openStartTaskModal(task.config.id, 'cron')"
                      >
                        <template #icon><icon-clock-circle /></template>
                        定时启动
                      </a-button>

                      <template v-if="task.context.status === 'SCHEDULED'">
                        <a-tooltip :content="'计划启动: ' + formatScheduledTime(task)">
                          <a-button
                            size="small"
                            status="warning"
                            @click="cancelSchedule(task.config.id)"
                          >
                            <template #icon><icon-clock-circle /></template>
                            取消定时
                          </a-button>
                        </a-tooltip>
                      </template>

                      <a-button
                        v-if="task.context.status === 'RUNNING'"
                        size="small"
                        status="warning"
                        @click="pauseTask(task.config.id)"
                      >
                        <template #icon><icon-pause /></template>
                        暂停
                      </a-button>

                      <a-button
                        v-if="task.context.status !== 'RUNNING' && task.context.status !== 'SCHEDULED'"
                        size="small"
                        status="danger"
                        @click="deleteTask(task.config.id)"
                      >
                        <template #icon><icon-delete /></template>
                        删除
                      </a-button>
                    </div>
                  </div>
                </a-card>
              </a-list-item>
            </a-list>

            <div class="task-pagination" v-if="taskPagination.total > 0">
              <a-pagination
                v-model:current="taskPagination.current"
                v-model:page-size="taskPagination.pageSize"
                :total="taskPagination.total"
                :page-size-options="['5', '10', '20', '50']"
                show-total
                show-page-size
                @change="(page, pageSize) => fetchTasks(page, pageSize)"
                @page-size-change="(pageSize) => fetchTasks(1, pageSize)"
              />
            </div>
          </a-card>
        </div>

        <!-- 系统配置页面 -->

        <div v-show="taskFormPage === 'none' && currentPage === 'config'" class="config-page-shell">
          <div class="config-hero">
            <div>
              <a-typography-title :heading="4" style="margin: 0 0 8px 0">系统配置</a-typography-title>
              <a-typography-text type="secondary">统一管理服务监听、日志、默认数据库与任务持久化配置。</a-typography-text>
            </div>
            <a-tag color="arcoblue" bordered>配置文件：etc/application.toml</a-tag>
          </div>

          <a-card class="config-section-card theme-config-card" :bordered="false">
            <template #title>界面主题</template>
            <div class="theme-option-grid">
              <button
                v-for="theme in uiThemeOptions"
                :key="theme.value"
                type="button"
                class="theme-option"
                :class="[`theme-option--${theme.value}`, { 'is-active': uiTheme === theme.value }]"
                @click="setUiTheme(theme.value)"
              >
                <span class="theme-option__swatch">
                  <span></span>
                  <span></span>
                  <span></span>
                </span>
                <span class="theme-option__content">
                  <span class="theme-option__title">{{ theme.label }}</span>
                  <span class="theme-option__desc">{{ theme.desc }}</span>
                </span>
                <span v-if="uiTheme === theme.value" class="theme-option__checked">已启用</span>
              </button>
            </div>
            <div class="config-hint">主题仅保存在当前浏览器本地，不会改写服务端配置文件。</div>
          </a-card>

          <a-row :gutter="16" class="config-summary-row">
            <a-col :span="8"><a-card class="config-summary-card" :bordered="false"><a-statistic title="HTTP 端口" :value="configForm.http.port" /></a-card></a-col>
            <a-col :span="8"><a-card class="config-summary-card" :bordered="false"><a-statistic title="Redis DB" :value="configForm.redis.db" /></a-card></a-col>
            <a-col :span="8"><a-card class="config-summary-card" :bordered="false"><a-statistic title="日志级别" :value="configForm.log.level?.toUpperCase() || '-'" /></a-card></a-col>
          </a-row>

          <a-card class="config-page-card" :bordered="false">
            <a-form :model="configForm" layout="vertical" @submit="saveConfig">
              <a-row :gutter="20">
                <a-col :span="12">
                  <a-card class="config-section-card" :bordered="false">
                    <template #title>基础连接</template>

                    <a-form-item label="HTTP 监听地址">
                      <a-input v-model="configForm.http.host" placeholder="0.0.0.0" />
                    </a-form-item>

                    <a-form-item label="HTTP 监听端口">
                      <a-input-number v-model="configForm.http.port" :min="1" :max="65535" style="width: 100%" />
                    </a-form-item>

                    <a-divider orientation="left" class="config-section-divider">Redis 状态持久化</a-divider>

                    <a-form-item label="Redis 主机">
                      <a-input v-model="configForm.redis.host" placeholder="127.0.0.1" />
                    </a-form-item>

                    <a-row :gutter="12">
                      <a-col :span="12">
                        <a-form-item label="Redis 端口">
                          <a-input-number v-model="configForm.redis.port" :min="1" :max="65535" style="width: 100%" />
                        </a-form-item>
                      </a-col>
                      <a-col :span="12">
                        <a-form-item label="数据库索引 (DB)">
                          <a-input-number v-model="configForm.redis.db" :min="0" :max="15" style="width: 100%" />
                        </a-form-item>
                      </a-col>
                    </a-row>

                    <a-form-item label="Redis 密码">
                      <a-input-password v-model="configForm.redis.password" placeholder="留空表示无密码" />
                    </a-form-item>
                  </a-card>
                </a-col>

                <a-col :span="12">
                  <a-card class="config-section-card" :bordered="false">
                    <template #title>
                      <span>日志与默认环境</span>
                      <a-tag color="green" size="small" style="margin-left: 8px">热加载</a-tag>
                    </template>

                    <a-form-item label="日志级别">
                      <a-select v-model="configForm.log.level">
                        <a-option value="debug">Debug</a-option>
                        <a-option value="info">Info</a-option>
                        <a-option value="warn">Warn</a-option>
                        <a-option value="error">Error</a-option>
                      </a-select>
                    </a-form-item>

                    <a-form-item label="输出开关">
                      <a-space direction="vertical" style="width: 100%">
                        <a-checkbox v-model="configForm.log.console.enable">开启控制台标准输出 (Stdout)</a-checkbox>
                        <a-checkbox v-model="configForm.log.file.enable">开启文件持久化输出 (File)</a-checkbox>
                      </a-space>
                    </a-form-item>

                    <a-form-item>
                      <a-button type="primary" status="success" :loading="logApplying" @click="applyLogConfig" style="width: 100%">
                        <template #icon><icon-sync /></template>
                        立即应用日志配置（无需重启）
                      </a-button>
                      <div class="config-hint">修改日志级别或输出开关后点击此按钮，配置即刻生效并持久化到配置文件。</div>
                    </a-form-item>

                    <a-divider orientation="left" class="config-section-divider">默认数据库环境</a-divider>

                    <a-form-item label="默认源库地址">
                      <a-input v-model="configForm.datasource.host" />
                    </a-form-item>

                    <a-form-item label="默认目标库地址">
                      <a-input v-model="configForm.target.host" />
                    </a-form-item>

                    <a-form-item label="调试模式 (Debug)">
                      <a-switch v-model="configForm.datasource.debug" />
                    </a-form-item>
                  </a-card>
                </a-col>
              </a-row>

              <a-card class="config-section-card config-storage-card" :bordered="false">
                <template #title>
                  <span>任务数据持久化</span>
                </template>

                <a-form-item label="持久化模式">
                  <a-radio-group v-model="configForm.storage.mode" type="button">
                    <a-radio value="file">本地文件</a-radio>
                    <a-radio value="mysql">MySQL 数据库</a-radio>
                  </a-radio-group>
                </a-form-item>

                <template v-if="configForm.storage.mode === 'file'">
                  <a-form-item label="数据目录">
                    <a-input v-model="configForm.storage.data_dir" placeholder="data" />
                  </a-form-item>
                </template>

                <template v-if="configForm.storage.mode === 'mysql'">
                  <a-row :gutter="16">
                    <a-col :span="8">
                      <a-form-item label="MySQL 主机">
                        <a-input v-model="configForm.storage.host" placeholder="127.0.0.1" />
                      </a-form-item>
                    </a-col>
                    <a-col :span="4">
                      <a-form-item label="端口">
                        <a-input-number v-model="configForm.storage.port" :min="1" :max="65535" style="width: 100%" />
                      </a-form-item>
                    </a-col>
                    <a-col :span="6">
                      <a-form-item label="数据库">
                        <a-input v-model="configForm.storage.database" />
                      </a-form-item>
                    </a-col>
                    <a-col :span="6">
                      <a-form-item label="用户名">
                        <a-input v-model="configForm.storage.username" />
                      </a-form-item>
                    </a-col>
                  </a-row>

                  <a-form-item label="密码">
                    <a-input-password v-model="configForm.storage.password" />
                  </a-form-item>
                </template>
              </a-card>

              <div class="config-actions-bar">
                <a-button type="primary" size="large" :loading="configLoading" @click="saveConfig">
                  保存并同步到 application.toml
                </a-button>
                <a-typography-text type="secondary">
                  <icon-info-circle /> 修改配置后将直接改写服务器磁盘文件，部分底层服务（如端口监听）需重启 Go 程序生效。
                </a-typography-text>
              </div>
            </a-form>
          </a-card>
        </div>
      </a-layout-content>
    </a-layout>

    <!-- 任务详情抽屉 -->

    <a-drawer
      v-model:visible="detailDrawerVisible"
      title="任务详情"
      :width="600"
      :footer="false"
    >
      <div v-if="selectedTaskForDetail" class="task-detail">
        <!-- 基本信息 -->

        <a-descriptions title="基本信息" :column="2" bordered>
          <a-descriptions-item label="任务ID">
            {{ selectedTaskForDetail.config.id }}
          </a-descriptions-item>

          <a-descriptions-item label="任务名称">
            {{ selectedTaskForDetail.config.name }}
          </a-descriptions-item>

          <a-descriptions-item label="同步级别">
            {{
              selectedTaskForDetail.config.sync_level === "DATABASE"
                ? "库级别"
                : "表级别"
            }}
          </a-descriptions-item>

          <a-descriptions-item label="同步模式">
            <a-tag
              v-if="selectedTaskForDetail.config.mode === 'FULL'"
              color="blue"
              >全量同步</a-tag
            >

            <a-tag
              v-else-if="selectedTaskForDetail.config.mode === 'INCREMENTAL'"
              color="green"
              >增量同步</a-tag
            >

            <a-tag v-else color="purple">全量+增量</a-tag>
          </a-descriptions-item>

          <a-descriptions-item
            v-if="selectedTaskForDetail.config.sync_level === 'DATABASE'"
            label="源数据库列表"
            :span="2"
          >
            <a-space wrap>
              <a-tag
                v-for="db in selectedTaskForDetail.config.source_databases ||
                []"
                :key="db"
                color="blue"
                >{{ db }}</a-tag
              >

              <span
                v-if="
                  !(selectedTaskForDetail.config.source_databases || []).length
                "
                >-</span
              >
            </a-space>
          </a-descriptions-item>

          <a-descriptions-item
            v-if="selectedTaskForDetail.config.sync_level === 'DATABASE'"
            label="数据库映射"
            :span="2"
          >
            <a-space wrap>
              <span
                v-for="mapping in getTaskDatabaseMappings(selectedTaskForDetail)"
                :key="mapping.source"
                style="
                  display: inline-flex;
                  align-items: center;
                  margin-right: 12px;
                "
              >
                <a-tag color="blue">{{ mapping.source }}</a-tag>

                <span style="margin: 0 4px">→</span>

                <a-tag color="green">{{ mapping.target }}</a-tag>
              </span>
            </a-space>
          </a-descriptions-item>

          <a-descriptions-item
            v-if="selectedTaskForDetail.config.sync_level !== 'DATABASE'"
            label="源数据库"
            :span="2"
          >
            <a-space wrap>
              <template
                v-if="
                  (selectedTaskForDetail.config.source_databases || []).length
                "
              >
                <a-tag
                  v-for="db in selectedTaskForDetail.config.source_databases"
                  :key="db"
                  color="arcoblue"
                  >{{ db }}</a-tag
                >
              </template>

              <span v-else>{{
                selectedTaskForDetail.config.source_schema || "-"
              }}</span>
            </a-space>
          </a-descriptions-item>

          <a-descriptions-item
            v-if="selectedTaskForDetail.config.sync_level !== 'DATABASE'"
            label="数据库映射"
            :span="2"
          >
            <a-space wrap>
              <span
                v-for="mapping in getTaskDatabaseMappings(selectedTaskForDetail)"
                :key="mapping.source"
                style="
                  display: inline-flex;
                  align-items: center;
                  margin-right: 12px;
                "
              >
                <a-tag color="blue">{{ mapping.source }}</a-tag>

                <span style="margin: 0 4px">→</span>

                <a-tag color="green">{{ mapping.target }}</a-tag>
              </span>
            </a-space>
          </a-descriptions-item>

          <a-descriptions-item
            v-if="selectedTaskForDetail.config.sync_level !== 'DATABASE'"
            label="目标数据库"
            :span="2"
          >
            <a-space wrap>
              <a-tag
                v-for="mapping in getTaskDatabaseMappings(selectedTaskForDetail)"
                :key="`target-${mapping.source}`"
                color="green"
                >{{ mapping.target }}</a-tag
              >
            </a-space>
          </a-descriptions-item>

          <a-descriptions-item label="批量大小">
            {{ selectedTaskForDetail.config.batch_size }}
          </a-descriptions-item>

          <a-descriptions-item label="表并发数">
            {{ selectedTaskForDetail.config.worker_count }}
          </a-descriptions-item>

          <a-descriptions-item label="单表内并发">
            {{
              selectedTaskForDetail.config.intra_table_worker_count > 0
                ? selectedTaskForDetail.config.intra_table_worker_count
                : "默认（≤16）"
            }}
          </a-descriptions-item>
          <a-descriptions-item label="无主键 LIMIT 1">
            {{ selectedTaskForDetail.config.enable_limit_one ? '开启' : '关闭' }}
          </a-descriptions-item>
          <a-descriptions-item label="并行事务提交间隔">
            {{ selectedTaskForDetail.config.tx_commit_every_n_parallel > 0 ? selectedTaskForDetail.config.tx_commit_every_n_parallel : '默认（5批）' }}
          </a-descriptions-item>
          <a-descriptions-item label="启用索引优化">
            {{ selectedTaskForDetail.config.optimize_index ? '开启' : '关闭' }}
          </a-descriptions-item>
          <a-descriptions-item v-if="selectedTaskForDetail.config.optimize_index" label="索引回放并发度">
            {{
              selectedTaskForDetail.config.index_restore_worker_count > 0
                ? selectedTaskForDetail.config.index_restore_worker_count
                : '自动（min(worker_count, 4)）'
            }}
          </a-descriptions-item>
          <a-descriptions-item label="目标库只读保护">
            {{ selectedTaskForDetail.config.enable_read_only ? '开启' : '关闭' }}
          </a-descriptions-item>
          <a-descriptions-item label="DDL前删除目标">
            {{ selectedTaskForDetail.config.enable_drop_table_before_ddl ? '开启' : '关闭' }}
          </a-descriptions-item>
        </a-descriptions>

        <!-- 源端和目标端配置 -->

        <a-descriptions
          title="源数据库配置"
          :column="2"
          bordered
          style="margin-top: 20px"
        >
          <a-descriptions-item label="主机地址">
            {{ selectedTaskForDetail.config.source_db?.host || configForm.datasource?.host || '-' }}
          </a-descriptions-item>

          <a-descriptions-item label="端口">
            {{ selectedTaskForDetail.config.source_db?.port || configForm.datasource?.port || '-' }}
          </a-descriptions-item>

          <a-descriptions-item label="用户名">
            {{ selectedTaskForDetail.config.source_db?.username || configForm.datasource?.username || '-' }}
          </a-descriptions-item>

          <a-descriptions-item label="密码">
            ******
          </a-descriptions-item>
        </a-descriptions>

        <template
          v-if="
            !selectedTaskForDetail.config.sink_configs ||
            selectedTaskForDetail.config.sink_configs.length === 0
          "
        >
          <a-descriptions
            title="目标数据库配置 (MySQL)"
            :column="2"
            bordered
            style="margin-top: 20px"
          >
            <a-descriptions-item label="主机地址">
              {{ selectedTaskForDetail.config.target_db?.host || configForm.target?.host || '-' }}
            </a-descriptions-item>

            <a-descriptions-item label="端口">
              {{ selectedTaskForDetail.config.target_db?.port || configForm.target?.port || '-' }}
            </a-descriptions-item>

            <a-descriptions-item label="用户名">
              {{ selectedTaskForDetail.config.target_db?.username || configForm.target?.username || '-' }}
            </a-descriptions-item>

            <a-descriptions-item label="密码">
              ******
            </a-descriptions-item>
          </a-descriptions>
        </template>
        <template v-else>
          <div
            v-for="(sink, index) in selectedTaskForDetail.config.sink_configs"
            :key="index"
          >
            <a-descriptions
              :title="`目标端配置 (${sink.type})`"
              :column="2"
              bordered
              style="margin-top: 20px"
            >
              <template v-if="sink.type === 'MYSQL'">
                <a-descriptions-item label="主机地址">{{ sink.options?.host || '-' }}</a-descriptions-item>
                <a-descriptions-item label="端口">{{ sink.options?.port || '-' }}</a-descriptions-item>
                <a-descriptions-item label="用户名">{{ sink.options?.username || '-' }}</a-descriptions-item>
                <a-descriptions-item label="密码">******</a-descriptions-item>
              </template>

              <template v-if="sink.type === 'KAFKA'">
                <a-descriptions-item label="Brokers">
                  {{
                    Array.isArray(sink.options?.brokers)
                      ? sink.options.brokers.join(', ')
                      : sink.options?.brokers || '-'
                  }}
                </a-descriptions-item>
                <a-descriptions-item label="Topic">{{ sink.options?.topic || '-' }}</a-descriptions-item>
                <a-descriptions-item label="Batch Size">{{ sink.options?.batch_size || '-' }}</a-descriptions-item>
                <a-descriptions-item label="Required Acks">{{ sink.options?.required_acks || '-' }}</a-descriptions-item>
              </template>

              <template v-if="sink.type === 'HTTP_WEBHOOK'">
                <a-descriptions-item label="URL">{{ sink.options?.url || '-' }}</a-descriptions-item>
                <a-descriptions-item label="Method">{{ sink.options?.method || '-' }}</a-descriptions-item>
                <a-descriptions-item label="Timeout (ms)">{{ sink.options?.timeout_ms || '-' }}</a-descriptions-item>
                <a-descriptions-item label="Retry Times">{{ sink.options?.retry_times || '-' }}</a-descriptions-item>
              </template>
            </a-descriptions>
          </div>
        </template>

        <!-- 执行状态 -->

        <a-descriptions
          title="执行状态"
          :column="2"
          bordered
          style="margin-top: 20px"
        >
          <a-descriptions-item label="状态">
            <a-tag
              :color="getStatusColor(selectedTaskForDetail.context.status)"
            >
              {{ getStatusText(selectedTaskForDetail.context.status) }}
            </a-tag>
            <span v-if="selectedTaskForDetail.context.status === 'SCHEDULED' && selectedTaskForDetail.context.scheduled_at" style="margin-left: 8px; color: #165DFF; font-size: 13px;">
              <icon-clock-circle /> {{ formatScheduledTime(selectedTaskForDetail) }}
            </span>
            <span v-if="selectedTaskForDetail.context.cron_expression" style="margin-left: 8px; color: #86909C; font-size: 12px;">
              {{ selectedTaskForDetail.context.cron_expression }}
            </span>
          </a-descriptions-item>

          <a-descriptions-item label="进度">
            {{ getProgress(selectedTaskForDetail) }}%
          </a-descriptions-item>

          <a-descriptions-item label="已处理行数">
            {{ getRowCounts(selectedTaskForDetail).processed }}
          </a-descriptions-item>

          <a-descriptions-item label="总行数">
            {{ getRowCounts(selectedTaskForDetail).total }}
          </a-descriptions-item>

          <a-descriptions-item label="当前位置">
            {{ selectedTaskForDetail.context.current_position || "-" }}
          </a-descriptions-item>

          <a-descriptions-item label="运行时长">
            {{
              calculateDuration(
                selectedTaskForDetail.context.start_time,
                selectedTaskForDetail.context.end_time,
              )
            }}
          </a-descriptions-item>

          <a-descriptions-item label="开始时间">
            {{ formatTime(selectedTaskForDetail.context.start_time) }}
          </a-descriptions-item>

          <a-descriptions-item label="结束时间">
            {{ formatTime(selectedTaskForDetail.context.end_time) }}
          </a-descriptions-item>
        </a-descriptions>

        <!-- 错误信息 -->

        <a-descriptions
          v-if="selectedTaskForDetail.context.error_stack"
          title="错误信息"
          :column="1"
          bordered
          style="margin-top: 20px"
        >
          <a-descriptions-item label="错误详情">
            <a-alert type="error" style="margin: 0">
              <pre
                style="margin: 0; white-space: pre-wrap; word-break: break-word"
                >{{ selectedTaskForDetail.context.error_stack }}</pre
              >
            </a-alert>
          </a-descriptions-item>
        </a-descriptions>

        <!-- 同步表列表（仅表级别显示） -->

        <a-descriptions
          v-if="selectedTaskForDetail.config.sync_level !== 'DATABASE'"
          title="同步表"
          :column="1"
          bordered
          style="margin-top: 20px"
        >
          <a-descriptions-item label="表列表">
            <a-space wrap>
              <a-tag
                v-for="table in selectedTaskForDetail.config.tables"
                :key="table"
                >{{ table }}</a-tag
              >

              <span
                v-if="
                  !selectedTaskForDetail.config.tables ||
                  selectedTaskForDetail.config.tables.length === 0
                "
              >
                全库同步
              </span>
            </a-space>
          </a-descriptions-item>
        </a-descriptions>

        <!-- 操作按钮 -->

        <div style="margin-top: 20px; text-align: right">
          <a-space>
            <a-button
              type="outline"
              @click="
                openDuplicateFromTask(selectedTaskForDetail);
                detailDrawerVisible = false;
              "
            >
              <template #icon><icon-copy /></template>

              复制新建
            </a-button>

            <a-button
              v-if="
                selectedTaskForDetail.context.status === 'PENDING' ||
                selectedTaskForDetail.context.status === 'PAUSED' ||
                selectedTaskForDetail.context.status === 'FAILED'
              "
              type="primary"
              status="success"
              @click="openStartTaskModal(selectedTaskForDetail.config.id, 'immediate'); detailDrawerVisible = false;"
            >
              <icon-play-arrow /> 启动
            </a-button>

            <a-button
              v-if="
                selectedTaskForDetail.context.status === 'PENDING' ||
                selectedTaskForDetail.context.status === 'PAUSED' ||
                selectedTaskForDetail.context.status === 'FAILED'
              "
              @click="openStartTaskModal(selectedTaskForDetail.config.id, 'cron'); detailDrawerVisible = false;"
            >
              <template #icon><icon-clock-circle /></template>
              定时启动
            </a-button>

            <a-button
              v-if="selectedTaskForDetail.context.status === 'SCHEDULED'"
              status="warning"
              @click="
                cancelSchedule(selectedTaskForDetail.config.id);
                detailDrawerVisible = false;
              "
            >
              <template #icon><icon-clock-circle /></template>
              取消定时
            </a-button>

            <a-button
              v-if="selectedTaskForDetail.context.status === 'RUNNING'"
              status="warning"
              @click="
                pauseTask(selectedTaskForDetail.config.id);
                detailDrawerVisible = false;
              "
            >
              <template #icon><icon-pause /></template>

              暂停
            </a-button>
          </a-space>
        </div>
      </div>
    </a-drawer>
  </a-layout>

  <!-- 启动任务弹窗（全局，主界面与任务详情页共用） -->
  <a-modal
    v-model:visible="startModalVisible"
    :title="startMode === 'immediate' ? '确认立即启动' : 'Cron 定时启动'"
    @ok="confirmStartTask"
    @cancel="startModalVisible = false"
    ok-text="确认"
    cancel-text="取消"
    :width="720"
  >
    <a-form layout="vertical">
      <a-alert
        v-if="startMode === 'immediate'"
        type="warning"
        :show-icon="true"
        style="margin-bottom: 16px"
        title="确认后将立即启动任务"
        description="如果你希望设置定时启动，请切换到定时启动方式后再提交。"
      />

      <template v-else>
        <a-alert
          type="info"
          :show-icon="true"
          style="margin-bottom: 16px"
          title="Cron 支持标准语法，并兼容 L / W / #"
          description="例如：0 9 * * 1-5 表示每周一到周五 09:00；0 0 L * * 表示每月最后一天 00:00。"
        />

        <a-form-item label="Cron 表达式">
          <a-input v-model="scheduleCron" placeholder="例如：0 9 * * 1-5" />
        </a-form-item>

        <a-form-item label="时区">
          <a-input v-model="scheduleTimezone" placeholder="例如：Asia/Shanghai" />
        </a-form-item>

        <a-form-item label="快捷模板">
          <a-space wrap>
            <a-button size="small" @click="scheduleCron = '0 9 * * 1-5'">工作日 9:00</a-button>
            <a-button size="small" @click="scheduleCron = '30 9 * * 1-5'">工作日 9:30</a-button>
            <a-button size="small" @click="scheduleCron = '0 0 L * *'">每月最后一个工作日 00:00</a-button>
            <a-button size="small" @click="scheduleCron = '0 10 ? * 1#1'">每月第一个周一 10:00</a-button>
          </a-space>
        </a-form-item>

        <a-typography-text type="secondary" style="font-size: 12px">
          支持标准 cron 与扩展语义。提交后系统会保存原始表达式，并据此计算下一次触发时间。
        </a-typography-text>
      </template>
    </a-form>
  </a-modal>
</template>

<style scoped>
.layout-container {
  height: 100vh;

  background: #f5f7fa;
}

.task-detail-page-layout {
  height: 100vh;

  background: #f5f7fa;
}

.detail-page-header {
  display: flex;

  align-items: center;

  justify-content: space-between;

  padding: 0 20px;

  background: #fff;

  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);

  height: 56px;

  position: sticky;

  top: 0;

  z-index: 10;
}

.detail-header-left {
  display: flex;

  align-items: center;
}

.detail-header-right {
  display: flex;

  align-items: center;
}

.detail-page-content {
  padding: 20px;

  overflow-y: auto;
}

.detail-page-body {
  max-width: 1400px;

  margin: 0 auto;
}

.detail-overview-row {
  margin-bottom: 16px;
}

.overview-card {
  height: 100%;
}

.overview-label {
  font-size: 13px;

  color: #86909c;

  margin-bottom: 8px;
}

.overview-value {
  font-size: 28px;

  font-weight: 600;

  color: #1d2129;

  line-height: 1.2;
}

.overview-value-sm {
  font-size: 20px;
}

.overview-sub {
  margin-top: 8px;

  font-size: 12px;

  color: #86909c;

  word-break: break-all;
}

.overview-unit {
  font-size: 13px;

  font-weight: 400;

  color: #86909c;

  margin-left: 2px;
}

.runtime-overview-row {
  margin-bottom: 16px;
}

.runtime-current-table {
  word-break: break-all;

  cursor: help;
}

.runtime-section-title {
  display: flex;

  align-items: center;

  gap: 6px;

  font-size: 15px;

  font-weight: 600;

  color: #1d2129;

  margin: 4px 0 12px;
}

.runtime-table-name {
  font-weight: 500;
}

.runtime-rows {
  font-variant-numeric: tabular-nums;
}

.runtime-rows-total {
  color: #86909c;
}

.runtime-speed {
  color: #165dff;

  font-variant-numeric: tabular-nums;
}

.runtime-speed-muted {
  color: #c9cdd4;
}

.runtime-time-cell {
  font-size: 12px;

  line-height: 1.6;

  color: #4e5969;
}

.runtime-time-end {
  color: #86909c;
}

.detail-tabs {
  background: #fff;

  border-radius: 4px;

  padding: 16px;
}

.task-form-full-page {
  max-width: 1100px;

  margin: 0 auto;

  padding: 8px 0 40px;
}

.sider {
  background: linear-gradient(180deg, #1d2129 0%, #165dff 100%);

  display: flex;

  flex-direction: column;
}

.logo {
  height: 64px;

  display: flex;

  align-items: center;

  padding: 0 20px;

  color: #fff;

  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.logo-icon {
  width: 32px;

  height: 32px;

  background: rgba(255, 255, 255, 0.2);

  border-radius: 8px;

  display: flex;

  align-items: center;

  justify-content: center;

  margin-right: 12px;

  font-size: 18px;
}

.logo-text {
  font-size: 16px;

  font-weight: 600;

  letter-spacing: 0.5px;
}

.sider-menu {
  flex: 1;

  background: transparent !important;

  padding: 12px 8px;

  width: 100% !important;
}

/* 禁止菜单收缩 */

.sider-menu:not(.arco-menu-collapsed) {
  width: 100% !important;
}

/* 菜单inner容器 - 禁止动画 */

.sider-menu :deep(.arco-menu-inner) {
  display: flex !important;

  flex-direction: column !important;

  opacity: 1 !important;

  animation: none !important;

  transition: none !important;
}

/* 菜单项 - 常亮显示，禁止所有动画和过渡 */

.sider-menu :deep(.arco-menu-item) {
  color: #fff !important;

  background: transparent !important;

  margin: 4px 0;

  border-radius: 6px;

  opacity: 1 !important;

  visibility: visible !important;

  padding: 0 12px !important;

  height: 40px !important;

  line-height: 40px !important;

  animation: none !important;

  transition: none !important;

  transform: none !important;
}

/* 强制覆盖Arco Design菜单项的所有状态背景 */

.sider-menu :deep(.arco-menu-item),
.sider-menu :deep(.arco-menu-item.arco-menu-selected),
.sider-menu :deep(.arco-menu-item:hover),
.sider-menu :deep(.arco-menu-item:focus),
.sider-menu :deep(.arco-menu-item:active),
.sider-menu :deep(.arco-menu-item[data-key="tasks"]),
.sider-menu :deep(.arco-menu-item[data-key="config"]) {
  background: transparent !important;

  background-color: transparent !important;
}

/* 菜单项图标 - 常亮显示，禁止动画 */

.sider-menu :deep(.arco-menu-item .arco-menu-icon) {
  color: #fff !important;

  opacity: 1 !important;

  visibility: visible !important;

  display: inline-flex !important;

  margin-right: 12px !important;

  animation: none !important;

  transition: none !important;
}

/* 菜单项文字 - 常亮显示，禁止动画 */

.sider-menu :deep(.arco-menu-item .arco-menu-title) {
  color: #fff !important;

  opacity: 1 !important;

  visibility: visible !important;

  display: inline !important;

  animation: none !important;

  transition: none !important;

  transform: none !important;
}

/* 菜单项内所有内容 */

.sider-menu :deep(.arco-menu-item *) {
  color: #fff !important;

  opacity: 1 !important;

  visibility: visible !important;

  animation: none !important;

  transition: none !important;
}

/* 禁止Arco Design菜单的所有动画效果 */

.sider-menu :deep(.arco-menu-collapse-icon) {
  display: none !important;
}

/* 禁止菜单展开/折叠的宽度动画 */

.sider-menu :deep(.arco-menu) {
  transition: none !important;

  animation: none !important;
}

/* 悬停状态 - 只改变背景，不改变透明度 */

.sider-menu :deep(.arco-menu-item:hover) {
  background: rgba(255, 255, 255, 0.15) !important;

  color: #fff !important;
}

/* 选中状态 */

.sider-menu :deep(.arco-menu-item.arco-menu-selected) {
  background: rgba(255, 255, 255, 0.25) !important;

  color: #fff !important;
}

/* 禁止所有伪元素动画 */

.sider-menu :deep(.arco-menu-item::before),
.sider-menu :deep(.arco-menu-item::after) {
  animation: none !important;

  transition: none !important;
}

/* 确保菜单图标svg也常亮 */

.sider-menu :deep(.arco-menu-item svg) {
  opacity: 1 !important;

  visibility: visible !important;

  color: #fff !important;
}

.sider-footer {
  padding: 16px 20px;

  border-top: 1px solid rgba(255, 255, 255, 0.1);
}

.header {
  background: #fff;

  padding: 0 24px;

  display: flex;

  justify-content: space-between;

  align-items: center;

  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);

  z-index: 10;
}

.content {
  padding: 24px;

  overflow-y: auto;
}

.stat-cards {
  margin-bottom: 24px;
}

.stat-card {
  border-radius: 8px;
}

.stat-icon {
  font-size: 20px;

  margin-right: 8px;
}

.stat-icon.blue {
  color: #165dff;
}

.stat-icon.green {
  color: #00b42a;
}

.stat-icon.red {
  color: #f53f3f;
}

.task-list-card {
  border-radius: 16px;
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
  border: 1px solid var(--app-border-soft);
  overflow: hidden;
}

.task-list-card :deep(.arco-card-header) {
  border-bottom: 1px solid var(--app-border-soft);
  padding: 20px 24px 18px;
  height: auto;
  min-height: 72px;
  overflow: visible;
  align-items: center;
}

.task-list-card :deep(.arco-card-body) {
  padding: 20px 24px 24px;
}

.task-list-header {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 20px 24px;
  width: 100%;
}

.task-list-card :deep(.arco-card-header-title) {
  overflow: visible;
  white-space: normal;
  text-overflow: clip;
  flex: 1;
  min-width: 0;
  height: auto;
  line-height: 1.5;
}

.task-list-title-wrap {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 0;
  padding: 2px 0;
}

.task-list-title-wrap :deep(.arco-typography),
.task-list-title-wrap :deep(.arco-typography-secondary) {
  overflow: visible;
  white-space: normal;
  line-height: 1.5;
  margin: 0;
}

.task-list-title-wrap :deep(h6.arco-typography) {
  line-height: 1.4;
}

.task-list-toolbar {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: nowrap;
  flex-shrink: 0;
}

.task-list-toolbar :deep(.arco-select),
.task-list-toolbar :deep(.arco-input-search) {
  flex-shrink: 0;
}

.task-list-toolbar :deep(.arco-select-view-single),
.task-list-toolbar :deep(.arco-input-wrapper) {
  height: 32px;
}

.task-list-toolbar :deep(.arco-tag) {
  height: 32px;
  display: inline-flex;
  align-items: center;
  flex-shrink: 0;
  margin: 0;
}

.task-filter-panel {
  margin-bottom: 18px;
  border: 1px solid #e5eaf3;
  border-radius: 16px;
  box-shadow: 0 8px 24px rgba(15, 23, 42, 0.04);
  overflow: hidden;
}

.task-filter-panel :deep(.arco-card-header) {
  padding: 16px 20px 12px;
  border-bottom: 1px solid #edf2f7;
}

.task-filter-panel :deep(.arco-card-body) {
  padding: 16px 20px 20px;
}

.task-filter-panel__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.task-filter-panel__title {
  font-size: 14px;
  font-weight: 600;
  color: #1d2129;
}

.task-filter-panel__desc {
  margin-top: 4px;
  font-size: 12px;
  color: #86909c;
}

.task-filter-panel__actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.task-filter-summary {
  background: linear-gradient(180deg, #f8fbff 0%, #ffffff 100%);
  border: 1px solid #e5eaf3;
  border-radius: 12px;
  padding: 12px 14px;
  margin-bottom: 14px;
}

.task-filter-summary__title {
  font-size: 13px;
  font-weight: 600;
  color: #1d2129;
  margin-bottom: 8px;
}

.task-filter-summary__chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.task-filter-summary__empty {
  font-size: 12px;
  color: var(--app-muted);
}

.empty-state {
  padding: 60px 0;

  text-align: center;
}

.empty-state--card {
  background: var(--app-surface-soft);
  border: 1px dashed var(--app-border);
  border-radius: 14px;
  margin: 4px 0 18px;
}

.task-list {
  margin-top: 4px;
}

.task-item {
  padding: 0 !important;

  margin-bottom: 16px;

  border: none !important;
}

.task-item:last-child {
  margin-bottom: 0;
}

.task-card-inner {
  background: linear-gradient(180deg, #ffffff 0%, #fbfdff 100%);
  border: 1px solid #edf2f7;
  border-radius: 14px;
  width: 100%;
  box-shadow: 0 6px 18px rgba(15, 23, 42, 0.04);
}

.task-card-inner :deep(.arco-card-body) {
  padding: 20px;
}

.task-card-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 240px;
  gap: 16px;
  align-items: start;
}

.task-card-main {
  min-width: 0;
}

.task-card-actions {
  display: flex;
  flex-direction: column;
  gap: 8px;
  align-items: stretch;
  justify-content: flex-start;
  padding-left: 16px;
  border-left: 1px solid #edf2f7;
  position: sticky;
  top: 12px;
}

.task-card-actions :deep(.arco-btn) {
  width: 100%;
  justify-content: center;
}

.task-header {
  display: flex;

  justify-content: space-between;

  align-items: center;

  margin-bottom: 12px;
}

.task-title {
  display: flex;

  align-items: center;

  gap: 10px;
  flex-wrap: wrap;
}

.task-status-tag {
  margin-right: 0;
}

.inline-tag {
  margin-right: 6px;
  margin-bottom: 4px;
}

.task-desc {
  margin-bottom: 12px;
}

.task-progress {
  padding: 14px 16px;
  background: #f8fbff;
  border: 1px solid #e6f0ff;
  border-radius: 12px;
}

.progress-details {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
  margin-top: 8px;
}

.progress-text {
  font-size: 12px;
  color: var(--color-text-3);
}

.progress-percent-text {
  font-size: 13px;
  font-weight: 600;
  color: var(--color-primary-light-4);
}

.task-pagination {
  display: flex;
  justify-content: flex-end;
  margin-top: 18px;
  padding-top: 16px;
  border-top: 1px solid #edf2f7;
}

.task-progress {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 16px;
}

.progress-details {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

.progress-text {
  font-size: 12px;
  color: var(--color-text-3);
}

.progress-percent-text {
  font-size: 13px;
  font-weight: 500;
  color: var(--color-primary-light-4);
}

.task-actions {
  display: flex;

  justify-content: flex-end;

  gap: 8px;

  border-top: 1px solid #e5e6eb;

  padding-top: 12px;
}

.task-pagination {
  display: flex;
  justify-content: flex-end;
  margin-top: 16px;
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

.table-toolbar {
  display: flex;

  align-items: center;

  gap: 8px;

  margin-bottom: 8px;

  width: 100%;
}

.table-search-input {
  flex: 1;

  min-width: 260px;
}

.table-selector-panel {
  height: 100%;

  min-height: 460px;

  border: 1px solid #e5e6eb;

  border-radius: 8px;

  background: #fff;

  padding: 12px;
}

.table-selector-form-item {
  margin-bottom: 0;
}

.table-selector-form-item :deep(.arco-form-item-content-wrapper) {
  width: 100%;
}

.table-selector-form-item :deep(.arco-form-item-content) {
  width: 100%;

  display: block;
}

.table-list-panel {
  width: 100%;

  height: 430px;

  overflow-y: auto;

  border: 1px solid var(--app-border-soft);

  border-radius: 6px;

  padding: 10px;

  background: var(--app-surface-soft);

  display: block;
}

.table-checkbox-group {
  width: 100%;

  display: block;
}

.table-list-panel > :deep(*) {
  width: 100%;
}

.table-list-grid {
  display: grid;

  width: 100%;

  grid-template-columns: repeat(2, minmax(0, 1fr));

  gap: 8px 12px;
}

.table-list-item {
  width: 100%;

  min-width: 0;
}

.table-list-item :deep(.arco-checkbox) {
  width: 100%;
}

.table-list-item :deep(.arco-checkbox-label) {
  display: inline-flex;

  align-items: center;

  gap: 6px;

  flex-wrap: wrap;
}

.advanced-config-row {
  margin-top: 16px;
}

.advanced-config-card {
  background: #fff;

  border-radius: 8px;
}

.table-mapping-panel {
  margin-bottom: 12px;

  border: 1px solid #e5e6eb;

  border-radius: 4px;

  padding: 10px 12px;

  background: #fafbfc;
}

.table-mapping-title {
  margin-bottom: 8px;

  color: #4e5969;

  font-size: 13px;

  font-weight: 500;
}

.table-mapping-list {
  display: flex;

  flex-direction: column;

  gap: 8px;
}

.table-mapping-item {
  display: flex;

  align-items: center;

  gap: 8px;
}

.table-mapping-source {
  min-width: 120px;

  color: #1d2129;

  font-size: 13px;
}

.table-db-collapse {
  border: 1px solid #e5e6eb;

  border-radius: 4px;

  background: #fff;
}

.table-db-panel-toolbar {
  display: flex;

  justify-content: flex-end;

  gap: 4px;

  margin-bottom: 8px;
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

.select-type-page {
  max-width: 800px;
  margin: 40px auto;
  padding: 0 24px;
}

.select-type-header {
  text-align: center;
  margin-bottom: 40px;
}

.type-cards-container {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 24px;
}

.type-card {
  border-radius: 8px;
  transition: all 0.3s ease;
  cursor: pointer;
  border: 1px solid transparent;
}

.type-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08);
  border-color: rgb(var(--primary-6));
}

.type-card :deep(.arco-card-body) {
  display: flex;
  align-items: flex-start;
  gap: 16px;
  padding: 24px;
}

.type-icon {
  width: 48px;
  height: 48px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 24px;
  flex-shrink: 0;
}

.mysql-icon {
  background: #e8f3ff;
  color: rgb(var(--primary-6));
}

.kafka-icon {
  background: #fff3e8;
  color: rgb(var(--orange-6));
}

.webhook-icon {
  background: #e8ffee;
  color: rgb(var(--green-6));
}

.multi-icon {
  background: #f2f3f5;
  color: var(--color-text-1);
}

.type-content h5 {
  margin: 0 0 8px 0;
  line-height: 1.4;
}

.type-content span {
  font-size: 13px;
  line-height: 1.5;
  display: block;
}

.text-ellipsis {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  display: inline-block;
  vertical-align: bottom;
}
/* Layout refinements for task list filters and table-level sync configuration */
.task-base-config-row {
  align-items: flex-start;
}

.table-config-row {
  margin-top: 4px;
  align-items: stretch;
}

.table-config-row > :deep(.arco-col) {
  display: flex;
  min-width: 0;
}

.table-target-mapping-panel,
.table-selector-panel {
  width: 100%;
  height: 560px;
  min-height: 560px;
  max-height: 560px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.table-target-mapping-panel .table-mapping-title,
.table-toolbar {
  flex: 0 0 auto;
}

.table-target-mapping-panel .table-db-collapse {
  flex: 1 1 auto;
  min-height: 0;
  overflow-y: auto;
}

.table-target-mapping-panel :deep(.arco-collapse-item-content-box) {
  max-height: 360px;
  overflow-y: auto;
  padding-right: 8px;
}

.table-target-mapping-panel > :deep(.arco-empty) {
  flex: 1 1 auto;
  display: flex;
  align-items: center;
  justify-content: center;
}

.table-mapping-list {
  min-width: 0;
}

.table-mapping-item {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 16px minmax(160px, 220px);
  width: 100%;
  align-items: center;
}

.table-mapping-source {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.table-mapping-item :deep(.arco-input-wrapper) {
  width: 100% !important;
  min-width: 0;
}

.table-selector-form-item,
.table-selector-form-item :deep(.arco-form-item-content-wrapper),
.table-selector-form-item :deep(.arco-form-item-content) {
  flex: 1 1 auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.table-list-panel {
  flex: 1 1 auto;
  height: auto;
  min-height: 0;
}

.table-list-item :deep(.arco-checkbox) {
  display: flex;
  align-items: flex-start;
  width: 100%;
  min-width: 0;
}

.table-list-item :deep(.arco-checkbox-label) {
  flex: 1 1 auto;
  min-width: 0;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 6px;
  flex-wrap: nowrap;
}

.table-name-text {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-filter-summary {
  display: grid;
  grid-template-columns: 112px minmax(0, 1fr);
  align-items: start;
  gap: 10px;
  min-height: 50px;
}

.task-filter-summary__title {
  margin-bottom: 0;
  line-height: 24px;
  white-space: nowrap;
}

.task-filter-summary__chips {
  max-height: 58px;
  overflow-y: auto;
  padding-right: 4px;
  align-content: flex-start;
}

.filter-chip {
  max-width: 100%;
  min-width: 0;
}

.filter-chip :deep(.arco-tag-content) {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-filter-form-row :deep(.arco-col) {
  min-width: 0;
}

.task-filter-form-row :deep(.arco-form-item) {
  margin-bottom: 0;
}

.task-filter-form-row :deep(.arco-form-item-label-col) {
  height: 22px;
  line-height: 22px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.task-filter-form-row :deep(.arco-form-item-content-wrapper),
.task-filter-form-row :deep(.arco-select),
.task-filter-form-row :deep(.arco-input-wrapper) {
  width: 100%;
  min-width: 0;
}

.task-header {
  align-items: flex-start;
  gap: 12px;
}

.task-title {
  min-width: 0;
  flex: 1 1 auto;
}

.task-title :deep(.arco-typography) {
  max-width: 100%;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-status-tag {
  flex: 0 0 auto;
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
.table-config-row,
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
  padding: 0 14px;
  border-bottom-color: #edf0f5;
  background: #fbfcff;
}

.transfer-header .title,
.table-mapping-title {
  color: #1d2129;
  font-weight: 600;
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

.table-mapping-panel {
  margin: 16px 0 22px;
  padding: 16px;
  border: 1px solid var(--app-border);
  border-radius: 8px;
  background: var(--app-surface);
}

.table-mapping-title {
  margin-bottom: 12px;
  font-size: 14px;
}

.table-mapping-item {
  min-height: 42px;
  padding: 8px 10px;
  border-radius: 6px;
  background: var(--app-surface-soft);
}

.table-mapping-source {
  min-width: 160px;
  overflow: hidden;
  color: var(--app-text);
  font-weight: 500;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.advanced-config-card {
  border: 1px solid var(--app-border);
  border-radius: 8px;
  box-shadow: 0 8px 22px rgba(29, 33, 41, 0.05);
}

.advanced-config-card :deep(.arco-card-body) {
  padding: 22px 24px;
}

@media (max-width: 768px) {
  .task-form-full-page {
    padding: 16px 10px 40px;
  }

  .task-base-config-row {
    padding: 18px 16px 4px;
  }

  .db-transfer-container {
    flex-direction: column;
  }

  .transfer-arrow {
    width: 100%;
    height: 34px;
    min-width: 0;
    padding-top: 0;
  }

  .table-mapping-item {
    align-items: stretch;
    flex-direction: column;
  }

  .table-mapping-source {
    min-width: 0;
  }
}
@media (max-width: 1200px) {
  .table-config-row > :deep(.arco-col) {
    flex: 0 0 100%;
    max-width: 100%;
    margin-bottom: 16px;
  }

  .table-target-mapping-panel,
  .table-selector-panel {
    height: 520px;
    min-height: 520px;
    max-height: 520px;
  }

  .task-filter-summary {
    grid-template-columns: 1fr;
  }
}
/* Corrected alignment pass */
.task-base-config-row > :deep(.arco-col) {
  flex: 0 0 100%;
  max-width: 100%;
}

.task-base-config-row :deep(.arco-select),
.task-base-config-row :deep(.arco-input-wrapper) {
  max-width: 100%;
}

.task-list-card :deep(.arco-card-header) {
  padding: 20px 24px 18px;
  height: auto;
  min-height: 72px;
  overflow: visible;
}

.task-list-card :deep(.arco-card-body) {
  padding: 18px 20px 22px;
}

.task-list-header,
.task-list,
.empty-state--card {
  width: 100%;
}

.task-filter-panel {
  margin: 0 0 18px;
  border: 0;
  border-radius: 0;
  box-shadow: none;
  background: transparent;
  overflow: visible;
}

.task-filter-panel :deep(.arco-card-header),
.task-filter-panel :deep(.arco-card-body) {
  padding-left: 0;
  padding-right: 0;
}

.task-filter-panel :deep(.arco-card-header) {
  padding-top: 0;
  padding-bottom: 12px;
}

.task-filter-panel :deep(.arco-card-body) {
  padding-top: 14px;
  padding-bottom: 0;
}

.task-filter-summary {
  margin-bottom: 14px;
  border-radius: 8px;
}

.advanced-filter-collapse {
  width: 100%;
  border-radius: 8px;
  overflow: hidden;
}

.task-card-inner {
  border-radius: 8px;
}

.table-config-row {
  margin-top: 12px;
}

.table-target-mapping-panel,
.table-selector-panel {
  height: 520px;
  min-height: 520px;
  max-height: 520px;
}

.table-target-mapping-panel .table-db-collapse {
  overflow: hidden;
}

.table-target-mapping-panel :deep(.arco-collapse-item-content-box) {
  max-height: 430px;
}

@media (max-width: 1200px) {
  .table-target-mapping-panel,
  .table-selector-panel {
    height: 500px;
    min-height: 500px;
    max-height: 500px;
  }
}

/* Sci-fi create task surface - uses CSS variables for theme-awareness */
.layout-container {
  background:
    radial-gradient(circle at 12% 8%, var(--app-glow), transparent 28%),
    linear-gradient(135deg, var(--app-bg) 0%, var(--app-surface-strong) 100%);
}

.sider {
  background: linear-gradient(180deg, var(--app-surface-strong) 0%, var(--app-surface) 100%);
  border-right: 1px solid var(--app-border);
  box-shadow: 12px 0 38px rgba(0, 0, 0, 0.28);
}

.sider :deep(.arco-layout-sider-children) {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
}

.sider-footer {
  margin-top: auto;
}

.logo {
  border-bottom-color: var(--app-border);
}

.logo-icon {
  background: linear-gradient(135deg, var(--app-accent) 0%, var(--app-accent-2) 100%);
  box-shadow: 0 0 24px var(--app-glow);
}

.header {
  background: var(--app-surface-strong);
  border-bottom: 1px solid var(--app-border);
  color: var(--app-text);
  backdrop-filter: blur(18px);
  box-shadow: 0 12px 34px rgba(0, 0, 0, 0.24);
}

.header :deep(.arco-typography) {
  color: var(--app-text);
}

.content {
  background:
    linear-gradient(color-mix(in srgb, var(--app-accent) 3.5%, transparent) 1px, transparent 1px),
    linear-gradient(90deg, color-mix(in srgb, var(--app-accent-2) 3.5%, transparent) 1px, transparent 1px);
  background-size: 28px 28px;
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
.transfer-pane,
.task-list-card,
.task-filter-summary,
.task-card-inner {
  border-color: var(--app-border) !important;
  background: linear-gradient(180deg, var(--app-surface), var(--app-surface-soft)) !important;
  box-shadow:
    0 18px 46px rgba(0, 0, 0, 0.26),
    inset 0 1px 0 rgba(255, 255, 255, 0.06);
  color: var(--app-text);
}

.task-base-config-row {
  position: relative;
  overflow: hidden;
  padding: 28px 30px 10px;
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

.task-base-config-row > :deep(.arco-col) {
  position: relative;
  z-index: 1;
}

.layout-container:not(.theme-default) .task-base-config-row :deep(.arco-form-item-label-col > label),
.layout-container:not(.theme-default) .transfer-header .title,
.layout-container:not(.theme-default) .table-mapping-title,
.layout-container:not(.theme-default) .task-list-title-wrap :deep(.arco-typography),
.layout-container:not(.theme-default) .task-title :deep(.arco-typography),
.layout-container:not(.theme-default) .table-mapping-source,
.layout-container:not(.theme-default) .mapped-item .source-name,
.layout-container:not(.theme-default) .transfer-list-item,
.layout-container:not(.theme-default) .table-name-text,
.layout-container:not(.theme-default) .select-type-header :deep(.arco-typography),
.layout-container:not(.theme-default) .type-content :deep(.arco-typography) {
  color: var(--app-text);
}

.layout-container:not(.theme-default) .transfer-list-header,
.layout-container:not(.theme-default) .transfer-header-tip,
.layout-container:not(.theme-default) .select-type-header :deep(.arco-typography-secondary),
.layout-container:not(.theme-default) .type-content :deep(.arco-typography-secondary),
.layout-container:not(.theme-default) .type-content span {
  color: var(--app-muted);
}

.task-base-config-row :deep(.arco-input-wrapper),
.task-base-config-row :deep(.arco-select-view),
.advanced-config-card :deep(.arco-input-wrapper),
.advanced-config-card :deep(.arco-input-number),
.table-selector-panel :deep(.arco-input-wrapper),
.table-mapping-panel :deep(.arco-input-wrapper),
.transfer-pane :deep(.arco-input-wrapper),
.transfer-pane :deep(.arco-select-view) {
  border-color: var(--app-border-soft);
  background: var(--app-surface-strong);
  color: var(--app-text);
}

.task-base-config-row :deep(.arco-input),
.advanced-config-card :deep(.arco-input),
.table-selector-panel :deep(.arco-input),
.table-mapping-panel :deep(.arco-input),
.transfer-pane :deep(.arco-input) {
  color: var(--app-text);
}

.task-base-config-row :deep(.arco-input-wrapper:hover),
.task-base-config-row :deep(.arco-select-view:hover),
.transfer-pane :deep(.arco-input-wrapper:hover),
.table-selector-panel :deep(.arco-input-wrapper:hover),
.table-mapping-panel :deep(.arco-input-wrapper:hover) {
  border-color: var(--app-accent);
  background: var(--app-surface);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--app-accent) 10%, transparent);
}

.task-base-config-row :deep(.arco-radio) {
  border-color: var(--app-border);
  background: var(--app-surface-strong);
  color: var(--app-muted);
}

.task-base-config-row :deep(.arco-radio:hover),
.task-base-config-row :deep(.arco-radio-checked) {
  border-color: var(--app-accent);
  background: linear-gradient(135deg, color-mix(in srgb, var(--app-accent) 16%, transparent), color-mix(in srgb, var(--app-accent-2) 18%, transparent));
  box-shadow: 0 0 24px color-mix(in srgb, var(--app-accent) 16%, transparent);
}

.db-transfer-container,
.table-source-transfer-container {
  height: 360px;
  margin-top: 8px;
  margin-bottom: 22px;
  padding: 16px;
  border: 1px solid var(--app-border);
  border-radius: 8px;
  background: var(--app-surface-strong);
  box-shadow: inset 0 0 40px color-mix(in srgb, var(--app-accent) 5%, transparent);
}

.transfer-pane {
  border-radius: 8px;
  overflow: hidden;
}

.transfer-header {
  min-height: 46px;
  border-bottom-color: var(--app-border-soft);
  background: linear-gradient(90deg, color-mix(in srgb, var(--app-accent) 14%, transparent), color-mix(in srgb, var(--app-accent-2) 8%, transparent), color-mix(in srgb, var(--app-accent) 12%, transparent));
}

.transfer-header-tip,
.transfer-list-header,
.transfer-search {
  border-bottom-color: var(--app-border-soft);
  background: var(--app-surface-strong);
  color: var(--app-muted);
}

.transfer-content.bg-white,
.transfer-content {
  background: var(--app-surface-strong);
}

.transfer-list-item,
.mapped-item,
.table-mapping-item {
  color: var(--app-text);
}

.transfer-list-item:hover,
.mapped-item:hover,
.table-mapping-item:hover {
  background: var(--app-glow);
}

.mapped-item {
  border-bottom-color: var(--app-border-soft);
}

.mapped-item .source-name,
.table-mapping-source {
  color: var(--app-text);
}

.transfer-arrow {
  color: var(--app-accent);
  filter: drop-shadow(0 0 12px var(--app-glow));
}

.advanced-config-card :deep(.arco-card-body) {
  color: var(--app-text);
}

.table-config-row {
  margin-top: 0;
}

.table-target-mapping-panel,
.table-selector-panel {
  height: 540px;
  min-height: 540px;
  max-height: 540px;
  border-radius: 8px;
}

.table-target-mapping-panel {
  margin: 0;
}

.table-list-panel,
.table-db-collapse {
  border-color: var(--app-border-soft);
  background: var(--app-surface-strong);
}

.table-db-collapse :deep(.arco-collapse-item-header) {
  background: color-mix(in srgb, var(--app-accent) 7%, transparent);
  color: var(--app-text);
}

.table-db-collapse :deep(.arco-collapse-item-content) {
  background: var(--app-surface-strong);
  color: var(--app-text);
}

.table-list-item {
  padding: 5px 7px;
  border-radius: 6px;
}

.table-list-item:hover {
  background: var(--app-glow);
}

.table-name-text {
  color: var(--app-text);
}

.table-selector-form-item :deep(.arco-form-item-label-col) {
  margin-bottom: 12px;
}

.table-selector-form-item :deep(.arco-form-item-label-col > label) {
  color: var(--app-text);
  font-weight: 600;
}

.table-selector-panel :deep(.arco-alert-info) {
  border-color: var(--app-border);
  background: color-mix(in srgb, var(--app-accent) 8%, transparent);
  color: var(--app-muted);
}

.advanced-config-row {
  margin-top: 0;
}

.task-base-config-row :deep(.arco-btn-primary),
.header-right :deep(.arco-btn-primary) {
  border: 0;
  background: linear-gradient(135deg, var(--app-accent) 0%, var(--app-accent-2) 100%);
  box-shadow: 0 0 26px var(--app-glow);
}

/* Keep text readable on the surface */
.task-base-config-row :deep(.arco-form-item-label-col > label),
.advanced-config-card :deep(.arco-form-item-label-col > label),
.table-mapping-panel :deep(.arco-form-item-label-col > label),
.table-selector-panel :deep(.arco-form-item-label-col > label),
.task-base-config-row :deep(.arco-typography),
.advanced-config-card :deep(.arco-typography),
.table-mapping-panel :deep(.arco-typography),
.table-selector-panel :deep(.arco-typography),
.task-base-config-row :deep(.arco-checkbox-label),
.advanced-config-card :deep(.arco-checkbox-label),
.table-selector-panel :deep(.arco-checkbox-label),
.task-base-config-row :deep(.arco-radio-label),
.task-base-config-row :deep(.arco-select-view-value) {
  color: var(--app-text) !important;
}

.task-base-config-row :deep(.arco-typography-secondary),
.advanced-config-card :deep(.arco-typography-secondary),
.table-mapping-panel :deep(.arco-typography-secondary),
.table-selector-panel :deep(.arco-typography-secondary),
.task-base-config-row :deep(.arco-form-item-extra),
.advanced-config-card :deep(.arco-form-item-extra),
.task-base-config-row :deep(.arco-checkbox-disabled .arco-checkbox-label),
.advanced-config-card :deep(.arco-checkbox-disabled .arco-checkbox-label),
.task-base-config-row :deep(.arco-radio-disabled .arco-radio-label) {
  color: var(--app-muted) !important;
}

.task-base-config-row :deep(.arco-radio-disabled),
.task-base-config-row :deep(.arco-checkbox-disabled),
.advanced-config-card :deep(.arco-checkbox-disabled) {
  opacity: 1;
}

.task-base-config-row :deep(.arco-input::placeholder),
.advanced-config-card :deep(.arco-input::placeholder),
.table-mapping-panel :deep(.arco-input::placeholder),
.table-selector-panel :deep(.arco-input::placeholder),
.transfer-pane :deep(.arco-input::placeholder) {
  color: var(--app-muted) !important;
}

.task-base-config-row :deep(.arco-input-number),
.advanced-config-card :deep(.arco-input-number),
.task-base-config-row :deep(.arco-input-number input),
.advanced-config-card :deep(.arco-input-number input),
.task-base-config-row :deep(.arco-checkbox),
.advanced-config-card :deep(.arco-checkbox),
.table-selector-panel :deep(.arco-checkbox) {
  color: var(--app-text);
}

.advanced-config-card :deep(.arco-card-header),
.advanced-config-card :deep(.arco-card-body),
.task-base-config-row :deep(.arco-form-item-content),
.advanced-config-card :deep(.arco-form-item-content),
.table-selector-panel :deep(.arco-form-item-content) {
  color: var(--app-text);
}

/* Unified theme pass - default variables (overridden by theme classes) */
.layout-container {
  --app-bg: #07111f;
  --app-surface: rgba(12, 24, 42, 0.94);
  --app-surface-soft: rgba(16, 33, 56, 0.9);
  --app-surface-strong: rgba(8, 17, 31, 0.98);
  --app-border: rgba(72, 188, 226, 0.28);
  --app-border-soft: rgba(113, 168, 199, 0.16);
  --app-text: #edf7ff;
  --app-muted: #9fb5c9;
  --app-accent: #20c7e8;
  --app-accent-2: #3b82f6;
  --app-glow: rgba(32, 199, 232, 0.24);
  background:
    radial-gradient(circle at 15% 12%, var(--app-glow), transparent 30%),
    linear-gradient(135deg, var(--app-bg) 0%, var(--app-surface-strong) 100%);
}

.sider {
  background: linear-gradient(180deg, var(--app-surface-strong) 0%, var(--app-surface) 100%);
  border-right-color: var(--app-border);
  box-shadow: 10px 0 34px rgba(0, 0, 0, 0.28);
}

.logo {
  border-bottom-color: var(--app-border);
}

.logo-icon {
  background: linear-gradient(135deg, var(--app-accent) 0%, var(--app-accent-2) 100%);
  box-shadow: 0 0 22px var(--app-glow);
}

.sider-menu :deep(.arco-menu-item:hover) {
  background: color-mix(in srgb, var(--app-accent) 12%, transparent) !important;
}

.sider-menu :deep(.arco-menu-item.arco-menu-selected) {
  background: color-mix(in srgb, var(--app-muted) 22%, transparent) !important;
  box-shadow: inset 3px 0 0 var(--app-accent);
}

.header {
  background: var(--app-surface-strong);
  border-bottom-color: var(--app-border);
}

.content {
  background:
    linear-gradient(color-mix(in srgb, var(--app-accent-2) 3.5%, transparent) 1px, transparent 1px),
    linear-gradient(90deg, color-mix(in srgb, var(--app-accent) 3.5%, transparent) 1px, transparent 1px);
  background-size: 30px 30px;
}

.stat-card,
.task-list-card,
.task-filter-panel,
.task-filter-summary,
.empty-state--card,
.task-card-inner,
.advanced-filter-collapse,
.task-base-config-row,
.advanced-config-card,
.table-mapping-panel,
.table-selector-panel,
.transfer-pane {
  border: 1px solid var(--app-border) !important;
  background:
    linear-gradient(180deg, rgba(18, 35, 58, 0.96) 0%, rgba(10, 22, 39, 0.96) 100%) !important;
  box-shadow:
    0 16px 38px rgba(0, 0, 0, 0.22),
    inset 0 1px 0 rgba(255, 255, 255, 0.06) !important;
  color: var(--app-text);
}

.stat-card :deep(.arco-card-body) {
  color: var(--app-text);
  padding: 20px 18px;
}

.stat-card :deep(.arco-statistic-title),
.task-filter-panel__desc,
.task-filter-summary__empty,
.task-list-title-wrap :deep(.arco-typography-secondary),
.empty-state :deep(.arco-empty-description) {
  color: var(--app-muted) !important;
}

.stat-card :deep(.arco-statistic-value),
.task-filter-panel__title,
.task-filter-summary__title,
.task-list-title-wrap :deep(.arco-typography),
.empty-state :deep(.arco-empty-image) {
  color: var(--app-text) !important;
}

.task-list-card :deep(.arco-card-header),
.task-filter-panel :deep(.arco-card-header),
.task-filter-panel :deep(.arco-card-body),
.advanced-filter-collapse :deep(.arco-collapse-item-header),
.advanced-filter-collapse :deep(.arco-collapse-item-content) {
  border-color: var(--app-border-soft) !important;
  background: transparent !important;
  color: var(--app-text);
}

.advanced-filter-collapse :deep(.arco-collapse-item-header-title),
.advanced-filter-collapse :deep(.arco-collapse-item-icon-hover) {
  color: var(--app-text);
}

.task-filter-summary {
  background: var(--app-surface-strong) !important;
}

.filter-chip :deep(.arco-tag),
.task-filter-summary :deep(.arco-tag),
.task-list-card :deep(.arco-tag),
.task-filter-panel :deep(.arco-tag) {
  border-color: color-mix(in srgb, var(--app-accent) 32%, transparent);
  background: color-mix(in srgb, var(--app-accent) 12%, transparent) !important;
  color: var(--app-text) !important;
}

.task-list-card :deep(.arco-input-wrapper),
.task-list-card :deep(.arco-select-view),
.task-filter-panel :deep(.arco-input-wrapper),
.task-filter-panel :deep(.arco-select-view),
.advanced-filter-collapse :deep(.arco-input-wrapper),
.advanced-filter-collapse :deep(.arco-select-view) {
  border-color: var(--app-border-soft);
  background: rgba(6, 14, 26, 0.64);
  color: var(--app-text);
}

.task-list-card :deep(.arco-input),
.task-filter-panel :deep(.arco-input),
.advanced-filter-collapse :deep(.arco-input),
.task-list-card :deep(.arco-select-view-value),
.task-filter-panel :deep(.arco-select-view-value),
.advanced-filter-collapse :deep(.arco-select-view-value) {
  color: var(--app-text);
}

.task-list-card :deep(.arco-input::placeholder),
.task-filter-panel :deep(.arco-input::placeholder),
.advanced-filter-collapse :deep(.arco-input::placeholder) {
  color: var(--app-muted) !important;
}

.header-right :deep(.arco-btn-primary),
.task-base-config-row :deep(.arco-btn-primary),
.empty-state :deep(.arco-btn-primary) {
  border: 1px solid rgba(125, 211, 252, 0.24);
  background: linear-gradient(135deg, var(--app-accent) 0%, var(--app-accent-2) 100%) !important;
  box-shadow: 0 10px 26px rgba(32, 199, 232, 0.2);
}

.task-list-card :deep(.arco-btn-text),
.task-filter-panel :deep(.arco-btn-text) {
  color: #7dd3fc;
}

.empty-state--card {
  border-style: dashed !important;
  min-height: 228px;
}

.empty-state--card :deep(.arco-empty) {
  color: var(--app-muted);
}

.task-list-card :deep(.arco-collapse),
.advanced-filter-collapse {
  background: transparent !important;
}

.task-filter-form-row :deep(.arco-form-item-label-col),
.task-filter-form-row :deep(.arco-form-item-label-col > label) {
  color: var(--app-muted) !important;
}

:global(html:not([data-ui-theme="default"]) .arco-message) {
  border: 1px solid var(--app-border, rgba(72, 188, 226, 0.28)) !important;
  background: var(--app-surface, rgba(12, 24, 42, 0.96)) !important;
  color: var(--app-text, #edf7ff) !important;
  box-shadow: 0 16px 34px rgba(0, 0, 0, 0.24) !important;
}

:global(html:not([data-ui-theme="default"]) .arco-message-content),
:global(html:not([data-ui-theme="default"]) .arco-message .arco-icon) {
  color: var(--app-text, #edf7ff) !important;
}

/* Clear target-type selection and filter panel typography */
.select-type-page {
  max-width: 860px;
  margin: 56px auto 72px;
  padding: 0 28px;
}

.select-type-header {
  margin-bottom: 34px;
  padding: 18px 0 6px;
  text-align: center;
}

.select-type-header :deep(.arco-typography) {
  color: var(--app-text) !important;
}

.select-type-header :deep(h1.arco-typography),
.select-type-header :deep(h2.arco-typography),
.select-type-header :deep(h3.arco-typography) {
  margin: 0 0 10px !important;
  color: #f3fbff !important;
  font-size: 30px;
  font-weight: 700;
  line-height: 1.25;
  text-shadow: 0 0 18px rgba(32, 199, 232, 0.18);
}

.select-type-header :deep(.arco-typography-secondary) {
  color: #b9cbe0 !important;
  font-size: 14px;
}

.type-cards-container {
  gap: 22px;
}

.type-card {
  min-height: 126px;
  border: 1px solid var(--app-border) !important;
  background:
    linear-gradient(180deg, var(--app-surface), var(--app-surface-soft)) !important;
  box-shadow: 0 16px 34px rgba(0, 0, 0, 0.22);
  color: var(--app-text);
  overflow: hidden;
}

.type-card:hover {
  transform: translateY(-3px);
  border-color: var(--app-accent) !important;
  background:
    linear-gradient(180deg, var(--app-surface-soft), var(--app-surface-strong)) !important;
  box-shadow: 0 18px 38px var(--app-glow);
}

.type-card :deep(.arco-card-body) {
  align-items: center;
  padding: 24px;
  color: var(--app-text);
}

.type-content {
  min-width: 0;
}

.type-content :deep(.arco-typography),
.type-content :deep(h5.arco-typography) {
  color: #f3fbff !important;
}

.type-content :deep(h5.arco-typography) {
  margin-bottom: 8px !important;
  font-size: 20px;
  line-height: 1.35;
}

.type-content :deep(.arco-typography-secondary) {
  color: var(--app-muted) !important;
  font-size: 13px;
  line-height: 1.65;
}

.type-icon {
  border: 1px solid var(--app-border-soft);
  background: color-mix(in srgb, var(--app-accent) 12%, transparent) !important;
  color: var(--app-accent) !important;
}

.kafka-icon {
  background: color-mix(in srgb, var(--app-accent-2) 12%, transparent) !important;
  color: var(--app-accent-2) !important;
}

.webhook-icon {
  background: color-mix(in srgb, var(--app-accent) 12%, transparent) !important;
  color: var(--app-accent) !important;
}

.multi-icon {
  background: color-mix(in srgb, var(--app-muted) 12%, transparent) !important;
  color: var(--app-muted) !important;
}

.task-filter-panel {
  overflow: visible;
}

.task-filter-panel :deep(.arco-card-header) {
  min-height: 76px;
  padding: 20px 20px 18px !important;
}

.task-filter-panel :deep(.arco-card-body) {
  line-height: 20px;
}

.task-filter-panel__header {
  align-items: center;
  min-height: 38px;
}

.task-filter-panel__title {
  color: var(--app-text) !important;
  font-size: 16px;
  line-height: 22px;
}

.task-filter-panel__desc {
  margin-top: 6px;
  color: var(--app-muted) !important;
  font-size: 13px;
  line-height: 20px;
}

@media (max-width: 1200px) {
  .task-form-full-page {
    max-width: 980px;
  }

  .task-list-header {
    grid-template-columns: 1fr;
    align-items: stretch;
    gap: 14px;
  }

  .task-list-toolbar {
    justify-content: flex-start;
    flex-wrap: wrap;
    row-gap: 10px;
  }

  .task-card-grid {
    grid-template-columns: 1fr;
  }

  .task-card-actions {
    width: 100%;
    min-height: auto;
    padding: 16px 0 0;
    border-left: 0;
    border-top: 1px solid var(--app-border-soft, #edf2f7);
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    grid-auto-rows: 32px;
    justify-content: stretch;
    align-content: start;
  }

  .task-info-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .table-target-mapping-panel,
  .table-selector-panel {
    height: 500px;
    min-height: 500px;
    max-height: 500px;
  }
}

@media (max-width: 768px) {
  .layout-container {
    height: auto;
    min-height: 100vh;
    flex-direction: column;
  }

  .sider {
    width: 100% !important;
    max-width: none !important;
    min-width: 0 !important;
    flex: 0 0 auto !important;
    height: auto;
  }

  .sider :deep(.arco-layout-sider-children) {
    min-height: 0;
  }

  .logo {
    height: 56px;
  }

  .sider-menu {
    flex: 0 0 auto;
    padding: 8px 10px;
  }

  .sider-menu :deep(.arco-menu-inner) {
    flex-direction: row !important;
    gap: 8px;
  }

  .sider-menu :deep(.arco-menu-item) {
    width: auto !important;
    flex: 1 1 0;
    margin: 0;
    text-align: center;
  }

  .sider-footer {
    display: none;
  }

  .header {
    min-height: 64px;
    height: auto;
    padding: 12px;
    gap: 10px;
    flex-wrap: wrap;
  }

  .content {
    padding: 14px 12px 28px;
  }

  .task-form-full-page {
    width: 100%;
    padding: 12px 0 36px;
  }

  .task-base-config-row {
    padding: 18px 14px 2px;
  }

  .db-transfer-container,
  .table-source-transfer-container {
    align-items: stretch;
    gap: 12px;
    height: auto;
    min-height: 620px;
    width: 100%;
    padding: 12px;
  }

  .db-transfer-container .transfer-pane,
  .table-source-transfer-container .transfer-pane {
    width: 100%;
    flex: 0 0 auto;
  }

  .db-transfer-container .transfer-arrow,
  .table-source-transfer-container .transfer-arrow {
    align-self: center;
    width: 100%;
  }

  .table-target-mapping-panel,
  .table-selector-panel {
    height: 520px;
    min-height: 520px;
    max-height: 520px;
  }

  .table-config-row > :deep(.arco-col) {
    flex: 0 0 100%;
    max-width: 100%;
    margin-bottom: 16px;
  }

  .table-list-grid {
    grid-template-columns: 1fr;
  }

  .task-title,
  .task-info-grid {
    grid-template-columns: 1fr;
  }

  .task-info-cell,
  .task-info-cell--count {
    justify-self: stretch;
  }

  .task-card-actions {
    grid-template-columns: 1fr;
  }
}

/* Theme selector in system config */
.config-page-shell {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.config-hero {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}

.config-hint {
  margin-top: 10px;
  font-size: 12px;
  line-height: 1.6;
  color: var(--app-muted, #86909c);
}

.config-summary-row {
  margin-bottom: 4px;
}

.config-page-card :deep(.arco-card-body) {
  padding: 20px 24px 24px;
}

.config-section-card :deep(.arco-card-header) {
  padding: 16px 20px 12px;
  border-bottom: 1px solid var(--app-border-soft, #edf2f7);
}

.config-section-card :deep(.arco-card-header-title) {
  color: var(--app-text, #1d2129);
  font-weight: 600;
  font-size: 15px;
  line-height: 22px;
}

.config-section-card :deep(.arco-card-body) {
  padding: 16px 20px 20px;
}

.config-section-divider {
  margin: 22px 0 16px !important;
  border-color: var(--app-border-soft, #e5e8ef) !important;
}

.config-section-divider :deep(.arco-divider-text) {
  padding: 0 12px 0 0;
  font-size: 13px;
  font-weight: 600;
  line-height: 20px;
  color: var(--app-text, #1d2129);
  background: var(--app-surface-soft, #fbfcff);
}

.theme-config-card {
  margin-bottom: 16px;
}

.theme-option-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 12px;
}

.theme-option {
  min-height: 86px;
  padding: 12px;
  border: 1px solid rgba(120, 144, 166, 0.24);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.04);
  color: inherit;
  cursor: pointer;
  display: flex;
  flex-direction: column;
  gap: 8px;
  text-align: left;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.theme-option:hover,
.theme-option.is-active {
  border-color: var(--app-accent, #165dff);
  box-shadow: 0 0 0 3px rgba(32, 199, 232, 0.12);
  transform: translateY(-1px);
}

.theme-option__swatch {
  display: grid;
  grid-template-columns: 1.2fr 1fr 1fr;
  gap: 4px;
  height: 18px;
}

.theme-option__swatch span {
  border-radius: 4px;
}

.theme-option__content {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.theme-option__title {
  font-weight: 600;
  font-size: 13px;
}

.theme-option__desc,
.theme-option__checked {
  color: var(--app-muted, #86909c);
  font-size: 12px;
  line-height: 18px;
}

.theme-option__checked {
  color: var(--app-accent, #165dff);
}

.theme-option--default .theme-option__swatch span:nth-child(1) { background: #ffffff; border: 1px solid #e5e6eb; }
.theme-option--default .theme-option__swatch span:nth-child(2) { background: #f5f7fa; }
.theme-option--default .theme-option__swatch span:nth-child(3) { background: #165dff; }
.theme-option--blue .theme-option__swatch span:nth-child(1) { background: #07111f; }
.theme-option--blue .theme-option__swatch span:nth-child(2) { background: #12233a; }
.theme-option--blue .theme-option__swatch span:nth-child(3) { background: #20c7e8; }
.theme-option--gray .theme-option__swatch span:nth-child(1) { background: #14181f; }
.theme-option--gray .theme-option__swatch span:nth-child(2) { background: #252c36; }
.theme-option--gray .theme-option__swatch span:nth-child(3) { background: #94a3b8; }
.theme-option--black .theme-option__swatch span:nth-child(1) { background: #000000; }
.theme-option--black .theme-option__swatch span:nth-child(2) { background: #0a0a0a; }
.theme-option--black .theme-option__swatch span:nth-child(3) { background: #38bdf8; }
.theme-option--dark .theme-option__swatch span:nth-child(1) { background: #111827; }
.theme-option--dark .theme-option__swatch span:nth-child(2) { background: #1f2937; }
.theme-option--dark .theme-option__swatch span:nth-child(3) { background: #60a5fa; }

.theme-blue {
  --app-bg: #07111f;
  --app-surface: rgba(18, 35, 58, 0.96);
  --app-surface-soft: rgba(10, 22, 39, 0.96);
  --app-surface-strong: rgba(7, 17, 31, 0.98);
  --app-border: rgba(72, 188, 226, 0.28);
  --app-border-soft: rgba(113, 168, 199, 0.16);
  --app-text: #edf7ff;
  --app-muted: #9fb5c9;
  --app-accent: #20c7e8;
  --app-accent-2: #3b82f6;
  --app-glow: rgba(32, 199, 232, 0.24);
}

.theme-gray {
  --app-bg: #111827;
  --app-surface: rgba(31, 41, 55, 0.96);
  --app-surface-soft: rgba(38, 48, 63, 0.94);
  --app-surface-strong: rgba(17, 24, 39, 0.98);
  --app-border: rgba(148, 163, 184, 0.28);
  --app-border-soft: rgba(148, 163, 184, 0.16);
  --app-text: #f1f5f9;
  --app-muted: #cbd5e1;
  --app-accent: #94a3b8;
  --app-accent-2: #64748b;
  --app-glow: rgba(148, 163, 184, 0.18);
}

.theme-black {
  --app-bg: #000000;
  --app-surface: rgba(10, 10, 10, 0.98);
  --app-surface-soft: rgba(18, 18, 18, 0.96);
  --app-surface-strong: rgba(0, 0, 0, 0.98);
  --app-border: rgba(125, 211, 252, 0.3);
  --app-border-soft: rgba(125, 211, 252, 0.16);
  --app-text: #f8fafc;
  --app-muted: #cbd5e1;
  --app-accent: #38bdf8;
  --app-accent-2: #2563eb;
  --app-glow: rgba(56, 189, 248, 0.2);
}

.theme-dark {
  --app-bg: #0f172a;
  --app-surface: rgba(30, 41, 59, 0.96);
  --app-surface-soft: rgba(39, 52, 73, 0.94);
  --app-surface-strong: rgba(15, 23, 42, 0.98);
  --app-border: rgba(96, 165, 250, 0.3);
  --app-border-soft: rgba(96, 165, 250, 0.16);
  --app-text: #f8fafc;
  --app-muted: #cbd5e1;
  --app-accent: #60a5fa;
  --app-accent-2: #818cf8;
  --app-glow: rgba(96, 165, 250, 0.2);
}

.theme-blue,
.theme-gray,
.theme-black,
.theme-dark {
  background:
    radial-gradient(circle at 14% 10%, var(--app-glow), transparent 30%),
    linear-gradient(135deg, var(--app-bg), var(--app-surface-strong));
}

.theme-blue .sider,
.theme-gray .sider,
.theme-black .sider,
.theme-dark .sider,
.theme-blue .header,
.theme-gray .header,
.theme-black .header,
.theme-dark .header {
  background: var(--app-surface-strong);
  border-color: var(--app-border);
}

.theme-blue .stat-card,
.theme-gray .stat-card,
.theme-black .stat-card,
.theme-dark .stat-card,
.theme-blue .task-list-card,
.theme-gray .task-list-card,
.theme-black .task-list-card,
.theme-dark .task-list-card,
.theme-blue .task-filter-panel,
.theme-gray .task-filter-panel,
.theme-black .task-filter-panel,
.theme-dark .task-filter-panel,
.theme-blue .task-filter-summary,
.theme-gray .task-filter-summary,
.theme-black .task-filter-summary,
.theme-dark .task-filter-summary,
.theme-blue .empty-state--card,
.theme-gray .empty-state--card,
.theme-black .empty-state--card,
.theme-dark .empty-state--card,
.theme-blue .type-card,
.theme-gray .type-card,
.theme-black .type-card,
.theme-dark .type-card,
.theme-blue .config-summary-card,
.theme-gray .config-summary-card,
.theme-black .config-summary-card,
.theme-dark .config-summary-card,
.theme-blue .config-page-card,
.theme-gray .config-page-card,
.theme-black .config-page-card,
.theme-dark .config-page-card,
.theme-blue .config-section-card,
.theme-gray .config-section-card,
.theme-black .config-section-card,
.theme-dark .config-section-card {
  border-color: var(--app-border) !important;
  background: linear-gradient(180deg, var(--app-surface), var(--app-surface-soft)) !important;
  color: var(--app-text);
}

.theme-blue .header-right :deep(.arco-btn-primary),
.theme-gray .header-right :deep(.arco-btn-primary),
.theme-black .header-right :deep(.arco-btn-primary),
.theme-dark .header-right :deep(.arco-btn-primary),
.theme-blue .task-base-config-row :deep(.arco-btn-primary),
.theme-gray .task-base-config-row :deep(.arco-btn-primary),
.theme-black .task-base-config-row :deep(.arco-btn-primary),
.theme-dark .task-base-config-row :deep(.arco-btn-primary) {
  background: linear-gradient(135deg, var(--app-accent), var(--app-accent-2)) !important;
}

/* Non-default themes: make every Arco data/control surface readable */
.layout-container:not(.theme-default),
.layout-container:not(.theme-default) :deep(.arco-layout),
.layout-container:not(.theme-default) :deep(.arco-card),
.layout-container:not(.theme-default) :deep(.arco-card-body),
.layout-container:not(.theme-default) :deep(.arco-card-header),
.layout-container:not(.theme-default) :deep(.arco-list),
.layout-container:not(.theme-default) :deep(.arco-list-item),
.layout-container:not(.theme-default) :deep(.arco-form),
.layout-container:not(.theme-default) :deep(.arco-form-item),
.layout-container:not(.theme-default) :deep(.arco-form-item-content),
.layout-container:not(.theme-default) :deep(.arco-collapse),
.layout-container:not(.theme-default) :deep(.arco-collapse-item),
.layout-container:not(.theme-default) :deep(.arco-collapse-item-content),
.layout-container:not(.theme-default) :deep(.arco-collapse-item-content-box) {
  color: var(--app-text) !important;
}

.layout-container:not(.theme-default) :deep(.arco-typography),
.layout-container:not(.theme-default) :deep(.arco-card-header-title),
.layout-container:not(.theme-default) :deep(.arco-form-item-label-col > label),
.layout-container:not(.theme-default) :deep(.arco-checkbox-label),
.layout-container:not(.theme-default) :deep(.arco-radio-label),
.layout-container:not(.theme-default) :deep(.arco-statistic-value),
.layout-container:not(.theme-default) :deep(.arco-collapse-item-header-title),
.layout-container:not(.theme-default) :deep(.arco-descriptions-title),
.layout-container:not(.theme-default) :deep(.arco-descriptions-item-value),
.layout-container:not(.theme-default) :deep(.arco-table-td),
.layout-container:not(.theme-default) :deep(.arco-table-th) {
  color: var(--app-text) !important;
}

.layout-container:not(.theme-default) :deep(.arco-typography-secondary),
.layout-container:not(.theme-default) :deep(.arco-statistic-title),
.layout-container:not(.theme-default) :deep(.arco-form-item-extra),
.layout-container:not(.theme-default) :deep(.arco-descriptions-item-label),
.layout-container:not(.theme-default) :deep(.arco-empty-description),
.layout-container:not(.theme-default) :deep(.arco-pagination-total),
.layout-container:not(.theme-default) :deep(.arco-pagination-jumper),
.layout-container:not(.theme-default) :deep(.arco-table-cell-with-sorter) {
  color: var(--app-muted) !important;
}

.layout-container:not(.theme-default) :deep(.arco-card),
.layout-container:not(.theme-default) :deep(.arco-table),
.layout-container:not(.theme-default) :deep(.arco-table-container),
.layout-container:not(.theme-default) :deep(.arco-descriptions-view),
.layout-container:not(.theme-default) :deep(.arco-alert),
.layout-container:not(.theme-default) :deep(.arco-collapse),
.layout-container:not(.theme-default) :deep(.arco-collapse-item-header),
.layout-container:not(.theme-default) :deep(.arco-collapse-item-content),
.layout-container:not(.theme-default) :deep(.arco-list),
.layout-container:not(.theme-default) :deep(.arco-list-item) {
  border-color: var(--app-border-soft) !important;
  background: transparent !important;
}

.layout-container:not(.theme-default) .config-page-card,
.layout-container:not(.theme-default) .config-section-card,
.layout-container:not(.theme-default) .config-summary-card,
.layout-container:not(.theme-default) .theme-config-card {
  border-color: var(--app-border) !important;
  background: linear-gradient(180deg, var(--app-surface), var(--app-surface-soft)) !important;
}

.layout-container:not(.theme-default) .config-page-card :deep(.arco-card-body),
.layout-container:not(.theme-default) .config-section-card :deep(.arco-card-body),
.layout-container:not(.theme-default) .config-summary-card :deep(.arco-card-body),
.layout-container:not(.theme-default) .theme-config-card :deep(.arco-card-body) {
  background: transparent !important;
}

.layout-container:not(.theme-default) :deep(.arco-table-th),
.layout-container:not(.theme-default) :deep(.arco-table-td),
.layout-container:not(.theme-default) :deep(.arco-descriptions-item-label-block),
.layout-container:not(.theme-default) :deep(.arco-descriptions-item-value-block),
.layout-container:not(.theme-default) :deep(.arco-descriptions-cell),
.layout-container:not(.theme-default) :deep(.arco-descriptions-table) {
  border-color: var(--app-border-soft) !important;
  background: rgba(6, 14, 26, 0.34) !important;
}

.layout-container:not(.theme-default) :deep(.arco-table-tr:hover .arco-table-td),
.layout-container:not(.theme-default) :deep(.arco-list-item:hover) {
  background: rgba(32, 199, 232, 0.08) !important;
}

/* Task card alignment: reserve fixed action column and stable info columns */
.task-list {
  --task-action-width: 224px;
  --task-action-button-height: 28px;
  --task-action-gap: 8px;
  --task-action-slot-count: 6;
}

.task-card-inner {
  min-height: 148px;
}

.task-card-inner :deep(.arco-card-body) {
  padding: 18px 20px;
}

.task-card-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) var(--task-action-width);
  gap: 22px;
  align-items: stretch;
  min-height: calc(
    var(--task-action-slot-count) * var(--task-action-button-height) +
    (var(--task-action-slot-count) - 1) * var(--task-action-gap)
  );
}

.task-card-main {
  min-width: 0;
  display: grid;
  grid-template-rows: auto 1fr auto;
  gap: 14px;
  padding-right: 8px;
}

.task-header {
  min-height: 28px;
  margin-bottom: 0;
}

.task-title {
  display: grid;
  grid-template-columns: minmax(220px, auto) auto auto auto;
  justify-content: flex-start;
  align-items: center;
  gap: 8px 10px;
  min-width: 0;
}

.task-title :deep(.arco-typography) {
  min-width: 0;
  max-width: 520px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-status-tag {
  margin: 0;
  justify-self: start;
}

.task-info-grid {
  display: grid;
  grid-template-columns:
    minmax(120px, 0.8fr)
    minmax(260px, 1.5fr)
    minmax(260px, 1.5fr)
    minmax(96px, 0.7fr);
  align-items: center;
  column-gap: 28px;
  row-gap: 12px;
  min-height: 54px;
}

.task-info-cell {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  column-gap: 12px;
  min-width: 0;
}

.task-info-label {
  color: var(--app-muted, #86909c);
  font-size: 13px;
  white-space: nowrap;
}

.task-info-value {
  min-width: 0;
  color: var(--app-text, #1d2129);
  font-size: 13px;
  font-weight: 500;
}

.theme-default .task-info-label {
  color: #86909c;
}

.theme-default .task-info-value {
  color: #1d2129;
}

.task-info-tags {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  min-height: 24px;
  overflow: hidden;
}

.task-info-tags .inline-tag {
  margin: 0;
  max-width: 180px;
}

.task-info-tags :deep(.arco-tag-content) {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-info-cell--count {
  justify-self: end;
}

.task-card-actions {
  width: 100%;
  box-sizing: border-box;
  min-height: calc(
    var(--task-action-slot-count) * var(--task-action-button-height) +
    (var(--task-action-slot-count) - 1) * var(--task-action-gap)
  );
  padding: 0 0 0 18px;
  border-left: 1px solid var(--app-border-soft, #edf2f7);
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  align-items: stretch;
  gap: var(--task-action-gap);
}

.theme-default .task-card-actions {
  border-left-color: #edf2f7;
}

.task-card-actions :deep(.arco-btn),
.task-card-actions :deep(.arco-tooltip) {
  width: 100%;
}

.task-card-actions :deep(.arco-btn) {
  height: var(--task-action-button-height);
  justify-content: center;
  flex-shrink: 0;
}

.layout-container:not(.theme-default) :deep(.arco-input-wrapper),
.layout-container:not(.theme-default) :deep(.arco-input-number),
.layout-container:not(.theme-default) :deep(.arco-input-tag),
.layout-container:not(.theme-default) :deep(.arco-select-view),
.layout-container:not(.theme-default) :deep(.arco-textarea-wrapper),
.layout-container:not(.theme-default) :deep(.arco-picker) {
  border-color: var(--app-border-soft) !important;
  background: rgba(6, 14, 26, 0.62) !important;
  color: var(--app-text) !important;
}

.layout-container:not(.theme-default) :deep(.arco-input),
.layout-container:not(.theme-default) :deep(.arco-input-number input),
.layout-container:not(.theme-default) :deep(.arco-textarea),
.layout-container:not(.theme-default) :deep(.arco-select-view-value),
.layout-container:not(.theme-default) :deep(.arco-select-view-input),
.layout-container:not(.theme-default) :deep(.arco-input-tag-input),
.layout-container:not(.theme-default) :deep(.arco-picker-input input) {
  color: var(--app-text) !important;
}

.layout-container:not(.theme-default) :deep(.arco-input::placeholder),
.layout-container:not(.theme-default) :deep(.arco-textarea::placeholder),
.layout-container:not(.theme-default) :deep(.arco-select-view-placeholder),
.layout-container:not(.theme-default) :deep(.arco-picker-input input::placeholder) {
  color: var(--app-muted) !important;
}

.layout-container:not(.theme-default) :deep(.arco-input-wrapper:hover),
.layout-container:not(.theme-default) :deep(.arco-input-number:hover),
.layout-container:not(.theme-default) :deep(.arco-select-view:hover),
.layout-container:not(.theme-default) :deep(.arco-textarea-wrapper:hover),
.layout-container:not(.theme-default) :deep(.arco-picker:hover) {
  border-color: var(--app-accent) !important;
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--app-accent) 16%, transparent);
}

.layout-container:not(.theme-default) :deep(.arco-tag) {
  border-color: color-mix(in srgb, var(--app-accent) 42%, transparent) !important;
  background: color-mix(in srgb, var(--app-accent) 14%, transparent) !important;
  color: var(--app-text) !important;
}

.layout-container:not(.theme-default) :deep(.arco-tag-content) {
  color: inherit !important;
}

.layout-container:not(.theme-default) :deep(.arco-btn:not(.arco-btn-primary):not(.arco-btn-status-danger):not(.arco-btn-status-warning):not(.arco-btn-status-success)) {
  border-color: var(--app-border-soft);
  background: rgba(6, 14, 26, 0.38);
  color: var(--app-text);
}

.layout-container:not(.theme-default) :deep(.arco-btn-text) {
  background: transparent !important;
  color: var(--app-accent) !important;
}

.layout-container:not(.theme-default) :deep(.arco-pagination-item),
.layout-container:not(.theme-default) :deep(.arco-pagination-jumper-input),
.layout-container:not(.theme-default) :deep(.arco-pagination-options .arco-select-view) {
  border-color: var(--app-border-soft) !important;
  background: rgba(6, 14, 26, 0.5) !important;
  color: var(--app-text) !important;
}

.layout-container:not(.theme-default) :deep(.arco-pagination-item-active) {
  border-color: var(--app-accent) !important;
  color: var(--app-accent) !important;
}

.layout-container:not(.theme-default) :deep(.arco-divider-horizontal) {
  border-color: var(--app-border-soft) !important;
}

.layout-container:not(.theme-default) :deep(.arco-divider-text) {
  color: var(--app-text) !important;
  background: var(--app-surface-soft) !important;
  padding: 0 12px;
  font-weight: 600;
}

.layout-container:not(.theme-default) .config-hero :deep(.arco-typography) {
  color: var(--app-text) !important;
}

.layout-container:not(.theme-default) .config-hero :deep(.arco-typography-secondary),
.layout-container:not(.theme-default) .config-hint {
  color: var(--app-muted) !important;
}

.layout-container:not(.theme-default) .config-section-card :deep(.arco-card-header),
.layout-container:not(.theme-default) .config-page-card :deep(.arco-card-header),
.layout-container:not(.theme-default) .theme-config-card :deep(.arco-card-header) {
  border-bottom-color: var(--app-border-soft) !important;
}

.layout-container:not(.theme-default) .config-section-card :deep(.arco-card-header-title),
.layout-container:not(.theme-default) .config-page-card :deep(.arco-card-header-title),
.layout-container:not(.theme-default) .theme-config-card :deep(.arco-card-header-title),
.layout-container:not(.theme-default) .config-section-divider :deep(.arco-divider-text) {
  color: var(--app-text) !important;
  background: var(--app-surface-soft) !important;
}

:global(html:not([data-ui-theme="default"]) .arco-trigger-popup),
:global(html:not([data-ui-theme="default"]) .arco-select-popup),
:global(html:not([data-ui-theme="default"]) .arco-dropdown),
:global(html:not([data-ui-theme="default"]) .arco-popover),
:global(html:not([data-ui-theme="default"]) .arco-modal),
:global(html:not([data-ui-theme="default"]) .arco-drawer),
:global(html:not([data-ui-theme="default"]) .arco-picker-container) {
  border-color: rgba(125, 211, 252, 0.24) !important;
  background: rgba(12, 24, 42, 0.98) !important;
  color: #edf7ff !important;
}

:global(html:not([data-ui-theme="default"]) .arco-modal-header),
:global(html:not([data-ui-theme="default"]) .arco-modal-body),
:global(html:not([data-ui-theme="default"]) .arco-modal-footer),
:global(html:not([data-ui-theme="default"]) .arco-drawer-header),
:global(html:not([data-ui-theme="default"]) .arco-drawer-body),
:global(html:not([data-ui-theme="default"]) .arco-drawer-footer),
:global(html:not([data-ui-theme="default"]) .arco-select-option),
:global(html:not([data-ui-theme="default"]) .arco-dropdown-option),
:global(html:not([data-ui-theme="default"]) .arco-popover-content) {
  border-color: rgba(125, 211, 252, 0.16) !important;
  background: transparent !important;
  color: #edf7ff !important;
}

:global(html:not([data-ui-theme="default"]) .arco-select-option-hover),
:global(html:not([data-ui-theme="default"]) .arco-select-option-active),
:global(html:not([data-ui-theme="default"]) .arco-dropdown-option:hover) {
  background: rgba(32, 199, 232, 0.12) !important;
  color: #edf7ff !important;
}

:global(html:not([data-ui-theme="default"]) .arco-modal-title),
:global(html:not([data-ui-theme="default"]) .arco-drawer-title),
:global(html:not([data-ui-theme="default"]) .arco-select-option-content),
:global(html:not([data-ui-theme="default"]) .arco-dropdown-option-content),
:global(html:not([data-ui-theme="default"]) .arco-modal .arco-typography),
:global(html:not([data-ui-theme="default"]) .arco-drawer .arco-typography) {
  color: #edf7ff !important;
}

/* Original light theme */
.layout-container.theme-default {
  --app-accent: #165dff;
  --app-accent-2: #4080ff;
  --app-bg: #f5f7fa;
  --app-surface: #ffffff;
  --app-surface-soft: #fbfcff;
  --app-surface-strong: #ffffff;
  --app-border: #e5e8ef;
  --app-border-soft: #edf0f5;
  --app-text: #1d2129;
  --app-muted: #86909c;
  --app-glow: rgba(22, 93, 255, 0.08);
  background: #f5f7fa;
}

.theme-default .sider {
  background: linear-gradient(180deg, #1d2129 0%, #165dff 100%);
  border-right: 0;
  box-shadow: none;
}

.theme-default .logo {
  border-bottom-color: rgba(255, 255, 255, 0.1);
}

.theme-default .logo-icon {
  background: rgba(255, 255, 255, 0.2);
  box-shadow: none;
}

.theme-default .header {
  background: #fff;
  color: #1d2129;
  border-bottom: 0;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);
  backdrop-filter: none;
}

.theme-default .header :deep(.arco-typography) {
  color: #1d2129;
}

.theme-default .content {
  background: #f5f7fa;
}

.theme-default .stat-card,
.theme-default .task-list-card,
.theme-default .task-filter-panel,
.theme-default .task-filter-summary,
.theme-default .empty-state--card,
.theme-default .task-card-inner,
.theme-default .advanced-filter-collapse,
.theme-default .task-base-config-row,
.theme-default .advanced-config-card,
.theme-default .table-mapping-panel,
.theme-default .table-selector-panel,
.theme-default .transfer-pane,
.theme-default .type-card,
.theme-default .config-summary-card,
.theme-default .config-page-card,
.theme-default .config-section-card {
  border-color: #e5e8ef !important;
  background: #fff !important;
  color: #1d2129 !important;
  box-shadow: 0 8px 22px rgba(29, 33, 41, 0.05) !important;
}

.theme-default .task-base-config-row::before {
  display: none;
}

.theme-default .select-type-header :deep(.arco-typography),
.theme-default .type-content :deep(.arco-typography),
.theme-default .task-filter-panel__title,
.theme-default .task-filter-summary__title,
.theme-default .task-list-title-wrap :deep(.arco-typography),
.theme-default .task-title :deep(.arco-typography),
.theme-default .task-base-config-row :deep(.arco-form-item-label-col > label),
.theme-default .advanced-config-card :deep(.arco-form-item-label-col > label),
.theme-default .table-selector-form-item :deep(.arco-form-item-label-col > label),
.theme-default .transfer-header .title,
.theme-default .table-mapping-title {
  color: #1d2129 !important;
  text-shadow: none;
}

.theme-default .select-type-header :deep(.arco-typography-secondary),
.theme-default .type-content :deep(.arco-typography-secondary),
.theme-default .task-filter-panel__desc,
.theme-default .task-filter-summary__empty,
.theme-default .task-list-title-wrap :deep(.arco-typography-secondary),
.theme-default .advanced-config-card :deep(.arco-typography-secondary) {
  color: #86909c !important;
}

.theme-default .task-base-config-row :deep(.arco-input-wrapper),
.theme-default .task-base-config-row :deep(.arco-select-view),
.theme-default .advanced-config-card :deep(.arco-input-wrapper),
.theme-default .advanced-config-card :deep(.arco-input-number),
.theme-default .table-selector-panel :deep(.arco-input-wrapper),
.theme-default .table-mapping-panel :deep(.arco-input-wrapper),
.theme-default .transfer-pane :deep(.arco-input-wrapper),
.theme-default .task-list-card :deep(.arco-input-wrapper),
.theme-default .task-list-card :deep(.arco-select-view),
.theme-default .task-filter-panel :deep(.arco-input-wrapper),
.theme-default .task-filter-panel :deep(.arco-select-view),
.theme-default .advanced-filter-collapse :deep(.arco-input-wrapper),
.theme-default .advanced-filter-collapse :deep(.arco-select-view) {
  border-color: #edf0f5;
  background: #f7f9fc;
  color: #1d2129;
}

.theme-default .task-base-config-row :deep(.arco-input),
.theme-default .advanced-config-card :deep(.arco-input),
.theme-default .table-selector-panel :deep(.arco-input),
.theme-default .table-mapping-panel :deep(.arco-input),
.theme-default .transfer-pane :deep(.arco-input),
.theme-default .task-list-card :deep(.arco-input),
.theme-default .task-filter-panel :deep(.arco-input),
.theme-default .advanced-filter-collapse :deep(.arco-input),
.theme-default .task-base-config-row :deep(.arco-select-view-value),
.theme-default .task-list-card :deep(.arco-select-view-value),
.theme-default .task-filter-panel :deep(.arco-select-view-value) {
  color: #1d2129 !important;
}

.theme-default .transfer-header,
.theme-default .transfer-header-tip,
.theme-default .transfer-list-header,
.theme-default .transfer-search,
.theme-default .table-list-panel,
.theme-default .table-db-collapse,
.theme-default .advanced-filter-collapse :deep(.arco-collapse-item-header),
.theme-default .advanced-filter-collapse :deep(.arco-collapse-item-content) {
  background: #fbfcff !important;
  border-color: #edf0f5 !important;
  color: #4e5969 !important;
}

.theme-default .type-icon {
  border: 0;
}

.theme-default .mysql-icon { background: #e8f3ff !important; color: rgb(var(--primary-6)) !important; }
.theme-default .kafka-icon { background: #fff3e8 !important; color: rgb(var(--orange-6)) !important; }
.theme-default .webhook-icon { background: #e8ffee !important; color: rgb(var(--green-6)) !important; }
.theme-default .multi-icon { background: #f2f3f5 !important; color: var(--color-text-1) !important; }

.theme-default .header-right :deep(.arco-btn-primary),
.theme-default .task-base-config-row :deep(.arco-btn-primary),
.theme-default .empty-state :deep(.arco-btn-primary) {
  border: 1px solid #165dff;
  background: #165dff !important;
  box-shadow: none;
}

.theme-default .task-list-card :deep(.arco-tag),
.theme-default .task-filter-panel :deep(.arco-tag),
.theme-default .task-card-inner :deep(.arco-tag),
.theme-default .filter-chip :deep(.arco-tag),
.theme-default .task-filter-summary :deep(.arco-tag) {
  border-color: #d9e3f0 !important;
  background: #f2f7ff !important;
  color: #1d2129 !important;
}

.theme-default .task-card-inner :deep(.arco-tag[color="green"]),
.theme-default .task-card-inner :deep(.arco-tag-green) {
  border-color: #7bcf96 !important;
  background: #f0fff4 !important;
  color: #16803a !important;
}

.theme-default .task-card-inner :deep(.arco-tag[color="red"]),
.theme-default .task-card-inner :deep(.arco-tag-red) {
  border-color: #f5a8a8 !important;
  background: #fff1f0 !important;
  color: #c92a2a !important;
}

.theme-default .task-card-inner :deep(.arco-tag[color="orange"]),
.theme-default .task-card-inner :deep(.arco-tag-orange) {
  border-color: #f8c88c !important;
  background: #fff7ed !important;
  color: #b45309 !important;
}

.layout-container:not(.theme-default) .content {
  background:
    radial-gradient(circle at 14% 10%, var(--app-glow), transparent 30%),
    linear-gradient(135deg, var(--app-bg), var(--app-surface-strong));
  background-size: auto;
}

.theme-default .sider-menu :deep(.arco-menu-item:hover) {
  background: rgba(255, 255, 255, 0.15) !important;
}

.theme-default .sider-menu :deep(.arco-menu-item.arco-menu-selected) {
  background: rgba(255, 255, 255, 0.25) !important;
  box-shadow: none;
}

@media (max-width: 920px) {
  .theme-option-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
