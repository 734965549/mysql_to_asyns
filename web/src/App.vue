<script setup>
import { ref, onMounted, onUnmounted, watch, computed } from "vue";

import { Message, Modal } from "@arco-design/web-vue";

const API_BASE = "/api";

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

const editMode = ref(false);

const editingTaskId = ref(null);

// 任务详情抽屉

const detailDrawerVisible = ref(false);

const selectedTaskForDetail = ref(null);

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
  const currentSourceSchema =
    activeTableSourceDatabase.value || taskForm.value.source_schema;

  if (!currentSourceSchema) {
    Message.warning("请先选择源数据库");

    return;
  }

  refreshingTables.value = true;

  try {
    await fetch(`${API_BASE}/metadata/refresh`, { method: "POST" });

    const res = await fetch(
      `${API_BASE}/metadata/tables?schema=${currentSourceSchema}`,
    );

    if (res.ok) {
      tables.value = await res.json();
    }
  } catch (e) {
    Message.error("刷新表列表失败");

    console.error("刷新表列表失败:", e);
  } finally {
    refreshingTables.value = false;
  }
}

// 获取表列表

async function fetchTables() {
  const currentSourceSchema =
    activeTableSourceDatabase.value || taskForm.value.source_schema;

  if (!currentSourceSchema) {
    return;
  }

  taskForm.value.source_schema = currentSourceSchema;

  loading.value = true;

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

          database:
            customSourceDB.value.database || taskForm.value.source_schema,
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

          database: configForm.value.datasource.database || currentSourceSchema,
        };
      }
    }

    let res;

    if (dbConfig && dbConfig.host) {
      // 使用自定义配置获取表列表

      res = await fetch(`${API_BASE}/metadata/tables-with-config`, {
        method: "POST",

        headers: { "Content-Type": "application/json" },

        body: JSON.stringify({
          ...dbConfig,

          schema: currentSourceSchema,
        }),
      });
    } else {
      // 使用默认连接（后端启动时建立的连接）

      res = await fetch(
        `${API_BASE}/metadata/tables?schema=${currentSourceSchema}`,
      );
    }

    if (res.ok) {
      tables.value = await res.json();
    } else {
      // 解析错误信息并显示给用户

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
    loading.value = false;
  }
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

    enable_read_only: false,

    enable_drop_table_before_ddl: false,
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

function onTableSourceDatabasesChange(databases) {
  selectedDatabases.value = databases;

  if (databases.length === 0) {
    activeTableSourceDatabase.value = "";

    tables.value = [];

    tableSearchText.value = "";

    return;
  }

  if (!databases.includes(activeTableSourceDatabase.value)) {
    activeTableSourceDatabase.value = databases[0];
  }

  taskForm.value.source_schema = activeTableSourceDatabase.value || "";

  fetchTables();
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
    targetDatabasesPayload = targetDatabaseMappings.value.map((m) => m.target);

    sourceSchemaPayload = "";
    targetSchemaPayload = "";
    targetDatabasePayload = taskForm.value.target_database || "";
  } else {
    sourceDatabasesPayload = selectedDatabases.value;
    targetDatabasesPayload = selectedDatabases.value.map((db) => {
      const mapping = targetDatabaseMappings.value.find(
        (item) => item.source === db,
      );

      return mapping?.target || db;
    });

    sourceSchemaPayload = selectedDatabases.value[0] || "";
    targetSchemaPayload = targetDatabasesPayload[0] || "";
    targetDatabasePayload = taskForm.value.target_database || targetSchemaPayload;

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

const scheduleModalVisible = ref(false);
const scheduleTaskId = ref("");
const scheduleMode = ref("cron");
const scheduleTime = ref("");
const scheduleCron = ref("0 9 * * 1-5");
const scheduleTimezone = ref(Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Shanghai");

function openStartTaskModal(taskId, mode = "cron") {
  scheduleTaskId.value = taskId;
  scheduleMode.value = mode;
  scheduleCron.value = "0 9 * * 1-5";
  scheduleTimezone.value = Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Shanghai";
  const d = new Date(Date.now() + 5 * 60 * 1000);
  scheduleTime.value = toDateTimeInputValue(d);
  scheduleModalVisible.value = true;
}

function toDateTimeInputValue(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function openScheduleModal(taskId) {
  openStartTaskModal(taskId, "cron");
}

async function confirmSchedule() {
  try {
    const payload = {};

    const expr = String(scheduleCron.value || "").trim();
    if (!expr) {
      Message.error("请输入 cron 表达式");
      return;
    }
    payload.scheduled_at = new Date().toISOString();
    payload.schedule_mode = "cron";
    payload.cron_expression = expr;
    payload.cron_timezone = String(scheduleTimezone.value || "").trim();

    const res = await fetch(`${API_BASE}/tasks/${scheduleTaskId.value}/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: Object.keys(payload).length ? JSON.stringify(payload) : undefined,
    });

    if (res.ok) {
      fetchTasks();
      Message.success(scheduleMode.value === "immediate" ? "任务已启动" : "定时启动已设置");
      scheduleModalVisible.value = false;
    } else {
      const errorMsg = await handleApiError(res, scheduleMode.value === "immediate" ? "启动失败" : "设置定时启动失败");
      Message.error(errorMsg);
    }
  } catch (e) {
    Message.error((scheduleMode.value === "immediate" ? "启动失败" : "设置定时启动失败") + ": " + e.message);
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

// 显示任务详情

function showTaskDetail(task) {
  selectedTaskForDetail.value = task;

  detailDrawerVisible.value = true;
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

    enable_read_only: task.config.enable_read_only || false,

    enable_drop_table_before_ddl: task.config.enable_drop_table_before_ddl || false,
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

    fetchTables();
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

// 计算进度

function getProgress(task) {
  // 防止除零错误

  if (!task.context.total_rows || task.context.total_rows <= 0) {
    return 0;
  }

  // 防止 processed_rows 为负数或异常值

  const processed = Math.max(0, task.context.processed_rows || 0);

  // 计算百分比，并限制在 0-100 之间，保留两位小数
  let percent = (processed / task.context.total_rows) * 100;
  percent = Math.min(100, Math.max(0, percent));

  // 如果正好是整数直接返回，否则保留两位小数
  return Number(percent.toFixed(2));
}

// 监听同步级别变化

function onSyncLevelChange() {
  selectedDatabases.value = [];

  targetDatabaseMappings.value = [];

  selectedTables.value = [];

  activeTableSourceDatabase.value = "";

  tableSelectionsByDatabase.value = {};

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
  window.addEventListener("popstate", handlePopState);

  await fetchDefaultConfig();

  fetchDatabases();

  loadTaskFiltersFromUrl();
  await fetchTasks(taskPagination.value.current, taskPagination.value.pageSize);

  refreshInterval = setInterval(() => {
    fetchTasks(taskPagination.value.current, taskPagination.value.pageSize);
  }, 3000);
});

onUnmounted(() => {
  window.removeEventListener("popstate", handlePopState);

  if (refreshInterval) clearInterval(refreshInterval);
});

// 菜单点击处理

function onMenuClick(key) {
  console.log("Menu item clicked:", key);

  const prevKey = selectedKey.value[0];

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
  sort: "created_at_desc",
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

        tables.value = [];
      } else if (!newDbs.includes(activeTableSourceDatabase.value)) {
        activeTableSourceDatabase.value = newDbs[0];

        fetchTables();
      }

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
</script>

<template>
  <a-layout class="layout-container">
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
          <a-form :model="taskForm" layout="vertical">
            <a-row :gutter="32">
              <a-col :span="12">
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

                <!-- 表级别：多选源数据库 -->

                <a-form-item
                  v-if="selectedSyncLevel === 'table'"
                  label="源数据库"
                  required
                >
                  <a-space wrap>
                    <a-select
                      v-model="selectedDatabases"
                      placeholder="请选择源数据库（可多选）"
                      style="width: 360px"
                      multiple
                      allow-clear
                      @change="onTableSourceDatabasesChange"
                    >
                      <a-option v-for="db in databases" :key="db" :value="db">{{
                        db
                      }}</a-option>
                    </a-select>

                    <a-select
                      v-model="activeTableSourceDatabase"
                      placeholder="请选择当前要选表的源库"
                      style="width: 300px"
                      :disabled="selectedDatabases.length === 0"
                      @change="onActiveTableSourceDatabaseChange"
                    >
                      <a-option
                        v-for="db in selectedDatabases"
                        :key="db"
                        :value="db"
                        >{{ db }}</a-option
                      >
                    </a-select>

                    <a-button
                      type="text"
                      size="small"
                      :loading="refreshingDatabases"
                      @click="refreshDatabases"
                    >
                      <template #icon><icon-refresh /></template>
                    </a-button>
                  </a-space>
                </a-form-item>

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

                <!-- 表级别：目标库、目标表映射 -->

                <a-row :gutter="16">
                  <a-col
                    v-if="selectedSyncLevel === 'table'"
                    :span="targetType === 'MYSQL' ? 12 : 24"
                  >
                    <a-form-item label="目标数据库" required>
                      <a-input
                        v-model="taskForm.target_database"
                        placeholder="请输入目标数据库名"
                      />
                    </a-form-item>
                  </a-col>

                  <a-col
                    :span="selectedSyncLevel === 'table' && targetType === 'MYSQL' ? 12 : 24"
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

                <div v-if="selectedSyncLevel === 'table'" class="table-mapping-panel">
                  <div class="table-mapping-title">表级别同步目标表配置</div>

                  <div class="table-mapping-list">
                    <div
                      v-for="mapping in currentDatabaseTargetTableMappings"
                      :key="mapping.source"
                      class="table-mapping-item"
                    >
                      <span class="table-mapping-source">{{ mapping.source }}</span>

                      <icon-arrow-right style="color: #86909c" />

                      <a-input
                        v-model="targetTableMappings[mapping.source]"
                        placeholder="目标表名"
                        style="width: 220px"
                      />
                    </div>

                    <a-empty
                      v-if="currentDatabaseTargetTableMappings.length === 0"
                      description="请先在左侧选择当前源库并勾选表"
                    />
                  </div>
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
                      v-if="!activeTableSourceDatabase"
                      type="info"
                      style="margin-bottom: 8px"
                      show-icon
                    >
                      请先选择至少一个源数据库，再选择当前源库后勾选表
                    </a-alert>

                    <div class="table-toolbar">
                      <a-input
                        v-model="tableSearchText"
                        placeholder="搜索表名..."
                        allow-clear
                        class="table-search-input"
                        :disabled="!activeTableSourceDatabase"
                      >
                        <template #suffix><icon-search /></template>
                      </a-input>

                      <a-button
                        type="text"
                        size="small"
                        :disabled="!activeTableSourceDatabase"
                        @click="toggleAllTables"
                      >
                        {{
                          currentDatabaseSelectedTables.length ===
                            filteredTables.length && filteredTables.length > 0
                            ? "取消全选"
                            : "全选"
                        }}
                      </a-button>

                      <a-button
                        type="text"
                        size="small"
                        :disabled="!activeTableSourceDatabase"
                        :loading="refreshingTables"
                        @click="refreshTables"
                      >
                        <template #icon><icon-refresh /></template>
                      </a-button>
                    </div>

                    <div class="table-list-panel">
                      <a-checkbox-group
                        v-model="currentDatabaseSelectedTables"
                        v-if="
                          activeTableSourceDatabase && filteredTables.length > 0
                        "
                        class="table-checkbox-group"
                      >
                        <div class="table-list-grid">
                          <div
                            class="table-list-item"
                            v-for="table in filteredTables"
                            :key="table.table_name"
                          >
                            <a-checkbox :value="table.table_name">
                              {{ table.table_name }}

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
                          activeTableSourceDatabase
                            ? '暂无匹配的表'
                            : '请先选择当前源库'
                        "
                        :style="{ padding: '20px 0' }"
                      />
                    </div>

                    <div style="margin-top: 8px">
                      <a-typography-text type="secondary">
                        当前库 {{ activeTableSourceDatabase || "-" }} 已选
                        {{ currentDatabaseSelectedTables.length }} 个表，
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
                  </a-row>

                  <a-form-item v-if="targetType === 'MYSQL'">
                    <a-checkbox v-model="taskForm.optimize_index">
                      <a-space direction="vertical" :size="4">
                        <span style="font-weight: 500">启用索引优化</span>

                        <a-typography-text
                          type="secondary"
                          style="font-size: 12px"
                        >
                          同步前删除非主键索引以提高写入性能，同步完成后自动重建
                        </a-typography-text>
                      </a-space>
                    </a-checkbox>
                  </a-form-item>

                  <a-form-item v-if="targetType === 'MYSQL'">
                    <a-checkbox v-model="taskForm.enable_read_only">
                      <a-space direction="vertical" :size="4">
                        <span style="font-weight: 500">临时关闭目标库只读</span>

                        <a-typography-text
                          type="secondary"
                          style="font-size: 12px"
                        >
                          同步开始时自动关闭目标库 read_only /
                          super_read_only，同步结束后自动恢复
                        </a-typography-text>
                      </a-space>
                    </a-checkbox>
                  </a-form-item>

                  <a-form-item v-if="targetType === 'MYSQL'">
                    <a-checkbox v-model="taskForm.enable_drop_table_before_ddl">
                      <a-space direction="vertical" :size="4">
                        <span style="font-weight: 500">同步 DDL 前删除目标表</span>

                        <a-typography-text
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
              <div
                style="
                  display: flex;
                  align-items: center;
                  justify-content: space-between;
                  width: 100%;
                "
              >
                <span>任务列表</span>
                <div style="display: flex; gap: 8px; align-items: center; flex-wrap: wrap; justify-content: flex-end;">
                  <a-select
                    v-model="taskFilters.status"
                    placeholder="状态"
                    allow-clear
                    style="width: 130px"
                    @change="() => fetchTasks(1, taskPagination.value.pageSize)"
                  >
                    <a-option value="PENDING">待执行</a-option>
                    <a-option value="RUNNING">运行中</a-option>
                    <a-option value="PAUSED">已暂停</a-option>
                    <a-option value="SCHEDULED">已计划</a-option>
                    <a-option value="COMPLETED">已完成</a-option>
                    <a-option value="FAILED">失败</a-option>
                  </a-select>
                  <a-select
                    v-model="taskFilters.sort"
                    placeholder="排序"
                    style="width: 150px"
                    @change="() => fetchTasks(1, taskPagination.pageSize)"
                  >
                    <a-option value="created_at_desc">创建时间倒序</a-option>
                    <a-option value="created_at_asc">创建时间正序</a-option>
                    <a-option value="name_asc">名称正序</a-option>
                    <a-option value="name_desc">名称倒序</a-option>
                    <a-option value="status_asc">状态正序</a-option>
                    <a-option value="status_desc">状态倒序</a-option>
                    <a-option value="progress_asc">进度正序</a-option>
                    <a-option value="progress_desc">进度倒序</a-option>
                  </a-select>
                  <a-input-search
                    v-model="taskFilters.keyword"
                    placeholder="搜索任务名称/ID/表名..."
                    style="width: 280px"
                    allow-clear
                    @search="() => fetchTasks(1, taskPagination.value.pageSize)"
                    @clear="() => fetchTasks(1, taskPagination.value.pageSize)"
                  />
                </div>
              </div>
            </template>

            <div v-if="filteredTasks.length === 0" class="empty-state">
              <a-empty description="暂无匹配的任务">
                <a-button type="primary" @click="openCreateDialog"
                  >创建任务</a-button
                >
              </a-empty>
            </div>

            <a-list v-else :bordered="false">
              <a-list-item
                v-for="task in paginatedTasks"
                :key="task.config.id"
                class="task-item"
              >
                <a-card :bordered="false" class="task-card-inner">
                  <div class="task-header">
                    <div class="task-title">
                      <a-typography-title :heading="6" style="margin: 0">
                        {{ task.config.name }}
                      </a-typography-title>

                      <a-tag
                        :color="getStatusColor(task.context.status)"
                        size="small"
                      >
                        {{ getStatusText(task.context.status) }}
                      </a-tag>
                      <a-tag
                        v-if="task.context.status === 'SCHEDULED' && task.context.scheduled_at"
                        color="arcoblue"
                        size="small"
                      >
                        <icon-clock-circle /> {{ formatScheduledTime(task) }}
                      </a-tag>
                      <!-- 目标端类型 Badge -->
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
                      >
                        {{
                          task.config.sink_configs.length > 1
                            ? "MULTI-SINK"
                            : getSinkTypeLabel(task.config.sink_configs[0].type)
                        }}
                      </a-tag>
                      <a-tag v-else color="blue" size="small" bordered
                        >MySQL 数据库</a-tag
                      >
                    </div>
                  </div>

                  <a-descriptions :column="4" size="small" class="task-desc">
                    <a-descriptions-item label="同步级别">
                      {{
                        task.config.sync_level === "DATABASE"
                          ? "库级别"
                          : "表级别"
                      }}
                    </a-descriptions-item>

                    <a-descriptions-item label="源库">
                      <template
                        v-if="
                          task.config.sync_level === 'DATABASE' &&
                          task.config.source_databases?.length
                        "
                      >
                        <a-tag
                          v-for="db in task.config.source_databases"
                          :key="db"
                          size="small"
                          color="arcoblue"
                          style="margin-right: 4px"
                          >{{ db }}</a-tag
                        >
                      </template>

                      <template v-else>{{
                        task.config.source_schema || "-"
                      }}</template>
                    </a-descriptions-item>

                    <a-descriptions-item label="目标端">
                      <!-- 根据目标类型显示不同的目标信息 -->
                      <template
                        v-if="
                          task.config.sink_configs &&
                          task.config.sink_configs.length > 0
                        "
                      >
                        <template v-if="task.config.sink_configs.length > 1">
                          <a-tag size="small" color="purple"
                            >{{
                              task.config.sink_configs.length
                            }}
                            个目标端</a-tag
                          >
                        </template>
                        <template v-else>
                          <template
                            v-if="task.config.sink_configs[0].type === 'KAFKA'"
                          >
                            <a-tag
                              size="small"
                              color="orange"
                              style="max-width: 150px"
                              class="text-ellipsis"
                              :title="
                                task.config.sink_configs[0].options?.topic
                              "
                            >
                              Topic:
                              {{
                                task.config.sink_configs[0].options?.topic ||
                                "-"
                              }}
                            </a-tag>
                          </template>
                          <template
                            v-else-if="
                              task.config.sink_configs[0].type ===
                              'HTTP_WEBHOOK'
                            "
                          >
                            <a-tag
                              size="small"
                              color="green"
                              style="max-width: 150px"
                              class="text-ellipsis"
                              :title="task.config.sink_configs[0].options?.url"
                            >
                              {{
                                task.config.sink_configs[0].options?.url || "-"
                              }}
                            </a-tag>
                          </template>
                          <template v-else>
                            <template
                              v-if="
                                task.config.sync_level === 'DATABASE' &&
                                task.config.target_databases?.length
                              "
                            >
                              <a-tag
                                v-for="db in task.config.target_databases"
                                :key="db"
                                size="small"
                                color="green"
                                style="margin-right: 4px"
                                >{{ db }}</a-tag
                              >
                            </template>
                            <template v-else>{{
                              task.config.sink_configs[0].options
                                ?.target_schema ||
                              task.config.target_schema ||
                              "-"
                            }}</template>
                          </template>
                        </template>
                      </template>
                      <template v-else>
                        <template
                          v-if="
                            task.config.sync_level === 'DATABASE' &&
                            task.config.target_databases?.length
                          "
                        >
                          <a-tag
                            v-for="db in task.config.target_databases"
                            :key="db"
                            size="small"
                            color="green"
                            style="margin-right: 4px"
                            >{{ db }}</a-tag
                          >
                        </template>
                        <template v-else>{{
                          task.config.target_schema || "-"
                        }}</template>
                      </template>
                    </a-descriptions-item>

                    <a-descriptions-item label="表数量">
                      {{
                        task.config.sync_level === "DATABASE"
                          ? "全库"
                          : task.config.tables?.length || 0
                      }}
                    </a-descriptions-item>
                  </a-descriptions>

                  <!-- 进度条 -->

                  <div
                    v-if="task.context.status === 'RUNNING'"
                    class="task-progress"
                  >
                    <a-progress
                      :percent="getProgress(task) / 100"
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

                  <!-- 操作按钮 -->

                  <div class="task-actions">
                    <a-button size="small" @click="showTaskDetail(task)">
                      <template #icon><icon-eye /></template>

                      详情
                    </a-button>

                    <a-button size="small" @click="openDuplicateFromTask(task)">
                      <template #icon><icon-copy /></template>

                      复制新建
                    </a-button>

                    <a-button
                      v-if="
                        task.context.status === 'PENDING' ||
                        task.context.status === 'PAUSED'
                      "
                      size="small"
                      @click="openEditDialog(task)"
                    >
                      <template #icon><icon-edit /></template>

                      编辑
                    </a-button>

                    <a-dropdown-button
                      v-if="
                        task.context.status === 'PENDING' ||
                        task.context.status === 'PAUSED' ||
                        task.context.status === 'FAILED'
                      "
                      type="primary"
                      size="small"
                      status="success"
                    >
                      <icon-play-arrow /> 启动
                      <template #content>
                        <a-doption @click="openStartTaskModal(task.config.id, 'cron')">Cron 定时启动</a-doption>
                      </template>
                    </a-dropdown-button>

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

        <div v-show="taskFormPage === 'none' && currentPage === 'config'">
          <a-card title="系统配置 (etc/application.toml)">
            <a-form :model="configForm" layout="vertical" @submit="saveConfig">
              <a-row :gutter="32">
                <!-- 基础配置 -->

                <a-col :span="12">
                  <a-typography-title :heading="6"
                    >HTTP 服务配置</a-typography-title
                  >

                  <a-form-item label="监听地址">
                    <a-input
                      v-model="configForm.http.host"
                      placeholder="0.0.0.0"
                    />
                  </a-form-item>

                  <a-form-item label="监听端口">
                    <a-input-number
                      v-model="configForm.http.port"
                      :min="1"
                      :max="65535"
                    />
                  </a-form-item>

                  <a-typography-title :heading="6" style="margin-top: 20px"
                    >Redis 状态持久化配置</a-typography-title
                  >

                  <a-form-item label="主机">
                    <a-input
                      v-model="configForm.redis.host"
                      placeholder="127.0.0.1"
                    />
                  </a-form-item>

                  <a-form-item label="端口">
                    <a-input-number
                      v-model="configForm.redis.port"
                      :min="1"
                      :max="65535"
                    />
                  </a-form-item>

                  <a-form-item label="密码">
                    <a-input-password
                      v-model="configForm.redis.password"
                      placeholder="留空表示无密码"
                    />
                  </a-form-item>

                  <a-form-item label="数据库索引 (DB)">
                    <a-input-number
                      v-model="configForm.redis.db"
                      :min="0"
                      :max="15"
                    />
                  </a-form-item>
                </a-col>

                <!-- 日志配置（支持热加载） -->

                <a-col :span="12">
                  <a-typography-title :heading="6">
                    日志配置
                    <a-tag color="green" size="small" style="margin-left: 8px; vertical-align: middle">热加载</a-tag>
                  </a-typography-title>

                  <a-form-item label="日志级别">
                    <a-select v-model="configForm.log.level">
                      <a-option value="debug">Debug</a-option>

                      <a-option value="info">Info</a-option>

                      <a-option value="warn">Warn</a-option>

                      <a-option value="error">Error</a-option>
                    </a-select>
                  </a-form-item>

                  <a-form-item label="输出开关">
                    <a-space direction="vertical">
                      <a-checkbox v-model="configForm.log.console.enable">
                        开启控制台标准输出 (Stdout)
                      </a-checkbox>

                      <a-checkbox v-model="configForm.log.file.enable">
                        开启文件持久化输出 (File)
                      </a-checkbox>
                    </a-space>
                  </a-form-item>

                  <a-form-item>
                    <a-button
                      type="primary"
                      status="success"
                      :loading="logApplying"
                      @click="applyLogConfig"
                      style="width: 100%"
                    >
                      <template #icon><icon-sync /></template>
                      立即应用日志配置（无需重启）
                    </a-button>
                    <div style="margin-top: 4px; color: #86909c; font-size: 12px">
                      修改日志级别或输出开关后点击此按钮，配置即刻生效并持久化到配置文件
                    </div>
                  </a-form-item>

                  <a-typography-title :heading="6" style="margin-top: 20px"
                    >默认数据库环境 (用于元数据浏览)</a-typography-title
                  >

                  <a-form-item label="默认源库地址">
                    <a-input v-model="configForm.datasource.host" />
                  </a-form-item>

                  <a-form-item label="默认目标库地址">
                    <a-input v-model="configForm.target.host" />
                  </a-form-item>

                  <a-form-item label="调试模式 (Debug)">
                    <a-switch v-model="configForm.datasource.debug" />
                  </a-form-item>

                  <a-typography-title :heading="6" style="margin-top: 20px"
                    >任务数据持久化配置</a-typography-title
                  >

                  <a-form-item label="持久化模式">
                    <a-radio-group
                      v-model="configForm.storage.mode"
                      type="button"
                    >
                      <a-radio value="file">本地文件</a-radio>

                      <a-radio value="mysql">MySQL 数据库</a-radio>
                    </a-radio-group>
                  </a-form-item>

                  <template v-if="configForm.storage.mode === 'file'">
                    <a-form-item label="数据目录">
                      <a-input
                        v-model="configForm.storage.data_dir"
                        placeholder="data"
                      />
                    </a-form-item>
                  </template>

                  <template v-if="configForm.storage.mode === 'mysql'">
                    <a-form-item label="MySQL 主机">
                      <a-input
                        v-model="configForm.storage.host"
                        placeholder="127.0.0.1"
                      />
                    </a-form-item>

                    <a-row :gutter="16">
                      <a-col :span="12">
                        <a-form-item label="端口">
                          <a-input-number
                            v-model="configForm.storage.port"
                            :min="1"
                            :max="65535"
                          />
                        </a-form-item>
                      </a-col>

                      <a-col :span="12">
                        <a-form-item label="数据库">
                          <a-input v-model="configForm.storage.database" />
                        </a-form-item>
                      </a-col>
                    </a-row>

                    <a-form-item label="用户名">
                      <a-input v-model="configForm.storage.username" />
                    </a-form-item>

                    <a-form-item label="密码">
                      <a-input-password v-model="configForm.storage.password" />
                    </a-form-item>
                  </template>
                </a-col>
              </a-row>

              <div
                style="
                  margin-top: 30px;
                  text-align: center;
                  border-top: 1px solid #f0f0f0;
                  padding-top: 20px;
                "
              >
                <a-button
                  type="primary"
                  size="large"
                  :loading="configLoading"
                  @click="saveConfig"
                >
                  保存并同步到 application.toml
                </a-button>

                <div style="margin-top: 12px">
                  <a-typography-text type="secondary">
                    <icon-info-circle />
                    注意：修改配置后将直接改写服务器磁盘文件，部分底层服务（如端口监听）需重启
                    Go 程序生效。
                  </a-typography-text>
                </div>
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
                v-for="(src, i) in selectedTaskForDetail.config
                  .source_databases || []"
                :key="src"
                style="
                  display: inline-flex;
                  align-items: center;
                  margin-right: 12px;
                "
              >
                <a-tag color="blue">{{ src }}</a-tag>

                <span style="margin: 0 4px">→</span>

                <a-tag color="green">{{
                  (selectedTaskForDetail.config.target_databases || [])[i] ||
                  src
                }}</a-tag>
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
            label="目标数据库"
            :span="2"
          >
            <a-space wrap>
              <template
                v-if="
                  (selectedTaskForDetail.config.target_databases || []).length
                "
              >
                <a-tag
                  v-for="db in selectedTaskForDetail.config.target_databases"
                  :key="db"
                  color="green"
                  >{{ db }}</a-tag
                >
              </template>

              <span v-else>{{
                selectedTaskForDetail.config.target_schema || "-"
              }}</span>
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
            {{ selectedTaskForDetail.context.processed_rows || 0 }}
          </a-descriptions-item>

          <a-descriptions-item label="总行数">
            {{ selectedTaskForDetail.context.total_rows || 0 }}
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

            <a-dropdown-button
              v-if="
                selectedTaskForDetail.context.status === 'PENDING' ||
                selectedTaskForDetail.context.status === 'PAUSED' ||
                selectedTaskForDetail.context.status === 'FAILED'
              "
              type="primary"
              status="success"
            >
              <icon-play-arrow /> 启动
              <template #content>
                <a-doption @click="openStartTaskModal(selectedTaskForDetail.config.id, 'cron'); detailDrawerVisible = false;">Cron 定时启动</a-doption>
              </template>
            </a-dropdown-button>

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

    <!-- 启动任务弹窗 -->
    <a-modal
      v-model:visible="scheduleModalVisible"
      title="Cron 定时启动"
      @ok="confirmSchedule"
      @cancel="scheduleModalVisible = false"
      ok-text="确认"
      cancel-text="取消"
      :width="720"
    >
      <a-form layout="vertical">
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
      </a-form>
    </a-modal>
  </a-layout>
</template>

<style scoped>
.layout-container {
  height: 100vh;

  background: #f5f7fa;
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
  border-radius: 8px;
}

.empty-state {
  padding: 60px 0;

  text-align: center;
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
  background: #fafbfc;

  border-radius: 8px;

  width: 100%;
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

  gap: 12px;
}

.task-desc {
  margin-bottom: 12px;
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

  border: 1px solid #e5e6eb;

  border-radius: 4px;

  background: #fff;

  height: 100%;

  overflow: hidden;
}

.transfer-header {
  padding: 8px 12px;

  border-bottom: 1px solid #e5e6eb;

  background: #f7f8fa;

  display: flex;

  justify-content: space-between;

  align-items: center;
}

.transfer-header .title {
  font-weight: 500;

  font-size: 14px;

  color: #1d2129;
}

.transfer-header-tip {
  display: flex;

  padding: 6px 12px;

  background: #fff;

  border-bottom: 1px solid #f2f3f5;

  font-size: 12px;

  color: #86909c;
}

.transfer-header-tip span {
  flex: 1;
}

.transfer-search {
  padding: 8px 12px;

  border-bottom: 1px solid #e5e6eb;
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

  border: 1px solid #e5e6eb;

  border-radius: 6px;

  padding: 10px;

  background: #fafafa;

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

  color: #86909c;

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
</style>
