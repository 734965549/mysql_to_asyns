#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB = path.resolve(__dirname, "..");
const SRC = path.join(WEB, "src");
const EXT = path.join(SRC, "_extract");
const read = (p) => fs.readFileSync(p, "utf8");
const write = (rel, c) => {
  const p = path.join(WEB, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, c.replace(/\r\n/g, "\n"), "utf8");
  console.log("OK", rel, c.split("\n").length);
};

function cleanTpl(html) {
  return html
    .replace(/\s+v-if="isTaskDetailPage"/g, "")
    .replace(/\s+v-if="!isTaskDetailPage"/g, "")
    .replace(/\s+v-if="taskFormPage === 'select_type'"/g, "")
    .replace(
      /\s+v-if="taskFormPage === 'create' \|\| taskFormPage === 'edit'"/g,
      "",
    )
    .replace(
      /\s+v-show="taskFormPage === 'none' && currentPage === 'tasks'"/g,
      "",
    )
    .replace(
      /\s+v-show="taskFormPage === 'none' && currentPage === 'config'"/g,
      "",
    )
    .replace(/\s*:class="appThemeClass"/g, "");
}

// ========== ConfigView ==========
{
  const tpl = cleanTpl(read(path.join(EXT, "tpl-config.html")));
  write(
    "src/views/ConfigView.vue",
    `<script setup>
import { ref, onMounted } from "vue";
import { Message } from "@arco-design/web-vue";
import { API_BASE } from "../composables/useApi.js";
import { useDefaultConfig } from "../composables/useDefaultConfig.js";
import { useUiTheme } from "../composables/useUiTheme.js";

const { configForm, configLoading, fetchDefaultConfig, ensureDefaultConfig } =
  useDefaultConfig();
const { uiTheme, uiThemeOptions, setUiTheme } = useUiTheme();
const logApplying = ref(false);

async function saveConfig() {
  configLoading.value = true;
  try {
    const res = await fetch(\`\${API_BASE}/config/update\`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(configForm.value),
    });
    if (res.ok) {
      Message.success("系统配置已更新，配置文件已同步");
      await fetchDefaultConfig();
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

async function applyLogConfig() {
  logApplying.value = true;
  try {
    const res = await fetch(\`\${API_BASE}/config/log\`, {
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
      Message.success(
        \`日志配置已热加载生效 — 级别: \${data.level?.toUpperCase()}, 控制台: \${data.console ? "开" : "关"}, 文件: \${data.file ? "开" : "关"}\`,
      );
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

onMounted(() => {
  ensureDefaultConfig();
});
</script>

<template>
${tpl}
</template>

<style scoped>
${read(path.join(EXT, "style-config.css"))}
</style>
`,
  );
}

// ========== TaskListView ==========
{
  // Extract list-related pieces by transforming App script lightly isn't practical;
  // build a focused list view that ports the essential list logic.
  const tpl = cleanTpl(read(path.join(EXT, "tpl-list.html")));
  // list template still references many helpers — we'll include them via imports + local ports
  write(
    "src/views/TaskListView.vue",
    `<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { Message } from "@arco-design/web-vue";
import { API_BASE } from "../composables/useApi.js";
import { useDefaultConfig } from "../composables/useDefaultConfig.js";
import { useTaskActions } from "../composables/useTaskActions.js";
import StartTaskModal from "../components/StartTaskModal.vue";
import { getTaskDatabaseMappings } from "../utils/databaseMappings.js";
import {
  hasExplicitSinkConfigs,
  resolveTaskTargetMySQLDisplay,
} from "../utils/taskTargetDisplay.js";
import {
  getStatusColor,
  getStatusText,
  getProgress,
  getProgressRatio,
  formatTime,
  calculateDuration,
  formatRowCount,
  formatTotalRowDisplay,
  getRowCountMeta,
  canEndTask,
  canCompareRows,
  isComparingRows,
  formatScheduledTime,
  showTaskDetail,
} from "../utils/taskFormatters.js";

const route = useRoute();
const router = useRouter();
const { configForm, ensureDefaultConfig } = useDefaultConfig();
const startModalRef = ref(null);

const TASK_SORT_OPTIONS_URL = "/api/tasks/sort-options";
const TASK_SORT_FALLBACK_OPTIONS = [
  { value: "created_at_desc", label: "创建时间（新 → 旧）", default: true },
  { value: "created_at_asc", label: "创建时间（旧 → 新）" },
  { value: "name_asc", label: "任务名称（A → Z）" },
  { value: "name_desc", label: "任务名称（Z → A）" },
  { value: "status_asc", label: "状态优先（待执行 → 失败）" },
  { value: "status_desc", label: "状态优先（失败 → 待执行）" },
  { value: "progress_asc", label: "进度（低 → 高）" },
  { value: "progress_desc", label: "进度（高 → 低）" },
];

const tasks = ref([]);
const taskSortOptions = ref([...TASK_SORT_FALLBACK_OPTIONS]);
const taskSortDefault = ref("created_at_desc");
const taskSortLabelMap = computed(() =>
  Object.fromEntries(taskSortOptions.value.map((o) => [o.value, o.label])),
);
const taskFilters = ref({ status: "", keyword: "", sort: "created_at_desc" });
const taskPagination = ref({ current: 1, pageSize: 10, total: 0 });
const filteredTasks = computed(() => tasks.value);
const paginatedTasks = computed(() => filteredTasks.value);

let refreshInterval = null;
let taskFetchSeq = 0;
const syncUrlDebounceState = { timer: null };

function getTaskTargetMySQLDisplay(config) {
  return resolveTaskTargetMySQLDisplay(config, configForm.value.target);
}

async function loadTaskSortOptions() {
  try {
    const res = await fetch(TASK_SORT_OPTIONS_URL);
    if (!res.ok) return;
    const data = await res.json();
    if (Array.isArray(data.options) && data.options.length > 0) {
      taskSortOptions.value = data.options;
      const defaultOption = data.options.find((o) => o.default) || data.options[0];
      if (defaultOption?.value) taskSortDefault.value = defaultOption.value;
    }
  } catch (e) {
    console.warn("加载任务排序选项失败，使用本地回退定义:", e);
  }
}

function syncTaskFiltersToUrl() {
  if (syncUrlDebounceState.timer) clearTimeout(syncUrlDebounceState.timer);
  syncUrlDebounceState.timer = setTimeout(() => {
    const query = {};
    if (taskPagination.value.current > 1) query.page = String(taskPagination.value.current);
    if (taskPagination.value.pageSize !== 10) query.page_size = String(taskPagination.value.pageSize);
    if (taskFilters.value.status) query.status = taskFilters.value.status;
    if (taskFilters.value.keyword) query.keyword = taskFilters.value.keyword;
    if (taskFilters.value.sort) query.sort = taskFilters.value.sort;
    router.replace({ query });
  }, 0);
}

function loadTaskFiltersFromUrl() {
  const q = route.query;
  const page = Number.parseInt(String(q.page || "1"), 10);
  const pageSize = Number.parseInt(String(q.page_size || "10"), 10);
  taskPagination.value.current = Number.isFinite(page) && page > 0 ? page : 1;
  taskPagination.value.pageSize = Number.isFinite(pageSize) && pageSize > 0 ? pageSize : 10;
  taskFilters.value.status = String(q.status || "");
  taskFilters.value.keyword = String(q.keyword || "");
  taskFilters.value.sort = String(q.sort || "created_at_desc");
}

async function fetchTasks(page = taskPagination.value.current, pageSize = taskPagination.value.pageSize) {
  const fetchSeq = ++taskFetchSeq;
  try {
    const params = new URLSearchParams({
      page: String(page),
      page_size: String(pageSize),
    });
    if (taskFilters.value.status) params.set("status", taskFilters.value.status);
    if (taskFilters.value.keyword) params.set("keyword", taskFilters.value.keyword);
    if (taskFilters.value.sort) params.set("sort", taskFilters.value.sort);
    const res = await fetch(\`\${API_BASE}/tasks?\${params.toString()}\`);
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

const { pauseTask, confirmEndTask, confirmStartRowCountComparison, cancelSchedule, deleteTask } =
  useTaskActions({ onChanged: () => fetchTasks() });

function openStartTaskModal(taskId, mode = "immediate") {
  startModalRef.value?.openStartTaskModal(taskId, mode);
}
function onStartSuccess() {
  fetchTasks();
}

function resetTaskFilters() {
  taskFilters.value = { status: "", keyword: "", sort: taskSortDefault.value };
  fetchTasks(1, taskPagination.value.pageSize);
}
function clearAllTaskFilters() {
  resetTaskFilters();
}
function getSortLabel(sortKey) {
  return taskSortLabelMap.value[sortKey] || sortKey;
}
const activeTaskFilterChips = computed(() => {
  const chips = [];
  if (taskFilters.value.status) chips.push({ key: "status", label: \`状态：\${getStatusText(taskFilters.value.status)}\` });
  if (taskFilters.value.keyword) chips.push({ key: "keyword", label: \`关键词：\${taskFilters.value.keyword}\` });
  if (taskFilters.value.sort && taskFilters.value.sort !== taskSortDefault.value) {
    chips.push({ key: "sort", label: \`排序：\${getSortLabel(taskFilters.value.sort)}\` });
  }
  return chips;
});
const hasActiveTaskFilters = computed(() => activeTaskFilterChips.value.length > 0);

function openEditDialog(task) {
  router.push(\`/tasks/\${task.config.id}/edit\`);
}
function openDuplicateFromTask(task) {
  router.push({ path: "/tasks/new", query: { clone_from: task.config.id } });
}
function openCreateDialog() {
  router.push("/tasks/new/select");
}

watch(
  () => ({ ...taskFilters.value }),
  () => {
    fetchTasks(1, taskPagination.value.pageSize);
  },
);

onMounted(async () => {
  await ensureDefaultConfig();
  await loadTaskSortOptions();
  loadTaskFiltersFromUrl();
  if (!taskFilters.value.sort || !taskSortOptions.value.some((o) => o.value === taskFilters.value.sort)) {
    taskFilters.value.sort = taskSortDefault.value;
  }
  await fetchTasks(taskPagination.value.current, taskPagination.value.pageSize);
  refreshInterval = setInterval(() => {
    fetchTasks(taskPagination.value.current, taskPagination.value.pageSize);
  }, 3000);
});

onUnmounted(() => {
  if (refreshInterval) clearInterval(refreshInterval);
  if (syncUrlDebounceState.timer) clearTimeout(syncUrlDebounceState.timer);
});
</script>

<template>
${tpl}
  <StartTaskModal ref="startModalRef" @success="onStartSuccess" />
</template>

<style scoped>
${read(path.join(EXT, "style-list.css"))}
</style>
`,
  );
}

console.log("config + list generated");
