#!/usr/bin/env node
/**
 * Generate TaskDetailView / ConfigView / TaskListView / TaskFormView / thin App.vue
 * from current App.vue + _extract templates. Run: node web/scripts/gen-pages.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEB = path.resolve(__dirname, "..");
const SRC = path.join(WEB, "src");
const EXT = path.join(SRC, "_extract");

const read = (p) => fs.readFileSync(p, "utf8");
const write = (rel, content) => {
  const p = path.join(WEB, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, content.replace(/\r\n/g, "\n"), "utf8");
  console.log("OK", rel, content.split("\n").length);
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
    .replace(/\s*:class="appThemeClass"/g, "")
    .replace(/taskFormPage === 'edit'/g, "isEditMode")
    .replace(/taskFormPage === \"edit\"/g, "isEditMode")
    .replace(
      /@click="taskFormPage = 'select_type'"/g,
      `@click="goSelectType"`,
    );
}

const style = (name) => read(path.join(EXT, `style-${name}.css`));

// -------------------- TaskDetailView --------------------
{
  const tpl = cleanTpl(read(path.join(EXT, "tpl-detail.html")));
  write(
    "src/views/TaskDetailView.vue",
    `<script setup>
import { ref, computed, onMounted, onUnmounted } from "vue";
import { useRoute } from "vue-router";
import { API_BASE } from "../composables/useApi.js";
import { useDefaultConfig } from "../composables/useDefaultConfig.js";
import { useTaskActions } from "../composables/useTaskActions.js";
import StartTaskModal from "../components/StartTaskModal.vue";
import { getTaskDatabaseMappings } from "../utils/databaseMappings.js";
import {
  hasExplicitSinkConfigs,
  resolveMySQLSinkConnectionDisplay,
  resolveTaskTargetMySQLDisplay,
} from "../utils/taskTargetDisplay.js";
import {
  getStatusColor,
  getStatusText,
  syncPhaseText,
  getProgress,
  getProgressRatio,
  getRowCountMeta,
  getRowOverviewLabel,
  getRowOverviewSubText,
  formatRowCount,
  formatTime,
  calculateDuration,
  formatSpeed,
  formatSeconds,
  formatRuntimeTableRows,
  runtimeStatusColor,
  runtimeStatusText,
  canEndTask,
  canCompareRows,
  isComparingRows,
  confirmEndTask as _unused,
  formatScheduledTime,
  resumeTableList,
  formatNullableRows,
  formatNullableDiff,
  rowCountComparisonRowKey,
  rowComparisonStatusText,
  rowComparisonStatusColor,
  getTotalRowDescriptionLabel,
  formatTotalRowDisplay,
} from "../utils/taskFormatters.js";

const route = useRoute();
const { configForm, ensureDefaultConfig } = useDefaultConfig();
const startModalRef = ref(null);

const detailPageTaskId = computed(() => String(route.params.id || ""));
const detailPageTask = ref(null);
const detailPageMetrics = ref({});
const detailPageProgress = ref(null);
const detailPageLoading = ref(false);
const detailPageActiveTab = ref("runtime");

let detailPageRefreshInterval = null;
let detailPageProgressInterval = null;

function getTaskTargetMySQLDisplay(config) {
  return resolveTaskTargetMySQLDisplay(config, configForm.value.target);
}
function getMySQLSinkDisplay(sink, config) {
  return resolveMySQLSinkConnectionDisplay(sink, config, configForm.value.target);
}

const detailPageResumeTables = computed(() => resumeTableList(detailPageTask.value));
const detailPageRowMeta = computed(() => getRowCountMeta(detailPageTask.value));

async function fetchTaskDetailPage(taskId) {
  if (!taskId) return;
  detailPageLoading.value = true;
  try {
    const [taskRes, metricsRes] = await Promise.allSettled([
      fetch(\`\${API_BASE}/tasks/\${taskId}\`),
      fetch(\`\${API_BASE}/tasks/\${taskId}/metrics\`),
    ]);
    if (taskRes.status === "fulfilled") {
      if (taskRes.value.ok) detailPageTask.value = await taskRes.value.json();
      else if (taskRes.value.status === 404) detailPageTask.value = null;
    }
    if (metricsRes.status === "fulfilled" && metricsRes.value.ok) {
      detailPageMetrics.value = await metricsRes.value.json();
    }
    await fetchTaskDetailProgress(taskId);
  } catch (e) {
    console.error("加载任务详情失败:", e);
  } finally {
    detailPageLoading.value = false;
  }
}

async function fetchTaskDetailProgress(taskId) {
  if (!taskId) return;
  try {
    const res = await fetch(\`\${API_BASE}/tasks/\${taskId}/progress\`);
    if (res.ok) detailPageProgress.value = await res.json();
    else if (res.status === 404) detailPageProgress.value = null;
  } catch (e) {
    /* ignore */
  }
}

function refreshDetailPage() {
  if (detailPageTaskId.value) fetchTaskDetailPage(detailPageTaskId.value);
}

function closeTaskDetailPage() {
  window.close();
  const url = new URL(window.location.href);
  url.hash = "";
  url.search = "";
  window.location.href = url.toString();
}

const {
  pauseTask,
  endTask,
  confirmEndTask,
  startRowCountComparison,
  confirmStartRowCountComparison,
  cancelSchedule,
} = useTaskActions({ onChanged: refreshDetailPage });

async function detailPagePause() {
  await pauseTask(detailPageTaskId.value);
  refreshDetailPage();
}
async function detailPageCancelSchedule() {
  await cancelSchedule(detailPageTaskId.value);
  refreshDetailPage();
}

function openStartTaskModal(taskId, mode = "immediate") {
  startModalRef.value?.openStartTaskModal(taskId, mode);
}
function onStartSuccess() {
  refreshDetailPage();
}

onMounted(async () => {
  document.title = "任务详情";
  await ensureDefaultConfig();
  await fetchTaskDetailPage(detailPageTaskId.value);
  detailPageRefreshInterval = setInterval(() => {
    fetchTaskDetailPage(detailPageTaskId.value);
  }, 3000);
  detailPageProgressInterval = setInterval(() => {
    fetchTaskDetailProgress(detailPageTaskId.value);
  }, 2000);
});

onUnmounted(() => {
  if (detailPageRefreshInterval) clearInterval(detailPageRefreshInterval);
  if (detailPageProgressInterval) clearInterval(detailPageProgressInterval);
});
</script>

<template>
${tpl}
  <StartTaskModal ref="startModalRef" @success="onStartSuccess" />
</template>

<style scoped>
${style("detail")}
</style>
`,
  );
}

// Fix accidental bad import in detail - confirmEndTask is not in formatters
{
  const p = path.join(SRC, "views/TaskDetailView.vue");
  let s = read(p);
  s = s.replace(
    /canCompareRows,\n  isComparingRows,\n  confirmEndTask as _unused,\n  formatScheduledTime,/g,
    `canCompareRows,\n  isComparingRows,\n  formatScheduledTime,`,
  );
  fs.writeFileSync(p, s);
}

console.log("detail done");
