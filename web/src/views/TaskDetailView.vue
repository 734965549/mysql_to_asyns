<script setup>
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
  formatScheduledTime,
  resumeTableList,
  formatNullableRows,
  formatNullableDiff,
  rowCountComparisonRowKey,
  rowComparisonStatusText,
  rowComparisonStatusColor,
  getTotalRowDescriptionLabel,
  formatTotalRowDisplay,
  shouldShowFullSyncFailedReason,
} from "../utils/taskFormatters.js";
import { useTaskEvents } from "../composables/useTaskEvents.js";

const route = useRoute();
const { configForm, ensureDefaultConfig } = useDefaultConfig();
const startModalRef = ref(null);

const detailPageTaskId = computed(() => String(route.params.id || ""));
const detailPageTask = ref(null);
const detailPageMetrics = ref({});
const detailPageProgress = ref(null);
const detailPageLoading = ref(false);
const detailPageActiveTab = ref("runtime");
// 表列表默认折叠：表数量大时一次性渲染会造成卡顿，用户按需展开。
const isTablesCollapsed = ref(true);

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
const detailPageTaskStatus = computed(() => detailPageTask.value?.context?.status || "");
const detailPageShowErrorStack = computed(() => {
  const ctx = detailPageTask.value?.context;
  return ctx?.status === "FAILED" && !!ctx?.error_stack;
});
const detailPageShowFullSyncFailedReason = computed(() =>
  shouldShowFullSyncFailedReason(detailPageTask.value?.context),
);

const {
  events: detailPageEvents,
  loading: detailPageEventsLoading,
  warnCount: detailPageEventWarnCount,
  errorCount: detailPageEventErrorCount,
  latestProgressEvent: detailPageLatestProgressEvent,
  currentExecutionId: detailPageCurrentExecutionId,
  eventFilter: detailPageEventFilter,
  sourceTableFilter: detailPageSourceTableFilter,
  eventFilterPresets: detailPageEventFilterPresets,
} = useTaskEvents(detailPageTaskId, detailPageActiveTab, detailPageTaskStatus);

const detailPageLogsTabTitle = computed(() => {
  let title = "日志与错误";
  if (detailPageEventWarnCount.value > 0 || detailPageEventErrorCount.value > 0) {
    title += ` ⚠${detailPageEventWarnCount.value} ✕${detailPageEventErrorCount.value}`;
  }
  return title;
});

function eventSeverityColor(severity) {
  switch (severity) {
    case "ERROR":
      return "red";
    case "WARN":
      return "orange";
    default:
      return "arcoblue";
  }
}

// 拉取任务详情。silent=true 时跳过 loading 状态切换，用于定时轮询避免全屏 loading 闪烁。
async function fetchTaskDetailPage(taskId, silent = false) {
  if (!taskId) return;
  if (!silent) {
    detailPageLoading.value = true;
  }
  try {
    const [taskRes, metricsRes] = await Promise.allSettled([
      fetch(`${API_BASE}/tasks/${taskId}`),
      fetch(`${API_BASE}/tasks/${taskId}/metrics`),
    ]);
    if (taskRes.status === "fulfilled") {
      if (taskRes.value.ok) detailPageTask.value = await taskRes.value.json();
      else if (taskRes.value.status === 404) detailPageTask.value = null;
    }
    if (metricsRes.status === "fulfilled" && metricsRes.value.ok) {
      detailPageMetrics.value = await metricsRes.value.json();
    }
    // Only auto-fetch progress in this call if it's the active tab, to avoid redundant calls
    if (detailPageActiveTab.value === "runtime") {
      await fetchTaskDetailProgress(taskId);
    }
  } catch (e) {
    console.error("加载任务详情失败:", e);
  } finally {
    if (!silent) {
      detailPageLoading.value = false;
    }
  }
}

async function fetchTaskDetailProgress(taskId) {
  if (!taskId) return;
  try {
    const res = await fetch(`${API_BASE}/tasks/${taskId}/progress`);
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
  
  // Set up a single interval loop to handle polling intelligently
  detailPageRefreshInterval = setInterval(() => {
    const status = detailPageTask.value?.context?.status;
    const isTerminal = ["COMPLETED", "FAILED", "STOPPED", "PAUSED"].includes(status);
    
    // If the task is in terminal status, we don't need to poll frequently
    if (isTerminal) {
      // Just poll occasionally (e.g. 15s) to check if status changes (like restart)
      // or we can skip it. For now, let's skip polling in terminal states to save resources
      return;
    }
    
    // Otherwise, perform silent updates
    fetchTaskDetailPage(detailPageTaskId.value, true);
  }, 3000);

  detailPageProgressInterval = setInterval(() => {
    const status = detailPageTask.value?.context?.status;
    // Only poll progress if the task is RUNNING and the active tab is "runtime"
    if (status === "RUNNING" && detailPageActiveTab.value === "runtime") {
      fetchTaskDetailProgress(detailPageTaskId.value);
    }
  }, 2000);
});

onUnmounted(() => {
  if (detailPageRefreshInterval) clearInterval(detailPageRefreshInterval);
  if (detailPageProgressInterval) clearInterval(detailPageProgressInterval);
});
</script>

<template>
  <a-layout class="task-detail-page-layout">
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
          <a-button
            v-if="canEndTask(detailPageTask)"
            status="danger"
            @click="confirmEndTask(detailPageTaskId)"
          >
            <template #icon><icon-stop /></template>
            结束
          </a-button>
          <a-button
            v-if="canCompareRows(detailPageTask)"
            :disabled="isComparingRows(detailPageTask)"
            @click="confirmStartRowCountComparison(detailPageTaskId)"
          >
            <template #icon><icon-sync /></template>
            {{ isComparingRows(detailPageTask) ? "对比中" : "对比行数" }}
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
                <div class="overview-label">{{ getRowOverviewLabel(detailPageTask) }}</div>
                <div class="overview-value">{{ formatRowCount(getRowCountMeta(detailPageTask).processed) }}</div>
                <div v-if="getRowOverviewSubText(detailPageTask)" class="overview-sub">
                  {{ getRowOverviewSubText(detailPageTask) }}
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

          <!-- lazy-load: 延迟渲染非活动 tab，减少首屏渲染负担 -->
          <a-tabs v-model:active-key="detailPageActiveTab" class="detail-tabs" lazy-load>
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
                    { title: '已同步 / 估算总行数', slotName: 'rows', width: 220 },
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
                    <span class="runtime-rows">{{ formatRuntimeTableRows(record) }}</span>
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
                <a-descriptions-item label="已同步行数">
                  {{ formatRowCount(getRowCountMeta(detailPageTask).processed) }}
                </a-descriptions-item>
                <a-descriptions-item
                  v-if="!getRowCountMeta(detailPageTask).isCompleted && (getRowCountMeta(detailPageTask).hasExactTotal || getRowCountMeta(detailPageTask).hasEstimatedTotal)"
                  :label="getTotalRowDescriptionLabel(detailPageTask)"
                >
                  {{ formatTotalRowDisplay(detailPageTask) }}
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
                <a-descriptions-item label="全量引擎">
                  {{ (detailPageTask.config.full_load_engine || 'v1').toLowerCase() === 'v2' ? 'V2（任务级流水线）' : 'V1（兼容）' }}
                </a-descriptions-item>
                <a-descriptions-item
                  v-if="(detailPageTask.config.full_load_engine || 'v1').toLowerCase() === 'v2'"
                  label="V2 读/写并发"
                >
                  {{ (detailPageTask.config.full_load_read_workers > 0 ? detailPageTask.config.full_load_read_workers : 4) }}
                  /
                  {{ (detailPageTask.config.full_load_write_workers > 0 ? detailPageTask.config.full_load_write_workers : 4) }}
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
                <a-descriptions-item label="全量关闭目标binlog">
                  {{ detailPageTask.config.enable_skip_binlog ? '开启' : '关闭' }}
                </a-descriptions-item>
              </a-descriptions>

              <a-descriptions title="源数据库配置" :column="1" bordered style="margin-top: 20px" :label-style="{ width: '120px' }">
                <a-descriptions-item label="连接地址">
                  <span style="font-weight: 500;">{{ detailPageTask.config.source_db?.host || '-' }}</span>
                  <span style="margin: 0 8px; color: var(--color-neutral-4)">:</span>
                  <span style="color: var(--color-text-2)">{{ detailPageTask.config.source_db?.port || '-' }}</span>
                </a-descriptions-item>
                <a-descriptions-item label="用户名">
                  {{ detailPageTask.config.source_db?.username || '-' }}
                </a-descriptions-item>
                <a-descriptions-item label="数据库">
                  <a-space wrap class="db-tags-container">
                    <a-tag
                      v-for="db in (detailPageTask.config.source_databases || [])"
                      :key="db"
                      color="arcoblue"
                      size="small"
                      >{{ db }}</a-tag
                    >
                    <span v-if="!(detailPageTask.config.source_databases || []).length">{{
                      detailPageTask.config.source_schema || '-'
                    }}</span>
                  </a-space>
                </a-descriptions-item>
              </a-descriptions>

              <a-descriptions title="目标数据库配置" :column="1" bordered style="margin-top: 20px" :label-style="{ width: '120px' }">
                <template
                  v-if="!hasExplicitSinkConfigs(detailPageTask.config.sink_configs)"
                >
                  <a-descriptions-item label="连接地址">
                    <span style="font-weight: 500;">{{ getTaskTargetMySQLDisplay(detailPageTask.config).host || '-' }}</span>
                    <span style="margin: 0 8px; color: var(--color-neutral-4)">:</span>
                    <span style="color: var(--color-text-2)">{{ getTaskTargetMySQLDisplay(detailPageTask.config).port || '-' }}</span>
                  </a-descriptions-item>
                  <a-descriptions-item label="用户名">
                    {{ getTaskTargetMySQLDisplay(detailPageTask.config).username || '-' }}
                  </a-descriptions-item>
                  <a-descriptions-item label="数据库映射">
                    <div class="db-mappings-grid">
                      <div
                        v-for="mapping in getTaskDatabaseMappings(detailPageTask)"
                        :key="mapping.source"
                        class="db-mapping-item"
                      >
                        <a-tag color="blue" size="small">{{ mapping.source }}</a-tag>
                        <icon-arrow-right class="mapping-arrow" />
                        <a-tag color="green" size="small">{{ mapping.target }}</a-tag>
                      </div>
                    </div>
                  </a-descriptions-item>
                </template>
                <template v-else>
                  <a-descriptions-item
                    v-for="(sink, idx) in detailPageTask.config.sink_configs"
                    :key="idx"
                    :label="`目标端 ${sink.type}`"
                  >
                    <div v-if="sink.type === 'MYSQL'">
                      <span style="font-weight: 500;">{{ getMySQLSinkDisplay(sink, detailPageTask.config).host || '-' }}</span>
                      <span style="margin: 0 8px; color: var(--color-neutral-4)">:</span>
                      <span>{{ getMySQLSinkDisplay(sink, detailPageTask.config).port || '-' }}</span>
                      <span style="margin-left: 16px; color: var(--color-text-3)">用户：</span>
                      <span>{{ getMySQLSinkDisplay(sink, detailPageTask.config).username || '-' }}</span>
                    </div>
                    <div v-else-if="sink.type === 'KAFKA'">
                      Brokers：{{
                        Array.isArray(sink.options?.brokers)
                          ? sink.options.brokers.join(', ')
                          : sink.options?.brokers || '-'
                      }}
                      Topic：{{ sink.options?.topic || '-' }}
                      Routing：{{ sink.options?.routing_mode || '-' }}
                      <span v-if="sink.options?.security?.sasl_mechanism"
                        >SASL：{{ sink.options.security.sasl_mechanism }}</span
                      >
                      <span v-if="sink.options?.security?.tls_enabled"
                        >TLS：已启用</span
                      >
                    </div>
                    <div v-else-if="sink.type === 'HTTP_WEBHOOK'">
                      URL：{{ sink.options?.url || '-' }} Method：{{ sink.options?.method || '-' }}
                      Retry：{{ sink.options?.retry_times || 0 }}次
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
                  <div class="sync-tables-wrapper">
                    <div class="sync-tables-header" style="margin-bottom: 8px;">
                      <a-button type="outline" size="mini" @click="isTablesCollapsed = !isTablesCollapsed">
                        <template #icon>
                          <icon-down v-if="isTablesCollapsed" />
                          <icon-up v-else />
                        </template>
                        {{ isTablesCollapsed ? `展开全部 (${(detailPageTask.config.tables || []).length}张表)` : '收起' }}
                      </a-button>
                    </div>
                    <div v-if="!isTablesCollapsed" class="sync-tables-content">
                      <a-space wrap>
                        <a-tag v-for="table in (detailPageTask.config.tables || [])" :key="table">{{
                          table
                        }}</a-tag>
                        <span
                          v-if="!detailPageTask.config.tables || detailPageTask.config.tables.length === 0"
                          >全库同步</span
                        >
                      </a-space>
                    </div>
                  </div>
                </a-descriptions-item>
              </a-descriptions>
            </a-tab-pane>

            <!-- 日志与错误 -->
            <a-tab-pane key="logs" :title="detailPageLogsTabTitle">
              <a-spin :loading="detailPageEventsLoading" style="width: 100%">
                <a-descriptions
                  v-if="detailPageCurrentExecutionId || detailPageLatestProgressEvent"
                  title="事件摘要"
                  :column="2"
                  bordered
                  style="margin-bottom: 16px"
                >
                  <a-descriptions-item label="当前轮次">
                    {{ detailPageCurrentExecutionId || '-' }}
                  </a-descriptions-item>
                  <a-descriptions-item label="WARN / ERROR">
                    {{ detailPageEventWarnCount }} / {{ detailPageEventErrorCount }}
                  </a-descriptions-item>
                  <a-descriptions-item label="最后阶段进展" :span="2">
                    {{ detailPageLatestProgressEvent?.message || '-' }}
                  </a-descriptions-item>
                </a-descriptions>

                <a-alert
                  v-if="detailPageShowErrorStack"
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
                  v-if="detailPageShowFullSyncFailedReason"
                  type="warning"
                  :show-icon="true"
                  style="margin-bottom: 16px"
                  title="全量同步失败原因"
                >
                  {{ detailPageTask.context.full_sync_failed_reason }}
                </a-alert>

                <a-space wrap style="margin-bottom: 12px">
                  <a-select
                    v-model="detailPageEventFilter"
                    :options="detailPageEventFilterPresets.map((p) => ({ value: p.id, label: p.label }))"
                    style="width: 180px"
                    placeholder="事件分类"
                  />
                  <a-input
                    v-model="detailPageSourceTableFilter"
                    allow-clear
                    placeholder="按表名筛选"
                    style="width: 200px"
                  />
                </a-space>

                <a-table
                  v-if="detailPageEvents.length > 0"
                  :columns="[
                    { title: '时间', slotName: 'time', width: 180 },
                    { title: '级别', slotName: 'severity', width: 80 },
                    { title: '事件', slotName: 'code', width: 220 },
                    { title: '说明', slotName: 'message' },
                  ]"
                  :data="detailPageEvents"
                  :pagination="{ pageSize: 20 }"
                  size="small"
                  row-key="seq"
                >
                  <template #time="{ record }">
                    {{ formatTime(record.timestamp) }}
                    <div
                      v-if="record.repeat_count > 1"
                      class="event-repeat-meta"
                    >
                      ×{{ record.repeat_count }}
                      <span v-if="record.first_at && record.last_at">
                        （{{ formatTime(record.first_at) }} ~ {{ formatTime(record.last_at) }}）
                      </span>
                    </div>
                  </template>
                  <template #severity="{ record }">
                    <a-tag :color="eventSeverityColor(record.severity)" size="small">
                      {{ record.severity }}
                    </a-tag>
                  </template>
                  <template #code="{ record }">
                    <div>{{ record.code }}</div>
                    <div v-if="record.source_schema" class="event-table-ref">
                      {{ record.source_schema }}.{{ record.source_table }}
                    </div>
                  </template>
                  <template #message="{ record }">
                    {{ record.message }}
                  </template>
                </a-table>

                <a-empty
                  v-else-if="!detailPageShowErrorStack && !detailPageShowFullSyncFailedReason"
                  description="暂无关键事件"
                  style="margin-top: 24px"
                />
              </a-spin>
            </a-tab-pane>

            <!-- 行数对比 -->
            <a-tab-pane key="row-compare" title="行数对比">
              <template v-if="!detailPageTask.context.row_count_comparison">
                <a-empty
                  description="暂无行数对比结果。仅 COMPLETED / STOPPED 且全量已完成的任务可发起对比。"
                  style="margin-top: 24px"
                />
                <div style="text-align: center; margin-top: 16px">
                  <a-button
                    v-if="canCompareRows(detailPageTask)"
                    type="primary"
                    :disabled="isComparingRows(detailPageTask)"
                    @click="confirmStartRowCountComparison(detailPageTaskId)"
                  >
                    <template #icon><icon-sync /></template>
                    {{ isComparingRows(detailPageTask) ? "对比中" : "开始对比行数" }}
                  </a-button>
                </div>
              </template>
              <template v-else>
                <a-alert
                  type="info"
                  :show-icon="true"
                  style="margin-bottom: 16px"
                  title="行数对比说明"
                >
                  结果为点击核对期间获得的行数快照，源端在任务结束后仍可能继续写入，不构成事务一致性证明。difference 定义为目标端行数减源端行数。
                </a-alert>

                <!-- 汇总卡片 -->
                <a-row :gutter="16" class="runtime-overview-row">
                  <a-col :xs="12" :md="6">
                    <a-card class="overview-card">
                      <div class="overview-label">核对状态</div>
                      <div class="overview-value overview-value-sm">
                        <a-tag :color="rowComparisonStatusColor(detailPageTask.context.row_count_comparison.status)" size="small">
                          {{ rowComparisonStatusText(detailPageTask.context.row_count_comparison.status) }}
                        </a-tag>
                      </div>
                      <div class="overview-sub">
                        开始：{{ formatTime(detailPageTask.context.row_count_comparison.started_at) }}
                      </div>
                      <div class="overview-sub">
                        完成：{{ formatTime(detailPageTask.context.row_count_comparison.completed_at) }}
                      </div>
                    </a-card>
                  </a-col>
                  <a-col :xs="12" :md="6">
                    <a-card class="overview-card">
                      <div class="overview-label">源端总行数</div>
                      <div class="overview-value">{{ formatRowCount(detailPageTask.context.row_count_comparison.source_total) }}</div>
                      <div class="overview-sub">
                        目标端：{{ formatRowCount(detailPageTask.context.row_count_comparison.target_total) }}
                      </div>
                    </a-card>
                  </a-col>
                  <a-col :xs="12" :md="6">
                    <a-card class="overview-card">
                      <div class="overview-label">总差值</div>
                      <div
                        class="overview-value"
                        :style="{ color: detailPageTask.context.row_count_comparison.difference === 0 ? 'var(--color-success-6)' : 'var(--color-warning-6)' }"
                      >
                        {{ formatNullableDiff(detailPageTask.context.row_count_comparison.difference) }}
                      </div>
                      <div class="overview-sub">目标端 - 源端</div>
                    </a-card>
                  </a-col>
                  <a-col :xs="12" :md="6">
                    <a-card class="overview-card">
                      <div class="overview-label">已核对 / 总表数</div>
                      <div class="overview-value">
                        {{ detailPageTask.context.row_count_comparison.checked_tables }} /
                        {{ detailPageTask.context.row_count_comparison.total_tables }}
                      </div>
                      <div class="overview-sub">
                        一致 {{ detailPageTask.context.row_count_comparison.matched_tables }} · 不一致
                        {{ detailPageTask.context.row_count_comparison.mismatched_tables }} · 失败
                        {{ detailPageTask.context.row_count_comparison.failed_tables }}
                      </div>
                    </a-card>
                  </a-col>
                </a-row>

                <a-alert
                  v-if="detailPageTask.context.row_count_comparison.failure_reason"
                  type="warning"
                  :show-icon="true"
                  style="margin-bottom: 16px"
                  title="核对失败原因"
                >
                  {{ detailPageTask.context.row_count_comparison.failure_reason }}
                </a-alert>

                <!-- 逐表结果 -->
                <div class="runtime-section-title">
                  <icon-storage />
                  <span>逐表行数对比</span>
                  <a-tag color="arcoblue" size="small" style="margin-left: 8px">
                    共 {{ (detailPageTask.context.row_count_comparison.tables || []).length }} 张表
                  </a-tag>
                </div>
                <a-table
                  :columns="[
                    { title: '源表', slotName: 'sourceTable', width: 240 },
                    { title: '目标表', slotName: 'targetTable', width: 240 },
                    { title: '源端行数', slotName: 'sourceRows', width: 140 },
                    { title: '目标端行数', slotName: 'targetRows', width: 140 },
                    { title: '差值', slotName: 'diff', width: 120 },
                    { title: '状态', slotName: 'rowStatus', width: 110 },
                    { title: '错误信息', slotName: 'rowError' },
                  ]"
                  :data="detailPageTask.context.row_count_comparison.tables || []"
                  :pagination="false"
                  size="medium"
                  :scroll="{ y: 480 }"
                  :row-key="rowCountComparisonRowKey"
                >
                  <template #sourceTable="{ record }">
                    <span>{{ record.source_schema }}.{{ record.source_table }}</span>
                  </template>
                  <template #targetTable="{ record }">
                    <span>{{ record.target_schema }}.{{ record.target_table }}</span>
                  </template>
                  <template #sourceRows="{ record }">
                    {{ formatNullableRows(record.source_rows) }}
                  </template>
                  <template #targetRows="{ record }">
                    {{ formatNullableRows(record.target_rows) }}
                  </template>
                  <template #diff="{ record }">
                    <span
                      :style="{ color: record.difference == null ? 'var(--color-text-3)' : (record.difference === 0 ? 'var(--color-success-6)' : 'var(--color-warning-6)') }"
                    >
                      {{ formatNullableDiff(record.difference) }}
                    </span>
                  </template>
                  <template #rowStatus="{ record }">
                    <a-tag
                      v-if="record.source_rows != null && record.target_rows != null"
                      :color="record.matched ? 'green' : 'orange'"
                      size="small"
                    >
                      {{ record.matched ? '一致' : '不一致' }}
                    </a-tag>
                    <a-tag v-else color="red" size="small">失败</a-tag>
                  </template>
                  <template #rowError="{ record }">
                    <a-typography-text v-if="record.error" type="secondary" style="font-size: 12px">
                      {{ record.error }}
                    </a-typography-text>
                    <span v-else>-</span>
                  </template>
                </a-table>

                <div style="margin-top: 16px; text-align: right">
                  <a-button
                    v-if="canCompareRows(detailPageTask)"
                    :disabled="isComparingRows(detailPageTask)"
                    @click="confirmStartRowCountComparison(detailPageTaskId)"
                  >
                    <template #icon><icon-refresh /></template>
                    {{ isComparingRows(detailPageTask) ? "对比中" : "重新对比" }}
                  </a-button>
                </div>
              </template>
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
  <StartTaskModal ref="startModalRef" @success="onStartSuccess" />
</template>

<style scoped>
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

.db-tags-container {
  max-width: 100%;
}

.db-mappings-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 12px;
  width: 100%;
  margin-top: 4px;
}

.db-mapping-item {
  display: flex;
  align-items: center;
  background: var(--color-fill-1);
  padding: 6px 12px;
  border-radius: 4px;
  border: 1px solid var(--color-neutral-3);
  transition: all 0.2s ease;
}

.db-mapping-item:hover {
  background: var(--color-fill-2);
  border-color: var(--color-neutral-4);
}

.mapping-arrow {
  margin: 0 8px;
  color: var(--color-text-3);
  font-size: 14px;
  flex-shrink: 0;
}

.sync-tables-wrapper {
  width: 100%;
}

.sync-tables-content {
  background: var(--color-fill-1);
  padding: 12px;
  border-radius: 4px;
  border: 1px solid var(--color-neutral-2);
  max-height: 250px;
  overflow-y: auto;
}

.event-repeat-meta {
  font-size: 12px;
  color: var(--color-text-3);
}

.event-table-ref {
  font-size: 12px;
  color: var(--color-text-3);
}

</style>
