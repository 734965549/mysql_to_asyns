<script setup>
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
  if (taskFilters.value.status) chips.push({ key: "status", label: `状态：${getStatusText(taskFilters.value.status)}` });
  if (taskFilters.value.keyword) chips.push({ key: "keyword", label: `关键词：${taskFilters.value.keyword}` });
  if (taskFilters.value.sort && taskFilters.value.sort !== taskSortDefault.value) {
    chips.push({ key: "sort", label: `排序：${getSortLabel(taskFilters.value.sort)}` });
  }
  return chips;
});
const hasActiveTaskFilters = computed(() => activeTaskFilterChips.value.length > 0);

function openEditDialog(task) {
  router.push(`/tasks/${task.config.id}/edit`);
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
        <div>
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
                    <a-option value="STOPPED">已结束</a-option>
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
                            已同步: {{ formatRowCount(getRowCountMeta(task).processed) }} /
                            {{ formatTotalRowDisplay(task) }}
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
                        v-if="canEndTask(task)"
                        size="small"
                        status="danger"
                        @click="confirmEndTask(task.config.id)"
                      >
                        <template #icon><icon-stop /></template>
                        结束
                      </a-button>

                      <a-button
                        v-if="canCompareRows(task)"
                        size="small"
                        :disabled="isComparingRows(task)"
                        @click="confirmStartRowCountComparison(task.config.id)"
                      >
                        <template #icon><icon-sync /></template>
                        {{ isComparingRows(task) ? "对比中" : "对比行数" }}
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
  <StartTaskModal ref="startModalRef" @success="onStartSuccess" />
</template>

<style scoped>
/* TaskListView 组件特定的样式，主题相关的样式在 themes.css 中 */
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
.task-card-inner {
  border-radius: 14px;
  border: 1px solid #edf2f7;
  width: 100%;
  box-shadow: 0 6px 18px rgba(15, 23, 42, 0.04);
}
.task-card-inner :deep(.arco-card-body) {
  padding: 20px;
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
.task-filter-panel__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
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
.task-pagination {
  margin-top: 20px;
}
.progress-details {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 8px;
}
.progress-text {
  color: var(--app-muted, #86909c);
  font-size: 13px;
}
.progress-percent-text {
  color: var(--app-accent, #165dff);
  font-weight: 600;
}

/* 响应式样式 */
@media (max-width: 920px) {
  .task-filter-summary {
    grid-template-columns: 1fr;
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
}

@media (max-width: 768px) {
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
</style>
