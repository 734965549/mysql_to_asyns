import { ref, computed, onUnmounted, watch } from "vue";
import { API_BASE } from "./useApi.js";

const TERMINAL_STATUSES = new Set(["COMPLETED", "FAILED", "STOPPED"]);

export const EVENT_FILTER_PRESETS = [
  { id: "all", label: "关键事件", minSeverity: "INFO" },
  { id: "warn", label: "WARN 及以上", minSeverity: "WARN" },
  { id: "error", label: "仅 ERROR", minSeverity: "ERROR" },
  { id: "schedule", label: "调度与并发", minSeverity: "INFO", categories: ["TABLE"] },
  { id: "pool", label: "连接池与背压", minSeverity: "INFO", categories: ["POOL", "QUEUE"] },
  { id: "retry", label: "重试与恢复", minSeverity: "INFO", categories: ["RETRY"] },
  { id: "phase", label: "阶段切换", minSeverity: "INFO", categories: ["PHASE"] },
];

export function useTaskEvents(taskIdRef, activeTabRef, taskStatusRef) {
  const events = ref([]);
  const executions = ref([]);
  const currentExecutionId = ref("");
  const loading = ref(false);
  const lastSeq = ref(0);
  const eventFilter = ref("all");
  const sourceTableFilter = ref("");
  let pollTimer = null;
  let tailTimer = null;

  const activePreset = computed(
    () => EVENT_FILTER_PRESETS.find((p) => p.id === eventFilter.value) || EVENT_FILTER_PRESETS[0]
  );

  const filteredEvents = computed(() => {
    const preset = activePreset.value;
    const minRank = severityRank(preset.minSeverity || "INFO");
    const tableNeedle = (sourceTableFilter.value || "").trim().toLowerCase();
    return events.value.filter((e) => {
      if (severityRank(e.severity) < minRank) return false;
      if (preset.categories && preset.categories.length > 0) {
        if (!preset.categories.includes(e.category)) return false;
      }
      if (tableNeedle) {
        const ref = `${e.source_schema || ""}.${e.source_table || ""}`.toLowerCase();
        if (!ref.includes(tableNeedle) && !(e.source_table || "").toLowerCase().includes(tableNeedle)) {
          return false;
        }
      }
      return true;
    });
  });

  const warnCount = computed(
    () => filteredEvents.value.filter((e) => e.severity === "WARN").length
  );
  const errorCount = computed(
    () => filteredEvents.value.filter((e) => e.severity === "ERROR").length
  );

  const latestProgressEvent = computed(() => {
    const phaseEvents = filteredEvents.value.filter((e) => e.category === "PHASE");
    return phaseEvents[0] || null;
  });

  function severityRank(s) {
    switch (s) {
      case "ERROR":
        return 2;
      case "WARN":
        return 1;
      default:
        return 0;
    }
  }

  function buildEventsUrl(afterSeq = 0) {
    const preset = activePreset.value;
    const params = new URLSearchParams({
      visibility: "KEY",
      min_severity: preset.minSeverity || "INFO",
      limit: "200",
    });
    if (currentExecutionId.value) {
      params.set("execution_id", currentExecutionId.value);
    }
    if (afterSeq > 0) {
      params.set("after_seq", String(afterSeq));
    }
    if (sourceTableFilter.value.trim()) {
      params.set("source_table", sourceTableFilter.value.trim());
    }
    return `${API_BASE}/tasks/${taskIdRef.value}/events?${params.toString()}`;
  }

  async function fetchExecutions() {
    const id = taskIdRef.value;
    if (!id) return;
    const res = await fetch(`${API_BASE}/tasks/${id}/event-executions`);
    if (!res.ok) return;
    const data = await res.json();
    executions.value = data.executions || [];
    if (!currentExecutionId.value && executions.value.length > 0) {
      currentExecutionId.value = executions.value[0].execution_id;
    }
  }

  async function fetchEvents(incremental = false) {
    const id = taskIdRef.value;
    if (!id) return;
    if (!incremental) loading.value = true;
    try {
      const res = await fetch(buildEventsUrl(incremental ? lastSeq.value : 0));
      if (!res.ok) return;
      const data = await res.json();
      const incoming = data.events || [];
      if (incremental && incoming.length > 0) {
        const merged = [...incoming.reverse(), ...events.value];
        const seen = new Set();
        events.value = merged.filter((e) => {
          if (seen.has(e.seq)) return false;
          seen.add(e.seq);
          return true;
        });
        events.value.sort((a, b) => b.seq - a.seq);
      } else if (!incremental) {
        events.value = incoming;
      }
      if (events.value.length > 0) {
        lastSeq.value = Math.max(...events.value.map((e) => e.seq || 0));
      }
    } finally {
      if (!incremental) loading.value = false;
    }
  }

  async function refreshAll() {
    await fetchExecutions();
    await fetchEvents(false);
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
    if (tailTimer) {
      clearTimeout(tailTimer);
      tailTimer = null;
    }
  }

  function startPolling() {
    stopPolling();
    if (activeTabRef.value !== "logs") return;
    const status = taskStatusRef.value;
    if (status === "RUNNING") {
      pollTimer = setInterval(() => fetchEvents(true), 3000);
    } else if (TERMINAL_STATUSES.has(status)) {
      tailTimer = setTimeout(() => fetchEvents(true), 30000);
    }
  }

  watch(
    [taskIdRef, () => activeTabRef.value, () => taskStatusRef.value, eventFilter, sourceTableFilter],
    async ([id, tab]) => {
      stopPolling();
      if (!id || tab !== "logs") return;
      await refreshAll();
      startPolling();
    },
    { immediate: true }
  );

  onUnmounted(stopPolling);

  return {
    events: filteredEvents,
    rawEvents: events,
    executions,
    currentExecutionId,
    loading,
    warnCount,
    errorCount,
    latestProgressEvent,
    eventFilter,
    sourceTableFilter,
    eventFilterPresets: EVENT_FILTER_PRESETS,
    refreshAll,
    stopPolling,
    startPolling,
  };
}
