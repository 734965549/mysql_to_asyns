export function formatTime(time) {
  if (!time) return "-";
  return new Date(time).toLocaleString("zh-CN");
}

export function calculateDuration(startTime, endTime) {
  if (!startTime) return "-";

  const start = new Date(startTime);
  if (isNaN(start.getTime()) || start.getFullYear() < 2000) return "-";

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

export function formatSpeed(rowsPerSec) {
  if (rowsPerSec == null || rowsPerSec <= 0) return "0";
  if (rowsPerSec >= 10000) return `${(rowsPerSec / 1000).toFixed(1)}k`;
  if (rowsPerSec >= 1000) return `${rowsPerSec.toFixed(0)}`;
  return `${rowsPerSec.toFixed(1)}`;
}

export function formatSeconds(seconds) {
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

export function runtimeStatusColor(status) {
  const map = {
    pending: "gray",
    running: "blue",
    completed: "green",
    failed: "red",
  };
  return map[status] || "gray";
}

export function runtimeStatusText(status) {
  const map = {
    pending: "待同步",
    running: "同步中",
    completed: "已完成",
    failed: "失败",
  };
  return map[status] || status;
}

export function getStatusColor(status) {
  const colors = {
    PENDING: "gray",
    RUNNING: "blue",
    PAUSED: "orange",
    COMPLETED: "green",
    FAILED: "red",
    SCHEDULED: "arcoblue",
    STOPPED: "gray",
  };
  return colors[status] || "gray";
}

export function getStatusText(status) {
  const texts = {
    PENDING: "待执行",
    RUNNING: "执行中",
    PAUSED: "已暂停",
    COMPLETED: "已完成",
    FAILED: "失败",
    SCHEDULED: "定时中",
    STOPPED: "已结束",
  };
  return texts[status] || status;
}

export function canEndTask(task) {
  return (
    !!task?.context &&
    task.context.status === "RUNNING" &&
    task.config.mode === "ALL" &&
    task.context.sync_phase === "INCREMENTAL_STARTED"
  );
}

export function canCompareRows(task) {
  if (!task?.context) return false;
  const status = task.context.status;
  if (status !== "COMPLETED" && status !== "STOPPED") return false;
  const phase = task.context.sync_phase;
  if (phase !== "FULL_COMPLETED" && phase !== "INCREMENTAL_STARTED") return false;
  return true;
}

export function isComparingRows(task) {
  return task?.context?.row_count_comparison?.status === "CHECKING";
}

export function rowComparisonStatusText(status) {
  const texts = {
    CHECKING: "对比中",
    MATCHED: "一致",
    MISMATCHED: "不一致",
    PARTIAL: "部分失败",
    FAILED: "失败",
  };
  return texts[status] || status || "-";
}

export function rowComparisonStatusColor(status) {
  const colors = {
    CHECKING: "blue",
    MATCHED: "green",
    MISMATCHED: "orange",
    PARTIAL: "orange",
    FAILED: "red",
  };
  return colors[status] || "gray";
}

export function formatNullableRows(v) {
  if (v == null) return "-";
  return Number(v).toLocaleString("zh-CN");
}

export function formatNullableDiff(v) {
  if (v == null) return "-";
  const n = Number(v);
  const s = Math.abs(n).toLocaleString("zh-CN");
  if (n > 0) return "+" + s;
  return s;
}

export function rowCountComparisonRowKey(record) {
  return `${record.source_schema}.${record.source_table}`;
}

export function getProgress(task) {
  if (!task?.context) return 0;

  const ctx = task.context;

  if (ctx.status === "COMPLETED") {
    return 100;
  }

  const effectiveTotal = Math.max(0, ctx.total_rows || ctx.estimated_total_rows || 0);

  if (ctx.progress_percent != null && ctx.progress_percent >= 0) {
    const fromServer = Math.min(100, Math.max(0, ctx.progress_percent));
    if (effectiveTotal <= 0) {
      return Number(fromServer.toFixed(2));
    }
  }

  if (effectiveTotal <= 0) {
    return 0;
  }

  const processed = Math.max(0, ctx.processed_rows || 0);
  let percent = (processed / effectiveTotal) * 100;
  percent = Math.min(100, Math.max(0, percent));
  return Number(percent.toFixed(2));
}

export function getProgressRatio(value) {
  const pct = typeof value === "number" ? value : getProgress(value);
  return Math.min(1, Math.max(0, pct / 100));
}

export function getRowCountMeta(task) {
  const ctx = task?.context;
  if (!ctx) {
    return {
      processed: 0,
      exactTotal: 0,
      estimatedTotal: 0,
      isCompleted: false,
      hasExactTotal: false,
      hasEstimatedTotal: false,
    };
  }

  const processed = Math.max(0, ctx.processed_rows || 0);
  const exactTotal = Math.max(0, ctx.total_rows || 0);
  const estimatedTotal = Math.max(0, ctx.estimated_total_rows || 0);
  const isCompleted = ctx.status === "COMPLETED";

  return {
    processed,
    exactTotal,
    estimatedTotal,
    isCompleted,
    hasExactTotal: exactTotal > 0,
    hasEstimatedTotal: estimatedTotal > 0 && exactTotal <= 0,
  };
}

export function formatRowCount(n) {
  return Math.max(0, Number(n) || 0).toLocaleString("zh-CN");
}

export function formatTotalRowDisplay(task) {
  const meta = getRowCountMeta(task);
  if (meta.isCompleted) {
    return formatRowCount(meta.processed);
  }
  if (meta.hasExactTotal) {
    return formatRowCount(meta.exactTotal);
  }
  if (meta.hasEstimatedTotal) {
    return `约 ${formatRowCount(meta.estimatedTotal)}`;
  }
  return "0";
}

export function getTotalRowDescriptionLabel(task) {
  const meta = getRowCountMeta(task);
  if (meta.hasExactTotal) {
    return "总行数";
  }
  if (meta.hasEstimatedTotal) {
    return "估算总行数";
  }
  return "总行数";
}

export function getRowOverviewLabel(task) {
  const meta = getRowCountMeta(task);
  if (meta.isCompleted) {
    return "已同步行数";
  }
  if (meta.hasExactTotal) {
    return "已同步 / 总行数";
  }
  if (meta.hasEstimatedTotal) {
    return "已同步 / 估算总行数";
  }
  return "已同步行数";
}

export function getRowOverviewSubText(task) {
  const meta = getRowCountMeta(task);
  if (meta.isCompleted) {
    return `共同步 ${formatRowCount(meta.processed)} 行`;
  }
  if (meta.hasExactTotal) {
    return `总行数：${formatRowCount(meta.exactTotal)}`;
  }
  if (meta.hasEstimatedTotal) {
    return `估算总行数：约 ${formatRowCount(meta.estimatedTotal)}`;
  }
  return "";
}

export function formatRuntimeTableRows(record) {
  const processed = formatRowCount(record?.processed_rows || 0);
  if (record?.status === "completed") {
    return processed;
  }
  const total = Math.max(0, record?.total_rows || 0);
  if (total > 0) {
    return `${processed} / 约 ${formatRowCount(total)}`;
  }
  return processed;
}

export function syncPhaseText(phase) {
  const map = {
    "": "未开始",
    FULL_STARTED: "全量进行中",
    FULL_COMPLETED: "全量已完成",
    FULL_FAILED: "全量失败",
    INCREMENTAL_STARTED: "增量进行中",
  };
  return map[phase] ?? (phase || "未开始");
}

export function formatScheduledTime(task) {
  if (task?.context?.scheduled_at) {
    const d = new Date(task.context.scheduled_at);
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
  return "";
}

export function resumeTableList(task) {
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

export function showTaskDetail(task) {
  const url = new URL(window.location.href);
  url.search = "";
  url.hash = `#/task-detail/${task.config.id}`;
  window.open(url.toString(), "_blank");
}
