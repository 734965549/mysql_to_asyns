package entity

import (
	"fmt"
	"strings"
	"time"
)

// EventSeverity 任务事件严重级别。
type EventSeverity string

const (
	EventSeverityInfo  EventSeverity = "INFO"
	EventSeverityWarn  EventSeverity = "WARN"
	EventSeverityError EventSeverity = "ERROR"
)

// EventVisibility 任务事件可见性：KEY 进入任务详情关键事件列表；DIAGNOSTIC 仅诊断用途。
type EventVisibility string

const (
	EventVisibilityKey        EventVisibility = "KEY"
	EventVisibilityDiagnostic EventVisibility = "DIAGNOSTIC"
)

// EventCategory 任务事件分类。
type EventCategory string

const (
	EventCategoryLifecycle EventCategory = "LIFECYCLE"
	EventCategoryPhase     EventCategory = "PHASE"
	EventCategoryConfig    EventCategory = "CONFIG"
	EventCategoryTable     EventCategory = "TABLE"
	EventCategoryPool      EventCategory = "POOL"
	EventCategoryQueue     EventCategory = "QUEUE"
	EventCategoryRetry     EventCategory = "RETRY"
)

// 生命周期事件码（第一批接入）。
const (
	EventCodeTaskScheduled       = "TASK_SCHEDULED"
	EventCodeTaskStarted         = "TASK_STARTED"
	EventCodeTaskResumed         = "TASK_RESUMED"
	EventCodeTaskPaused          = "TASK_PAUSED"
	EventCodeTaskStopped         = "TASK_STOPPED"
	EventCodeTaskCompleted       = "TASK_COMPLETED"
	EventCodeTaskFailed          = "TASK_FAILED"
	EventCodeTaskPersistFailed   = "TASK_PERSIST_FAILED"
	EventCodeTaskConfigEffective = "TASK_CONFIG_EFFECTIVE"
)

// 阶段事件码（第一批接入）。
const (
	EventCodePhaseDDLPrepStarted   = "PHASE_DDL_PREP_STARTED"
	EventCodePhaseDDLPrepCompleted = "PHASE_DDL_PREP_COMPLETED"
	EventCodePhaseP0Captured       = "PHASE_P0_CAPTURED"
	EventCodePhaseBaseScanStarted  = "PHASE_BASE_SCAN_STARTED"
	EventCodePhaseBaseScanCompleted = "PHASE_BASE_SCAN_COMPLETED"
	EventCodePhaseP1Captured       = "PHASE_P1_CAPTURED"
	EventCodePhaseCatchupStarted   = "PHASE_CATCHUP_STARTED"
	EventCodePhaseCatchupCompleted = "PHASE_CATCHUP_COMPLETED"
	EventCodePhaseIndexRestoreStarted   = "PHASE_INDEX_RESTORE_STARTED"
	EventCodePhaseIndexRestoreCompleted = "PHASE_INDEX_RESTORE_COMPLETED"
	EventCodePhaseIncrementalStarted    = "PHASE_INCREMENTAL_STARTED"
)

// 全量关键过程事件码（第二批接入）。
const (
	EventCodeFullLoadConfigEffective = "FULL_LOAD_CONFIG_EFFECTIVE"

	EventCodeTablePlanCreated        = "TABLE_PLAN_CREATED"
	EventCodeTableEstimateFailed     = "TABLE_ESTIMATE_FAILED"
	EventCodeTableParallelismReduced = "TABLE_PARALLELISM_REDUCED"
	EventCodeNOPKSequentialFallback  = "NOPK_SEQUENTIAL_FALLBACK"
	EventCodeTableChunkPlanFallback  = "TABLE_CHUNK_PLAN_FALLBACK"
	EventCodeTableNoProgress         = "TABLE_NO_PROGRESS"
	EventCodeTableProgressRecovered  = "TABLE_PROGRESS_RECOVERED"

	EventCodeSourcePoolBudgetCapped = "SOURCE_POOL_BUDGET_CAPPED"
	EventCodeSourcePoolWaitHigh     = "SOURCE_POOL_WAIT_HIGH"
	EventCodeTargetPoolBudgetCapped = "TARGET_POOL_BUDGET_CAPPED"

	EventCodeWriteLockRetry           = "WRITE_LOCK_RETRY"
	EventCodeTxCommitUnknown          = "TX_COMMIT_UNKNOWN"
	EventCodeTxCommitVerifiedApplied  = "TX_COMMIT_VERIFIED_APPLIED"
	EventCodeTxCommitVerifiedRolledBack = "TX_COMMIT_VERIFIED_ROLLED_BACK"
	EventCodeTxReplayStarted          = "TX_REPLAY_STARTED"
	EventCodeTxReplaySucceeded        = "TX_REPLAY_SUCCEEDED"
	EventCodeTxReplayExhausted        = "TX_REPLAY_EXHAUSTED"
	EventCodeTableReadRetry           = "TABLE_READ_RETRY"
	EventCodeTableReadRetryExhausted  = "TABLE_READ_RETRY_EXHAUSTED"
	EventCodeTableReadBatchRetry      = "TABLE_READ_BATCH_RETRY"

	EventCodeStagingTableCreated   = "STAGING_TABLE_CREATED"
	EventCodeStagingTablePublished = "STAGING_TABLE_PUBLISHED"
	EventCodeStagingTableDropped   = "STAGING_TABLE_DROPPED"

	EventCodeQueueBackpressureHigh      = "QUEUE_BACKPRESSURE_HIGH"
	EventCodeQueueBackpressureRecovered = "QUEUE_BACKPRESSURE_RECOVERED"
	EventCodeSlowSourceQuery            = "SLOW_SOURCE_QUERY"

	EventCodeWideTableTwoPhaseEnabled = "WIDE_TABLE_TWO_PHASE_ENABLED"
	EventCodeRowExceedsBatchBytes     = "ROW_EXCEEDS_BATCH_BYTES"

	EventCodeSchemaLockLost          = "SCHEMA_LOCK_LOST"
	EventCodeIndexRestoreFailed      = "INDEX_RESTORE_FAILED"
	EventCodeCheckpointPersistFailed = "CHECKPOINT_PERSIST_FAILED"
	EventCodeTableAbortedByTaskFailure = "TABLE_ABORTED_BY_TASK_FAILURE"
)

const EventCodeWideTableAdaptiveWindowEnabled = "WIDE_TABLE_ADAPTIVE_WINDOW_ENABLED"

// TaskEvent 任务关键事件，持久化供详情页追溯。
type TaskEvent struct {
	Seq          int64                  `json:"seq"`
	EventID      string                 `json:"event_id"`
	TaskID       string                 `json:"task_id"`
	ExecutionID  string                 `json:"execution_id"`
	Timestamp    time.Time              `json:"timestamp"`
	Severity     EventSeverity          `json:"severity"`
	Visibility   EventVisibility        `json:"visibility"`
	Category     EventCategory          `json:"category"`
	Code         string                 `json:"code"`
	Phase        string                 `json:"phase,omitempty"`
	SourceSchema string                 `json:"source_schema,omitempty"`
	SourceTable  string                 `json:"source_table,omitempty"`
	Message      string                 `json:"message"`
	Details      map[string]interface{} `json:"details,omitempty"`
	RepeatCount  int                    `json:"repeat_count,omitempty"`
	FirstAt      *time.Time             `json:"first_at,omitempty"`
	LastAt       *time.Time             `json:"last_at,omitempty"`
}

// TaskEventExecution 单次 execution 摘要。
type TaskEventExecution struct {
	ExecutionID string     `json:"execution_id"`
	StartedAt   time.Time  `json:"started_at"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
	EventCount  int        `json:"event_count"`
}

// ValidateEventSeverity 校验严重级别枚举。
func ValidateEventSeverity(s EventSeverity) error {
	switch s {
	case EventSeverityInfo, EventSeverityWarn, EventSeverityError:
		return nil
	default:
		return fmt.Errorf("invalid event severity: %q", s)
	}
}

// ValidateEventVisibility 校验可见性枚举。
func ValidateEventVisibility(v EventVisibility) error {
	switch v {
	case EventVisibilityKey, EventVisibilityDiagnostic:
		return nil
	default:
		return fmt.Errorf("invalid event visibility: %q", v)
	}
}

// ValidateTaskEvent 校验事件必填字段与枚举。
func ValidateTaskEvent(ev *TaskEvent) error {
	if ev == nil {
		return fmt.Errorf("task event is nil")
	}
	if ev.TaskID == "" {
		return fmt.Errorf("task event task_id is required")
	}
	if ev.ExecutionID == "" {
		return fmt.Errorf("task event execution_id is required")
	}
	if ev.EventID == "" {
		return fmt.Errorf("task event event_id is required")
	}
	if ev.Code == "" {
		return fmt.Errorf("task event code is required")
	}
	if ev.Message == "" {
		return fmt.Errorf("task event message is required")
	}
	if err := ValidateEventSeverity(ev.Severity); err != nil {
		return err
	}
	if err := ValidateEventVisibility(ev.Visibility); err != nil {
		return err
	}
	return nil
}

// IsNeverSuppressEventCode 返回 true 表示该事件码永不参与指纹聚合抑制。
func IsNeverSuppressEventCode(code string) bool {
	switch code {
	case EventCodeTaskStarted, EventCodeTaskFailed, EventCodeTaskCompleted,
		EventCodeTaskStopped, EventCodeTaskPaused, EventCodeTaskResumed,
		EventCodeTaskScheduled, EventCodeTaskPersistFailed, EventCodeTaskConfigEffective:
		return true
	}
	if strings.HasPrefix(code, "PHASE_") {
		return true
	}
	if strings.HasSuffix(code, "_EXHAUSTED") {
		return true
	}
	return false
}

// SeverityRank 用于 min_severity 过滤。
func SeverityRank(s EventSeverity) int {
	switch s {
	case EventSeverityInfo:
		return 0
	case EventSeverityWarn:
		return 1
	case EventSeverityError:
		return 2
	default:
		return -1
	}
}
