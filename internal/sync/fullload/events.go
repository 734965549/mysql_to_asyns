package fullload

import "fmt"

// EventSeverity mirrors task event severity without importing internal/task.
type EventSeverity string

const (
	EventSeverityInfo  EventSeverity = "INFO"
	EventSeverityWarn  EventSeverity = "WARN"
	EventSeverityError EventSeverity = "ERROR"
)

// EventVisibility mirrors task event visibility.
type EventVisibility string

const (
	EventVisibilityKey        EventVisibility = "KEY"
	EventVisibilityDiagnostic EventVisibility = "DIAGNOSTIC"
)

// Event category strings (mirror task entity categories).
const (
	EventCategoryConfig = "CONFIG"
	EventCategoryTable  = "TABLE"
	EventCategoryPool   = "POOL"
	EventCategoryQueue  = "QUEUE"
	EventCategoryRetry  = "RETRY"
)

// B2 全量关键事件码（字符串与 task 层 entity 对齐）。
const (
	EventCodeFullLoadConfigEffective = "FULL_LOAD_CONFIG_EFFECTIVE"

	EventCodeTablePlanCreated         = "TABLE_PLAN_CREATED"
	EventCodeTableEstimateFailed      = "TABLE_ESTIMATE_FAILED"
	EventCodeTableParallelismReduced  = "TABLE_PARALLELISM_REDUCED"
	EventCodeNOPKSequentialFallback   = "NOPK_SEQUENTIAL_FALLBACK"
	EventCodeTableChunkPlanFallback   = "TABLE_CHUNK_PLAN_FALLBACK"

	EventCodeSourcePoolBudgetCapped = "SOURCE_POOL_BUDGET_CAPPED"
	EventCodeSourcePoolWaitHigh     = "SOURCE_POOL_WAIT_HIGH"
	EventCodeTargetPoolBudgetCapped = "TARGET_POOL_BUDGET_CAPPED"

	EventCodeWriteLockRetry            = "WRITE_LOCK_RETRY"
	EventCodeTxCommitUnknown           = "TX_COMMIT_UNKNOWN"
	EventCodeTxCommitVerifiedApplied   = "TX_COMMIT_VERIFIED_APPLIED"
	EventCodeTxCommitVerifiedRolledBack  = "TX_COMMIT_VERIFIED_ROLLED_BACK"
	EventCodeTxReplayStarted           = "TX_REPLAY_STARTED"
	EventCodeTxReplaySucceeded         = "TX_REPLAY_SUCCEEDED"
	EventCodeTxReplayExhausted         = "TX_REPLAY_EXHAUSTED"
	EventCodeTableReadRetry            = "TABLE_READ_RETRY"
	EventCodeTableReadRetryExhausted   = "TABLE_READ_RETRY_EXHAUSTED"
	EventCodeTableReadBatchRetry       = "TABLE_READ_BATCH_RETRY"

	EventCodeStagingTableCreated   = "STAGING_TABLE_CREATED"
	EventCodeStagingTablePublished = "STAGING_TABLE_PUBLISHED"
	EventCodeStagingTableDropped   = "STAGING_TABLE_DROPPED"

	EventCodeQueueBackpressureHigh      = "QUEUE_BACKPRESSURE_HIGH"
	EventCodeQueueBackpressureRecovered = "QUEUE_BACKPRESSURE_RECOVERED"
	EventCodeSlowSourceQuery            = "SLOW_SOURCE_QUERY"

	EventCodeWideTableTwoPhaseEnabled = "WIDE_TABLE_TWO_PHASE_ENABLED"
	EventCodeRowExceedsBatchBytes     = "ROW_EXCEEDS_BATCH_BYTES"
)

const EventCodeWideTableAdaptiveWindowEnabled = "WIDE_TABLE_ADAPTIVE_WINDOW_ENABLED"

// FullLoadEvent 全量引擎上报的事件载荷（不含 task 包类型）。
type FullLoadEvent struct {
	Severity     EventSeverity
	Visibility   EventVisibility
	Category     string
	Code         string
	Phase        string
	SourceSchema string
	SourceTable  string
	Message      string
	Details      map[string]interface{}
}

// EventSink 全量引擎可选事件出口；由 task 层适配为 TaskEventRecorder。
type EventSink interface {
	Emit(event FullLoadEvent)
}

// Emit 若 sink 非 nil 则上报事件；默认 visibility=KEY。
func Emit(sink EventSink, ev FullLoadEvent) {
	if sink == nil {
		return
	}
	if ev.Visibility == "" {
		ev.Visibility = EventVisibilityKey
	}
	sink.Emit(ev)
}

func (e *Engine) emit(ev FullLoadEvent) {
	if e == nil {
		return
	}
	Emit(e.EventSink, ev)
}

func tableEvent(sink EventSink, schema, table, code, category string, severity EventSeverity, message string, details map[string]interface{}) {
	Emit(sink, FullLoadEvent{
		Severity:     severity,
		Category:     category,
		Code:         code,
		SourceSchema: schema,
		SourceTable:  table,
		Message:      message,
		Details:      details,
	})
}

func configEvent(sink EventSink, code, message string, details map[string]interface{}) {
	Emit(sink, FullLoadEvent{
		Severity: EventSeverityInfo,
		Category: EventCategoryConfig,
		Code:     code,
		Message:  message,
		Details:  details,
	})
}

func poolEvent(sink EventSink, code, message string, severity EventSeverity, details map[string]interface{}) {
	Emit(sink, FullLoadEvent{
		Severity: severity,
		Category: EventCategoryPool,
		Code:     code,
		Message:  message,
		Details:  details,
	})
}

func queueEvent(sink EventSink, code, message string, details map[string]interface{}) {
	Emit(sink, FullLoadEvent{
		Severity: EventSeverityWarn,
		Category: EventCategoryQueue,
		Code:     code,
		Message:  message,
		Details:  details,
	})
}

func retryEvent(sink EventSink, schema, table, code, message string, severity EventSeverity, details map[string]interface{}) {
	Emit(sink, FullLoadEvent{
		Severity:     severity,
		Category:     EventCategoryRetry,
		Code:         code,
		SourceSchema: schema,
		SourceTable:  table,
		Message:      message,
		Details:      details,
	})
}

// optionsEventDetails 构建 V2 生效配置详情。
func optionsEventDetails(opt Options, sourcePoolMax, targetPoolMax int) map[string]interface{} {
	return map[string]interface{}{
		"read_workers":             opt.ReadWorkers,
		"global_read_budget":       opt.GlobalReadBudget,
		"table_workers":            opt.TableWorkers,
		"write_workers":            opt.WriteWorkers,
		"table_parallel_readers":   opt.TableParallelReaders,
		"buffer_bytes":             opt.BufferBytes,
		"batch_rows":               opt.BatchRows,
		"batch_bytes":              opt.BatchBytes,
		"commit_rows":              opt.CommitRows,
		"commit_bytes":             opt.CommitBytes,
		"large_table_rows":         opt.LargeTableRows,
		"read_retry_times":         opt.ReadRetryTimes,
		"staging_enabled":          opt.StagingEnabled,
		"two_phase_read":           opt.TwoPhaseRead,
		"source_pool_max_open":     sourcePoolMax,
		"target_pool_max_open":     targetPoolMax,
		"query_timeout_sec":        int(opt.QueryTimeout.Seconds()),
		"slow_query_warn_sec":      int(opt.SlowQueryWarnThreshold.Seconds()),
	}
}

func optionsEffectiveMessage(opt Options) string {
	return fmt.Sprintf("global_read_budget=%d table_workers=%d write_workers=%d table_parallel=%d buffer=%dMiB batch_rows=%d staging=%t",
		opt.GlobalReadBudget, opt.TableWorkers, opt.WriteWorkers, opt.TableParallelReaders,
		opt.BufferBytes/1024/1024, opt.BatchRows, opt.StagingEnabled)
}
