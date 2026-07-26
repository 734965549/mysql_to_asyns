package metrics // 声明当前文件属于metrics包，用于Prometheus指标管理

import ( // 导入外部包和标准库
	"sync" // 导入sync包，用于并发控制

	"github.com/prometheus/client_golang/prometheus" // 导入Prometheus客户端库
)

// Metrics Prometheus指标结构体
type Metrics struct { // 定义Prometheus指标结构体
	TasksTotal     prometheus.Gauge // 任务总数指标
	TasksRunning   prometheus.Gauge // 运行中任务数指标
	TasksCompleted prometheus.Gauge // 已完成任务数指标
	TasksFailed    prometheus.Gauge // 失败任务数指标

	RowsProcessed    prometheus.Counter // 已处理行数计数器
	RowsTotal        prometheus.Gauge   // 总行数指标
	BytesTransferred prometheus.Counter // 传输字节数计数器

	SyncDuration prometheus.Histogram // 同步耗时直方图
	SyncErrors   prometheus.Counter   // 同步错误计数器

	BinlogLag      prometheus.Gauge // Binlog延迟指标
	BinlogPosition prometheus.Gauge // Binlog位置指标

	// === 修复 10/14：增量阶段 UPDATE/DELETE 命中 0 行的累计指标 ===
	// 0 行匹配通常意味着目标侧已与事件不一致（漂移）；当前实现选择 warn + 埋点而非 hard fail，
	// 但这两个计数器允许通过 Prometheus 长期观测漂移趋势，必要时触发告警。
	IncrementalZeroRowUpdateTotal prometheus.Counter // 增量 UPDATE 命中 0 行的次数（按事件计）
	IncrementalZeroRowDeleteTotal prometheus.Counter // 增量 DELETE 命中 0 行的次数（按事件计）

	// === 修复 9/14：无主键表增量事件计数 ===
	// 用户选择"无主键表照走"，至少要把无主键事件流量量化出来，方便定位风险表与衡量影响面。
	IncrementalNoPKTableEventsTotal prometheus.Counter

	// IncrementalSinkTxnBufferLimitTotal 外部 sink（Kafka/Webhook）单事务缓冲硬上限触顶次数。
	// 与 subscriber spill 独立；触顶后事务无法应用，checkpoint 不推进，需运维介入（调大 limit 或拆分源事务）。
	IncrementalSinkTxnBufferLimitTotal prometheus.Counter

	// === 全量 V2 引擎观测指标（低基数聚合，不带任务 ID / 表名标签）===
	FullLoadReadRowsTotal      prometheus.Counter // V2 已读取行数
	FullLoadReadBytesTotal     prometheus.Counter // V2 已读取字节数
	FullLoadWriteRowsTotal     prometheus.Counter // V2 已写入（未必已提交）行数
	FullLoadWriteBytesTotal    prometheus.Counter // V2 已写入字节数
	FullLoadCommitRowsTotal    prometheus.Counter // V2 已提交行数
	FullLoadCommitBytesTotal   prometheus.Counter // V2 已提交字节数
	FullLoadCommitsTotal       prometheus.Counter // V2 事务提交次数
	FullLoadTxReplaysTotal     prometheus.Counter // V2 整事务重放次数
	FullLoadLockRetriesTotal   prometheus.Counter // V2 可重试锁错误次数
	FullLoadQueueBytes         prometheus.Gauge   // V2 队列当前字节数
	FullLoadActiveReaders      prometheus.Gauge   // V2 当前活跃读取 worker
	FullLoadActiveWriters      prometheus.Gauge   // V2 当前活跃写入 worker
	FullLoadSnapshotGroups     prometheus.Gauge   // V2 当前活跃表级 snapshot group
	FullLoadSnapshotTxns       prometheus.Gauge   // V2 当前活跃快照事务/连接
	FullLoadOldestSnapshotMs   prometheus.Gauge   // V2 最老活跃 snapshot group 存活毫秒
	FullLoadAlignDegradesTotal prometheus.Counter // V2 对齐取锁失败降级次数

	// P3.6: P0/P2 可观测性指标(低基数聚合,不带任务 ID / 表名标签)
	FullLoadQueryTimeoutsTotal       prometheus.Counter // V2 源端查询超时次数
	FullLoadSlowQueriesTotal         prometheus.Counter // V2 源端慢查询次数
	FullLoadTableRetriesTotal        prometheus.Counter // V2 表级重试次数
	FullLoadTableRetryExhaustedTotal prometheus.Counter // V2 表级重试耗尽次数
	FullLoadActiveStagingTables      prometheus.Gauge   // V2 当前活跃 staging 表数

	// oldestSnapshotByTask 进程内各任务当前最老快照年龄；全局 gauge 取其 max，避免并行任务互相覆盖。
	oldestSnapshotMu     sync.Mutex
	oldestSnapshotByTask map[string]int64
}

var ( // 定义包级别变量
	instance *Metrics  // Metrics实例，单例模式
	once     sync.Once // 用于确保只初始化一次
)

// GetMetrics 获取指标实例函数
func GetMetrics() *Metrics { // 获取Metrics单例实例
	once.Do(func() { // 确保只执行一次初始化
		instance = &Metrics{ // 创建Metrics实例
			TasksTotal: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建任务总数指标
				Name: "mysql_sync_tasks_total",     // 指标名称
				Help: "Total number of sync tasks", // 指标帮助信息
			}),
			TasksRunning: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建运行中任务数指标
				Name: "mysql_sync_tasks_running", // 指标名称
				Help: "Number of running tasks",  // 指标帮助信息
			}),
			TasksCompleted: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建已完成任务数指标
				Name: "mysql_sync_tasks_completed", // 指标名称
				Help: "Number of completed tasks",  // 指标帮助信息
			}),
			TasksFailed: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建失败任务数指标
				Name: "mysql_sync_tasks_failed", // 指标名称
				Help: "Number of failed tasks",  // 指标帮助信息
			}),
			RowsProcessed: prometheus.NewCounter(prometheus.CounterOpts{ // 创建已处理行数计数器
				Name: "mysql_sync_rows_processed_total", // 指标名称
				Help: "Total number of rows processed",  // 指标帮助信息
			}),
			RowsTotal: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建总行数指标
				Name: "mysql_sync_rows_total",           // 指标名称
				Help: "Total number of rows to process", // 指标帮助信息
			}),
			BytesTransferred: prometheus.NewCounter(prometheus.CounterOpts{ // 创建传输字节数计数器
				Name: "mysql_sync_bytes_transferred_total", // 指标名称
				Help: "Total bytes transferred",            // 指标帮助信息
			}),
			SyncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{ // 创建同步耗时直方图
				Name:    "mysql_sync_duration_seconds", // 指标名称
				Help:    "Duration of sync operations", // 指标帮助信息
				Buckets: prometheus.DefBuckets,         // 使用默认桶配置
			}),
			SyncErrors: prometheus.NewCounter(prometheus.CounterOpts{ // 创建同步错误计数器
				Name: "mysql_sync_errors_total",     // 指标名称
				Help: "Total number of sync errors", // 指标帮助信息
			}),
			BinlogLag: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建Binlog延迟指标
				Name: "mysql_sync_binlog_lag_seconds", // 指标名称
				Help: "Binlog lag in seconds",         // 指标帮助信息
			}),
			BinlogPosition: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建Binlog位置指标
				Name: "mysql_sync_binlog_position", // 指标名称
				Help: "Current binlog position",    // 指标帮助信息
			}),
			IncrementalZeroRowUpdateTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_incremental_zero_row_update_total",
				Help: "Total number of incremental UPDATE events that matched 0 rows (possible data drift)",
			}),
			IncrementalZeroRowDeleteTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_incremental_zero_row_delete_total",
				Help: "Total number of incremental DELETE events that matched 0 rows (possible data drift)",
			}),
			IncrementalNoPKTableEventsTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_incremental_no_pk_table_events_total",
				Help: "Total number of incremental events targeting tables without primary/unique key (idempotency at risk)",
			}),
			IncrementalSinkTxnBufferLimitTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_incremental_sink_txn_buffer_limit_total",
				Help: "Total times an external sink (Kafka/Webhook) rejected a source transaction due to in-memory buffer hard limit",
			}),
			FullLoadReadRowsTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_read_rows_total",
				Help: "Total rows read by the full-load V2 engine",
			}),
			FullLoadReadBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_read_bytes_total",
				Help: "Total bytes read by the full-load V2 engine",
			}),
			FullLoadWriteRowsTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_write_rows_total",
				Help: "Total rows written (not necessarily committed) by the full-load V2 engine",
			}),
			FullLoadWriteBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_write_bytes_total",
				Help: "Total bytes written by the full-load V2 engine",
			}),
			FullLoadCommitRowsTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_commit_rows_total",
				Help: "Total committed rows by the full-load V2 engine",
			}),
			FullLoadCommitBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_commit_bytes_total",
				Help: "Total committed bytes by the full-load V2 engine",
			}),
			FullLoadCommitsTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_commits_total",
				Help: "Total transaction commits by the full-load V2 engine",
			}),
			FullLoadTxReplaysTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_tx_replays_total",
				Help: "Total whole-transaction replays after retryable lock errors in the full-load V2 engine",
			}),
			FullLoadLockRetriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_full_load_lock_retries_total",
				Help: "Total retryable lock errors hit by the full-load V2 engine",
			}),
			FullLoadQueueBytes: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_full_load_queue_bytes",
				Help: "Current buffered bytes in the full-load V2 row-batch queue",
			}),
			FullLoadActiveReaders: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_full_load_active_readers",
				Help: "Current active source read workers in the full-load V2 engine",
			}),
			FullLoadActiveWriters: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_full_load_active_writers",
				Help: "Current active target write workers in the full-load V2 engine",
			}),
			FullLoadSnapshotGroups: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_full_load_snapshot_groups",
				Help: "Current active table-level snapshot groups in the full-load V2 engine",
			}),
			FullLoadSnapshotTxns: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_full_load_snapshot_txns",
				Help: "Current active consistent-snapshot transactions/connections in the full-load V2 engine",
			}),
			FullLoadOldestSnapshotMs: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_full_load_oldest_snapshot_age_millis",
				Help: "Age in milliseconds of the oldest active full-load snapshot group",
			}),
			FullLoadAlignDegradesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mysql_sync_full_load_snapshot_align_degrades_total",
			Help: "Times multi-reader snapshot alignment lock failed and degraded to single-reader",
		}),
		FullLoadQueryTimeoutsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mysql_sync_full_load_source_query_timeouts_total",
			Help: "Total source query timeouts in the full-load V2 engine",
		}),
		FullLoadSlowQueriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mysql_sync_full_load_slow_queries_total",
			Help: "Total slow source queries (exceeded slow_query_warn threshold) in the full-load V2 engine",
		}),
		FullLoadTableRetriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mysql_sync_full_load_table_retries_total",
			Help: "Total table-level read retries in the full-load V2 engine",
		}),
		FullLoadTableRetryExhaustedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mysql_sync_full_load_table_retry_exhausted_total",
			Help: "Total table-level read retries exhausted in the full-load V2 engine",
		}),
		FullLoadActiveStagingTables: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mysql_sync_full_load_staging_tables",
			Help: "Current active staging tables in the full-load V2 engine",
		}),
	}

		// 注册指标
		prometheus.MustRegister(instance.TasksTotal)       // 注册任务总数指标
		prometheus.MustRegister(instance.TasksRunning)     // 注册运行中任务数指标
		prometheus.MustRegister(instance.TasksCompleted)   // 注册已完成任务数指标
		prometheus.MustRegister(instance.TasksFailed)      // 注册失败任务数指标
		prometheus.MustRegister(instance.RowsProcessed)    // 注册已处理行数指标
		prometheus.MustRegister(instance.RowsTotal)        // 注册总行数指标
		prometheus.MustRegister(instance.BytesTransferred) // 注册传输字节数指标
		prometheus.MustRegister(instance.SyncDuration)     // 注册同步耗时指标
		prometheus.MustRegister(instance.SyncErrors)       // 注册同步错误指标
		prometheus.MustRegister(instance.BinlogLag)        // 注册Binlog延迟指标
		prometheus.MustRegister(instance.BinlogPosition)   // 注册Binlog位置指标
		prometheus.MustRegister(instance.IncrementalZeroRowUpdateTotal)
		prometheus.MustRegister(instance.IncrementalZeroRowDeleteTotal)
		prometheus.MustRegister(instance.IncrementalNoPKTableEventsTotal)
		prometheus.MustRegister(instance.IncrementalSinkTxnBufferLimitTotal)
		prometheus.MustRegister(instance.FullLoadReadRowsTotal)
		prometheus.MustRegister(instance.FullLoadReadBytesTotal)
		prometheus.MustRegister(instance.FullLoadWriteRowsTotal)
		prometheus.MustRegister(instance.FullLoadWriteBytesTotal)
		prometheus.MustRegister(instance.FullLoadCommitRowsTotal)
		prometheus.MustRegister(instance.FullLoadCommitBytesTotal)
		prometheus.MustRegister(instance.FullLoadCommitsTotal)
		prometheus.MustRegister(instance.FullLoadTxReplaysTotal)
		prometheus.MustRegister(instance.FullLoadLockRetriesTotal)
		prometheus.MustRegister(instance.FullLoadQueueBytes)
		prometheus.MustRegister(instance.FullLoadActiveReaders)
		prometheus.MustRegister(instance.FullLoadActiveWriters)
		prometheus.MustRegister(instance.FullLoadSnapshotGroups)
		prometheus.MustRegister(instance.FullLoadSnapshotTxns)
		prometheus.MustRegister(instance.FullLoadOldestSnapshotMs)
		prometheus.MustRegister(instance.FullLoadAlignDegradesTotal)
		prometheus.MustRegister(instance.FullLoadQueryTimeoutsTotal)
		prometheus.MustRegister(instance.FullLoadSlowQueriesTotal)
		prometheus.MustRegister(instance.FullLoadTableRetriesTotal)
		prometheus.MustRegister(instance.FullLoadTableRetryExhaustedTotal)
		prometheus.MustRegister(instance.FullLoadActiveStagingTables)
	})

	return instance // 返回实例
}

// IncrementIncrementalZeroRowUpdate 增量 UPDATE 命中 0 行时调用，累计漂移事件。
func (m *Metrics) IncrementIncrementalZeroRowUpdate() {
	m.IncrementalZeroRowUpdateTotal.Inc()
}

// IncrementIncrementalZeroRowDelete 增量 DELETE 命中 0 行时调用，累计漂移事件。
func (m *Metrics) IncrementIncrementalZeroRowDelete() {
	m.IncrementalZeroRowDeleteTotal.Inc()
}

// IncrementIncrementalNoPKTableEvents 增量事件落到无主键/无唯一键表时调用，按事件计数。
func (m *Metrics) IncrementIncrementalNoPKTableEvents() {
	m.IncrementalNoPKTableEventsTotal.Inc()
}

// IncrementIncrementalSinkTxnBufferLimit 外部 sink 单事务缓冲硬上限触顶时调用。
func (m *Metrics) IncrementIncrementalSinkTxnBufferLimit() {
	m.IncrementalSinkTxnBufferLimitTotal.Inc()
}

// addNonNegative 仅累加非负增量，防止快照差为负时污染 Counter。
func addNonNegative(c prometheus.Counter, delta int64) {
	if delta > 0 {
		c.Add(float64(delta))
	}
}

// AddFullLoadRead 累加 V2 已读取行数/字节数。
func (m *Metrics) AddFullLoadRead(rows, bytes int64) {
	addNonNegative(m.FullLoadReadRowsTotal, rows)
	addNonNegative(m.FullLoadReadBytesTotal, bytes)
}

// AddFullLoadWrite 累加 V2 已写入行数/字节数。
func (m *Metrics) AddFullLoadWrite(rows, bytes int64) {
	addNonNegative(m.FullLoadWriteRowsTotal, rows)
	addNonNegative(m.FullLoadWriteBytesTotal, bytes)
}

// AddFullLoadCommit 累加 V2 已提交行数/字节数/提交次数。
func (m *Metrics) AddFullLoadCommit(rows, bytes, commits int64) {
	addNonNegative(m.FullLoadCommitRowsTotal, rows)
	addNonNegative(m.FullLoadCommitBytesTotal, bytes)
	addNonNegative(m.FullLoadCommitsTotal, commits)
}

// AddFullLoadTxReplays 累加 V2 整事务重放次数。
func (m *Metrics) AddFullLoadTxReplays(n int64) { addNonNegative(m.FullLoadTxReplaysTotal, n) }

// AddFullLoadLockRetries 累加 V2 可重试锁错误次数。
func (m *Metrics) AddFullLoadLockRetries(n int64) { addNonNegative(m.FullLoadLockRetriesTotal, n) }

// SetFullLoadQueueBytes 设置 V2 队列当前字节数。
func (m *Metrics) SetFullLoadQueueBytes(bytes int64) { m.FullLoadQueueBytes.Set(float64(bytes)) }

// SetFullLoadActiveWorkers 设置 V2 当前活跃读/写 worker 数。
func (m *Metrics) SetFullLoadActiveWorkers(readers, writers int64) {
	m.FullLoadActiveReaders.Set(float64(readers))
	m.FullLoadActiveWriters.Set(float64(writers))
}

// AddFullLoadQueueBytes 按任务快照差值更新聚合队列字节数，支持多个 V2 任务并行运行。
func (m *Metrics) AddFullLoadQueueBytes(delta int64) {
	m.FullLoadQueueBytes.Add(float64(delta))
}

// AddFullLoadActiveWorkers 按任务快照差值更新聚合 worker 数，避免任务间互相覆盖 Gauge。
func (m *Metrics) AddFullLoadActiveWorkers(readersDelta, writersDelta int64) {
	m.FullLoadActiveReaders.Add(float64(readersDelta))
	m.FullLoadActiveWriters.Add(float64(writersDelta))
}

// AddFullLoadSnapshotGroups 按任务快照差值更新活跃 snapshot group 数。
func (m *Metrics) AddFullLoadSnapshotGroups(delta int64) {
	m.FullLoadSnapshotGroups.Add(float64(delta))
}

// AddFullLoadSnapshotTxns 按任务快照差值更新活跃快照事务数。
func (m *Metrics) AddFullLoadSnapshotTxns(delta int64) {
	m.FullLoadSnapshotTxns.Add(float64(delta))
}

// SetFullLoadOldestSnapshotAgeMillis 设置最老活跃 snapshot group 存活毫秒。
// Deprecated: 使用 SetTaskFullLoadOldestSnapshotAgeMillis，避免并行任务互相覆盖。
func (m *Metrics) SetFullLoadOldestSnapshotAgeMillis(ms int64) {
	m.FullLoadOldestSnapshotMs.Set(float64(ms))
}

// SetTaskFullLoadOldestSnapshotAgeMillis 按任务登记快照年龄，并将全局 gauge 设为所有任务的 max。
// ms<=0 表示该任务当前无活跃快照，从 registry 移除，避免任务结束写 0 掩盖其他任务。
func (m *Metrics) SetTaskFullLoadOldestSnapshotAgeMillis(taskID string, ms int64) {
	if taskID == "" {
		taskID = "_unknown"
	}
	m.oldestSnapshotMu.Lock()
	defer m.oldestSnapshotMu.Unlock()
	if m.oldestSnapshotByTask == nil {
		m.oldestSnapshotByTask = make(map[string]int64)
	}
	if ms <= 0 {
		delete(m.oldestSnapshotByTask, taskID)
	} else {
		m.oldestSnapshotByTask[taskID] = ms
	}
	var max int64
	for _, age := range m.oldestSnapshotByTask {
		if age > max {
			max = age
		}
	}
	m.FullLoadOldestSnapshotMs.Set(float64(max))
}

// ClearTaskFullLoadOldestSnapshotAge 清除任务的快照年龄登记并刷新全局 max gauge。
func (m *Metrics) ClearTaskFullLoadOldestSnapshotAge(taskID string) {
	m.SetTaskFullLoadOldestSnapshotAgeMillis(taskID, 0)
}

// AddFullLoadSnapshotAlignDegrades 累加对齐取锁失败降级次数。
func (m *Metrics) AddFullLoadSnapshotAlignDegrades(n int64) {
	addNonNegative(m.FullLoadAlignDegradesTotal, n)
}

// AddFullLoadQueryTimeouts 累加源端查询超时次数(P3.6)。
func (m *Metrics) AddFullLoadQueryTimeouts(n int64) {
	addNonNegative(m.FullLoadQueryTimeoutsTotal, n)
}

// AddFullLoadSlowQueries 累加源端慢查询次数(P3.6)。
func (m *Metrics) AddFullLoadSlowQueries(n int64) {
	addNonNegative(m.FullLoadSlowQueriesTotal, n)
}

// AddFullLoadTableRetries 累加表级重试次数(P3.6)。
func (m *Metrics) AddFullLoadTableRetries(n int64) {
	addNonNegative(m.FullLoadTableRetriesTotal, n)
}

// AddFullLoadTableRetryExhausted 累加表级重试耗尽次数(P3.6)。
func (m *Metrics) AddFullLoadTableRetryExhausted(n int64) {
	addNonNegative(m.FullLoadTableRetryExhaustedTotal, n)
}

// AddFullLoadActiveStagingTables 按差值更新活跃 staging 表数(P3.6)。
func (m *Metrics) AddFullLoadActiveStagingTables(delta int64) {
	m.FullLoadActiveStagingTables.Add(float64(delta))
}

// UpdateTaskMetrics 更新任务指标方法
func (m *Metrics) UpdateTaskMetrics(total, running, completed, failed int) { // 更新任务相关指标
	m.TasksTotal.Set(float64(total))         // 设置任务总数
	m.TasksRunning.Set(float64(running))     // 设置运行中任务数
	m.TasksCompleted.Set(float64(completed)) // 设置已完成任务数
	m.TasksFailed.Set(float64(failed))       // 设置失败任务数
}

// IncrementRowsProcessed 增加已处理行数方法
func (m *Metrics) IncrementRowsProcessed(count int) { // 增加已处理行数
	m.RowsProcessed.Add(float64(count)) // 增加已处理行数计数器
}

// SetRowsTotal 设置总行数方法
func (m *Metrics) SetRowsTotal(total int64) { // 设置总行数
	m.RowsTotal.Set(float64(total)) // 设置总行数指标
}

// IncrementBytesTransferred 增加传输字节数方法
func (m *Metrics) IncrementBytesTransferred(bytes int) { // 增加传输字节数
	m.BytesTransferred.Add(float64(bytes)) // 增加传输字节数计数器
}

// RecordSyncDuration 记录同步耗时方法
func (m *Metrics) RecordSyncDuration(duration float64) { // 记录同步耗时
	m.SyncDuration.Observe(duration) // 观察并记录同步耗时
}

// IncrementSyncErrors 增加错误计数方法
func (m *Metrics) IncrementSyncErrors() { // 增加错误计数
	m.SyncErrors.Inc() // 增加同步错误计数器
}

// SetBinlogLag 设置Binlog延迟方法
func (m *Metrics) SetBinlogLag(lag float64) { // 设置Binlog延迟
	m.BinlogLag.Set(lag) // 设置Binlog延迟指标
}

// SetBinlogPosition 设置Binlog位置方法
func (m *Metrics) SetBinlogPosition(position uint32) { // 设置Binlog位置
	m.BinlogPosition.Set(float64(position)) // 设置Binlog位置指标
}
