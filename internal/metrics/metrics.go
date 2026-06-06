package metrics // 声明当前文件属于metrics包，用于Prometheus指标管理

import ( // 导入外部包和标准库
	"sync" // 导入sync包，用于并发控制

	"github.com/prometheus/client_golang/prometheus" // 导入Prometheus客户端库
)

// Metrics Prometheus指标结构体
type Metrics struct { // 定义Prometheus指标结构体
	TasksTotal       prometheus.Gauge // 任务总数指标
	TasksRunning     prometheus.Gauge // 运行中任务数指标
	TasksCompleted   prometheus.Gauge // 已完成任务数指标
	TasksFailed      prometheus.Gauge // 失败任务数指标
	
	RowsProcessed    prometheus.Counter // 已处理行数计数器
	RowsTotal        prometheus.Gauge // 总行数指标
	BytesTransferred prometheus.Counter // 传输字节数计数器
	
	SyncDuration     prometheus.Histogram // 同步耗时直方图
	SyncErrors       prometheus.Counter // 同步错误计数器
	
	BinlogLag        prometheus.Gauge // Binlog延迟指标
	BinlogPosition   prometheus.Gauge // Binlog位置指标

	// === 修复 10/14：增量阶段 UPDATE/DELETE 命中 0 行的累计指标 ===
	// 0 行匹配通常意味着目标侧已与事件不一致（漂移）；当前实现选择 warn + 埋点而非 hard fail，
	// 但这两个计数器允许通过 Prometheus 长期观测漂移趋势，必要时触发告警。
	IncrementalZeroRowUpdateTotal prometheus.Counter // 增量 UPDATE 命中 0 行的次数（按事件计）
	IncrementalZeroRowDeleteTotal prometheus.Counter // 增量 DELETE 命中 0 行的次数（按事件计）

	// === 修复 9/14：无主键表增量事件计数 ===
	// 用户选择"无主键表照走"，至少要把无主键事件流量量化出来，方便定位风险表与衡量影响面。
	IncrementalNoPKTableEventsTotal prometheus.Counter
}

var ( // 定义包级别变量
	instance *Metrics // Metrics实例，单例模式
	once     sync.Once // 用于确保只初始化一次
)

// GetMetrics 获取指标实例函数
func GetMetrics() *Metrics { // 获取Metrics单例实例
	once.Do(func() { // 确保只执行一次初始化
		instance = &Metrics{ // 创建Metrics实例
			TasksTotal: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建任务总数指标
				Name: "mysql_sync_tasks_total", // 指标名称
				Help: "Total number of sync tasks", // 指标帮助信息
			}),
			TasksRunning: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建运行中任务数指标
				Name: "mysql_sync_tasks_running", // 指标名称
				Help: "Number of running tasks", // 指标帮助信息
			}),
			TasksCompleted: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建已完成任务数指标
				Name: "mysql_sync_tasks_completed", // 指标名称
				Help: "Number of completed tasks", // 指标帮助信息
			}),
			TasksFailed: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建失败任务数指标
				Name: "mysql_sync_tasks_failed", // 指标名称
				Help: "Number of failed tasks", // 指标帮助信息
			}),
			RowsProcessed: prometheus.NewCounter(prometheus.CounterOpts{ // 创建已处理行数计数器
				Name: "mysql_sync_rows_processed_total", // 指标名称
				Help: "Total number of rows processed", // 指标帮助信息
			}),
			RowsTotal: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建总行数指标
				Name: "mysql_sync_rows_total", // 指标名称
				Help: "Total number of rows to process", // 指标帮助信息
			}),
			BytesTransferred: prometheus.NewCounter(prometheus.CounterOpts{ // 创建传输字节数计数器
				Name: "mysql_sync_bytes_transferred_total", // 指标名称
				Help: "Total bytes transferred", // 指标帮助信息
			}),
			SyncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{ // 创建同步耗时直方图
				Name:    "mysql_sync_duration_seconds", // 指标名称
				Help:    "Duration of sync operations", // 指标帮助信息
				Buckets: prometheus.DefBuckets, // 使用默认桶配置
			}),
			SyncErrors: prometheus.NewCounter(prometheus.CounterOpts{ // 创建同步错误计数器
				Name: "mysql_sync_errors_total", // 指标名称
				Help: "Total number of sync errors", // 指标帮助信息
			}),
			BinlogLag: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建Binlog延迟指标
				Name: "mysql_sync_binlog_lag_seconds", // 指标名称
				Help: "Binlog lag in seconds", // 指标帮助信息
			}),
			BinlogPosition: prometheus.NewGauge(prometheus.GaugeOpts{ // 创建Binlog位置指标
				Name: "mysql_sync_binlog_position", // 指标名称
				Help: "Current binlog position", // 指标帮助信息
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
		}
		
		// 注册指标
		prometheus.MustRegister(instance.TasksTotal) // 注册任务总数指标
		prometheus.MustRegister(instance.TasksRunning) // 注册运行中任务数指标
		prometheus.MustRegister(instance.TasksCompleted) // 注册已完成任务数指标
		prometheus.MustRegister(instance.TasksFailed) // 注册失败任务数指标
		prometheus.MustRegister(instance.RowsProcessed) // 注册已处理行数指标
		prometheus.MustRegister(instance.RowsTotal) // 注册总行数指标
		prometheus.MustRegister(instance.BytesTransferred) // 注册传输字节数指标
		prometheus.MustRegister(instance.SyncDuration) // 注册同步耗时指标
		prometheus.MustRegister(instance.SyncErrors) // 注册同步错误指标
		prometheus.MustRegister(instance.BinlogLag) // 注册Binlog延迟指标
		prometheus.MustRegister(instance.BinlogPosition) // 注册Binlog位置指标
		prometheus.MustRegister(instance.IncrementalZeroRowUpdateTotal)
		prometheus.MustRegister(instance.IncrementalZeroRowDeleteTotal)
		prometheus.MustRegister(instance.IncrementalNoPKTableEventsTotal)
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

// UpdateTaskMetrics 更新任务指标方法
func (m *Metrics) UpdateTaskMetrics(total, running, completed, failed int) { // 更新任务相关指标
	m.TasksTotal.Set(float64(total)) // 设置任务总数
	m.TasksRunning.Set(float64(running)) // 设置运行中任务数
	m.TasksCompleted.Set(float64(completed)) // 设置已完成任务数
	m.TasksFailed.Set(float64(failed)) // 设置失败任务数
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
