package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics Prometheus指标
type Metrics struct {
	TasksTotal       prometheus.Gauge
	TasksRunning     prometheus.Gauge
	TasksCompleted   prometheus.Gauge
	TasksFailed      prometheus.Gauge
	
	RowsProcessed    prometheus.Counter
	RowsTotal        prometheus.Gauge
	BytesTransferred prometheus.Counter
	
	SyncDuration     prometheus.Histogram
	SyncErrors       prometheus.Counter
	
	BinlogLag        prometheus.Gauge
	BinlogPosition   prometheus.Gauge
}

var (
	instance *Metrics
	once     sync.Once
)

// GetMetrics 获取指标实例
func GetMetrics() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			TasksTotal: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_tasks_total",
				Help: "Total number of sync tasks",
			}),
			TasksRunning: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_tasks_running",
				Help: "Number of running tasks",
			}),
			TasksCompleted: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_tasks_completed",
				Help: "Number of completed tasks",
			}),
			TasksFailed: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_tasks_failed",
				Help: "Number of failed tasks",
			}),
			RowsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_rows_processed_total",
				Help: "Total number of rows processed",
			}),
			RowsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_rows_total",
				Help: "Total number of rows to process",
			}),
			BytesTransferred: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_bytes_transferred_total",
				Help: "Total bytes transferred",
			}),
			SyncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "mysql_sync_duration_seconds",
				Help:    "Duration of sync operations",
				Buckets: prometheus.DefBuckets,
			}),
			SyncErrors: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "mysql_sync_errors_total",
				Help: "Total number of sync errors",
			}),
			BinlogLag: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_binlog_lag_seconds",
				Help: "Binlog lag in seconds",
			}),
			BinlogPosition: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "mysql_sync_binlog_position",
				Help: "Current binlog position",
			}),
		}
		
		// 注册指标
		prometheus.MustRegister(instance.TasksTotal)
		prometheus.MustRegister(instance.TasksRunning)
		prometheus.MustRegister(instance.TasksCompleted)
		prometheus.MustRegister(instance.TasksFailed)
		prometheus.MustRegister(instance.RowsProcessed)
		prometheus.MustRegister(instance.RowsTotal)
		prometheus.MustRegister(instance.BytesTransferred)
		prometheus.MustRegister(instance.SyncDuration)
		prometheus.MustRegister(instance.SyncErrors)
		prometheus.MustRegister(instance.BinlogLag)
		prometheus.MustRegister(instance.BinlogPosition)
	})
	
	return instance
}

// UpdateTaskMetrics 更新任务指标
func (m *Metrics) UpdateTaskMetrics(total, running, completed, failed int) {
	m.TasksTotal.Set(float64(total))
	m.TasksRunning.Set(float64(running))
	m.TasksCompleted.Set(float64(completed))
	m.TasksFailed.Set(float64(failed))
}

// IncrementRowsProcessed 增加已处理行数
func (m *Metrics) IncrementRowsProcessed(count int) {
	m.RowsProcessed.Add(float64(count))
}

// SetRowsTotal 设置总行数
func (m *Metrics) SetRowsTotal(total int64) {
	m.RowsTotal.Set(float64(total))
}

// IncrementBytesTransferred 增加传输字节数
func (m *Metrics) IncrementBytesTransferred(bytes int) {
	m.BytesTransferred.Add(float64(bytes))
}

// RecordSyncDuration 记录同步耗时
func (m *Metrics) RecordSyncDuration(duration float64) {
	m.SyncDuration.Observe(duration)
}

// IncrementSyncErrors 增加错误计数
func (m *Metrics) IncrementSyncErrors() {
	m.SyncErrors.Inc()
}

// SetBinlogLag 设置Binlog延迟
func (m *Metrics) SetBinlogLag(lag float64) {
	m.BinlogLag.Set(lag)
}

// SetBinlogPosition 设置Binlog位置
func (m *Metrics) SetBinlogPosition(position uint32) {
	m.BinlogPosition.Set(float64(position))
}
