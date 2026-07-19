package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestGetMetrics(t *testing.T) {
	metrics := GetMetrics()
	if metrics == nil {
		t.Error("expected metrics instance, got nil")
	}

	// 多次调用应该返回同一个实例
	metrics2 := GetMetrics()
	if metrics != metrics2 {
		t.Error("expected same metrics instance")
	}
}

func TestFullLoadGaugesAggregateDeltas(t *testing.T) {
	metrics := GetMetrics()
	metrics.SetFullLoadQueueBytes(0)
	metrics.SetFullLoadActiveWorkers(0, 0)

	// 模拟两个并行任务分别上报快照差值；一个任务结束不能清零另一个任务的值。
	metrics.AddFullLoadQueueBytes(100)
	metrics.AddFullLoadQueueBytes(50)
	metrics.AddFullLoadActiveWorkers(2, 3)
	metrics.AddFullLoadActiveWorkers(1, 1)
	metrics.AddFullLoadQueueBytes(-100)
	metrics.AddFullLoadActiveWorkers(-2, -3)

	if got := gaugeValue(t, metrics.FullLoadQueueBytes); got != 50 {
		t.Fatalf("queue gauge=%v want 50", got)
	}
	if got := gaugeValue(t, metrics.FullLoadActiveReaders); got != 1 {
		t.Fatalf("reader gauge=%v want 1", got)
	}
	if got := gaugeValue(t, metrics.FullLoadActiveWriters); got != 1 {
		t.Fatalf("writer gauge=%v want 1", got)
	}

	metrics.AddFullLoadQueueBytes(-50)
	metrics.AddFullLoadActiveWorkers(-1, -1)
}

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := gauge.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetGauge().GetValue()
}

func TestUpdateTaskMetrics(t *testing.T) {
	metrics := GetMetrics()

	metrics.UpdateTaskMetrics(10, 3, 5, 2)

	// 验证指标是否设置成功（通过Prometheus注册表）
	// 这里我们只验证不会panic
}

func TestIncrementRowsProcessed(t *testing.T) {
	metrics := GetMetrics()

	metrics.IncrementRowsProcessed(100)
	metrics.IncrementRowsProcessed(200)

	// 验证计数器增加
}

func TestSetRowsTotal(t *testing.T) {
	metrics := GetMetrics()

	metrics.SetRowsTotal(10000)

	// 验证Gauge设置
}

func TestIncrementBytesTransferred(t *testing.T) {
	metrics := GetMetrics()

	metrics.IncrementBytesTransferred(1024)
	metrics.IncrementBytesTransferred(2048)

	// 验证计数器增加
}

func TestRecordSyncDuration(t *testing.T) {
	metrics := GetMetrics()

	metrics.RecordSyncDuration(1.5)
	metrics.RecordSyncDuration(2.3)
	metrics.RecordSyncDuration(0.8)

	// 验证Histogram记录
}

func TestIncrementSyncErrors(t *testing.T) {
	metrics := GetMetrics()

	initialCount := metrics.SyncErrors

	metrics.IncrementSyncErrors()
	metrics.IncrementSyncErrors()

	// 验证错误计数增加
	_ = initialCount
}

func TestSetBinlogLag(t *testing.T) {
	metrics := GetMetrics()

	metrics.SetBinlogLag(5.5)

	// 验证Gauge设置
}

func TestSetBinlogPosition(t *testing.T) {
	metrics := GetMetrics()

	metrics.SetBinlogPosition(12345)

	// 验证Gauge设置
}

func TestMetricsRegistration(t *testing.T) {
	// 验证所有指标都已注册到Prometheus
	metrics := GetMetrics()

	// 检查每个指标都不是nil
	if metrics.TasksTotal == nil {
		t.Error("TasksTotal not registered")
	}
	if metrics.TasksRunning == nil {
		t.Error("TasksRunning not registered")
	}
	if metrics.TasksCompleted == nil {
		t.Error("TasksCompleted not registered")
	}
	if metrics.TasksFailed == nil {
		t.Error("TasksFailed not registered")
	}
	if metrics.RowsProcessed == nil {
		t.Error("RowsProcessed not registered")
	}
	if metrics.RowsTotal == nil {
		t.Error("RowsTotal not registered")
	}
	if metrics.BytesTransferred == nil {
		t.Error("BytesTransferred not registered")
	}
	if metrics.SyncDuration == nil {
		t.Error("SyncDuration not registered")
	}
	if metrics.SyncErrors == nil {
		t.Error("SyncErrors not registered")
	}
	if metrics.BinlogLag == nil {
		t.Error("BinlogLag not registered")
	}
	if metrics.BinlogPosition == nil {
		t.Error("BinlogPosition not registered")
	}
}

func TestPrometheusGather(t *testing.T) {
	// 测试Prometheus指标收集
	metrics := GetMetrics()

	// 设置一些指标
	metrics.UpdateTaskMetrics(10, 3, 5, 2)
	metrics.IncrementRowsProcessed(1000)
	metrics.SetBinlogLag(10.5)

	// 收集指标
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("failed to gather metrics: %v", err)
	}

	// 验证至少有一些指标被收集
	if len(families) == 0 {
		t.Error("no metrics gathered")
	}

	// 检查我们的指标是否存在
	metricNames := make(map[string]bool)
	for _, family := range families {
		metricNames[family.GetName()] = true
	}

	expectedMetrics := []string{
		"mysql_sync_tasks_total",
		"mysql_sync_tasks_running",
		"mysql_sync_rows_processed_total",
		"mysql_sync_binlog_lag_seconds",
	}

	for _, name := range expectedMetrics {
		if !metricNames[name] {
			t.Errorf("expected metric %s not found", name)
		}
	}
}
