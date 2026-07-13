package entity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   TaskStatus
		expected string
	}{
		{"Pending", TaskStatusPending, "PENDING"},
		{"Running", TaskStatusRunning, "RUNNING"},
		{"Paused", TaskStatusPaused, "PAUSED"},
		{"Completed", TaskStatusCompleted, "COMPLETED"},
		{"Failed", TaskStatusFailed, "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.status) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.status)
			}
		})
	}
}

func TestSyncMode(t *testing.T) {
	tests := []struct {
		name     string
		mode     SyncMode
		expected string
	}{
		{"Full", SyncModeFull, "FULL"},
		{"Incremental", SyncModeIncremental, "INCREMENTAL"},
		{"All", SyncModeAll, "ALL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.mode) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.mode)
			}
		})
	}
}

func TestNewSyncTask(t *testing.T) {
	config := TaskConfig{
		ID:           "test_task_1",
		Name:         "Test Task",
		SourceSchema: "source_db",
		TargetSchema: "target_db",
		Tables:       []string{"users", "orders"},
		Mode:         SyncModeFull,
		BatchSize:    1000,
		WorkerCount:  4,
	}

	task := NewSyncTask(config)

	if task.Config.ID != config.ID {
		t.Errorf("expected ID %s, got %s", config.ID, task.Config.ID)
	}

	if task.Context.Status != TaskStatusPending {
		t.Errorf("expected status %s, got %s", TaskStatusPending, task.Context.Status)
	}

	if task.Context.ProcessedRows != 0 {
		t.Errorf("expected processed rows 0, got %d", task.Context.ProcessedRows)
	}
}

// TestNewSyncTask_NormalizesMode 验证 NewSyncTask 将 Mode 归一化为大写，
// 确保所有下游比较（校验、执行路径）大小写不敏感。
func TestNewSyncTask_NormalizesMode(t *testing.T) {
	tests := []struct {
		input    SyncMode
		expected SyncMode
	}{
		{"all", SyncModeAll},
		{"All", SyncModeAll},
		{"ALL", SyncModeAll},
		{"full", SyncModeFull},
		{"incremental", SyncModeIncremental},
		{"INCREMENTAL", SyncModeIncremental},
	}
	for _, tt := range tests {
		task := NewSyncTask(TaskConfig{ID: "test_mode", Mode: tt.input})
		assert.Equal(t, tt.expected, task.Config.Mode, "input mode %q should normalize to %q", tt.input, tt.expected)
	}
}

func TestTaskStart(t *testing.T) {
	config := TaskConfig{
		ID:   "test_task_2",
		Name: "Test Task",
	}

	task := NewSyncTask(config)
	task.Start()

	if task.Context.Status != TaskStatusRunning {
		t.Errorf("expected status %s, got %s", TaskStatusRunning, task.Context.Status)
	}

	if task.Context.StartTime.IsZero() {
		t.Error("start time should not be zero")
	}
}

// TestStart_DoesNotResetCounters 验证 Start 不再重置运行计数字段，
// 计数重置已移至 ResetFullSyncCounters，仅在全量开始时调用。
func TestStart_DoesNotResetCounters(t *testing.T) {
	task := NewSyncTask(TaskConfig{ID: "test_start_reset", Name: "Test"})
	// 模拟上一轮全量残留的计数
	task.Context.ProcessedRows = 9999
	task.Context.TotalRows = 10000
	task.Context.EstimatedTotalRows = 8000
	task.Context.ProgressPercent = 99.9
	task.Context.CurrentPosition = "old_pos"

	task.Start()

	// Start 应保留历史全量统计，不再清零
	assert.Equal(t, int64(9999), task.Context.ProcessedRows)
	assert.Equal(t, int64(10000), task.Context.TotalRows)
	assert.Equal(t, int64(8000), task.Context.EstimatedTotalRows)
	assert.Equal(t, float64(99.9), task.Context.ProgressPercent)
	assert.Equal(t, "old_pos", task.Context.CurrentPosition)
}

// TestResetFullSyncCounters 验证 ResetFullSyncCounters 重置运行计数字段，
// 仅在确定进入 executeFullSync 时调用。
func TestResetFullSyncCounters(t *testing.T) {
	task := NewSyncTask(TaskConfig{ID: "test_reset_counters", Name: "Test"})
	task.Context.ProcessedRows = 9999
	task.Context.TotalRows = 10000
	task.Context.EstimatedTotalRows = 8000
	task.Context.ProgressPercent = 99.9
	task.Context.CurrentPosition = "old_pos"

	task.ResetFullSyncCounters()

	assert.Equal(t, int64(0), task.Context.ProcessedRows)
	assert.Equal(t, int64(0), task.Context.TotalRows)
	assert.Equal(t, int64(0), task.Context.EstimatedTotalRows)
	assert.Equal(t, float64(0), task.Context.ProgressPercent)
	assert.Equal(t, "", task.Context.CurrentPosition)
}

func TestTaskPause(t *testing.T) {
	config := TaskConfig{
		ID:   "test_task_3",
		Name: "Test Task",
	}

	task := NewSyncTask(config)
	task.Start()
	task.Pause()

	if task.Context.Status != TaskStatusPaused {
		t.Errorf("expected status %s, got %s", TaskStatusPaused, task.Context.Status)
	}
}

func TestTaskComplete(t *testing.T) {
	config := TaskConfig{
		ID:   "test_task_4",
		Name: "Test Task",
	}

	task := NewSyncTask(config)
	task.Start()
	task.Complete()

	if task.Context.Status != TaskStatusCompleted {
		t.Errorf("expected status %s, got %s", TaskStatusCompleted, task.Context.Status)
	}

	if task.Context.EndTime.IsZero() {
		t.Error("end time should not be zero")
	}

	if task.Context.ProgressPercent != 100 {
		t.Errorf("expected progress 100, got %f", task.Context.ProgressPercent)
	}
}

func TestTaskComplete_PreservesTotalRows(t *testing.T) {
	task := NewSyncTask(TaskConfig{ID: "test_complete_rows", Name: "Test"})
	task.Start()
	// 在 Start 之后设置，因为 Start 会重置运行计数
	task.Context.ProcessedRows = 507165220
	task.Context.TotalRows = 507578780
	task.Complete()

	// TotalRows 不应被 ProcessedRows 覆盖；两者是独立事实
	if task.Context.TotalRows != 507578780 {
		t.Errorf("expected total_rows preserved as %d, got %d",
			507578780, task.Context.TotalRows)
	}
	if task.Context.TotalRows == task.Context.ProcessedRows {
		t.Error("total_rows should NOT be overwritten to match processed_rows")
	}
}

func TestTaskUpdateProgress(t *testing.T) {
	config := TaskConfig{
		ID:   "test_task_5",
		Name: "Test Task",
	}

	task := NewSyncTask(config)
	task.Start()
	task.Context.TotalRows = 10000
	task.UpdateProgress(5000, "users:5000")

	if task.Context.ProcessedRows != 5000 {
		t.Errorf("expected processed rows 5000, got %d", task.Context.ProcessedRows)
	}

	if task.Context.CurrentPosition != "users:5000" {
		t.Errorf("expected position users:5000, got %s", task.Context.CurrentPosition)
	}

	if task.Context.ProgressPercent != 50.0 {
		t.Errorf("expected progress 50.0, got %f", task.Context.ProgressPercent)
	}
}

func TestTaskFail(t *testing.T) {
	config := TaskConfig{
		ID:   "test_task_6",
		Name: "Test Task",
	}

	task := NewSyncTask(config)
	task.Start()
	task.Fail(assertAnError("test error"))

	if task.Context.Status != TaskStatusFailed {
		t.Errorf("expected status %s, got %s", TaskStatusFailed, task.Context.Status)
	}

	if task.Context.ErrorStack != "test error" {
		t.Errorf("expected error 'test error', got %s", task.Context.ErrorStack)
	}

	if task.Context.EndTime.IsZero() {
		t.Error("end time should not be zero")
	}
}

func TestDatabaseConfig(t *testing.T) {
	config := &DatabaseConfig{
		Host:     "localhost",
		Port:     3306,
		Database: "test_db",
		Username: "root",
		Password: "password",
	}

	if config.Host != "localhost" {
		t.Errorf("expected host localhost, got %s", config.Host)
	}

	if config.Port != 3306 {
		t.Errorf("expected port 3306, got %d", config.Port)
	}
}

func TestTaskConfigWithCustomDB(t *testing.T) {
	config := TaskConfig{
		ID:   "test_task_7",
		Name: "Test Task",
		SourceDB: &DatabaseConfig{
			Host:     "source.example.com",
			Port:     3306,
			Database: "source_db",
			Username: "source_user",
			Password: "source_pass",
		},
		TargetDB: &DatabaseConfig{
			Host:     "target.example.com",
			Port:     3306,
			Database: "target_db",
			Username: "target_user",
			Password: "target_pass",
		},
	}

	if config.SourceDB.Host != "source.example.com" {
		t.Errorf("expected source host source.example.com, got %s", config.SourceDB.Host)
	}

	if config.TargetDB.Host != "target.example.com" {
		t.Errorf("expected target host target.example.com, got %s", config.TargetDB.Host)
	}
}

func TestSyncLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    SyncLevel
		expected string
	}{
		{"Database", SyncLevelDatabase, "DATABASE"},
		{"Table", SyncLevelTable, "TABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.level) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.level)
			}
		})
	}
}

func TestProcessContext(t *testing.T) {
	now := time.Now()
	ctx := ProcessContext{
		Status:          TaskStatusRunning,
		CurrentPosition: "binlog.000001:12345",
		ProgressPercent: 75.5,
		TotalRows:       10000,
		ProcessedRows:   7550,
		StartTime:       now,
		LastUpdateTime:  now,
	}

	if ctx.Status != TaskStatusRunning {
		t.Errorf("expected status RUNNING, got %s", ctx.Status)
	}

	if ctx.ProgressPercent != 75.5 {
		t.Errorf("expected progress 75.5, got %f", ctx.ProgressPercent)
	}

	if ctx.TotalRows != 10000 {
		t.Errorf("expected total rows 10000, got %d", ctx.TotalRows)
	}
}

// 辅助函数
func assertAnError(msg string) error {
	return &testError{message: msg}
}

type testError struct {
	message string
}

func (e *testError) Error() string {
	return e.message
}

func TestTableProgressInfo(t *testing.T) {
	now := time.Now()
	ti := TableProgressInfo{
		Schema:        "test_db",
		Table:         "users",
		TotalRows:     10000,
		ProcessedRows: 5000,
		ProgressPct:   50.0,
		SpeedRowsSec:  200.5,
		Status:        "running",
		StartedAt:     &now,
	}

	if ti.Schema != "test_db" {
		t.Errorf("expected schema test_db, got %s", ti.Schema)
	}
	if ti.Table != "users" {
		t.Errorf("expected table users, got %s", ti.Table)
	}
	if ti.TotalRows != 10000 {
		t.Errorf("expected total_rows 10000, got %d", ti.TotalRows)
	}
	if ti.ProcessedRows != 5000 {
		t.Errorf("expected processed_rows 5000, got %d", ti.ProcessedRows)
	}
	if ti.ProgressPct != 50.0 {
		t.Errorf("expected progress_pct 50.0, got %f", ti.ProgressPct)
	}
	if ti.SpeedRowsSec != 200.5 {
		t.Errorf("expected speed_rows_sec 200.5, got %f", ti.SpeedRowsSec)
	}
	if ti.Status != "running" {
		t.Errorf("expected status running, got %s", ti.Status)
	}
	if ti.StartedAt == nil || !ti.StartedAt.Equal(now) {
		t.Error("expected started_at to be set")
	}
}

func TestTableProgressInfo_StatusValues(t *testing.T) {
	tests := []struct {
		name   string
		status string
	}{
		{"pending", "pending"},
		{"running", "running"},
		{"completed", "completed"},
		{"failed", "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ti := TableProgressInfo{Status: tt.status}
			if ti.Status != tt.status {
				t.Errorf("expected status %s, got %s", tt.status, ti.Status)
			}
		})
	}
}

func TestTableProgressInfo_CompletedAt(t *testing.T) {
	now := time.Now()
	ti := TableProgressInfo{
		Status:      "completed",
		CompletedAt: &now,
		ProgressPct: 100,
	}

	if ti.CompletedAt == nil || !ti.CompletedAt.Equal(now) {
		t.Error("expected completed_at to be set")
	}
	if ti.ProgressPct != 100 {
		t.Errorf("expected progress_pct 100, got %f", ti.ProgressPct)
	}
}

func TestRunningProgress(t *testing.T) {
	now := time.Now()
	rp := RunningProgress{
		CurrentTable:    "test_db.users",
		OverallSpeed:    1050.8,
		ElapsedSeconds:  127.5,
		EstimatedRemain: 119.3,
		Phase:           "full",
		UpdatedAt:       now,
		Tables: []*TableProgressInfo{
			{Schema: "test_db", Table: "users", Status: "completed", ProgressPct: 100},
			{Schema: "test_db", Table: "orders", Status: "running", ProgressPct: 42.5},
		},
	}

	if rp.CurrentTable != "test_db.users" {
		t.Errorf("expected current_table test_db.users, got %s", rp.CurrentTable)
	}
	if rp.OverallSpeed != 1050.8 {
		t.Errorf("expected overall_speed 1050.8, got %f", rp.OverallSpeed)
	}
	if rp.ElapsedSeconds != 127.5 {
		t.Errorf("expected elapsed_seconds 127.5, got %f", rp.ElapsedSeconds)
	}
	if rp.EstimatedRemain != 119.3 {
		t.Errorf("expected estimated_remain 119.3, got %f", rp.EstimatedRemain)
	}
	if rp.Phase != "full" {
		t.Errorf("expected phase full, got %s", rp.Phase)
	}
	if len(rp.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(rp.Tables))
	}
}

func TestRunningProgress_EstimatedRemainNegative(t *testing.T) {
	rp := RunningProgress{
		EstimatedRemain: -1,
		Phase:           "full",
	}

	if rp.EstimatedRemain != -1 {
		t.Errorf("expected estimated_remain -1, got %f", rp.EstimatedRemain)
	}
}

func TestRunningProgress_PhaseValues(t *testing.T) {
	tests := []struct {
		name  string
		phase string
	}{
		{"full", "full"},
		{"incremental", "incremental"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rp := RunningProgress{Phase: tt.phase}
			if rp.Phase != tt.phase {
				t.Errorf("expected phase %s, got %s", tt.phase, rp.Phase)
			}
		})
	}
}

// TestResetRepeat_OnlyClearsRepeatFields 验证 ResetRepeat 仅清理 repeat 字段，
// 不触碰 cron 调度字段，以便 ConfigureCronSchedule 调用它后仍能保留刚写入的 cron 配置。
func TestResetRepeat_OnlyClearsRepeatFields(t *testing.T) {
	task := NewSyncTask(TaskConfig{ID: "reset_repeat_scope", Name: "Reset Repeat Scope"})
	task.ConfigureCronSchedule("0 9 * * 1-5", "Asia/Shanghai")
	task.Context.RepeatCount = 5
	task.Context.RepeatRemaining = 5
	task.Context.RepeatIntervalSec = 60

	task.ResetRepeat()

	assert.Zero(t, task.Context.RepeatCount)
	assert.Zero(t, task.Context.RepeatRemaining)
	assert.Zero(t, task.Context.RepeatIntervalSec)
	assert.Equal(t, "cron", task.Context.ScheduleMode)
	assert.Equal(t, "0 9 * * 1-5", task.Context.CronExpression)
	assert.Equal(t, "Asia/Shanghai", task.Context.CronTimezone)
}

// TestClearScheduleConfig_ClearsAllScheduleFields 验证 ClearScheduleConfig 清空全部调度字段。
func TestClearScheduleConfig_ClearsAllScheduleFields(t *testing.T) {
	task := NewSyncTask(TaskConfig{ID: "clear_cfg", Name: "Clear Cfg"})
	task.ConfigureCronSchedule("0 9 * * 1-5", "Asia/Shanghai")
	task.Context.RepeatCount = 3
	task.Context.RepeatRemaining = 3
	task.Context.RepeatIntervalSec = 30
	when := time.Now().Add(time.Hour)
	task.Context.ScheduledAt = &when
	prev := TaskStatusPaused
	task.Context.ScheduledFromStatus = &prev

	task.ClearScheduleConfig()

	assert.Empty(t, task.Context.ScheduleMode)
	assert.Empty(t, task.Context.CronExpression)
	assert.Empty(t, task.Context.CronTimezone)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.Zero(t, task.Context.RepeatCount)
	assert.Zero(t, task.Context.RepeatRemaining)
	assert.Zero(t, task.Context.RepeatIntervalSec)
}

// TestConfigureCronSchedule_PreservesCronFieldsAndFromStatus 验证设置 cron 调度后，
// cron 字段与 ScheduledFromStatus 都保留，CancelSchedule 才能正确恢复原始状态。
func TestConfigureCronSchedule_PreservesCronFieldsAndFromStatus(t *testing.T) {
	task := NewSyncTask(TaskConfig{ID: "cron_preserve", Name: "Cron Preserve"})
	task.Pause()
	prev := task.Context.Status
	require.NotEqual(t, TaskStatusScheduled, prev)
	task.Schedule(time.Now().Add(time.Hour))
	require.NotNil(t, task.Context.ScheduledFromStatus)

	task.ConfigureCronSchedule("0 9 * * 1-5", "Asia/Shanghai")

	assert.Equal(t, "cron", task.Context.ScheduleMode)
	assert.Equal(t, "0 9 * * 1-5", task.Context.CronExpression)
	assert.Equal(t, "Asia/Shanghai", task.Context.CronTimezone)
	require.NotNil(t, task.Context.ScheduledFromStatus)
	assert.Equal(t, prev, *task.Context.ScheduledFromStatus)
}

// TestCancelSchedule_ClearsCronConfigAndRestoresStatus 验证取消定时后恢复原状态并清空全部调度配置。
func TestCancelSchedule_ClearsCronConfigAndRestoresStatus(t *testing.T) {
	task := NewSyncTask(TaskConfig{ID: "cancel_cron", Name: "Cancel Cron"})
	task.Pause()
	task.Schedule(time.Now().Add(time.Hour))
	task.ConfigureCronSchedule("0 9 * * 1-5", "Asia/Shanghai")

	task.CancelSchedule()

	assert.Equal(t, TaskStatusPaused, task.Context.Status)
	assert.Empty(t, task.Context.ScheduleMode)
	assert.Empty(t, task.Context.CronExpression)
	assert.Empty(t, task.Context.CronTimezone)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
}

// TestStart_DoesNotMutateScheduleMode 验证 Start 不会清空 ScheduleMode，
// completeTask 需要依据 ScheduleMode 判断是否重新调度 cron/repeat 任务。
func TestStart_DoesNotMutateScheduleMode(t *testing.T) {
	task := NewSyncTask(TaskConfig{ID: "start_no_mutate", Name: "Start No Mutate"})
	task.ConfigureCronSchedule("0 9 * * 1-5", "Asia/Shanghai")

	task.Start()

	assert.Equal(t, "cron", task.Context.ScheduleMode)
	assert.Equal(t, "0 9 * * 1-5", task.Context.CronExpression)
	assert.Equal(t, TaskStatusRunning, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
}
