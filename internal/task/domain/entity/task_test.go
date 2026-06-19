package entity

import (
	"testing"
	"time"
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
