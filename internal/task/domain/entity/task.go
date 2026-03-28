package entity

import (
	"time"
)

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"   // 待执行
	TaskStatusRunning   TaskStatus = "RUNNING"   // 执行中
	TaskStatusPaused    TaskStatus = "PAUSED"    // 已暂停
	TaskStatusCompleted TaskStatus = "COMPLETED" // 已完成
	TaskStatusFailed    TaskStatus = "FAILED"    // 失败
)

// SyncMode 同步模式
type SyncMode string

const (
	SyncModeFull        SyncMode = "FULL"        // 全量同步
	SyncModeIncremental SyncMode = "INCREMENTAL" // 增量同步
	SyncModeAll         SyncMode = "ALL"         // 全量+增量
)

// SyncLevel 同步级别
type SyncLevel string

const (
	SyncLevelDatabase SyncLevel = "DATABASE" // 库级别同步（全库）
	SyncLevelTable    SyncLevel = "TABLE"    // 表级别同步（指定表）
)

// DatabaseConfig 数据库连接配置
type DatabaseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// TaskConfig 任务配置
type TaskConfig struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	SyncLevel       SyncLevel       `json:"sync_level"` // 同步级别：DATABASE(全库) 或 TABLE(指定表)
	SourceSchema    string          `json:"source_schema"`
	TargetSchema    string          `json:"target_schema"`
	SourceDatabases []string        `json:"source_databases"` // 源数据库列表（库级别同步时使用）
	TargetDatabase  string          `json:"target_database"`  // 目标数据库（库级别同步时，所有库同步到此库）
	TargetDatabases []string        `json:"target_databases"` // 目标数据库列表（与 SourceDatabases 一一对应）
	Tables          []string        `json:"tables"`
	Mode            SyncMode        `json:"mode"`
	BatchSize       int             `json:"batch_size"`
	WorkerCount     int             `json:"worker_count"`
	EnableLimitOne  bool            `json:"enable_limit_one"`    // 无主键表LIMIT 1保护
	OptimizeIndex   bool            `json:"optimize_index"`      // 索引优化：先删除非主键索引，数据迁移完成后再重建
	EnableReadOnly  bool            `json:"enable_read_only"`    // 同步前临时关闭目标库只读，同步后恢复
	SourceDB        *DatabaseConfig `json:"source_db,omitempty"` // 源数据库配置（可选，覆盖配置文件）
	TargetDB        *DatabaseConfig `json:"target_db,omitempty"` // 目标数据库配置（可选，覆盖配置文件）
}

// ProcessContext 处理上下文
type ProcessContext struct {
	Status          TaskStatus `json:"status"`
	CurrentPosition string     `json:"current_position"` // 当前位点
	ProgressPercent float64    `json:"progress_percent"` // 进度百分比
	TotalRows       int64      `json:"total_rows"`       // 总行数
	ProcessedRows   int64      `json:"processed_rows"`   // 已处理行数
	StartTime       time.Time  `json:"start_time"`
	EndTime         time.Time  `json:"end_time"`
	LastUpdateTime  time.Time  `json:"last_update_time"`
	ErrorStack      string     `json:"error_stack"`
}

// SyncTask 同步任务
type SyncTask struct {
	Config  TaskConfig     `json:"config"`
	Context ProcessContext `json:"context"`
}

// NewSyncTask 创建同步任务
func NewSyncTask(config TaskConfig) *SyncTask {
	return &SyncTask{
		Config: config,
		Context: ProcessContext{
			Status: TaskStatusPending,
		},
	}
}

// Start 启动任务
func (t *SyncTask) Start() {
	t.Context.Status = TaskStatusRunning
	t.Context.StartTime = time.Now()
	t.Context.LastUpdateTime = time.Now()
}

// Pause 暂停任务
func (t *SyncTask) Pause() {
	t.Context.Status = TaskStatusPaused
	t.Context.LastUpdateTime = time.Now()
}

// Complete 完成任务
func (t *SyncTask) Complete() {
	t.Context.Status = TaskStatusCompleted
	t.Context.EndTime = time.Now()
	t.Context.LastUpdateTime = time.Now()
	t.Context.ProgressPercent = 100
}

// Fail 任务失败
func (t *SyncTask) Fail(err error) {
	t.Context.Status = TaskStatusFailed
	t.Context.ErrorStack = err.Error()
	t.Context.EndTime = time.Now()
	t.Context.LastUpdateTime = time.Now()
}

// UpdateProgress 更新进度
func (t *SyncTask) UpdateProgress(processedRows int64, position string) {
	t.Context.ProcessedRows = processedRows
	t.Context.CurrentPosition = position
	if t.Context.TotalRows > 0 {
		t.Context.ProgressPercent = float64(processedRows) / float64(t.Context.TotalRows) * 100
	}
	t.Context.LastUpdateTime = time.Now()
}

// Checkpoint 位点信息
type Checkpoint struct {
	TaskID     string    `json:"task_id"`
	TableName  string    `json:"table_name"`
	BinlogFile string    `json:"binlog_file"`
	BinlogPos  uint32    `json:"binlog_pos"`
	LastUpdate time.Time `json:"last_update"`
}
