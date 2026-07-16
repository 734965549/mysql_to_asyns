package entity // 声明当前文件属于entity包，用于定义数据实体

import ( // 导入外部包和标准库

	"strings"
	"time" // 导入time包，用于时间处理

	"mysql-to-sync/pkg/crypto"
	sink "mysql-to-sync/internal/sync/domain/sink"
)

// TaskStatus 任务状态
type TaskStatus string // 定义任务状态为字符串类型

const ( // 定义常量
	TaskStatusPending   TaskStatus = "PENDING"   // 待执行：任务已创建但未开始
	TaskStatusRunning   TaskStatus = "RUNNING"   // 执行中：任务正在执行
	TaskStatusPaused    TaskStatus = "PAUSED"    // 已暂停：任务被暂停
	TaskStatusCompleted TaskStatus = "COMPLETED" // 已完成：任务执行完成
	TaskStatusFailed    TaskStatus = "FAILED"    // 失败：任务执行失败
	TaskStatusScheduled TaskStatus = "SCHEDULED" // 已计划：任务已设定定时启动时间
)

// SyncMode 同步模式
type SyncMode string // 定义同步模式为字符串类型

const ( // 定义常量
	SyncModeFull        SyncMode = "FULL"        // 全量同步：执行一次无缝全表遍历，不捕获 binlog 位点，不追平同步期间的变化
	SyncModeIncremental SyncMode = "INCREMENTAL" // 增量同步：只同步变更数据
	SyncModeAll         SyncMode = "ALL"         // 全量+增量：先捕获 binlog 位点，再全量遍历，然后从位点回放 binlog 追平变化并持续同步
)

// SyncLevel 同步级别
type SyncLevel string // 定义同步级别为字符串类型

const ( // 定义常量
	SyncLevelDatabase SyncLevel = "DATABASE" // 库级别同步：同步整个数据库
	SyncLevelTable    SyncLevel = "TABLE"    // 表级别同步：同步指定的表
)

// SyncPhase 同步阶段（独立于 TaskStatus 的细粒度阶段标志，用于恢复时区分"全量未完成 / 全量已完成 / 增量已接管"）。
//
// TaskStatus 描述任务整体运行/暂停/失败，SyncPhase 描述同步进度处在哪个阶段。
// 恢复决策分两层：
//   - 阶段级：能不能直接接增量 → 看 SyncPhase（HasFullSyncEverCompleted / FullSyncIncomplete）
//   - 全量未完成：不再支持 full_sync_resume 续传，需要重新准备目标端后启动新一轮全量
//
// 增量 binlog 位点由 checkpoint.Manager 管理，与 FullSyncResume 无关。
type SyncPhase string

const (
	SyncPhaseInit               SyncPhase = ""                    // 未开始（兼容历史任务的零值，等价于"从未同步过"）
	SyncPhaseFullStarted        SyncPhase = "FULL_STARTED"        // 全量已开始，但尚未完成（中途崩溃/暂停后停留在该阶段）
	SyncPhaseFullCompleted      SyncPhase = "FULL_COMPLETED"      // 全量已完成，但尚未启动增量
	SyncPhaseFullFailed         SyncPhase = "FULL_FAILED"         // 全量明确失败（被 failTaskUnlessCancelled 标记），下次必须重跑全量
	SyncPhaseIncrementalStarted SyncPhase = "INCREMENTAL_STARTED" // 增量已启动（可重启时直接接增量，无需重跑全量）
)

// DatabaseConfig 数据库连接配置
type DatabaseConfig struct { // 定义数据库连接配置结构体
	Host     string `json:"host"`     // 数据库主机地址
	Port     int    `json:"port"`     // 数据库端口
	Database string `json:"database"` // 数据库名称
	Username string `json:"username"` // 用户名
	Password string `json:"password"` // 密码
}

// TaskConfig 任务配置
// TaskConfig is the durable input contract for a sync task.
//
// API handlers build this value from JSON requests, TaskService uses it to
// initialize per-task runtime resources, and storage serializes it as part of
// SyncTask. Do not put live resources such as DB handles or cancel functions in
// this struct.
type TaskConfig struct { // 定义任务配置结构体
	StorageID             int64     `json:"storage_id,omitempty"`     // 存储ID
	ID                    string    `json:"id"`                       // 任务ID
	Name                  string    `json:"name"`                     // 任务名称
	SyncLevel             SyncLevel `json:"sync_level"`               // 同步级别：DATABASE(全库) 或 TABLE(指定表)
	SourceSchema          string    `json:"source_schema"`            // 源模式名
	TargetSchema          string    `json:"target_schema"`            // 目标模式名
	SourceDatabases       []string  `json:"source_databases"`         // 源数据库列表（库级别/表级别多库时使用）
	TargetDatabase        string    `json:"target_database"`          // 目标数据库（库级别同步时，所有库同步到此库）
	TargetDatabases       []string  `json:"target_databases"`         // 目标数据库列表（与 SourceDatabases 一一对应）
	Tables                []string  `json:"tables"`                   // 源表名列表
	TargetTables          []string  `json:"target_tables"`            // 目标表名列表（与 Tables 一一对应；空则沿用源表名）
	Mode                  SyncMode  `json:"mode"`                     // 同步模式
	BatchSize             int       `json:"batch_size"`               // 批处理大小
	WorkerCount           int       `json:"worker_count"`             // 工作线程数
	IntraTableWorkerCount int       `json:"intra_table_worker_count"` // 单表内并行读/写 goroutine 数；0 表示沿用旧逻辑 min(worker_count,16)
	EnableLimitOne        bool      `json:"enable_limit_one"`         // 无主键表LIMIT 1保护
	OptimizeIndex         bool      `json:"optimize_index"`           // 索引优化：先删除非主键索引，数据迁移完成后再重建
	// IndexRestoreWorkerCount 阶段3索引回放的表级并发度；0 表示按 min(worker_count,4) 推导。
	// 单表内多个索引仍串行重建，避免同表 MDL 锁竞争。建议 ≤ target_max_open_conns。
	IndexRestoreWorkerCount  int             `json:"index_restore_worker_count"`   // 索引回放并发度；0=自动推导
	EnableReadOnly           bool            `json:"enable_read_only"`             // 同步前临时关闭目标库只读，同步后恢复
	EnableDropTableBeforeDDL bool            `json:"enable_drop_table_before_ddl"` // 同步DDL前先执行 DROP TABLE IF EXISTS；开启后可重建目标端重新全量
	EnableSkipBinlog         bool            `json:"enable_skip_binlog"`           // 全量同步写入前在目标端临时关闭 sql_log_bin，写入后恢复；需目标账号具备 SUPER 权限
	TxCommitEveryNParallel   int             `json:"tx_commit_every_n_parallel"`   // 并行 worker 每 N 批提交一次事务；0 表示使用默认值 5。减小可降低锁等待，增大可减少 fsync 频率提高吞吐
	SourceDB                 *DatabaseConfig `json:"source_db,omitempty"`          // 源数据库配置（可选，覆盖配置文件）
	TargetDB                 *DatabaseConfig `json:"target_db,omitempty"`          // 目标数据库配置（可选，覆盖配置文件）
	SinkConfigs              []sink.SinkConfig `json:"sink_configs,omitempty"`     // 增量目标端配置（可选，默认 MYSQL）
}

// ProcessContext 处理上下文
// ProcessContext is the durable execution state of a task.
//
// TaskStatus is the external lifecycle state. SyncPhase is the internal sync
// stage used to decide whether full sync can resume or incremental sync can
// take over. FullSyncResume remains here as a historical archive-compatible
// field; incremental binlog positions are stored separately.
type ProcessContext struct { // 定义处理上下文结构体
	Status              TaskStatus  `json:"status"`                         // 任务状态
	CurrentPosition     string      `json:"current_position"`               // 当前位点
	ProgressPercent     float64     `json:"progress_percent"`               // 进度百分比
	TotalRows           int64       `json:"total_rows"`                     // 已同步总行数（由 worker 汇总），仅用于进度展示
	EstimatedTotalRows  int64       `json:"estimated_total_rows,omitempty"` // 估算总行数（information_schema），仅用于 ETA，不用于正确性校验
	ProcessedRows       int64       `json:"processed_rows"`                 // 已处理行数
	CreatedAt           time.Time   `json:"created_at"`                     // 创建时间
	StartTime           time.Time   `json:"start_time"`                     // 开始时间
	EndTime             time.Time   `json:"end_time"`                       // 结束时间
	LastUpdateTime      time.Time   `json:"last_update_time"`               // 最后更新时间
	ErrorStack          string      `json:"error_stack"`                    // 错误堆栈
	ScheduledAt         *time.Time  `json:"scheduled_at,omitempty"`         // 下次定时启动时间（为空表示立即启动）
	ScheduledFromStatus *TaskStatus `json:"scheduled_from_status,omitempty"`
	RepeatCount         int         `json:"repeat_count,omitempty"`        // 定时启动总次数（包含首次执行）
	RepeatRemaining     int         `json:"repeat_remaining,omitempty"`    // 剩余重复次数（包含下一次执行）
	RepeatIntervalSec   int         `json:"repeat_interval_sec,omitempty"` // 重复启动间隔（秒）
	ScheduleMode        string      `json:"schedule_mode,omitempty"`       // 定时模式：once / repeat / cron
	CronExpression      string      `json:"cron_expression,omitempty"`     // Cron 表达式（支持扩展语义）
	CronTimezone        string      `json:"cron_timezone,omitempty"`       // Cron 时区（可选，默认本地时区）

	// === 全量/增量阶段状态机（独立于 Status）===
	// 这些字段持久化在任务存档中，重启 / 重新 StartTask 时用于判断"该跑全量还是直接接增量"。
	// 历史任务无这些字段时按零值处理（SyncPhase=""），等价于 SyncPhaseInit，需要按 Mode 走完整流程。
	SyncPhase               SyncPhase  `json:"sync_phase,omitempty"`                // 同步阶段：FULL_STARTED / FULL_COMPLETED / FULL_FAILED / INCREMENTAL_STARTED
	FullSyncStartedAt       *time.Time `json:"full_sync_started_at,omitempty"`      // 最近一次全量启动时间
	FullSyncCompletedAt     *time.Time `json:"full_sync_completed_at,omitempty"`    // 全量完成时间（仅 SyncPhaseFullCompleted/IncrementalStarted 时有意义）
	FullSyncStartPosition   string     `json:"full_sync_start_position,omitempty"`  // 全量启动时捕获的 binlog 位点 "file:pos"（增量的起点）
	LastIncrementalPosition string     `json:"last_incremental_position,omitempty"` // 最近一次成功落库的增量位点 "file:pos"
	FullSyncFailedReason    string     `json:"full_sync_failed_reason,omitempty"`   // 全量失败原因（便于排查；SyncPhase=FULL_FAILED 时填充）

	// === 历史全量断点字段 ===
	// 历史兼容字段：曾用于记录每张表的全量同步进度，key = "sourceSchema.tableName"。
	// 当前全量使用普通 INSERT，暂停/失败后不再续传；进入新一轮全量前会清空。
	FullSyncResume map[string]*TableSyncProgress `json:"full_sync_resume,omitempty"`
}

// ResumeKey 历史全量断点键：主键各列的字符串化值（单列主键长度为 1，复合主键按列顺序）。
// interface{} 主键值（含 []byte / int / string 等）统一转为字符串持久化，
// 回填给 ReadBatchByKeys 作为 SQL bind 参数（字符串可被 MySQL 隐式转换为目标列类型）。
type ResumeKey struct {
	Vals []string `json:"vals"`
}

// TableSyncProgress 单表全量同步断点。
//
// 读取路径 read_path 取值：keyset（单线程主键顺序）、range（数值主键并行分片）、
// sample（采样边界并行）、nopk（无主键流式）。当前仅作历史断点字段兼容。
type TableSyncProgress struct {
	Done             bool               `json:"done"`                        // 该表是否已完整同步
	ReadPath         string             `json:"read_path,omitempty"`         // 读取路径：keyset / range / sample / nopk
	IntraWorkers     int                `json:"intra_workers,omitempty"`     // 首跑时的表内并行度（变化则分片游标失效）
	Cursor           *ResumeKey         `json:"cursor,omitempty"`            // 单线程 keyset 路径整表游标
	ShardCursors     map[int]*ResumeKey `json:"shard_cursors,omitempty"`     // 并行路径每分片游标，key = worker 序号
	SampleBoundaries []*ResumeKey       `json:"sample_boundaries,omitempty"` // sample 路径首跑边界（历史断点字段）
	ProcessedRows    int64              `json:"processed_rows,omitempty"`    // 该表已处理行数（仅用于展示/排查）
}

// SyncTask 同步任务
type SyncTask struct { // 定义同步任务结构体
	Config  TaskConfig     `json:"config"`  // 任务配置
	Context ProcessContext `json:"context"` // 处理上下文
}

// NewSyncTask 创建同步任务函数
func NewSyncTask(config TaskConfig) *SyncTask { // 创建同步任务实例
	// 归一化 Mode 为大写，确保所有下游比较（校验、执行路径）大小写不敏感
	config.Mode = SyncMode(strings.ToUpper(string(config.Mode)))
	now := time.Now()
	return &SyncTask{ // 返回任务实例
		Config: config, // 设置配置
		Context: ProcessContext{ // 初始化上下文
			Status:         TaskStatusPending, // 设置初始状态为待执行
			CreatedAt:      now,
			LastUpdateTime: now,
		},
	}
}

// Start 启动任务方法（仅管理生命周期状态，不清除全量统计）
func (t *SyncTask) Start() { // 启动任务
	t.Context.Status = TaskStatusRunning  // 设置状态为执行中
	t.Context.StartTime = time.Now()      // 记录开始时间
	t.Context.LastUpdateTime = time.Now() // 更新最后更新时间
	t.Context.ScheduledAt = nil           // 清除定时启动时间
	t.Context.ScheduledFromStatus = nil
}

// ResetFullSyncCounters 重置全量同步运行计数。
// 仅在确定进入 executeFullSync 时调用，避免 ALL/INCREMENTAL 重启时清空历史全量统计。
func (t *SyncTask) ResetFullSyncCounters() {
	t.Context.ProcessedRows = 0
	t.Context.TotalRows = 0
	t.Context.EstimatedTotalRows = 0
	t.Context.ProgressPercent = 0
	t.Context.CurrentPosition = ""
}

// ConfigureRepeat 设置重复定时启动参数
func (t *SyncTask) ConfigureRepeat(repeatCount, intervalSec int) {
	if repeatCount < 1 {
		repeatCount = 1
	}
	if intervalSec < 0 {
		intervalSec = 0
	}
	t.Context.ScheduleMode = "repeat"
	t.Context.RepeatCount = repeatCount
	t.Context.RepeatRemaining = repeatCount
	t.Context.RepeatIntervalSec = intervalSec
}

// ConfigureCronSchedule 设置 Cron 定时启动。
func (t *SyncTask) ConfigureCronSchedule(expr, timezone string) {
	t.Context.ScheduleMode = "cron"
	t.Context.CronExpression = strings.TrimSpace(expr)
	t.Context.CronTimezone = strings.TrimSpace(timezone)
	t.ResetRepeat()
}

// ConsumeScheduledRun 消耗一次已计划执行次数，返回是否还需要继续调度。
func (t *SyncTask) ConsumeScheduledRun() bool {
	if t.Context.RepeatRemaining > 0 {
		t.Context.RepeatRemaining--
	}
	return t.Context.RepeatRemaining > 0
}

// ResetRepeat 清空重复定时配置。
// 仅清理 repeat 相关字段，不触碰 cron/once 的调度字段与 ScheduledFromStatus，
// 以便 ConfigureCronSchedule 在调用本方法后仍能保留刚写入的 cron 配置。
func (t *SyncTask) ResetRepeat() {
	t.Context.RepeatCount = 0
	t.Context.RepeatRemaining = 0
	t.Context.RepeatIntervalSec = 0
}

// ClearScheduleConfig 清空所有定时调度配置（once / repeat / cron 模式字段 + 调度位点）。
// 用于任务最终完成、取消定时或立即启动时清除残留调度状态，避免前端在非 SCHEDULED 状态下误展示。
func (t *SyncTask) ClearScheduleConfig() {
	t.ResetRepeat()
	t.Context.ScheduleMode = ""
	t.Context.CronExpression = ""
	t.Context.CronTimezone = ""
	t.Context.ScheduledAt = nil
	t.Context.ScheduledFromStatus = nil
}

// Schedule 设置定时启动
func (t *SyncTask) Schedule(scheduledAt time.Time) { // 设置定时启动
	if t.Context.Status != TaskStatusScheduled {
		previousStatus := t.Context.Status
		t.Context.ScheduledFromStatus = &previousStatus
	} else if t.Context.ScheduledFromStatus == nil {
		previousStatus := TaskStatusPending
		t.Context.ScheduledFromStatus = &previousStatus
	}
	t.Context.Status = TaskStatusScheduled // 设置状态为已计划
	t.Context.ScheduledAt = &scheduledAt   // 记录定时启动时间
	t.Context.LastUpdateTime = time.Now()  // 更新最后更新时间
}

// CancelSchedule 取消定时启动
func (t *SyncTask) CancelSchedule() { // 取消定时启动
	restoreStatus := TaskStatusPending
	if t.Context.ScheduledFromStatus != nil {
		restoreStatus = *t.Context.ScheduledFromStatus
	}
	t.Context.Status = restoreStatus
	t.ClearScheduleConfig()
	t.Context.LastUpdateTime = time.Now()
}

// Pause 暂停任务方法
func (t *SyncTask) Pause() { // 暂停任务
	t.Context.Status = TaskStatusPaused   // 设置状态为已暂停
	t.Context.LastUpdateTime = time.Now() // 更新最后更新时间
}

// Complete 完成任务方法
func (t *SyncTask) Complete() { // 完成任务
	t.Context.Status = TaskStatusCompleted // 设置状态为已完成
	t.Context.EndTime = time.Now()         // 记录结束时间
	t.Context.LastUpdateTime = time.Now()  // 更新最后更新时间
	t.Context.ProgressPercent = 100        // 设置进度为100%
}

// Fail 任务失败方法
func (t *SyncTask) Fail(err error) { // 任务失败处理
	t.Context.Status = TaskStatusFailed   // 设置状态为失败
	t.Context.ErrorStack = err.Error()    // 记录错误信息
	t.Context.EndTime = time.Now()        // 记录结束时间
	t.Context.LastUpdateTime = time.Now() // 更新最后更新时间
}

// UpdateProgress 更新进度方法
func (t *SyncTask) UpdateProgress(processedRows int64, position string) { // 更新任务进度
	t.Context.ProcessedRows = processedRows // 设置已处理行数
	t.Context.CurrentPosition = position    // 设置当前位点
	// 进度计算优先使用精确总数，未到位时用估算总数兜底（仅 ETA 展示）
	effectiveTotal := t.Context.TotalRows
	if effectiveTotal <= 0 {
		effectiveTotal = t.Context.EstimatedTotalRows
	}
	if effectiveTotal > 0 {
		t.Context.ProgressPercent = float64(processedRows) / float64(effectiveTotal) * 100
	}
	t.Context.LastUpdateTime = time.Now() // 更新最后更新时间
}

// MarkFullSyncStarted 标记全量同步进入"已开始但未完成"阶段，并记录 binlog 起点位点字符串。
// startPosition 形如 "mysql-bin.000123:456"；FULL 模式传空串（不捕获位点），ALL 模式传实际位点。
func (t *SyncTask) MarkFullSyncStarted(startPosition string) {
	now := time.Now()
	t.Context.SyncPhase = SyncPhaseFullStarted
	t.Context.FullSyncStartedAt = &now
	t.Context.FullSyncStartPosition = startPosition
	t.Context.FullSyncCompletedAt = nil
	t.Context.FullSyncFailedReason = ""
	t.Context.LastUpdateTime = now
}

// MarkFullSyncCompleted 标记全量同步完成。完成后才允许增量直接接管。
func (t *SyncTask) MarkFullSyncCompleted() {
	now := time.Now()
	t.Context.SyncPhase = SyncPhaseFullCompleted
	t.Context.FullSyncCompletedAt = &now
	t.Context.FullSyncFailedReason = ""
	t.Context.LastUpdateTime = now
}

// MarkFullSyncFailed 标记全量明确失败。失败后不能直接接增量，也不再做 full_sync_resume 续传。
func (t *SyncTask) MarkFullSyncFailed(reason string) {
	t.Context.SyncPhase = SyncPhaseFullFailed
	t.Context.FullSyncFailedReason = reason
	t.Context.LastUpdateTime = time.Now()
}

// MarkIncrementalStarted 标记增量已启动。一旦进入该阶段，重启时即可直接接增量，不再重跑全量。
func (t *SyncTask) MarkIncrementalStarted() {
	t.Context.SyncPhase = SyncPhaseIncrementalStarted
	t.Context.LastUpdateTime = time.Now()
}

// UpdateIncrementalPosition 持久化最近一次增量位点字符串到任务存档（与 checkpoint manager 互为冗余，方便快照与离线排查）。
// position 形如 "mysql-bin.000123:7890"。
func (t *SyncTask) UpdateIncrementalPosition(position string) {
	t.Context.LastIncrementalPosition = position
	t.Context.LastUpdateTime = time.Now()
}

// ResetSyncPhase 清空所有阶段标志，回到 SyncPhaseInit，下次启动必须从全量开始。
// 用于显式"重新全量"的运维操作（目前未暴露 API，但保留方法以便后续扩展）。
func (t *SyncTask) ResetSyncPhase() {
	t.Context.SyncPhase = SyncPhaseInit
	t.Context.FullSyncStartedAt = nil
	t.Context.FullSyncCompletedAt = nil
	t.Context.FullSyncStartPosition = ""
	t.Context.LastIncrementalPosition = ""
	t.Context.FullSyncFailedReason = ""
	t.Context.LastUpdateTime = time.Now()
}

// HasFullSyncEverCompleted 返回是否曾经完成过一次全量（含 INCREMENTAL_STARTED 阶段）。
// 这是"INCREMENTAL 模式能不能直接启动"以及"ALL 模式能不能跳过全量"的唯一判据。
func (t *SyncTask) HasFullSyncEverCompleted() bool {
	return t.Context.SyncPhase == SyncPhaseFullCompleted ||
		t.Context.SyncPhase == SyncPhaseIncrementalStarted
}

// FullSyncIncomplete 返回全量是否处于"开始过但未完成"的中间态（崩溃/暂停/失败留下的不一致快照）。
// 该状态下不允许直接接增量；未开启 destructive rebuild 时也不允许继续全量续传。
func (t *SyncTask) FullSyncIncomplete() bool {
	return t.Context.SyncPhase == SyncPhaseFullStarted ||
		t.Context.SyncPhase == SyncPhaseFullFailed
}

// ResetFullSyncResume 清空所有表的历史全量断点（全新一轮全量开始时调用）。
func (t *SyncTask) ResetFullSyncResume() {
	t.Context.FullSyncResume = nil
}

// GetTableProgress 返回指定表的历史全量断点；不存在时返回 nil。
func (t *SyncTask) GetTableProgress(tableKey string) *TableSyncProgress {
	if t.Context.FullSyncResume == nil {
		return nil
	}
	return t.Context.FullSyncResume[tableKey]
}

// ensureTableProgress 获取（或创建）指定表的历史全量断点。
func (t *SyncTask) ensureTableProgress(tableKey string) *TableSyncProgress {
	if t.Context.FullSyncResume == nil {
		t.Context.FullSyncResume = make(map[string]*TableSyncProgress)
	}
	p := t.Context.FullSyncResume[tableKey]
	if p == nil {
		p = &TableSyncProgress{}
		t.Context.FullSyncResume[tableKey] = p
	}
	return p
}

// InitTableProgress 记录该表本次同步采用的读取路径与并行度（历史断点字段）。
func (t *SyncTask) InitTableProgress(tableKey, readPath string, intraWorkers int) {
	p := t.ensureTableProgress(tableKey)
	p.ReadPath = readPath
	p.IntraWorkers = intraWorkers
}

// SetTableCursor 记录单线程 keyset 路径整表游标。
func (t *SyncTask) SetTableCursor(tableKey string, key *ResumeKey) {
	p := t.ensureTableProgress(tableKey)
	p.Cursor = key
}

// SetShardCursor 记录并行路径某分片的游标。
func (t *SyncTask) SetShardCursor(tableKey string, shard int, key *ResumeKey) {
	p := t.ensureTableProgress(tableKey)
	if p.ShardCursors == nil {
		p.ShardCursors = make(map[int]*ResumeKey)
	}
	p.ShardCursors[shard] = key
}

// SetSampleBoundaries 持久化 sample 路径首跑的采样边界（历史断点字段）。
func (t *SyncTask) SetSampleBoundaries(tableKey string, boundaries []*ResumeKey) {
	p := t.ensureTableProgress(tableKey)
	p.SampleBoundaries = boundaries
}

// MarkTableDone 标记某表已完整同步，并清理其游标以释放存档体积。
func (t *SyncTask) MarkTableDone(tableKey string) {
	p := t.ensureTableProgress(tableKey)
	p.Done = true
	p.Cursor = nil
	p.ShardCursors = nil
	p.SampleBoundaries = nil
}

// EffectiveIntraTableWorkers 计算单表内实际并行 worker 数。intraConfigured<=0 时：取表级 worker 数且封顶 legacyCap；>0 时显式配置，封顶 hardMax。
// legacyCap/hardMax 传入 <=0 时分别回退为 16、64。
func EffectiveIntraTableWorkers(intraConfigured, tableWorkerCount, legacyCap, hardMax int) int { // 计算单表内实际并行worker数量
	if legacyCap < 1 { // 如果遗留封顶值小于1
		legacyCap = 16 // 设置为默认值16
	}
	if hardMax < 1 { // 如果硬封顶值小于1
		hardMax = 64 // 设置为默认值64
	}
	if legacyCap > hardMax { // 如果遗留封顶大于硬封顶
		legacyCap = hardMax // 使用硬封顶
	}
	tw := tableWorkerCount // 获取表级worker数
	if tw < 1 {            // 如果表级worker数小于1
		tw = 1 // 设置为最小值1
	}
	var intra int            // 定义表内worker数
	if intraConfigured > 0 { // 如果显式配置了表内worker数
		intra = intraConfigured // 使用配置值
	} else { // 否则
		intra = tw             // 使用表级worker数
		if intra > legacyCap { // 如果超过遗留封顶
			intra = legacyCap // 使用遗留封顶
		}
	}
	if intra < 1 { // 如果计算值小于1
		intra = 1 // 设置为最小值1
	}
	if intra > hardMax { // 如果超过硬封顶
		intra = hardMax // 使用硬封顶
	}
	return intra // 返回表内worker数
}

// EffectiveIndexRestoreWorkers 计算阶段3索引回放的实际表级并发度。
//   - configured: 任务配置 index_restore_worker_count
//   - workerCount: 任务配置 worker_count（回退基准）
//   - hardMax: 全局硬上限（来自 config.Sync.IndexRestoreHardMax，<=0 用内置 16）
//
// 推导规则：configured<=0 时取 min(workerCount,4)；再受 hardMax 封顶；最低 1。
// 单表内多个索引不并发，仅表间并发。
func EffectiveIndexRestoreWorkers(configured, workerCount, hardMax int) int {
	const defaultCap = 4
	const builtinHardMax = 16

	n := configured
	if n <= 0 {
		n = workerCount
		if n <= 0 {
			n = defaultCap
		}
		if n > defaultCap {
			n = defaultCap
		}
	}
	max := hardMax
	if max <= 0 {
		max = builtinHardMax
	}
	if n > max {
		n = max
	}
	if n < 1 {
		n = 1
	}
	return n
}

// EncryptPasswords 将任务中 SourceDB/TargetDB 的密码加密（存储前调用）
// key 为空时不做任何加密操作
func (t *SyncTask) EncryptPasswords(key string) error {
	if key == "" {
		return nil
	}
	k := crypto.NormalizeKey(key)
	if t.Config.SourceDB != nil && t.Config.SourceDB.Password != "" && !crypto.IsEncrypted(t.Config.SourceDB.Password) {
		enc, err := crypto.Encrypt(t.Config.SourceDB.Password, k)
		if err != nil {
			return err
		}
		t.Config.SourceDB.Password = enc
	}
	if t.Config.TargetDB != nil && t.Config.TargetDB.Password != "" && !crypto.IsEncrypted(t.Config.TargetDB.Password) {
		enc, err := crypto.Encrypt(t.Config.TargetDB.Password, k)
		if err != nil {
			return err
		}
		t.Config.TargetDB.Password = enc
	}
	if err := sink.EncryptSinkSecrets(t.Config.SinkConfigs, key); err != nil {
		return err
	}
	return nil
}

// DecryptPasswords 将任务中 SourceDB/TargetDB 的密码解密（加载后调用）
// key 为空时不做任何解密操作；明文旧数据会兼容返回
func (t *SyncTask) DecryptPasswords(key string) error {
	if key == "" {
		return nil
	}
	k := crypto.NormalizeKey(key)
	if t.Config.SourceDB != nil && t.Config.SourceDB.Password != "" {
		dec, err := crypto.Decrypt(t.Config.SourceDB.Password, k)
		if err != nil {
			return err
		}
		t.Config.SourceDB.Password = dec
	}
	if t.Config.TargetDB != nil && t.Config.TargetDB.Password != "" {
		dec, err := crypto.Decrypt(t.Config.TargetDB.Password, k)
		if err != nil {
			return err
		}
		t.Config.TargetDB.Password = dec
	}
	if err := sink.DecryptSinkSecrets(t.Config.SinkConfigs, key); err != nil {
		return err
	}
	return nil
}

// Checkpoint 位点信息
type Checkpoint struct { // 定义检查点结构体
	TaskID     string    `json:"task_id"`     // 任务ID
	TableName  string    `json:"table_name"`  // 表名
	BinlogFile string    `json:"binlog_file"` // Binlog文件名
	BinlogPos  uint32    `json:"binlog_pos"`  // Binlog位置
	LastUpdate time.Time `json:"last_update"` // 最后更新时间
}

// TableProgressInfo 单表实时同步进度（仅内存，不持久化到任务存档）。
// 用于前端任务详情页展示"当前同步到哪个表、该表进度、速度"等实时信息。
type TableProgressInfo struct {
	Schema        string     `json:"schema"`                 // 源库名
	Table         string     `json:"table"`                  // 表名
	TotalRows     int64      `json:"total_rows"`             // 该表估算总行数
	ProcessedRows int64      `json:"processed_rows"`         // 该表已处理行数
	ProgressPct   float64    `json:"progress_pct"`           // 该表进度百分比 0-100
	SpeedRowsSec  float64    `json:"speed_rows_sec"`         // 该表同步速度（行/秒）
	Status        string     `json:"status"`                 // pending / running / completed / failed
	StartedAt     *time.Time `json:"started_at,omitempty"`   // 该表开始同步时间
	CompletedAt   *time.Time `json:"completed_at,omitempty"` // 该表完成时间
}

// RunningProgress 任务运行时进度快照（仅内存，不持久化）。
// 聚合所有表的进度信息，用于前端实时展示。
type RunningProgress struct {
	CurrentTable    string               `json:"current_table"`    // 当前正在同步的表 "schema.table"
	Tables          []*TableProgressInfo `json:"tables"`           // 所有表的进度列表
	OverallSpeed    float64              `json:"overall_speed"`    // 整体同步速度（行/秒）
	ElapsedSeconds  float64              `json:"elapsed_seconds"`  // 已耗时（秒）
	EstimatedRemain float64              `json:"estimated_remain"` // 预估剩余时间（秒），-1 表示无法估算
	Phase           string               `json:"phase"`            // 当前阶段：full / incremental
	UpdatedAt       time.Time            `json:"updated_at"`       // 最后更新时间
}
