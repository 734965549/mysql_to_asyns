package entity // 声明当前文件属于entity包，用于定义数据实体

import ( // 导入外部包和标准库
	"time" // 导入time包，用于时间处理
)

// TaskStatus 任务状态
type TaskStatus string // 定义任务状态为字符串类型

const ( // 定义常量
	TaskStatusPending   TaskStatus = "PENDING"   // 待执行：任务已创建但未开始
	TaskStatusRunning   TaskStatus = "RUNNING"   // 执行中：任务正在执行
	TaskStatusPaused    TaskStatus = "PAUSED"    // 已暂停：任务被暂停
	TaskStatusCompleted TaskStatus = "COMPLETED" // 已完成：任务执行完成
	TaskStatusFailed    TaskStatus = "FAILED"    // 失败：任务执行失败
)

// SyncMode 同步模式
type SyncMode string // 定义同步模式为字符串类型

const ( // 定义常量
	SyncModeFull        SyncMode = "FULL"        // 全量同步：同步所有数据
	SyncModeIncremental SyncMode = "INCREMENTAL" // 增量同步：只同步变更数据
	SyncModeAll         SyncMode = "ALL"         // 全量+增量：先全量同步后增量同步
)

// SyncLevel 同步级别
type SyncLevel string // 定义同步级别为字符串类型

const ( // 定义常量
	SyncLevelDatabase SyncLevel = "DATABASE" // 库级别同步：同步整个数据库
	SyncLevelTable    SyncLevel = "TABLE"    // 表级别同步：同步指定的表
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
type TaskConfig struct { // 定义任务配置结构体
	StorageID                int64           `json:"storage_id,omitempty"`       // 存储ID
	ID                       string          `json:"id"`                         // 任务ID
	Name                     string          `json:"name"`                       // 任务名称
	SyncLevel                SyncLevel       `json:"sync_level"`                 // 同步级别：DATABASE(全库) 或 TABLE(指定表)
	SourceSchema             string          `json:"source_schema"`              // 源模式名
	TargetSchema             string          `json:"target_schema"`              // 目标模式名
	SourceDatabases          []string        `json:"source_databases"`           // 源数据库列表（库级别/表级别多库时使用）
	TargetDatabase           string          `json:"target_database"`            // 目标数据库（库级别同步时，所有库同步到此库）
	TargetDatabases          []string        `json:"target_databases"`           // 目标数据库列表（与 SourceDatabases 一一对应）
	Tables                   []string        `json:"tables"`                     // 表名列表
	Mode                     SyncMode        `json:"mode"`                       // 同步模式
	BatchSize                int             `json:"batch_size"`                 // 批处理大小
	WorkerCount              int             `json:"worker_count"`               // 工作线程数
	IntraTableWorkerCount    int             `json:"intra_table_worker_count"`   // 单表内并行读/写 goroutine 数；0 表示沿用旧逻辑 min(worker_count,16)
	EnableLimitOne           bool            `json:"enable_limit_one"`           // 无主键表LIMIT 1保护
	OptimizeIndex            bool            `json:"optimize_index"`             // 索引优化：先删除非主键索引，数据迁移完成后再重建
	EnableReadOnly           bool            `json:"enable_read_only"`           // 同步前临时关闭目标库只读，同步后恢复
	EnableConsistentSnapshot bool            `json:"enable_consistent_snapshot"` // 全量阶段使用一致性快照读取（牺牲部分并行度换取一致性）
	SourceDB                 *DatabaseConfig `json:"source_db,omitempty"`        // 源数据库配置（可选，覆盖配置文件）
	TargetDB                 *DatabaseConfig `json:"target_db,omitempty"`        // 目标数据库配置（可选，覆盖配置文件）
}

// ProcessContext 处理上下文
type ProcessContext struct { // 定义处理上下文结构体
	Status          TaskStatus `json:"status"`           // 任务状态
	CurrentPosition string     `json:"current_position"` // 当前位点
	ProgressPercent float64    `json:"progress_percent"` // 进度百分比
	TotalRows       int64      `json:"total_rows"`       // 总行数
	ProcessedRows   int64      `json:"processed_rows"`   // 已处理行数
	StartTime       time.Time  `json:"start_time"`       // 开始时间
	EndTime         time.Time  `json:"end_time"`         // 结束时间
	LastUpdateTime  time.Time  `json:"last_update_time"` // 最后更新时间
	ErrorStack      string     `json:"error_stack"`      // 错误堆栈
}

// SyncTask 同步任务
type SyncTask struct { // 定义同步任务结构体
	Config  TaskConfig     `json:"config"`  // 任务配置
	Context ProcessContext `json:"context"` // 处理上下文
}

// NewSyncTask 创建同步任务函数
func NewSyncTask(config TaskConfig) *SyncTask { // 创建同步任务实例
	return &SyncTask{ // 返回任务实例
		Config: config, // 设置配置
		Context: ProcessContext{ // 初始化上下文
			Status: TaskStatusPending, // 设置初始状态为待执行
		},
	}
}

// Start 启动任务方法
func (t *SyncTask) Start() { // 启动任务
	t.Context.Status = TaskStatusRunning  // 设置状态为执行中
	t.Context.StartTime = time.Now()      // 记录开始时间
	t.Context.LastUpdateTime = time.Now() // 更新最后更新时间
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
	if t.Context.TotalRows > 0 {            // 如果总行数大于0
		t.Context.ProgressPercent = float64(processedRows) / float64(t.Context.TotalRows) * 100 // 计算进度百分比
	}
	t.Context.LastUpdateTime = time.Now() // 更新最后更新时间
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

// Checkpoint 位点信息
type Checkpoint struct { // 定义检查点结构体
	TaskID     string    `json:"task_id"`     // 任务ID
	TableName  string    `json:"table_name"`  // 表名
	BinlogFile string    `json:"binlog_file"` // Binlog文件名
	BinlogPos  uint32    `json:"binlog_pos"`  // Binlog位置
	LastUpdate time.Time `json:"last_update"` // 最后更新时间
}
