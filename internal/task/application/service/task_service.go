// 声明 service 包

package service

import (
	"context" // 上下文管理
	"errors"
	"sort"

	"database/sql"        // 数据库操作
	"database/sql/driver" // 驱动错误（ErrBadConn）

	"encoding/json" // JSON 编解码

	"fmt" // 格式化输出

	"mysql-to-sync/pkg/binlog"
	"mysql-to-sync/pkg/logger" // 日志记录

	"os" // 操作系统接口

	"path/filepath" // 文件路径操作

	"runtime/debug" // 运行时调试信息

	// 排序

	"strconv" // 字符串数字转换

	"strings" // 字符串处理

	"sync" // 并发同步

	"sync/atomic" // 原子操作

	"time" // 时间处理

	cron "github.com/robfig/cron/v3"

	"mysql-to-sync/internal/audit" // 审计日志包

	"mysql-to-sync/internal/checkpoint" // 检查点管理包

	"mysql-to-sync/internal/config" // 配置管理包

	"mysql-to-sync/internal/metadata/domain/entity" // 元数据实体包

	"mysql-to-sync/internal/metadata/domain/service" // 元数据服务包

	"mysql-to-sync/internal/metadata/infrastructure" // 元数据基础设施包

	syncApp "mysql-to-sync/internal/sync/application" // 同步应用包

	"mysql-to-sync/internal/sync/fullload"

	"mysql-to-sync/internal/sync/infrastructure/reader" // 同步读取器包

	"mysql-to-sync/internal/sync/infrastructure/readonly" // 只读管理包

	"mysql-to-sync/internal/sync/infrastructure/writer" // 同步写入器包

	sink "mysql-to-sync/internal/sync/domain/sink" // Sink 领域模型

	taskEntity "mysql-to-sync/internal/task/domain/entity" // 任务实体包

	// MySQL binlog 位置

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/redis/go-redis/v9" // Redis 客户端
)

// tableEntry 全量同步表条目（用于进度追踪初始化）
type tableEntry struct {
	schema   string
	table    string
	identity *entity.TableIdentity
}

// schemaPair 源库 -> 目标库映射。
type schemaPair struct{ src, dst string }

// TaskService 任务服务结构体

// TaskService is the application boundary for task lifecycle and sync
// orchestration.
//
// It owns durable task state, scheduling, per-task runtime isolation, and the
// handoff between full sync and incremental sync. Low-level SQL construction,
// schema inspection, and binlog subscription are delegated to metadata, sync,
// and checkpoint packages.
type TaskService struct {
	mu sync.RWMutex // 读写锁，保护并发访问

	tasks map[string]*taskEntity.SyncTask // 任务映射表，键为任务ID

	runtimes map[string]*taskRuntime // 任务运行时上下文（每任务独立连接）

	// 测试注入点：允许在单测中替换 runtime 初始化逻辑，避免依赖真实数据库

	initRuntimeFn func(task *taskEntity.SyncTask) (*taskRuntime, error)

	// 测试注入点：允许在单测中替换异步执行逻辑，稳定断言 StartTask 并发行为

	executeSyncFn func(ctx context.Context, taskID string, runtime *taskRuntime)

	storage TaskStorage // 任务存储接口

	storageCloser func() error

	sourceDB *sql.DB // 源数据库连接

	targetDB *sql.DB // 目标数据库连接

	analyzer service.IdentityAnalyzer // 身份分析器

	readOnlyManager *readonly.ReadOnlyManager // 只读管理器

	enableReadOnly bool // 是否启用只读限制

	checkpointManager checkpoint.Manager // 位点管理器

	checkpointCloser func() error

	incrementalSyncs map[string]*syncApp.IncrementalSyncService // 增量同步服务映射

	config *config.Config // 配置对象

	auditLogger *audit.AuditLogger // 审计日志器

	schedulerMu sync.Mutex // 保护调度器启停状态

	schedulerStop chan struct{} // 定时调度器停止信号

	// 运行时进度追踪（仅内存，不持久化）
	runningProgress map[string]*taskEntity.RunningProgress // 任务ID -> 运行时进度
	progressMu      sync.RWMutex                           // 保护 runningProgress 的读写

	// 进度持久化节流：记录每个任务上次落盘时间，避免每批都写存储
	lastProgressPersist map[string]time.Time

	// 行数对比后台任务追踪：每个任务最多一个核对 goroutine。
	// cancelCancels 保存对比 context 的 cancel；comparisonWgs 等待 goroutine 退出。
	// 删除任务 / 关闭服务时先 cancel 并 wg.Wait，避免后台 goroutine 写入已关闭的存储。
	comparisonCancels map[string]context.CancelFunc
	comparisonWgs     map[string]*sync.WaitGroup
	comparisonMu      sync.Mutex // 保护 comparisonCancels / comparisonWgs

	eventRecorder    *TaskEventRecorder
	eventStoreCloser func() error
	pruneStop        chan struct{}
}

// taskRuntime contains resources that must not be shared across running tasks.
//
// Each StartTask call creates an isolated runtime so one task's DB pools,
// analyzer, read-only manager, or cancellation path cannot affect another task.
type taskRuntime struct {
	sourceDB *sql.DB

	targetDB *sql.DB

	analyzer service.IdentityAnalyzer

	readOnlyManager *readonly.ReadOnlyManager

	cancel context.CancelFunc

	// executionID 标识单次 StartTask→结束 的运行轮次，与 FullLoadRunID（V2 staging 恢复）职责分离。
	executionID string
}

// pendingIndexRestore records indexes removed for full-sync bulk loading.
// Entries are accumulated across all database pairs and restored only after
// every table has finished copying data, preventing index builds from
// competing with still-running INSERT workloads.
type pendingIndexRestore struct {
	targetSchema string
	targetTable  string
	indexes      []map[string]interface{}
}

func (r *taskRuntime) Close() {

	if r == nil {

		return

	}

	if r.cancel != nil {

		r.cancel()

	}

	if r.sourceDB != nil {

		r.sourceDB.Close()

	}

	if r.targetDB != nil && r.targetDB != r.sourceDB {

		r.targetDB.Close()

	}

}

type sourceQueryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// TaskStorage 任务存储接口

// TaskStorage persists SyncTask archives.
//
// Implementations must preserve plaintext passwords in memory while encrypting
// only the serialized form when an encryption key is configured.
type TaskStorage interface {
	Save(task *taskEntity.SyncTask) error // 保存任务

	Delete(taskID string) error // 删除任务

	LoadAll() ([]*taskEntity.SyncTask, error) // 加载所有任务

	QueryTasksPage(page, pageSize int, status, keyword, sortBy string) ([]*taskEntity.SyncTask, int, int, int, error) // 分页查询任务

}

// MySQLTaskStorage MySQL 任务存储结构体

type MySQLTaskStorage struct {
	db *sql.DB // 数据库连接

	mu sync.RWMutex // 读写锁，保护并发访问

	encryptKey string // 密码加密密钥（为空则不加密）

}

// NewMySQLTaskStorage 创建 MySQL 任务存储（dsn 不含数据库名，dbName 为目标库名）

func NewMySQLTaskStorage(db *sql.DB, encryptKey string) *MySQLTaskStorage {

	s := &MySQLTaskStorage{db: db, encryptKey: encryptKey} // 创建存储实例

	if err := s.initTable(); err != nil { // 初始化数据表

		logger.Warn("Warning: failed to initialize task storage table: %v", err) // 打印警告日志

	}

	return s // 返回存储实例

}

// NewMySQLTaskStorageFromConfig 通过配置创建 MySQL 任务存储，自动建库建表

func NewMySQLTaskStorageFromConfig(cfg *config.StorageConfig, encryptKey string) (*MySQLTaskStorage, error) {

	// 先用不带数据库名的 DSN 连接，创建数据库

	noDB := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",

		cfg.Username, cfg.Password, cfg.Host, cfg.Port) // 构建无数据库名的DSN

	tmpDB, err := sql.Open("mysql", noDB) // 打开临时数据库连接

	if err != nil {

		return nil, fmt.Errorf("failed to open mysql: %w", err) // 返回错误

	}

	if _, err = tmpDB.Exec(fmt.Sprintf(

		"CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", cfg.Database)); err != nil { // 创建数据库

		tmpDB.Close() // 关闭临时连接

		return nil, fmt.Errorf("failed to create database %s: %w", cfg.Database, err) // 返回错误

	}

	tmpDB.Close() // 关闭临时连接

	// 再连接到目标数据库

	dsn := cfg.GetDSN() // 获取完整的DSN

	db, err := sql.Open("mysql", dsn) // 打开数据库连接

	if err != nil {

		return nil, fmt.Errorf("failed to open storage database: %w", err) // 返回错误

	}

	if err = db.Ping(); err != nil { // 测试连接

		db.Close() // 关闭连接

		return nil, fmt.Errorf("failed to ping storage database: %w", err) // 返回错误

	}

	return NewMySQLTaskStorage(db, encryptKey), nil // 创建并返回存储实例

}

func (s *MySQLTaskStorage) initTable() error { // 初始化数据表

	// 定义建表SQL语句

	query := `

	CREATE TABLE IF NOT EXISTS sys_sync_tasks (

		pk_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

		id VARCHAR(64) NOT NULL,

		name VARCHAR(255),

		content JSON,

		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

		PRIMARY KEY (pk_id),

		UNIQUE KEY uk_task_id (id)

	)`

	if _, err := s.db.Exec(query); err != nil { // 执行建表语句

		return err // 返回错误

	}

	// 检查并添加 pk_id 列（如果不存在）

	var pkIDExists int

	err := s.db.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE table_schema = DATABASE() AND table_name = 'sys_sync_tasks' AND column_name = 'pk_id'").Scan(&pkIDExists)

	if err == nil && pkIDExists == 0 {

		if _, err := s.db.Exec("ALTER TABLE sys_sync_tasks ADD COLUMN pk_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST"); err != nil {

			logger.Warn("Warning: failed to add pk_id column: %v", err) // 打印警告日志

		}

	}

	// 检查并添加唯一索引（如果不存在）

	var indexExists int

	err = s.db.QueryRow("SELECT COUNT(*) FROM information_schema.STATISTICS WHERE table_schema = DATABASE() AND table_name = 'sys_sync_tasks' AND index_name = 'uk_task_id'").Scan(&indexExists)

	if err == nil && indexExists == 0 {

		if _, err := s.db.Exec("ALTER TABLE sys_sync_tasks ADD UNIQUE KEY uk_task_id (id)"); err != nil {

			logger.Warn("Warning: failed to add uk_task_id index: %v", err) // 打印警告日志

		}

	}

	return nil

}

// loadStoredTaskLocked 读取已持久化的任务存档；调用方需已持有 storage 写锁。
func loadStoredTaskFileLocked(dataDir, taskID string) (*taskEntity.SyncTask, error) {
	filePath := filepath.Join(dataDir, taskID+".json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var stored taskEntity.SyncTask
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stored task %s: %w", taskID, err)
	}
	return &stored, nil
}

// Save 保存任务到数据库

func (s *MySQLTaskStorage) Save(task *taskEntity.SyncTask) error {

	s.mu.Lock() // 获取写锁

	defer s.mu.Unlock() // 延迟释放写锁

	// 加密密码：先备份明文，加密后序列化，再还原明文（避免污染内存中的任务对象）
	var origSourcePwd, origTargetPwd string
	if task.Config.SourceDB != nil {
		origSourcePwd = task.Config.SourceDB.Password
	}
	if task.Config.TargetDB != nil {
		origTargetPwd = task.Config.TargetDB.Password
	}
	origSinkConfigs := sink.CloneConfigs(task.Config.SinkConfigs)
	defer func() { // 还原内存中的明文密码和 sink 密钥
		if task.Config.SourceDB != nil {
			task.Config.SourceDB.Password = origSourcePwd
		}
		if task.Config.TargetDB != nil {
			task.Config.TargetDB.Password = origTargetPwd
		}
		task.Config.SinkConfigs = origSinkConfigs
	}()
	if err := task.EncryptPasswords(s.encryptKey); err != nil {
		return fmt.Errorf("encrypt passwords: %w", err)
	}

	var existingContent []byte
	loadErr := s.db.QueryRow("SELECT content FROM sys_sync_tasks WHERE id = ?", task.Config.ID).Scan(&existingContent)
	if loadErr == nil {
		var stored taskEntity.SyncTask
		if err := json.Unmarshal(existingContent, &stored); err != nil {
			return fmt.Errorf("unmarshal stored task %s: %w", task.Config.ID, err)
		}
		if taskEntity.ShouldRejectArchiveOverwrite(&stored, task) {
			return nil
		}
	} else if loadErr != sql.ErrNoRows {
		return loadErr
	}

	data, err := json.Marshal(task) // 序列化任务

	if err != nil {

		return err // 返回错误

	}

	// 先尝试 UPDATE，命中则不消耗自增值；未命中再 INSERT
	updRes, updErr := s.db.Exec("UPDATE sys_sync_tasks SET name = ?, content = ? WHERE id = ?", task.Config.Name, data, task.Config.ID)
	if updErr != nil {
		return updErr
	}
	rowsAffected, _ := updRes.RowsAffected()
	if rowsAffected > 0 {
		// UPDATE 命中，查询已有的 pk_id
		if err := s.db.QueryRow("SELECT pk_id FROM sys_sync_tasks WHERE id = ?", task.Config.ID).Scan(&task.Config.StorageID); err != nil {
			logger.Warn("Warning: failed to query pk_id after update: %v", err)
		}
		return nil
	}

	// 不存在则插入
	res, err := s.db.Exec("INSERT INTO sys_sync_tasks (id, name, content) VALUES (?, ?, ?)", task.Config.ID, task.Config.Name, data)
	if err != nil {
		return err
	}
	if storageID, idErr := res.LastInsertId(); idErr == nil && storageID > 0 {
		task.Config.StorageID = storageID
	}

	return nil // 返回成功

}

// Delete 从数据库删除任务

func (s *MySQLTaskStorage) Delete(taskID string) error {

	s.mu.Lock() // 获取写锁

	defer s.mu.Unlock() // 延迟释放写锁

	query := "DELETE FROM sys_sync_tasks WHERE id = ?" // 构建删除SQL

	_, err := s.db.Exec(query, taskID) // 执行删除

	return err // 返回结果

}

func (s *MySQLTaskStorage) buildTaskSortClause(sortBy string) string {
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "created_at_asc":
		return "ORDER BY created_at ASC, pk_id ASC"
	case "name_asc":
		return "ORDER BY name ASC, pk_id ASC"
	case "name_desc":
		return "ORDER BY name DESC, pk_id DESC"
	case "status_asc":
		return "ORDER BY JSON_UNQUOTE(JSON_EXTRACT(content, '$.context.status')) ASC, pk_id ASC"
	case "status_desc":
		return "ORDER BY JSON_UNQUOTE(JSON_EXTRACT(content, '$.context.status')) DESC, pk_id DESC"
	case "progress_asc":
		return "ORDER BY CAST(JSON_UNQUOTE(JSON_EXTRACT(content, '$.context.progress_percent')) AS DECIMAL(10,2)) ASC, pk_id ASC"
	case "progress_desc":
		return "ORDER BY CAST(JSON_UNQUOTE(JSON_EXTRACT(content, '$.context.progress_percent')) AS DECIMAL(10,2)) DESC, pk_id DESC"
	default:
		return "ORDER BY created_at DESC, pk_id DESC"
	}
}

func (s *MySQLTaskStorage) buildTaskWhereClause(status, keyword string) (string, []interface{}) {
	clauses := make([]string, 0, 4)
	args := make([]interface{}, 0, 4)

	if status = strings.TrimSpace(status); status != "" {
		clauses = append(clauses, "UPPER(JSON_UNQUOTE(JSON_EXTRACT(content, '$.context.status'))) = UPPER(?)")
		args = append(args, status)
	}

	if keyword = strings.TrimSpace(keyword); keyword != "" {
		like := "%" + keyword + "%"
		clauses = append(clauses, "(LOWER(JSON_UNQUOTE(JSON_EXTRACT(content, '$.config.name'))) LIKE LOWER(?) OR LOWER(JSON_UNQUOTE(JSON_EXTRACT(content, '$.config.id'))) LIKE LOWER(?) OR LOWER(JSON_UNQUOTE(JSON_EXTRACT(content, '$.config.source_schema'))) LIKE LOWER(?) OR LOWER(JSON_UNQUOTE(JSON_EXTRACT(content, '$.config.target_schema'))) LIKE LOWER(?) OR JSON_SEARCH(content, 'one', ?, NULL, '$.config.tables[*]') IS NOT NULL)")
		args = append(args, like, like, like, like, like)
	}

	if len(clauses) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func (s *MySQLTaskStorage) QueryTasksPage(page, pageSize int, status, keyword, sortBy string) ([]*taskEntity.SyncTask, int, int, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if pageSize <= 0 {
		pageSize = 10
	}
	if page <= 0 {
		page = 1
	}

	whereClause, args := s.buildTaskWhereClause(status, keyword)
	countSQL := "SELECT COUNT(*) FROM sys_sync_tasks " + whereClause

	var total int
	if err := s.db.QueryRow(countSQL, args...).Scan(&total); err != nil {
		return nil, 0, page, pageSize, err
	}

	orderBy := s.buildTaskSortClause(sortBy)
	query := "SELECT pk_id, content FROM sys_sync_tasks " + whereClause + " " + orderBy + " LIMIT ? OFFSET ?"
	queryArgs := append(append([]interface{}{}, args...), pageSize, (page-1)*pageSize)

	rows, err := s.db.Query(query, queryArgs...)
	if err != nil {
		return nil, 0, page, pageSize, err
	}
	defer rows.Close()

	items := make([]*taskEntity.SyncTask, 0, pageSize)
	for rows.Next() {
		var storageID int64
		var data []byte
		if err := rows.Scan(&storageID, &data); err != nil {
			continue
		}
		var task taskEntity.SyncTask
		if err := json.Unmarshal(data, &task); err != nil {
			continue
		}
		task.Config.StorageID = storageID
		if decErr := task.DecryptPasswords(s.encryptKey); decErr != nil {
			logger.Warn("Warning: failed to decrypt task passwords: %v", decErr)
		}
		items = append(items, &task)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, page, pageSize, err
	}
	return items, total, page, pageSize, nil
}

// LoadAll 从数据库加载所有任务

func (s *MySQLTaskStorage) LoadAll() ([]*taskEntity.SyncTask, error) {
	tasks, _, _, _, err := s.QueryTasksPage(1, 1000000, "", "", "created_at_desc")
	return tasks, err
}

func newTaskStorageFromConfig(cfg *config.Config) (TaskStorage, func() error, string, error) {

	encryptKey := ""

	if cfg != nil {

		encryptKey = cfg.Security.EncryptKey

	}

	if cfg != nil && cfg.Storage.Mode == "mysql" {

		storage, err := NewMySQLTaskStorageFromConfig(&cfg.Storage, encryptKey)

		if err != nil {

			return nil, nil, "", err

		}

		return storage, storage.db.Close, "mysql", nil

	}

	dataDir := "data"

	if cfg != nil && cfg.Storage.DataDir != "" {

		dataDir = cfg.Storage.DataDir

	}

	return NewFileTaskStorage(dataDir, encryptKey), nil, "file", nil

}

func newCheckpointManagerFromConfig(cfg *config.Config) (checkpoint.Manager, func() error, string, error) {

	if cfg != nil && cfg.Redis.Host != "" {

		rdb := redis.NewClient(&redis.Options{

			Addr: fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),

			Password: cfg.Redis.Password,

			DB: cfg.Redis.DB,
		})

		return checkpoint.NewRedisCheckpointManager(rdb, "dts:checkpoint"), rdb.Close, "redis", nil

	}

	return checkpoint.NewMemoryCheckpointManager(), nil, "memory", nil

}

func closeResource(closer func() error, resourceName string) {

	if closer == nil {

		return

	}

	if err := closer(); err != nil {

		logger.Warn("Warning: failed to close %s: %v", resourceName, err)

	}

}

// NewTaskService 创建任务服务（启动时不依赖数据库）

func NewTaskService(cfg *config.Config) *TaskService {

	ts := &TaskService{

		tasks: make(map[string]*taskEntity.SyncTask), // 初始化任务映射

		runtimes: make(map[string]*taskRuntime), // 初始化任务运行时映射

		incrementalSyncs: make(map[string]*syncApp.IncrementalSyncService), // 初始化增量同步映射

		runningProgress: make(map[string]*taskEntity.RunningProgress), // 初始化运行时进度映射

		lastProgressPersist: make(map[string]time.Time), // 初始化进度持久化节流

		comparisonCancels: make(map[string]context.CancelFunc), // 初始化行数对比 cancel 映射

		comparisonWgs: make(map[string]*sync.WaitGroup), // 初始化行数对比 wait group 映射

		config: cfg, // 设置配置

		auditLogger: audit.NewAuditLogger("logs/audit"), // 创建审计日志器

	}

	// 初始化存储后端

	storage, storageCloser, storageType, err := newTaskStorageFromConfig(cfg)

	if err != nil {

		logger.Warn("Warning: failed to initialize storage: %v, falling back to file storage", err)

		storage, storageCloser, storageType, err = newTaskStorageFromConfig(&config.Config{Storage: config.StorageConfig{Mode: "file"}})

		if err != nil {

			logger.Fatal("%v", err)

		}

	}

	ts.storage = storage
	ts.storageCloser = storageCloser
	logger.Info("Using %s task storage", storageType)

	initTaskEventInfrastructure(ts, cfg)

	// 初始化位点管理器

	checkpointManager, checkpointCloser, checkpointType, err := newCheckpointManagerFromConfig(cfg)

	if err != nil {

		logger.Warn("Warning: failed to initialize checkpoint manager: %v, falling back to memory checkpoint manager", err)

		checkpointManager, checkpointCloser, checkpointType, err = newCheckpointManagerFromConfig(nil)

		if err != nil {

			logger.Fatal("%v", err)

		}

	}

	ts.checkpointManager = checkpointManager
	ts.checkpointCloser = checkpointCloser
	logger.Info("Using %s checkpoint manager", checkpointType)

	// 加载已保存的任务

	ts.loadTasks() // 加载任务

	// P3.5: 生产入口也必须执行 V2 状态恢复（此前仅挂在 NewTaskServiceWithDBAndConfig）。
	ts.recoverFullLoadV2States()
	// 崩溃遗留 staging 的精确清理挂在 executeFullSync：此时才有任务级 targetDB。
	// NewTaskService 构造期无全局目标连接，不能在此 DROP。

	// 启动定时调度器
	ts.StartScheduler()

	return ts // 返回服务实例

}

// NewTaskServiceWithDB 创建带数据库连接的任务服务

func NewTaskServiceWithDB(sourceDB, targetDB *sql.DB, analyzer service.IdentityAnalyzer) *TaskService {

	ts := &TaskService{

		tasks: make(map[string]*taskEntity.SyncTask), // 初始化任务映射

		runtimes: make(map[string]*taskRuntime), // 初始化任务运行时映射

		storage: NewFileTaskStorage("data"), // 使用文件存储

		sourceDB: sourceDB, // 设置源数据库

		targetDB: targetDB, // 设置目标数据库

		analyzer: analyzer, // 设置分析器

		enableReadOnly: true, // 默认启用只读限制

		incrementalSyncs: make(map[string]*syncApp.IncrementalSyncService), // 初始化增量同步映射

		runningProgress: make(map[string]*taskEntity.RunningProgress), // 初始化运行时进度映射

		lastProgressPersist: make(map[string]time.Time), // 初始化进度持久化节流

		comparisonCancels: make(map[string]context.CancelFunc), // 初始化行数对比 cancel 映射

		comparisonWgs: make(map[string]*sync.WaitGroup), // 初始化行数对比 wait group 映射

		auditLogger: audit.NewAuditLogger("logs/audit"), // 创建审计日志器

	}

	// 初始化只读管理器

	ts.readOnlyManager = readonly.NewReadOnlyManager(targetDB) // 创建只读管理器

	// 初始化位点管理器（默认使用内存）

	ts.checkpointManager = checkpoint.NewMemoryCheckpointManager() // 创建内存检查点管理器
	ts.checkpointCloser = nil

	ts.loadTasks() // 加载任务

	// 启动定时调度器
	ts.StartScheduler()

	return ts // 返回服务实例

}

// NewTaskServiceWithDBAndConfig 创建带数据库连接和配置的任务服务

func NewTaskServiceWithDBAndConfig(sourceDB, targetDB *sql.DB, analyzer service.IdentityAnalyzer, cfg *config.Config) *TaskService {

	ts := &TaskService{

		tasks: make(map[string]*taskEntity.SyncTask),

		runtimes: make(map[string]*taskRuntime),

		sourceDB: sourceDB,

		targetDB: targetDB,

		analyzer: analyzer,

		enableReadOnly: true, // 默认启用只读限制

		incrementalSyncs: make(map[string]*syncApp.IncrementalSyncService),

		runningProgress: make(map[string]*taskEntity.RunningProgress),

		lastProgressPersist: make(map[string]time.Time),

		comparisonCancels: make(map[string]context.CancelFunc),

		comparisonWgs: make(map[string]*sync.WaitGroup),

		config: cfg,

		auditLogger: audit.NewAuditLogger("logs/audit"),
	}

	// 初始化存储后端

	storage, storageCloser, storageType, err := newTaskStorageFromConfig(cfg)

	if err != nil {

		logger.Warn("Warning: failed to initialize storage: %v, falling back to file storage", err)

		storage, storageCloser, storageType, err = newTaskStorageFromConfig(&config.Config{Storage: config.StorageConfig{Mode: "file"}})

		if err != nil {

			logger.Fatal("%v", err)

		}

	}

	ts.storage = storage
	ts.storageCloser = storageCloser
	logger.Info("Using %s task storage", storageType)

	// 初始化只读管理器

	ts.readOnlyManager = readonly.NewReadOnlyManager(targetDB)

	// 初始化位点管理器

	checkpointManager, checkpointCloser, checkpointType, err := newCheckpointManagerFromConfig(cfg)

	if err != nil {

		logger.Warn("Warning: failed to initialize checkpoint manager: %v, falling back to memory checkpoint manager", err)

		checkpointManager, checkpointCloser, checkpointType, err = newCheckpointManagerFromConfig(nil)

		if err != nil {

			logger.Fatal("%v", err)

		}

	}

	ts.checkpointManager = checkpointManager
	ts.checkpointCloser = checkpointCloser
	logger.Info("Using %s checkpoint manager", checkpointType)

	ts.loadTasks()

	// P3.5: 扫描 V2 任务的持久化状态,标记需要恢复的任务。
	// 不自动启动恢复;只修正状态:已全量完成的标记为可接增量,未完成的保持 Paused 等待手动启动。
	ts.recoverFullLoadV2States()

	// P3.1: 测试/注入入口在构造期已有共享 targetDB，可立即按清单清理。
	// 生产 NewTaskService 路径无全局 targetDB，改在 executeFullSync 任务建连后清理。
	if ts.targetDB != nil {
		ts.cleanupAllStaleStagingTables(context.Background(), ts.targetDB)
	}

	// 启动定时调度器
	ts.StartScheduler()

	return ts

}

func syncTuneFrom(cfg *config.Config) *config.SyncTuneConfig {

	if cfg == nil {

		return nil

	}

	return &cfg.Sync

}

func (s *TaskService) intraTableConcurrencyCaps() (legacyCap, hardMax int) {

	legacyCap, hardMax = 16, 64

	if s.config == nil {

		return

	}

	t := s.config.Sync

	if t.IntraTableLegacyCap > 0 {

		legacyCap = t.IntraTableLegacyCap

	}

	if t.IntraTableHardMax > 0 {

		hardMax = t.IntraTableHardMax

	}

	return

}

// SetEnableReadOnly 设置是否启用只读限制

func (s *TaskService) SetEnableReadOnly(enable bool) {

	s.mu.Lock() // 获取写锁

	defer s.mu.Unlock() // 延迟释放写锁

	s.enableReadOnly = enable // 设置只读状态

}

// GetEnableReadOnly 获取是否启用只读限制

func (s *TaskService) GetEnableReadOnly() bool {

	s.mu.RLock() // 获取读锁

	defer s.mu.RUnlock() // 延迟释放读锁

	return s.enableReadOnly // 返回只读状态

}

// loadTasks 从存储加载任务

func (s *TaskService) loadTasks() {

	tasks, err := s.storage.LoadAll() // 加载所有任务

	if err != nil {

		fmt.Printf("加载任务失败: %v\n", err) // 打印错误信息

		return // 返回

	}

	for _, task := range tasks { // 遍历任务列表

		// 行数对比恢复：服务重启时若存档仍为 CHECKING，说明后台核对被中断，
		// 改为 FAILED 并注明原因。后台 goroutine 已随进程退出，不会继续写入。
		if task.Context.RowCountComparison != nil &&
			task.Context.RowCountComparison.Status == taskEntity.RowCountComparisonChecking {
			now := time.Now()
			task.Context.RowCountComparison.Status = taskEntity.RowCountComparisonFailed
			task.Context.RowCountComparison.CompletedAt = &now
			task.Context.RowCountComparison.FailureReason = "服务重启导致核对中断"
			if saveErr := s.storage.Save(task); saveErr != nil {
				logger.Warn("[Task %s] failed to persist row-count comparison recovery: %v", task.Config.ID, saveErr)
			}
		}

		s.tasks[task.Config.ID] = task // 添加到任务映射

	}

}

// initFullLoadV2Manifest 在全量开始前持久化完整预期表清单（全部 PENDING）与 runID。
func (s *TaskService) initFullLoadV2Manifest(taskID, runID string, tableKeys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}
	task.InitFullLoadV2Manifest(runID, tableKeys)
	if err := s.storage.Save(task); err != nil {
		return fmt.Errorf("save V2 manifest: %w", err)
	}
	return nil
}

// recoverFullLoadV2States 扫描所有 V2 任务的持久化表级状态,进行恢复决策(P3.5)。
//
// 恢复策略:
//   - FULL 且全部表 PUBLISHED: 标记全量完成(SyncPhaseFullCompleted),允许接增量
//   - ALL 且全部表 PUBLISHED: 仅表示基线完成，保留阶段供 catch-up/索引 resume，不得标完成
//   - 有表未完成: 保持当前状态(Paused),用户手动 StartTask 时 V2 引擎会跳过 PUBLISHED 表、重新处理未完成表
//   - 无 V2 状态: 跳过(非 V2 任务或全新任务)
//
// 注意:此方法不自动启动恢复,只修正状态。实际恢复在用户手动 StartTask 时由 V2 引擎执行。
func (s *TaskService) recoverFullLoadV2States() {
	s.mu.Lock()
	defer s.mu.Unlock()

	recovered := 0
	for taskID, task := range s.tasks {
		if task == nil || !task.Config.UsesFullLoadV2() {
			continue
		}
		if len(task.Context.FullLoadV2States) == 0 {
			continue
		}

		if !task.AllFullLoadV2TablesPublished() {
			published := 0
			pending := 0
			for _, st := range task.Context.FullLoadV2States {
				if st != nil && st.Phase == "PUBLISHED" {
					published++
				} else {
					pending++
				}
			}
			logger.Info("[Task %s] recoverFullLoadV2: %d published, %d pending; task remains paused for manual restart",
				taskID, published, pending)
			continue
		}

		// ALL：全部 PUBLISHED 只表示基线完成，catch-up/索引恢复仍可能未完成，不得标 FULL_COMPLETED。
		if task.Config.Mode == taskEntity.SyncModeAll {
			logger.Info("[Task %s] recoverFullLoadV2: all tables PUBLISHED but ALL post-baseline may be pending (subphase=%q); keep phase for resume",
				taskID, task.Context.FullSyncSubphase)
			continue
		}

		// FULL：全部 PUBLISHED 即全量完成。
		if task.Context.SyncPhase == taskEntity.SyncPhaseFullStarted ||
			task.Context.SyncPhase == taskEntity.SyncPhaseFullFailed {
			now := time.Now()
			task.Context.SyncPhase = taskEntity.SyncPhaseFullCompleted
			task.Context.FullSyncCompletedAt = &now
			task.Context.FullSyncFailedReason = ""
			if err := s.storage.Save(task); err != nil {
				logger.Warn("[Task %s] recoverFullLoadV2: failed to save recovered state: %v", taskID, err)
			} else {
				logger.Info("[Task %s] recoverFullLoadV2: all tables PUBLISHED, marked full sync completed", taskID)
				recovered++
			}
		}
	}
	if recovered > 0 {
		logger.Info("[TaskService] recoverFullLoadV2States: recovered %d tasks to full sync completed", recovered)
	}
}

// CreateTask 创建任务

func (s *TaskService) CreateTask(config taskEntity.TaskConfig) (*taskEntity.SyncTask, error) {

	if err := config.ValidateFullLoadOptions(); err != nil {
		return nil, err
	}

	s.mu.Lock() // 获取写锁

	defer s.mu.Unlock() // 延迟释放写锁

	task := taskEntity.NewSyncTask(config) // 创建同步任务
	if config.AllowNopkAll {
		task.AcknowledgeNopkAllRisk(time.Now())
	}

	s.tasks[config.ID] = task // 添加到任务映射

	// 保存到存储

	if err := s.storage.Save(task); err != nil {

		fmt.Printf("保存任务失败: %v\n", err)

	}

	return task, nil // 返回任务和成功状态

}

// GetTask 获取任务

func (s *TaskService) GetTask(taskID string) (*taskEntity.SyncTask, bool) {

	s.mu.RLock()

	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]

	return task, exists

}

// GetTaskSnapshot 在锁内深拷贝任务后返回，供 API 序列化，避免与后台行数核对等并发写共享 live 对象。
func (s *TaskService) GetTaskSnapshot(taskID string) (*taskEntity.SyncTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]
	if !exists || task == nil {
		return nil, false
	}
	return task.CloneForRead(), true
}

// GetAllTasks 返回所有任务的只读快照，供路由指标等在释放锁后安全读取 Status，
// 避免与后台进度更新共享 live 指针产生数据竞争。内部修改请继续用 GetTask。
func (s *TaskService) GetAllTasks() []*taskEntity.SyncTask {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		if task == nil {
			continue
		}
		tasks = append(tasks, task.CloneForRead())
	}
	return tasks
}

// GetTasksPage 获取分页任务列表
func taskStatusRank(status taskEntity.TaskStatus) int {
	switch status {
	case taskEntity.TaskStatusPending:
		return 0
	case taskEntity.TaskStatusScheduled:
		return 1
	case taskEntity.TaskStatusRunning:
		return 2
	case taskEntity.TaskStatusPaused:
		return 3
	case taskEntity.TaskStatusFailed:
		return 4
	case taskEntity.TaskStatusCompleted:
		return 5
	case taskEntity.TaskStatusStopped:
		return 6
	default:
		return 7
	}
}

func taskDisplayTime(task *taskEntity.SyncTask) time.Time {
	if task == nil {
		return time.Time{}
	}
	if !task.Context.CreatedAt.IsZero() {
		return task.Context.CreatedAt
	}
	if !task.Context.LastUpdateTime.IsZero() {
		return task.Context.LastUpdateTime
	}
	if !task.Context.StartTime.IsZero() {
		return task.Context.StartTime
	}
	return time.Time{}
}

func taskStorageOrderKey(task *taskEntity.SyncTask) int64 {
	if task == nil {
		return 0
	}
	if task.Config.StorageID > 0 {
		return task.Config.StorageID
	}
	if !task.Context.CreatedAt.IsZero() {
		return task.Context.CreatedAt.UnixNano()
	}
	if !task.Context.LastUpdateTime.IsZero() {
		return task.Context.LastUpdateTime.UnixNano()
	}
	if !task.Context.StartTime.IsZero() {
		return task.Context.StartTime.UnixNano()
	}
	return 0
}

func (s *TaskService) GetTasksPage(page, pageSize int, status, keyword, sortBy string) ([]*taskEntity.SyncTask, int, int, int) {
	s.mu.RLock()
	storage := s.storage
	s.mu.RUnlock()

	if storage == nil {
		return []*taskEntity.SyncTask{}, 0, page, pageSize
	}

	items, total, page, pageSize, err := storage.QueryTasksPage(page, pageSize, status, keyword, sortBy)
	if err != nil {
		logger.Warn("[TaskService] QueryTasksPage failed, falling back to in-memory scan: %v", err)
		return s.getTasksPageFromMemory(page, pageSize, status, keyword, sortBy)
	}
	return items, total, page, pageSize
}

func (s *TaskService) getTasksPageFromMemory(page, pageSize int, status, keyword, sortBy string) ([]*taskEntity.SyncTask, int, int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if pageSize <= 0 {
		pageSize = 10
	}
	if page <= 0 {
		page = 1
	}

	status = strings.ToUpper(strings.TrimSpace(status))
	keyword = strings.ToLower(strings.TrimSpace(keyword))
	sortBy = strings.ToLower(strings.TrimSpace(sortBy))

	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		if task == nil {
			continue
		}
		if status != "" && strings.ToUpper(string(task.Context.Status)) != status {
			continue
		}
		if keyword != "" {
			name := strings.ToLower(strings.TrimSpace(task.Config.Name))
			id := strings.ToLower(strings.TrimSpace(task.Config.ID))
			sourceSchema := strings.ToLower(strings.TrimSpace(task.Config.SourceSchema))
			targetSchema := strings.ToLower(strings.TrimSpace(task.Config.TargetSchema))
			matched := strings.Contains(name, keyword) || strings.Contains(id, keyword) || strings.Contains(sourceSchema, keyword) || strings.Contains(targetSchema, keyword)
			if !matched {
				for _, tableName := range task.Config.Tables {
					if strings.Contains(strings.ToLower(tableName), keyword) {
						matched = true
						break
					}
				}
			}
			if !matched {
				continue
			}
		}
		tasks = append(tasks, task.CloneForRead())
	}

	sort.Slice(tasks, func(i, j int) bool {
		a := tasks[i]
		b := tasks[j]
		createdAtA := taskDisplayTime(a)
		createdAtB := taskDisplayTime(b)
		switch sortBy {
		case "created_at_asc":
			if createdAtA.Equal(createdAtB) {
				return a.Config.ID > b.Config.ID
			}
			return createdAtA.Before(createdAtB)
		case "name_asc":
			if a.Config.Name == b.Config.Name {
				return a.Config.ID < b.Config.ID
			}
			return a.Config.Name < b.Config.Name
		case "name_desc":
			if a.Config.Name == b.Config.Name {
				return a.Config.ID > b.Config.ID
			}
			return a.Config.Name > b.Config.Name
		case "status_asc":
			aStatus, bStatus := taskStatusRank(a.Context.Status), taskStatusRank(b.Context.Status)
			if aStatus == bStatus {
				if createdAtA.Equal(createdAtB) {
					return a.Config.ID > b.Config.ID
				}
				return createdAtA.After(createdAtB)
			}
			return aStatus < bStatus
		case "status_desc":
			aStatus, bStatus := taskStatusRank(a.Context.Status), taskStatusRank(b.Context.Status)
			if aStatus == bStatus {
				if createdAtA.Equal(createdAtB) {
					return a.Config.ID > b.Config.ID
				}
				return createdAtA.After(createdAtB)
			}
			return aStatus > bStatus
		case "progress_asc":
			if a.Context.ProgressPercent == b.Context.ProgressPercent {
				if createdAtA.Equal(createdAtB) {
					return a.Config.ID > b.Config.ID
				}
				return createdAtA.After(createdAtB)
			}
			return a.Context.ProgressPercent < b.Context.ProgressPercent
		case "progress_desc":
			if a.Context.ProgressPercent == b.Context.ProgressPercent {
				if createdAtA.Equal(createdAtB) {
					return a.Config.ID > b.Config.ID
				}
				return createdAtA.After(createdAtB)
			}
			return a.Context.ProgressPercent > b.Context.ProgressPercent
		default:
			if createdAtA.Equal(createdAtB) {
				return a.Config.ID > b.Config.ID
			}
			return createdAtA.After(createdAtB)
		}
	})

	total := len(tasks)
	start := (page - 1) * pageSize
	if start >= total {
		return []*taskEntity.SyncTask{}, total, page, pageSize
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return tasks[start:end], total, page, pageSize
}

// UpdateTask 更新任务配置。写锁内复核 RUNNING/STOPPED，并将传入对象的 Config 合并到
// 内存中的 live 任务，保留 Context（进度、行数核对等），避免 API 侧快照覆盖并发写入。

func (s *TaskService) UpdateTask(task *taskEntity.SyncTask) error {

	if task == nil {
		return fmt.Errorf("task is nil")
	}
	if err := task.Config.ValidateFullLoadOptions(); err != nil {
		return err
	}

	s.mu.Lock()

	defer s.mu.Unlock()

	existing, exists := s.tasks[task.Config.ID]
	if !exists {

		return fmt.Errorf("task not found: %s", task.Config.ID)

	}

	// 写锁内复核：拒绝运行中编辑，避免 handler 先读快照后的 TOCTOU。
	if existing.Context.Status == taskEntity.TaskStatusRunning {
		return fmt.Errorf("cannot update running task: %s", task.Config.ID)
	}

	// STOPPED 为终态，不允许编辑原任务（可复制新建或删除）。
	if existing.Context.Status == taskEntity.TaskStatusStopped {
		return fmt.Errorf("task is stopped and cannot be edited: %s", task.Config.ID)
	}

	existing.Config = task.Config

	// AllowNopkAll 只是配置开关；真正生效的确认时间戳必须在服务层原子写入 live Context。
	// handler 拿到的是快照，不能依赖快照上的 AcknowledgeNopkAllRisk。
	if task.Config.AllowNopkAll {
		if !existing.HasNopkAllRiskAcknowledgement() {
			existing.AcknowledgeNopkAllRisk(time.Now())
		} else {
			existing.Config.AllowNopkAll = true
		}
	} else {
		existing.ClearNopkAllRiskAcknowledgement()
	}

	// 保存到存储

	if err := s.storage.Save(existing); err != nil {

		fmt.Printf("保存任务失败: %v\n", err)

	}

	return nil

}

// DeleteTask 删除任务

func (s *TaskService) DeleteTask(taskID string) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	existing, exists := s.tasks[taskID]
	if !exists {

		return fmt.Errorf("task not found: %s", taskID)

	}

	// 写锁内复核：拒绝删除运行中任务，避免 handler 先读后删的 TOCTOU。
	if existing.Context.Status == taskEntity.TaskStatusRunning {
		return fmt.Errorf("cannot delete running task: %s", taskID)
	}

	// 停止增量同步服务（如果存在）

	if incrSync, exists := s.incrementalSyncs[taskID]; exists {

		logger.Info("[Task %s] Stopping incremental sync service before deletion", taskID)

		incrSync.Stop()

		delete(s.incrementalSyncs, taskID)

	}

	if runtime, exists := s.runtimes[taskID]; exists {

		runtime.Close()

		delete(s.runtimes, taskID)

	}

	delete(s.tasks, taskID)
	clearFullLoadStats(taskID)

	// 清理进度持久化节流记录
	delete(s.lastProgressPersist, taskID)

	// 释放主锁后取消并等待行数对比后台 goroutine 退出，避免 goroutine 写入已删除的任务。
	s.mu.Unlock()
	s.cancelRowComparisonAndWait(taskID)
	s.mu.Lock()

	// 从存储删除

	if err := s.storage.Delete(taskID); err != nil {

		fmt.Printf("删除任务失败: %v\n", err)

	}

	if s.eventRecorder != nil {
		if err := s.eventRecorder.DeleteByTask(taskID); err != nil {
			logger.Warn("[Task %s] Failed to delete task events: %v", taskID, err)
		}
	}

	// 删除位点信息

	if s.checkpointManager != nil {

		if err := s.checkpointManager.Delete(context.Background(), taskID); err != nil {

			logger.Error("[Task %s] Failed to delete checkpoint: %v", taskID, err)

		}

	}

	// 记录审计日志

	if s.auditLogger != nil {

		s.auditLogger.LogTaskDeleted(taskID)

	}

	return nil

}

// StartTask 启动任务

func (s *TaskService) StartTask(ctx context.Context, taskID string) error {

	s.mu.Lock() // 获取写锁

	defer s.mu.Unlock() // 延迟释放写锁

	task, exists := s.tasks[taskID]

	if !exists {

		return fmt.Errorf("task not found: %s", taskID)

	}

	// 检查任务状态，防止重复启动

	if task.Context.Status == taskEntity.TaskStatusRunning { // 检查是否正在运行

		return fmt.Errorf("task is already running: %s", taskID) // 返回错误

	}

	// STOPPED 为用户确认结束的终态，原任务不允许再次启动（可复制新建或删除）。
	if task.Context.Status == taskEntity.TaskStatusStopped {
		return fmt.Errorf("task is stopped and cannot be restarted: %s", taskID)
	}

	if err := fullSyncRestartBlockedError(task); err != nil {
		task.MarkFullSyncFailed(err.Error())
		task.Fail(err)
		task.ResetFullSyncResume()
		if saveErr := s.storage.Save(task); saveErr != nil {
			return fmt.Errorf("%w; additionally failed to save task state: %v", err, saveErr)
		}
		return err
	}

	if err := validateSinkMode(task); err != nil {
		return err
	}

	wasScheduled := task.Context.Status == taskEntity.TaskStatusScheduled
	prevStatus := task.Context.Status

	// 动态创建数据库连接（如果还没有创建或需要更新）

	initRuntime := s.initRuntimeFn

	if initRuntime == nil {

		initRuntime = s.initDatabaseConnections

	}

	runtime, err := initRuntime(task)

	if err != nil {

		return fmt.Errorf("failed to initialize database connections: %w", err)

	}

	// 同一任务重复启动时，先回收旧 runtime 再替换，避免连接泄漏

	if oldRuntime, exists := s.runtimes[taskID]; exists {

		oldRuntime.Close()

	}

	s.runtimes[taskID] = runtime

	s.bindExecution(taskID, runtime)

	task.Start()
	if !wasScheduled {
		task.ClearScheduleConfig()
	}

	startCode := taskEntity.EventCodeTaskStarted
	startMsg := "任务已启动"
	if prevStatus == taskEntity.TaskStatusPaused {
		startCode = taskEntity.EventCodeTaskResumed
		startMsg = "任务已从暂停恢复"
	} else if wasScheduled {
		startCode = taskEntity.EventCodeTaskStarted
		startMsg = "定时任务已启动"
	}
	s.emitLifecycle(taskID, startCode, startMsg, taskEntity.EventSeverityInfo)
	s.emitTaskConfigEffective(task)

	// 记录审计日志

	if s.auditLogger != nil {

		s.auditLogger.LogTaskResumed(taskID)

	}

	// 保存状态

	if err := s.storage.Save(task); err != nil {

		fmt.Printf("保存任务状态失败: %v\n", err)

	}

	// 启动实际的同步逻辑

	// 注意：使用新的 context，而不是 HTTP 请求的 context

	// 因为 HTTP 请求完成后 context 会被取消

	syncCtx, syncCancel := context.WithCancel(context.Background())

	runtime.cancel = syncCancel

	execSync := s.executeSyncFn

	if execSync == nil {

		execSync = s.executeSync

	}

	go execSync(syncCtx, taskID, runtime)

	return nil

}

func collectNoPKTableNames(entries []tableEntry) []string {
	out := make([]string, 0)
	for _, e := range entries {
		if e.identity == nil {
			continue
		}
		if e.identity.Strategy == entity.FullColumnsStrategy {
			out = append(out, e.schema+"."+e.table)
		}
	}
	return out
}

func fullSyncRestartBlockedError(task *taskEntity.SyncTask) error {
	if task == nil {
		return nil
	}

	mode := strings.ToUpper(string(task.Config.Mode))
	// 旧 snapshot/HWM 语义下未完成的 ALL：禁止混合恢复。
	// 仅当 enable_drop_table_before_ddl=true（可重建目标）时允许作为 fresh run 启动。
	if mode == "ALL" && len(task.Context.TableBinlogHWMs) > 0 && !task.Config.EnableDropTableBeforeDDL {
		switch task.Context.SyncPhase {
		case taskEntity.SyncPhaseFullStarted, taskEntity.SyncPhaseFullFailed:
			return fmt.Errorf(
				"legacy ALL task with table_binlog_hwms cannot resume under new P0/P1 catch-up semantics (phase=%q); enable enable_drop_table_before_ddl to rebuild the target for a fresh run, or create a new task",
				task.Context.SyncPhase,
			)
		}
	}

	if task.Config.EnableDropTableBeforeDDL || !task.FullSyncIncomplete() {
		return nil
	}

	switch mode {
	case "FULL", "ALL":
	default:
		return nil
	}

	return fmt.Errorf(
		"full sync was interrupted before completion (phase=%q, enable_drop_table_before_ddl=false); full-sync resume is disabled for plain INSERT full sync, enable enable_drop_table_before_ddl to rebuild the target, or manually clear/rebuild the target and create/reset the task before starting a new full sync",
		task.Context.SyncPhase,
	)
}

func validateSinkMode(task *taskEntity.SyncTask) error {
	if task == nil || len(task.Config.SinkConfigs) == 0 {
		return nil
	}
	mode := strings.ToUpper(string(task.Config.Mode))
	if mode != "FULL" && mode != "ALL" {
		return nil
	}
	for _, sc := range task.Config.SinkConfigs {
		if sc.Type != sink.SinkTypeMYSQL {
			return fmt.Errorf("Kafka/Webhook sink (type=%q) 仅支持 INCREMENTAL 模式，当前模式为 %s", sc.Type, mode)
		}
	}
	return nil
}

func (s *TaskService) resolveSourceSchema(task *taskEntity.SyncTask) string {

	if task != nil && task.Config.SourceDB != nil {

		if dbName := strings.TrimSpace(task.Config.SourceDB.Database); dbName != "" {

			return dbName

		}

	}

	if task != nil {

		if schema := strings.TrimSpace(task.Config.SourceSchema); schema != "" {

			return schema

		}

	}

	if s != nil && s.config != nil {

		if schema := strings.TrimSpace(s.config.Datasource.Database); schema != "" {

			return schema

		}

	}

	return ""

}

func (s *TaskService) resolveTargetSchema(task *taskEntity.SyncTask, sourceSchema string) string {

	if task != nil {

		if task.Config.TargetDB != nil {

			if dbName := strings.TrimSpace(task.Config.TargetDB.Database); dbName != "" {

				return dbName

			}

		}

		if schema := strings.TrimSpace(task.Config.TargetSchema); schema != "" {

			return schema

		}

	}

	if sourceSchema != "" {

		return sourceSchema

	}

	return s.resolveSourceSchema(task)

}

// resolveTableTargetName resolves a target table from the durable, globally aligned
// Tables/TargetTables arrays. In multi-database table sync, index is only the
// table's position inside the current database and must not be used against the
// global TargetTables array; doing so sends the first table of every later source
// database to the first target table of the task.
func (s *TaskService) resolveTableTargetName(task *taskEntity.SyncTask, sourceSchema, tableName string, index int) string {

	if task == nil || len(task.Config.TargetTables) == 0 {

		return tableName

	}

	sourceSchema = strings.TrimSpace(sourceSchema)
	tableName = strings.TrimSpace(tableName)
	defaultSource := strings.TrimSpace(task.Config.SourceSchema)
	if defaultSource == "" && len(task.Config.SourceDatabases) == 1 {
		defaultSource = strings.TrimSpace(task.Config.SourceDatabases[0])
	}

	for configuredIndex, configuredTable := range task.Config.Tables {
		configuredTable = strings.TrimSpace(configuredTable)
		configuredSchema := ""
		configuredName := configuredTable
		if parts := strings.SplitN(configuredTable, ".", 2); len(parts) == 2 {
			configuredSchema = strings.TrimSpace(parts[0])
			configuredName = strings.TrimSpace(parts[1])
		}

		if configuredName != tableName {
			continue
		}
		if configuredSchema != "" && configuredSchema != sourceSchema {
			continue
		}
		if configuredSchema == "" {
			// An unqualified table is only safe when the task has one source, or
			// an explicit default source identifies which database owns it.
			if defaultSource == "" || defaultSource != sourceSchema {
				continue
			}
		}
		if configuredIndex >= len(task.Config.TargetTables) {
			return tableName
		}
		if target := strings.TrimSpace(task.Config.TargetTables[configuredIndex]); target != "" {
			return target
		}
		return tableName
	}

	// Backward compatibility for historical single-database tasks whose Tables
	// entries cannot be matched (for example, older archives with an empty list).
	// This positional fallback is deliberately disabled for multi-database tasks.
	if len(task.Config.SourceDatabases) <= 1 && index >= 0 && index < len(task.Config.TargetTables) {
		if target := strings.TrimSpace(task.Config.TargetTables[index]); target != "" {
			return target
		}

	}

	return tableName

}

func (s *TaskService) resolveSourceTableName(task *taskEntity.SyncTask, index int, fallback string) string {

	if task == nil || len(task.Config.Tables) == 0 {

		return fallback

	}

	if index < 0 || index >= len(task.Config.Tables) {

		return fallback

	}

	if name := strings.TrimSpace(task.Config.Tables[index]); name != "" {

		return name

	}

	return fallback

}

// initDatabaseConnections 初始化数据库连接（每次任务启动都重建，确保连接指向正确的库）

func (s *TaskService) initDatabaseConnections(task *taskEntity.SyncTask) (*taskRuntime, error) {

	var err error

	var sourceDB *sql.DB

	var targetDB *sql.DB

	cleanup := func() {

		if sourceDB != nil {

			sourceDB.Close()

		}

		if targetDB != nil && targetDB != sourceDB {

			targetDB.Close()

		}

	}

	// 确定源数据库配置

	resolvedSourceSchema := s.resolveSourceSchema(task)

	sourceConfig := task.Config.SourceDB

	if sourceConfig == nil && s.config != nil {

		// 使用配置文件中的默认值

		sourceConfig = &taskEntity.DatabaseConfig{

			Host: s.config.Datasource.Host,

			Port: s.config.Datasource.Port,

			Database: resolvedSourceSchema,

			Username: s.config.Datasource.Username,

			Password: s.config.Datasource.Password,
		}

	}

	if sourceConfig == nil {

		return nil, fmt.Errorf("source database config is required")

	}

	if strings.TrimSpace(sourceConfig.Database) == "" {

		clonedSourceConfig := *sourceConfig

		clonedSourceConfig.Database = resolvedSourceSchema

		sourceConfig = &clonedSourceConfig

	}

	if strings.TrimSpace(sourceConfig.Database) == "" {

		return nil, fmt.Errorf("source schema is required")

	}

	// 确定目标数据库配置

	targetConfig := task.Config.TargetDB

	if targetConfig == nil && s.config != nil && s.config.Target.Host != "" {

		// 使用配置文件中的默认值

		targetConfig = &taskEntity.DatabaseConfig{

			Host: s.config.Target.Host,

			Port: s.config.Target.Port,

			Database: task.Config.TargetSchema,

			Username: s.config.Target.Username,

			Password: s.config.Target.Password,
		}

	}

	if targetConfig == nil {

		// 没有目标配置：借用源库的连接信息，但连接到目标 Schema，确保数据写入目标库

		targetSchema := task.Config.TargetSchema

		if targetSchema == "" {

			targetSchema = resolvedSourceSchema

		}

		targetConfig = &taskEntity.DatabaseConfig{

			Host: sourceConfig.Host,

			Port: sourceConfig.Port,

			Database: targetSchema,

			Username: sourceConfig.Username,

			Password: sourceConfig.Password,
		}

	}

	// 连接源数据库

	srcCompress := s.config != nil && s.config.Datasource.Compress

	sourceDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",

		sourceConfig.Username,

		sourceConfig.Password,

		sourceConfig.Host,

		sourceConfig.Port,

		sourceConfig.Database,

		config.MySQLTCPParams(srcCompress),
	)

	sourceDB, err = sql.Open("mysql", sourceDSN)

	if err != nil {

		return nil, fmt.Errorf("failed to connect source database: %w", err)

	}

	// 测试连接

	if err = sourceDB.Ping(); err != nil {

		cleanup()

		return nil, fmt.Errorf("failed to ping source database: %w", err)

	}

	config.ApplySyncMySQLPool(sourceDB, syncTuneFrom(s.config), true, fmt.Sprintf("task %s source", task.Config.ID))

	logger.Info("[Task %s] Source database connected: %s:%d/%s", task.Config.ID, sourceConfig.Host, sourceConfig.Port, sourceConfig.Database)

	// 连接目标数据库

	tgtCompress := s.config != nil && s.config.Target.Compress

	// 先连接到MySQL服务器（不指定数据库）以便能够创建数据库

	targetDSNNoDB := fmt.Sprintf("%s:%s@tcp(%s:%d)/?%s",

		targetConfig.Username,

		targetConfig.Password,

		targetConfig.Host,

		targetConfig.Port,

		config.MySQLTCPParams(tgtCompress),
	)

	targetDBNoDB, err := sql.Open("mysql", targetDSNNoDB)

	if err == nil {

		// 尝试创建数据库（如果不存在）

		_, err = targetDBNoDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", targetConfig.Database))

		if err != nil {

			logger.Warn("Warning: Failed to create target database: %v", err)

		} else {

			logger.Info("Target database '%s' created or already exists", targetConfig.Database)

		}

		targetDBNoDB.Close()

	}

	// 连接到目标数据库

	targetDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",

		targetConfig.Username,

		targetConfig.Password,

		targetConfig.Host,

		targetConfig.Port,

		targetConfig.Database,

		config.MySQLTCPParams(tgtCompress),
	)

	targetDB, err = sql.Open("mysql", targetDSN)

	if err != nil {

		cleanup()

		return nil, fmt.Errorf("failed to connect target database: %w", err)

	}

	// 测试连接

	if err = targetDB.Ping(); err != nil {

		cleanup()

		return nil, fmt.Errorf("failed to ping target database: %w", err)

	}

	config.ApplySyncMySQLPool(targetDB, syncTuneFrom(s.config), false, fmt.Sprintf("task %s target", task.Config.ID))

	logger.Info("[Task %s] Target database connected: %s:%d/%s", task.Config.ID, targetConfig.Host, targetConfig.Port, targetConfig.Database)

	// 初始化元数据分析器（如果还没有创建）

	schemaDetector := infrastructure.NewSchemaDetector(sourceDB)

	analyzer := service.NewIdentityAnalyzerService(schemaDetector)

	// 检查binlog_row_image设置

	binlogImage, err := schemaDetector.CheckBinlogRowImage()

	if err != nil {

		logger.Warn("Warning: Failed to check binlog_row_image: %v", err)

	} else {

		logger.Info("binlog_row_image = %s", binlogImage)

		if binlogImage != "FULL" {

			logger.Warn("Warning: binlog_row_image is not FULL. Incremental sync for no-PK tables may not work correctly.")

		}

	}

	return &taskRuntime{

		sourceDB: sourceDB,

		targetDB: targetDB,

		analyzer: analyzer,

		readOnlyManager: readonly.NewReadOnlyManager(targetDB),
	}, nil

}

// errFullSyncStoppedByUser 表示全量同步是被用户主动停止/暂停打断的，而不是真正的失败。
// 上层 executeSync 据此区分"应当标 FAILED"还是"应当保留当前阶段并退出"。
var errFullSyncStoppedByUser = errors.New("full sync stopped by user")

// taskStopReasonKind 区分任务非 RUNNING 的具体原因；与 isTaskStopped（worker 级宽语义）分离。
type taskStopReasonKind int

const (
	taskStopReasonRunning taskStopReasonKind = iota
	taskStopReasonPaused
	taskStopReasonStopped
	taskStopReasonFailed
	taskStopReasonMissing
	taskStopReasonOther
)

// taskStopReason 依据内存中 live task 的 Status 映射到停止原因种类，供 worker 级与编排级共用。
// 任务不在内存表时返回 taskStopReasonMissing（通常意味着服务重启/shutdown 后任务未恢复）。
func (s *TaskService) taskStopReason(taskID string) taskStopReasonKind {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, exists := s.tasks[taskID]
	if !exists {
		return taskStopReasonMissing
	}
	switch task.Context.Status {
	case taskEntity.TaskStatusRunning:
		return taskStopReasonRunning
	case taskEntity.TaskStatusPaused:
		return taskStopReasonPaused
	case taskEntity.TaskStatusStopped:
		return taskStopReasonStopped
	case taskEntity.TaskStatusFailed:
		return taskStopReasonFailed
	default:
		return taskStopReasonOther
	}
}

// isUserFullSyncStop 判断全量是否因用户/服务侧主动停止而中断。
// taskStopReasonMissing（任务不在内存表，通常为服务 shutdown）也视为非失败停止，
// 避免服务重启时把未恢复的任务误判为 FAILED（32c51b6 修复点）。
func (s *TaskService) isUserFullSyncStop(taskID string) bool {
	switch s.taskStopReason(taskID) {
	case taskStopReasonPaused, taskStopReasonStopped, taskStopReasonMissing:
		return true
	default:
		return false
	}
}

// fullLoadStopCause 将 live task 的停止状态映射为具名 cause error，供 WithCancelCause 传播到全量引擎。
// 与 isUserFullSyncStop 的分工：本方法返回具体 error（ErrUserPaused/ErrUserStopped/ErrServiceShutdown），
// 后者仅返回 bool；Missing 映射为 ErrServiceShutdown。
func (s *TaskService) fullLoadStopCause(taskID string) error {
	switch s.taskStopReason(taskID) {
	case taskStopReasonPaused:
		return fullload.ErrUserPaused
	case taskStopReasonStopped:
		return fullload.ErrUserStopped
	case taskStopReasonMissing:
		return fullload.ErrServiceShutdown
	default:
		return nil
	}
}

// errFullSyncPreflight 表示全量尚未进入 FULL_STARTED 前的可修正校验失败。
// 不得 MarkFullSyncFailed，否则会把未启动任务污染成不可恢复的 FULL_FAILED。
var errFullSyncPreflight = errors.New("full sync preflight failed")

// abortFullSyncIfCancelled 在阶段边界检查丢锁 cause 与用户停止信号。
// 丢锁优先于用户停止：必须 fail-closed，不能误标 FULL_COMPLETED。
func (s *TaskService) abortFullSyncIfCancelled(ctx context.Context, taskID string) error {
	if err := fullload.SchemaLockLostError(ctx); err != nil {
		logger.Error("[Task %s] Full sync aborted after schema lock lost", taskID)
		s.emitRetryEvent(taskID, taskEntity.EventCodeSchemaLockLost, "", "",
			"target schema advisory lock lost; full sync aborted (fail-closed)",
			taskEntity.EventSeverityError, nil)
		return err
	}
	if s.isUserFullSyncStop(taskID) {
		logger.Info("[Task %s] Full sync detected user stop signal; aborting without marking completed", taskID)
		return errFullSyncStoppedByUser
	}
	return nil
}

// updateSyncPhase 持有写锁修改任务阶段相关字段并持久化。mutator 必须是无副作用的纯字段写入。
// 若任务不存在则静默返回，不抛错，与 updateTaskStatus 行为一致。
func (s *TaskService) updateSyncPhase(taskID string, mutator func(*taskEntity.SyncTask)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.tasks[taskID]
	if !exists {
		return
	}
	mutator(task)
	if err := s.storage.Save(task); err != nil {
		logger.Warn("[Task %s] Failed to persist sync phase update: %v", taskID, err)
	}
}

// formatBinlogPosition 把 mysql.Position 序列化成 "file:pos"，便于在任务存档与日志中表示。
// 位点为空时返回空串。
func formatBinlogPosition(pos mysql.Position) string {
	if pos.Name == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", pos.Name, pos.Pos)
}

// persistFullLoadV2TableState 持久化单表 V2 加载状态(P3)。
// 由 fullload.Engine 的表级状态机在状态转换时通过 OnTableStateChange 回调触发。
// 持久化失败必须返回错误（fail-closed），由引擎中止以免重启后状态与目标不一致。
func (s *TaskService) persistFullLoadV2TableState(taskID, schema, table, phase string, attemptID int, stagingTable, errMsg string, committedRows int64) error {
	tableKey := schema + "." + table
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found while persisting V2 state for %s", taskID, tableKey)
	}
	task.SetFullLoadV2TableState(tableKey, &taskEntity.FullLoadV2TableState{
		Phase:         phase,
		AttemptID:     attemptID,
		StagingTable:  stagingTable,
		CommittedRows: committedRows,
		LastError:     errMsg,
		UpdatedAt:     time.Now(),
	})
	if err := s.storage.Save(task); err != nil {
		return fmt.Errorf("persist V2 table state for %s: %w", tableKey, err)
	}
	return nil
}

// collectStaleStagingTableRefsFromTask 从单个任务的持久化 V2 状态收集需清理的精确 staging 表。
// StagingTable 由引擎写入；目标 schema 从任务映射的源 schema 推导。不持有 TaskService 锁。
func collectStaleStagingTableRefsFromTask(task *taskEntity.SyncTask) []fullload.StagingTableRef {
	if task == nil || !task.Config.UsesFullLoadV2() {
		return nil
	}
	var refs []fullload.StagingTableRef
	seen := make(map[string]struct{})
	for tableKey, st := range task.Context.FullLoadV2States {
		if st == nil || st.Phase == "PUBLISHED" || st.StagingTable == "" {
			continue
		}
		schema, _, ok := splitSourceTableKey(tableKey)
		if !ok {
			continue
		}
		targetSchema := schema
		if task.Config.TargetSchema != "" && schema == task.Config.SourceSchema {
			targetSchema = task.Config.TargetSchema
		}
		for i, src := range task.Config.SourceDatabases {
			if src != schema {
				continue
			}
			if i < len(task.Config.TargetDatabases) && task.Config.TargetDatabases[i] != "" {
				targetSchema = task.Config.TargetDatabases[i]
			} else if task.Config.TargetDatabase != "" {
				targetSchema = task.Config.TargetDatabase
			}
			break
		}
		refKey := targetSchema + "." + st.StagingTable
		if _, ok := seen[refKey]; ok {
			continue
		}
		seen[refKey] = struct{}{}
		refs = append(refs, fullload.StagingTableRef{Schema: targetSchema, Table: st.StagingTable})
	}
	return refs
}

// collectStaleStagingTableRefs 汇总所有任务的残留 staging 引用（测试/共享 targetDB 构造路径）。
func (s *TaskService) collectStaleStagingTableRefs() []fullload.StagingTableRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var refs []fullload.StagingTableRef
	seen := make(map[string]struct{})
	for _, task := range s.tasks {
		for _, ref := range collectStaleStagingTableRefsFromTask(task) {
			refKey := ref.Schema + "." + ref.Table
			if _, ok := seen[refKey]; ok {
				continue
			}
			seen[refKey] = struct{}{}
			refs = append(refs, ref)
		}
	}
	return refs
}

// cleanupStaleStagingTablesForTask 按单个任务的持久化清单清理崩溃遗留 staging。
func (s *TaskService) cleanupStaleStagingTablesForTask(ctx context.Context, db *sql.DB, task *taskEntity.SyncTask) {
	if db == nil || task == nil {
		return
	}
	refs := collectStaleStagingTableRefsFromTask(task)
	if len(refs) == 0 {
		return
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 60*time.Second)
	dropped, err := fullload.CleanupStaleStagingTables(cleanupCtx, db, refs)
	cleanupCancel()
	if err != nil {
		logger.Warn("[Task %s] staging cleanup failed (partial): %v", task.Config.ID, err)
	}
	if dropped > 0 {
		logger.Info("[Task %s] staging cleanup: dropped %d stale staging tables from manifest", task.Config.ID, dropped)
	}
}

// cleanupAllStaleStagingTables 清理所有已加载任务的残留 staging（共享 targetDB 构造路径）。
func (s *TaskService) cleanupAllStaleStagingTables(ctx context.Context, db *sql.DB) {
	if db == nil {
		return
	}
	refs := s.collectStaleStagingTableRefs()
	if len(refs) == 0 {
		return
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 60*time.Second)
	dropped, err := fullload.CleanupStaleStagingTables(cleanupCtx, db, refs)
	cleanupCancel()
	if err != nil {
		logger.Warn("[TaskService] startup staging cleanup failed (partial): %v", err)
	}
	if dropped > 0 {
		logger.Info("[TaskService] startup staging cleanup: dropped %d stale staging tables from manifest", dropped)
	}
}

func splitSourceTableKey(key string) (schema, table string, ok bool) {
	idx := strings.IndexByte(key, '.')
	if idx <= 0 || idx >= len(key)-1 {
		return "", "", false
	}
	return key[:idx], key[idx+1:], true
}

// parseBinlogPosition 解析 "file:pos"；与 formatBinlogPosition 互逆。
func parseBinlogPosition(s string) (mysql.Position, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return mysql.Position{}, fmt.Errorf("empty binlog position")
	}
	idx := strings.LastIndex(s, ":")
	if idx <= 0 || idx == len(s)-1 {
		return mysql.Position{}, fmt.Errorf("invalid binlog position %q", s)
	}
	pos, err := strconv.ParseUint(s[idx+1:], 10, 32)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("invalid binlog position %q: %w", s, err)
	}
	p := mysql.Position{Name: s[:idx], Pos: uint32(pos)}
	if err := binlog.ValidatePosition(p); err != nil {
		return mysql.Position{}, fmt.Errorf("invalid binlog position %q: %w", s, err)
	}
	return p, nil
}

// makeThrottledIncrementalPositionPersister 构造一个"节流型"位点回写回调，每 minInterval 最多
// 写一次任务存档。增量同步每事件都会调它，但只有间隔足够时才真正落盘，避免频繁写引发的存储开销。
//
// 内部用 mutex + lastWriteAt 实现；不依赖任务级锁，回调本身完全幂等。
// 注意：捕获 service 指针；持有 service 写锁的位置不要调用回调，避免反向锁顺序。
func (s *TaskService) makeThrottledIncrementalPositionPersister(minInterval time.Duration) syncApp.PositionPersister {
	if minInterval <= 0 {
		minInterval = 5 * time.Second
	}
	var (
		mu          sync.Mutex
		lastWriteAt time.Time
	)
	return func(taskID string, pos mysql.Position) {
		mu.Lock()
		if time.Since(lastWriteAt) < minInterval {
			mu.Unlock()
			return
		}
		lastWriteAt = time.Now()
		mu.Unlock()

		posStr := formatBinlogPosition(pos)
		if posStr == "" {
			return
		}
		s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
			t.UpdateIncrementalPosition(posStr)
		})
	}
}

// executeSync 执行同步任务

func (s *TaskService) executeSync(ctx context.Context, taskID string, runtime *taskRuntime) {

	s.mu.RLock()

	task, exists := s.tasks[taskID]

	s.mu.RUnlock()

	if !exists {

		return

	}

	enableReadOnly := task.Config.EnableReadOnly

	logger.Info("[Task %s] Starting sync, mode: %s, tables: %v", taskID, task.Config.Mode, task.Config.Tables)

	// 在同步开始前，若任务开启了只读管理，临时关闭目标库只读以允许数据写入

	if enableReadOnly && runtime != nil && runtime.readOnlyManager != nil {

		logger.Info("[Task %s] 正在临时关闭目标实例只读以进行同步...", taskID)

		if err := runtime.readOnlyManager.SetReadOnly(); err != nil {

			logger.Warn("[Task %s] 警告: 关闭只读失败: %v", taskID, err)

			// 记录错误但继续执行同步

		} else {

			logger.Info("[Task %s] 目标实例只读已临时关闭，同步结束后自动恢复", taskID)

		}

	}

	// 确保在函数退出时恢复只读状态

	defer func() {

		if enableReadOnly && runtime != nil && runtime.readOnlyManager != nil {

			logger.Info("[Task %s] 正在恢复目标实例只读状态...", taskID)

			if err := runtime.readOnlyManager.RestoreReadOnly(); err != nil {

				logger.Warn("[Task %s] 警告: 恢复只读状态失败: %v", taskID, err)

			} else {

				logger.Info("[Task %s] 目标实例用户权限已恢复", taskID)

			}

		}

	}()

	// 根据模式执行同步（支持大小写不敏感）

	mode := strings.ToUpper(string(task.Config.Mode))

	// 阶段快照日志：让运维一眼看出当前任务处在哪个同步阶段（修复 14）
	logger.Info("[Task %s] Phase snapshot: phase=%q full_sync_completed_at=%v full_sync_start_position=%q last_incremental_position=%q",
		taskID,
		task.Context.SyncPhase,
		task.Context.FullSyncCompletedAt,
		task.Context.FullSyncStartPosition,
		task.Context.LastIncrementalPosition,
	)

	switch mode {

	case "FULL":

		if err := s.executeFullSync(ctx, task, runtime); err != nil {

			if errors.Is(err, errFullSyncStoppedByUser) {
				// 用户主动暂停/停止：保留当前阶段，不调 completeTask、不标 FAILED
				logger.Info("[Task %s] Full sync interrupted by user; phase remains %q", taskID, task.Context.SyncPhase)
				return
			}
			if errors.Is(err, errFullSyncPreflight) {
				// 启动前校验失败：保持原 SyncPhase，允许用户修正后重试。
				logger.Error("[Task %s] Full sync preflight failed (phase unchanged=%q): %v", taskID, task.Context.SyncPhase, err)
				s.failTaskUnlessCancelled(ctx, taskID, err.Error())
				return
			}

			s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
				t.MarkFullSyncFailed(err.Error())
			})
			s.failTaskUnlessCancelled(ctx, taskID, err.Error())

		} else {

			s.completeTask(taskID)

		}

	case "INCREMENTAL":

		// === 修复 5：纯 INCREMENTAL 模式必须验证此前完成过全量 ===
		// 否则起始位点要么是空（订阅当前主库位点 → 漏数据），要么是上次失败留下的脏位点。
		if !task.HasFullSyncEverCompleted() {
			msg := fmt.Sprintf(
				"incremental sync requires a previously completed full sync (current phase=%q); please run FULL or ALL mode first to seed the target",
				task.Context.SyncPhase,
			)
			logger.Error("[Task %s] %s", taskID, msg)
			s.failTaskUnlessCancelled(ctx, taskID, msg)
			return
		}

		s.executeIncrementalSync(ctx, task, runtime)

	case "ALL":

		// === 修复 5：ALL 模式根据阶段决定是否跳过全量 ===
		// 已经完成过全量（含已接管增量）→ 直接接增量，避免重复全量拷贝
		// 全量未完成或处于 FULL_FAILED 中间态 → 仅允许在 destructive rebuild 场景重新全量
		if task.HasFullSyncEverCompleted() {
			logger.Info("[Task %s] Full sync already completed (phase=%q, completed_at=%v); skipping full sync and resuming incremental from %q",
				taskID, task.Context.SyncPhase, task.Context.FullSyncCompletedAt, task.Context.LastIncrementalPosition,
			)
			s.executeIncrementalSync(ctx, task, runtime)
			return
		}

		if task.FullSyncIncomplete() {
			logger.Warn("[Task %s] Previous full sync did not complete (phase=%q, reason=%q); starting a fresh full sync after target rebuild",
				taskID, task.Context.SyncPhase, task.Context.FullSyncFailedReason,
			)
		}

		if err := s.executeFullSync(ctx, task, runtime); err == nil {

			s.executeIncrementalSync(ctx, task, runtime)

		} else if errors.Is(err, errFullSyncStoppedByUser) {

			logger.Info("[Task %s] Full sync interrupted by user during ALL mode; not starting incremental, phase remains %q",
				taskID, task.Context.SyncPhase)
			return

		} else if errors.Is(err, errFullSyncPreflight) {

			logger.Error("[Task %s] ALL full sync preflight failed (phase unchanged=%q): %v", taskID, task.Context.SyncPhase, err)
			s.failTaskUnlessCancelled(ctx, taskID, err.Error())
			return

		} else {

			s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
				t.MarkFullSyncFailed(err.Error())
			})
			s.failTaskUnlessCancelled(ctx, taskID, err.Error())

		}

	default:

		s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, "unknown sync mode: "+string(task.Config.Mode))

	}

}

// executeFullSync 执行全量同步（支持多库）

func (s *TaskService) executeFullSync(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime) error {

	taskID := task.Config.ID

	if runtime == nil || runtime.sourceDB == nil || runtime.targetDB == nil || runtime.analyzer == nil {

		return fmt.Errorf("task runtime is not initialized")

	}

	// 修复 1d：早期停止检查。任务在到达此处之前可能已被暂停/停止（例如用户调用 PauseTask
	// 后又立刻触发新一轮 executeSync 调度的极端时序），此处一次性返回 sentinel，避免后续
	// 还去拿 FTWRL / 跑全量。
	if s.isUserFullSyncStop(taskID) {
		logger.Info("[Task %s] Full sync skipped: task already stopped before any work was done", taskID)
		return errFullSyncStoppedByUser
	}

	// 全量同步使用普通 INSERT 语义，暂停/失败后不再支持 full_sync_resume 续传。
	// 每次进入全量前都清空旧断点和运行计数，避免沿用陈旧游标或累加旧值。
	// 计数重置放在此处（而非通用 Start()），确保 ALL/INCREMENTAL 重启时不清空历史全量统计。
	s.resetResumeIfFresh(taskID)

	// 构建 (sourceSchema, targetSchema) 对列表

	var pairs []schemaPair

	if len(task.Config.SourceDatabases) > 0 {

		// 多库模式：SourceDatabases[i] -> TargetDatabases[i]

		// 若 TargetDatabases 不足，用源库名作为目标库名

		for i, src := range task.Config.SourceDatabases {

			dst := src

			if i < len(task.Config.TargetDatabases) && task.Config.TargetDatabases[i] != "" {

				dst = task.Config.TargetDatabases[i]

			}

			pairs = append(pairs, schemaPair{src, dst})

		}

	} else {

		// 单库模式（兼容旧逻辑）

		resolvedSourceSchema := s.resolveSourceSchema(task)

		if resolvedSourceSchema == "" {

			return fmt.Errorf("source schema is required for single-database sync")

		}

		dst := task.Config.TargetSchema

		if dst == "" {

			dst = resolvedSourceSchema

		}

		pairs = append(pairs, schemaPair{resolvedSourceSchema, dst})

	}

	tablesBySource := make(map[string][]string)

	if len(task.Config.Tables) > 0 {

		defaultSource := task.Config.SourceSchema

		if defaultSource == "" && len(task.Config.SourceDatabases) > 0 {

			defaultSource = task.Config.SourceDatabases[0]

		} else if defaultSource == "" {

			defaultSource = s.resolveSourceSchema(task)

		}

		for _, fullTableName := range task.Config.Tables {

			sourceSchema := defaultSource

			tableName := fullTableName

			if parts := strings.SplitN(fullTableName, ".", 2); len(parts) == 2 {

				sourceSchema = parts[0]

				tableName = parts[1]

			}

			if sourceSchema == "" || tableName == "" {

				continue

			}

			tablesBySource[sourceSchema] = append(tablesBySource[sourceSchema], tableName)

		}

	}

	// 快速估算所有库的总行数（使用 information_schema，毫秒级返回）

	var estimatedRows int64

	var allTableEntries []tableEntry

	for _, p := range pairs {

		tables := tablesBySource[p.src]

		if task.Config.SyncLevel == taskEntity.SyncLevelTable && len(task.Config.Tables) > 0 && len(tables) == 0 {

			continue

		}

		if len(tables) == 0 {

			allTables, err := runtime.analyzer.GetAllTables(p.src)

			if err != nil {

				logger.Error("[Task %s] Failed to get tables for %s: %v", taskID, p.src, err)
				if task.Config.Mode == taskEntity.SyncModeAll {
					return fmt.Errorf("%w: list tables for %s: %v", errFullSyncPreflight, p.src, err)
				}
				continue

			}

			for _, t := range allTables {

				tables = append(tables, t.TableName)

			}

		}

		for _, tableName := range tables {

			identity, err := runtime.analyzer.AnalyzeTable(p.src, tableName)

			if err != nil {
				if task.Config.Mode == taskEntity.SyncModeAll {
					return fmt.Errorf("%w: analyze table %s.%s: %v", errFullSyncPreflight, p.src, tableName, err)
				}
				continue

			}

			allTableEntries = append(allTableEntries, tableEntry{schema: p.src, table: tableName, identity: identity})

			r := reader.NewReader(runtime.sourceDB, p.src, tableName, identity)

			count, err := r.GetEstimatedCount(ctx)

			if err != nil {

				continue

			}

			estimatedRows += count

		}

	}

	// 先用估算值快速启动，让前端立即看到进度（仅用于 ETA，不用于正确性校验）
	s.updateTaskEstimatedRows(taskID, estimatedRows)

	logger.Info("[Task %s] Fast estimated total rows: %d (via information_schema)", taskID, estimatedRows)

	// 初始化运行时进度追踪（供前端实时展示）
	s.initRunningProgress(taskID, allTableEntries, "full")

	// 必须在捕获 P0 前判定 V2 resume，避免覆盖原 P0/checkpoint 导致 PUBLISHED 跳过窗口静默漏数。
	s.mu.RLock()
	liveForResume := s.tasks[taskID]
	v2Resume := detectFullLoadV2Resume(liveForResume)
	s.mu.RUnlock()
	isV2Resume := v2Resume.active
	v2BaselineDone := v2Resume.baselineDone

	// === Binlog 增量起点：仅 ALL 模式需要 ===
	// FULL 模式不捕获 binlog 位点、不保存增量 checkpoint：只做一次无缝全表遍历，
	// 同步期间发生的变化不进行追平。如需覆盖同步期间的变化，请使用 ALL 模式。
	var startPosStr string
	if task.Config.Mode == taskEntity.SyncModeAll {
		nopkTables := collectNoPKTableNames(allTableEntries)
		if len(nopkTables) > 0 && !task.HasNopkAllRiskAcknowledgement() {
			return fmt.Errorf(
				"%w: ALL mode includes no-PK/UK tables and requires user confirmation (allow_nopk_all=true): %v; consistency=best_effort reason=no_primary_or_unique_key",
				errFullSyncPreflight,
				nopkTables,
			)
		}
		if len(nopkTables) > 0 {
			logger.Warn("[Task %s] ALL mode no-PK/UK tables acknowledged: %v (consistency=best_effort)", taskID, nopkTables)
		}

		if isV2Resume {
			startPosStr = v2Resume.startPos
			if startPosStr == "" {
				errMsg := "V2 resume requires persisted FullSyncStartPosition (P0); refusing to capture a new P0 that would skip changes for already-PUBLISHED tables"
				logger.Error("[Task %s] %s", taskID, errMsg)
				s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
					t.MarkFullSyncFailed(errMsg)
				})
				s.failTaskUnlessCancelled(ctx, taskID, errMsg)
				return fmt.Errorf("%s", errMsg)
			}
			curPos, cpErr := s.checkpointManager.GetPosition(ctx, taskID)
			if cpErr != nil {
				errMsg := fmt.Sprintf("Failed to read checkpoint during V2 resume: %v", cpErr)
				s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
					t.MarkFullSyncFailed(errMsg)
				})
				s.failTaskUnlessCancelled(ctx, taskID, errMsg)
				return fmt.Errorf("%s", errMsg)
			}
			if curPos.Name == "" {
				// checkpoint 丢失时回退到原 P0（可重放，不可前移）。
				p0, parseErr := parseBinlogPosition(startPosStr)
				if parseErr != nil {
					errMsg := fmt.Sprintf("V2 resume: invalid persisted P0 %q: %v", startPosStr, parseErr)
					s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
						t.MarkFullSyncFailed(errMsg)
					})
					s.failTaskUnlessCancelled(ctx, taskID, errMsg)
					return fmt.Errorf("%s", errMsg)
				}
				if err := s.checkpointManager.SavePosition(ctx, taskID, p0); err != nil {
					errMsg := fmt.Sprintf("V2 resume: failed to restore checkpoint from P0: %v", err)
					s.emitRetryEvent(taskID, taskEntity.EventCodeCheckpointPersistFailed, "", "",
						errMsg, taskEntity.EventSeverityError,
						map[string]interface{}{"stage": "v2_resume_p0", "error": err.Error()})
					s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
						t.MarkFullSyncFailed(errMsg)
					})
					s.failTaskUnlessCancelled(ctx, taskID, errMsg)
					return fmt.Errorf("%s", errMsg)
				}
				logger.Info("[Task %s] V2 resume: restored checkpoint from persisted P0=%s", taskID, startPosStr)
			} else {
				logger.Info("[Task %s] V2 resume: preserving P0=%s checkpoint=%s (baseline_done=%v subphase=%q)",
					taskID, startPosStr, formatBinlogPosition(curPos), v2BaselineDone, v2Resume.subphase)
			}
		} else {
			// fresh run：增量起点 P0 必须早于所有全量读取；无锁读取 SHOW MASTER STATUS。
			logger.Info("[Task %s] ALL mode: capturing unlocked binlog start position P0 before full scan", taskID)

			binlogPos, posErr := s.captureFullSyncStartPosition(ctx, runtime)
			startPosStr = formatBinlogPosition(binlogPos)
			if posErr != nil {
				errMsg := fmt.Sprintf("Failed to capture full-sync start binlog position: %v. "+
					"Without a start position, incremental sync would fall back to current master position and miss all changes during full sync. "+
					"Task failed to prevent silent data loss.", posErr)
				logger.Error("[Task %s] %s", taskID, errMsg)
				s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
					t.MarkFullSyncFailed(errMsg)
				})
				s.failTaskUnlessCancelled(ctx, taskID, errMsg)
				return fmt.Errorf("%s", errMsg)
			}
			if binlogPos.Name == "" {
				errMsg := "Captured empty binlog start position (file name is empty). " +
					"Incremental sync cannot be seeded correctly; task failed to prevent silent data loss."
				logger.Error("[Task %s] %s", taskID, errMsg)
				s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
					t.MarkFullSyncFailed(errMsg)
				})
				s.failTaskUnlessCancelled(ctx, taskID, errMsg)
				return fmt.Errorf("%s", errMsg)
			}
			if err := s.checkpointManager.SavePosition(ctx, taskID, binlogPos); err != nil {
				errMsg := fmt.Sprintf("Failed to save binlog start position for incremental sync: %v. "+
					"Without a persisted checkpoint, incremental sync would fall back to current master position and miss all changes during full sync. "+
					"Task failed to prevent silent data loss.", err)
				s.emitRetryEvent(taskID, taskEntity.EventCodeCheckpointPersistFailed, "", "",
					"failed to persist P0 binlog checkpoint", taskEntity.EventSeverityError,
					map[string]interface{}{"stage": "p0_save", "error": err.Error()})
				logger.Error("[Task %s] %s", taskID, errMsg)
				s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
					t.MarkFullSyncFailed(errMsg)
				})
				s.failTaskUnlessCancelled(ctx, taskID, errMsg)
				return fmt.Errorf("%s", errMsg)
			}
			logger.Info("[Task %s] Full-sync start binlog position saved (will be incremental catch-up start point): %s",
				taskID, startPosStr)
			s.emitPhase(taskID, taskEntity.EventCodePhaseP0Captured, "P0",
				fmt.Sprintf("已捕获全量起始 binlog 位点 P0=%s", startPosStr))
		}
	} else {
		logger.Info("[Task %s] FULL mode: skipping binlog position capture and incremental checkpoint (no change catch-up)", taskID)
	}

	// 进入"全量已开始"阶段。resume 不得重置 P0/P1/subphase。
	if isV2Resume {
		s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
			t.ClearTableBinlogHWMs()
			t.Context.SyncPhase = taskEntity.SyncPhaseFullStarted
			t.Context.FullSyncFailedReason = ""
			t.Context.LastUpdateTime = time.Now()
		})
	} else {
		s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
			t.ClearTableBinlogHWMs()
			t.MarkFullSyncStarted(startPosStr)
		})
	}

	s.emitPhase(taskID, taskEntity.EventCodePhaseDDLPrepStarted, "DDL_PREP", "开始目标端 DDL 准备（schema 锁与表结构）")

	// [P1] 在首次目标端 DDL 前获取所有目标 schema 锁，覆盖完整生命周期
	// （库级重建 → 表结构准备 → 数据写入 → 索引恢复），直到任务级收尾完成才释放。
	var targetSchemas []string
	seen := make(map[string]struct{}, len(pairs))
	for _, p := range pairs {
		if _, ok := seen[p.dst]; ok {
			continue
		}
		seen[p.dst] = struct{}{}
		targetSchemas = append(targetSchemas, p.dst)
	}

	var schemaLocks *fullload.SchemaLocks
	if task.Config.UsesFullLoadV2() && len(targetSchemas) > 0 {
		sort.Strings(targetSchemas)
		var lockErr error
		schemaLocks, lockErr = fullload.AcquireSchemaLocks(ctx, runtime.targetDB, targetSchemas)
		if lockErr != nil {
			errMsg := fmt.Sprintf("Failed to acquire target schema locks: %v", lockErr)
			s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
				t.MarkFullSyncFailed(errMsg)
			})
			s.failTaskUnlessCancelled(ctx, taskID, errMsg)
			return fmt.Errorf("%s", errMsg)
		}
		// 启动心跳：锁连接失活时以明确 cause 取消派生 ctx，覆盖后续 DDL / 完成标记。
		lockCtx, lockCancel := context.WithCancelCause(ctx)
		schemaLocks.StartHeartbeat(lockCtx, func() {
			logger.Error("[Task %s] Schema lock heartbeat lost; cancelling full sync (fail-closed)", taskID)
			s.emitRetryEvent(taskID, taskEntity.EventCodeSchemaLockLost, "", "",
				"target schema advisory lock heartbeat lost (fail-closed)", taskEntity.EventSeverityError, nil)
			lockCancel(fullload.ErrSchemaLockLost)
		})
		defer lockCancel(nil)
		defer func() {
			if rErr := schemaLocks.Release(context.Background()); rErr != nil {
				logger.Warn("[Task %s] Release target schema locks: %v", taskID, rErr)
			}
		}()
		// 使用 lockCtx 替换后续操作的 ctx，确保锁失活可传播取消。
		ctx = lockCtx
		s.emitPhase(taskID, taskEntity.EventCodePhaseDDLPrepCompleted, "DDL_PREP", "目标端 schema 锁已获取，DDL 准备就绪")
	}

	// 崩溃恢复：必须在 GET_LOCK 成功且 heartbeat/派生 ctx 建立之后，再按持久化精确表名 DROP 遗留 staging。
	// staging 名仅由目标表名+attemptID 组成，不含 taskID；若在锁外清理，同 schema 并发任务 B
	// 持锁写入的活跃 staging 可能被崩溃恢复任务 A 误 DROP，绕过 schema 隔离。
	// 同时也避免引擎 attempt 从 1 重启复用半成品 staging（有主键重复键 / 无主键重复发布）。
	if task.Config.UsesFullLoadV2() && runtime != nil && runtime.targetDB != nil {
		s.cleanupStaleStagingTablesForTask(ctx, runtime.targetDB, task)
	}

	// isV2Resume / v2BaselineDone 已在捕获 P0 前判定。
	// 全新 V2 全量：在任何 destructive DDL 前持久化完整表清单（全部 PENDING）+ runID。
	if task.Config.UsesFullLoadV2() && !isV2Resume {
		tableKeys := make([]string, 0, len(allTableEntries))
		for _, e := range allTableEntries {
			tableKeys = append(tableKeys, e.schema+"."+e.table)
		}
		runID := fmt.Sprintf("flv2-%s-%d", taskID, time.Now().UnixNano())
		if err := s.initFullLoadV2Manifest(taskID, runID, tableKeys); err != nil {
			errMsg := fmt.Sprintf("Failed to init full-load V2 manifest: %v", err)
			s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
				t.MarkFullSyncFailed(errMsg)
			})
			s.failTaskUnlessCancelled(ctx, taskID, errMsg)
			return fmt.Errorf("%s", errMsg)
		}
		logger.Info("[Task %s] FullLoadV2 manifest initialized: run_id=%s tables=%d", taskID, runID, len(tableKeys))
	}

	// 库级别同步且开启"DDL 前删除"时，在任何目标表 DDL/数据写入前统一重建目标库。
	// 表级别同步保持原有逐表 DROP TABLE 行为（在 ensureTargetTable 内执行）。
	// 增量阶段不会进入 executeFullSync，故此处天然不会在增量阶段执行删除。
	// V2 恢复模式禁止库级重建。
	dbLevelRebuilt := false
	if task.Config.EnableDropTableBeforeDDL && task.Config.SyncLevel == taskEntity.SyncLevelDatabase && !isV2Resume {
		rebuildSchemas := make([]string, 0, len(pairs))
		for _, p := range pairs {
			rebuildSchemas = append(rebuildSchemas, p.dst)
		}
		if err := s.rebuildTargetDatabases(ctx, runtime, taskID, rebuildSchemas); err != nil {

			if errors.Is(err, errFullSyncStoppedByUser) {

				return err

			}

			errMsg := fmt.Sprintf("Failed to rebuild target databases: %v", err)

			s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {

				t.MarkFullSyncFailed(errMsg)

			})

			s.failTaskUnlessCancelled(ctx, taskID, errMsg)

			return fmt.Errorf("%s", errMsg)

		}
		dbLevelRebuilt = true
		logger.Info("[Task %s] Database-level rebuild completed for %d target database(s); per-table DROP TABLE disabled",
			taskID, len(rebuildSchemas))
	} else if isV2Resume {
		logger.Info("[Task %s] FullLoadV2 resume mode: skip database-level rebuild to preserve published tables", taskID)
	}

	// 依次同步每个库（库间串行，库内表间并行）。索引恢复任务跨库累计，
	// 必须等所有库、所有表的数据同步完成后再统一串行执行。
	var pendingIndexRestores []pendingIndexRestore

	if isV2Resume && v2BaselineDone {
		logger.Info("[Task %s] FullLoadV2 resume: baseline already PUBLISHED; skip table copy and resume post-baseline phases (subphase=%q)",
			taskID, v2Resume.subphase)
	} else {
		s.emitPhase(taskID, taskEntity.EventCodePhaseBaseScanStarted, "BASE_SCAN", "开始基线数据扫描")
		for _, p := range pairs {

			if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {
				return err
			}

			if task.Config.SyncLevel == taskEntity.SyncLevelTable && len(task.Config.Tables) > 0 && len(tablesBySource[p.src]) == 0 {

				continue

			}

			// full_load_engine=v2 时使用任务级流水线引擎；否则保持 V1 内联逐表调度。
			var pairErr error
			if task.Config.UsesFullLoadV2() {
				pairErr = s.syncDatabasePairV2(ctx, task, runtime, p.src, p.dst, tablesBySource[p.src], &pendingIndexRestores, dbLevelRebuilt, schemaLocks)
			} else {
				pairErr = s.syncDatabasePair(ctx, task, runtime, p.src, p.dst, tablesBySource[p.src], &pendingIndexRestores, dbLevelRebuilt)
			}
			if pairErr != nil {

				// 一并区分"被停止"和"真实失败"：库级 err 时再核查一次任务状态。
				if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {
					logger.Info("[Task %s] Full sync stopped during pair %s->%s: %v", taskID, p.src, p.dst, pairErr)
					return err
				}
				return pairErr

			}

		}
		s.emitPhase(taskID, taskEntity.EventCodePhaseBaseScanCompleted, "BASE_SCAN", "基线数据扫描完成")
	}

	// PUBLISHED 跳过不会把延迟索引写入 pending；resume/部分完成后统一补齐缺失延迟索引。
	if task.Config.Mode == taskEntity.SyncModeAll || task.Config.OptimizeIndex {
		missing, missErr := s.collectMissingDeferredIndexes(ctx, task, runtime, pairs, tablesBySource)
		if missErr != nil {
			errMsg := fmt.Sprintf("Failed to collect deferred indexes for restore: %v", missErr)
			s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
				t.MarkFullSyncFailed(errMsg)
			})
			s.failTaskUnlessCancelled(ctx, taskID, errMsg)
			return fmt.Errorf("%s", errMsg)
		}
		pendingIndexRestores = mergePendingIndexRestores(pendingIndexRestores, missing)
	}

	// ALL：基线结束后捕获/复用 P1，先 bounded catch-up，再恢复非 identity 索引。
	if task.Config.Mode == taskEntity.SyncModeAll {
		if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {
			return err
		}
		subphase := v2Resume.subphase
		if !isV2Resume {
			subphase = "BASE_SCAN"
		} else {
			s.mu.RLock()
			if live := s.tasks[taskID]; live != nil {
				subphase = live.Context.FullSyncSubphase
			}
			s.mu.RUnlock()
		}

		if subphase != "RESTORE_INDEX" {
			var endPos mysql.Position
			var endPosStr string
			if subphase == "CATCH_UP" && v2Resume.endPos != "" {
				var parseErr error
				endPos, parseErr = parseBinlogPosition(v2Resume.endPos)
				if parseErr != nil {
					errMsg := fmt.Sprintf("V2 resume: invalid persisted P1 %q: %v", v2Resume.endPos, parseErr)
					s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
						t.MarkFullSyncFailed(errMsg)
					})
					s.failTaskUnlessCancelled(ctx, taskID, errMsg)
					return fmt.Errorf("%s", errMsg)
				}
				endPosStr = v2Resume.endPos
				logger.Info("[Task %s] ALL mode: resuming bounded catch-up with persisted P1=%q", taskID, endPosStr)
			} else {
				logger.Info("[Task %s] ALL mode: capturing unlocked binlog end position P1 after baseline scan", taskID)
				var endErr error
				endPos, endErr = s.captureBinlogPosition(ctx, runtime)
				if endErr != nil {
					errMsg := fmt.Sprintf("Failed to capture full-sync end binlog position P1: %v", endErr)
					s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
						t.MarkFullSyncFailed(errMsg)
					})
					s.failTaskUnlessCancelled(ctx, taskID, errMsg)
					return fmt.Errorf("%s", errMsg)
				}
				endPosStr = formatBinlogPosition(endPos)
				s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
					t.Context.FullSyncEndPosition = endPosStr
					t.Context.FullSyncSubphase = "CATCH_UP"
					t.Context.LastUpdateTime = time.Now()
				})
				s.emitPhase(taskID, taskEntity.EventCodePhaseP1Captured, "P1",
					fmt.Sprintf("已捕获基线结束 binlog 位点 P1=%s", endPosStr))
			}
			s.emitPhase(taskID, taskEntity.EventCodePhaseCatchupStarted, "CATCH_UP",
				fmt.Sprintf("开始 bounded catch-up：P0=%s -> P1=%s", startPosStr, endPosStr))
			logger.Info("[Task %s] ALL mode: starting bounded catch-up P0=%q -> P1=%q", taskID, startPosStr, endPosStr)
			if err := s.runBoundedCatchUp(ctx, task, runtime, endPos); err != nil {
				if stopErr := s.abortFullSyncIfCancelled(ctx, taskID); stopErr != nil {
					return stopErr
				}
				errMsg := fmt.Sprintf("bounded catch-up to P1 failed: %v", err)
				s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
					t.MarkFullSyncFailed(errMsg)
				})
				s.failTaskUnlessCancelled(ctx, taskID, errMsg)
				return fmt.Errorf("%s", errMsg)
			}
			s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
				t.Context.FullSyncCatchupPosition = endPosStr
				t.Context.FullSyncSubphase = "RESTORE_INDEX"
				t.Context.LastUpdateTime = time.Now()
			})
			logger.Info("[Task %s] ALL mode: bounded catch-up completed at P1=%q", taskID, endPosStr)
			s.emitPhase(taskID, taskEntity.EventCodePhaseCatchupCompleted, "CATCH_UP",
				fmt.Sprintf("bounded catch-up 已完成，位点=%s", endPosStr))
		} else {
			logger.Info("[Task %s] ALL mode: subphase=RESTORE_INDEX; skip catch-up and resume index restore", taskID)
		}
	}

	if len(pendingIndexRestores) > 0 {
		if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {
			return err
		}
		workers := taskEntity.EffectiveIndexRestoreWorkers(
			task.Config.IndexRestoreWorkerCount,
			task.Config.WorkerCount,
			s.config.Sync.IndexRestoreHardMax,
		)
		logger.Info("[Task %s] 阶段3: 所有表数据同步完成，并发恢复 %d 张表非 identity/延迟索引 (workers=%d)...", taskID, len(pendingIndexRestores), workers)
		s.emitPhase(taskID, taskEntity.EventCodePhaseIndexRestoreStarted, "RESTORE_INDEX",
			fmt.Sprintf("开始恢复 %d 张表的索引", len(pendingIndexRestores)))
		if err := s.restorePendingIndexes(ctx, runtime, taskID, pendingIndexRestores, workers); err != nil {
			s.emitTableEvent(taskID, taskEntity.EventCodeIndexRestoreFailed, "", "",
				fmt.Sprintf("index restore failed: %v", err), taskEntity.EventSeverityError, nil)
			return err
		}
		logger.Info("[Task %s] 阶段3完成：所有待恢复索引已并发处理", taskID)
		s.emitPhase(taskID, taskEntity.EventCodePhaseIndexRestoreCompleted, "RESTORE_INDEX", "索引恢复完成")
	}

	// 完成前最后一次停止/丢锁检查：避免末尾竞态下误标 FULL_COMPLETED。
	if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {
		return err
	}

	// === 修复 4：标记"全量已完成"，是后续 INCREMENTAL/ALL 模式跳过全量的唯一依据 ===
	s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
		t.MarkFullSyncCompleted()
		if t.Config.Mode == taskEntity.SyncModeAll {
			t.Context.FullSyncSubphase = "STREAMING"
		}
	})

	// 全量整体完成，历史断点已无意义，清空以释放存档体积。
	s.clearFullSyncResume(taskID)

	logger.Info("[Task %s] Full sync completed (phase=FULL_COMPLETED, estimated rows=%d, start_position=%q)",
		taskID, estimatedRows, startPosStr)

	return nil

}

// syncReadBatchLimit 每次从源库读取的最大行数，必须与 SQL LIMIT 一致。

// 使用任务配置中的 batch_size；<=0 时用默认；过大时封顶防止单次占用过多内存。

func syncReadBatchLimit(batchSize int) int64 {

	const defaultLimit int64 = 1000

	const hardMax int64 = 100000

	b := int64(batchSize)

	if b <= 0 {

		return defaultLimit

	}

	if b > hardMax {

		return hardMax

	}

	return b

}

// syncDatabasePair 同步单个源库到目标库（含全部或指定表），只负责表结构准备和数据复制；索引恢复任务写入 pending，
// 由全量同步最外层在所有数据库对完成后统一串行执行。
// dbLevelRebuilt 表示上层已在库级别统一重建过目标库（DATABASE 级别 + 开启删除），
// 此时目标库已清空，ensureTargetTable 内的逐表 DROP TABLE 不再需要。
func (s *TaskService) syncDatabasePair(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime, sourceSchema, targetSchema string, specifiedTables []string, pending *[]pendingIndexRestore, dbLevelRebuilt bool) error {

	taskID := task.Config.ID

	// 确定要同步的表

	tables := append([]string{}, specifiedTables...)

	if len(tables) == 0 {

		logger.Info("[Task %s] 库级别同步：正在获取数据库 %s 的所有表...", taskID, sourceSchema)

		allTables, err := runtime.analyzer.GetAllTables(sourceSchema)

		if err != nil {

			errMsg := fmt.Sprintf("Failed to get tables for database %s: %v", sourceSchema, err)

			s.failTaskUnlessCancelled(ctx, taskID, errMsg)

			return fmt.Errorf("%s", errMsg)

		}

		for _, t := range allTables {

			tables = append(tables, t.TableName)

		}

		logger.Info("[Task %s] 找到 %d 个表: %v", taskID, len(tables), tables)

	}

	workerCount := task.Config.WorkerCount

	if workerCount <= 0 {

		workerCount = 4

	}

	// === 阶段1：串行同步所有表结构（集中 DDL，减少 read_only 切换次数） ===

	type tableReady struct {
		name         string
		targetName   string
		identity     *entity.TableIdentity
		savedIndexes []map[string]interface{}
	}

	logger.Info("[Task %s] 阶段1: 同步 %d 个表结构...", taskID, len(tables))

	ready := make([]tableReady, 0, len(tables))

	for i, tableName := range tables {

		if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {

			return err

		}

		targetTableName := s.resolveTableTargetName(task, sourceSchema, tableName, i)

		logger.Info("[Task %s] 确保目标表: %s.%s -> %s.%s", taskID, sourceSchema, tableName, targetSchema, targetTableName)

		identity, err := runtime.analyzer.AnalyzeTable(sourceSchema, tableName)

		if err != nil {

			errMsg := fmt.Sprintf("Failed to analyze table %s: %v", tableName, err)

			s.failTaskUnlessCancelled(ctx, taskID, errMsg)

			return fmt.Errorf("%s", errMsg)

		}

		// 库级别重建已清空整个目标库，逐表 DROP TABLE 不再需要；表级别保持原有删除行为。
		effectiveDropBeforeDDL := task.Config.EnableDropTableBeforeDDL && !dbLevelRebuilt
		savedIndexes, err := s.ensureTargetTable(ctx, runtime, sourceSchema, targetSchema, tableName, targetTableName, task.Config.OptimizeIndex, effectiveDropBeforeDDL, identity, task.Config.Mode)

		if err != nil {

			errMsg := fmt.Sprintf("Failed to ensure target table %s.%s -> %s.%s: %v", sourceSchema, tableName, targetSchema, targetTableName, err)

			s.failTaskUnlessCancelled(ctx, taskID, errMsg)

			return fmt.Errorf("%s", errMsg)

		}

		// 对已存在且未重建的目标表，ensureTargetTable 不会返回索引定义。
		// ALL 即使 optimize_index=false 也要延迟非 identity 唯一索引；FULL 仍仅在 optimize_index 时处理。
		needDefer := (task.Config.OptimizeIndex || task.Config.Mode == taskEntity.SyncModeAll) && len(savedIndexes) == 0
		if needDefer {
			logger.Info("[Task %s] Dropping deferred indexes for target table %s.%s (mode=%s optimize_index=%v)...", taskID, targetSchema, targetTableName, task.Config.Mode, task.Config.OptimizeIndex)
			indexes, dropErr := s.dropDeferredIndexes(ctx, runtime, targetSchema, targetTableName, identity, task.Config.Mode, task.Config.OptimizeIndex)
			if dropErr != nil {
				errMsg := fmt.Sprintf("Failed to drop deferred indexes for %s.%s: %v", targetSchema, targetTableName, dropErr)
				s.failTaskUnlessCancelled(ctx, taskID, errMsg)
				return fmt.Errorf("%s", errMsg)
			}
			savedIndexes = indexes
			logger.Info("[Task %s] Dropped %d deferred indexes from target table %s.%s", taskID, len(savedIndexes), targetSchema, targetTableName)
		}

		logger.Info("[Task %s] Target table %s.%s is ready", taskID, targetSchema, targetTableName)

		ready = append(ready, tableReady{name: tableName, targetName: targetTableName, identity: identity, savedIndexes: savedIndexes})

	}

	logger.Info("[Task %s] 阶段1完成：%d 个表结构就绪，开始同步数据...", taskID, len(ready))

	// === 阶段2：并发同步所有表数据 ===

	sem := make(chan struct{}, workerCount)

	var wg sync.WaitGroup

	errChan := make(chan error, len(ready))

	for _, r := range ready {

		wg.Add(1)

		go func(sourceTableName, targetTableName string, identity *entity.TableIdentity) {

			defer wg.Done()

			defer func() {

				if recovered := recover(); recovered != nil {

					logger.Error("[Task %s] Critical: Table %s sync panicked: %v\n%s", taskID, sourceTableName, recovered, debug.Stack())

					errChan <- fmt.Errorf("table %s panic: %v", sourceTableName, recovered)

				}

			}()

			sem <- struct{}{}

			defer func() { <-sem }()

			if s.isTaskStopped(taskID) {

				return

			}

			logger.Info("[Task %s] Syncing table data: %s.%s -> %s.%s", taskID, sourceSchema, sourceTableName, targetSchema, targetTableName)

			// Keep the existing sync code readable while making rename semantics explicit:
			// sourceTableName/tableName are used for reads and progress marks; targetIdentity/targetTableName are used for writes.
			tableName := sourceTableName
			targetIdentity := *identity
			targetIdentity.TableName = targetTableName

			// === 历史全量断点兼容：当前 resumeEnabled=false，不会用于续传 ===
			tableKey := fullSyncTableKey(sourceSchema, tableName)
			canResume := resumeEnabled(task)
			var tableProgress *taskEntity.TableSyncProgress
			if canResume {
				tableProgress = s.getTableProgress(taskID, tableKey)
				// 已完整同步过的表：直接跳过数据同步与索引处理（索引在首次完成时已恢复）。
				if tableProgress != nil && tableProgress.Done {
					logger.Info("[Task %s] Historical full-sync checkpoint: table %s.%s already completed, skipping", taskID, sourceSchema, tableName)
					s.completeTableProgress(taskID, sourceSchema, tableName)
					return
				}
			}

			// 标记该表开始同步（前端进度展示）
			tableStartTime := time.Now()
			s.startTableProgress(taskID, sourceSchema, tableName, 0)

			var tableProcessedRows int64

			readLimit := syncReadBatchLimit(task.Config.BatchSize)

			if task.Config.BatchSize > 0 && int64(task.Config.BatchSize) != readLimit {

				logger.Info("[Task %s] Table %s.%s: batch_size=%d capped to read limit %d per round-trip",

					taskID, sourceSchema, tableName, task.Config.BatchSize, readLimit)

			}

			const txCommitEveryN = 200 // 每 200 批 commit 一次，减少 fsync 频率、提高吞吐

			txCommitEveryNParallel := task.Config.TxCommitEveryNParallel
			if txCommitEveryNParallel <= 0 {
				txCommitEveryNParallel = 5 // 默认每 5 批 commit，减少锁持有时间，避免 lock wait timeout
			}

			const parallelWriteMaxRetries = 3 // 并行写入遇到锁超时/死锁时最大重试次数

			legacyCap, hardMax := s.intraTableConcurrencyCaps()

			intraWorkers := taskEntity.EffectiveIntraTableWorkers(task.Config.IntraTableWorkerCount, workerCount, legacyCap, hardMax)

			if task.Config.IntraTableWorkerCount > 0 {

				logger.Info("[Task %s] Table %s.%s: intra_table_worker_count effective=%d (table-level worker_count=%d)",

					taskID, sourceSchema, tableName, intraWorkers, workerCount)

			}

			// === 策略检测 ===

			cursorCols := identity.EffectiveCursorCols()

			if len(identity.IdentifyCols) > 1 && len(cursorCols) == 1 {
				logger.Info("[Task %s] Table %s.%s: composite PK with auto_increment, using cursor key `%s` (identify=%v)",
					taskID, sourceSchema, tableName, cursorCols[0], identity.IdentifyCols)
			}

			canParallelRange := identity.Strategy != entity.FullColumnsStrategy &&

				len(cursorCols) == 1 &&

				intraWorkers > 1 &&

				isNumericPKColumn(identity, cursorCols[0])

			canParallelSample := !canParallelRange &&

				identity.Strategy != entity.FullColumnsStrategy &&

				len(cursorCols) >= 1 &&

				intraWorkers > 1

			var minPK, maxPK int64

			if canParallelRange {

				if err := runtime.sourceDB.QueryRowContext(ctx,

					fmt.Sprintf("SELECT COALESCE(MIN(`%s`), 0), COALESCE(MAX(`%s`), -1) FROM `%s`.`%s`",

						cursorCols[0], cursorCols[0], sourceSchema, tableName),
				).Scan(&minPK, &maxPK); err != nil || maxPK < minPK {

					canParallelRange = false

					canParallelSample = identity.Strategy != entity.FullColumnsStrategy &&

						len(cursorCols) >= 1 && intraWorkers > 1

					logger.Info("[Task %s] Cannot get numeric PK range for %s.%s, trying sample parallel", taskID, sourceSchema, tableName)

				}

			}

			var sampleBoundaries []interface{}

			if canParallelSample {

				// 用 information_schema.TABLE_ROWS 替代 COUNT(*) 避免 88M 行的全索引扫描
				// 估算值有 10%~40% 误差，但只用于计算 step，不影响分片正确性
				estimatedReader := reader.NewReader(runtime.sourceDB, sourceSchema, tableName, identity)
				estimatedRows, estErr := estimatedReader.GetEstimatedCount(ctx)
				if estErr != nil || estimatedRows < int64(intraWorkers)*2 {

					canParallelSample = false

					logger.Info("[Task %s] Skipping sample parallel for %s.%s (estimatedRows=%d, err=%v)", taskID, sourceSchema, tableName, estimatedRows, estErr)

				} else {

					var bErr error

					sampleBoundaries, bErr = s.samplePKBoundariesImproved(ctx, runtime.sourceDB, sourceSchema, tableName, cursorCols, estimatedRows, intraWorkers)

					if bErr != nil {

						canParallelSample = false

						logger.Info("[Task %s] Boundary sampling failed for %s.%s: %v", taskID, sourceSchema, tableName, bErr)

					} else if len(sampleBoundaries) == 0 {

						canParallelSample = false

						logger.Info("[Task %s] Boundary sampling produced no usable split for %s.%s; falling back to sequential keyset", taskID, sourceSchema, tableName)

					} else if effectiveWorkers := len(sampleBoundaries) + 1; effectiveWorkers < intraWorkers {

						logger.Info("[Task %s] Boundary sampling reduced sample workers for %s.%s from %d to %d (usable boundaries=%d)",
							taskID, sourceSchema, tableName, intraWorkers, effectiveWorkers, len(sampleBoundaries))

						intraWorkers = effectiveWorkers

					}

				}

			}

			// === 修复 14：决策摘要日志，让运维直接看出"模式 / 是否无主键 / 是否并行" ===
			// 严格全局快照模式已下线，并行读取存在跨 worker 时间差；仅 ALL 模式会在全量前捕获
			// binlog 起点并由后续增量追平，FULL 模式不捕获位点、不启动增量。
			noPK := identity.Strategy == entity.FullColumnsStrategy
			if noPK {
				s.emitTableEvent(taskID, taskEntity.EventCodeNOPKSequentialFallback, sourceSchema, tableName,
					"no PK/UK table uses FullColumns strategy with single-worker streaming read (V1 path)",
					taskEntity.EventSeverityWarn,
					map[string]interface{}{"engine": "v1", "strategy": string(identity.Strategy)})
				logger.Warn("[NoPK][Task %s] Table %s.%s will use FullColumns strategy (no primary key, no unique key); falling back to single-worker streaming read + INSERT IGNORE; idempotency on re-run is best-effort, recommend adding a primary or unique key",
					taskID, sourceSchema, tableName)
			}
			skewNote := ", changes during sync will not be caught up (FULL mode)"
			if task.Config.Mode == taskEntity.SyncModeAll {
				skewNote = ", will be caught up by binlog (ALL mode)"
			}
			switch {
			case canParallelRange:
				logger.Info("[Task %s] Table %s.%s decision: parallel read enabled (range, workers=%d, strategy=%s, non-snapshot mode -> cross-worker time skew accepted%s)",
					taskID, sourceSchema, tableName, intraWorkers, identity.Strategy, skewNote)
			case canParallelSample:
				logger.Info("[Task %s] Table %s.%s decision: parallel read enabled (sample, workers=%d, strategy=%s, non-snapshot mode -> cross-worker time skew accepted%s)",
					taskID, sourceSchema, tableName, intraWorkers, identity.Strategy, skewNote)
			default:
				logger.Info("[Task %s] Table %s.%s decision: sequential read (workers=1, strategy=%s, no_pk=%t)",
					taskID, sourceSchema, tableName, identity.Strategy, noPK)
			}

			// === 执行同步 ===

			if identity.Strategy == entity.FullColumnsStrategy {

				// 无主键表：单协程流式读取 + 事务批量提交。
				// 全量中断后不续传，需重新准备目标端再启动新一轮全量。
				if canResume {
					s.initTableProgress(taskID, tableKey, "nopk", 1)
				}

				conn, err := runtime.targetDB.Conn(ctx)

				if err != nil {

					errMsg := fmt.Sprintf("Failed to get write connection for %s: %v", tableName, err)

					logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

					s.failTaskUnlessCancelled(ctx, taskID, errMsg)

					errChan <- err

					return

				}

				writeSessionLabel := fmt.Sprintf("%s.%s", targetSchema, targetTableName)
				if err := disableFullSyncWriteSession(ctx, conn, writeSessionLabel, task.Config.EnableSkipBinlog); err != nil {
					conn.Close()
					errMsg := fmt.Sprintf("Failed to configure target write session for `%s`.`%s`: %v", targetSchema, targetTableName, err)
					logger.Error("[Task %s] ERROR: %s", taskID, errMsg)
					s.failTaskUnlessCancelled(ctx, taskID, errMsg)
					errChan <- err
					return
				}
				defer func() {
					restoreFullSyncWriteSession(conn, writeSessionLabel, task.Config.EnableSkipBinlog)
					conn.Close()
				}()

				var curTx *sql.Tx

				var txW *writer.BatchWriter

				var txBatchN int

				var txStartMark string

				defer func() {

					if curTx != nil {

						curTx.Rollback()

					}

				}()

				doWrite := func(rows []map[string]interface{}, mark string) error {

					if curTx == nil {

						var e error

						curTx, e = conn.BeginTx(ctx, nil)

						if e != nil {

							return fmt.Errorf("begin tx at %s: %v", mark, e)

						}

						txW = writer.NewBatchWriterWithTx(curTx, &targetIdentity, task.Config.BatchSize, targetSchema)

						if s.auditLogger != nil {

							txW.SetAuditLogger(s.auditLogger, taskID, targetSchema, targetTableName)

						}

						// 全量同步统一使用普通 INSERT；目标端必须由用户保证为空，
						// 或通过 enable_drop_table_before_ddl 重建为空。
						txW.EnablePlainInsert()

						txBatchN = 0

						txStartMark = mark

					}

					if e := txW.WriteBatch(ctx, rows); e != nil {

						curTx.Rollback()

						curTx = nil

						return fmt.Errorf("write at %s (tx from %s) rolled back: %v", mark, txStartMark, e)

					}

					txBatchN++

					if txBatchN >= txCommitEveryN {

						if e := curTx.Commit(); e != nil {

							curTx = nil

							return fmt.Errorf("commit at %s (tx from %s): %v", mark, txStartMark, e)

						}

						curTx = nil

					}

					return nil

				}

				dr := reader.NewReader(runtime.sourceDB, sourceSchema, tableName, identity)

				for {

					if s.isTaskStopped(taskID) {

						return

					}

					rows, err := dr.ReadBatch(ctx, 0, readLimit)

					if err != nil {

						errMsg := fmt.Sprintf("Failed to read batch for `%s`.`%s`: %v", sourceSchema, tableName, err)

						logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

						s.failTaskUnlessCancelled(ctx, taskID, errMsg)

						errChan <- err

						return

					}

					if len(rows) == 0 {

						break

					}

					mark := fmt.Sprintf("%s.%s:%d", sourceSchema, tableName, tableProcessedRows+int64(len(rows)))

					if err := doWrite(rows, mark); err != nil {

						errMsg := fmt.Sprintf("Write failed for `%s`.`%s`: %v", sourceSchema, tableName, err)

						logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

						s.failTaskUnlessCancelled(ctx, taskID, errMsg)

						errChan <- err

						return

					}

					tableProcessedRows += int64(len(rows))

					taskTotalRows := s.incrementTaskProgress(taskID, int64(len(rows)), mark)
					s.updateTableProgress(taskID, sourceSchema, tableName, int64(len(rows)), time.Since(tableStartTime).Seconds(), task.Context.StartTime, taskTotalRows)

				}

				if curTx != nil {

					if err := curTx.Commit(); err != nil {

						errMsg := fmt.Sprintf("Final commit failed for `%s`.`%s` (from %s): %v", sourceSchema, tableName, txStartMark, err)

						logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

						s.failTaskUnlessCancelled(ctx, taskID, errMsg)

						errChan <- err

						return

					}

					curTx = nil

				}

			} else if canParallelRange {

				// 真正的keyset分页：每个worker从上一个worker的结束位置开始

				logger.Info("[Task %s] Table %s.%s: parallel keyset sync workers=%d",

					taskID, sourceSchema, tableName, intraWorkers)

				var syncWg sync.WaitGroup

				syncErrChan := make(chan error, intraWorkers)

				var atomicProcessed int64

				// minPK / maxPK 已在策略检测阶段获取，此处直接复用

				// 数值主键按 min/max 等分，避免采样边界导致有效 worker 塌缩

				span := maxPK - minPK + 1

				if span < int64(intraWorkers) {

					intraWorkers = int(span)

					if intraWorkers < 1 {

						intraWorkers = 1

					}

				}

				chunkSize := (span + int64(intraWorkers) - 1) / int64(intraWorkers)

				logger.Info("[Task %s] Table %s.%s: numeric range split min=%d max=%d workers=%d chunk=%d",

					taskID, sourceSchema, tableName, minPK, maxPK, intraWorkers, chunkSize)

				// 历史断点兼容：resumeEnabled=false 时不会读取分片断点。
				var rangeShardSeeds map[int]*taskEntity.ResumeKey
				if canResume {
					if tableProgress != nil && tableProgress.IntraWorkers == intraWorkers && len(tableProgress.ShardCursors) > 0 {
						rangeShardSeeds = make(map[int]*taskEntity.ResumeKey, len(tableProgress.ShardCursors))
						for k, v := range tableProgress.ShardCursors {
							rangeShardSeeds[k] = v
						}
					}
					s.initTableProgress(taskID, tableKey, "range", intraWorkers)
				}

				for w := 0; w < intraWorkers; w++ {

					syncWg.Add(1)

					go func(wIdx int) {

						defer syncWg.Done()

						defer func() {

							if r := recover(); r != nil {

								syncErrChan <- fmt.Errorf("w%d panic: %v", wIdx, r)

							}

						}()

						wReader := reader.NewRangeShardingReader(runtime.sourceDB, sourceSchema, tableName, identity)

						workerStart := minPK + int64(wIdx)*chunkSize

						if workerStart > maxPK {

							return

						}

						// 使用与 sample 路径统一的 (start, end] 语义，由 ReadBatchByKeyRange
						// 将边界写入 SQL WHERE 子句，让 MySQL 用原生类型判断：
						//   worker 0: start=nil, end=workerStart[1]  → pk <= workerStart[1]
						//   worker i: start=workerStart[i], end=workerStart[i+1]  → pk > workerStart[i] AND pk <= workerStart[i+1]
						//   worker last: start=workerStart[last], end=nil  → pk > workerStart[last]
						// 不使用 ±1、不做 Go 侧裁剪，完全由 MySQL 执行边界判断。
						var startBoundary, endBoundary interface{}

						if wIdx > 0 {
							startBoundary = interface{}(workerStart)
						}

						if wIdx < intraWorkers-1 {
							nextStart := minPK + int64(wIdx+1)*chunkSize
							endBoundary = interface{}(nextStart)
						}

						conn, err := runtime.targetDB.Conn(ctx)

						if err != nil {

							syncErrChan <- fmt.Errorf("w%d conn: %v", wIdx, err)

							return

						}

						writeSessionLabel := fmt.Sprintf("%s.%s w%d", targetSchema, targetTableName, wIdx)
						if err := disableFullSyncWriteSession(ctx, conn, writeSessionLabel, task.Config.EnableSkipBinlog); err != nil {
							conn.Close()
							syncErrChan <- err
							return
						}

						defer func() {
							restoreFullSyncWriteSession(conn, writeSessionLabel, task.Config.EnableSkipBinlog)
							conn.Close()
						}()

						if err := setFullSyncLockWaitTimeout(ctx, conn, writeSessionLabel); err != nil {
							syncErrChan <- err
							return
						}

						var curTx *sql.Tx

						var txW *writer.BatchWriter

						var txBatchN int

						var txStartMark string

						defer func() {

							if curTx != nil {

								curTx.Rollback()

							}

						}()

						doWrite := func(rows []map[string]interface{}, mark string, rkey *taskEntity.ResumeKey) error {

							for attempt := 0; ; attempt++ {

								if curTx == nil {

									var e error

									curTx, e = conn.BeginTx(ctx, nil)

									if e != nil {

										return fmt.Errorf("w%d begin tx at %s: %v", wIdx, mark, e)

									}

									txW = writer.NewBatchWriterWithTx(curTx, &targetIdentity, task.Config.BatchSize, targetSchema)

									if s.auditLogger != nil {

										txW.SetAuditLogger(s.auditLogger, taskID, targetSchema, targetTableName)

									}

									// 全量同步统一使用普通 INSERT；目标端必须由用户保证为空，
									// 或通过 enable_drop_table_before_ddl 重建为空。
									txW.EnablePlainInsert()

									txBatchN = 0

									txStartMark = mark

								}

								if e := txW.WriteBatch(ctx, rows); e != nil {

									curTx.Rollback()

									curTx = nil

									if isRetryableLockError(e) && attempt < parallelWriteMaxRetries {
										backoff := time.Duration(1+attempt) * time.Second
										logger.Info("[Task %s] w%d lock contention at %s, retry %d/%d after %v: %v",
											taskID, wIdx, mark, attempt+1, parallelWriteMaxRetries, backoff, e)
										select {
										case <-ctx.Done():
											return ctx.Err()
										case <-time.After(backoff):
										}
										continue
									}

									return fmt.Errorf("w%d write at %s (tx from %s) rolled back: %v", wIdx, mark, txStartMark, e)

								}

								break

							}

							txBatchN++

							if txBatchN >= txCommitEveryNParallel {

								if e := curTx.Commit(); e != nil {

									curTx = nil

									return fmt.Errorf("w%d commit at %s (tx from %s): %v", wIdx, mark, txStartMark, e)

								}

								curTx = nil

								// 历史断点兼容：当前不会推进本分片游标。
								if canResume {
									s.recordResumeCursor(taskID, tableKey, wIdx, rkey)
								}

							}

							return nil

						}

						lastID := startBoundary

						// 历史断点兼容：当前不会从旧分片游标继续。
						if canResume && rangeShardSeeds != nil {
							if rk := rangeShardSeeds[wIdx]; rk != nil {
								if cur, ok := resumeKeyToInt64(rk); ok && cur >= workerStart {
									lastID = interface{}(cur)
									logger.Info("[Task %s] Historical full-sync checkpoint: table %s.%s w%d continue after %d", taskID, sourceSchema, tableName, wIdx, cur)
								}
							}
						}

						rangeMark := fmt.Sprintf("%s.%s:w%d:keyset", sourceSchema, tableName, wIdx)

						for {

							if s.isTaskStopped(taskID) {

								return

							}

							// 上界写入 SQL，让 MySQL 用原生类型判断边界，与 sample 路径统一
							batch, err := wReader.ReadBatchByKeyRange(ctx, lastID, endBoundary, readLimit)

							if err != nil {

								syncErrChan <- fmt.Errorf("w%d read after %v: %v", wIdx, lastID, err)

								return

							}

							if len(batch) == 0 {

								logger.Info("[Task %s] w%d reached end of data at %v", taskID, wIdx, lastID)

								break

							}

							firstPK := batch[0][cursorCols[0]]

							lastPK := batch[len(batch)-1][cursorCols[0]]

							logger.Info("[Task %s] w%d processing batch: %s (%d rows) from %v to %v",

								taskID, wIdx, rangeMark, len(batch), firstPK, lastPK)

							if err := doWrite(batch, rangeMark, resumeKeyFromValue(lastPK)); err != nil {

								syncErrChan <- err

								return

							}

							n := int64(len(batch))

							atomic.AddInt64(&atomicProcessed, n)

							taskTotalRows := s.incrementTaskProgress(taskID, n, rangeMark)
							s.updateTableProgress(taskID, sourceSchema, tableName, n, time.Since(tableStartTime).Seconds(), task.Context.StartTime, taskTotalRows)

							lastID = lastPK

						}

						if curTx != nil {

							if err := curTx.Commit(); err != nil {

								syncErrChan <- fmt.Errorf("w%d final commit (from %s): %v", wIdx, txStartMark, err)

								curTx = nil

								return

							}

							curTx = nil

						}

					}(w)

				}

				syncWg.Wait()

				close(syncErrChan)

				if err := <-syncErrChan; err != nil {

					errMsg := fmt.Sprintf("Parallel range sync failed for `%s`.`%s`: %v", sourceSchema, tableName, err)

					logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

					s.failTaskUnlessCancelled(ctx, taskID, errMsg)

					errChan <- err

					return

				}

				tableProcessedRows = atomic.LoadInt64(&atomicProcessed)

			} else if canParallelSample {

				// 非数值单列主键 / 复合主键：采样边界 + 表内并行 keyset + 每 worker 独立事务批量提交。
				// 全量中断后不续传，需重新准备目标端再启动新一轮全量。
				if canResume {
					s.initTableProgress(taskID, tableKey, "sample", intraWorkers)
				}

				pkCol := cursorCols[0]

				logger.Info("[Task %s] Table %s.%s: parallel sample sync pk=%s workers=%d",
					taskID, sourceSchema, tableName, pkCol, intraWorkers)

				var syncWg sync.WaitGroup

				syncErrChan := make(chan error, intraWorkers)
				var atomicProcessed int64

				for w := 0; w < intraWorkers; w++ {

					syncWg.Add(1)

					go func(wIdx int) {

						defer syncWg.Done()

						defer func() {

							if r := recover(); r != nil {

								syncErrChan <- fmt.Errorf("w%d panic: %v", wIdx, r)

							}

						}()

						// sampleBoundaries[i] = 第 i+1 个 worker 的起始 key（前一 worker 的最后 key，exclusive）

						var startBoundary, endBoundary interface{}

						if wIdx > 0 {

							startBoundary = sampleBoundaries[wIdx-1]

						}

						if wIdx < intraWorkers-1 {

							endBoundary = sampleBoundaries[wIdx]

						}

						conn, err := runtime.targetDB.Conn(ctx)

						if err != nil {

							syncErrChan <- fmt.Errorf("w%d conn: %v", wIdx, err)

							return

						}

						writeSessionLabel := fmt.Sprintf("%s.%s w%d", targetSchema, targetTableName, wIdx)
						if err := disableFullSyncWriteSession(ctx, conn, writeSessionLabel, task.Config.EnableSkipBinlog); err != nil {
							conn.Close()
							syncErrChan <- err
							return
						}

						defer func() {
							restoreFullSyncWriteSession(conn, writeSessionLabel, task.Config.EnableSkipBinlog)
							conn.Close()
						}()

						if err := setFullSyncLockWaitTimeout(ctx, conn, writeSessionLabel); err != nil {
							syncErrChan <- err
							return
						}

						var curTx *sql.Tx

						var txW *writer.BatchWriter

						var txBatchN int

						var txStartMark string

						defer func() {

							if curTx != nil {

								curTx.Rollback()

							}

						}()

						doWrite := func(rows []map[string]interface{}, mark string) error {

							for attempt := 0; ; attempt++ {

								if curTx == nil {

									var e error

									curTx, e = conn.BeginTx(ctx, nil)

									if e != nil {

										return fmt.Errorf("w%d begin tx at %s: %v", wIdx, mark, e)

									}

									txW = writer.NewBatchWriterWithTx(curTx, &targetIdentity, task.Config.BatchSize, targetSchema)

									if s.auditLogger != nil {

										txW.SetAuditLogger(s.auditLogger, taskID, targetSchema, targetTableName)

									}

									// 全量同步统一使用普通 INSERT；目标端必须由用户保证为空，
									// 或通过 enable_drop_table_before_ddl 重建为空。
									txW.EnablePlainInsert()

									txBatchN = 0

									txStartMark = mark

								}

								if e := txW.WriteBatch(ctx, rows); e != nil {

									curTx.Rollback()

									curTx = nil

									if isRetryableLockError(e) && attempt < parallelWriteMaxRetries {
										backoff := time.Duration(1+attempt) * time.Second
										logger.Info("[Task %s] w%d lock contention at %s, retry %d/%d after %v: %v",
											taskID, wIdx, mark, attempt+1, parallelWriteMaxRetries, backoff, e)
										select {
										case <-ctx.Done():
											return ctx.Err()
										case <-time.After(backoff):
										}
										continue
									}

									return fmt.Errorf("w%d write at %s (tx from %s) rolled back: %v", wIdx, mark, txStartMark, e)

								}

								break

							}

							txBatchN++

							if txBatchN >= txCommitEveryNParallel {

								if e := curTx.Commit(); e != nil {

									curTx = nil

									return fmt.Errorf("w%d commit at %s (tx from %s): %v", wIdx, mark, txStartMark, e)

								}

								curTx = nil

							}

							return nil

						}

						wReader := reader.NewReader(runtime.sourceDB, sourceSchema, tableName, identity)

						lastID := startBoundary

						for {

							if s.isTaskStopped(taskID) {

								return

							}

							// 上界写入 SQL，让 MySQL 用原生类型/collation 判断，杜绝 Go 字符串比较的语义差异
							batchRows, err := wReader.ReadBatchByKeyRange(ctx, lastID, endBoundary, readLimit)

							if err != nil {

								syncErrChan <- fmt.Errorf("w%d read after %v: %v", wIdx, lastID, err)

								return

							}

							if len(batchRows) == 0 {

								break

							}

							lastRow := batchRows[len(batchRows)-1]
							firstPKVal := lastRow[pkCol]

							mark := fmt.Sprintf("%s.%s:w%d:pk=%v", sourceSchema, tableName, wIdx, firstPKVal)

							if err := doWrite(batchRows, mark); err != nil {

								syncErrChan <- err

								return

							}

							n := int64(len(batchRows))

							atomic.AddInt64(&atomicProcessed, n)

							taskTotalRows := s.incrementTaskProgress(taskID, n, mark)
							s.updateTableProgress(taskID, sourceSchema, tableName, n, time.Since(tableStartTime).Seconds(), task.Context.StartTime, taskTotalRows)

							// 更新 lastID：复合主键需要传完整的 []interface{} 给 ReadBatchByKeyRange
							if len(cursorCols) == 1 {
								lastID = firstPKVal
							} else {
								compositePK := make([]interface{}, len(cursorCols))
								for ci, col := range cursorCols {
									compositePK[ci] = lastRow[col]
								}
								lastID = compositePK
							}

							// 上界已在 SQL 中处理：MySQL 只返回 < endBoundary 的行，
							// 当 batch 不足 readLimit 时下一轮会返回空自动 break，无需 Go 侧裁剪

						}

						if curTx != nil {

							if err := curTx.Commit(); err != nil {

								syncErrChan <- fmt.Errorf("w%d final commit (from %s): %v", wIdx, txStartMark, err)

								curTx = nil

								return

							}

							curTx = nil

						}

					}(w)

				}

				syncWg.Wait()

				close(syncErrChan)

				if err := <-syncErrChan; err != nil {

					errMsg := fmt.Sprintf("Parallel sample sync failed for `%s`.`%s`: %v", sourceSchema, tableName, err)

					logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

					s.failTaskUnlessCancelled(ctx, taskID, errMsg)

					errChan <- err

					return

				}

				tableProcessedRows = atomic.LoadInt64(&atomicProcessed)

			} else {

				// 回退（单worker / 采样失败）：Keyset Pagination 顺序读取 + 事务批量提交

				conn, err := runtime.targetDB.Conn(ctx)

				if err != nil {

					errMsg := fmt.Sprintf("Failed to get write connection for %s: %v", tableName, err)

					logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

					s.failTaskUnlessCancelled(ctx, taskID, errMsg)

					errChan <- err

					return

				}

				writeSessionLabel := fmt.Sprintf("%s.%s", targetSchema, targetTableName)
				if err := disableFullSyncWriteSession(ctx, conn, writeSessionLabel, task.Config.EnableSkipBinlog); err != nil {
					conn.Close()
					errMsg := fmt.Sprintf("Failed to configure target write session for `%s`.`%s`: %v", targetSchema, targetTableName, err)
					logger.Error("[Task %s] ERROR: %s", taskID, errMsg)
					s.failTaskUnlessCancelled(ctx, taskID, errMsg)
					errChan <- err
					return
				}
				defer func() {
					restoreFullSyncWriteSession(conn, writeSessionLabel, task.Config.EnableSkipBinlog)
					conn.Close()
				}()

				var curTx *sql.Tx

				var txW *writer.BatchWriter

				var txBatchN int

				var txStartMark string

				defer func() {
					if curTx != nil {
						curTx.Rollback()
					}
				}()

				if canResume {
					s.initTableProgress(taskID, tableKey, "keyset", 1)
				}

				doWrite := func(rows []map[string]interface{}, mark string, rkey *taskEntity.ResumeKey) error {

					if curTx == nil {

						var e error

						curTx, e = conn.BeginTx(ctx, nil)

						if e != nil {

							return fmt.Errorf("begin tx at %s: %v", mark, e)

						}

						txW = writer.NewBatchWriterWithTx(curTx, &targetIdentity, task.Config.BatchSize, targetSchema)

						if s.auditLogger != nil {

							txW.SetAuditLogger(s.auditLogger, taskID, targetSchema, targetTableName)

						}

						// 全量同步统一使用普通 INSERT；目标端必须由用户保证为空，
						// 或通过 enable_drop_table_before_ddl 重建为空。
						txW.EnablePlainInsert()

						txBatchN = 0

						txStartMark = mark

					}

					if e := txW.WriteBatch(ctx, rows); e != nil {

						curTx.Rollback()

						curTx = nil

						return fmt.Errorf("write at %s (tx from %s) rolled back: %v", mark, txStartMark, e)

					}

					txBatchN++

					if txBatchN >= txCommitEveryN {

						if e := curTx.Commit(); e != nil {

							curTx = nil

							return fmt.Errorf("commit at %s (tx from %s): %v", mark, txStartMark, e)

						}

						curTx = nil

						// 历史断点兼容：当前不会推进整表游标。
						if canResume {
							s.recordResumeCursor(taskID, tableKey, -1, rkey)
						}

					}

					return nil

				}

				dr := reader.NewReader(runtime.sourceDB, sourceSchema, tableName, identity)

				var lastID interface{}

				// 历史断点兼容：当前不会从旧整表游标继续。
				if canResume && tableProgress != nil && tableProgress.Cursor != nil {
					lastID = lastIDFromResumeKey(tableProgress.Cursor, len(cursorCols))
					logger.Info("[Task %s] Historical full-sync checkpoint: table %s.%s keyset continue after %v", taskID, sourceSchema, tableName, lastID)
				}

				for {

					if s.isTaskStopped(taskID) {

						return

					}

					rows, err := dr.ReadBatchByKeys(ctx, lastID, readLimit)

					if err != nil {

						errMsg := fmt.Sprintf("Failed to read batch for `%s`.`%s` via keyset: %v", sourceSchema, tableName, err)

						logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

						s.failTaskUnlessCancelled(ctx, taskID, errMsg)

						errChan <- err

						return

					}

					if len(rows) == 0 {

						break

					}

					lastRow := rows[len(rows)-1]

					pkCols := cursorCols

					var rkey *taskEntity.ResumeKey

					if len(pkCols) == 1 {

						lastID = lastRow[pkCols[0]]

						rkey = resumeKeyFromValue(lastID)

					} else {

						vals := make([]interface{}, len(pkCols))

						for i, col := range pkCols {

							vals[i] = lastRow[col]

						}

						lastID = vals

						rkey = resumeKeyFromValues(vals)

					}

					mark := fmt.Sprintf("%s.%s:%v", sourceSchema, tableName, lastID)

					if err := doWrite(rows, mark, rkey); err != nil {

						errMsg := fmt.Sprintf("Write failed for `%s`.`%s`: %v", sourceSchema, tableName, err)

						logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

						s.failTaskUnlessCancelled(ctx, taskID, errMsg)

						errChan <- err

						return

					}

					tableProcessedRows += int64(len(rows))

					taskTotalRows := s.incrementTaskProgress(taskID, int64(len(rows)), mark)
					s.updateTableProgress(taskID, sourceSchema, tableName, int64(len(rows)), time.Since(tableStartTime).Seconds(), task.Context.StartTime, taskTotalRows)

				}

				if curTx != nil {

					if err := curTx.Commit(); err != nil {

						errMsg := fmt.Sprintf("Final commit failed for `%s`.`%s` (from %s): %v", sourceSchema, tableName, txStartMark, err)

						logger.Error("[Task %s] ERROR: %s", taskID, errMsg)

						s.failTaskUnlessCancelled(ctx, taskID, errMsg)

						errChan <- err

						return

					}

					curTx = nil

				}

			}

			if s.isTaskStopped(taskID) {
				return
			}

			// 该表全量同步完成：记录断点为 Done，重启时直接跳过。
			if canResume {
				s.markTableDone(taskID, tableKey)
			}

			logger.Info("[Task %s] Table %s.%s -> %s.%s completed, processed %d rows", taskID, sourceSchema, tableName, targetSchema, targetTableName, tableProcessedRows)

			s.completeTableProgress(taskID, sourceSchema, tableName)
			s.refreshOverallProgress(taskID, task.Context.StartTime, s.getTaskTotalRows(taskID))

		}(r.name, r.targetName, r.identity)

	}

	wg.Wait()

	close(errChan)

	if len(errChan) > 0 {

		return <-errChan

	}

	if pending != nil {
		for _, r := range ready {
			if len(r.savedIndexes) == 0 {
				continue
			}
			*pending = append(*pending, pendingIndexRestore{
				targetSchema: targetSchema,
				targetTable:  r.targetName,
				indexes:      append([]map[string]interface{}(nil), r.savedIndexes...),
			})
		}
	}

	return nil

}

// executeIncrementalSync 执行增量同步

// runBoundedCatchUp 从已持久化的 checkpoint（P0）回放到 until（P1），成功落盘后返回。
// checkpoint 保存失败不会标记 catch-up 完成；binlog 不可用时 fail-closed。
func (s *TaskService) runBoundedCatchUp(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime, until mysql.Position) error {
	if task == nil || runtime == nil {
		return fmt.Errorf("nil task/runtime for bounded catch-up")
	}
	if until.Name == "" {
		return fmt.Errorf("empty until position for bounded catch-up")
	}
	taskID := task.Config.ID

	cfg := s.config
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	resolvedSourceSchema := s.resolveSourceSchema(task)
	sourceHost := cfg.Datasource.Host
	sourcePort := cfg.Datasource.Port
	sourceUsername := cfg.Datasource.Username
	sourcePassword := cfg.Datasource.Password
	if task.Config.SourceDB != nil {
		sourceHost = task.Config.SourceDB.Host
		sourcePort = task.Config.SourceDB.Port
		sourceUsername = task.Config.SourceDB.Username
		sourcePassword = task.Config.SourceDB.Password
	}
	targetSchema := task.Config.TargetSchema
	if targetSchema == "" {
		targetSchema = resolvedSourceSchema
	}

	syncConfig := &syncApp.SyncConfig{
		TaskID:          taskID,
		SourceHost:      sourceHost,
		SourcePort:      sourcePort,
		SourceUsername:  sourceUsername,
		SourcePassword:  sourcePassword,
		SourceSchema:    resolvedSourceSchema,
		TargetSchema:    targetSchema,
		SourceDatabases: task.Config.SourceDatabases,
		TargetDatabases: task.Config.TargetDatabases,
		Tables:          task.Config.Tables,
		BatchSize:       task.Config.BatchSize,
		ServerID:        generateServerID(taskID + ":catchup"),
		SinkConfigs:     task.Config.SinkConfigs,
		UntilPosition:   &until,
	}

	incrSync := syncApp.NewIncrementalSyncService(runtime.sourceDB, runtime.targetDB, runtime.analyzer, s.checkpointManager)
	incrSync.SetPositionPersister(func(id string, pos mysql.Position) {
		s.updateSyncPhase(id, func(t *taskEntity.SyncTask) {
			t.Context.FullSyncCatchupPosition = formatBinlogPosition(pos)
			t.Context.LastIncrementalPosition = formatBinlogPosition(pos)
			t.Context.LastUpdateTime = time.Now()
		})
	})

	s.mu.Lock()
	s.incrementalSyncs[taskID] = incrSync
	s.mu.Unlock()
	defer func() {
		incrSync.Stop()
		s.mu.Lock()
		if cur, ok := s.incrementalSyncs[taskID]; ok && cur == incrSync {
			delete(s.incrementalSyncs, taskID)
		}
		s.mu.Unlock()
	}()

	if err := incrSync.Start(ctx, taskID, syncConfig); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.isTaskStopped(taskID) {
		return fmt.Errorf("task stopped during bounded catch-up")
	}
	if !incrSync.CatchUpCompleted() {
		// Start 在 ctx cancel 时也可能返回 nil；未达 P1 不得继续。
		return fmt.Errorf("bounded catch-up exited before reaching P1 %s", formatBinlogPosition(until))
	}
	return nil
}

func (s *TaskService) executeIncrementalSync(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime) {

	taskID := task.Config.ID

	// 修复 14：进入增量前打印一次"接管前快照"，便于追溯起始位点和上次完成的全量
	logger.Info("[Task %s] Entering incremental sync (phase=%q, full_sync_completed_at=%v, full_sync_start_position=%q, last_incremental_position=%q)",
		taskID, task.Context.SyncPhase, task.Context.FullSyncCompletedAt,
		task.Context.FullSyncStartPosition, task.Context.LastIncrementalPosition,
	)

	if runtime == nil || runtime.sourceDB == nil || runtime.targetDB == nil || runtime.analyzer == nil {

		logger.Error("[Task %s] Error: runtime is nil, cannot start incremental sync", taskID)

		s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, "task runtime is nil")

		return

	}

	// 获取配置信息

	cfg := s.config

	if cfg == nil {

		logger.Error("[Task %s] Error: config is nil, cannot start incremental sync", taskID)

		s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, "config is nil")

		return

	}

	// 获取源数据库配置

	resolvedSourceSchema := s.resolveSourceSchema(task)

	sourceHost := cfg.Datasource.Host

	sourcePort := cfg.Datasource.Port

	sourceUsername := cfg.Datasource.Username

	sourcePassword := cfg.Datasource.Password

	// 如果任务配置中有自定义源数据库，使用任务配置

	if task.Config.SourceDB != nil {

		sourceHost = task.Config.SourceDB.Host

		sourcePort = task.Config.SourceDB.Port

		sourceUsername = task.Config.SourceDB.Username

		sourcePassword = task.Config.SourceDB.Password

	}

	if resolvedSourceSchema == "" && len(task.Config.SourceDatabases) == 0 {

		logger.Error("[Task %s] Error: source schema is required for single-database incremental sync", taskID)

		s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, "source schema is required for single-database incremental sync")

		return

	}

	logger.Info("[Task %s] Starting incremental sync for schema: %s, tables: %v", taskID, resolvedSourceSchema, task.Config.Tables)

	// 确定目标 schema（与 executeFullSync 保持一致）

	targetSchema := task.Config.TargetSchema

	if targetSchema == "" {

		targetSchema = resolvedSourceSchema

	}

	// 创建增量同步配置

	syncConfig := &syncApp.SyncConfig{

		TaskID: taskID,

		SourceHost: sourceHost,

		SourcePort: sourcePort,

		SourceUsername: sourceUsername,

		SourcePassword: sourcePassword,

		SourceSchema: resolvedSourceSchema,

		TargetSchema: targetSchema,

		SourceDatabases: task.Config.SourceDatabases,

		TargetDatabases: task.Config.TargetDatabases,

		Tables: task.Config.Tables,

		BatchSize: task.Config.BatchSize,

		ServerID: generateServerID(taskID),

		SinkConfigs: task.Config.SinkConfigs,
	}

	// 创建增量同步服务

	incrSync := syncApp.NewIncrementalSyncService(

		runtime.sourceDB,

		runtime.targetDB,

		runtime.analyzer,

		s.checkpointManager,
	)

	// 注入节流型位点回写回调（修复 4：把"最近落库位点"冗余到任务存档，方便恢复/审计）。
	// 5 秒间隔足够把"任务存档里的位点"控制在合理新鲜度，又不会被每事件存储 IO 拖垮。
	incrSync.SetPositionPersister(s.makeThrottledIncrementalPositionPersister(5 * time.Second))

	// ALL 无 PK/UK：best-effort；基线与增量重叠窗口可能重复 INSERT。
	if task.Config.Mode == taskEntity.SyncModeAll {
		logger.Warn("[Task %s] ALL mode no-PK/UK tables use best-effort consistency (consistency=best_effort reason=no_primary_or_unique_key)", taskID)
	}

	// 保存到映射中

	s.mu.Lock()

	s.incrementalSyncs[taskID] = incrSync

	s.mu.Unlock()

	// === 修复 12：统一资源清理 ===
	// 无论 Start 成功阻塞返回、立刻报错、panic，都要从 incrementalSyncs map 移除条目，
	// 避免 PauseTask/Close/DeleteTask 误以为还有活跃的订阅。
	defer func() {
		s.mu.Lock()
		if cur, ok := s.incrementalSyncs[taskID]; ok && cur == incrSync {
			delete(s.incrementalSyncs, taskID)
		}
		s.mu.Unlock()
	}()

	// === 修复 4/14：标记增量阶段已启动并打"接管成功"日志 ===
	// 注意：MarkIncrementalStarted 在 Start() 之前调用是有意的——Start() 会阻塞直到订阅退出，
	// 此时任务确实已经进入"增量已接管"语义；即便 Start() 立刻报错，下方失败分支会回滚到 FullCompleted。
	s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
		t.MarkIncrementalStarted()
	})

	s.emitPhase(taskID, taskEntity.EventCodePhaseIncrementalStarted, "INCREMENTAL_STARTED",
		"增量同步已接管，开始订阅 binlog")

	logger.Info("[Task %s] Incremental sync taking over: subscribing binlog from saved checkpoint (phase=INCREMENTAL_STARTED)", taskID)

	// 启动增量同步

	if err := incrSync.Start(ctx, taskID, syncConfig); err != nil {

		logger.Error("[Task %s] Failed to start incremental sync: %v", taskID, err)

		// 启动失败 → 回退阶段到 FULL_COMPLETED（如果全量曾完成）或 INIT，避免下次启动误认为增量在跑
		s.updateSyncPhase(taskID, func(t *taskEntity.SyncTask) {
			if t.Context.FullSyncCompletedAt != nil {
				t.Context.SyncPhase = taskEntity.SyncPhaseFullCompleted
			} else {
				t.Context.SyncPhase = taskEntity.SyncPhaseInit
			}
		})

		// 主动 Stop，确保订阅、写入连接、审计 goroutine 等内部资源全部释放
		incrSync.Stop()

		s.failTaskUnlessCancelled(ctx, taskID, err.Error())

		return

	}

	logger.Info("[Task %s] Incremental sync exited cleanly (subscriber stopped)", taskID)

}

// generateServerID 生成唯一的ServerID用于binlog订阅

func generateServerID(taskID string) uint32 {

	// 简单的哈希算法生成ServerID

	hash := uint32(0)

	for _, c := range taskID {

		hash = hash*31 + uint32(c)

	}

	// 确保在合理范围内 (1 - 2^32-1)

	if hash == 0 {

		hash = 1

	}

	return hash

}

// withDDL 在只读目标库上安全执行 DDL：若 readOnlyManager 存在则临时关闭 read_only 再恢复，否则直接执行

func (s *TaskService) withDDL(runtime *taskRuntime, fn func() error) error {

	if runtime != nil && runtime.readOnlyManager != nil {

		return runtime.readOnlyManager.WithWriteAccess(fn)

	}

	return fn()

}

// preferSchemaLockLost 在 ctx 因丢锁取消时优先返回类型化 cause，否则用 %w 包装原错误。
// DDL 执行中心跳取消时 ExecContext 常返回 context.Canceled，直接 %v 包装会丢失 ErrSchemaLockLost。
func preferSchemaLockLost(ctx context.Context, err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	if lost := fullload.SchemaLockLostError(ctx); lost != nil {
		return lost
	}
	all := make([]any, 0, len(args)+1)
	all = append(all, args...)
	all = append(all, err)
	return fmt.Errorf(format+": %w", all...)
}

func (s *TaskService) dropTargetTableIfNeeded(ctx context.Context, conn *sql.Conn, schema, tableName string, enabled bool) error {
	if !enabled {
		return nil
	}
	if conn == nil {
		return fmt.Errorf("target connection is nil")
	}
	if strings.TrimSpace(schema) == "" || strings.TrimSpace(tableName) == "" {
		return nil
	}
	if err := fullload.SchemaLockLostError(ctx); err != nil {
		return err
	}
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", schema, tableName))
	if err != nil {
		return preferSchemaLockLost(ctx, err, "failed to drop target table %s.%s", schema, tableName)
	}
	logger.Info("[Task] Dropped target table %s.%s before DDL", schema, tableName)
	return nil
}

func (s *TaskService) applyDropBeforeDDL(ctx context.Context, conn *sql.Conn, schema, tableName string, enabled bool) error {
	return s.dropTargetTableIfNeeded(ctx, conn, schema, tableName, enabled)
}

// rebuildTargetDatabases 在库级别同步且开启"DDL 前删除"时统一重建目标库：
// 对每个唯一目标库依次执行 DROP DATABASE IF EXISTS 与 CREATE DATABASE IF NOT EXISTS
// （utf8mb4 / utf8mb4_unicode_ci）。任一步失败立即返回错误，调用方应据此终止全量同步。
// 仅在全量阶段、任何目标表 DDL/数据写入前调用一次；增量阶段不调用。
func (s *TaskService) rebuildTargetDatabases(ctx context.Context, runtime *taskRuntime, taskID string, targetSchemas []string) error {

	if runtime == nil || runtime.targetDB == nil {

		return fmt.Errorf("task runtime is not initialized")

	}

	if err := fullload.SchemaLockLostError(ctx); err != nil {
		return err
	}

	conn, err := runtime.targetDB.Conn(ctx)

	if err != nil {

		return preferSchemaLockLost(ctx, err, "failed to get target connection for database rebuild")

	}

	defer conn.Close()

	// 整体重建包在单次 withDDL 内，最小化 read_only 切换次数
	seen := make(map[string]struct{})

	return s.withDDL(runtime, func() error {

		for _, schema := range targetSchemas {

			if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {
				return err
			}

			if strings.TrimSpace(schema) == "" {

				continue

			}

			if _, ok := seen[schema]; ok {

				continue

			}

			seen[schema] = struct{}{}

			if _, e := conn.ExecContext(ctx,

				fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", schema)); e != nil {

				return preferSchemaLockLost(ctx, e, "failed to drop target database %s", schema)

			}

			logger.Info("[Task %s] Dropped target database %s before full sync (database-level rebuild)", taskID, schema)

			if _, e := conn.ExecContext(ctx,

				fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", schema)); e != nil {

				return preferSchemaLockLost(ctx, e, "failed to recreate target database %s", schema)

			}

			logger.Info("[Task %s] Recreated target database %s (utf8mb4/utf8mb4_unicode_ci)", taskID, schema)

		}

		return nil

	})

}

// ensureTargetTable 确保目标表存在

func (s *TaskService) ensureTargetTable(ctx context.Context, runtime *taskRuntime, sourceSchema, targetSchema, sourceTableName, targetTableName string, optimizeIndex bool, dropBeforeDDL bool, identity *entity.TableIdentity, mode taskEntity.SyncMode) ([]map[string]interface{}, error) {

	if runtime == nil || runtime.sourceDB == nil || runtime.targetDB == nil {

		return nil, fmt.Errorf("task runtime is not initialized")

	}

	if err := fullload.SchemaLockLostError(ctx); err != nil {
		return nil, err
	}

	targetDB := runtime.targetDB

	sourceDB := runtime.sourceDB

	// 首先确保目标数据库存在：先用 SELECT 查询（只读操作），避免在 read_only 目标库上无谓执行 DDL

	var dbExists string

	err := targetDB.QueryRowContext(ctx,

		"SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?", targetSchema,
	).Scan(&dbExists)

	if err == sql.ErrNoRows {

		// 数据库不存在，临时解除只读后创建

		if err = s.withDDL(runtime, func() error {

			_, e := targetDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", targetSchema))

			return e

		}); err != nil {

			return nil, preferSchemaLockLost(ctx, err, "failed to create target database %s", targetSchema)

		}

		logger.Info("[Task] Target database '%s' created", targetSchema)

	} else if err != nil {

		return nil, fmt.Errorf("failed to check target database %s: %v", targetSchema, err)

	}

	// err == nil 说明数据库已存在，直接跳过创建

	// 检查目标表是否存在

	var tableNameCheck string

	err = targetDB.QueryRowContext(ctx,

		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", targetSchema, targetTableName,
	).Scan(&tableNameCheck)

	tableExists := err == nil
	if err != nil && err != sql.ErrNoRows {

		// 查询出错

		return nil, fmt.Errorf("failed to check target table %s.%s: %v", targetSchema, targetTableName, err)

	}

	if tableExists {
		logger.Info("[Task] Target table %s.%s already exists", targetSchema, targetTableName)
		if !dropBeforeDDL {
			logger.Info("[Task] Skipping creation for existing target table %s.%s (drop-before-DDL disabled)", targetSchema, targetTableName)
			return nil, nil
		}
		logger.Info("[Task] drop-before-DDL enabled, target table %s.%s will be recreated", targetSchema, targetTableName)
	} else {
		logger.Info("[Task] Creating target table %s.%s (from %s.%s)", targetSchema, targetTableName, sourceSchema, sourceTableName)
	}

	// 获取一个专用目标连接，临时关闭外键检查，避免因父表尚未创建而导致 DDL 失败
	tgtDDLConn, tgtConnErr := targetDB.Conn(ctx)
	if tgtConnErr != nil {
		return nil, preferSchemaLockLost(ctx, tgtConnErr, "failed to get target connection for DDL")
	}
	defer func() {
		// 会话收尾不受父 ctx 取消影响，避免丢锁取消后无法恢复 FOREIGN_KEY_CHECKS。
		cleanupCtx := context.WithoutCancel(ctx)
		tgtDDLConn.ExecContext(cleanupCtx, "SET SESSION FOREIGN_KEY_CHECKS=1")
		tgtDDLConn.Close()
	}()
	if _, err := tgtDDLConn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0"); err != nil {
		return nil, preferSchemaLockLost(ctx, err, "failed to disable FOREIGN_KEY_CHECKS for DDL")
	}

	// 方法1：尝试在已禁用外键检查的目标连接上用 CREATE TABLE ... LIKE 创建表（源和目标在同一服务器时有效）。
	// ALL 即使 optimize_index=false 也需要后续剥离非 identity 唯一索引，因此跳过 LIKE，走 SHOW CREATE 路径。
	useCreateLike := sourceDB != nil && !optimizeIndex && mode != taskEntity.SyncModeAll
	if useCreateLike {

		tryErr := s.withDDL(runtime, func() error {

			if err := s.dropTargetTableIfNeeded(ctx, tgtDDLConn, targetSchema, targetTableName, dropBeforeDDL); err != nil {
				return err
			}

			_, e := tgtDDLConn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`",

				targetSchema, targetTableName, sourceSchema, sourceTableName))

			if e != nil {
				return preferSchemaLockLost(ctx, e, "CREATE TABLE LIKE `%s`.`%s`", targetSchema, targetTableName)
			}
			return nil

		})

		if tryErr == nil {

			logger.Info("[Task] Successfully created target table %s.%s (using CREATE TABLE LIKE)", targetSchema, targetTableName)

			return nil, nil

		}

		// 丢锁属于 fail-closed，不得回退到 SHOW CREATE 路径。
		if errors.Is(tryErr, fullload.ErrSchemaLockLost) {
			return nil, tryErr
		}
		if lost := fullload.SchemaLockLostError(ctx); lost != nil {
			return nil, lost
		}

		logger.Error("[Task] Failed to create table using CREATE TABLE LIKE: %v", tryErr)

	}

	// 方法2：获取源表的CREATE TABLE语句并在目标数据库执行
	// 使用独立连接并临时去掉 ANSI_QUOTES，确保 DDL 输出用反引号而非双引号

	var createSQL string

	ddlConn, connErr := sourceDB.Conn(ctx)
	if connErr != nil {
		return nil, fmt.Errorf("failed to get source connection for DDL: %v", connErr)
	}
	defer ddlConn.Close()

	// 去掉 ANSI_QUOTES，避免 SHOW CREATE TABLE 输出双引号列名导致目标库执行失败
	_, _ = ddlConn.ExecContext(ctx,
		"SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'ANSI_QUOTES', '')")

	var showCreateTableName string
	err = ddlConn.QueryRowContext(ctx,

		fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", sourceSchema, sourceTableName),
	).Scan(&showCreateTableName, &createSQL)

	if err != nil {

		return nil, fmt.Errorf("failed to get CREATE TABLE statement for %s.%s: %v", sourceSchema, sourceTableName, err)

	}

	var savedIndexes []map[string]interface{}

	// FULL：optimize_index=true 时剥离全部非 PK 二级索引。
	// ALL：始终按 TableIdentity 保留 identity UK，并至少延迟其他唯一索引；
	//      非唯一索引仍由 optimize_index 控制。
	deferIndexes := optimizeIndex || mode == taskEntity.SyncModeAll
	if deferIndexes {

		savedIndexes, err = loadNonPrimaryKeyIndexes(sourceDB, sourceSchema, sourceTableName)

		if err != nil {

			return nil, fmt.Errorf("failed to load source indexes for %s.%s: %v", sourceSchema, sourceTableName, err)

		}

		// 在剥离 CREATE TABLE 中的二级索引前，先提取自增列集合。

		// 任何包含自增列的索引都会随 CREATE TABLE 保留在目标表上，因此不应再加入恢复列表，

		// 避免阶段 3 再次创建同名索引触发 MySQL 1061。

		autoIncrementColumns := extractAutoIncrementColumnsFromCreateSQL(strings.Split(createSQL, "\n"))

		savedIndexes = filterIndexesUsingAutoIncrementColumns(savedIndexes, autoIncrementColumns)
		savedIndexes = selectDeferredIndexes(savedIndexes, identity, mode, optimizeIndex)

		createSQL = stripIndexesByNameFromCreateSQL(createSQL, indexNamesSet(savedIndexes))

	}

	createSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` %s", targetSchema, targetTableName,

		extractTableDefinition(createSQL))

	// 在已关闭外键检查的专用连接上执行 DDL
	if err = s.withDDL(runtime, func() error {

		if err := s.dropTargetTableIfNeeded(ctx, tgtDDLConn, targetSchema, targetTableName, dropBeforeDDL); err != nil {
			return err
		}

		_, e := tgtDDLConn.ExecContext(ctx, createSQL)
		if e != nil {
			return preferSchemaLockLost(ctx, e, "CREATE TABLE `%s`.`%s`", targetSchema, targetTableName)
		}
		return nil

	}); err != nil {

		return nil, preferSchemaLockLost(ctx, err, "failed to create target table %s.%s", targetSchema, targetTableName)

	}

	logger.Info("[Task] Successfully created target table %s.%s (using CREATE TABLE statement)", targetSchema, targetTableName)

	return savedIndexes, nil

}

// extractTableDefinition 从CREATE TABLE语句中提取表定义部分

func extractTableDefinition(createSQL string) string {

	// 找到第一个 '(' 的位置

	startIdx := strings.Index(createSQL, "(")

	if startIdx == -1 {

		return createSQL

	}

	// 找到最后一个 ')' 的位置

	endIdx := strings.LastIndex(createSQL, ")")

	if endIdx == -1 {

		return createSQL

	}

	// 提取表定义部分（包括表选项）

	return createSQL[startIdx:]

}

func stripNonPrimaryIndexesFromCreateSQL(createSQL string) string {

	lines := strings.Split(createSQL, "\n")
	autoIncrementColumns := extractAutoIncrementColumnsFromCreateSQL(lines)

	filtered := make([]string, 0, len(lines))

	for i, line := range lines {

		trimmed := strings.TrimSpace(line)

		if i > 0 && i < len(lines)-1 && isSecondaryIndexDefinitionLine(trimmed) && !indexDefinitionUsesAnyColumn(trimmed, autoIncrementColumns) {

			continue

		}

		filtered = append(filtered, line)

	}

	for i := 1; i < len(filtered); i++ {

		if strings.HasPrefix(strings.TrimSpace(filtered[i]), ")") {

			filtered[i-1] = trimTrailingComma(filtered[i-1])

		}

	}

	return strings.Join(filtered, "\n")

}

// stripIndexesByNameFromCreateSQL 从 CREATE TABLE 中剥离指定名称的二级索引定义。
// 自增列相关索引仍始终保留（与 stripNonPrimaryIndexesFromCreateSQL 一致）。
func stripIndexesByNameFromCreateSQL(createSQL string, dropNames map[string]struct{}) string {
	if len(dropNames) == 0 {
		return createSQL
	}
	lines := strings.Split(createSQL, "\n")
	autoIncrementColumns := extractAutoIncrementColumnsFromCreateSQL(lines)
	filtered := make([]string, 0, len(lines))
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if i > 0 && i < len(lines)-1 && isSecondaryIndexDefinitionLine(trimmed) &&
			!indexDefinitionUsesAnyColumn(trimmed, autoIncrementColumns) {
			if name := secondaryIndexNameFromDefinitionLine(trimmed); name != "" {
				if _, drop := dropNames[name]; drop {
					continue
				}
			}
		}
		filtered = append(filtered, line)
	}
	for i := 1; i < len(filtered); i++ {
		if strings.HasPrefix(strings.TrimSpace(filtered[i]), ")") {
			filtered[i-1] = trimTrailingComma(filtered[i-1])
		}
	}
	return strings.Join(filtered, "\n")
}

// secondaryIndexNameFromDefinitionLine 从 CREATE TABLE 的索引定义行解析索引名。
// 识别前缀 UNIQUE KEY/INDEX、FULLTEXT KEY/INDEX、SPATIAL KEY/INDEX、KEY、INDEX，
// 并提取反引号包裹的索引名；无名索引返回空串。
func secondaryIndexNameFromDefinitionLine(line string) string {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
	upper := strings.ToUpper(trimmed)
	prefixes := []string{
		"UNIQUE KEY ", "UNIQUE INDEX ",
		"FULLTEXT KEY ", "FULLTEXT INDEX ",
		"SPATIAL KEY ", "SPATIAL INDEX ",
		"KEY ", "INDEX ",
	}
	for _, p := range prefixes {
		if !strings.HasPrefix(upper, p) {
			continue
		}
		rest := strings.TrimSpace(trimmed[len(p):])
		if strings.HasPrefix(rest, "`") {
			end := strings.Index(rest[1:], "`")
			if end >= 0 {
				return rest[1 : 1+end]
			}
		}
		return ""
	}
	return ""
}

// indexNamesSet 把索引列表（[]map[string]interface{}）转为以 "name" 键值为准的名字集合。
func indexNamesSet(indexes []map[string]interface{}) map[string]struct{} {
	out := make(map[string]struct{}, len(indexes))
	for _, idx := range indexes {
		name, _ := idx["name"].(string)
		if name != "" {
			out[name] = struct{}{}
		}
	}
	return out
}

// parseIndexColumnNames 解析 information_schema.STATISTICS 拼接的列名字符串（如 "`a`,`b`(10)"）。
// 输入格式假设：列名以反引号包裹、列间逗号分隔，可能带 (N) 前缀长度；返回纯列名列表。
func parseIndexColumnNames(columns string) []string {
	if strings.TrimSpace(columns) == "" {
		return nil
	}
	parts := strings.Split(columns, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// `col` or `col`(N)
		if i := strings.Index(p, "`"); i >= 0 {
			rest := p[i+1:]
			if j := strings.Index(rest, "`"); j >= 0 {
				out = append(out, rest[:j])
				continue
			}
		}
		out = append(out, strings.TrimSpace(strings.Split(p, "(")[0]))
	}
	return out
}

// indexMatchesIdentifyCols 判断索引列是否与 identity 列完全匹配（顺序敏感，大小写不敏感）。
// 是 shouldDeferIndex 保留 identity UK 的核心判断：identity 索引不延迟。
func indexMatchesIdentifyCols(idx map[string]interface{}, identifyCols []string) bool {
	if len(identifyCols) == 0 {
		return false
	}
	cols, _ := idx["columns"].(string)
	got := parseIndexColumnNames(cols)
	if len(got) != len(identifyCols) {
		return false
	}
	for i := range identifyCols {
		if !strings.EqualFold(got[i], identifyCols[i]) {
			return false
		}
	}
	return true
}

// indexNonUnique 读取 idx["non_unique"] 并转为 int，处理 interface{} 类型断言（int/int64/float64）；
// 默认返回 1（视为非唯一），保证无法判断时保守延迟。
func indexNonUnique(idx map[string]interface{}) int {
	switch v := idx["non_unique"].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 1
	}
}

// shouldDeferIndex 判断索引是否应在基线/catch-up 期间延迟创建。
// ALL：始终保留 identity UK；始终延迟其他唯一索引；非唯一索引跟随 optimize_index。
// FULL：保持原 optimize_index 语义（true 时延迟全部非 PK 二级索引）。
func shouldDeferIndex(idx map[string]interface{}, identity *entity.TableIdentity, mode taskEntity.SyncMode, optimizeIndex bool) bool {
	isUnique := indexNonUnique(idx) == 0
	if mode == taskEntity.SyncModeAll {
		if isUnique && identity != nil && identity.Strategy == entity.UKStrategy &&
			indexMatchesIdentifyCols(idx, identity.IdentifyCols) {
			return false
		}
		if isUnique {
			return true
		}
		return optimizeIndex
	}
	return optimizeIndex
}

// selectDeferredIndexes 按 shouldDeferIndex 过滤索引列表，返回需延迟创建的索引子集。
func selectDeferredIndexes(indexes []map[string]interface{}, identity *entity.TableIdentity, mode taskEntity.SyncMode, optimizeIndex bool) []map[string]interface{} {
	if len(indexes) == 0 {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(indexes))
	for _, idx := range indexes {
		if shouldDeferIndex(idx, identity, mode, optimizeIndex) {
			out = append(out, idx)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// mergePendingIndexRestores 将 extra 合并到 base，同一 schema.table 的索引按名称去重（8830199 修复点）。
// 旧版按表去重会导致 resume 与 PUBLISHED 跳过路径重复入队同一索引，使 ALTER TABLE 重建同名索引失败；
// 现改为同表按索引名合并，保证每个索引只恢复一次。
func mergePendingIndexRestores(base, extra []pendingIndexRestore) []pendingIndexRestore {
	if len(extra) == 0 {
		return base
	}

	indexNameOf := func(idx map[string]interface{}) string {
		name, _ := idx["name"].(string)
		return name
	}
	mergeIndexes := func(dst, src []map[string]interface{}) []map[string]interface{} {
		if len(src) == 0 {
			return dst
		}
		seen := make(map[string]struct{}, len(dst))
		for _, idx := range dst {
			if name := indexNameOf(idx); name != "" {
				seen[name] = struct{}{}
			}
		}
		out := append([]map[string]interface{}(nil), dst...)
		for _, idx := range src {
			name := indexNameOf(idx)
			if name == "" {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			out = append(out, idx)
		}
		return out
	}

	tableKey := func(schema, table string) string {
		return schema + "." + table
	}
	byTable := make(map[string]int, len(base)+len(extra))
	out := make([]pendingIndexRestore, 0, len(base)+len(extra))
	for _, p := range base {
		key := tableKey(p.targetSchema, p.targetTable)
		if i, ok := byTable[key]; ok {
			out[i].indexes = mergeIndexes(out[i].indexes, p.indexes)
			continue
		}
		byTable[key] = len(out)
		out = append(out, pendingIndexRestore{
			targetSchema: p.targetSchema,
			targetTable:  p.targetTable,
			indexes:      append([]map[string]interface{}(nil), p.indexes...),
		})
	}
	for _, p := range extra {
		key := tableKey(p.targetSchema, p.targetTable)
		if i, ok := byTable[key]; ok {
			out[i].indexes = mergeIndexes(out[i].indexes, p.indexes)
			continue
		}
		byTable[key] = len(out)
		out = append(out, pendingIndexRestore{
			targetSchema: p.targetSchema,
			targetTable:  p.targetTable,
			indexes:      append([]map[string]interface{}(nil), p.indexes...),
		})
	}
	return out
}

// collectMissingDeferredIndexes 对比源/目标，收集目标上缺失的延迟索引（供 resume 与 PUBLISHED 跳过路径补齐）。
func (s *TaskService) collectMissingDeferredIndexes(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime, pairs []schemaPair, tablesBySource map[string][]string) ([]pendingIndexRestore, error) {
	if task == nil || runtime == nil || runtime.sourceDB == nil || runtime.targetDB == nil || runtime.analyzer == nil {
		return nil, fmt.Errorf("task runtime is not initialized")
	}
	var pending []pendingIndexRestore
	for _, p := range pairs {
		tables := tablesBySource[p.src]
		if task.Config.SyncLevel == taskEntity.SyncLevelTable && len(task.Config.Tables) > 0 && len(tables) == 0 {
			continue
		}
		if len(tables) == 0 {
			allTables, err := runtime.analyzer.GetAllTables(p.src)
			if err != nil {
				return nil, fmt.Errorf("list tables for %s: %w", p.src, err)
			}
			for _, t := range allTables {
				tables = append(tables, t.TableName)
			}
		}
		for i, tableName := range tables {
			if err := fullload.SchemaLockLostError(ctx); err != nil {
				return nil, err
			}
			targetTableName := s.resolveTableTargetName(task, p.src, tableName, i)
			identity, err := runtime.analyzer.AnalyzeTable(p.src, tableName)
			if err != nil {
				return nil, fmt.Errorf("analyze %s.%s: %w", p.src, tableName, err)
			}
			sourceIndexes, err := loadNonPrimaryKeyIndexes(runtime.sourceDB, p.src, tableName)
			if err != nil {
				return nil, fmt.Errorf("load source indexes %s.%s: %w", p.src, tableName, err)
			}
			deferred := selectDeferredIndexes(sourceIndexes, identity, task.Config.Mode, task.Config.OptimizeIndex)
			if len(deferred) == 0 {
				continue
			}
			targetIndexes, err := loadNonPrimaryKeyIndexes(runtime.targetDB, p.dst, targetTableName)
			if err != nil {
				// 目标表可能尚未创建（不应在 baselineDone resume 出现）；按缺失全部延迟索引处理失败。
				return nil, fmt.Errorf("load target indexes %s.%s: %w", p.dst, targetTableName, err)
			}
			present := indexNamesSet(targetIndexes)
			missing := make([]map[string]interface{}, 0, len(deferred))
			for _, idx := range deferred {
				name, _ := idx["name"].(string)
				if name == "" {
					continue
				}
				if _, ok := present[name]; !ok {
					missing = append(missing, idx)
				}
			}
			if len(missing) == 0 {
				continue
			}
			pending = append(pending, pendingIndexRestore{
				targetSchema: p.dst,
				targetTable:  targetTableName,
				indexes:      missing,
			})
		}
	}
	return pending, nil
}

func extractAutoIncrementColumnsFromCreateSQL(lines []string) map[string]struct{} {

	columns := make(map[string]struct{})

	for _, line := range lines {

		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "`") || !strings.Contains(strings.ToUpper(trimmed), "AUTO_INCREMENT") {
			continue
		}

		end := strings.Index(trimmed[1:], "`")
		if end < 0 {
			continue
		}

		columns[trimmed[1:1+end]] = struct{}{}

	}

	return columns

}

func indexDefinitionUsesAnyColumn(indexDefinition string, columns map[string]struct{}) bool {

	if len(columns) == 0 {

		return false

	}

	for column := range columns {

		if strings.Contains(indexDefinition, fmt.Sprintf("`%s`", column)) {

			return true

		}

	}

	return false

}

// indexColumnsUseAnyColumn 判断索引列字符串（如 "`col1`, `col2`"）是否包含给定列中的任意一个。

func indexColumnsUseAnyColumn(columns string, cols map[string]struct{}) bool {

	if len(cols) == 0 {

		return false

	}

	for c := range cols {

		if strings.Contains(columns, fmt.Sprintf("`%s`", c)) {

			return true

		}

	}

	return false

}

// filterIndexesUsingAutoIncrementColumns 从索引恢复列表中移除使用 AUTO_INCREMENT 列的索引。

// 这些索引会随 CREATE TABLE 保留在目标表上，不应重复恢复。

func filterIndexesUsingAutoIncrementColumns(indexes []map[string]interface{}, autoIncrementColumns map[string]struct{}) []map[string]interface{} {

	if len(autoIncrementColumns) == 0 {

		return indexes

	}

	filtered := make([]map[string]interface{}, 0, len(indexes))

	for _, idx := range indexes {

		cols, _ := idx["columns"].(string)

		if !indexColumnsUseAnyColumn(cols, autoIncrementColumns) {

			filtered = append(filtered, idx)

		}

	}

	return filtered

}

func isSecondaryIndexDefinitionLine(line string) bool {

	upper := strings.ToUpper(strings.TrimSuffix(strings.TrimSpace(line), ","))

	switch {

	case strings.HasPrefix(upper, "UNIQUE KEY "),
		strings.HasPrefix(upper, "UNIQUE INDEX "),
		strings.HasPrefix(upper, "FULLTEXT KEY "),
		strings.HasPrefix(upper, "FULLTEXT INDEX "),
		strings.HasPrefix(upper, "SPATIAL KEY "),
		strings.HasPrefix(upper, "SPATIAL INDEX "),
		strings.HasPrefix(upper, "KEY "),
		strings.HasPrefix(upper, "INDEX "):
		return !strings.HasPrefix(upper, "PRIMARY KEY ")
	default:
		return false
	}

}

func trimTrailingComma(line string) string {

	trimmed := strings.TrimRight(line, " \t")

	if strings.HasSuffix(trimmed, ",") {

		trimmed = strings.TrimSuffix(trimmed, ",")

	}

	return trimmed

}

// 并发采样边界计算：keyset 步进 + 仅取 PK 列。
//
// 取代基于 LIMIT 1 OFFSET ? 的深分页串行循环，配合 information_schema.TABLE_ROWS
// 估算行数，把预扫描从约 7.5× 全表索引扫描降为 ~1× 主键索引扫描，且每批是短查询、
// 内存有界、可随 ctx 取消。详细设计见 docs/optimization/SAMPLE_BOUNDARY_OPTIMIZATION.md。
//
// 参数 estimatedRows 仅用于计算 step（每批扫描量），不参与分片正确性：
//   - 估算偏小：循环提前结束，有效 worker 自动减少
//   - 估算偏大：多扫一个 step 收敛，影响可忽略
func (s *TaskService) samplePKBoundariesImproved(ctx context.Context, readSource sourceQueryer, schema, table string, pkCols []string, estimatedRows int64, n int) ([]interface{}, error) {
	if readSource == nil {
		return nil, fmt.Errorf("read source is not initialized")
	}
	if n < 2 {
		return nil, fmt.Errorf("parallel worker count must be at least 2: %d", n)
	}
	if estimatedRows < int64(n)*2 {
		// 数据量太少，不值得并行
		return nil, fmt.Errorf("insufficient rows for parallel processing: %d", estimatedRows)
	}
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("no primary key columns provided for %s.%s", schema, table)
	}

	// 构建 PK 列选择列表（仅取 PK 列，避免读全表列拖慢预扫描 IO）
	quotedCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		quotedCols[i] = fmt.Sprintf("`%s`", c)
	}
	colList := strings.Join(quotedCols, ", ")

	// step：相邻边界之间的行数。估算行数足够即可，不要求精确
	step := estimatedRows / int64(n)
	if step < 1 {
		step = 1
	}

	boundaries := make([]interface{}, 0, n-1)
	var lastID interface{}

	for i := 0; i < n-1; i++ {
		// 短查询：每步独立检查 ctx 取消，配合外层 ctx 超时/取消可立即退出
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		query, args := buildKeysetStepQuery(schema, table, pkCols, colList, quotedCols[0], lastID, step)

		// 流式读取，仅保留最后一行作为下一边界；返回 (lastBoundary, rowsRead, err)
		lastBoundary, rowsRead, err := readKeysetStepLastPK(ctx, readSource, query, args, len(pkCols))
		if err != nil {
			return nil, fmt.Errorf("keyset step %d failed: %v", i+1, err)
		}
		if rowsRead < step {
			// 表已读完（实际行数少于估算），无更多可用边界
			break
		}
		if lastBoundary == nil {
			break
		}

		// 边界单调性保护（与原实现语义一致）
		// 兜底：理论上 keyset + ORDER BY + 唯一主键保证单调递增，但若数据出现重复或回退，
		// 必须仍推进 lastID 避免下一批重读相同行导致循环。
		if len(boundaries) > 0 && compareBoundaryValues(boundaries[len(boundaries)-1], lastBoundary) >= 0 {
			logger.Warn("[SampleBoundary] Skip non-increasing boundary for %s.%s at step=%d: prev=%v current=%v",
				schema, table, i+1, boundaries[len(boundaries)-1], lastBoundary)
			lastID = lastBoundary
			continue
		}

		boundaries = append(boundaries, lastBoundary)
		lastID = lastBoundary
	}

	logger.Info("Dynamic boundary sampling: estimatedRows=%d, requestedWorkers=%d, effectiveWorkers=%d, step=%d, pkCols=%v, boundaries=%v",
		estimatedRows, n, len(boundaries)+1, step, pkCols, boundaries)

	return boundaries, nil
}

// buildKeysetStepQuery 构造 keyset 步进 SQL：仅选择 PK 列，沿 PK 升序推进。
// lastID 为 nil 时从表头开始；单列主键传标量；复合主键传 []interface{}。
// 复合主键 (pk1,...,pkN) > (v1,...,vN) 展开为 OR 走联合索引，与 reader 包 keyset 风格一致。
func buildKeysetStepQuery(schema, table string, pkCols []string, colList, firstQuoted string, lastID interface{}, step int64) (string, []interface{}) {
	if len(pkCols) == 1 {
		if lastID == nil {
			return fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s ASC LIMIT ?",
					colList, schema, table, colList),
				[]interface{}{step}
		}
		return fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s > ? ORDER BY %s ASC LIMIT ?",
				colList, schema, table, colList, colList),
			[]interface{}{lastID, step}
	}

	// 复合主键
	if lastID == nil {
		return fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s ASC LIMIT ?",
				colList, schema, table, colList),
			[]interface{}{step}
	}
	lastIDs, ok := lastID.([]interface{})
	if !ok || len(lastIDs) != len(pkCols) {
		// 兜底：lastID 不是完整复合主键值，退化为按首列定位
		return fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s > ? ORDER BY %s ASC LIMIT ?",
				colList, schema, table, firstQuoted, colList),
			[]interface{}{lastID, step}
	}
	whereClause, whereArgs := buildKeysetCompositeWhere(pkCols, lastIDs)
	return fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s ORDER BY %s ASC LIMIT ?",
			colList, schema, table, whereClause, colList),
		append(whereArgs, step)
}

// buildKeysetCompositeWhere 将复合主键元组比较 (pk1,...,pkN) > (v1,...,vN) 展开为 OR 表达式，
// 2 列示例：(pk1=? AND pk2>?) OR (pk1>?)；N 列通用：尾部相等、前一列更大的累加 OR。
// 与 reader.buildCompositeKeysetWhere 等价；此处独立实现以避免 service 包依赖 reader 内部函数。
func buildKeysetCompositeWhere(pkCols []string, lastIDs []interface{}) (string, []interface{}) {
	n := len(pkCols)
	var branches []string
	var args []interface{}
	for k := n; k >= 1; k-- {
		var conds []string
		for j := 0; j < k-1; j++ {
			conds = append(conds, fmt.Sprintf("`%s` = ?", pkCols[j]))
			args = append(args, lastIDs[j])
		}
		conds = append(conds, fmt.Sprintf("`%s` > ?", pkCols[k-1]))
		args = append(args, lastIDs[k-1])
		branches = append(branches, "("+strings.Join(conds, " AND ")+")")
	}
	return strings.Join(branches, " OR "), args
}

// readKeysetStepLastPK 流式读取一批 keyset 结果，仅返回最后一行的 PK 值。
// 使用 rows.Next() 逐行消费 + 仅保留末位，内存 O(1)，不构建 row map。
// 返回 (lastBoundary, rowsRead, err)：lastBoundary 为 nil 表示空结果；rowsRead 用于判断是否到达表尾。
func readKeysetStepLastPK(ctx context.Context, readSource sourceQueryer, query string, args []interface{}, pkCount int) (interface{}, int64, error) {
	rows, err := readSource.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var lastVals []interface{}
	var read int64
	for rows.Next() {
		if pkCount == 1 {
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				return nil, read, err
			}
			lastVals = []interface{}{v}
		} else {
			vals := make([]interface{}, pkCount)
			ptrs := make([]interface{}, pkCount)
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, read, err
			}
			lastVals = vals
		}
		read++
	}
	if err := rows.Err(); err != nil {
		return nil, read, err
	}
	if len(lastVals) == 0 {
		return nil, read, nil
	}

	if pkCount == 1 {
		return normalizePKBoundaryValue(lastVals[0]), read, nil
	}
	result := make([]interface{}, len(lastVals))
	for i, v := range lastVals {
		result[i] = normalizePKBoundaryValue(v)
	}
	return result, read, nil
}

// shortLockStepTimeout 单步（连接获取 / FTWRL / SHOW MASTER STATUS / UNLOCK）的超时上限。
// 短锁取位点必须毫秒~秒级完成，超过该上限就视为源库异常或网络抖动，立即放弃并释放锁。
//
// 这个超时与上层任务 ctx 解绑：即便上层因为暂停/超时被 cancel，每一步仍会用 background ctx
// 派生自己的超时，确保 UNLOCK 路径总能跑完，避免源库长期持锁。
const shortLockStepTimeout = 5 * time.Second

// captureBinlogPosition 无锁读取当前 binlog 位点（SHOW MASTER STATUS / 等价接口）。
// ALL 的 P0/P1 均走此路径；不再使用 FTWRL。
func (s *TaskService) captureBinlogPosition(_ context.Context, runtime *taskRuntime) (mysql.Position, error) {
	if runtime == nil || runtime.sourceDB == nil {
		return mysql.Position{}, fmt.Errorf("task runtime source db is not initialized")
	}

	connCtx, connCancel := context.WithTimeout(context.Background(), shortLockStepTimeout)
	defer connCancel()
	conn, err := runtime.sourceDB.Conn(connCtx)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("get position connection failed: %w", err)
	}
	defer conn.Close()

	posCtx, posCancel := context.WithTimeout(context.Background(), shortLockStepTimeout)
	defer posCancel()
	pos, err := queryMasterPosition(posCtx, conn)
	if err != nil {
		return mysql.Position{}, err
	}
	if pos.Name == "" {
		return mysql.Position{}, fmt.Errorf("captured empty binlog position (file name is empty)")
	}
	logger.Info("[BinlogPosition] Captured unlocked master position: %s:%d", pos.Name, pos.Pos)
	return pos, nil
}

// captureFullSyncStartPosition 兼容旧名：现为无锁位点捕获（P0）。
func (s *TaskService) captureFullSyncStartPosition(ctx context.Context, runtime *taskRuntime) (mysql.Position, error) {
	return s.captureBinlogPosition(ctx, runtime)
}

func queryMasterPosition(ctx context.Context, db sourceQueryer) (mysql.Position, error) {
	var binlogFile, binlogDoDB, binlogIgnoreDB, executedGtidSet string
	var binlogPos uint32
	if err := db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(
		&binlogFile, &binlogPos, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet,
	); err != nil {
		if err2 := db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(
			&binlogFile, &binlogPos, &binlogDoDB, &binlogIgnoreDB,
		); err2 != nil {
			return mysql.Position{}, fmt.Errorf("show master status failed (5col: %v, 4col: %v)", err, err2)
		}
	}
	if binlogFile == "" || binlogPos == 0 {
		return mysql.Position{}, fmt.Errorf("empty master status position")
	}
	return mysql.Position{Name: binlogFile, Pos: binlogPos}, nil
}

func toInt64PK(v interface{}) (int64, bool) {

	switch t := v.(type) {

	case int:

		return int64(t), true

	case int8:

		return int64(t), true

	case int16:

		return int64(t), true

	case int32:

		return int64(t), true

	case int64:

		return t, true

	case uint:

		return int64(t), true

	case uint8:

		return int64(t), true

	case uint16:

		return int64(t), true

	case uint32:

		return int64(t), true

	case uint64:

		if t > uint64(^uint64(0)>>1) {

			return 0, false

		}

		return int64(t), true

	case string:

		i, err := strconv.ParseInt(t, 10, 64)

		return i, err == nil

	case []byte:

		i, err := strconv.ParseInt(string(t), 10, 64)

		return i, err == nil

	default:

		return 0, false

	}

}

func comparePKValues(a, b interface{}) int {

	ai, aok := toInt64PK(a)

	bi, bok := toInt64PK(b)

	if aok && bok {

		switch {

		case ai < bi:

			return -1

		case ai > bi:

			return 1

		default:

			return 0

		}

	}

	as := dbScanToString(a)

	bs := dbScanToString(b)

	switch {

	case as < bs:

		return -1

	case as > bs:

		return 1

	default:

		return 0

	}

}

// dbScanToString 将数据库扫描结果（interface{}）安全转换为字符串
func dbScanToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func normalizePKBoundaryValue(v interface{}) interface{} {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// dbScanToInt 将数据库扫描结果（interface{}）安全转换为整数
func dbScanToInt(v interface{}) int {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return int(val)
	case []byte:
		n, _ := strconv.Atoi(string(val))
		return n
	case string:
		n, _ := strconv.Atoi(val)
		return n
	default:
		return 0
	}
}

// boundaryToString 将边界值转换为可比较的字符串（支持单值和复合主键 []interface{}）
func boundaryToString(v interface{}) string {
	if v == nil {
		return ""
	}
	if vals, ok := v.([]interface{}); ok {
		parts := make([]string, len(vals))
		for i, val := range vals {
			parts[i] = dbScanToString(val)
		}
		return strings.Join(parts, "\x00")
	}
	return dbScanToString(v)
}

func compareBoundaryValues(a, b interface{}) int {
	compareScalar := func(x, y interface{}) int {
		xs := dbScanToString(x)
		ys := dbScanToString(y)
		switch {
		case xs < ys:
			return -1
		case xs > ys:
			return 1
		default:
			return 0
		}
	}

	aVals, aComposite := a.([]interface{})
	bVals, bComposite := b.([]interface{})
	if aComposite && bComposite {
		n := len(aVals)
		if len(bVals) < n {
			n = len(bVals)
		}
		for i := 0; i < n; i++ {
			if c := compareScalar(aVals[i], bVals[i]); c != 0 {
				return c
			}
		}
		switch {
		case len(aVals) < len(bVals):
			return -1
		case len(aVals) > len(bVals):
			return 1
		default:
			return 0
		}
	}
	if !aComposite && !bComposite {
		return compareScalar(a, b)
	}
	return compareScalar(boundaryToString(a), boundaryToString(b))
}

// comparePKWithBoundary 比较一行数据的主键值与边界值，返回 -1 / 0 / +1
// 支持单列边界（interface{}）和复合主键边界（[]interface{}）
func comparePKWithBoundary(pkCols []string, row map[string]interface{}, boundary interface{}) int {
	if boundary == nil {
		return -1
	}
	if boundaryVals, ok := boundary.([]interface{}); ok {
		for i, col := range pkCols {
			if i >= len(boundaryVals) {
				break
			}
			rowStr := dbScanToString(row[col])
			bndStr := dbScanToString(boundaryVals[i])
			if rowStr < bndStr {
				return -1
			}
			if rowStr > bndStr {
				return 1
			}
		}
		return 0
	}
	// 单值边界：与第一列比较
	rowStr := dbScanToString(row[pkCols[0]])
	bndStr := dbScanToString(boundary)
	if rowStr < bndStr {
		return -1
	}
	if rowStr > bndStr {
		return 1
	}
	return 0
}

// isRetryableLockError 检查是否为可重试的 MySQL 锁错误（1205 Lock wait timeout / 1213 Deadlock）
func isRetryableLockError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "Error 1205") ||
		strings.Contains(s, "Error 1213") ||
		strings.Contains(s, "Lock wait timeout") ||
		strings.Contains(s, "Deadlock found")
}

const indexRestoreConnMaxRetries = 3

// isConnRetryable 判断是否为连接失效类错误，可安全换连接重试。
func isConnRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "invalid connection") ||
		strings.Contains(s, "bad connection") ||
		strings.Contains(s, "unexpected packet") ||
		strings.Contains(s, "connection was bad") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection reset")
}

func retryIndexRestoreConn(ctx context.Context, attempt int) error {
	if attempt <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(25+attempt*25) * time.Millisecond):
		return nil
	}
}

// isNumericPKColumn 检查单列主键是否为整数类型（支持表内并行范围分片）

func isNumericPKColumn(identity *entity.TableIdentity, pkCol string) bool {

	for _, col := range identity.Columns {

		if col.Name == pkCol {

			switch strings.ToLower(col.DataType) {

			case "int", "bigint", "tinyint", "smallint", "mediumint":

				return true

			}

			return false

		}

	}

	return false

}

// isTaskStopped 检查任务是否已停止

func (s *TaskService) isTaskStopped(taskID string) bool {

	s.mu.RLock()

	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]

	if !exists {

		return true

	}

	return task.Context.Status != taskEntity.TaskStatusRunning

}

// === 历史全量断点辅助 ===
//
// 全量同步写入使用普通 INSERT 语义，暂停/失败后不再支持 full_sync_resume 续传。
// FullSyncResume 仅作为历史存档兼容字段保留；进入新一轮全量前会清空。
// 增量同步恢复仍由 checkpoint.Manager 管理，与这里无关。
//
// 详见 docs/guides/FULL_SYNC_RESUME_GUIDE.md

// fullSyncTableKey 生成历史全量断点的表键（与 FullSyncResume 的 key 保持一致）。
func fullSyncTableKey(schema, table string) string {
	return schema + "." + table
}

// stringifyKeyVal 把主键列值统一转为字符串以便持久化（[]byte 转字符串，其余用 %v）。
func stringifyKeyVal(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

// resumeKeyFromValue 由单列主键值构造历史断点键。
func resumeKeyFromValue(v interface{}) *taskEntity.ResumeKey {
	return &taskEntity.ResumeKey{Vals: []string{stringifyKeyVal(v)}}
}

// resumeKeyFromValues 由复合主键值构造历史断点键。
func resumeKeyFromValues(vals []interface{}) *taskEntity.ResumeKey {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = stringifyKeyVal(v)
	}
	return &taskEntity.ResumeKey{Vals: out}
}

// lastIDFromResumeKey 把历史断点键还原为 ReadBatchByKeys 所需的 lastID。
// pkCols<=1 返回标量字符串；>1 返回 []interface{}（各列字符串）。无效返回 nil。
func lastIDFromResumeKey(rk *taskEntity.ResumeKey, pkCols int) interface{} {
	if rk == nil || len(rk.Vals) == 0 {
		return nil
	}
	if pkCols <= 1 {
		return rk.Vals[0]
	}
	out := make([]interface{}, len(rk.Vals))
	for i, v := range rk.Vals {
		out[i] = v
	}
	return out
}

// resumeKeyToInt64 解析数值主键历史断点键，失败返回 (0,false)。
func resumeKeyToInt64(rk *taskEntity.ResumeKey) (int64, bool) {
	if rk == nil || len(rk.Vals) == 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(rk.Vals[0]), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// resumeEnabled 判断该任务是否启用全量续传。
// 当前全量写入使用普通 INSERT，任务暂停/失败后必须重新准备目标端，不再启用 full_sync_resume。
func resumeEnabled(task *taskEntity.SyncTask) bool {
	return false
}

// resetResumeIfFresh 在全量开始时清空历史断点和运行计数：
// 全量续传已禁用，每次进入全量前都清空历史断点；
// 同时重置运行计数（ProcessedRows/TotalRows/ProgressPercent 等），避免新一轮全量累加旧值。
//
// P3 例外: V2 引擎 + 有持久化表级状态(FullLoadV2States)时,保留 V2 状态用于恢复(跳过已 PUBLISHED 的表)。
// 仅在全新全量(无 V2 状态或已全部完成)时清空 V2 状态。
// fullLoadV2ResumeState 描述进入 executeFullSync 时的 V2 恢复判定（必须在捕获 P0 之前完成）。
type fullLoadV2ResumeState struct {
	active       bool // 是否 V2 resume（保留原 P0/checkpoint，禁止库级重建）
	baselineDone bool // 全部表已 PUBLISHED：跳过基线，直接恢复 catch-up/索引
	startPos     string
	subphase     string
	endPos       string
}

// detectFullLoadV2Resume 判定进入 executeFullSync 时是否为 V2 resume。
// 仅当引擎为 V2、存在持久化 FullLoadV2States、且 SyncPhase 为 FullStarted/FullFailed 时才视为 resume；
// resume 保留原 P0/checkpoint，禁止库级重建，已 PUBLISHED 的表跳过基线扫描。
// 必须在捕获 P0 之前调用：resume 复用旧 P0，非 resume 才捕获新 P0。
func detectFullLoadV2Resume(task *taskEntity.SyncTask) fullLoadV2ResumeState {
	var st fullLoadV2ResumeState
	if task == nil || !task.Config.UsesFullLoadV2() || len(task.Context.FullLoadV2States) == 0 {
		return st
	}
	switch task.Context.SyncPhase {
	case taskEntity.SyncPhaseFullStarted, taskEntity.SyncPhaseFullFailed:
	default:
		return st
	}
	st.active = true
	st.baselineDone = task.AllFullLoadV2TablesPublished()
	st.startPos = task.Context.FullSyncStartPosition
	st.subphase = task.Context.FullSyncSubphase
	st.endPos = task.Context.FullSyncEndPosition
	return st
}

func (s *TaskService) resetResumeIfFresh(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.tasks[taskID]
	if !exists {
		return
	}
	task.ResetFullSyncResume()
	task.ResetFullSyncCounters()

	// 旧 ALL 表级 HWM + drop_ddl 被放行的 fresh run：必须清空旧 V2 manifest/run，
	// 禁止与 PUBLISHED 跳过混合，避免失败窗口变更既不被基线也不被 P0..P1 catch-up 覆盖。
	legacyHWMFresh := strings.EqualFold(string(task.Config.Mode), "ALL") &&
		len(task.Context.TableBinlogHWMs) > 0 &&
		task.Config.EnableDropTableBeforeDDL
	if legacyHWMFresh {
		task.ClearTableBinlogHWMs()
		task.ClearFullLoadV2States()
		s.storage.Save(task)
		return
	}

	// V2：全量未完成（含基线已 PUBLISHED 但 catch-up/索引未完成）时保留 manifest 供 resume。
	if task.Config.UsesFullLoadV2() && len(task.Context.FullLoadV2States) > 0 {
		if task.Context.SyncPhase == taskEntity.SyncPhaseFullStarted ||
			task.Context.SyncPhase == taskEntity.SyncPhaseFullFailed {
			// resume：保留 V2 状态与原 P0
		} else {
			task.ClearFullLoadV2States()
		}
	} else {
		task.ClearFullLoadV2States()
	}
	s.storage.Save(task)
}

// clearFullSyncResume 清空历史全量断点（全量完成后调用）。
func (s *TaskService) clearFullSyncResume(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, exists := s.tasks[taskID]; exists {
		task.ResetFullSyncResume()
		s.storage.Save(task)
	}
}

// getTableProgress 在锁保护下读取某表的历史断点（返回 nil 表示无）。
func (s *TaskService) getTableProgress(taskID, tableKey string) *taskEntity.TableSyncProgress {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if task, exists := s.tasks[taskID]; exists {
		return task.GetTableProgress(tableKey)
	}
	return nil
}

// initTableProgress 记录某表本次采用的读取路径与并行度（历史断点字段）。
func (s *TaskService) initTableProgress(taskID, tableKey, readPath string, intraWorkers int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, exists := s.tasks[taskID]; exists {
		task.InitTableProgress(tableKey, readPath, intraWorkers)
		s.storage.Save(task)
	}
}

// recordResumeCursor 在事务提交成功后记录某表/分片历史游标（shard<0 表示整表 keyset 游标）。
func (s *TaskService) recordResumeCursor(taskID, tableKey string, shard int, key *taskEntity.ResumeKey) {
	if key == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.tasks[taskID]
	if !exists {
		return
	}
	if shard < 0 {
		task.SetTableCursor(tableKey, key)
	} else {
		task.SetShardCursor(tableKey, shard, key)
	}
	s.storage.Save(task)
}

// saveSampleBoundaries 持久化 sample 路径首跑的采样边界（历史断点字段）。
func (s *TaskService) saveSampleBoundaries(taskID, tableKey string, boundaries []interface{}) {
	keys := make([]*taskEntity.ResumeKey, len(boundaries))
	for i, b := range boundaries {
		keys[i] = resumeKeyFromValue(b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, exists := s.tasks[taskID]; exists {
		task.SetSampleBoundaries(tableKey, keys)
		s.storage.Save(task)
	}
}

// markTableDone 标记某表全量同步完成。
func (s *TaskService) markTableDone(taskID, tableKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, exists := s.tasks[taskID]; exists {
		task.MarkTableDone(tableKey)
		s.storage.Save(task)
	}
}

// ============================================================
// 运行时进度追踪（仅内存，不持久化，供前端实时展示）
// ============================================================

// initRunningProgress 初始化任务的运行时进度追踪。
// 在 executeFullSync 开始时调用，填充所有待同步表的初始状态。
func (s *TaskService) initRunningProgress(taskID string, tables []tableEntry, phase string) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	rp := &taskEntity.RunningProgress{
		Phase:     phase,
		UpdatedAt: time.Now(),
	}

	for _, entry := range tables {
		ti := &taskEntity.TableProgressInfo{
			Schema:    entry.schema,
			Table:     entry.table,
			TotalRows: 0,
			Status:    "pending",
		}
		rp.Tables = append(rp.Tables, ti)
	}

	s.runningProgress[taskID] = rp
}

// startTableProgress 标记某表开始同步，记录开始时间。
func (s *TaskService) startTableProgress(taskID, schema, table string, totalRows int64) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	rp, exists := s.runningProgress[taskID]
	if !exists {
		return
	}

	key := schema + "." + table
	rp.CurrentTable = key
	rp.UpdatedAt = time.Now()

	for _, ti := range rp.Tables {
		if ti.Schema == schema && ti.Table == table {
			now := time.Now()
			ti.Status = "running"
			ti.StartedAt = &now
			ti.TotalRows = totalRows
			return
		}
	}
}

// updateTableProgress 更新某表的已处理行数、速度和整体 ETA。
// delta: 本次新增行数
// elapsed: 该表从开始到现在的耗时（秒）
func (s *TaskService) updateTableProgress(taskID, schema, table string, delta int64, elapsedSec float64, taskStartTime time.Time, taskTotalRows int64) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	rp, exists := s.runningProgress[taskID]
	if !exists {
		return
	}

	key := schema + "." + table
	rp.CurrentTable = key
	rp.UpdatedAt = time.Now()

	for _, ti := range rp.Tables {
		if ti.Schema == schema && ti.Table == table {
			ti.ProcessedRows += delta
			if ti.TotalRows > 0 {
				ti.ProgressPct = float64(ti.ProcessedRows) / float64(ti.TotalRows) * 100
				if ti.ProgressPct > 100 {
					ti.ProgressPct = 100
				}
			}
			if elapsedSec > 0 {
				ti.SpeedRowsSec = float64(ti.ProcessedRows) / elapsedSec
			}
			s.refreshOverallProgressLocked(rp, taskStartTime, taskTotalRows)
			return
		}
	}
}

// completeTableProgress 标记某表同步完成。
func (s *TaskService) completeTableProgress(taskID, schema, table string) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	rp, exists := s.runningProgress[taskID]
	if !exists {
		return
	}

	rp.UpdatedAt = time.Now()

	for _, ti := range rp.Tables {
		if ti.Schema == schema && ti.Table == table {
			now := time.Now()
			ti.Status = "completed"
			ti.CompletedAt = &now
			ti.ProgressPct = 100
			return
		}
	}
}

// failTableProgress 标记某表同步失败。
func (s *TaskService) failTableProgress(taskID, schema, table string) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	rp, exists := s.runningProgress[taskID]
	if !exists {
		return
	}

	rp.UpdatedAt = time.Now()

	for _, ti := range rp.Tables {
		if ti.Schema == schema && ti.Table == table {
			ti.Status = "failed"
			return
		}
	}
}

// refreshOverallProgressLocked 刷新整体进度统计。调用方必须持有 progressMu。
func (s *TaskService) refreshOverallProgressLocked(rp *taskEntity.RunningProgress, startTime time.Time, taskTotalRows int64) {
	rp.UpdatedAt = time.Now()
	rp.ElapsedSeconds = time.Since(startTime).Seconds()

	var totalRows, processedRows int64
	var activeTable string
	for _, ti := range rp.Tables {
		totalRows += ti.TotalRows
		processedRows += ti.ProcessedRows
		if ti.Status == "running" {
			activeTable = ti.Schema + "." + ti.Table
		}
	}
	rp.CurrentTable = activeTable

	if rp.ElapsedSeconds > 0 {
		rp.OverallSpeed = float64(processedRows) / rp.ElapsedSeconds
	}

	effectiveTotalRows := taskTotalRows
	if effectiveTotalRows <= 0 {
		effectiveTotalRows = totalRows
	}

	if rp.OverallSpeed > 0 && effectiveTotalRows > processedRows {
		rp.EstimatedRemain = float64(effectiveTotalRows-processedRows) / rp.OverallSpeed
	} else {
		rp.EstimatedRemain = -1
	}
}

// refreshOverallProgress 刷新整体进度统计（速度、耗时、预估剩余时间）。
func (s *TaskService) refreshOverallProgress(taskID string, startTime time.Time, taskTotalRows int64) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	rp, exists := s.runningProgress[taskID]
	if !exists {
		return
	}

	s.refreshOverallProgressLocked(rp, startTime, taskTotalRows)
}

// GetTaskProgress 获取任务的运行时进度快照（供API查询）。
func (s *TaskService) GetTaskProgress(taskID string) (*taskEntity.RunningProgress, error) {
	s.progressMu.RLock()
	defer s.progressMu.RUnlock()

	rp, exists := s.runningProgress[taskID]
	if !exists {
		return nil, fmt.Errorf("task progress not found: %s", taskID)
	}
	return rp, nil
}

// clearRunningProgress 清除任务的运行时进度（任务完成/停止时调用）。
func (s *TaskService) clearRunningProgress(taskID string) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	delete(s.runningProgress, taskID)
}

// clearLastProgressPersistLocked 清理进度持久化节流记录；调用方必须已持有 s.mu。
func (s *TaskService) clearLastProgressPersistLocked(taskID string) {
	delete(s.lastProgressPersist, taskID)
}

// shouldPersistAsyncProgressSnapshot 判断锁外进度快照是否仍应落盘。
// incrementTaskProgress 在锁内序列化、锁外 Save；Pause/End/Fail/Complete 等在锁内同步保存较新状态。
// 校验 archiveGen 与终态，避免滞后的 RUNNING 快照在终态存档之后落盘。
func (s *TaskService) shouldPersistAsyncProgressSnapshot(taskID string, snapshot *taskEntity.SyncTask) bool {
	if snapshot == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return false
	}
	if taskEntity.IsTerminalTaskStatus(task.Context.Status) {
		return false
	}
	if snapshot.Context.ArchiveGen != task.Context.ArchiveGen {
		return false
	}
	if task.Context.Status != snapshot.Context.Status {
		return false
	}
	if snapshot.Context.Status != taskEntity.TaskStatusRunning {
		return false
	}
	if task.Context.LastUpdateTime.After(snapshot.Context.LastUpdateTime) {
		return false
	}
	return true
}

// persistTaskArchive 持久化任务存档，带短重试；失败时打 ERROR 并写入生命周期事件。
func (s *TaskService) persistTaskArchive(taskID string, task *taskEntity.SyncTask, reason string) error {
	if s.storage == nil || task == nil {
		return nil
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := s.storage.Save(task); err != nil {
			lastErr = err
			if attempt < 2 {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			break
		}
		return nil
	}
	logger.Error("[Task %s] Failed to persist task archive (%s, status=%s): %v", taskID, reason, task.Context.Status, lastErr)
	s.emitLifecycle(taskID, taskEntity.EventCodeTaskPersistFailed,
		fmt.Sprintf("任务存档持久化失败 (%s): %v", reason, lastErr),
		taskEntity.EventSeverityError)
	return lastErr
}

// updateTaskProgress 更新任务进度

func (s *TaskService) updateTaskProgress(taskID string, processedRows int64, position string) {

	s.mu.Lock()

	defer s.mu.Unlock()

	if task, exists := s.tasks[taskID]; exists {

		task.UpdateProgress(processedRows, position)

		s.storage.Save(task)

	}

}

// incrementTaskProgress 原子增加任务进度，并返回当前任务总行数供运行时 ETA 复用。
// 内存进度每批都更新，但存储落盘按 1 秒节流，避免高频 I/O。
func (s *TaskService) incrementTaskProgress(taskID string, delta int64, position string) int64 {
	s.mu.Lock()
	var snapshotJSON []byte
	var effectiveTotal int64
	if task, exists := s.tasks[taskID]; exists {
		task.Context.ProcessedRows += delta
		task.Context.CurrentPosition = position
		task.Context.LastUpdateTime = time.Now()
		// 进度计算优先使用精确总数，未到位时用估算总数兜底（仅 ETA）
		effectiveTotal = task.Context.TotalRows
		if effectiveTotal <= 0 {
			effectiveTotal = task.Context.EstimatedTotalRows
		}
		if effectiveTotal > 0 {
			task.Context.ProgressPercent = taskEntity.CapProgressPercent(
				float64(task.Context.ProcessedRows) / float64(effectiveTotal) * 100,
			)
		}

		// 节流：至少间隔 1 秒才落盘一次，减少存储 I/O
		now := time.Now()
		last, ok := s.lastProgressPersist[taskID]
		if !ok || now.Sub(last) >= time.Second {
			s.lastProgressPersist[taskID] = now
			// 在锁内冻结一份不可变快照（含 archiveGen）；真正的 I/O 在释放全局任务锁后执行。
			snapshotJSON, _ = json.Marshal(task)
		}
	}
	s.mu.Unlock()

	if len(snapshotJSON) > 0 {
		var snapshot taskEntity.SyncTask
		if err := json.Unmarshal(snapshotJSON, &snapshot); err == nil {
			if s.shouldPersistAsyncProgressSnapshot(taskID, &snapshot) {
				_ = s.storage.Save(&snapshot)
			}
		}
	}
	return effectiveTotal
}

// updateTaskEstimatedRows 更新任务估算总行数（information_schema），仅用于 ETA
func (s *TaskService) updateTaskEstimatedRows(taskID string, estimatedRows int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, exists := s.tasks[taskID]; exists {
		task.Context.EstimatedTotalRows = estimatedRows
		task.Context.LastUpdateTime = time.Now()
		s.storage.Save(task)
	}
}

func (s *TaskService) getTaskTotalRows(taskID string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if task, exists := s.tasks[taskID]; exists {
		// 优先返回精确总数，未到位时用估算总数兜底（仅 ETA）
		if task.Context.TotalRows > 0 {
			return task.Context.TotalRows
		}
		return task.Context.EstimatedTotalRows
	}
	return 0
}

// updateTaskStatus 更新任务状态

func (s *TaskService) updateTaskStatus(taskID string, status taskEntity.TaskStatus, errMsg string) {

	s.mu.Lock()

	defer s.mu.Unlock()

	if task, exists := s.tasks[taskID]; exists {

		task.Context.Status = status

		task.Context.ErrorStack = errMsg

		task.Context.LastUpdateTime = time.Now()

		if status == taskEntity.TaskStatusFailed {

			task.Context.EndTime = time.Now()

		}

		task.BumpArchiveGen()

		if err := s.persistTaskArchive(taskID, task, "update_status"); err != nil {
			if status != taskEntity.TaskStatusFailed {
				task.Context.Status = taskEntity.TaskStatusFailed
				task.Context.ErrorStack = fmt.Sprintf("failed to persist task status %s: %v", status, err)
				task.Context.EndTime = time.Now()
				task.BumpArchiveGen()
				_ = s.persistTaskArchive(taskID, task, "update_status_failed")
			}
		}

	}

}

// failTaskUnlessCancelled 仅当不是主动取消/暂停时才标记 FAILED。
// 暂停或停止会先 cancel ctx，DB 调用返回 context.Canceled 属于正常中断，不应记为失败。
func (s *TaskService) failTaskUnlessCancelled(ctx context.Context, taskID, errMsg string) {
	if ctx.Err() != nil {
		logger.Info("[Task %s] Ignoring error during shutdown: %s", taskID, errMsg)
		return
	}
	if s.isUserFullSyncStop(taskID) {
		logger.Info("[Task %s] Ignoring error during user stop: %s", taskID, errMsg)
		return
	}
	s.emitLifecycle(taskID, taskEntity.EventCodeTaskFailed, errMsg, taskEntity.EventSeverityError)
	s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)
}

// completeTask 完成任务

func (s *TaskService) completeTask(taskID string) {

	s.mu.Lock()

	defer s.mu.Unlock()

	// 清除运行时进度（任务完成）
	defer s.clearRunningProgress(taskID)
	defer s.clearLastProgressPersistLocked(taskID)

	if task, exists := s.tasks[taskID]; exists {

		task.Complete()

		if task.Context.ScheduleMode == "cron" {
			next, err := nextCronRun(task, time.Now())
			if err != nil {
				logger.Warn("[Task %s] Failed to compute next cron run: %v", taskID, err)
				task.ClearScheduleConfig()
			} else {
				task.Context.Status = taskEntity.TaskStatusScheduled
				task.Context.ScheduledAt = &next
				task.Context.LastUpdateTime = time.Now()
				task.BumpArchiveGen()
			}
		} else if task.Context.RepeatRemaining > 0 && task.ConsumeScheduledRun() {
			interval := time.Duration(task.Context.RepeatIntervalSec) * time.Second
			if interval < 0 {
				interval = 0
			}
			next := time.Now().Add(interval)
			task.Context.Status = taskEntity.TaskStatusScheduled
			task.Context.ScheduledAt = &next
			task.Context.LastUpdateTime = time.Now()
			task.BumpArchiveGen()
		} else {
			task.ClearScheduleConfig()
		}

		if err := s.persistTaskArchive(taskID, task, "complete"); err != nil {
			task.Context.Status = taskEntity.TaskStatusFailed
			task.Context.ErrorStack = fmt.Sprintf("failed to persist completed state: %v", err)
			if task.Context.EndTime.IsZero() {
				task.Context.EndTime = time.Now()
			}
			task.BumpArchiveGen()
			_ = s.persistTaskArchive(taskID, task, "complete_failed")
			s.emitLifecycle(taskID, taskEntity.EventCodeTaskFailed, task.Context.ErrorStack, taskEntity.EventSeverityError)
			return
		}

		s.emitLifecycle(taskID, taskEntity.EventCodeTaskCompleted, "任务已完成", taskEntity.EventSeverityInfo)

	}

}

// PauseTask 暂停任务

func (s *TaskService) PauseTask(taskID string) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	// 清除运行时进度（任务暂停/停止）
	defer s.clearRunningProgress(taskID)
	defer s.clearLastProgressPersistLocked(taskID)

	task, exists := s.tasks[taskID]

	if !exists {

		return fmt.Errorf("task not found: %s", taskID)

	}

	task.Pause()

	s.emitLifecycle(taskID, taskEntity.EventCodeTaskPaused, "任务已暂停", taskEntity.EventSeverityInfo)

	// 停止增量同步服务（如果存在）

	if incrSync, exists := s.incrementalSyncs[taskID]; exists {

		logger.Info("[Task %s] Stopping incremental sync service", taskID)

		incrSync.Stop()

		delete(s.incrementalSyncs, taskID)

	}

	if runtime, exists := s.runtimes[taskID]; exists {

		runtime.Close()

		delete(s.runtimes, taskID)

	}

	// 保存状态

	if err := s.storage.Save(task); err != nil {

		fmt.Printf("保存任务状态失败: %v\n", err)

	}

	// 记录审计日志

	if s.auditLogger != nil {

		s.auditLogger.LogTaskPaused(taskID)

	}

	return nil

}

// EndTask 结束持续运行的 ALL 任务（用户在增量阶段点击"结束"）。
//
// 仅当同时满足 status=RUNNING、mode=ALL、sync_phase=INCREMENTAL_STARTED 时允许结束。
// 结束为终态：任务进入 STOPPED，记录 end_time，清除 Cron/repeat 调度配置，
// 从运行时映射中摘除增量服务和 task runtime，并写入 TASK_STOPPED 审计事件。
//
// 采用"立即优雅停止"语义：完成当前正在退出的处理和资源关闭，但不捕获新的结束位点，
// 也不等待持续产生的源端写入全部追平。结束后原任务不能重启、编辑或调度；
// 仍允许查看、行数对比、复制新建和删除。
//
// 锁策略：在任务锁内原子地校验、设置 STOPPED、清除调度、从映射摘除增量服务和 runtime
// 引用并持久化状态、写审计；释放锁后再依次停止增量订阅与 Sink、关闭任务数据库连接，
// 避免在全局任务锁内等待外部资源（与 DeleteTask/Close 一致）。
//
// 返回错误：task not found（404）、模式/阶段/状态不允许（409）。
func (s *TaskService) EndTask(taskID string) error {
	// 第一阶段：持锁完成状态变更与映射摘除
	s.mu.Lock()

	task, exists := s.tasks[taskID]

	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("task not found: %s", taskID)
	}

	// 校验结束条件：仅 RUNNING + ALL + INCREMENTAL_STARTED 允许结束
	if task.Context.Status != taskEntity.TaskStatusRunning {
		s.mu.Unlock()
		return fmt.Errorf("task is not running and cannot be ended (status=%s): %s", task.Context.Status, taskID)
	}
	if task.Config.Mode != taskEntity.SyncModeAll {
		s.mu.Unlock()
		return fmt.Errorf("only ALL mode tasks can be ended (mode=%s): %s", task.Config.Mode, taskID)
	}
	if task.Context.SyncPhase != taskEntity.SyncPhaseIncrementalStarted {
		s.mu.Unlock()
		return fmt.Errorf("task can only be ended in incremental phase (sync_phase=%s): %s", task.Context.SyncPhase, taskID)
	}

	// 原子地设置 STOPPED、记录 end_time、清除调度配置
	task.Stop()

	s.emitLifecycle(taskID, taskEntity.EventCodeTaskStopped, "任务已结束（增量阶段手动停止）", taskEntity.EventSeverityInfo)

	// 从映射摘除增量服务和 runtime 引用（实际停止在释放锁后进行）
	var incrSyncToStop *syncApp.IncrementalSyncService
	if incrSync, exists := s.incrementalSyncs[taskID]; exists {
		incrSyncToStop = incrSync
		delete(s.incrementalSyncs, taskID)
	}
	var runtimeToClose *taskRuntime
	if runtime, exists := s.runtimes[taskID]; exists {
		runtimeToClose = runtime
		delete(s.runtimes, taskID)
	}

	// 清除运行时进度（任务结束）
	s.clearRunningProgress(taskID)
	s.clearLastProgressPersistLocked(taskID)

	// 保存状态
	if err := s.storage.Save(task); err != nil {
		fmt.Printf("保存任务状态失败: %v\n", err)
	}

	// 记录审计日志
	if s.auditLogger != nil {
		s.auditLogger.LogTaskStopped(taskID)
	}

	s.mu.Unlock()

	// 第二阶段：释放锁后关闭外部资源，避免在全局任务锁内等待
	if incrSyncToStop != nil {
		logger.Info("[Task %s] Stopping incremental sync service on end", taskID)
		incrSyncToStop.Stop()
	}
	if runtimeToClose != nil {
		runtimeToClose.Close()
	}

	return nil

}

// ScheduleTask 设置定时启动
func (s *TaskService) ScheduleTask(taskID string, scheduledAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	// 只有 PENDING / PAUSED / FAILED 状态的任务才能设置定时启动
	switch task.Context.Status {
	case taskEntity.TaskStatusPending, taskEntity.TaskStatusPaused, taskEntity.TaskStatusFailed, taskEntity.TaskStatusScheduled:
		// 允许
	default:
		return fmt.Errorf("cannot schedule task in status %s: %s", task.Context.Status, taskID)
	}

	task.Schedule(scheduledAt)

	// 保存状态
	if err := s.storage.Save(task); err != nil {
		return fmt.Errorf("failed to save scheduled task state: %w", err)
	}

	s.emitLifecycle(taskID, taskEntity.EventCodeTaskScheduled,
		fmt.Sprintf("任务已计划启动：%s", scheduledAt.Format(time.RFC3339)),
		taskEntity.EventSeverityInfo)

	// 记录审计日志
	if s.auditLogger != nil {
		s.auditLogger.Log(&audit.Event{
			TaskID:    taskID,
			EventType: "TASK_SCHEDULED",
			Success:   true,
			Details:   map[string]interface{}{"scheduled_at": scheduledAt.Format(time.RFC3339)},
		})
	}

	logger.Info("[Task %s] Scheduled to start at %s", taskID, scheduledAt.Format(time.RFC3339))
	return nil
}

// ScheduleTaskWithRepeat 设置定时启动并可重复执行。
func (s *TaskService) ScheduleTaskWithRepeat(taskID string, scheduledAt time.Time, repeatCount, intervalSec int) error {
	if repeatCount < 1 {
		repeatCount = 1
	}
	if err := s.ScheduleTask(taskID, scheduledAt); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}
	task.ConfigureRepeat(repeatCount, intervalSec)
	if err := s.storage.Save(task); err != nil {
		return fmt.Errorf("failed to save repeat schedule state: %w", err)
	}
	return nil
}

// ScheduleCronTask 设置 cron 定时启动。
func (s *TaskService) ScheduleCronTask(taskID string, scheduledAt time.Time, expr, timezone string) error {
	if strings.TrimSpace(expr) == "" {
		return fmt.Errorf("cron expression cannot be empty")
	}
	// 先校验 cron 表达式合法性，避免设置无效的定时任务
	if _, _, err := parseCronExpression(expr, timezone, scheduledAt.Location()); err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}

	if err := s.ScheduleTask(taskID, scheduledAt); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}
	task.ConfigureCronSchedule(expr, timezone)

	// cron 模式下首次执行时间应由 cron 表达式计算得出，而非沿用传入的基准时间（通常是“当前时刻”），
	// 否则调度器会在下一秒立即启动任务，而不是等到 cron 指定的时刻。
	next, err := nextCronRun(task, time.Now())
	if err != nil {
		return fmt.Errorf("failed to compute next cron run for %q: %w", expr, err)
	}
	task.Context.ScheduledAt = &next
	task.Context.LastUpdateTime = time.Now()

	if err := s.storage.Save(task); err != nil {
		return fmt.Errorf("failed to save cron schedule state: %w", err)
	}
	logger.Info("[Task %s] Cron 定时启动已设置: expr=%q tz=%q, 下次执行时间 %s", taskID, expr, timezone, next.Format(time.RFC3339))
	return nil
}

// CancelSchedule 取消定时启动
func (s *TaskService) CancelSchedule(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	if task.Context.Status != taskEntity.TaskStatusScheduled {
		return fmt.Errorf("task is not scheduled: %s (current status: %s)", taskID, task.Context.Status)
	}

	task.CancelSchedule()

	// 保存状态
	if err := s.storage.Save(task); err != nil {
		return fmt.Errorf("failed to save cancelled schedule state: %w", err)
	}

	// 记录审计日志
	if s.auditLogger != nil {
		s.auditLogger.Log(&audit.Event{
			TaskID:    taskID,
			EventType: "TASK_SCHEDULE_CANCELLED",
			Success:   true,
		})
	}

	logger.Info("[Task %s] Schedule cancelled", taskID)
	return nil
}

// StartScheduler 启动定时调度器，每秒检查一次是否有到期的定时任务需要启动
func (s *TaskService) StartScheduler() {
	s.schedulerMu.Lock()
	if s.schedulerStop != nil {
		s.schedulerMu.Unlock()
		return
	}
	stop := make(chan struct{})
	s.schedulerStop = stop
	s.schedulerMu.Unlock()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				logger.Info("Task scheduler stopped")
				return
			case <-ticker.C:
				s.checkScheduledTasks()
			}
		}
	}()
	logger.Info("Task scheduler started")
}

// StopScheduler 停止定时调度器
func (s *TaskService) StopScheduler() {
	s.schedulerMu.Lock()
	stop := s.schedulerStop
	if stop != nil {
		s.schedulerStop = nil
	}
	s.schedulerMu.Unlock()

	if stop != nil {
		close(stop)
	}
}

// checkScheduledTasks 检查并启动到期的定时任务
func (s *TaskService) checkScheduledTasks() {
	s.mu.RLock()
	// 收集需要启动的任务ID
	var toStart []string
	now := time.Now()
	for taskID, task := range s.tasks {
		if task.Context.Status == taskEntity.TaskStatusScheduled &&
			task.Context.ScheduledAt != nil &&
			!task.Context.ScheduledAt.After(now) {
			toStart = append(toStart, taskID)
		}
	}
	s.mu.RUnlock()

	// 逐个启动到期任务
	for _, taskID := range toStart {
		logger.Info("[Task %s] Scheduled time reached, starting task...", taskID)
		if err := s.StartTask(context.Background(), taskID); err != nil {
			logger.Warn("[Task %s] Failed to start scheduled task: %v", taskID, err)
		}
	}
}

// SkipError 跳过错误

func parseCronExpression(expr, timezone string, fallbackLoc *time.Location) (cron.Schedule, bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false, fmt.Errorf("cron expression cannot be empty")
	}
	if strings.HasSuffix(expr, " L * *") || strings.Contains(expr, " L ") {
		return nil, true, nil
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(expr)
	if err != nil {
		return nil, false, err
	}
	_ = timezone
	_ = fallbackLoc
	return sched, false, nil
}

func nextCronRun(task *taskEntity.SyncTask, now time.Time) (time.Time, error) {
	if task == nil {
		return time.Time{}, fmt.Errorf("task is nil")
	}
	expr := strings.TrimSpace(task.Context.CronExpression)
	if expr == "" {
		return time.Time{}, fmt.Errorf("cron expression cannot be empty")
	}
	if strings.Contains(expr, " L ") {
		parts := strings.Fields(expr)
		if len(parts) != 5 {
			return time.Time{}, fmt.Errorf("unsupported cron expression: %s", expr)
		}
		minute, hour := parts[0], parts[1]
		if minute == "*" || hour == "*" {
			return time.Time{}, fmt.Errorf("unsupported last-business-day cron: %s", expr)
		}
		next := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
		for i := 0; i < 15; i++ {
			candidate := next.AddDate(0, i, 0)
			lastDay := time.Date(candidate.Year(), candidate.Month()+1, 0, 0, 0, 0, 0, now.Location())
			for lastDay.Weekday() == time.Saturday || lastDay.Weekday() == time.Sunday {
				lastDay = lastDay.AddDate(0, 0, -1)
			}
			hh, _ := strconv.Atoi(hour)
			mm, _ := strconv.Atoi(minute)
			run := time.Date(lastDay.Year(), lastDay.Month(), lastDay.Day(), hh, mm, 0, 0, now.Location())
			if run.After(now) {
				return run, nil
			}
		}
		return time.Time{}, fmt.Errorf("failed to compute next run for %s", expr)
	}
	sched, _, err := parseCronExpression(expr, task.Context.CronTimezone, now.Location())
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(now), nil
}

func (s *TaskService) SkipError(taskID string) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]

	if !exists {

		return fmt.Errorf("task not found: %s", taskID)

	}

	// 清除错误状态，重新启动

	task.Context.ErrorStack = ""

	task.Context.Status = taskEntity.TaskStatusPaused

	// 保存状态

	if err := s.storage.Save(task); err != nil {

		fmt.Printf("保存任务状态失败: %v\n", err)

	}

	return nil

}

// GetTaskMetrics 获取任务指标

func (s *TaskService) GetTaskMetrics(taskID string) (map[string]interface{}, error) {

	s.mu.RLock()

	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]

	if !exists {

		return nil, fmt.Errorf("task not found: %s", taskID)

	}

	metrics := map[string]interface{}{

		"processed_rows": task.Context.ProcessedRows,

		"total_rows": task.Context.TotalRows,

		"estimated_total_rows": task.Context.EstimatedTotalRows,

		"progress_percent": taskEntity.CapProgressPercent(task.Context.ProgressPercent),

		"tables_completed": 0,

		"tables_total": len(task.Config.Tables),

		"status": task.Context.Status,

		"current_position": task.Context.CurrentPosition,
	}

	// 全量 V2 引擎任务级观测数据（若该任务本轮使用了 V2）
	if snap, ok := fullLoadStatsSnapshot(taskID); ok {
		metrics["full_load_v2"] = snap
	}

	// 如果是增量同步，添加增量同步特有的指标

	if incrSync, exists := s.incrementalSyncs[taskID]; exists {

		pos := incrSync.GetPosition()

		metrics["binlog_file"] = pos.Name

		metrics["binlog_pos"] = pos.Pos

		// 获取延迟

		if lag, err := incrSync.GetLag(); err == nil {

			metrics["lag"] = lag

		}

	}

	return metrics, nil

}

// FileTaskStorage 文件任务存储

type FileTaskStorage struct {
	dataDir string

	mu sync.RWMutex

	encryptKey string // 密码加密密钥（为空则不加密）
}

// NewFileTaskStorage 创建文件任务存储

func NewFileTaskStorage(dataDir string, encryptKeys ...string) *FileTaskStorage {

	// 确保数据目录存在

	if err := os.MkdirAll(dataDir, 0755); err != nil {

		logger.Warn("Warning: failed to create data directory: %v", err)

	}

	var ek string
	if len(encryptKeys) > 0 {
		ek = encryptKeys[0]
	}

	return &FileTaskStorage{dataDir: dataDir, encryptKey: ek}

}

// Save 保存任务到JSON文件

func (s *FileTaskStorage) Save(task *taskEntity.SyncTask) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	// 确保目录存在

	if err := os.MkdirAll(s.dataDir, 0755); err != nil {

		return fmt.Errorf("failed to create data directory: %w", err)

	}

	// 加密密码：先备份明文，加密后序列化，再还原明文（避免污染内存中的任务对象）
	var origSourcePwd, origTargetPwd string
	if task.Config.SourceDB != nil {
		origSourcePwd = task.Config.SourceDB.Password
	}
	if task.Config.TargetDB != nil {
		origTargetPwd = task.Config.TargetDB.Password
	}
	origSinkConfigs := sink.CloneConfigs(task.Config.SinkConfigs)
	defer func() {
		if task.Config.SourceDB != nil {
			task.Config.SourceDB.Password = origSourcePwd
		}
		if task.Config.TargetDB != nil {
			task.Config.TargetDB.Password = origTargetPwd
		}
		task.Config.SinkConfigs = origSinkConfigs
	}()
	if err := task.EncryptPasswords(s.encryptKey); err != nil {
		return fmt.Errorf("encrypt passwords: %w", err)
	}

	if stored, err := loadStoredTaskFileLocked(s.dataDir, task.Config.ID); err != nil {
		return err
	} else if taskEntity.ShouldRejectArchiveOverwrite(stored, task) {
		return nil
	}

	// 序列化任务

	data, err := json.MarshalIndent(task, "", "  ")

	if err != nil {

		return fmt.Errorf("failed to marshal task: %w", err)

	}

	// 写入文件

	filePath := filepath.Join(s.dataDir, task.Config.ID+".json")

	if err := os.WriteFile(filePath, data, 0644); err != nil {

		return fmt.Errorf("failed to write task file: %w", err)

	}

	return nil

}

// Delete 删除任务文件

func (s *FileTaskStorage) Delete(taskID string) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	filePath := filepath.Join(s.dataDir, taskID+".json")

	if err := os.Remove(filePath); err != nil {

		if os.IsNotExist(err) {

			return nil // 文件不存在不算错误

		}

		return fmt.Errorf("failed to delete task file: %w", err)

	}

	return nil

}

// LoadAll 加载所有任务

func (s *FileTaskStorage) LoadAll() ([]*taskEntity.SyncTask, error) {

	s.mu.RLock()

	defer s.mu.RUnlock()

	// 检查目录是否存在

	if _, err := os.Stat(s.dataDir); os.IsNotExist(err) {

		return []*taskEntity.SyncTask{}, nil

	}

	// 读取目录下所有JSON文件

	files, err := os.ReadDir(s.dataDir)

	if err != nil {

		return nil, fmt.Errorf("failed to read data directory: %w", err)

	}

	var tasks []*taskEntity.SyncTask

	for _, file := range files {

		if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {

			continue

		}

		filePath := filepath.Join(s.dataDir, file.Name())

		data, err := os.ReadFile(filePath)

		if err != nil {

			logger.Warn("Warning: failed to read task file %s: %v", file.Name(), err)

			continue

		}

		var task taskEntity.SyncTask

		if err := json.Unmarshal(data, &task); err != nil {

			logger.Warn("Warning: failed to unmarshal task file %s: %v", file.Name(), err)

			continue

		}

		if decErr := task.DecryptPasswords(s.encryptKey); decErr != nil { // 解密密码
			logger.Warn("Warning: failed to decrypt task passwords in file %s: %v", file.Name(), decErr)
		}

		tasks = append(tasks, &task)

	}

	return tasks, nil

}

func (s *FileTaskStorage) QueryTasksPage(page, pageSize int, status, keyword, sortBy string) ([]*taskEntity.SyncTask, int, int, int, error) {
	tasks, err := s.LoadAll()
	if err != nil {
		return nil, 0, page, pageSize, err
	}
	filtered := make([]*taskEntity.SyncTask, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if status != "" && strings.ToUpper(string(task.Context.Status)) != strings.ToUpper(strings.TrimSpace(status)) {
			continue
		}
		if keyword != "" {
			kw := strings.ToLower(strings.TrimSpace(keyword))
			matched := strings.Contains(strings.ToLower(task.Config.Name), kw) || strings.Contains(strings.ToLower(task.Config.ID), kw) || strings.Contains(strings.ToLower(task.Config.SourceSchema), kw) || strings.Contains(strings.ToLower(task.Config.TargetSchema), kw)
			if !matched {
				for _, tableName := range task.Config.Tables {
					if strings.Contains(strings.ToLower(tableName), kw) {
						matched = true
						break
					}
				}
			}
			if !matched {
				continue
			}
		}
		filtered = append(filtered, task)
	}
	order := strings.ToLower(strings.TrimSpace(sortBy))
	sort.Slice(filtered, func(i, j int) bool {
		a := filtered[i]
		b := filtered[j]
		ai := taskStorageOrderKey(a)
		bi := taskStorageOrderKey(b)
		switch order {
		case "created_at_asc":
			return ai < bi
		case "name_asc":
			return strings.ToLower(a.Config.Name) < strings.ToLower(b.Config.Name)
		case "name_desc":
			return strings.ToLower(a.Config.Name) > strings.ToLower(b.Config.Name)
		case "status_asc":
			aRank := taskStatusRank(a.Context.Status)
			bRank := taskStatusRank(b.Context.Status)
			if aRank == bRank {
				return ai < bi
			}
			return aRank < bRank
		case "status_desc":
			aRank := taskStatusRank(a.Context.Status)
			bRank := taskStatusRank(b.Context.Status)
			if aRank == bRank {
				return ai > bi
			}
			return aRank > bRank
		case "progress_asc":
			if a.Context.ProgressPercent == b.Context.ProgressPercent {
				return ai < bi
			}
			return a.Context.ProgressPercent < b.Context.ProgressPercent
		case "progress_desc":
			if a.Context.ProgressPercent == b.Context.ProgressPercent {
				return ai > bi
			}
			return a.Context.ProgressPercent > b.Context.ProgressPercent
		default:
			return ai > bi
		}
	})
	if pageSize <= 0 {
		pageSize = 10
	}
	if page <= 0 {
		page = 1
	}
	total := len(filtered)
	start := (page - 1) * pageSize
	if start >= total {
		return []*taskEntity.SyncTask{}, total, page, pageSize, nil
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return filtered[start:end], total, page, pageSize, nil
}

// Close 优雅关闭任务服务

func (s *TaskService) Close() error {

	s.mu.Lock()

	defer s.mu.Unlock()

	logger.Info("Closing task service...")

	// 0. 停止定时调度器
	s.StopScheduler()

	// 1. 停止所有增量同步服务

	for taskID, incrSync := range s.incrementalSyncs {

		logger.Info("[Task %s] Stopping incremental sync service", taskID)

		incrSync.Stop()

		delete(s.incrementalSyncs, taskID)

	}

	for taskID, runtime := range s.runtimes {

		runtime.Close()

		delete(s.runtimes, taskID)

	}

	// 2. 取消所有行数对比后台 goroutine 并等待退出，避免写入即将关闭的存储。
	//    先收集 WaitGroup 再释放锁等待，防止持锁等待 goroutine 造成死锁（goroutine 可能需要 s.mu）。
	s.comparisonMu.Lock()
	for _, cancel := range s.comparisonCancels {
		cancel()
	}
	wgSnapshot := make([]*sync.WaitGroup, 0, len(s.comparisonWgs))
	for _, wg := range s.comparisonWgs {
		wgSnapshot = append(wgSnapshot, wg)
	}
	s.comparisonMu.Unlock()

	// 3. 保存所有任务状态

	for taskID, task := range s.tasks {

		// 如果任务正在运行，暂停它

		if task.Context.Status == taskEntity.TaskStatusRunning {

			task.Pause()

			logger.Info("[Task %s] Task paused due to service shutdown", taskID)

		}

		// 保存任务状态

		if err := s.storage.Save(task); err != nil {

			logger.Info("[Task %s] Failed to save task state: %v", taskID, err)

		}

	}

	// 释放主锁后等待对比 goroutine 退出，再关闭存储。
	s.mu.Unlock()
	for _, wg := range wgSnapshot {
		wg.Wait()
	}
	s.mu.Lock()

	closeResource(s.checkpointCloser, "checkpoint manager")
	s.checkpointCloser = nil

	closeResource(s.storageCloser, "task storage")
	s.storageCloser = nil

	if s.pruneStop != nil {
		close(s.pruneStop)
	}
	if s.eventRecorder != nil {
		s.eventRecorder.Close()
	}

	// 3. 关闭审计日志器

	if s.auditLogger != nil {

		if err := s.auditLogger.Close(); err != nil {

			logger.Info("Failed to close audit logger: %v", err)

		}

	}

	logger.Info("Task service closed successfully")

	return nil

}

func loadNonPrimaryKeyIndexes(db *sql.DB, schema, tableName string) ([]map[string]interface{}, error) {

	if db == nil {

		return nil, fmt.Errorf("db is not initialized")

	}

	rows, err := db.Query(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", schema, tableName))

	if err != nil {

		return nil, fmt.Errorf("failed to show indexes: %v", err)

	}

	defer rows.Close()

	return scanNonPrimaryKeyIndexes(rows)

}

// scanNonPrimaryKeyIndexes 从 information_schema.STATISTICS 的查询结果扫描非主键索引并组装为 map 列表。
// 每个 map 的键名约定（name/columns/non_unique/type）是下游 shouldDeferIndex/restoreIndexes 依赖的隐式契约。
func scanNonPrimaryKeyIndexes(rows *sql.Rows) ([]map[string]interface{}, error) {

	cols, err := rows.Columns()

	if err != nil {

		return nil, fmt.Errorf("failed to get index columns: %v", err)

	}

	type indexColumn struct {
		Column     string
		SeqInIndex int
		SubPart    string
	}

	type indexMeta struct {
		NonUnique int
		IndexType string
		Columns   []indexColumn
	}

	indexMap := make(map[string]*indexMeta)

	for rows.Next() {

		vals := make([]interface{}, len(cols))

		ptrs := make([]interface{}, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {

			logger.Warn("Warning: failed to scan index row: %v", err)

			continue

		}

		keyName := dbScanToString(vals[2])

		if keyName == "PRIMARY" {

			continue

		}

		nonUnique := dbScanToInt(vals[1])
		seqInIndex := dbScanToInt(vals[3])
		columnName := dbScanToString(vals[4])
		subPart := dbScanToString(vals[7])
		indexType := dbScanToString(vals[10])

		meta, exists := indexMap[keyName]

		if !exists {
			meta = &indexMeta{NonUnique: nonUnique, IndexType: indexType}
			indexMap[keyName] = meta
		}

		meta.Columns = append(meta.Columns, indexColumn{Column: columnName, SeqInIndex: seqInIndex, SubPart: subPart})

	}

	if err := rows.Err(); err != nil {

		return nil, fmt.Errorf("failed to read index rows: %v", err)

	}

	if len(indexMap) == 0 {

		return nil, nil

	}

	indexNames := make([]string, 0, len(indexMap))

	for name := range indexMap {
		indexNames = append(indexNames, name)
	}

	sort.Strings(indexNames)

	savedIndexes := make([]map[string]interface{}, 0, len(indexNames))

	for _, name := range indexNames {

		meta := indexMap[name]

		sort.Slice(meta.Columns, func(i, j int) bool {
			return meta.Columns[i].SeqInIndex < meta.Columns[j].SeqInIndex
		})

		var colDefs []string

		for _, c := range meta.Columns {
			if c.SubPart != "" && c.SubPart != "0" {
				colDefs = append(colDefs, fmt.Sprintf("`%s`(%s)", c.Column, c.SubPart))
			} else {
				colDefs = append(colDefs, fmt.Sprintf("`%s`", c.Column))
			}
		}

		savedIndexes = append(savedIndexes, map[string]interface{}{
			"name":       name,
			"non_unique": meta.NonUnique,
			"type":       meta.IndexType,
			"columns":    strings.Join(colDefs, ", "),
		})

	}

	return savedIndexes, nil

}

// deferredIndexRollbackTimeout 限制 DROP 失败/取消/丢锁后的索引回滚最长耗时，避免 cleanup 无限挂起。
const deferredIndexRollbackTimeout = 5 * time.Minute

// afterDeferredIndexDropped 仅供测试在两次 DROP 之间注入取消/丢锁；生产路径保持 nil。
var afterDeferredIndexDropped func()

// deferredIndexRollbackContext 返回不受父 ctx 取消影响、且有截止时间的 cleanup context。
func deferredIndexRollbackContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(parent), deferredIndexRollbackTimeout)
}

// dropNonPrimaryKeyIndexes 删除非主键索引（兼容旧调用：等价于 FULL + optimize_index=true）。
func (s *TaskService) dropNonPrimaryKeyIndexes(ctx context.Context, runtime *taskRuntime, schema, tableName string) ([]map[string]interface{}, error) {
	return s.dropDeferredIndexes(ctx, runtime, schema, tableName, nil, taskEntity.SyncModeFull, true)
}

// dropDeferredIndexes 按模式/identity 删除应延迟恢复的索引，并返回成功删除的列表。
func (s *TaskService) dropDeferredIndexes(ctx context.Context, runtime *taskRuntime, schema, tableName string, identity *entity.TableIdentity, mode taskEntity.SyncMode, optimizeIndex bool) ([]map[string]interface{}, error) {

	if runtime == nil || runtime.targetDB == nil {

		return nil, fmt.Errorf("task runtime target db is not initialized")

	}

	if err := fullload.SchemaLockLostError(ctx); err != nil {
		return nil, err
	}

	targetDB := runtime.targetDB

	savedIndexes, err := loadNonPrimaryKeyIndexes(targetDB, schema, tableName)

	if err != nil {

		return nil, err

	}
	savedIndexes = selectDeferredIndexes(savedIndexes, identity, mode, optimizeIndex)

	if len(savedIndexes) == 0 {

		return nil, nil

	}

	// 只有成功删除的索引才需要恢复，避免恢复阶段再次创建失败。

	dropped := make([]map[string]interface{}, 0, len(savedIndexes))

	// 回滚使用独立、有界 cleanup context：父 ctx 取消/超时/丢锁时仍可尽最大努力恢复本轮已删索引。
	// 策略：即使目标 schema 顾问锁已丢失，也对“本函数自己删掉的索引”做 best-effort 回滚，避免目标表长期缺索引；任务仍因原始错误 fail-closed。
	rollbackDropped := func(cause error) ([]map[string]interface{}, error) {
		if len(dropped) == 0 {
			return nil, cause
		}
		cleanupCtx, cancel := deferredIndexRollbackContext(ctx)
		defer cancel()
		if restoreErr := s.restoreIndexes(cleanupCtx, runtime, schema, tableName, dropped); restoreErr != nil {
			return dropped, fmt.Errorf("%v; additionally failed to restore already-dropped indexes: %w", cause, restoreErr)
		}
		return nil, cause
	}

	for _, indexInfo := range savedIndexes {

		if err := fullload.SchemaLockLostError(ctx); err != nil {
			return rollbackDropped(err)
		}

		name, ok := indexInfo["name"].(string)

		if !ok || name == "" {

			continue

		}

		dropQuery := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`", schema, tableName, name)

		_, err := targetDB.ExecContext(ctx, dropQuery)

		if err != nil {
			// ALL 要求非 identity 唯一索引在 catch-up 前不存在：DROP 失败必须 fail-closed。
			// 同时回滚本轮已成功删除的索引，避免目标表长期缺索引。
			dropErr := fmt.Errorf("failed to drop deferred index %s on %s.%s: %w", name, schema, tableName, err)
			return rollbackDropped(dropErr)
		}

		dropped = append(dropped, indexInfo)

		logger.Info("Dropped index %s from table %s.%s", name, schema, tableName)

		if afterDeferredIndexDropped != nil {
			afterDeferredIndexDropped()
		}

	}

	// 最后一次 DROP 成功后也可能立刻取消/丢锁：循环内检查不会再跑一轮，必须在返回前收口。
	if err := fullload.SchemaLockLostError(ctx); err != nil {
		return rollbackDropped(err)
	}
	if err := context.Cause(ctx); err != nil {
		return rollbackDropped(err)
	}

	return dropped, nil

}

// restorePendingIndexes 在所有表数据同步完成后，按表级并发恢复全量同步期间移除的索引。
// 单表内待建索引按类型（BTREE / FULLTEXT / SPATIAL）分批合并 ALTER TABLE；不同表之间按 workers 并发。
// 任一表失败或任务被停止时，通过 context 取消其余在途任务并尽快返回。
func (s *TaskService) restorePendingIndexes(ctx context.Context, runtime *taskRuntime, taskID string, pending []pendingIndexRestore, workers int) error {
	if len(pending) == 0 {
		return nil
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(pending) {
		workers = len(pending)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	sem := make(chan struct{}, workers)

	for _, item := range pending {
		if len(item.indexes) == 0 {
			continue
		}
		// 停止信号快速退出
		if s.isTaskStopped(taskID) {
			cancel()
			break
		}
		if ctx.Err() != nil {
			break
		}

		item := item
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}
			logger.Info("[Task %s] Restoring indexes for target table %s.%s...", taskID, item.targetSchema, item.targetTable)
			if err := s.restoreIndexes(ctx, runtime, item.targetSchema, item.targetTable, item.indexes); err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("restore indexes for %s.%s: %w", item.targetSchema, item.targetTable, err)
					cancel() // 取消其余在途任务
				})
				return
			}
			logger.Info("[Task %s] Restored indexes for target table %s.%s", taskID, item.targetSchema, item.targetTable)
		}()
	}
	wg.Wait()

	// worker 级停止判断保留宽语义；编排层在 executeFullSync 中再区分用户停止与失败。
	if s.isTaskStopped(taskID) {
		if s.isUserFullSyncStop(taskID) {
			return errFullSyncStoppedByUser
		}
		if firstErr != nil {
			return firstErr
		}
		return context.Canceled
	}
	if firstErr == nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return firstErr
}

// targetIndexExists 检查目标表上是否已存在同名索引，并校验定义是否一致。

// 返回的 exists 表示是否存在；match 为 true 表示名称、唯一性、索引类型、列顺序及前缀长度均一致。

func targetIndexExists(ctx context.Context, targetDB *sql.DB, schema, tableName, indexName string, nonUnique int, indexType, columns string) (bool, bool, error) {

	rows, err := targetDB.QueryContext(ctx,

		`SELECT NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, SEQ_IN_INDEX

		 FROM information_schema.STATISTICS

		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?

		 ORDER BY SEQ_IN_INDEX`,

		schema, tableName, indexName,
	)

	if err != nil {

		return false, false, err

	}

	defer rows.Close()

	var (
		parts []string

		actualNonUnique int

		actualIndexType string

		firstRowScanned bool
	)

	for rows.Next() {

		var colName string

		var subPart sql.NullString

		var seqInIndex int

		if err := rows.Scan(&actualNonUnique, &actualIndexType, &colName, &subPart, &seqInIndex); err != nil {

			return false, false, err

		}

		firstRowScanned = true

		if subPart.Valid && subPart.String != "" && subPart.String != "0" {

			parts = append(parts, fmt.Sprintf("`%s`(%s)", colName, subPart.String))

		} else {

			parts = append(parts, fmt.Sprintf("`%s`", colName))

		}

	}

	if err := rows.Err(); err != nil {

		return false, false, err

	}

	if !firstRowScanned {

		return false, false, nil

	}

	actualColumns := strings.Join(parts, ", ")

	match := actualNonUnique == nonUnique &&

		strings.EqualFold(actualIndexType, indexType) &&

		strings.EqualFold(actualColumns, columns)

	return true, match, nil

}

const (
	indexRestoreBatchBTREE    = "BTREE"
	indexRestoreBatchFULLTEXT = "FULLTEXT"
	indexRestoreBatchSPATIAL  = "SPATIAL"
)

// indexRestoreBatchKind 将索引类型归入 ALTER TABLE 批次：BTREE / FULLTEXT / SPATIAL 分开，
// 避免部分 MySQL 版本拒绝 FULLTEXT/SPATIAL 与 BTREE 同句混加。
func indexRestoreBatchKind(indexType string) string {
	if strings.EqualFold(indexType, "FULLTEXT") {
		return indexRestoreBatchFULLTEXT
	}
	if strings.EqualFold(indexType, "SPATIAL") {
		return indexRestoreBatchSPATIAL
	}
	return indexRestoreBatchBTREE
}

type indexRestoreBatch struct {
	kind    string
	clauses []string
	names   []string
	items   []indexRestoreItem
}

// indexRestoreItem 是 restoreIndexes 内部使用的索引恢复项，记录单个索引的恢复所需信息。
type indexRestoreItem struct {
	name      string
	nonUnique int
	indexType string
	columns   string
}

// groupIndexRestoreBatches 按索引类型分组，同组内合并为一条 ALTER TABLE。
func groupIndexRestoreBatches(items []indexRestoreItem) []indexRestoreBatch {
	order := []string{indexRestoreBatchBTREE, indexRestoreBatchFULLTEXT, indexRestoreBatchSPATIAL}
	byKind := map[string]*indexRestoreBatch{
		indexRestoreBatchBTREE:    {kind: indexRestoreBatchBTREE},
		indexRestoreBatchFULLTEXT: {kind: indexRestoreBatchFULLTEXT},
		indexRestoreBatchSPATIAL:  {kind: indexRestoreBatchSPATIAL},
	}
	for _, item := range items {
		kind := indexRestoreBatchKind(item.indexType)
		batch := byKind[kind]
		batch.clauses = append(batch.clauses, buildAddIndexClause(item.name, item.nonUnique, item.indexType, item.columns))
		batch.names = append(batch.names, item.name)
		batch.items = append(batch.items, item)
	}
	batches := make([]indexRestoreBatch, 0, len(order))
	for _, kind := range order {
		if len(byKind[kind].clauses) > 0 {
			batches = append(batches, *byKind[kind])
		}
	}
	return batches
}

// buildAddIndexClause 构造 ALTER TABLE 中的单条 ADD INDEX 子句。
func buildAddIndexClause(indexName string, nonUnique int, indexType, columns string) string {
	if nonUnique == 0 {
		return fmt.Sprintf("ADD UNIQUE INDEX `%s` (%s)", indexName, columns)
	}
	if strings.EqualFold(indexType, "FULLTEXT") {
		return fmt.Sprintf("ADD FULLTEXT INDEX `%s` (%s)", indexName, columns)
	}
	if strings.EqualFold(indexType, "SPATIAL") {
		return fmt.Sprintf("ADD SPATIAL INDEX `%s` (%s)", indexName, columns)
	}
	return fmt.Sprintf("ADD INDEX `%s` (%s)", indexName, columns)
}

// buildAlterAddIndexesSQL 将同表多个索引合并为一条 ALTER TABLE，避免多次全表扫描建索引。
func buildAlterAddIndexesSQL(schema, tableName string, clauses []string) string {
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", schema, tableName, strings.Join(clauses, ", "))
}

func checkExistingIndexWithRetry(ctx context.Context, targetDB *sql.DB, schema, tableName string, item indexRestoreItem) (bool, bool, error) {
	var (
		exists bool
		match  bool
		err    error
	)
	for attempt := 0; attempt <= indexRestoreConnMaxRetries; attempt++ {
		if retryErr := retryIndexRestoreConn(ctx, attempt); retryErr != nil {
			return false, false, retryErr
		}
		exists, match, err = targetIndexExists(ctx, targetDB, schema, tableName, item.name, item.nonUnique, item.indexType, item.columns)
		if err == nil || !isConnRetryable(err) || attempt >= indexRestoreConnMaxRetries {
			break
		}
		logger.Warn("Retrying index existence check for `%s` on `%s`.`%s` after connection error (attempt %d): %v",
			item.name, schema, tableName, attempt+1, err)
	}
	return exists, match, err
}

func isAlterBatchAlreadyApplied(ctx context.Context, targetDB *sql.DB, schema, tableName string, batch indexRestoreBatch) (bool, error) {
	for _, item := range batch.items {
		exists, match, err := checkExistingIndexWithRetry(ctx, targetDB, schema, tableName, item)
		if err != nil {
			return false, err
		}
		if !exists || !match {
			return false, nil
		}
	}
	return true, nil
}

func execAlterAddIndexes(ctx context.Context, targetDB *sql.DB, schema, tableName string, batch indexRestoreBatch) error {
	alterSQL := buildAlterAddIndexesSQL(schema, tableName, batch.clauses)
	logger.Info("[Task] Restoring %d %s indexes on `%s`.`%s` in one ALTER TABLE: %s",
		len(batch.clauses), batch.kind, schema, tableName, strings.Join(batch.names, ", "))

	var err error
	for attempt := 0; attempt <= indexRestoreConnMaxRetries; attempt++ {
		if retryErr := retryIndexRestoreConn(ctx, attempt); retryErr != nil {
			return retryErr
		}
		_, err = targetDB.ExecContext(ctx, alterSQL)
		if err == nil || !isConnRetryable(err) || attempt >= indexRestoreConnMaxRetries {
			break
		}
		logger.Warn("Retrying ALTER TABLE ADD INDEX on `%s`.`%s` after connection error (attempt %d): %v",
			schema, tableName, attempt+1, err)
	}
	if err != nil {
		if lost := fullload.SchemaLockLostError(ctx); lost != nil {
			return lost
		}
		if applied, checkErr := isAlterBatchAlreadyApplied(ctx, targetDB, schema, tableName, batch); checkErr == nil && applied {
			logger.Warn("ALTER TABLE on `%s`.`%s` returned error but all target indexes already match; treating as success (error: %v)",
				schema, tableName, err)
			logger.Info("Created %d indexes on table %s.%s: %s", len(batch.names), schema, tableName, strings.Join(batch.names, ", "))
			return nil
		} else if checkErr != nil {
			logger.Warn("Post-error index verification failed on `%s`.`%s`: %v", schema, tableName, checkErr)
		}
		logger.Warn("Warning: failed to restore indexes on `%s`.`%s`: %v (SQL: %s)", schema, tableName, err, alterSQL)
		return fmt.Errorf("restore indexes on `%s`.`%s` (%s): %w", schema, tableName, strings.Join(batch.names, ", "), err)
	}

	logger.Info("Created %d indexes on table %s.%s: %s", len(batch.names), schema, tableName, strings.Join(batch.names, ", "))
	return nil
}

// restoreIndexes 恢复索引：同表待建索引按类型分批合并 ALTER TABLE，InnoDB 每批一次扫描建齐。
func (s *TaskService) restoreIndexes(ctx context.Context, runtime *taskRuntime, schema, tableName string, indexes []map[string]interface{}) error {
	if runtime == nil || runtime.targetDB == nil {
		return fmt.Errorf("task runtime target db is not initialized")
	}
	targetDB := runtime.targetDB
	if len(indexes) == 0 {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	logger.Info("[Task] Restoring %d indexes for table %s.%s...", len(indexes), schema, tableName)

	pending := make([]indexRestoreItem, 0, len(indexes))

	for _, indexInfo := range indexes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		indexName, ok := indexInfo["name"].(string)
		if !ok || indexName == "" {
			continue
		}

		// columns 已在 dropNonPrimaryKeyIndexes 中预构建好（含前缀长度）
		columns, ok := indexInfo["columns"].(string)
		if !ok || columns == "" {
			continue
		}

		nonUnique := 1
		if v, ok := indexInfo["non_unique"].(int); ok {
			nonUnique = v
		}

		indexType := ""
		if v, ok := indexInfo["type"].(string); ok {
			indexType = v
		}

		// 恢复前检查目标表上是否已存在同名索引；定义一致则跳过，不一致则报错。
		item := indexRestoreItem{name: indexName, nonUnique: nonUnique, indexType: indexType, columns: columns}
		exists, match, err := checkExistingIndexWithRetry(ctx, targetDB, schema, tableName, item)
		if err != nil {
			return fmt.Errorf("check existing index `%s` on `%s`.`%s`: %w", indexName, schema, tableName, err)
		}

		if exists {
			if match {
				logger.Info("Index `%s` already exists on `%s`.`%s` with matching definition, skipping", indexName, schema, tableName)
				continue
			}
			return fmt.Errorf("index `%s` already exists on `%s`.`%s` but with a different definition", indexName, schema, tableName)
		}

		pending = append(pending, item)
	}

	if len(pending) == 0 {
		return nil
	}

	for _, batch := range groupIndexRestoreBatches(pending) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := execAlterAddIndexes(ctx, targetDB, schema, tableName, batch); err != nil {
			return err
		}
	}
	return nil
}

// ReinitStorage 动态切换存储后端

func (s *TaskService) ReinitStorage(cfg *config.Config) error {

	newStorage, newStorageCloser, storageType, err := newTaskStorageFromConfig(cfg)

	if err != nil {

		return err

	}

	s.mu.Lock()

	oldStorageCloser := s.storageCloser
	s.storage = newStorage
	s.storageCloser = newStorageCloser
	if cfg != nil {
		s.config = cfg
	}

	s.mu.Unlock()

	closeResource(oldStorageCloser, "task storage")
	logger.Info("Storage backend switched to %s", storageType)

	return nil

}

// ReinitCheckpointManager 动态切换位点管理器

func (s *TaskService) ReinitCheckpointManager(cfg *config.Config) error {

	newCheckpointManager, newCheckpointCloser, checkpointType, err := newCheckpointManagerFromConfig(cfg)

	if err != nil {

		return err

	}

	s.mu.Lock()

	oldCheckpointCloser := s.checkpointCloser
	s.checkpointManager = newCheckpointManager
	s.checkpointCloser = newCheckpointCloser
	if cfg != nil {
		s.config = cfg
	}

	s.mu.Unlock()

	closeResource(oldCheckpointCloser, "checkpoint manager")
	logger.Info("Checkpoint manager switched to %s", checkpointType)

	return nil

}

// GetRunningTaskCount 获取正在运行的任务数量

func (s *TaskService) GetRunningTaskCount() int {

	s.mu.RLock()

	defer s.mu.RUnlock()

	count := 0

	for _, task := range s.tasks {

		if task.Context.Status == taskEntity.TaskStatusRunning {

			count++

		}

	}

	return count

}
