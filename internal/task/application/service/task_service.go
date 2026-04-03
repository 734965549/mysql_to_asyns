// 声明 service 包

package service

import (
	"context" // 上下文管理
	"sort"

	"database/sql" // 数据库操作

	"encoding/json" // JSON 编解码

	"fmt" // 格式化输出

	"log" // 日志记录

	"os" // 操作系统接口

	"path/filepath" // 文件路径操作

	"runtime/debug" // 运行时调试信息

	// 排序

	"strconv" // 字符串数字转换

	"strings" // 字符串处理

	"sync" // 并发同步

	"sync/atomic" // 原子操作

	"time" // 时间处理

	"mysql-to-async/internal/audit" // 审计日志包

	"mysql-to-async/internal/checkpoint" // 检查点管理包

	"mysql-to-async/internal/config" // 配置管理包

	"mysql-to-async/internal/metadata/domain/entity" // 元数据实体包

	"mysql-to-async/internal/metadata/domain/service" // 元数据服务包

	"mysql-to-async/internal/metadata/infrastructure" // 元数据基础设施包

	syncApp "mysql-to-async/internal/sync/application" // 同步应用包

	"mysql-to-async/internal/sync/infrastructure/reader" // 同步读取器包

	"mysql-to-async/internal/sync/infrastructure/readonly" // 只读管理包

	"mysql-to-async/internal/sync/infrastructure/writer" // 同步写入器包

	taskEntity "mysql-to-async/internal/task/domain/entity" // 任务实体包

	"github.com/redis/go-redis/v9" // Redis 客户端
)

// TaskService 任务服务结构体

type TaskService struct {
	mu sync.RWMutex // 读写锁，保护并发访问

	tasks map[string]*taskEntity.SyncTask // 任务映射表，键为任务ID

	runtimes map[string]*taskRuntime // 任务运行时上下文（每任务独立连接）

	// 测试注入点：允许在单测中替换 runtime 初始化逻辑，避免依赖真实数据库

	initRuntimeFn func(task *taskEntity.SyncTask) (*taskRuntime, error)

	// 测试注入点：允许在单测中替换异步执行逻辑，稳定断言 StartTask 并发行为

	executeSyncFn func(ctx context.Context, taskID string, runtime *taskRuntime)

	storage TaskStorage // 任务存储接口

	sourceDB *sql.DB // 源数据库连接

	targetDB *sql.DB // 目标数据库连接

	analyzer service.IdentityAnalyzer // 身份分析器

	readOnlyManager *readonly.ReadOnlyManager // 只读管理器

	enableReadOnly bool // 是否启用只读限制

	checkpointManager checkpoint.Manager // 位点管理器

	incrementalSyncs map[string]*syncApp.IncrementalSyncService // 增量同步服务映射

	config *config.Config // 配置对象

	auditLogger *audit.AuditLogger // 审计日志器

}

type taskRuntime struct {
	sourceDB *sql.DB

	targetDB *sql.DB

	analyzer service.IdentityAnalyzer

	readOnlyManager *readonly.ReadOnlyManager
}

func (r *taskRuntime) Close() {

	if r == nil {

		return

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

type TaskStorage interface {
	Save(task *taskEntity.SyncTask) error // 保存任务

	Delete(taskID string) error // 删除任务

	LoadAll() ([]*taskEntity.SyncTask, error) // 加载所有任务

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

		log.Printf("Warning: failed to initialize task storage table: %v", err) // 打印警告日志

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

			log.Printf("Warning: failed to add pk_id column: %v", err) // 打印警告日志

		}

	}

	// 检查并添加 created_at 列（如果不存在）

	var createdExists int

	err = s.db.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE table_schema = DATABASE() AND table_name = 'sys_sync_tasks' AND column_name = 'created_at'").Scan(&createdExists)

	if err == nil && createdExists == 0 {

		if _, err := s.db.Exec("ALTER TABLE sys_sync_tasks ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"); err != nil {

			log.Printf("Warning: failed to add created_at column: %v", err) // 打印警告日志

		}

	}

	// 检查并添加唯一索引（如果不存在）

	var indexExists int

	err = s.db.QueryRow("SELECT COUNT(*) FROM information_schema.STATISTICS WHERE table_schema = DATABASE() AND table_name = 'sys_sync_tasks' AND index_name = 'uk_task_id'").Scan(&indexExists)

	if err == nil && indexExists == 0 {

		if _, err := s.db.Exec("ALTER TABLE sys_sync_tasks ADD UNIQUE KEY uk_task_id (id)"); err != nil {

			log.Printf("Warning: failed to add uk_task_id index: %v", err) // 打印警告日志

		}

	}

	return nil

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
	if err := task.EncryptPasswords(s.encryptKey); err != nil {
		return fmt.Errorf("encrypt passwords: %w", err)
	}
	defer func() { // 还原明文密码
		if task.Config.SourceDB != nil {
			task.Config.SourceDB.Password = origSourcePwd
		}
		if task.Config.TargetDB != nil {
			task.Config.TargetDB.Password = origTargetPwd
		}
	}()

	data, err := json.Marshal(task) // 序列化任务

	if err != nil {

		return err // 返回错误

	}

	query := "INSERT INTO sys_sync_tasks (id, name, content) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), content = VALUES(content), pk_id = LAST_INSERT_ID(pk_id)" // 构建插入或更新SQL

	res, err := s.db.Exec(query, task.Config.ID, task.Config.Name, data) // 执行SQL

	if err != nil {

		fallbackQuery := "INSERT INTO sys_sync_tasks (id, name, content) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), content = VALUES(content)" // 构建备用SQL

		if _, fbErr := s.db.Exec(fallbackQuery, task.Config.ID, task.Config.Name, data); fbErr != nil { // 执行备用SQL

			return err // 返回原始错误

		}

		return nil // 返回成功

	}

	if storageID, idErr := res.LastInsertId(); idErr == nil && storageID > 0 { // 获取插入的ID

		task.Config.StorageID = storageID // 设置存储ID

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

// LoadAll 从数据库加载所有任务

func (s *MySQLTaskStorage) LoadAll() ([]*taskEntity.SyncTask, error) {

	s.mu.RLock() // 获取读锁

	defer s.mu.RUnlock() // 延迟释放读锁

	query := "SELECT pk_id, content FROM sys_sync_tasks ORDER BY pk_id ASC" // 构建查询SQL

	rows, err := s.db.Query(query) // 执行查询

	if err != nil {

		fallbackRows, fbErr := s.db.Query("SELECT content FROM sys_sync_tasks") // 执行备用查询

		if fbErr != nil {

			return nil, err // 返回原始错误

		}

		defer fallbackRows.Close() // 延迟关闭结果集

		var fallbackTasks []*taskEntity.SyncTask // 声明备用任务列表

		for fallbackRows.Next() { // 遍历备用结果集

			var data []byte // 声明数据字节数组

			if scanErr := fallbackRows.Scan(&data); scanErr != nil { // 扫描数据

				continue // 跳过错误行

			}

			var task taskEntity.SyncTask // 声明任务对象

			if unmarshalErr := json.Unmarshal(data, &task); unmarshalErr != nil { // 反序列化任务

				log.Printf("Warning: failed to unmarshal task: %v", unmarshalErr) // 打印警告日志

				continue // 跳过错误行

			}

			if decErr := task.DecryptPasswords(s.encryptKey); decErr != nil { // 解密密码
				log.Printf("Warning: failed to decrypt task passwords: %v", decErr)
			}

			fallbackTasks = append(fallbackTasks, &task) // 添加到任务列表

		}

		return fallbackTasks, nil // 返回任务列表

	}

	defer rows.Close()

	var tasks []*taskEntity.SyncTask // 声明任务列表

	for rows.Next() { // 遍历结果集

		var storageID int64 // 声明存储ID

		var data []byte // 声明数据字节数组

		if err := rows.Scan(&storageID, &data); err != nil { // 扫描数据

			continue // 跳过错误行

		}

		var task taskEntity.SyncTask // 声明任务对象

		if err := json.Unmarshal(data, &task); err != nil { // 反序列化任务

			continue // 跳过错误行

		}

		task.Config.StorageID = storageID // 设置存储ID

		if decErr := task.DecryptPasswords(s.encryptKey); decErr != nil { // 解密密码
			log.Printf("Warning: failed to decrypt task passwords: %v", decErr)
		}

		tasks = append(tasks, &task) // 添加到任务列表

	}

	return tasks, nil // 返回任务列表

}

// NewTaskService 创建任务服务（启动时不依赖数据库）

func NewTaskService(cfg *config.Config) *TaskService {

	ts := &TaskService{

		tasks: make(map[string]*taskEntity.SyncTask), // 初始化任务映射

		runtimes: make(map[string]*taskRuntime), // 初始化任务运行时映射

		incrementalSyncs: make(map[string]*syncApp.IncrementalSyncService), // 初始化增量同步映射

		config: cfg, // 设置配置

		auditLogger: audit.NewAuditLogger("logs/audit"), // 创建审计日志器

	}

	// 初始化存储后端

	if cfg.Storage.Mode == "mysql" { // 检查存储模式是否为MySQL

		storage, err := NewMySQLTaskStorageFromConfig(&cfg.Storage, cfg.Security.EncryptKey) // 创建MySQL存储

		if err != nil {

			log.Printf("Warning: failed to initialize MySQL storage: %v, falling back to file storage", err) // 打印警告日志

			ts.storage = NewFileTaskStorage("data", cfg.Security.EncryptKey) // 降级为文件存储

		} else {

			ts.storage = storage // 使用MySQL存储

			log.Println("Using MySQL task storage") // 打印日志

		}

	} else {

		ts.storage = NewFileTaskStorage("data", cfg.Security.EncryptKey) // 使用文件存储

		log.Println("Using file task storage") // 打印日志

	}

	// 初始化位点管理器

	if cfg != nil && cfg.Redis.Host != "" { // 检查Redis配置

		// 使用Redis位点管理器

		rdb := redis.NewClient(&redis.Options{ // 创建Redis客户端

			Addr: fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port), // 设置地址

			Password: cfg.Redis.Password, // 设置密码

			DB: cfg.Redis.DB, // 设置数据库

		})

		ts.checkpointManager = checkpoint.NewRedisCheckpointManager(rdb, "dts:checkpoint") // 创建Redis检查点管理器

		log.Println("Using Redis checkpoint manager") // 打印日志

	} else {

		// 使用内存位点管理器

		ts.checkpointManager = checkpoint.NewMemoryCheckpointManager() // 创建内存检查点管理器

		log.Println("Using in-memory checkpoint manager") // 打印日志

	}

	// 加载已保存的任务

	ts.loadTasks() // 加载任务

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

		auditLogger: audit.NewAuditLogger("logs/audit"), // 创建审计日志器

	}

	// 初始化只读管理器

	ts.readOnlyManager = readonly.NewReadOnlyManager(targetDB) // 创建只读管理器

	// 初始化位点管理器（默认使用内存）

	ts.checkpointManager = checkpoint.NewMemoryCheckpointManager() // 创建内存检查点管理器

	ts.loadTasks() // 加载任务

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

		config: cfg,

		auditLogger: audit.NewAuditLogger("logs/audit"),
	}

	// 初始化存储后端

	if cfg.Storage.Mode == "mysql" {

		storage, err := NewMySQLTaskStorageFromConfig(&cfg.Storage, cfg.Security.EncryptKey)

		if err != nil {

			log.Printf("Warning: failed to initialize MySQL storage: %v, falling back to file storage", err)

			ts.storage = NewFileTaskStorage("data", cfg.Security.EncryptKey)

		} else {

			ts.storage = storage

		}

	} else {

		ts.storage = NewFileTaskStorage("data", cfg.Security.EncryptKey)

	}

	// 初始化只读管理器

	ts.readOnlyManager = readonly.NewReadOnlyManager(targetDB)

	// 初始化位点管理器

	if cfg.Redis.Host != "" {

		// 使用Redis位点管理器

		rdb := redis.NewClient(&redis.Options{

			Addr: fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),

			Password: cfg.Redis.Password,

			DB: cfg.Redis.DB,
		})

		ts.checkpointManager = checkpoint.NewRedisCheckpointManager(rdb, "dts:checkpoint")

		log.Println("Using Redis checkpoint manager")

	} else {

		// 使用内存位点管理器

		ts.checkpointManager = checkpoint.NewMemoryCheckpointManager()

		log.Println("Using in-memory checkpoint manager")

	}

	ts.loadTasks()

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

		s.tasks[task.Config.ID] = task // 添加到任务映射

	}

}

// CreateTask 创建任务

func (s *TaskService) CreateTask(config taskEntity.TaskConfig) (*taskEntity.SyncTask, error) {

	s.mu.Lock() // 获取写锁

	defer s.mu.Unlock() // 延迟释放写锁

	task := taskEntity.NewSyncTask(config) // 创建同步任务

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

// GetAllTasks 获取所有任务

func (s *TaskService) GetAllTasks() []*taskEntity.SyncTask {

	s.mu.RLock()

	defer s.mu.RUnlock()

	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks))

	for _, task := range s.tasks {

		tasks = append(tasks, task)

	}

	return tasks

}

// UpdateTask 更新任务

func (s *TaskService) UpdateTask(task *taskEntity.SyncTask) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	if _, exists := s.tasks[task.Config.ID]; !exists {

		return fmt.Errorf("task not found: %s", task.Config.ID)

	}

	s.tasks[task.Config.ID] = task

	// 保存到存储

	if err := s.storage.Save(task); err != nil {

		fmt.Printf("保存任务失败: %v\n", err)

	}

	return nil

}

// DeleteTask 删除任务

func (s *TaskService) DeleteTask(taskID string) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	if _, exists := s.tasks[taskID]; !exists {

		return fmt.Errorf("task not found: %s", taskID)

	}

	// 停止增量同步服务（如果存在）

	if incrSync, exists := s.incrementalSyncs[taskID]; exists {

		log.Printf("[Task %s] Stopping incremental sync service before deletion", taskID)

		incrSync.Stop()

		delete(s.incrementalSyncs, taskID)

	}

	if runtime, exists := s.runtimes[taskID]; exists {

		runtime.Close()

		delete(s.runtimes, taskID)

	}

	delete(s.tasks, taskID)

	// 从存储删除

	if err := s.storage.Delete(taskID); err != nil {

		fmt.Printf("删除任务失败: %v\n", err)

	}

	// 删除位点信息

	if s.checkpointManager != nil {

		if err := s.checkpointManager.Delete(context.Background(), taskID); err != nil {

			log.Printf("[Task %s] Failed to delete checkpoint: %v", taskID, err)

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

	task.Start()

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

	syncCtx := context.Background()

	execSync := s.executeSyncFn

	if execSync == nil {

		execSync = s.executeSync

	}

	go execSync(syncCtx, taskID, runtime)

	return nil

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

	sourceConfig := task.Config.SourceDB

	if sourceConfig == nil && s.config != nil {

		// 使用配置文件中的默认值

		sourceConfig = &taskEntity.DatabaseConfig{

			Host: s.config.Datasource.Host,

			Port: s.config.Datasource.Port,

			Database: task.Config.SourceSchema,

			Username: s.config.Datasource.Username,

			Password: s.config.Datasource.Password,
		}

	}

	if sourceConfig == nil {

		return nil, fmt.Errorf("source database config is required")

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

			targetSchema = sourceConfig.Database

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

	log.Printf("[Task %s] Source database connected: %s:%d/%s", task.Config.ID, sourceConfig.Host, sourceConfig.Port, sourceConfig.Database)

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

			log.Printf("Warning: Failed to create target database: %v", err)

		} else {

			log.Printf("Target database '%s' created or already exists", targetConfig.Database)

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

	log.Printf("[Task %s] Target database connected: %s:%d/%s", task.Config.ID, targetConfig.Host, targetConfig.Port, targetConfig.Database)

	// 初始化元数据分析器（如果还没有创建）

	schemaDetector := infrastructure.NewSchemaDetector(sourceDB)

	analyzer := service.NewIdentityAnalyzerService(schemaDetector)

	// 检查binlog_row_image设置

	binlogImage, err := schemaDetector.CheckBinlogRowImage()

	if err != nil {

		log.Printf("Warning: Failed to check binlog_row_image: %v", err)

	} else {

		log.Printf("binlog_row_image = %s", binlogImage)

		if binlogImage != "FULL" {

			log.Println("Warning: binlog_row_image is not FULL. Incremental sync for no-PK tables may not work correctly.")

		}

	}

	return &taskRuntime{

		sourceDB: sourceDB,

		targetDB: targetDB,

		analyzer: analyzer,

		readOnlyManager: readonly.NewReadOnlyManager(targetDB),
	}, nil

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

	log.Printf("[Task %s] Starting sync, mode: %s, tables: %v", taskID, task.Config.Mode, task.Config.Tables)

	// 在同步开始前，若任务开启了只读管理，临时关闭目标库只读以允许数据写入

	if enableReadOnly && runtime != nil && runtime.readOnlyManager != nil {

		log.Printf("[Task %s] 正在临时关闭目标实例只读以进行同步...", taskID)

		if err := runtime.readOnlyManager.SetReadOnly(); err != nil {

			log.Printf("[Task %s] 警告: 关闭只读失败: %v", taskID, err)

			// 记录错误但继续执行同步

		} else {

			log.Printf("[Task %s] 目标实例只读已临时关闭，同步结束后自动恢复", taskID)

		}

	}

	// 确保在函数退出时恢复只读状态

	defer func() {

		if enableReadOnly && runtime != nil && runtime.readOnlyManager != nil {

			log.Printf("[Task %s] 正在恢复目标实例只读状态...", taskID)

			if err := runtime.readOnlyManager.RestoreReadOnly(); err != nil {

				log.Printf("[Task %s] 警告: 恢复只读状态失败: %v", taskID, err)

			} else {

				log.Printf("[Task %s] 目标实例用户权限已恢复", taskID)

			}

		}

	}()

	// 根据模式执行同步（支持大小写不敏感）

	mode := strings.ToUpper(string(task.Config.Mode))

	switch mode {

	case "FULL":

		if err := s.executeFullSync(ctx, task, runtime); err != nil {

			if s.isTaskStopped(taskID) {

				return

			}

			s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, err.Error())

		}

	case "INCREMENTAL":

		s.executeIncrementalSync(ctx, task, runtime)

	case "ALL":

		// 先全量后增量

		if err := s.executeFullSync(ctx, task, runtime); err == nil {

			s.executeIncrementalSync(ctx, task, runtime)

		} else if !s.isTaskStopped(taskID) {

			s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, err.Error())

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

	// 构建 (sourceSchema, targetSchema) 对列表

	type dbPair struct{ src, dst string }

	var pairs []dbPair

	if len(task.Config.SourceDatabases) > 0 {

		// 多库模式：SourceDatabases[i] -> TargetDatabases[i]

		// 若 TargetDatabases 不足，用源库名作为目标库名

		for i, src := range task.Config.SourceDatabases {

			dst := src

			if i < len(task.Config.TargetDatabases) && task.Config.TargetDatabases[i] != "" {

				dst = task.Config.TargetDatabases[i]

			}

			pairs = append(pairs, dbPair{src, dst})

		}

	} else {

		// 单库模式（兼容旧逻辑）

		src := task.Config.SourceSchema

		if src == "" {

			src = "test"

		}

		dst := task.Config.TargetSchema

		if dst == "" {

			dst = src

		}

		pairs = append(pairs, dbPair{src, dst})

	}

	tablesBySource := make(map[string][]string)

	if len(task.Config.Tables) > 0 {

		defaultSource := task.Config.SourceSchema

		if defaultSource == "" && len(task.Config.SourceDatabases) > 0 {

			defaultSource = task.Config.SourceDatabases[0]

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

	// 计算所有库的总行数

	var totalRows int64

	for _, p := range pairs {

		tables := tablesBySource[p.src]

		if task.Config.SyncLevel == taskEntity.SyncLevelTable && len(task.Config.Tables) > 0 && len(tables) == 0 {

			continue

		}

		if len(tables) == 0 {

			allTables, err := runtime.analyzer.GetAllTables(p.src)

			if err != nil {

				log.Printf("[Task %s] Failed to get tables for %s: %v", taskID, p.src, err)

				continue

			}

			for _, t := range allTables {

				tables = append(tables, t.TableName)

			}

		}

		for _, tableName := range tables {

			identity, err := runtime.analyzer.AnalyzeTable(p.src, tableName)

			if err != nil {

				continue

			}

			r := reader.NewReader(runtime.sourceDB, p.src, tableName, identity)

			count, err := r.GetTotalCount(ctx)

			if err != nil {

				continue

			}

			totalRows += count

		}

	}

	s.updateTaskTotalRows(taskID, totalRows)

	// 依次同步每个库（库间串行，库内表间并行）

	for _, p := range pairs {

		if s.isTaskStopped(taskID) {

			return nil

		}

		if task.Config.SyncLevel == taskEntity.SyncLevelTable && len(task.Config.Tables) > 0 && len(tablesBySource[p.src]) == 0 {

			continue

		}

		if err := s.syncDatabasePair(ctx, task, runtime, p.src, p.dst, tablesBySource[p.src]); err != nil {

			return err

		}

	}

	s.completeTask(taskID)

	log.Printf("[Task %s] Full sync completed, total rows: %d", taskID, totalRows)

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

// adjustReadLimitForWideColumns 当表含 JSON/BLOB/TEXT 等大列时缩小单次 LIMIT，降低单轮结果集体积与驱动 Scan 耗时。

func adjustReadLimitForWideColumns(base int64, identity *entity.TableIdentity) int64 {

	if identity == nil || len(identity.Columns) == 0 {

		return base

	}

	heavy := 0

	for _, col := range identity.Columns {

		dt := strings.ToLower(strings.TrimSpace(col.DataType))

		if i := strings.IndexByte(dt, '('); i >= 0 {

			dt = dt[:i]

		}

		dt = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(dt, " unsigned"), " zerofill"))

		switch dt {

		case "json", "blob", "tinyblob", "mediumblob", "longblob",

			"text", "tinytext", "mediumtext", "longtext":

			heavy++

		}

	}

	if heavy == 0 {

		return base

	}

	const maxWideBatch int64 = 500

	div := int64(2 + heavy)

	if div < 2 {

		div = 2

	}

	scaled := base / div

	if scaled > maxWideBatch {

		scaled = maxWideBatch

	}

	if scaled < 25 {

		scaled = 25

	}

	if scaled > base {

		return base

	}

	return scaled

}

// syncDatabasePair 同步单个源库到目标库（含全部或指定表）

func (s *TaskService) syncDatabasePair(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime, sourceSchema, targetSchema string, specifiedTables []string) error {

	taskID := task.Config.ID

	// 确定要同步的表

	tables := append([]string{}, specifiedTables...)

	if len(tables) == 0 {

		log.Printf("[Task %s] 库级别同步：正在获取数据库 %s 的所有表...", taskID, sourceSchema)

		allTables, err := runtime.analyzer.GetAllTables(sourceSchema)

		if err != nil {

			errMsg := fmt.Sprintf("Failed to get tables for database %s: %v", sourceSchema, err)

			s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

			return fmt.Errorf("%s", errMsg)

		}

		for _, t := range allTables {

			tables = append(tables, t.TableName)

		}

		log.Printf("[Task %s] 找到 %d 个表: %v", taskID, len(tables), tables)

	}

	workerCount := task.Config.WorkerCount

	if workerCount <= 0 {

		workerCount = 4

	}

	// === 阶段1：串行同步所有表结构（集中 DDL，减少 read_only 切换次数） ===

	type tableReady struct {
		name string

		identity *entity.TableIdentity
	}

	log.Printf("[Task %s] 阶段1: 同步 %d 个表结构...", taskID, len(tables))

	ready := make([]tableReady, 0, len(tables))

	for _, tableName := range tables {

		if s.isTaskStopped(taskID) {

			return fmt.Errorf("task stopped")

		}

		log.Printf("[Task %s] 确保目标表: %s.%s -> %s.%s", taskID, sourceSchema, tableName, targetSchema, tableName)

		identity, err := runtime.analyzer.AnalyzeTable(sourceSchema, tableName)

		if err != nil {

			errMsg := fmt.Sprintf("Failed to analyze table %s: %v", tableName, err)

			s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

			return fmt.Errorf("%s", errMsg)

		}

		if err := s.ensureTargetTable(runtime, sourceSchema, targetSchema, tableName, identity); err != nil {

			errMsg := fmt.Sprintf("Failed to ensure target table %s.%s: %v", targetSchema, tableName, err)

			s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

			return fmt.Errorf("%s", errMsg)

		}

		log.Printf("[Task %s] Target table %s.%s is ready", taskID, targetSchema, tableName)

		ready = append(ready, tableReady{tableName, identity})

	}

	log.Printf("[Task %s] 阶段1完成：%d 个表结构就绪，开始同步数据...", taskID, len(ready))

	// === 阶段2：并发同步所有表数据 ===

	sem := make(chan struct{}, workerCount)

	var wg sync.WaitGroup

	errChan := make(chan error, len(ready))

	for _, r := range ready {

		wg.Add(1)

		go func(tableName string, identity *entity.TableIdentity) {

			defer wg.Done()

			defer func() {

				if r := recover(); r != nil {

					log.Printf("[Task %s] Critical: Table %s sync panicked: %v\n%s", taskID, tableName, r, debug.Stack())

					errChan <- fmt.Errorf("table %s panic: %v", tableName, r)

				}

			}()

			sem <- struct{}{}

			defer func() { <-sem }()

			if s.isTaskStopped(taskID) {

				return

			}

			log.Printf("[Task %s] Syncing table data: %s.%s -> %s.%s", taskID, sourceSchema, tableName, targetSchema, tableName)

			var savedIndexes []map[string]interface{}

			if task.Config.OptimizeIndex {

				log.Printf("[Task %s] Dropping non-primary indexes for table %s...", taskID, tableName)

				indexes, err := s.dropNonPrimaryKeyIndexes(runtime, targetSchema, tableName)

				if err != nil {

					log.Printf("[Task %s] Warning: Failed to drop indexes for %s: %v", taskID, tableName, err)

				} else {

					savedIndexes = indexes

					log.Printf("[Task %s] Dropped %d indexes from %s", taskID, len(savedIndexes), tableName)

				}

			}

			var tableProcessedRows int64

			readLimit := syncReadBatchLimit(task.Config.BatchSize)

			if task.Config.BatchSize > 0 && int64(task.Config.BatchSize) != readLimit {

				log.Printf("[Task %s] Table %s.%s: batch_size=%d capped to read limit %d per round-trip",

					taskID, sourceSchema, tableName, task.Config.BatchSize, readLimit)

			}

			readLimitBefore := readLimit

			readLimit = adjustReadLimitForWideColumns(readLimit, identity)

			if readLimit != readLimitBefore {

				log.Printf("[Task %s] Table %s.%s: wide columns (json/blob/text) detected, read batch %d -> %d rows per round-trip",

					taskID, sourceSchema, tableName, readLimitBefore, readLimit)

			}

			const txCommitEveryN = 40 // 每 40 批 commit 一次，减少 fsync 频率、提高吞吐

			const txCommitEveryNParallel = 5 // 并行 worker 每 5 批 commit，减少锁持有时间，避免 lock wait timeout

			const parallelWriteMaxRetries = 3 // 并行写入遇到锁超时/死锁时最大重试次数

			legacyCap, hardMax := s.intraTableConcurrencyCaps()

			intraWorkers := taskEntity.EffectiveIntraTableWorkers(task.Config.IntraTableWorkerCount, workerCount, legacyCap, hardMax)

			if task.Config.IntraTableWorkerCount > 0 {

				log.Printf("[Task %s] Table %s.%s: intra_table_worker_count effective=%d (table-level worker_count=%d)",

					taskID, sourceSchema, tableName, intraWorkers, workerCount)

			}

			// === 策略检测 ===

			canParallelRange := identity.Strategy != entity.FullColumnsStrategy &&

				len(identity.IdentifyCols) == 1 &&

				intraWorkers > 1 &&

				isNumericPKColumn(identity, identity.IdentifyCols[0])

			canParallelSample := !canParallelRange &&

				identity.Strategy != entity.FullColumnsStrategy &&

				len(identity.IdentifyCols) >= 1 &&

				intraWorkers > 1

			var minPK, maxPK int64

			if canParallelRange {

				if err := runtime.sourceDB.QueryRowContext(ctx,

					fmt.Sprintf("SELECT COALESCE(MIN(`%s`), 0), COALESCE(MAX(`%s`), -1) FROM `%s`.`%s`",

						identity.IdentifyCols[0], identity.IdentifyCols[0], sourceSchema, tableName),
				).Scan(&minPK, &maxPK); err != nil || maxPK < minPK {

					canParallelRange = false

					canParallelSample = identity.Strategy != entity.FullColumnsStrategy &&

						len(identity.IdentifyCols) >= 1 && intraWorkers > 1

					log.Printf("[Task %s] Cannot get numeric PK range for %s.%s, trying sample parallel", taskID, sourceSchema, tableName)

				}

			}

			var sampleBoundaries []interface{}

			if canParallelSample {

				var totalRows int64

				if qErr := runtime.sourceDB.QueryRowContext(ctx,

					fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", sourceSchema, tableName),
				).Scan(&totalRows); qErr != nil || totalRows < int64(intraWorkers)*2 {

					canParallelSample = false

					log.Printf("[Task %s] Skipping sample parallel for %s.%s", taskID, sourceSchema, tableName)

				} else {

					var bErr error

					sampleBoundaries, bErr = s.samplePKBoundariesImproved(ctx, runtime, sourceSchema, tableName, identity.IdentifyCols, totalRows, intraWorkers)

					if bErr != nil {

						canParallelSample = false

						log.Printf("[Task %s] Boundary sampling failed for %s.%s: %v", taskID, sourceSchema, tableName, bErr)

					}

				}

			}

			// === 执行同步 ===

			if identity.Strategy == entity.FullColumnsStrategy {

				// 无主键表：单协程流式读取 + 事务批量提交

				conn, err := runtime.targetDB.Conn(ctx)

				if err != nil {

					errMsg := fmt.Sprintf("Failed to get write connection for %s: %v", tableName, err)

					log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

					s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

					errChan <- err

					return

				}

				defer func() {

					conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=1, UNIQUE_CHECKS=1")

					conn.Close()

				}()

				conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0")

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

						txW = writer.NewBatchWriterWithTx(curTx, identity, task.Config.BatchSize, targetSchema)

						if s.auditLogger != nil {

							txW.SetAuditLogger(s.auditLogger, taskID, sourceSchema, tableName)

						}

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

						log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

						s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

						errChan <- err

						return

					}

					if len(rows) == 0 {

						break

					}

					mark := fmt.Sprintf("%s.%s:%d", sourceSchema, tableName, tableProcessedRows+int64(len(rows)))

					if err := doWrite(rows, mark); err != nil {

						errMsg := fmt.Sprintf("Write failed for `%s`.`%s`: %v", sourceSchema, tableName, err)

						log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

						s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

						errChan <- err

						return

					}

					tableProcessedRows += int64(len(rows))

					s.incrementTaskProgress(taskID, int64(len(rows)), mark)

				}

				if curTx != nil {

					if err := curTx.Commit(); err != nil {

						errMsg := fmt.Sprintf("Final commit failed for `%s`.`%s` (from %s): %v", sourceSchema, tableName, txStartMark, err)

						log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

						s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

						errChan <- err

						return

					}

					curTx = nil

				}

			} else if canParallelRange {

				// 真正的keyset分页：每个worker从上一个worker的结束位置开始

				log.Printf("[Task %s] Table %s.%s: parallel keyset sync workers=%d",

					taskID, sourceSchema, tableName, intraWorkers)

				var syncWg sync.WaitGroup

				syncErrChan := make(chan error, intraWorkers)

				var atomicProcessed int64

				var workerSnapshotConns []*sql.Conn

				if task.Config.EnableConsistentSnapshot {

					conns, err := s.prepareConsistentSnapshotReaders(ctx, runtime, intraWorkers)

					if err != nil {

						errMsg := fmt.Sprintf("Failed to prepare parallel consistent snapshot for `%s`.`%s`: %v", sourceSchema, tableName, err)

						log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

						s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

						errChan <- err

						return

					}

					workerSnapshotConns = conns

					defer s.releaseConsistentSnapshotReaders(workerSnapshotConns)

					if len(workerSnapshotConns) > 0 {

						if err := workerSnapshotConns[0].QueryRowContext(ctx,

							fmt.Sprintf("SELECT COALESCE(MIN(`%s`), 0), COALESCE(MAX(`%s`), -1) FROM `%s`.`%s`",

								identity.IdentifyCols[0], identity.IdentifyCols[0], sourceSchema, tableName),
						).Scan(&minPK, &maxPK); err != nil || maxPK < minPK {

							errMsg := fmt.Sprintf("Failed to get numeric PK range in snapshot for `%s`.`%s`: %v", sourceSchema, tableName, err)

							log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

							s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

							errChan <- fmt.Errorf("snapshot range failed: %w", err)

							return

						}

					}

					log.Printf("[Task %s] Table %s.%s: parallel consistent snapshot enabled with %d workers",

						taskID, sourceSchema, tableName, intraWorkers)

				}

				// 数值主键按 min/max 等分，避免采样边界导致有效 worker 塌缩

				span := maxPK - minPK + 1

				if span < int64(intraWorkers) {

					intraWorkers = int(span)

					if intraWorkers < 1 {

						intraWorkers = 1

					}

				}

				chunkSize := (span + int64(intraWorkers) - 1) / int64(intraWorkers)

				log.Printf("[Task %s] Table %s.%s: numeric range split min=%d max=%d workers=%d chunk=%d",

					taskID, sourceSchema, tableName, minPK, maxPK, intraWorkers, chunkSize)

				for w := 0; w < intraWorkers; w++ {

					syncWg.Add(1)

					go func(wIdx int) {

						defer syncWg.Done()

						defer func() {

							if r := recover(); r != nil {

								syncErrChan <- fmt.Errorf("w%d panic: %v", wIdx, r)

							}

						}()

						readSource := sourceQueryer(runtime.sourceDB)

						if len(workerSnapshotConns) == intraWorkers {

							readSource = workerSnapshotConns[wIdx]

						}

						// 创建reader

						wReader := reader.NewRangeShardingReader(readSource, sourceSchema, tableName, identity)

						// 每个worker负责 [workerStart, workerEnd)

						workerStart := minPK + int64(wIdx)*chunkSize

						if workerStart > maxPK {

							return

						}

						workerEnd := maxPK + 1

						if wIdx < intraWorkers-1 {

							nextStart := minPK + int64(wIdx+1)*chunkSize

							if nextStart < workerEnd {

								workerEnd = nextStart

							}

						}

						startBoundary := interface{}(workerStart - 1)

						endBoundary := interface{}(workerEnd)

						conn, err := runtime.targetDB.Conn(ctx)

						if err != nil {

							syncErrChan <- fmt.Errorf("w%d conn: %v", wIdx, err)

							return

						}

						conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0")
						conn.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout=300")

						defer func() {

							conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=1, UNIQUE_CHECKS=1")

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

							for attempt := 0; ; attempt++ {

								if curTx == nil {

									var e error

									curTx, e = conn.BeginTx(ctx, nil)

									if e != nil {

										return fmt.Errorf("w%d begin tx at %s: %v", wIdx, mark, e)

									}

									txW = writer.NewBatchWriterWithTx(curTx, identity, task.Config.BatchSize, targetSchema)

									if s.auditLogger != nil {

										txW.SetAuditLogger(s.auditLogger, taskID, sourceSchema, tableName)

									}

									txBatchN = 0

									txStartMark = mark

								}

								if e := txW.WriteBatch(ctx, rows); e != nil {

									curTx.Rollback()

									curTx = nil

									if isRetryableLockError(e) && attempt < parallelWriteMaxRetries {
										backoff := time.Duration(1+attempt) * time.Second
										log.Printf("[Task %s] w%d lock contention at %s, retry %d/%d after %v: %v",
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

						lastID := startBoundary

						rangeMark := fmt.Sprintf("%s.%s:w%d:keyset", sourceSchema, tableName, wIdx)

						for {

							if s.isTaskStopped(taskID) {

								return

							}

							// 使用 ReadBatchByKeys 进行keyset分页

							batch, err := wReader.ReadBatchByKeys(ctx, lastID, readLimit)

							if err != nil {

								syncErrChan <- fmt.Errorf("w%d read after %v: %v", wIdx, lastID, err)

								return

							}

							if len(batch) == 0 {

								log.Printf("[Task %s] w%d reached end of data at %v", taskID, wIdx, lastID)

								break

							}

							// 数据一致性检查：验证第一批数据的连续性

							if lastID != nil {

								firstPK := batch[0][identity.IdentifyCols[0]]

								if comparePKValues(firstPK, lastID) <= 0 {

									syncErrChan <- fmt.Errorf("w%d data continuity error: first PK %v should be > lastID %v", wIdx, firstPK, lastID)

									return

								}

							}

							// 非最后一个worker：检查是否超出边界

							if endBoundary != nil {

								cutIdx := len(batch)

								for j, row := range batch {

									if comparePKValues(row[identity.IdentifyCols[0]], endBoundary) >= 0 {

										cutIdx = j

										break

									}

								}

								if cutIdx < len(batch) {

									log.Printf("[Task %s] w%d cutting %d rows that exceed boundary %v", taskID, wIdx, len(batch)-cutIdx, endBoundary)

									batch = batch[:cutIdx]

								}

							}

							if len(batch) == 0 {

								break

							}

							// 记录批次范围信息

							firstPK := batch[0][identity.IdentifyCols[0]]

							lastPK := batch[len(batch)-1][identity.IdentifyCols[0]]

							log.Printf("[Task %s] w%d processing batch: %s (%d rows) from %v to %v",

								taskID, wIdx, rangeMark, len(batch), firstPK, lastPK)

							if err := doWrite(batch, rangeMark); err != nil {

								syncErrChan <- err

								return

							}

							n := int64(len(batch))

							atomic.AddInt64(&atomicProcessed, n)

							s.incrementTaskProgress(taskID, n, rangeMark)

							// 更新 lastID 为最后一条记录的主键值，确保连续性

							lastRow := batch[len(batch)-1]

							lastID = lastRow[identity.IdentifyCols[0]]

							// 检查是否达到边界

							if endBoundary != nil && comparePKValues(lastID, endBoundary) >= 0 {

								log.Printf("[Task %s] w%d reached boundary %v at %v", taskID, wIdx, endBoundary, lastID)

								break

							}

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

					log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

					s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

					errChan <- err

					return

				}

				tableProcessedRows = atomic.LoadInt64(&atomicProcessed)

			} else if canParallelSample {

				// 非数值单列主键 / 复合主键：采样边界 + 表内并行 keyset + 每 worker 独立事务批量提交

				pkCol := identity.IdentifyCols[0]

				log.Printf("[Task %s] Table %s.%s: parallel sample sync pk=%s workers=%d",

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

						conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0")
						conn.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout=300")

						defer func() {

							conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=1, UNIQUE_CHECKS=1")

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

							for attempt := 0; ; attempt++ {

								if curTx == nil {

									var e error

									curTx, e = conn.BeginTx(ctx, nil)

									if e != nil {

										return fmt.Errorf("w%d begin tx at %s: %v", wIdx, mark, e)

									}

									txW = writer.NewBatchWriterWithTx(curTx, identity, task.Config.BatchSize, targetSchema)

									if s.auditLogger != nil {

										txW.SetAuditLogger(s.auditLogger, taskID, sourceSchema, tableName)

									}

									txBatchN = 0

									txStartMark = mark

								}

								if e := txW.WriteBatch(ctx, rows); e != nil {

									curTx.Rollback()

									curTx = nil

									if isRetryableLockError(e) && attempt < parallelWriteMaxRetries {
										backoff := time.Duration(1+attempt) * time.Second
										log.Printf("[Task %s] w%d lock contention at %s, retry %d/%d after %v: %v",
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

							batchRows, err := wReader.ReadBatchByKeys(ctx, lastID, readLimit)

							if err != nil {

								syncErrChan <- fmt.Errorf("w%d read after %v: %v", wIdx, lastID, err)

								return

							}

							if len(batchRows) == 0 {

								break

							}

							// 非最后一个 worker：裁剪超出 endBoundary 的行（支持复合主键完整比较）

							if endBoundary != nil {

								cutIdx := len(batchRows)

								for j, row := range batchRows {

									if comparePKWithBoundary(identity.IdentifyCols, row, endBoundary) > 0 {

										cutIdx = j

										break

									}

								}

								batchRows = batchRows[:cutIdx]

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

							s.incrementTaskProgress(taskID, n, mark)

							// 更新 lastID：复合主键需要传完整的 []interface{} 给 ReadBatchByKeys
							if len(identity.IdentifyCols) == 1 {
								lastID = firstPKVal
							} else {
								compositePK := make([]interface{}, len(identity.IdentifyCols))
								for ci, col := range identity.IdentifyCols {
									compositePK[ci] = lastRow[col]
								}
								lastID = compositePK
							}

							// 边界检查：使用完整复合主键比较（采样边界已包含所有主键列）
							if endBoundary != nil && comparePKWithBoundary(identity.IdentifyCols, lastRow, endBoundary) >= 0 {

								break

							}

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

					log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

					s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

					errChan <- err

					return

				}

				tableProcessedRows = atomic.LoadInt64(&atomicProcessed)

			} else {

				// 回退（单worker / 采样失败）：Keyset Pagination 顺序读取 + 事务批量提交

				conn, err := runtime.targetDB.Conn(ctx)

				if err != nil {

					errMsg := fmt.Sprintf("Failed to get write connection for %s: %v", tableName, err)

					log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

					s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

					errChan <- err

					return

				}

				defer func() {

					conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=1, UNIQUE_CHECKS=1")

					conn.Close()

				}()

				conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0")

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

						txW = writer.NewBatchWriterWithTx(curTx, identity, task.Config.BatchSize, targetSchema)

						if s.auditLogger != nil {

							txW.SetAuditLogger(s.auditLogger, taskID, sourceSchema, tableName)

						}

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

				var lastID interface{}

				for {

					if s.isTaskStopped(taskID) {

						return

					}

					rows, err := dr.ReadBatchByKeys(ctx, lastID, readLimit)

					if err != nil {

						errMsg := fmt.Sprintf("Failed to read batch for `%s`.`%s` via keyset: %v", sourceSchema, tableName, err)

						log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

						s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

						errChan <- err

						return

					}

					if len(rows) == 0 {

						break

					}

					lastRow := rows[len(rows)-1]

					pkCols := identity.IdentifyCols

					if len(pkCols) == 1 {

						lastID = lastRow[pkCols[0]]

					} else {

						vals := make([]interface{}, len(pkCols))

						for i, col := range pkCols {

							vals[i] = lastRow[col]

						}

						lastID = vals

					}

					mark := fmt.Sprintf("%s.%s:%v", sourceSchema, tableName, lastID)

					if err := doWrite(rows, mark); err != nil {

						errMsg := fmt.Sprintf("Write failed for `%s`.`%s`: %v", sourceSchema, tableName, err)

						log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

						s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

						errChan <- err

						return

					}

					tableProcessedRows += int64(len(rows))

					s.incrementTaskProgress(taskID, int64(len(rows)), mark)

				}

				if curTx != nil {

					if err := curTx.Commit(); err != nil {

						errMsg := fmt.Sprintf("Final commit failed for `%s`.`%s` (from %s): %v", sourceSchema, tableName, txStartMark, err)

						log.Printf("[Task %s] ERROR: %s", taskID, errMsg)

						s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)

						errChan <- err

						return

					}

					curTx = nil

				}

			}

			if task.Config.OptimizeIndex && len(savedIndexes) > 0 {

				log.Printf("[Task %s] Restoring indexes for table %s...", taskID, tableName)

				if err := s.restoreIndexes(runtime, targetSchema, tableName, savedIndexes); err != nil {

					log.Printf("[Task %s] Warning: Failed to restore indexes for %s: %v", taskID, tableName, err)

				} else {

					log.Printf("[Task %s] Restored indexes for table %s", taskID, tableName)

				}

			}

			log.Printf("[Task %s] Table %s.%s completed, processed %d rows", taskID, sourceSchema, tableName, tableProcessedRows)

		}(r.name, r.identity)

	}

	wg.Wait()

	close(errChan)

	if len(errChan) > 0 {

		return <-errChan

	}

	return nil

}

// executeIncrementalSync 执行增量同步

func (s *TaskService) executeIncrementalSync(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime) {

	taskID := task.Config.ID

	if runtime == nil || runtime.sourceDB == nil || runtime.targetDB == nil || runtime.analyzer == nil {

		log.Printf("[Task %s] Error: runtime is nil, cannot start incremental sync", taskID)

		s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, "task runtime is nil")

		return

	}

	// 获取配置信息

	cfg := s.config

	if cfg == nil {

		log.Printf("[Task %s] Error: config is nil, cannot start incremental sync", taskID)

		s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, "config is nil")

		return

	}

	// 获取源数据库配置

	sourceHost := cfg.Datasource.Host

	sourcePort := cfg.Datasource.Port

	sourceUsername := cfg.Datasource.Username

	sourcePassword := cfg.Datasource.Password

	sourceSchema := task.Config.SourceSchema

	// 如果任务配置中有自定义源数据库，使用任务配置

	if task.Config.SourceDB != nil {

		sourceHost = task.Config.SourceDB.Host

		sourcePort = task.Config.SourceDB.Port

		sourceUsername = task.Config.SourceDB.Username

		sourcePassword = task.Config.SourceDB.Password

		if task.Config.SourceDB.Database != "" {

			sourceSchema = task.Config.SourceDB.Database

		}

	}

	// 如果没有指定schema，使用默认值

	if sourceSchema == "" {

		sourceSchema = "test"

	}

	log.Printf("[Task %s] Starting incremental sync for schema: %s, tables: %v", taskID, sourceSchema, task.Config.Tables)

	// 确定目标 schema（与 executeFullSync 保持一致）

	targetSchema := task.Config.TargetSchema

	if targetSchema == "" {

		targetSchema = sourceSchema

	}

	// 创建增量同步配置

	syncConfig := &syncApp.SyncConfig{

		TaskID: taskID,

		SourceHost: sourceHost,

		SourcePort: sourcePort,

		SourceUsername: sourceUsername,

		SourcePassword: sourcePassword,

		SourceSchema: sourceSchema,

		TargetSchema: targetSchema,

		SourceDatabases: task.Config.SourceDatabases,

		TargetDatabases: task.Config.TargetDatabases,

		Tables: task.Config.Tables,

		BatchSize: task.Config.BatchSize,

		ServerID: generateServerID(taskID),
	}

	// 创建增量同步服务

	incrSync := syncApp.NewIncrementalSyncService(

		runtime.sourceDB,

		runtime.targetDB,

		runtime.analyzer,

		s.checkpointManager,
	)

	// 保存到映射中

	s.mu.Lock()

	s.incrementalSyncs[taskID] = incrSync

	s.mu.Unlock()

	// 启动增量同步

	if err := incrSync.Start(ctx, taskID, syncConfig); err != nil {

		log.Printf("[Task %s] Failed to start incremental sync: %v", taskID, err)

		s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, err.Error())

		return

	}

	log.Printf("[Task %s] Incremental sync started successfully", taskID)

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

// ensureTargetTable 确保目标表存在

func (s *TaskService) ensureTargetTable(runtime *taskRuntime, sourceSchema, targetSchema, tableName string, identity *entity.TableIdentity) error {

	if runtime == nil || runtime.sourceDB == nil || runtime.targetDB == nil {

		return fmt.Errorf("task runtime is not initialized")

	}

	targetDB := runtime.targetDB

	sourceDB := runtime.sourceDB

	// 首先确保目标数据库存在：先用 SELECT 查询（只读操作），避免在 read_only 目标库上无谓执行 DDL

	var dbExists string

	err := targetDB.QueryRow(

		"SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?", targetSchema,
	).Scan(&dbExists)

	if err == sql.ErrNoRows {

		// 数据库不存在，临时解除只读后创建

		if err = s.withDDL(runtime, func() error {

			_, e := targetDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", targetSchema))

			return e

		}); err != nil {

			return fmt.Errorf("failed to create target database %s: %v", targetSchema, err)

		}

		log.Printf("[Task] Target database '%s' created", targetSchema)

	} else if err != nil {

		return fmt.Errorf("failed to check target database %s: %v", targetSchema, err)

	}

	// err == nil 说明数据库已存在，直接跳过创建

	// 检查目标表是否存在

	var tableNameCheck string

	err = targetDB.QueryRow(

		fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", targetSchema, tableName),
	).Scan(&tableNameCheck)

	if err == nil {

		// 表已存在，不需要创建

		log.Printf("[Task] Target table %s.%s already exists, skipping creation", targetSchema, tableName)

		return nil

	}

	if err != sql.ErrNoRows {

		// 查询出错

		return fmt.Errorf("failed to check target table %s.%s: %v", targetSchema, tableName, err)

	}

	// 表不存在，创建它

	log.Printf("[Task] Creating target table %s.%s (from %s.%s)", targetSchema, tableName, sourceSchema, tableName)

	// 方法1：尝试使用源数据库连接在目标库创建表（源和目标在同一服务器时有效）

	if sourceDB != nil {

		tryErr := s.withDDL(runtime, func() error {

			_, e := sourceDB.Exec(fmt.Sprintf("CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`",

				targetSchema, tableName, sourceSchema, tableName))

			return e

		})

		if tryErr == nil {

			log.Printf("[Task] Successfully created target table %s.%s (using source DB connection)", targetSchema, tableName)

			return nil

		}

		log.Printf("[Task] Failed to create table using source DB connection: %v", tryErr)

	}

	// 方法2：获取源表的CREATE TABLE语句并在目标数据库执行

	var createSQL string

	err = sourceDB.QueryRow(

		fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", sourceSchema, tableName),
	).Scan(&tableName, &createSQL)

	if err != nil {

		return fmt.Errorf("failed to get CREATE TABLE statement for %s.%s: %v", sourceSchema, tableName, err)

	}

	createSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` %s", targetSchema, tableName,

		extractTableDefinition(createSQL))

	if err = s.withDDL(runtime, func() error {

		_, e := targetDB.Exec(createSQL)

		return e

	}); err != nil {

		return fmt.Errorf("failed to create target table %s.%s: %v", targetSchema, tableName, err)

	}

	log.Printf("[Task] Successfully created target table %s.%s (using CREATE TABLE statement)", targetSchema, tableName)

	return nil

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

// 并发采样边界计算：统一处理单列和复合主键

func (s *TaskService) samplePKBoundariesImproved(ctx context.Context, runtime *taskRuntime, schema, table string, pkCols []string, totalRows int64, n int) ([]interface{}, error) {

	if runtime == nil || runtime.sourceDB == nil {

		return nil, fmt.Errorf("task runtime source db is not initialized")

	}

	if totalRows < int64(n)*2 {

		// 数据量太少，不值得并行

		return nil, fmt.Errorf("insufficient rows for parallel processing: %d", totalRows)

	}

	// 构建 SELECT 和 ORDER BY 列列表
	var quotedCols []string
	for _, col := range pkCols {
		quotedCols = append(quotedCols, fmt.Sprintf("`%s`", col))
	}
	colList := strings.Join(quotedCols, ", ")

	boundaries := make([]interface{}, n-1)

	// 使用动态采样：先获取几个采样点来评估分布

	samplePoints := min(10, n) // 最多采样10个点

	samples := make([]interface{}, samplePoints)

	for i := 0; i < samplePoints; i++ {

		offset := totalRows * int64(i) / int64(samplePoints-1)

		if offset >= totalRows {

			offset = totalRows - 1

		}

		if len(pkCols) == 1 {
			// 单列主键：返回单值
			var pk interface{}
			err := runtime.sourceDB.QueryRowContext(ctx,
				fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s LIMIT 1 OFFSET ?",
					colList, schema, table, colList),
				offset,
			).Scan(&pk)
			if err != nil {
				return nil, fmt.Errorf("sample point %d failed: %v", i, err)
			}
			samples[i] = pk
		} else {
			// 复合主键：返回 []interface{} 包含所有列值
			vals := make([]interface{}, len(pkCols))
			ptrs := make([]interface{}, len(pkCols))
			for j := range vals {
				ptrs[j] = &vals[j]
			}
			err := runtime.sourceDB.QueryRowContext(ctx,
				fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s LIMIT 1 OFFSET ?",
					colList, schema, table, colList),
				offset,
			).Scan(ptrs...)
			if err != nil {
				return nil, fmt.Errorf("sample point %d failed: %v", i, err)
			}
			result := make([]interface{}, len(vals))
			copy(result, vals)
			samples[i] = result
		}

	}

	// 基于采样点智能分配边界

	if n <= 2 {

		// 2个worker：直接在中点分割

		if len(samples) > 1 {

			// 确保边界不等于第一个采样点

			for i := 1; i < len(samples); i++ {

				if boundaryToString(samples[i]) != boundaryToString(samples[0]) {

					boundaries[0] = samples[i]

					break

				}

			}

		}

	} else {

		// 多个worker：均匀分布边界点，确保不重复

		step := float64(samplePoints-1) / float64(n-1)

		for i := 1; i < n; i++ {

			sampleIdx := int(step * float64(i))

			if sampleIdx >= len(samples) {

				sampleIdx = len(samples) - 1

			}

			// 确保边界不重复且递增

			if i > 1 {

				prevBoundary := boundaries[i-2]

				for sampleIdx < len(samples) &&

					boundaryToString(samples[sampleIdx]) == boundaryToString(prevBoundary) {

					sampleIdx++

				}

			}

			if sampleIdx < len(samples) {

				boundaries[i-1] = samples[sampleIdx]

			}

		}

	}

	log.Printf("Dynamic boundary sampling: totalRows=%d, workers=%d, pkCols=%v, boundaries=%v",

		totalRows, n, pkCols, boundaries)

	return boundaries, nil

}

func min(a, b int) int {

	if a < b {

		return a

	}

	return b

}

func (s *TaskService) prepareConsistentSnapshotReaders(ctx context.Context, runtime *taskRuntime, workers int) ([]*sql.Conn, error) {

	if runtime == nil || runtime.sourceDB == nil {

		return nil, fmt.Errorf("task runtime source db is not initialized")

	}

	if workers < 1 {

		return nil, fmt.Errorf("invalid workers: %d", workers)

	}

	lockConn, err := runtime.sourceDB.Conn(ctx)

	if err != nil {

		return nil, fmt.Errorf("get lock connection failed: %w", err)

	}

	if _, err := lockConn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {

		lockConn.Close()

		return nil, fmt.Errorf("acquire global read lock failed: %w", err)

	}

	conns := make([]*sql.Conn, 0, workers)

	cleanup := func() {

		for _, c := range conns {

			if c != nil {

				c.ExecContext(context.Background(), "ROLLBACK")

				c.Close()

			}

		}

		lockConn.ExecContext(context.Background(), "UNLOCK TABLES")

		lockConn.Close()

	}

	for i := 0; i < workers; i++ {

		conn, err := runtime.sourceDB.Conn(ctx)

		if err != nil {

			cleanup()

			return nil, fmt.Errorf("get worker snapshot connection %d failed: %w", i, err)

		}

		if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {

			conn.Close()

			cleanup()

			return nil, fmt.Errorf("set worker %d isolation failed: %w", i, err)

		}

		if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"); err != nil {

			conn.Close()

			cleanup()

			return nil, fmt.Errorf("start worker %d snapshot transaction failed: %w", i, err)

		}

		conns = append(conns, conn)

	}

	if _, err := lockConn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {

		lockConn.Close()

		for _, c := range conns {

			if c != nil {

				c.ExecContext(context.Background(), "ROLLBACK")

				c.Close()

			}

		}

		return nil, fmt.Errorf("unlock tables failed: %w", err)

	}

	lockConn.Close()

	return conns, nil

}

func (s *TaskService) releaseConsistentSnapshotReaders(conns []*sql.Conn) {

	for _, c := range conns {

		if c == nil {

			continue

		}

		c.ExecContext(context.Background(), "ROLLBACK")

		c.Close()

	}

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

	as := fmt.Sprintf("%v", a)

	bs := fmt.Sprintf("%v", b)

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
			parts[i] = fmt.Sprintf("%v", val)
		}
		return strings.Join(parts, "\x00")
	}
	return fmt.Sprintf("%v", v)
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
			rowStr := fmt.Sprintf("%v", row[col])
			bndStr := fmt.Sprintf("%v", boundaryVals[i])
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
	rowStr := fmt.Sprintf("%v", row[pkCols[0]])
	bndStr := fmt.Sprintf("%v", boundary)
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

// updateTaskProgress 更新任务进度

func (s *TaskService) updateTaskProgress(taskID string, processedRows int64, position string) {

	s.mu.Lock()

	defer s.mu.Unlock()

	if task, exists := s.tasks[taskID]; exists {

		task.UpdateProgress(processedRows, position)

		s.storage.Save(task)

	}

}

// incrementTaskProgress 原子增加任务进度

func (s *TaskService) incrementTaskProgress(taskID string, delta int64, position string) {

	s.mu.Lock()

	defer s.mu.Unlock()

	if task, exists := s.tasks[taskID]; exists {

		task.Context.ProcessedRows += delta

		task.Context.CurrentPosition = position

		task.Context.LastUpdateTime = time.Now()

		if task.Context.TotalRows > 0 {

			task.Context.ProgressPercent = float64(task.Context.ProcessedRows) / float64(task.Context.TotalRows) * 100

		}

		s.storage.Save(task)

	}

}

// updateTaskTotalRows 更新任务总行数

func (s *TaskService) updateTaskTotalRows(taskID string, totalRows int64) {

	s.mu.Lock()

	defer s.mu.Unlock()

	if task, exists := s.tasks[taskID]; exists {

		task.Context.TotalRows = totalRows

		task.Context.LastUpdateTime = time.Now()

		s.storage.Save(task)

	}

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

		s.storage.Save(task)

	}

}

// completeTask 完成任务

func (s *TaskService) completeTask(taskID string) {

	s.mu.Lock()

	defer s.mu.Unlock()

	if task, exists := s.tasks[taskID]; exists {

		task.Complete()

		s.storage.Save(task)

	}

}

// PauseTask 暂停任务

func (s *TaskService) PauseTask(taskID string) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]

	if !exists {

		return fmt.Errorf("task not found: %s", taskID)

	}

	task.Pause()

	// 停止增量同步服务（如果存在）

	if incrSync, exists := s.incrementalSyncs[taskID]; exists {

		log.Printf("[Task %s] Stopping incremental sync service", taskID)

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

// SkipError 跳过错误

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

		"progress_percent": task.Context.ProgressPercent,

		"tables_completed": 0,

		"tables_total": len(task.Config.Tables),

		"status": task.Context.Status,

		"current_position": task.Context.CurrentPosition,
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

		log.Printf("Warning: failed to create data directory: %v", err)

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
	if err := task.EncryptPasswords(s.encryptKey); err != nil {
		return fmt.Errorf("encrypt passwords: %w", err)
	}
	defer func() {
		if task.Config.SourceDB != nil {
			task.Config.SourceDB.Password = origSourcePwd
		}
		if task.Config.TargetDB != nil {
			task.Config.TargetDB.Password = origTargetPwd
		}
	}()

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

			log.Printf("Warning: failed to read task file %s: %v", file.Name(), err)

			continue

		}

		var task taskEntity.SyncTask

		if err := json.Unmarshal(data, &task); err != nil {

			log.Printf("Warning: failed to unmarshal task file %s: %v", file.Name(), err)

			continue

		}

		if decErr := task.DecryptPasswords(s.encryptKey); decErr != nil { // 解密密码
			log.Printf("Warning: failed to decrypt task passwords in file %s: %v", file.Name(), decErr)
		}

		tasks = append(tasks, &task)

	}

	return tasks, nil

}

// Close 优雅关闭任务服务

func (s *TaskService) Close() error {

	s.mu.Lock()

	defer s.mu.Unlock()

	log.Println("Closing task service...")

	// 1. 停止所有增量同步服务

	for taskID, incrSync := range s.incrementalSyncs {

		log.Printf("[Task %s] Stopping incremental sync service", taskID)

		incrSync.Stop()

		delete(s.incrementalSyncs, taskID)

	}

	for taskID, runtime := range s.runtimes {

		runtime.Close()

		delete(s.runtimes, taskID)

	}

	// 2. 保存所有任务状态

	for taskID, task := range s.tasks {

		// 如果任务正在运行，暂停它

		if task.Context.Status == taskEntity.TaskStatusRunning {

			task.Pause()

			log.Printf("[Task %s] Task paused due to service shutdown", taskID)

		}

		// 保存任务状态

		if err := s.storage.Save(task); err != nil {

			log.Printf("[Task %s] Failed to save task state: %v", taskID, err)

		}

	}

	// 3. 关闭审计日志器

	if s.auditLogger != nil {

		if err := s.auditLogger.Close(); err != nil {

			log.Printf("Failed to close audit logger: %v", err)

		}

	}

	log.Println("Task service closed successfully")

	return nil

}

// dropNonPrimaryKeyIndexes 删除非主键索引

func (s *TaskService) dropNonPrimaryKeyIndexes(runtime *taskRuntime, schema, tableName string) ([]map[string]interface{}, error) {

	if runtime == nil || runtime.targetDB == nil {

		return nil, fmt.Errorf("task runtime target db is not initialized")

	}

	targetDB := runtime.targetDB

	// 查询所有索引

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", schema, tableName)

	rows, err := targetDB.Query(query)

	if err != nil {

		return nil, fmt.Errorf("failed to show indexes: %v", err)

	}

	defer rows.Close()

	cols, err := rows.Columns()

	if err != nil {

		return nil, fmt.Errorf("failed to get index columns: %v", err)

	}

	// 按 Key_name 分组收集索引信息（正确处理多列复合索引）
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

		// 动态扫描所有列（兼容 MySQL 5.7 / 8.0+ 列数差异）
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {

			log.Printf("Warning: failed to scan index row: %v", err)

			continue

		}

		// SHOW INDEX 列顺序（固定）：
		// 0:Table, 1:Non_unique, 2:Key_name, 3:Seq_in_index, 4:Column_name,
		// 5:Collation, 6:Cardinality, 7:Sub_part, 8:Packed, 9:Null,
		// 10:Index_type, 11:Comment, 12:Index_comment [, 13:Visible, 14:Expression]
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
		meta.Columns = append(meta.Columns, indexColumn{
			Column: columnName, SeqInIndex: seqInIndex, SubPart: subPart,
		})

	}

	if len(indexMap) == 0 {

		return nil, nil

	}

	// 构建保存的索引信息（用于恢复）
	var savedIndexes []map[string]interface{}
	for name, meta := range indexMap {
		// 按 Seq_in_index 排序列
		sort.Slice(meta.Columns, func(i, j int) bool {
			return meta.Columns[i].SeqInIndex < meta.Columns[j].SeqInIndex
		})
		// 构建列定义字符串（含前缀长度）
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

	// 删除非主键索引

	for name := range indexMap {

		dropQuery := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`", schema, tableName, name)

		_, err := targetDB.Exec(dropQuery)

		if err != nil {

			log.Printf("Warning: failed to drop index %s: %v", name, err)

			continue

		}

		log.Printf("Dropped index %s from table %s.%s", name, schema, tableName)

	}

	return savedIndexes, nil

}

// restoreIndexes 恢复索引

func (s *TaskService) restoreIndexes(runtime *taskRuntime, schema, tableName string, indexes []map[string]interface{}) error {

	if runtime == nil || runtime.targetDB == nil {

		return fmt.Errorf("task runtime target db is not initialized")

	}

	targetDB := runtime.targetDB

	if len(indexes) == 0 {

		return nil

	}

	log.Printf("[Task] Restoring %d indexes for table %s.%s...", len(indexes), schema, tableName)

	for _, indexInfo := range indexes {

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

		// 构建 CREATE INDEX 语句（区分 UNIQUE / FULLTEXT / SPATIAL / 普通索引）
		var createSQL string
		if nonUnique == 0 {
			createSQL = fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s)",
				indexName, schema, tableName, columns)
		} else if strings.EqualFold(indexType, "FULLTEXT") {
			createSQL = fmt.Sprintf("CREATE FULLTEXT INDEX `%s` ON `%s`.`%s` (%s)",
				indexName, schema, tableName, columns)
		} else if strings.EqualFold(indexType, "SPATIAL") {
			createSQL = fmt.Sprintf("CREATE SPATIAL INDEX `%s` ON `%s`.`%s` (%s)",
				indexName, schema, tableName, columns)
		} else {
			createSQL = fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s` (%s)",
				indexName, schema, tableName, columns)
		}

		_, err := targetDB.Exec(createSQL)

		if err != nil {

			log.Printf("Warning: failed to create index %s: %v (SQL: %s)", indexName, err, createSQL)

			continue

		}

		log.Printf("Created index %s on table %s.%s", indexName, schema, tableName)

	}

	return nil

}

// ReinitStorage 动态切换存储后端

func (s *TaskService) ReinitStorage(cfg *config.Config) error {

	var newStorage TaskStorage

	if cfg.Storage.Mode == "mysql" {

		storage, err := NewMySQLTaskStorageFromConfig(&cfg.Storage, cfg.Security.EncryptKey)

		if err != nil {

			return err

		}

		newStorage = storage

		log.Println("Storage backend switched to MySQL")

	} else {

		dataDir := cfg.Storage.DataDir

		if dataDir == "" {

			dataDir = "data"

		}

		newStorage = NewFileTaskStorage(dataDir, cfg.Security.EncryptKey)

		log.Println("Storage backend switched to file")

	}

	s.mu.Lock()

	s.storage = newStorage

	s.mu.Unlock()

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
