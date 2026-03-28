package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mysql-to-async/internal/audit"
	"mysql-to-async/internal/checkpoint"
	"mysql-to-async/internal/config"
	"mysql-to-async/internal/metadata/domain/entity"
	"mysql-to-async/internal/metadata/domain/service"
	"mysql-to-async/internal/metadata/infrastructure"
	syncApp "mysql-to-async/internal/sync/application"
	"mysql-to-async/internal/sync/infrastructure/reader"
	"mysql-to-async/internal/sync/infrastructure/readonly"
	"mysql-to-async/internal/sync/infrastructure/writer"
	taskEntity "mysql-to-async/internal/task/domain/entity"

	"github.com/redis/go-redis/v9"
)

// TaskService 任务服务
type TaskService struct {
	mu                sync.RWMutex
	tasks             map[string]*taskEntity.SyncTask
	storage           TaskStorage
	sourceDB          *sql.DB
	targetDB          *sql.DB
	analyzer          service.IdentityAnalyzer
	readOnlyManager   *readonly.ReadOnlyManager
	enableReadOnly    bool                                       // 是否启用只读限制
	checkpointManager checkpoint.Manager                         // 位点管理器
	incrementalSyncs  map[string]*syncApp.IncrementalSyncService // 增量同步服务映射
	config            *config.Config                             // 配置
	auditLogger       *audit.AuditLogger                         // 审计日志器
}

// TaskStorage 任务存储接口
type TaskStorage interface {
	Save(task *taskEntity.SyncTask) error
	Delete(taskID string) error
	LoadAll() ([]*taskEntity.SyncTask, error)
}

// MySQLTaskStorage MySQL 任务存储
type MySQLTaskStorage struct {
	db *sql.DB
	mu sync.RWMutex
}

// NewMySQLTaskStorage 创建 MySQL 任务存储（dsn 不含数据库名，dbName 为目标库名）
func NewMySQLTaskStorage(db *sql.DB) *MySQLTaskStorage {
	s := &MySQLTaskStorage{db: db}
	if err := s.initTable(); err != nil {
		log.Printf("Warning: failed to initialize task storage table: %v", err)
	}
	return s
}

// NewMySQLTaskStorageFromConfig 通过配置创建 MySQL 任务存储，自动建库建表
func NewMySQLTaskStorageFromConfig(cfg *config.StorageConfig) (*MySQLTaskStorage, error) {
	// 先用不带数据库名的 DSN 连接，创建数据库
	noDB := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port)
	tmpDB, err := sql.Open("mysql", noDB)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql: %w", err)
	}
	if _, err = tmpDB.Exec(fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", cfg.Database)); err != nil {
		tmpDB.Close()
		return nil, fmt.Errorf("failed to create database %s: %w", cfg.Database, err)
	}
	tmpDB.Close()

	// 再连接到目标数据库
	dsn := cfg.GetDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage database: %w", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping storage database: %w", err)
	}
	return NewMySQLTaskStorage(db), nil
}

func (s *MySQLTaskStorage) initTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS sys_sync_tasks (
		id VARCHAR(64) PRIMARY KEY,
		name VARCHAR(255),
		content JSON,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`
	_, err := s.db.Exec(query)
	return err
}

// Save 保存任务到数据库
func (s *MySQLTaskStorage) Save(task *taskEntity.SyncTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	query := "INSERT INTO sys_sync_tasks (id, name, content) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), content = VALUES(content)"
	_, err = s.db.Exec(query, task.Config.ID, task.Config.Name, data)
	return err
}

// Delete 从数据库删除任务
func (s *MySQLTaskStorage) Delete(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := "DELETE FROM sys_sync_tasks WHERE id = ?"
	_, err := s.db.Exec(query, taskID)
	return err
}

// LoadAll 从数据库加载所有任务
func (s *MySQLTaskStorage) LoadAll() ([]*taskEntity.SyncTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := "SELECT content FROM sys_sync_tasks"
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*taskEntity.SyncTask
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}
		var task taskEntity.SyncTask
		if err := json.Unmarshal(data, &task); err != nil {
			continue
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

// NewTaskService 创建任务服务（启动时不依赖数据库）
func NewTaskService(cfg *config.Config) *TaskService {
	ts := &TaskService{
		tasks:            make(map[string]*taskEntity.SyncTask),
		incrementalSyncs: make(map[string]*syncApp.IncrementalSyncService),
		config:           cfg,
		auditLogger:      audit.NewAuditLogger("logs/audit"),
	}

	// 初始化存储后端
	if cfg.Storage.Mode == "mysql" {
		storage, err := NewMySQLTaskStorageFromConfig(&cfg.Storage)
		if err != nil {
			log.Printf("Warning: failed to initialize MySQL storage: %v, falling back to file storage", err)
			ts.storage = NewFileTaskStorage("data")
		} else {
			ts.storage = storage
			log.Println("Using MySQL task storage")
		}
	} else {
		ts.storage = NewFileTaskStorage("data")
		log.Println("Using file task storage")
	}

	// 初始化位点管理器
	if cfg != nil && cfg.Redis.Host != "" {
		// 使用Redis位点管理器
		rdb := redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
		})
		ts.checkpointManager = checkpoint.NewRedisCheckpointManager(rdb, "dts:checkpoint")
		log.Println("Using Redis checkpoint manager")
	} else {
		// 使用内存位点管理器
		ts.checkpointManager = checkpoint.NewMemoryCheckpointManager()
		log.Println("Using in-memory checkpoint manager")
	}

	// 加载已保存的任务
	ts.loadTasks()
	return ts
}

// NewTaskServiceWithDB 创建带数据库连接的任务服务
func NewTaskServiceWithDB(sourceDB, targetDB *sql.DB, analyzer service.IdentityAnalyzer) *TaskService {
	ts := &TaskService{
		tasks:            make(map[string]*taskEntity.SyncTask),
		storage:          NewFileTaskStorage("data"),
		sourceDB:         sourceDB,
		targetDB:         targetDB,
		analyzer:         analyzer,
		enableReadOnly:   true, // 默认启用只读限制
		incrementalSyncs: make(map[string]*syncApp.IncrementalSyncService),
		auditLogger:      audit.NewAuditLogger("logs/audit"),
	}
	// 初始化只读管理器
	ts.readOnlyManager = readonly.NewReadOnlyManager(targetDB)
	// 初始化位点管理器（默认使用内存）
	ts.checkpointManager = checkpoint.NewMemoryCheckpointManager()
	ts.loadTasks()
	return ts
}

// NewTaskServiceWithDBAndConfig 创建带数据库连接和配置的任务服务
func NewTaskServiceWithDBAndConfig(sourceDB, targetDB *sql.DB, analyzer service.IdentityAnalyzer, cfg *config.Config) *TaskService {
	ts := &TaskService{
		tasks:            make(map[string]*taskEntity.SyncTask),
		sourceDB:         sourceDB,
		targetDB:         targetDB,
		analyzer:         analyzer,
		enableReadOnly:   true, // 默认启用只读限制
		incrementalSyncs: make(map[string]*syncApp.IncrementalSyncService),
		config:           cfg,
		auditLogger:      audit.NewAuditLogger("logs/audit"),
	}

	// 初始化存储后端
	if cfg.Storage.Mode == "mysql" {
		storage, err := NewMySQLTaskStorageFromConfig(&cfg.Storage)
		if err != nil {
			log.Printf("Warning: failed to initialize MySQL storage: %v, falling back to file storage", err)
			ts.storage = NewFileTaskStorage("data")
		} else {
			ts.storage = storage
		}
	} else {
		ts.storage = NewFileTaskStorage("data")
	}
	// 初始化只读管理器
	ts.readOnlyManager = readonly.NewReadOnlyManager(targetDB)

	// 初始化位点管理器
	if cfg.Redis.Host != "" {
		// 使用Redis位点管理器
		rdb := redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
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

// SetEnableReadOnly 设置是否启用只读限制
func (s *TaskService) SetEnableReadOnly(enable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enableReadOnly = enable
}

// GetEnableReadOnly 获取是否启用只读限制
func (s *TaskService) GetEnableReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enableReadOnly
}

// loadTasks 从存储加载任务
func (s *TaskService) loadTasks() {
	tasks, err := s.storage.LoadAll()
	if err != nil {
		fmt.Printf("加载任务失败: %v\n", err)
		return
	}
	for _, task := range tasks {
		s.tasks[task.Config.ID] = task
	}
}

// CreateTask 创建任务
func (s *TaskService) CreateTask(config taskEntity.TaskConfig) (*taskEntity.SyncTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task := taskEntity.NewSyncTask(config)
	s.tasks[config.ID] = task

	// 保存到存储
	if err := s.storage.Save(task); err != nil {
		fmt.Printf("保存任务失败: %v\n", err)
	}

	return task, nil
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
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	// 检查任务状态，防止重复启动
	if task.Context.Status == taskEntity.TaskStatusRunning {
		return fmt.Errorf("task is already running: %s", taskID)
	}

	// 动态创建数据库连接（如果还没有创建或需要更新）
	if err := s.initDatabaseConnections(task); err != nil {
		return fmt.Errorf("failed to initialize database connections: %w", err)
	}

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
	go s.executeSync(syncCtx, taskID)

	return nil
}

// initDatabaseConnections 初始化数据库连接（每次任务启动都重建，确保连接指向正确的库）
func (s *TaskService) initDatabaseConnections(task *taskEntity.SyncTask) error {
	var err error

	// 每次任务启动都重置连接，防止不同任务复用错误的连接
	if s.sourceDB != nil {
		s.sourceDB.Close()
		s.sourceDB = nil
	}
	if s.targetDB != nil {
		s.targetDB.Close()
		s.targetDB = nil
	}
	s.analyzer = nil

	// 确定源数据库配置
	sourceConfig := task.Config.SourceDB
	if sourceConfig == nil && s.config != nil {
		// 使用配置文件中的默认值
		sourceConfig = &taskEntity.DatabaseConfig{
			Host:     s.config.Datasource.Host,
			Port:     s.config.Datasource.Port,
			Database: task.Config.SourceSchema,
			Username: s.config.Datasource.Username,
			Password: s.config.Datasource.Password,
		}
	}

	if sourceConfig == nil {
		return fmt.Errorf("source database config is required")
	}

	// 确定目标数据库配置
	targetConfig := task.Config.TargetDB
	if targetConfig == nil && s.config != nil && s.config.Target.Host != "" {
		// 使用配置文件中的默认值
		targetConfig = &taskEntity.DatabaseConfig{
			Host:     s.config.Target.Host,
			Port:     s.config.Target.Port,
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
			Host:     sourceConfig.Host,
			Port:     sourceConfig.Port,
			Database: targetSchema,
			Username: sourceConfig.Username,
			Password: sourceConfig.Password,
		}
	}

	// 连接源数据库
	if s.sourceDB == nil {
		sourceDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			sourceConfig.Username,
			sourceConfig.Password,
			sourceConfig.Host,
			sourceConfig.Port,
			sourceConfig.Database,
		)
		s.sourceDB, err = sql.Open("mysql", sourceDSN)
		if err != nil {
			return fmt.Errorf("failed to connect source database: %w", err)
		}

		// 测试连接
		if err = s.sourceDB.Ping(); err != nil {
			s.sourceDB.Close()
			s.sourceDB = nil
			return fmt.Errorf("failed to ping source database: %w", err)
		}

		log.Printf("[Task %s] Source database connected: %s:%d/%s", task.Config.ID, sourceConfig.Host, sourceConfig.Port, sourceConfig.Database)
	}

	// 连接目标数据库
	if s.targetDB == nil {
		// 先连接到MySQL服务器（不指定数据库）以便能够创建数据库
		targetDSNNoDB := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
			targetConfig.Username,
			targetConfig.Password,
			targetConfig.Host,
			targetConfig.Port,
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
		targetDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			targetConfig.Username,
			targetConfig.Password,
			targetConfig.Host,
			targetConfig.Port,
			targetConfig.Database,
		)
		s.targetDB, err = sql.Open("mysql", targetDSN)
		if err != nil {
			return fmt.Errorf("failed to connect target database: %w", err)
		}

		// 测试连接
		if err = s.targetDB.Ping(); err != nil {
			s.targetDB.Close()
			s.targetDB = nil
			return fmt.Errorf("failed to ping target database: %w", err)
		}

		log.Printf("[Task %s] Target database connected: %s:%d/%s", task.Config.ID, targetConfig.Host, targetConfig.Port, targetConfig.Database)
	}

	// 初始化元数据分析器（如果还没有创建）
	if s.analyzer == nil {
		schemaDetector := infrastructure.NewSchemaDetector(s.sourceDB)
		s.analyzer = service.NewIdentityAnalyzerService(schemaDetector)

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
	}

	// 初始化只读管理器（每次都重新创建，确保使用新的数据库连接）
	s.readOnlyManager = readonly.NewReadOnlyManager(s.targetDB)
	s.enableReadOnly = true

	return nil
}

// executeSync 执行同步任务
func (s *TaskService) executeSync(ctx context.Context, taskID string) {
	s.mu.RLock()
	task, exists := s.tasks[taskID]
	s.mu.RUnlock()

	if !exists {
		return
	}

	enableReadOnly := task.Config.EnableReadOnly

	log.Printf("[Task %s] Starting sync, mode: %s, tables: %v", taskID, task.Config.Mode, task.Config.Tables)

	// 在同步开始前，若任务开启了只读管理，临时关闭目标库只读以允许数据写入
	if enableReadOnly && s.readOnlyManager != nil {
		log.Printf("[Task %s] 正在临时关闭目标实例只读以进行同步...", taskID)
		if err := s.readOnlyManager.SetReadOnly(); err != nil {
			log.Printf("[Task %s] 警告: 关闭只读失败: %v", taskID, err)
			// 记录错误但继续执行同步
		} else {
			log.Printf("[Task %s] 目标实例只读已临时关闭，同步结束后自动恢复", taskID)
		}
	}

	// 确保在函数退出时恢复只读状态
	defer func() {
		if enableReadOnly && s.readOnlyManager != nil {
			log.Printf("[Task %s] 正在恢复目标实例只读状态...", taskID)
			if err := s.readOnlyManager.RestoreReadOnly(); err != nil {
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
		s.executeFullSync(ctx, task)
	case "INCREMENTAL":
		s.executeIncrementalSync(ctx, task)
	case "ALL":
		// 先全量后增量
		if err := s.executeFullSync(ctx, task); err == nil {
			s.executeIncrementalSync(ctx, task)
		}
	default:
		s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, "unknown sync mode: "+string(task.Config.Mode))
	}
}

// executeFullSync 执行全量同步（支持多库）
func (s *TaskService) executeFullSync(ctx context.Context, task *taskEntity.SyncTask) error {
	taskID := task.Config.ID

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

	// 计算所有库的总行数
	var totalRows int64
	for _, p := range pairs {
		tables := task.Config.Tables
		if len(tables) == 0 {
			allTables, err := s.analyzer.GetAllTables(p.src)
			if err != nil {
				log.Printf("[Task %s] Failed to get tables for %s: %v", taskID, p.src, err)
				continue
			}
			for _, t := range allTables {
				tables = append(tables, t.TableName)
			}
		}
		for _, tableName := range tables {
			identity, err := s.analyzer.AnalyzeTable(p.src, tableName)
			if err != nil {
				continue
			}
			r := reader.NewReader(s.sourceDB, p.src, tableName, identity)
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
		if err := s.syncDatabasePair(ctx, task, p.src, p.dst); err != nil {
			return err
		}
	}

	s.completeTask(taskID)
	log.Printf("[Task %s] Full sync completed, total rows: %d", taskID, totalRows)
	return nil
}

// syncDatabasePair 同步单个源库到目标库（含全部或指定表）
func (s *TaskService) syncDatabasePair(ctx context.Context, task *taskEntity.SyncTask, sourceSchema, targetSchema string) error {
	taskID := task.Config.ID

	// 确定要同步的表
	tables := task.Config.Tables
	if len(tables) == 0 {
		log.Printf("[Task %s] 库级别同步：正在获取数据库 %s 的所有表...", taskID, sourceSchema)
		allTables, err := s.analyzer.GetAllTables(sourceSchema)
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
		name     string
		identity *entity.TableIdentity
	}
	log.Printf("[Task %s] 阶段1: 同步 %d 个表结构...", taskID, len(tables))
	ready := make([]tableReady, 0, len(tables))
	for _, tableName := range tables {
		if s.isTaskStopped(taskID) {
			return fmt.Errorf("task stopped")
		}
		log.Printf("[Task %s] 确保目标表: %s.%s -> %s.%s", taskID, sourceSchema, tableName, targetSchema, tableName)
		identity, err := s.analyzer.AnalyzeTable(sourceSchema, tableName)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to analyze table %s: %v", tableName, err)
			s.updateTaskStatus(taskID, taskEntity.TaskStatusFailed, errMsg)
			return fmt.Errorf("%s", errMsg)
		}
		if err := s.ensureTargetTable(sourceSchema, targetSchema, tableName, identity); err != nil {
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
			sem <- struct{}{}
			defer func() { <-sem }()

			if s.isTaskStopped(taskID) {
				return
			}

			log.Printf("[Task %s] Syncing table data: %s.%s -> %s.%s", taskID, sourceSchema, tableName, targetSchema, tableName)

			var savedIndexes []map[string]interface{}
			if task.Config.OptimizeIndex {
				log.Printf("[Task %s] Dropping non-primary indexes for table %s...", taskID, tableName)
				indexes, err := s.dropNonPrimaryKeyIndexes(targetSchema, tableName)
				if err != nil {
					log.Printf("[Task %s] Warning: Failed to drop indexes for %s: %v", taskID, tableName, err)
				} else {
					savedIndexes = indexes
					log.Printf("[Task %s] Dropped %d indexes from %s", taskID, len(savedIndexes), tableName)
				}
			}

			var tableProcessedRows int64
			batchSize := int64(task.Config.BatchSize)
			const txCommitEveryN = 20 // 每 20 批 commit 一次，平衡事务大小与 commit 开销

			// 单表内部并发 worker 数：独立于表级并发数，避免单表开 N*N 个连接
			const maxIntraTableWorkers = 8
			intraWorkers := workerCount
			if intraWorkers > maxIntraTableWorkers {
				intraWorkers = maxIntraTableWorkers
			}

			// === 策略检测 ===
			canParallelRange := identity.Strategy != entity.FullColumnsStrategy &&
				len(identity.IdentifyCols) == 1 &&
				intraWorkers > 1 &&
				isNumericPKColumn(identity, identity.IdentifyCols[0])

			canParallelSample := !canParallelRange &&
				identity.Strategy != entity.FullColumnsStrategy &&
				len(identity.IdentifyCols) == 1 &&
				intraWorkers > 1

			var minPK, maxPK int64
			if canParallelRange {
				if err := s.sourceDB.QueryRowContext(ctx,
					fmt.Sprintf("SELECT COALESCE(MIN(`%s`), 0), COALESCE(MAX(`%s`), -1) FROM `%s`.`%s`",
						identity.IdentifyCols[0], identity.IdentifyCols[0], sourceSchema, tableName),
				).Scan(&minPK, &maxPK); err != nil || maxPK < minPK {
					canParallelRange = false
					canParallelSample = identity.Strategy != entity.FullColumnsStrategy &&
						len(identity.IdentifyCols) == 1 && intraWorkers > 1
					log.Printf("[Task %s] Cannot get numeric PK range for %s.%s, trying sample parallel", taskID, sourceSchema, tableName)
				}
			}

			var sampleBoundaries []interface{}
			if canParallelSample {
				var totalRows int64
				if qErr := s.sourceDB.QueryRowContext(ctx,
					fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", sourceSchema, tableName),
				).Scan(&totalRows); qErr != nil || totalRows < int64(intraWorkers)*2 {
					canParallelSample = false
					log.Printf("[Task %s] Skipping sample parallel for %s.%s", taskID, sourceSchema, tableName)
				} else {
					var bErr error
					sampleBoundaries, bErr = s.samplePKBoundaries(ctx, sourceSchema, tableName, identity.IdentifyCols[0], totalRows, intraWorkers)
					if bErr != nil {
						canParallelSample = false
						log.Printf("[Task %s] Boundary sampling failed for %s.%s: %v", taskID, sourceSchema, tableName, bErr)
					}
				}
			}

			// === 执行同步 ===
			if identity.Strategy == entity.FullColumnsStrategy {
				// 无主键表：单协程流式读取 + 事务批量提交
				conn, err := s.targetDB.Conn(ctx)
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

				dr := reader.NewReader(s.sourceDB, sourceSchema, tableName, identity)
				for {
					if s.isTaskStopped(taskID) {
						return
					}
					rows, err := dr.ReadBatch(ctx, 0, batchSize)
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
				// 数字单列主键：表内并行范围读写 + 每 worker 独立事务批量提交
				log.Printf("[Task %s] Table %s.%s: parallel range sync PK=[%d,%d] workers=%d",
					taskID, sourceSchema, tableName, minPK, maxPK, intraWorkers)
				rangeSize := (maxPK - minPK + int64(intraWorkers)) / int64(intraWorkers)
				var syncWg sync.WaitGroup
				syncErrChan := make(chan error, intraWorkers)
				var atomicProcessed int64

				for w := 0; w < intraWorkers; w++ {
					syncWg.Add(1)
					go func(wIdx int) {
						defer syncWg.Done()
						rangeStart := minPK + int64(wIdx)*rangeSize
						rangeEnd := rangeStart + rangeSize
						if wIdx == intraWorkers-1 {
							rangeEnd = maxPK + 1
						}
						conn, err := s.targetDB.Conn(ctx)
						if err != nil {
							syncErrChan <- fmt.Errorf("w%d conn: %v", wIdx, err)
							return
						}
						conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0")
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
								return fmt.Errorf("w%d write at %s (tx from %s) rolled back: %v", wIdx, mark, txStartMark, e)
							}
							txBatchN++
							if txBatchN >= txCommitEveryN {
								if e := curTx.Commit(); e != nil {
									curTx = nil
									return fmt.Errorf("w%d commit at %s (tx from %s): %v", wIdx, mark, txStartMark, e)
								}
								curTx = nil
							}
							return nil
						}

						// 每 worker 用独立源库连接执行一次流式查询，避免反复发小范围查询引起的源库 I/O 抖动
						srcConn, err := s.sourceDB.Conn(ctx)
						if err != nil {
							syncErrChan <- fmt.Errorf("w%d source conn: %v", wIdx, err)
							return
						}
						defer srcConn.Close()

						wReader := reader.NewRangeShardingReader(s.sourceDB, sourceSchema, tableName, identity)
						sqlRows, cols, err := wReader.OpenRangeStream(srcConn, ctx, rangeStart, rangeEnd)
						if err != nil {
							syncErrChan <- fmt.Errorf("w%d stream [%d,%d): %v", wIdx, rangeStart, rangeEnd, err)
							return
						}
						defer sqlRows.Close()

						values := make([]interface{}, len(cols))
						valuePtrs := make([]interface{}, len(cols))
						for i := range values {
							valuePtrs[i] = &values[i]
						}
						batch := make([]map[string]interface{}, 0, batchSize)
						rangeMark := fmt.Sprintf("%s.%s:w%d:[%d,%d)", sourceSchema, tableName, wIdx, rangeStart, rangeEnd)

						for {
							if s.isTaskStopped(taskID) {
								return
							}
							if !sqlRows.Next() {
								break
							}
							if err := sqlRows.Scan(valuePtrs...); err != nil {
								syncErrChan <- fmt.Errorf("w%d scan: %v", wIdx, err)
								return
							}
							row := make(map[string]interface{}, len(cols))
							for i, col := range cols {
								if b, ok := values[i].([]byte); ok {
									row[col] = string(b)
								} else {
									row[col] = values[i]
								}
							}
							batch = append(batch, row)
							if int64(len(batch)) >= batchSize {
								if err := doWrite(batch, rangeMark); err != nil {
									syncErrChan <- err
									return
								}
								n := int64(len(batch))
								atomic.AddInt64(&atomicProcessed, n)
								s.incrementTaskProgress(taskID, n, rangeMark)
								batch = batch[:0]
							}
						}
						if err := sqlRows.Err(); err != nil {
							syncErrChan <- fmt.Errorf("w%d stream error: %v", wIdx, err)
							return
						}
						if len(batch) > 0 {
							if err := doWrite(batch, rangeMark); err != nil {
								syncErrChan <- err
								return
							}
							n := int64(len(batch))
							atomic.AddInt64(&atomicProcessed, n)
							s.incrementTaskProgress(taskID, n, rangeMark)
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
				// 字符串单列主键：采样边界 + 表内并行 keyset + 每 worker 独立事务批量提交
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
						// sampleBoundaries[i] = 第 i+1 个 worker 的起始 key（前一 worker 的最后 key，exclusive）
						var startBoundary, endBoundary interface{}
						if wIdx > 0 {
							startBoundary = sampleBoundaries[wIdx-1]
						}
						if wIdx < intraWorkers-1 {
							endBoundary = sampleBoundaries[wIdx]
						}

						conn, err := s.targetDB.Conn(ctx)
						if err != nil {
							syncErrChan <- fmt.Errorf("w%d conn: %v", wIdx, err)
							return
						}
						conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0")
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
								return fmt.Errorf("w%d write at %s (tx from %s) rolled back: %v", wIdx, mark, txStartMark, e)
							}
							txBatchN++
							if txBatchN >= txCommitEveryN {
								if e := curTx.Commit(); e != nil {
									curTx = nil
									return fmt.Errorf("w%d commit at %s (tx from %s): %v", wIdx, mark, txStartMark, e)
								}
								curTx = nil
							}
							return nil
						}

						wReader := reader.NewReader(s.sourceDB, sourceSchema, tableName, identity)
						lastID := startBoundary
						for {
							if s.isTaskStopped(taskID) {
								return
							}
							batchRows, err := wReader.ReadBatchByKeys(ctx, lastID, batchSize)
							if err != nil {
								syncErrChan <- fmt.Errorf("w%d read after %v: %v", wIdx, lastID, err)
								return
							}
							if len(batchRows) == 0 {
								break
							}
							// 非最后一个 worker：裁剪超出 endBoundary 的行
							if endBoundary != nil {
								endStr := fmt.Sprintf("%v", endBoundary)
								cutIdx := len(batchRows)
								for j, row := range batchRows {
									if fmt.Sprintf("%v", row[pkCol]) > endStr {
										cutIdx = j
										break
									}
								}
								batchRows = batchRows[:cutIdx]
							}
							if len(batchRows) == 0 {
								break
							}
							lastRowPK := batchRows[len(batchRows)-1][pkCol]
							mark := fmt.Sprintf("%s.%s:w%d:pk=%v", sourceSchema, tableName, wIdx, lastRowPK)
							if err := doWrite(batchRows, mark); err != nil {
								syncErrChan <- err
								return
							}
							n := int64(len(batchRows))
							atomic.AddInt64(&atomicProcessed, n)
							s.incrementTaskProgress(taskID, n, mark)
							lastID = lastRowPK
							if endBoundary != nil && fmt.Sprintf("%v", lastRowPK) >= fmt.Sprintf("%v", endBoundary) {
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
				// 复合主键/回退：Keyset Pagination 顺序读取 + 事务批量提交
				conn, err := s.targetDB.Conn(ctx)
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

				dr := reader.NewReader(s.sourceDB, sourceSchema, tableName, identity)
				var lastID interface{}
				for {
					if s.isTaskStopped(taskID) {
						return
					}
					rows, err := dr.ReadBatchByKeys(ctx, lastID, batchSize)
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
				if err := s.restoreIndexes(targetSchema, tableName, savedIndexes); err != nil {
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
func (s *TaskService) executeIncrementalSync(ctx context.Context, task *taskEntity.SyncTask) {
	taskID := task.Config.ID

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
		TaskID:          taskID,
		SourceHost:      sourceHost,
		SourcePort:      sourcePort,
		SourceUsername:  sourceUsername,
		SourcePassword:  sourcePassword,
		SourceSchema:    sourceSchema,
		TargetSchema:    targetSchema,
		SourceDatabases: task.Config.SourceDatabases,
		TargetDatabases: task.Config.TargetDatabases,
		Tables:          task.Config.Tables,
		BatchSize:       task.Config.BatchSize,
		ServerID:        generateServerID(taskID),
	}

	// 创建增量同步服务
	incrSync := syncApp.NewIncrementalSyncService(
		s.sourceDB,
		s.targetDB,
		s.analyzer,
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
func (s *TaskService) withDDL(fn func() error) error {
	if s.readOnlyManager != nil {
		return s.readOnlyManager.WithWriteAccess(fn)
	}
	return fn()
}

// ensureTargetTable 确保目标表存在
func (s *TaskService) ensureTargetTable(sourceSchema, targetSchema, tableName string, identity *entity.TableIdentity) error {
	// 首先确保目标数据库存在：先用 SELECT 查询（只读操作），避免在 read_only 目标库上无谓执行 DDL
	var dbExists string
	err := s.targetDB.QueryRow(
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?", targetSchema,
	).Scan(&dbExists)
	if err == sql.ErrNoRows {
		// 数据库不存在，临时解除只读后创建
		if err = s.withDDL(func() error {
			_, e := s.targetDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", targetSchema))
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
	err = s.targetDB.QueryRow(
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
	if s.sourceDB != nil {
		tryErr := s.withDDL(func() error {
			_, e := s.sourceDB.Exec(fmt.Sprintf("CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`",
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
	err = s.sourceDB.QueryRow(
		fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", sourceSchema, tableName),
	).Scan(&tableName, &createSQL)
	if err != nil {
		return fmt.Errorf("failed to get CREATE TABLE statement for %s.%s: %v", sourceSchema, tableName, err)
	}

	createSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` %s", targetSchema, tableName,
		extractTableDefinition(createSQL))

	if err = s.withDDL(func() error {
		_, e := s.targetDB.Exec(createSQL)
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

// samplePKBoundaries 并行采样 N-1 个边界值，将 PK 空间分成 n 段供 n 个 worker 使用
// boundaries[i] 是第 i+1 个 worker 的起始 key（exclusive），前一 worker 的最后 key（inclusive）
func (s *TaskService) samplePKBoundaries(ctx context.Context, schema, table, pkCol string, totalRows int64, n int) ([]interface{}, error) {
	boundaries := make([]interface{}, n-1)
	errs := make([]error, n-1)
	var wg sync.WaitGroup

	for i := 1; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			offset := totalRows*int64(idx)/int64(n) - 1
			if offset < 0 {
				offset = 0
			}
			var pk interface{}
			err := s.sourceDB.QueryRowContext(ctx,
				fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` ORDER BY `%s` LIMIT 1 OFFSET ?",
					pkCol, schema, table, pkCol),
				offset,
			).Scan(&pk)
			if err != nil {
				errs[idx-1] = fmt.Errorf("boundary %d (offset %d): %v", idx, offset, err)
				return
			}
			if b, ok := pk.([]byte); ok {
				pk = string(b)
			}
			boundaries[idx-1] = pk
		}(i)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	return boundaries, nil
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
		"processed_rows":   task.Context.ProcessedRows,
		"total_rows":       task.Context.TotalRows,
		"progress_percent": task.Context.ProgressPercent,
		"tables_completed": 0,
		"tables_total":     len(task.Config.Tables),
		"status":           task.Context.Status,
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
	mu      sync.RWMutex
}

// NewFileTaskStorage 创建文件任务存储
func NewFileTaskStorage(dataDir string) *FileTaskStorage {
	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Printf("Warning: failed to create data directory: %v", err)
	}
	return &FileTaskStorage{dataDir: dataDir}
}

// Save 保存任务到JSON文件
func (s *FileTaskStorage) Save(task *taskEntity.SyncTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 确保目录存在
	if err := os.MkdirAll(s.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
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
			log.Printf("Warning: failed to read task file %s: %v", file.Name(), err)
			continue
		}

		var task taskEntity.SyncTask
		if err := json.Unmarshal(data, &task); err != nil {
			log.Printf("Warning: failed to unmarshal task file %s: %v", file.Name(), err)
			continue
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

	// 3. 关闭数据库连接
	if s.sourceDB != nil {
		if err := s.sourceDB.Close(); err != nil {
			log.Printf("Error closing source database: %v", err)
		}
	}
	if s.targetDB != nil && s.targetDB != s.sourceDB {
		if err := s.targetDB.Close(); err != nil {
			log.Printf("Error closing target database: %v", err)
		}
	}

	// 4. 关闭审计日志器
	if s.auditLogger != nil {
		if err := s.auditLogger.Close(); err != nil {
			log.Printf("Failed to close audit logger: %v", err)
		}
	}

	log.Println("Task service closed successfully")
	return nil
}

// dropNonPrimaryKeyIndexes 删除非主键索引
func (s *TaskService) dropNonPrimaryKeyIndexes(schema, tableName string) ([]map[string]interface{}, error) {
	var savedIndexes []map[string]interface{}

	// 查询所有索引
	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", schema, tableName)
	rows, err := s.targetDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to show indexes: %v", err)
	}
	defer rows.Close()

	// 记录需要删除的索引（非主键索引）
	indexesToDrop := make(map[string]bool)
	for rows.Next() {
		var indexName, columnName string
		var nonUnique int
		var seqInIndex, collation, cardinality, subPart, packed, null int
		var indexType, comment string
		var indexComment sql.NullString

		err := rows.Scan(&indexName, &columnName, &nonUnique, &seqInIndex, &collation,
			&cardinality, &subPart, &packed, &null, &indexType, &comment, &indexComment)
		if err != nil {
			log.Printf("Warning: failed to scan index: %v", err)
			continue
		}

		// 跳过主键索引
		if indexName == "PRIMARY" {
			continue
		}

		// 记录索引信息（避免重复）
		if _, exists := indexesToDrop[indexName]; !exists {
			indexesToDrop[indexName] = true
			savedIndexes = append(savedIndexes, map[string]interface{}{
				"name":       indexName,
				"column":     columnName,
				"non_unique": nonUnique,
				"type":       indexType,
			})
		}
	}

	// 删除非主键索引
	for indexName := range indexesToDrop {
		dropQuery := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`", schema, tableName, indexName)
		_, err := s.targetDB.Exec(dropQuery)
		if err != nil {
			log.Printf("Warning: failed to drop index %s: %v", indexName, err)
			continue
		}
		log.Printf("Dropped index %s from table %s.%s", indexName, schema, tableName)
	}

	return savedIndexes, nil
}

// restoreIndexes 恢复索引
func (s *TaskService) restoreIndexes(schema, tableName string, indexes []map[string]interface{}) error {
	if len(indexes) == 0 {
		return nil
	}

	log.Printf("[Task] Restoring %d indexes for table %s.%s...", len(indexes), schema, tableName)

	// 为每个索引重新创建
	for _, indexInfo := range indexes {
		indexName, ok := indexInfo["name"].(string)
		if !ok {
			continue
		}

		// 获取该索引的所有列
		var columns []string
		for _, idx := range indexes {
			if idx["name"] == indexName {
				if col, ok := idx["column"].(string); ok {
					columns = append(columns, fmt.Sprintf("`%s`", col))
				}
			}
		}

		if len(columns) == 0 {
			continue
		}

		// 构建索引名（确保唯一）
		uniqueIndexName := indexName
		for _, idx := range indexes {
			if idx["name"] == indexName {
				if _, exists := idx["used"]; !exists {
					idx["used"] = true
					break
				}
				uniqueIndexName = fmt.Sprintf("%s_%d", indexName, time.Now().UnixNano())
			}
		}

		// 判断索引类型
		nonUnique, ok := indexInfo["non_unique"].(int)
		if !ok {
			nonUnique = 1
		}

		// 构建CREATE INDEX语句
		var createSQL string
		if nonUnique == 0 {
			createSQL = fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s)",
				uniqueIndexName, schema, tableName, strings.Join(columns, ", "))
		} else {
			createSQL = fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s` (%s)",
				uniqueIndexName, schema, tableName, strings.Join(columns, ", "))
		}

		_, err := s.targetDB.Exec(createSQL)
		if err != nil {
			log.Printf("Warning: failed to create index %s: %v", uniqueIndexName, err)
			continue
		}
		log.Printf("Created index %s on table %s.%s", uniqueIndexName, schema, tableName)
	}

	return nil
}

// ReinitStorage 动态切换存储后端
func (s *TaskService) ReinitStorage(cfg *config.Config) error {
	var newStorage TaskStorage
	if cfg.Storage.Mode == "mysql" {
		storage, err := NewMySQLTaskStorageFromConfig(&cfg.Storage)
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
		newStorage = NewFileTaskStorage(dataDir)
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
