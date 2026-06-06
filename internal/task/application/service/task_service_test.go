package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"mysql-to-async/internal/audit"
	"mysql-to-async/internal/checkpoint"
	"mysql-to-async/internal/config"
	"mysql-to-async/internal/metadata/domain/entity"
	taskEntity "mysql-to-async/internal/task/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAnalyzer 是一个模拟的 IdentityAnalyzer
type mockAnalyzer struct{}

type failingTaskStorage struct {
	err error
}

func (s *failingTaskStorage) Save(task *taskEntity.SyncTask) error {
	return s.err
}

func (s *failingTaskStorage) Delete(taskID string) error {
	return nil
}

func (s *failingTaskStorage) LoadAll() ([]*taskEntity.SyncTask, error) {
	return nil, nil
}

func (m *mockAnalyzer) AnalyzeTable(schema, tableName string) (*entity.TableIdentity, error) {
	return &entity.TableIdentity{
		TableName:    tableName,
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
	}, nil
}

func (m *mockAnalyzer) GetAllTables(schema string) ([]entity.TableInfo, error) {
	return []entity.TableInfo{
		{Schema: schema, TableName: "users"},
		{Schema: schema, TableName: "orders"},
	}, nil
}

func (m *mockAnalyzer) GetAllDatabases() ([]string, error) {
	return []string{"test", "test_target"}, nil
}

func TestStripNonPrimaryIndexesFromCreateSQL(t *testing.T) {
	createSQL := "CREATE TABLE `users` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `email` varchar(255) NOT NULL,\n" +
		"  `name` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  UNIQUE KEY `uk_email` (`email`),\n" +
		"  KEY `idx_name` (`name`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	stripped := stripNonPrimaryIndexesFromCreateSQL(createSQL)

	assert.Contains(t, stripped, "PRIMARY KEY (`id`)")
	assert.NotContains(t, stripped, "UNIQUE KEY `uk_email`")
	assert.NotContains(t, stripped, "KEY `idx_name`")
	assert.False(t, strings.Contains(stripped, ",\n) ENGINE"))
}

func TestStripNonPrimaryIndexesFromCreateSQL_KeepPrimaryOnlyDDLValid(t *testing.T) {
	createSQL := "CREATE TABLE `orders` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `tenant_id` bigint NOT NULL,\n" +
		"  PRIMARY KEY (`id`,`tenant_id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	stripped := stripNonPrimaryIndexesFromCreateSQL(createSQL)

	assert.Equal(t, createSQL, stripped)
	assert.Contains(t, stripped, "PRIMARY KEY (`id`,`tenant_id`)")
}

func TestStripNonPrimaryIndexesFromCreateSQL_KeepsAutoIncrementKey(t *testing.T) {
	createSQL := "CREATE TABLE `repair` (\n" +
		"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
		"  `dealer_id` bigint DEFAULT NULL,\n" +
		"  `name` varchar(255) DEFAULT NULL,\n" +
		"  UNIQUE KEY `uk_id` (`id`),\n" +
		"  KEY `idx_dealer_id` (`dealer_id`),\n" +
		"  KEY `idx_name` (`name`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	stripped := stripNonPrimaryIndexesFromCreateSQL(createSQL)

	assert.Contains(t, stripped, "`id` bigint NOT NULL AUTO_INCREMENT")
	assert.Contains(t, stripped, "UNIQUE KEY `uk_id` (`id`)")
	assert.NotContains(t, stripped, "KEY `idx_dealer_id` (`dealer_id`)")
	assert.NotContains(t, stripped, "KEY `idx_name` (`name`)")
	assert.False(t, strings.Contains(stripped, ",\n) ENGINE"))
}

// newTestTaskService 创建一个使用自定义数据目录的测试任务服务
func newTestTaskService(dataDir string) *TaskService {
	storage := NewFileTaskStorage(dataDir)
	return &TaskService{
		tasks:    make(map[string]*taskEntity.SyncTask),
		runtimes: make(map[string]*taskRuntime),
		storage:  storage,
	}
}

func newScheduledTestTaskService(dataDir string) *TaskService {
	ts := newTestTaskService(dataDir)
	ts.initRuntimeFn = func(task *taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(ctx context.Context, taskID string, runtime *taskRuntime) {}
	return ts
}

func newDefaultConfig() *config.Config {
	return &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: "data"},
	}
}

func TestResolveSourceSchema(t *testing.T) {
	t.Run("prefers task source db database", func(t *testing.T) {
		ts := NewTaskService(&config.Config{
			Storage:    config.StorageConfig{Mode: "file", DataDir: t.TempDir()},
			Datasource: config.DatasourceConfig{Database: "config_db"},
		})
		defer ts.Close()

		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
			ID:           "task-source-db",
			SourceSchema: "task_schema",
			SourceDB: &taskEntity.DatabaseConfig{
				Database: "source_db_override",
			},
		})

		assert.Equal(t, "source_db_override", ts.resolveSourceSchema(task))
	})

	t.Run("falls back to task source schema", func(t *testing.T) {
		ts := NewTaskService(&config.Config{
			Storage:    config.StorageConfig{Mode: "file", DataDir: t.TempDir()},
			Datasource: config.DatasourceConfig{Database: "config_db"},
		})
		defer ts.Close()

		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
			ID:           "task-schema",
			SourceSchema: "task_schema",
		})

		assert.Equal(t, "task_schema", ts.resolveSourceSchema(task))
	})

	t.Run("falls back to config datasource database", func(t *testing.T) {
		ts := NewTaskService(&config.Config{
			Storage:    config.StorageConfig{Mode: "file", DataDir: t.TempDir()},
			Datasource: config.DatasourceConfig{Database: "config_db"},
		})
		defer ts.Close()

		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "config-fallback"})

		assert.Equal(t, "config_db", ts.resolveSourceSchema(task))
	})
}

func TestResolveTargetSchema(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "rename-target"})
	assert.Equal(t, "target_db", ts.resolveTargetSchema(task, "target_db"))

	task.Config.TargetSchema = "custom_target"
	assert.Equal(t, "custom_target", ts.resolveTargetSchema(task, "target_db"))

	task.Config.TargetSchema = ""
	task.Config.TargetDB = &taskEntity.DatabaseConfig{Database: "target_override"}
	assert.Equal(t, "target_override", ts.resolveTargetSchema(task, "source_db"))
}

func TestResolveTableTargetName(t *testing.T) {
	ts := NewTaskService(&config.Config{Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()}})
	defer ts.Close()

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "rename-table", Tables: []string{"users", "orders"}, TargetTables: []string{"users_bak", "orders_bak"}})
	assert.Equal(t, "users_bak", ts.resolveTableTargetName(task, "users", 0))
	assert.Equal(t, "orders_bak", ts.resolveTableTargetName(task, "orders", 1))
	assert.Equal(t, "fallback", ts.resolveTableTargetName(task, "fallback", 5))

	task.Config.TargetTables = nil
	assert.Equal(t, "users", ts.resolveTableTargetName(task, "users", 0))
}

func TestNewTaskService(t *testing.T) {
	dataDir := "./test_task_service"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)
	assert.NotNil(t, ts)
	assert.NotNil(t, ts.tasks)
	assert.NotNil(t, ts.storage)
}

func TestNewTaskServiceWithDB(t *testing.T) {
	dataDir := "./test_task_service_db"
	defer os.RemoveAll(dataDir)

	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	analyzer := &mockAnalyzer{}

	ts := NewTaskServiceWithDB(sourceDB, targetDB, analyzer)
	assert.NotNil(t, ts)
	assert.NotNil(t, ts.sourceDB)
	assert.NotNil(t, ts.targetDB)
	assert.NotNil(t, ts.analyzer)
	assert.NotNil(t, ts.readOnlyManager)
	assert.NotNil(t, ts.checkpointManager)
	assert.True(t, ts.enableReadOnly)
}

func TestNewTaskServiceWithDBAndConfig(t *testing.T) {
	dataDir := "./test_task_service_config"
	defer os.RemoveAll(dataDir)

	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	analyzer := &mockAnalyzer{}

	cfg := &config.Config{
		Redis: config.RedisConfig{
			Host:     "",
			Port:     0,
			Password: "",
			DB:       0,
		},
	}

	ts := NewTaskServiceWithDBAndConfig(sourceDB, targetDB, analyzer, cfg)
	assert.NotNil(t, ts)
	assert.NotNil(t, ts.sourceDB)
	assert.NotNil(t, ts.targetDB)
	assert.NotNil(t, ts.analyzer)
	assert.NotNil(t, ts.readOnlyManager)
	assert.NotNil(t, ts.checkpointManager)
	assert.NotNil(t, ts.config)
	assert.True(t, ts.enableReadOnly)
}

func TestSetEnableReadOnly(t *testing.T) {
	dataDir := "./test_task_service_readonly"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	// 测试设置为 false
	ts.SetEnableReadOnly(false)
	assert.False(t, ts.GetEnableReadOnly())

	// 测试设置为 true
	ts.SetEnableReadOnly(true)
	assert.True(t, ts.GetEnableReadOnly())
}

func TestReinitStorage_FileMode(t *testing.T) {
	dataDir := "./test_task_service_reinit_storage_old"
	newDataDir := "./test_task_service_reinit_storage_new"
	defer os.RemoveAll(dataDir)
	defer os.RemoveAll(newDataDir)

	cfg := &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: dataDir},
	}

	ts := NewTaskService(cfg)
	require.NotNil(t, ts)

	fileStorage, ok := ts.storage.(*FileTaskStorage)
	require.True(t, ok)
	assert.Equal(t, dataDir, fileStorage.dataDir)

	newCfg := &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: newDataDir},
	}

	err := ts.ReinitStorage(newCfg)
	require.NoError(t, err)

	fileStorage, ok = ts.storage.(*FileTaskStorage)
	require.True(t, ok)
	assert.Equal(t, newDataDir, fileStorage.dataDir)
	assert.Equal(t, newCfg, ts.config)
	assert.DirExists(t, newDataDir)
	assert.NoError(t, ts.Close())
}

func TestReinitCheckpointManager_MemoryMode(t *testing.T) {
	dataDir := "./test_task_service_reinit_checkpoint"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(&config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: dataDir},
		Redis:   config.RedisConfig{},
	})
	require.NotNil(t, ts)

	_, ok := ts.checkpointManager.(*checkpoint.MemoryCheckpointManager)
	require.True(t, ok)

	newCfg := &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: dataDir},
		Redis:   config.RedisConfig{},
	}

	err := ts.ReinitCheckpointManager(newCfg)
	require.NoError(t, err)

	_, ok = ts.checkpointManager.(*checkpoint.MemoryCheckpointManager)
	require.True(t, ok)
	assert.Equal(t, newCfg, ts.config)
	assert.NoError(t, ts.Close())
}

func TestCreateTask(t *testing.T) {
	dataDir := "./test_task_service_create"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	taskConfig := taskEntity.TaskConfig{
		ID:                       "test_task_1",
		Name:                     "Test Task",
		SourceSchema:             "source_db",
		TargetSchema:             "target_db",
		Tables:                   []string{"users", "orders"},
		Mode:                     taskEntity.SyncModeFull,
		EnableDropTableBeforeDDL: true,
	}

	task, err := ts.CreateTask(taskConfig)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, "test_task_1", task.Config.ID)
	assert.True(t, task.Config.EnableDropTableBeforeDDL)

	// 验证任务已添加到服务中
	retrievedTask, exists := ts.GetTask("test_task_1")
	assert.True(t, exists)
	assert.Equal(t, task.Config.ID, retrievedTask.Config.ID)
	assert.True(t, retrievedTask.Config.EnableDropTableBeforeDDL)
}

func TestGetTask(t *testing.T) {
	dataDir := "./test_task_service_get_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 测试获取不存在的任务
	task, exists := ts.GetTask("non_existent")
	assert.False(t, exists)
	assert.Nil(t, task)

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_2",
		Name: "Test Task",
	}
	ts.CreateTask(taskConfig)

	// 测试获取存在的任务
	task, exists = ts.GetTask("test_task_2")
	assert.True(t, exists)
	assert.NotNil(t, task)
	assert.Equal(t, "test_task_2", task.Config.ID)
}

func TestGetAllTasks(t *testing.T) {
	dataDir := "./test_task_service_getall_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 初始应该为空
	tasks := ts.GetAllTasks()
	assert.Empty(t, tasks)

	// 创建多个任务
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_1_unique", Name: "Task 1"})
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_2_unique", Name: "Task 2"})
	ts.CreateTask(taskEntity.TaskConfig{ID: "task_3_unique", Name: "Task 3"})

	// 获取所有任务
	tasks = ts.GetAllTasks()
	assert.Len(t, tasks, 3)

	taskIDs := make(map[string]bool)
	for _, task := range tasks {
		taskIDs[task.Config.ID] = true
	}

	assert.True(t, taskIDs["task_1_unique"])
	assert.True(t, taskIDs["task_2_unique"])
	assert.True(t, taskIDs["task_3_unique"])
}

func TestUpdateTask(t *testing.T) {
	dataDir := "./test_task_service_update_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_update",
		Name: "Original Name",
	}
	task, _ := ts.CreateTask(taskConfig)

	// 修改任务
	task.Config.Name = "Updated Name"
	task.Start()

	// 更新任务
	err := ts.UpdateTask(task)
	assert.NoError(t, err)

	// 验证更新
	retrievedTask, _ := ts.GetTask("test_task_update")
	assert.Equal(t, "Updated Name", retrievedTask.Config.Name)
	assert.Equal(t, taskEntity.TaskStatusRunning, retrievedTask.Context.Status)
}

func TestUpdateTask_NotFound(t *testing.T) {
	dataDir := "./test_task_service_update_notfound_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "non_existent",
		Name: "Test",
	})

	err := ts.UpdateTask(task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestDeleteTask(t *testing.T) {
	dataDir := "./test_task_service_delete_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_delete",
		Name: "Test Task",
	}
	ts.CreateTask(taskConfig)

	// 验证任务存在
	_, exists := ts.GetTask("test_task_delete")
	assert.True(t, exists)

	// 删除任务
	err := ts.DeleteTask("test_task_delete")
	assert.NoError(t, err)

	// 验证任务已删除
	_, exists = ts.GetTask("test_task_delete")
	assert.False(t, exists)
}

func TestDeleteTask_NotFound(t *testing.T) {
	dataDir := "./test_task_service_delete_notfound_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	err := ts.DeleteTask("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestStartTask(t *testing.T) {
	dataDir := "./test_task_service_start"
	defer os.RemoveAll(dataDir)

	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	analyzer := &mockAnalyzer{}

	ts := NewTaskServiceWithDB(sourceDB, targetDB, analyzer)

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:           "test_task_start",
		Name:         "Test Task",
		SourceSchema: "source_db",
		TargetSchema: "target_db",
		Tables:       []string{"users"},
		Mode:         taskEntity.SyncModeFull,
		SourceDB: &taskEntity.DatabaseConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			Database: "source_db",
			Username: "root",
			Password: "pwd",
		},
		TargetDB: &taskEntity.DatabaseConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			Database: "target_db",
			Username: "root",
			Password: "pwd",
		},
	}
	ts.CreateTask(taskConfig)

	// 启动任务（当前实现会在启动时重建真实数据库连接，单元测试环境下预期失败）
	ctx := context.Background()
	err = ts.StartTask(ctx, "test_task_start")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to initialize database connections")

	// 验证任务状态保持未启动
	task, _ := ts.GetTask("test_task_start")
	assert.Equal(t, taskEntity.TaskStatusPending, task.Context.Status)
}

func TestStartTask_NotFound(t *testing.T) {
	dataDir := "./test_task_service_start_notfound"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	ctx := context.Background()
	err := ts.StartTask(ctx, "non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestStartTask_AlreadyRunning(t *testing.T) {
	dataDir := "./test_task_service_start_running"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	// 创建并启动任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_running",
		Name: "Test Task",
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Start()

	ctx := context.Background()
	err := ts.StartTask(ctx, "test_task_running")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already running")
}

func TestStartTask_ConcurrentRuntimeIsolation(t *testing.T) {
	dataDir := "./test_task_service_start_concurrent_runtime"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_concurrent_1", Name: "Task Concurrent 1"})
	require.NoError(t, err)
	_, err = ts.CreateTask(taskEntity.TaskConfig{ID: "task_concurrent_2", Name: "Task Concurrent 2"})
	require.NoError(t, err)

	createdRuntimes := make(map[string]*taskRuntime)
	var createdMu sync.Mutex
	ts.initRuntimeFn = func(task *taskEntity.SyncTask) (*taskRuntime, error) {
		r := &taskRuntime{}
		createdMu.Lock()
		createdRuntimes[task.Config.ID] = r
		createdMu.Unlock()
		return r, nil
	}

	execStarted := make(chan string, 2)
	ts.executeSyncFn = func(_ context.Context, taskID string, _ *taskRuntime) {
		execStarted <- taskID
	}

	startErrCh := make(chan error, 2)
	var wg sync.WaitGroup
	for _, taskID := range []string{"task_concurrent_1", "task_concurrent_2"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			startErrCh <- ts.StartTask(context.Background(), id)
		}(taskID)
	}
	wg.Wait()
	close(startErrCh)

	for startErr := range startErrCh {
		assert.NoError(t, startErr)
	}

	received := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case taskID := <-execStarted:
			received[taskID] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting executeSync for task %d", i+1)
		}
	}
	assert.True(t, received["task_concurrent_1"])
	assert.True(t, received["task_concurrent_2"])

	ts.mu.RLock()
	runtime1 := ts.runtimes["task_concurrent_1"]
	runtime2 := ts.runtimes["task_concurrent_2"]
	ts.mu.RUnlock()

	require.NotNil(t, runtime1)
	require.NotNil(t, runtime2)
	assert.NotSame(t, runtime1, runtime2)

	createdMu.Lock()
	assert.Same(t, createdRuntimes["task_concurrent_1"], runtime1)
	assert.Same(t, createdRuntimes["task_concurrent_2"], runtime2)
	createdMu.Unlock()

	task1, ok1 := ts.GetTask("task_concurrent_1")
	task2, ok2 := ts.GetTask("task_concurrent_2")
	require.True(t, ok1)
	require.True(t, ok2)
	assert.Equal(t, taskEntity.TaskStatusRunning, task1.Context.Status)
	assert.Equal(t, taskEntity.TaskStatusRunning, task2.Context.Status)
}

func TestStartTask_SuccessPath(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{
		ID:   "task_success",
		Name: "Success Path Task",
		Mode: taskEntity.SyncModeFull,
	})
	require.NoError(t, err)

	// Inject fake runtime init
	fakeRuntime := &taskRuntime{}
	ts.initRuntimeFn = func(task *taskEntity.SyncTask) (*taskRuntime, error) {
		return fakeRuntime, nil
	}

	// Capture executeSync call
	type execCall struct {
		taskID  string
		runtime *taskRuntime
	}
	execCh := make(chan execCall, 1)
	ts.executeSyncFn = func(ctx context.Context, taskID string, rt *taskRuntime) {
		execCh <- execCall{taskID: taskID, runtime: rt}
	}

	// Act
	beforeStart := time.Now().Add(-time.Millisecond)
	err = ts.StartTask(context.Background(), "task_success")
	require.NoError(t, err)

	// Assert: runtime is stored
	ts.mu.RLock()
	storedRT := ts.runtimes["task_success"]
	ts.mu.RUnlock()
	assert.Same(t, fakeRuntime, storedRT, "runtime should be stored in runtimes map")

	// Assert: task status changed to Running with StartTime set
	task, ok := ts.GetTask("task_success")
	require.True(t, ok)
	assert.Equal(t, taskEntity.TaskStatusRunning, task.Context.Status)
	assert.False(t, task.Context.StartTime.IsZero(), "StartTime should be set")
	assert.True(t, task.Context.StartTime.After(beforeStart), "StartTime should be recent")

	// Assert: cancel function is wired on runtime
	assert.NotNil(t, fakeRuntime.cancel, "runtime.cancel should be set by StartTask")

	// Assert: executeSync was triggered asynchronously with correct args
	select {
	case call := <-execCh:
		assert.Equal(t, "task_success", call.taskID)
		assert.Same(t, fakeRuntime, call.runtime)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for executeSync to be called")
	}
}

func TestStartTask_SuccessPath_ExecuteSyncGetsIndependentContext(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_ctx", Name: "Ctx Task"})
	require.NoError(t, err)

	ts.initRuntimeFn = func(_ *taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}

	// Capture the context passed to executeSync
	ctxCh := make(chan context.Context, 1)
	ts.executeSyncFn = func(ctx context.Context, _ string, _ *taskRuntime) {
		ctxCh <- ctx
	}

	// Use a cancellable "HTTP request" context
	httpCtx, httpCancel := context.WithCancel(context.Background())
	err = ts.StartTask(httpCtx, "task_ctx")
	require.NoError(t, err)

	// Cancel the HTTP context — executeSync's context should remain alive
	httpCancel()

	select {
	case syncCtx := <-ctxCh:
		assert.NoError(t, syncCtx.Err(), "executeSync context should NOT be cancelled when HTTP ctx is cancelled")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for executeSync")
	}
}

func TestStartTask_SuccessPath_OldRuntimeClosed(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_restart", Name: "Restart Task"})
	require.NoError(t, err)

	// Simulate a pre-existing runtime with a cancel func to verify Close() is called
	oldCancelled := false
	oldRuntime := &taskRuntime{
		cancel: func() { oldCancelled = true },
	}
	ts.runtimes["task_restart"] = oldRuntime

	newRuntime := &taskRuntime{}
	ts.initRuntimeFn = func(_ *taskEntity.SyncTask) (*taskRuntime, error) {
		return newRuntime, nil
	}
	ts.executeSyncFn = func(_ context.Context, _ string, _ *taskRuntime) {}

	// Task must not be Running to pass the guard
	task.Context.Status = taskEntity.TaskStatusPaused

	err = ts.StartTask(context.Background(), "task_restart")
	require.NoError(t, err)

	// Old runtime should have been closed (cancel called)
	assert.True(t, oldCancelled, "old runtime cancel should be called during Close()")

	// New runtime replaces old
	ts.mu.RLock()
	assert.Same(t, newRuntime, ts.runtimes["task_restart"])
	ts.mu.RUnlock()
}

func TestStartTask_SuccessPath_AuditLoggerCalled(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_audit", Name: "Audit Task"})
	require.NoError(t, err)

	ts.initRuntimeFn = func(_ *taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(_ context.Context, _ string, _ *taskRuntime) {}

	// Wire up a real audit logger to a temp dir, then query it
	auditDir := t.TempDir()
	ts.auditLogger = audit.NewAuditLogger(auditDir)
	defer ts.auditLogger.Close()

	err = ts.StartTask(context.Background(), "task_audit")
	require.NoError(t, err)

	events, err := ts.auditLogger.Query(audit.QueryOptions{TaskID: "task_audit"})
	require.NoError(t, err)
	require.NotEmpty(t, events, "audit logger should have recorded a task-resumed event")
}

func TestStartTask_SuccessPath_StorageSaveCalled(t *testing.T) {
	dataDir := t.TempDir()

	ts := newTestTaskService(dataDir)
	ts.runtimes = make(map[string]*taskRuntime)

	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: "task_save", Name: "Save Task"})
	require.NoError(t, err)

	ts.initRuntimeFn = func(_ *taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(_ context.Context, _ string, _ *taskRuntime) {}

	err = ts.StartTask(context.Background(), "task_save")
	require.NoError(t, err)

	// Reload from storage to confirm the Running state was persisted
	loaded, err := ts.storage.LoadAll()
	require.NoError(t, err)

	var found bool
	for _, lt := range loaded {
		if lt.Config.ID == "task_save" {
			found = true
			assert.Equal(t, taskEntity.TaskStatusRunning, lt.Context.Status,
				"persisted task should have Running status")
		}
	}
	assert.True(t, found, "task_save should exist in storage")
}

func TestPauseTask(t *testing.T) {
	dataDir := "./test_task_service_pause"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	// 创建并启动任务
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_pause",
		Name: "Test Task",
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Start()

	// 暂停任务
	err := ts.PauseTask("test_task_pause")
	assert.NoError(t, err)

	// 验证任务状态
	retrievedTask, _ := ts.GetTask("test_task_pause")
	assert.Equal(t, taskEntity.TaskStatusPaused, retrievedTask.Context.Status)
}

func TestPauseTask_NotFound(t *testing.T) {
	dataDir := "./test_task_service_pause_notfound"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	err := ts.PauseTask("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestSkipError(t *testing.T) {
	dataDir := "./test_task_service_skip"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	// 创建任务并设置错误状态
	taskConfig := taskEntity.TaskConfig{
		ID:   "test_task_skip",
		Name: "Test Task",
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Context.Status = taskEntity.TaskStatusFailed
	task.Context.ErrorStack = "some error"

	// 跳过错误
	err := ts.SkipError("test_task_skip")
	assert.NoError(t, err)

	// 验证错误已清除
	retrievedTask, _ := ts.GetTask("test_task_skip")
	assert.Equal(t, taskEntity.TaskStatusPaused, retrievedTask.Context.Status)
	assert.Empty(t, retrievedTask.Context.ErrorStack)
}

func TestSkipError_NotFound(t *testing.T) {
	dataDir := "./test_task_service_skip_notfound"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	err := ts.SkipError("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestGetTaskMetrics(t *testing.T) {
	dataDir := "./test_task_service_metrics_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 创建任务
	taskConfig := taskEntity.TaskConfig{
		ID:     "test_task_metrics_unique",
		Name:   "Test Task",
		Tables: []string{"users", "orders"},
	}
	task, _ := ts.CreateTask(taskConfig)
	task.Context.ProcessedRows = 1000
	task.Context.TotalRows = 2000
	task.Context.CurrentPosition = "position_1"
	// 手动计算进度百分比
	task.Context.ProgressPercent = 50.0

	// 获取指标
	metrics, err := ts.GetTaskMetrics("test_task_metrics_unique")
	assert.NoError(t, err)
	assert.NotNil(t, metrics)
	assert.Equal(t, int64(1000), metrics["processed_rows"])
	assert.Equal(t, int64(2000), metrics["total_rows"])
	assert.Equal(t, 50.0, metrics["progress_percent"])
	assert.Equal(t, 0, metrics["tables_completed"])
	assert.Equal(t, 2, metrics["tables_total"])
	assert.Equal(t, "position_1", metrics["current_position"])
}

func TestGetTaskMetrics_NotFound(t *testing.T) {
	dataDir := "./test_task_service_metrics_notfound_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	_, err := ts.GetTaskMetrics("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task not found")
}

func TestGetRunningTaskCount(t *testing.T) {
	dataDir := "./test_task_service_running_count_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 初始应该为 0
	count := ts.GetRunningTaskCount()
	assert.Equal(t, 0, count)

	// 创建并启动一些任务
	task1, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1_unique", Name: "Task 1"})
	task1.Start()

	task2, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_2_unique", Name: "Task 2"})
	task2.Start()

	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_3_unique", Name: "Task 3"})
	// task3 保持暂停状态

	count = ts.GetRunningTaskCount()
	assert.Equal(t, 2, count)
}

func TestClose(t *testing.T) {
	dataDir := "./test_task_service_close"
	defer os.RemoveAll(dataDir)

	sourceDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	analyzer := &mockAnalyzer{}

	ts := NewTaskServiceWithDB(sourceDB, targetDB, analyzer)

	// 创建一些任务
	task1, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task1.Start()

	task2, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_2", Name: "Task 2"})
	task2.Start()

	// 关闭服务
	err = ts.Close()
	assert.NoError(t, err)

	// 验证运行中的任务已暂停
	retrievedTask1, _ := ts.GetTask("task_1")
	assert.Equal(t, taskEntity.TaskStatusPaused, retrievedTask1.Context.Status)

	retrievedTask2, _ := ts.GetTask("task_2")
	assert.Equal(t, taskEntity.TaskStatusPaused, retrievedTask2.Context.Status)
}

func TestGenerateServerID(t *testing.T) {
	tests := []struct {
		name     string
		taskID   string
		expected uint32
	}{
		{"simple", "task_1", 0},
		{"empty", "", 1},
		{"complex", "task_with_long_name_12345", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverID := generateServerID(tt.taskID)
			assert.NotZero(t, serverID)
			if tt.taskID == "" {
				assert.Equal(t, tt.expected, serverID)
			}
		})
	}
}

func TestIsTaskStopped(t *testing.T) {
	dataDir := "./test_task_service_stopped_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 测试不存在的任务
	assert.True(t, ts.isTaskStopped("non_existent"))

	// 创建任务
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// 测试未运行的任务
	assert.True(t, ts.isTaskStopped("task_1"))

	// 启动任务
	task.Start()

	// 测试运行中的任务
	assert.False(t, ts.isTaskStopped("task_1"))
}

func TestUpdateTaskProgress(t *testing.T) {
	dataDir := "./test_task_service_progress_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 创建任务
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// 更新进度
	ts.updateTaskProgress("task_1", 100, "position_1")

	// 验证更新
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(100), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "position_1", retrievedTask.Context.CurrentPosition)
}

func TestIncrementTaskProgress(t *testing.T) {
	dataDir := "./test_task_service_increment_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 创建任务
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Context.TotalRows = 1000

	// 增加进度
	ts.incrementTaskProgress("task_1", 100, "position_1")
	ts.incrementTaskProgress("task_1", 200, "position_2")

	// 验证更新
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(300), retrievedTask.Context.ProcessedRows)
	assert.Equal(t, "position_2", retrievedTask.Context.CurrentPosition)
	assert.Equal(t, 30.0, retrievedTask.Context.ProgressPercent)
}

func TestUpdateTaskTotalRows(t *testing.T) {
	dataDir := "./test_task_service_totalrows_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 创建任务
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// 更新总行数
	ts.updateTaskTotalRows("task_1", 5000)

	// 验证更新
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, int64(5000), retrievedTask.Context.TotalRows)
}

func TestUpdateTaskStatus(t *testing.T) {
	dataDir := "./test_task_service_status_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 创建任务
	_, _ = ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})

	// 更新状态
	ts.updateTaskStatus("task_1", taskEntity.TaskStatusFailed, "test error")

	// 验证更新
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, taskEntity.TaskStatusFailed, retrievedTask.Context.Status)
	assert.Equal(t, "test error", retrievedTask.Context.ErrorStack)
	assert.NotNil(t, retrievedTask.Context.EndTime)
}

func TestCompleteTask(t *testing.T) {
	dataDir := "./test_task_service_complete_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 创建并启动任务
	task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "task_1", Name: "Task 1"})
	task.Start()

	// 完成任务
	ts.completeTask("task_1")

	// 验证完成
	retrievedTask, _ := ts.GetTask("task_1")
	assert.Equal(t, taskEntity.TaskStatusCompleted, retrievedTask.Context.Status)
	assert.NotNil(t, retrievedTask.Context.EndTime)
}

func TestTaskStorage_Save_Error(t *testing.T) {
	dataDir := "./test_task_storage_error"
	defer os.RemoveAll(dataDir)

	// 创建一个无效的目录路径（使用保留字符）
	storage := NewFileTaskStorage("invalid:dir")

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "test_task",
		Name: "Test Task",
	})

	err := storage.Save(task)
	assert.Error(t, err)
}

func TestTaskStorage_LoadAll_InvalidJSON(t *testing.T) {
	dataDir := "./test_task_storage_invalid_json"
	defer os.RemoveAll(dataDir)

	storage := NewFileTaskStorage(dataDir)

	// 创建一个无效的 JSON 文件
	invalidJSON := `{"invalid": json}`
	filePath := dataDir + "/invalid.json"
	os.WriteFile(filePath, []byte(invalidJSON), 0644)

	// 加载应该跳过无效文件
	tasks, err := storage.LoadAll()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestTaskStorage_LoadAll_ReadError(t *testing.T) {
	dataDir := "./test_task_storage_read_error"
	defer os.RemoveAll(dataDir)

	storage := NewFileTaskStorage(dataDir)

	// 创建一个目录而不是文件
	dirPath := dataDir + "/subdir"
	os.MkdirAll(dirPath, 0755)

	// 加载应该跳过目录
	tasks, err := storage.LoadAll()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestTaskStorage_NewTaskStorage_Error(t *testing.T) {
	// 测试创建目录失败的情况
	// 由于 os.MkdirAll 在大多数情况下不会失败，这里只是测试函数不会 panic
	storage := NewFileTaskStorage("data")
	assert.NotNil(t, storage)
}

func TestTaskService_ConcurrentOperations(t *testing.T) {
	dataDir := "./test_task_service_concurrent_unique"
	defer os.RemoveAll(dataDir)

	ts := newTestTaskService(dataDir)

	// 并发创建任务
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			taskID := "concurrent_task_unique_" + string(rune('0'+id))
			ts.CreateTask(taskEntity.TaskConfig{
				ID:   taskID,
				Name: "Concurrent Task",
			})
			done <- true
		}(i)
	}

	// 等待所有操作完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证所有任务都已创建
	tasks := ts.GetAllTasks()
	assert.Equal(t, 10, len(tasks))
}

// ==================== syncReadBatchLimit ====================

func TestSyncReadBatchLimit(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
		expected  int64
	}{
		{"zero returns default 1000", 0, 1000},
		{"negative returns default 1000", -1, 1000},
		{"small positive returns as-is", 500, 500},
		{"default boundary", 1000, 1000},
		{"large value capped at 100000", 200000, 100000},
		{"exact hard max", 100000, 100000},
		{"one", 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := syncReadBatchLimit(tt.batchSize)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ==================== adjustReadLimitForWideColumns ====================

func TestAdjustReadLimitForWideColumns(t *testing.T) {
	t.Run("nil identity returns base", func(t *testing.T) {
		assert.Equal(t, int64(1000), adjustReadLimitForWideColumns(1000, nil))
	})

	t.Run("empty columns returns base", func(t *testing.T) {
		identity := &entity.TableIdentity{Columns: []entity.ColumnMeta{}}
		assert.Equal(t, int64(1000), adjustReadLimitForWideColumns(1000, identity))
	})

	t.Run("no heavy columns returns base", func(t *testing.T) {
		identity := &entity.TableIdentity{
			Columns: []entity.ColumnMeta{
				{Name: "id", DataType: "bigint"},
				{Name: "name", DataType: "varchar(255)"},
			},
		}
		assert.Equal(t, int64(1000), adjustReadLimitForWideColumns(1000, identity))
	})

	t.Run("one json column reduces limit", func(t *testing.T) {
		identity := &entity.TableIdentity{
			Columns: []entity.ColumnMeta{
				{Name: "id", DataType: "bigint"},
				{Name: "data", DataType: "json"},
			},
		}
		result := adjustReadLimitForWideColumns(1000, identity)
		assert.True(t, result < 1000)
		assert.True(t, result >= 25)
	})

	t.Run("multiple heavy columns reduce more", func(t *testing.T) {
		identity := &entity.TableIdentity{
			Columns: []entity.ColumnMeta{
				{Name: "id", DataType: "bigint"},
				{Name: "data", DataType: "json"},
				{Name: "content", DataType: "longtext"},
				{Name: "avatar", DataType: "blob"},
			},
		}
		result := adjustReadLimitForWideColumns(1000, identity)
		assert.True(t, result < 500)
		assert.True(t, result >= 25)
	})

	t.Run("small base does not go below 25", func(t *testing.T) {
		identity := &entity.TableIdentity{
			Columns: []entity.ColumnMeta{
				{Name: "data", DataType: "longblob"},
				{Name: "content", DataType: "mediumtext"},
				{Name: "extra", DataType: "tinyblob"},
			},
		}
		result := adjustReadLimitForWideColumns(50, identity)
		assert.Equal(t, int64(25), result)
	})

	t.Run("various blob/text types recognized", func(t *testing.T) {
		for _, dt := range []string{"json", "blob", "tinyblob", "mediumblob", "longblob", "text", "tinytext", "mediumtext", "longtext"} {
			identity := &entity.TableIdentity{
				Columns: []entity.ColumnMeta{
					{Name: "id", DataType: "int"},
					{Name: "col", DataType: dt},
				},
			}
			result := adjustReadLimitForWideColumns(1000, identity)
			assert.True(t, result < 1000, "data type %s should be recognized as heavy", dt)
		}
	})
}

// ==================== extractTableDefinition ====================

func TestExtractTableDefinition(t *testing.T) {
	t.Run("normal create table", func(t *testing.T) {
		sql := "CREATE TABLE `users` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
		result := extractTableDefinition(sql)
		assert.True(t, strings.HasPrefix(result, "("))
		assert.Contains(t, result, "PRIMARY KEY")
		assert.Contains(t, result, "ENGINE=InnoDB")
	})

	t.Run("no parentheses returns original", func(t *testing.T) {
		sql := "SOMETHING WEIRD"
		assert.Equal(t, sql, extractTableDefinition(sql))
	})

	t.Run("only opening paren", func(t *testing.T) {
		sql := "CREATE TABLE `t` (incomplete"
		result := extractTableDefinition(sql)
		assert.Equal(t, sql, result)
	})
}

// ==================== isSecondaryIndexDefinitionLine ====================

func TestIsSecondaryIndexDefinitionLine(t *testing.T) {
	tests := []struct {
		line     string
		expected bool
	}{
		{"  UNIQUE KEY `uk_email` (`email`),", true},
		{"  UNIQUE INDEX `ui_email` (`email`),", true},
		{"  KEY `idx_name` (`name`),", true},
		{"  INDEX `idx_name` (`name`),", true},
		{"  FULLTEXT KEY `ft_content` (`content`),", true},
		{"  FULLTEXT INDEX `ft_content` (`content`),", true},
		{"  SPATIAL KEY `sp_geo` (`geo`),", true},
		{"  SPATIAL INDEX `sp_geo` (`geo`),", true},
		{"  PRIMARY KEY (`id`),", false},
		{"  `id` bigint NOT NULL,", false},
		{"  ) ENGINE=InnoDB", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			assert.Equal(t, tt.expected, isSecondaryIndexDefinitionLine(tt.line))
		})
	}
}

// ==================== trimTrailingComma ====================

func TestTrimTrailingComma(t *testing.T) {
	assert.Equal(t, "  PRIMARY KEY (`id`)", trimTrailingComma("  PRIMARY KEY (`id`),"))
	assert.Equal(t, "  PRIMARY KEY (`id`)", trimTrailingComma("  PRIMARY KEY (`id`)"))
	assert.Equal(t, "", trimTrailingComma(","))
	assert.Equal(t, "", trimTrailingComma(""))
	assert.Equal(t, "abc", trimTrailingComma("abc,  "))
}

// ==================== toInt64PK ====================

func TestToInt64PK(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
		ok       bool
	}{
		{"int", int(42), 42, true},
		{"int8", int8(8), 8, true},
		{"int16", int16(16), 16, true},
		{"int32", int32(32), 32, true},
		{"int64", int64(64), 64, true},
		{"uint", uint(10), 10, true},
		{"uint8", uint8(8), 8, true},
		{"uint16", uint16(16), 16, true},
		{"uint32", uint32(32), 32, true},
		{"uint64 small", uint64(100), 100, true},
		{"uint64 overflow", uint64(^uint64(0)), 0, false},
		{"string numeric", "12345", 12345, true},
		{"string non-numeric", "abc", 0, false},
		{"[]byte numeric", []byte("999"), 999, true},
		{"[]byte non-numeric", []byte("xyz"), 0, false},
		{"nil", nil, 0, false},
		{"float64", float64(3.14), 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toInt64PK(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// ==================== comparePKValues ====================

func TestComparePKValues(t *testing.T) {
	// int64 comparison
	assert.Equal(t, -1, comparePKValues(int64(1), int64(2)))
	assert.Equal(t, 0, comparePKValues(int64(5), int64(5)))
	assert.Equal(t, 1, comparePKValues(int64(10), int64(3)))

	// string comparison
	assert.Equal(t, -1, comparePKValues("abc", "def"))
	assert.Equal(t, 0, comparePKValues("same", "same"))
	assert.Equal(t, 1, comparePKValues("z", "a"))

	// mixed: both convertible to int64
	assert.Equal(t, 0, comparePKValues(int64(42), "42"))

	// fallback to string comparison
	assert.Equal(t, -1, comparePKValues(nil, "something"))
}

// ==================== comparePKWithBoundary ====================

func TestComparePKWithBoundary(t *testing.T) {
	t.Run("nil boundary returns -1", func(t *testing.T) {
		row := map[string]interface{}{"id": int64(1)}
		assert.Equal(t, -1, comparePKWithBoundary([]string{"id"}, row, nil))
	})

	t.Run("single column boundary less (string comparison)", func(t *testing.T) {
		row := map[string]interface{}{"id": "10"}
		// comparePKWithBoundary uses fmt.Sprintf("%v",...) string comparison
		assert.Equal(t, -1, comparePKWithBoundary([]string{"id"}, row, "20"))
	})

	t.Run("single column boundary equal", func(t *testing.T) {
		row := map[string]interface{}{"id": "10"}
		assert.Equal(t, 0, comparePKWithBoundary([]string{"id"}, row, "10"))
	})

	t.Run("single column boundary greater", func(t *testing.T) {
		row := map[string]interface{}{"id": "30"}
		assert.Equal(t, 1, comparePKWithBoundary([]string{"id"}, row, "20"))
	})

	t.Run("single column uses string repr of int64", func(t *testing.T) {
		// fmt.Sprintf("%v", int64(5)) = "5", fmt.Sprintf("%v", int64(10)) = "10"
		// "5" > "10" lexicographically, so result is 1 (not -1)
		row := map[string]interface{}{"id": int64(5)}
		assert.Equal(t, 1, comparePKWithBoundary([]string{"id"}, row, int64(10)))
	})

	t.Run("composite boundary less on first col", func(t *testing.T) {
		row := map[string]interface{}{"a": "1", "b": "5"}
		boundary := []interface{}{"2", "3"}
		assert.Equal(t, -1, comparePKWithBoundary([]string{"a", "b"}, row, boundary))
	})

	t.Run("composite boundary equal", func(t *testing.T) {
		row := map[string]interface{}{"a": "2", "b": "3"}
		boundary := []interface{}{"2", "3"}
		assert.Equal(t, 0, comparePKWithBoundary([]string{"a", "b"}, row, boundary))
	})

	t.Run("composite boundary greater on second col", func(t *testing.T) {
		row := map[string]interface{}{"a": "2", "b": "9"}
		boundary := []interface{}{"2", "3"}
		assert.Equal(t, 1, comparePKWithBoundary([]string{"a", "b"}, row, boundary))
	})
}

// ==================== boundaryToString ====================

func TestBoundaryToString(t *testing.T) {
	assert.Equal(t, "", boundaryToString(nil))
	assert.Equal(t, "42", boundaryToString(42))
	assert.Equal(t, "hello", boundaryToString("hello"))
	// composite
	result := boundaryToString([]interface{}{"a", "b", "c"})
	assert.Equal(t, "a\x00b\x00c", result)
}

// ==================== isRetryableLockError ====================

func TestIsRetryableLockError(t *testing.T) {
	assert.False(t, isRetryableLockError(nil))
	assert.False(t, isRetryableLockError(fmt.Errorf("some random error")))
	assert.True(t, isRetryableLockError(fmt.Errorf("Error 1205: Lock wait timeout exceeded")))
	assert.True(t, isRetryableLockError(fmt.Errorf("Error 1213: Deadlock found when trying to get lock")))
	assert.True(t, isRetryableLockError(fmt.Errorf("Lock wait timeout")))
	assert.True(t, isRetryableLockError(fmt.Errorf("Deadlock found")))
}

// ==================== isNumericPKColumn ====================

func TestIsNumericPKColumn(t *testing.T) {
	identity := &entity.TableIdentity{
		Columns: []entity.ColumnMeta{
			{Name: "id", DataType: "bigint"},
			{Name: "name", DataType: "varchar"},
			{Name: "age", DataType: "int"},
			{Name: "code", DataType: "smallint"},
			{Name: "tiny", DataType: "tinyint"},
			{Name: "mid", DataType: "mediumint"},
		},
	}

	assert.True(t, isNumericPKColumn(identity, "id"))
	assert.False(t, isNumericPKColumn(identity, "name"))
	assert.True(t, isNumericPKColumn(identity, "age"))
	assert.True(t, isNumericPKColumn(identity, "code"))
	assert.True(t, isNumericPKColumn(identity, "tiny"))
	assert.True(t, isNumericPKColumn(identity, "mid"))
	assert.False(t, isNumericPKColumn(identity, "nonexistent"))
}

// ==================== dbScanToString ====================

func TestDbScanToString(t *testing.T) {
	assert.Equal(t, "", dbScanToString(nil))
	assert.Equal(t, "hello", dbScanToString([]byte("hello")))
	assert.Equal(t, "world", dbScanToString("world"))
	assert.Equal(t, "42", dbScanToString(int64(42)))
	assert.Equal(t, "3.14", dbScanToString(3.14))
}

// ==================== dbScanToInt ====================

func TestDbScanToInt(t *testing.T) {
	assert.Equal(t, 0, dbScanToInt(nil))
	assert.Equal(t, 42, dbScanToInt(int64(42)))
	assert.Equal(t, 99, dbScanToInt([]byte("99")))
	assert.Equal(t, 7, dbScanToInt("7"))
	assert.Equal(t, 0, dbScanToInt("abc"))
	assert.Equal(t, 0, dbScanToInt(3.14))
}

// ==================== syncTuneFrom ====================

func TestSyncTuneFrom(t *testing.T) {
	t.Run("nil config returns nil", func(t *testing.T) {
		assert.Nil(t, syncTuneFrom(nil))
	})

	t.Run("non-nil config returns sync config pointer", func(t *testing.T) {
		cfg := &config.Config{
			Sync: config.SyncTuneConfig{
				IntraTableLegacyCap: 8,
				IntraTableHardMax:   32,
			},
		}
		result := syncTuneFrom(cfg)
		require.NotNil(t, result)
		assert.Equal(t, 8, result.IntraTableLegacyCap)
		assert.Equal(t, 32, result.IntraTableHardMax)
	})
}

// ==================== intraTableConcurrencyCaps ====================

func TestIntraTableConcurrencyCaps(t *testing.T) {
	t.Run("nil config returns defaults", func(t *testing.T) {
		ts := &TaskService{config: nil}
		legacy, hard := ts.intraTableConcurrencyCaps()
		assert.Equal(t, 16, legacy)
		assert.Equal(t, 64, hard)
	})

	t.Run("zero sync config returns defaults", func(t *testing.T) {
		ts := &TaskService{config: &config.Config{}}
		legacy, hard := ts.intraTableConcurrencyCaps()
		assert.Equal(t, 16, legacy)
		assert.Equal(t, 64, hard)
	})

	t.Run("custom values override defaults", func(t *testing.T) {
		ts := &TaskService{config: &config.Config{
			Sync: config.SyncTuneConfig{
				IntraTableLegacyCap: 8,
				IntraTableHardMax:   32,
			},
		}}
		legacy, hard := ts.intraTableConcurrencyCaps()
		assert.Equal(t, 8, legacy)
		assert.Equal(t, 32, hard)
	})

	t.Run("only legacy cap set", func(t *testing.T) {
		ts := &TaskService{config: &config.Config{
			Sync: config.SyncTuneConfig{
				IntraTableLegacyCap: 4,
			},
		}}
		legacy, hard := ts.intraTableConcurrencyCaps()
		assert.Equal(t, 4, legacy)
		assert.Equal(t, 64, hard)
	})
}

// ==================== failTaskUnlessCancelled ====================

func TestFailTaskUnlessCancelled(t *testing.T) {
	t.Run("marks task failed when context active and task running", func(t *testing.T) {
		ts := newTestTaskService(t.TempDir())
		task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "ft1", Name: "T1"})
		task.Start() // must be running, otherwise isTaskStopped returns true

		ctx := context.Background()
		ts.failTaskUnlessCancelled(ctx, "ft1", "some error")

		task, _ = ts.GetTask("ft1")
		assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
		assert.Equal(t, "some error", task.Context.ErrorStack)
	})

	t.Run("ignores error when context cancelled", func(t *testing.T) {
		ts := newTestTaskService(t.TempDir())
		task, _ := ts.CreateTask(taskEntity.TaskConfig{ID: "ft2", Name: "T2"})
		task.Start()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ts.failTaskUnlessCancelled(ctx, "ft2", "should be ignored")

		task, _ = ts.GetTask("ft2")
		assert.Equal(t, taskEntity.TaskStatusRunning, task.Context.Status)
		assert.Empty(t, task.Context.ErrorStack)
	})

	t.Run("ignores error when task already stopped", func(t *testing.T) {
		ts := newTestTaskService(t.TempDir())
		ts.CreateTask(taskEntity.TaskConfig{ID: "ft3", Name: "T3"})
		// task is in Pending status (not running), so isTaskStopped returns true

		ctx := context.Background()
		ts.failTaskUnlessCancelled(ctx, "ft3", "should be ignored")

		task, _ := ts.GetTask("ft3")
		assert.Equal(t, taskEntity.TaskStatusPending, task.Context.Status)
	})
}

// ==================== taskRuntime.Close ====================

func TestTaskRuntimeClose(t *testing.T) {
	t.Run("nil runtime does not panic", func(t *testing.T) {
		var r *taskRuntime
		assert.NotPanics(t, func() { r.Close() })
	})

	t.Run("calls cancel and closes DBs", func(t *testing.T) {
		cancelCalled := false

		sourceDB, _, err := sqlmock.New()
		require.NoError(t, err)

		targetDB, _, err := sqlmock.New()
		require.NoError(t, err)

		r := &taskRuntime{
			sourceDB: sourceDB,
			targetDB: targetDB,
			cancel:   func() { cancelCalled = true },
		}

		r.Close()
		assert.True(t, cancelCalled)
	})

	t.Run("does not double-close when source equals target", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)

		r := &taskRuntime{
			sourceDB: db,
			targetDB: db,
		}
		assert.NotPanics(t, func() { r.Close() })
	})

	t.Run("nil cancel does not panic", func(t *testing.T) {
		r := &taskRuntime{}
		assert.NotPanics(t, func() { r.Close() })
	})
}

// ==================== closeResource ====================

func TestCloseResource(t *testing.T) {
	t.Run("nil closer does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() { closeResource(nil, "test") })
	})

	t.Run("successful close", func(t *testing.T) {
		called := false
		closer := func() error {
			called = true
			return nil
		}
		closeResource(closer, "test")
		assert.True(t, called)
	})

	t.Run("error close does not panic", func(t *testing.T) {
		closer := func() error {
			return fmt.Errorf("close error")
		}
		assert.NotPanics(t, func() { closeResource(closer, "test") })
	})
}

// ==================== withDDL ====================

func TestWithDDL(t *testing.T) {
	t.Run("nil runtime executes fn directly", func(t *testing.T) {
		ts := &TaskService{}
		called := false
		err := ts.withDDL(nil, func() error {
			called = true
			return nil
		})
		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("nil readOnlyManager executes fn directly", func(t *testing.T) {
		ts := &TaskService{}
		rt := &taskRuntime{readOnlyManager: nil}
		called := false
		err := ts.withDDL(rt, func() error {
			called = true
			return nil
		})
		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("fn error propagated", func(t *testing.T) {
		ts := &TaskService{}
		err := ts.withDDL(nil, func() error {
			return fmt.Errorf("ddl error")
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ddl error")
	})
}

func TestDropTargetTableIfNeeded(t *testing.T) {
	ts := &TaskService{}
	err := ts.dropTargetTableIfNeeded(nil, "test", "users", true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "target connection is nil")

	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	conn, err := mockDB.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	t.Run("disabled returns nil", func(t *testing.T) {
		err := ts.dropTargetTableIfNeeded(conn, "test", "users", false)
		assert.NoError(t, err)
	})

	t.Run("drops target table when enabled", func(t *testing.T) {
		mock.ExpectExec("DROP TABLE IF EXISTS `test`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))
		err := ts.dropTargetTableIfNeeded(conn, "test", "users", true)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("uses custom target table name when dropping", func(t *testing.T) {
		customMockDB, customMock, err := sqlmock.New()
		require.NoError(t, err)
		defer customMockDB.Close()

		customConn, err := customMockDB.Conn(context.Background())
		require.NoError(t, err)
		defer customConn.Close()

		customMock.ExpectExec("DROP TABLE IF EXISTS `prod_backup`.`users_archive`").WillReturnResult(sqlmock.NewResult(0, 0))
		err = ts.dropTargetTableIfNeeded(customConn, "prod_backup", "users_archive", true)
		require.NoError(t, err)
		require.NoError(t, customMock.ExpectationsWereMet())
	})
}

func TestEnsureTargetTable_DropBeforeDDLRecreatesExistingTable(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	require.NoError(t, err)
	defer sourceDB.Close()

	targetDB, targetMock, err := sqlmock.New()
	require.NoError(t, err)
	defer targetDB.Close()

	ts := &TaskService{tasks: make(map[string]*taskEntity.SyncTask)}
	runtime := &taskRuntime{sourceDB: sourceDB, targetDB: targetDB}

	targetMock.ExpectQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?").WithArgs("target_db").WillReturnError(sql.ErrNoRows)
	targetMock.ExpectExec("CREATE DATABASE IF NOT EXISTS `target_db` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci").WillReturnResult(sqlmock.NewResult(0, 0))
	targetMock.ExpectQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'target_db' AND table_name = 'users'").WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users"))
	targetMock.ExpectExec("DROP TABLE IF EXISTS `target_db`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))
	targetMock.ExpectExec("CREATE TABLE `target_db`.`users` LIKE `source_db`.`users`").WillReturnResult(sqlmock.NewResult(0, 0))

	t.Run("existing table is recreated when enabled", func(t *testing.T) {
		targetConn, err := targetDB.Conn(context.Background())
		require.NoError(t, err)
		defer targetConn.Close()
		sourceConn, err := sourceDB.Conn(context.Background())
		require.NoError(t, err)
		defer sourceConn.Close()

		runtime.sourceDB = sourceDB
		runtime.targetDB = targetDB

		// ensureTargetTable 只会在 CREATE TABLE LIKE 前走目标库连接的 DROP，因此这里仅验证执行路径
		_, err = ts.ensureTargetTable(runtime, "source_db", "target_db", "users", "users", false, true)
		require.NoError(t, err)
	})

	require.NoError(t, targetMock.ExpectationsWereMet())
	require.NoError(t, sourceMock.ExpectationsWereMet())
}

// ==================== min ====================

func TestMin(t *testing.T) {
	assert.Equal(t, 3, min(3, 5))
	assert.Equal(t, 3, min(5, 3))
	assert.Equal(t, 4, min(4, 4))
	assert.Equal(t, -1, min(-1, 0))
}

// ==================== captureFullSyncStartPosition（短锁取位点） ====================

// captureFullSyncStartPosition 已固定为"短锁取位点"模式：
// 永远先 FTWRL，SHOW MASTER STATUS 拿到位点后立即 UNLOCK。
func TestCaptureFullSyncStartPosition_ShortReadLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("FLUSH TABLES WITH READ LOCK").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow("mysql-bin.000123", 456, "", "", ""))
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 0))

	ts := newTestTaskService(t.TempDir())
	pos, err := ts.captureFullSyncStartPosition(context.Background(), &taskRuntime{sourceDB: db})

	require.NoError(t, err)
	assert.Equal(t, "mysql-bin.000123", pos.Name)
	assert.Equal(t, uint32(456), pos.Pos)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCaptureFullSyncStartPosition_NilRuntime(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	_, err := ts.captureFullSyncStartPosition(context.Background(), nil)
	assert.Error(t, err)
}

// ==================== resolveSourceSchema edge cases ====================

func TestResolveSourceSchema_Empty(t *testing.T) {
	t.Run("nil task returns empty from nil config", func(t *testing.T) {
		ts := &TaskService{}
		assert.Equal(t, "", ts.resolveSourceSchema(nil))
	})

	t.Run("empty everything returns empty", func(t *testing.T) {
		ts := &TaskService{}
		task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "empty"})
		assert.Equal(t, "", ts.resolveSourceSchema(task))
	})
}

// ==================== FileTaskStorage.Delete edge cases ====================

func TestFileTaskStorage_Delete_NonExistent(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	err := storage.Delete("non_existent_task")
	assert.NoError(t, err)
}

func TestFileTaskStorage_Delete_Existing(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "delete_me", Name: "Delete Me"})
	err := storage.Save(task)
	require.NoError(t, err)

	err = storage.Delete("delete_me")
	assert.NoError(t, err)

	tasks, err := storage.LoadAll()
	require.NoError(t, err)
	assert.Empty(t, tasks)
}

// ==================== FileTaskStorage LoadAll empty dir ====================

func TestFileTaskStorage_LoadAll_EmptyDir(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	tasks, err := storage.LoadAll()
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

// ==================== FileTaskStorage Save and LoadAll round-trip ====================

func TestFileTaskStorage_SaveAndLoad_RoundTrip(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "round_trip",
		Name:         "Round Trip Task",
		SourceSchema: "src",
		TargetSchema: "tgt",
		Tables:       []string{"t1", "t2"},
	})

	err := storage.Save(task)
	require.NoError(t, err)

	tasks, err := storage.LoadAll()
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "round_trip", tasks[0].Config.ID)
	assert.Equal(t, "Round Trip Task", tasks[0].Config.Name)
	assert.Equal(t, "src", tasks[0].Config.SourceSchema)
	assert.Equal(t, "tgt", tasks[0].Config.TargetSchema)
	assert.Equal(t, []string{"t1", "t2"}, tasks[0].Config.Tables)
}

func TestCancelScheduleRestoresPreviousStatus(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "schedule_restore", Name: "Schedule Restore"})
	require.NoError(t, err)

	task.Pause()
	require.NoError(t, ts.ScheduleTask(task.Config.ID, time.Now().Add(time.Minute)))
	assert.Equal(t, taskEntity.TaskStatusScheduled, task.Context.Status)
	require.NotNil(t, task.Context.ScheduledFromStatus)
	assert.Equal(t, taskEntity.TaskStatusPaused, *task.Context.ScheduledFromStatus)

	require.NoError(t, ts.CancelSchedule(task.Config.ID))
	assert.Equal(t, taskEntity.TaskStatusPaused, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.NoError(t, ts.Close())
}

func TestCancelScheduleRestoresFailedStatus(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "schedule_restore_failed", Name: "Schedule Restore Failed"})
	require.NoError(t, err)

	task.Fail(assert.AnError)
	require.NoError(t, ts.ScheduleTask(task.Config.ID, time.Now().Add(time.Minute)))
	require.NoError(t, ts.CancelSchedule(task.Config.ID))

	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.NoError(t, ts.Close())
}

func TestScheduleTaskWithRepeatConfiguresRepeatFields(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "schedule_repeat", Name: "Schedule Repeat"})
	require.NoError(t, err)

	when := time.Now().Add(time.Minute)
	require.NoError(t, ts.ScheduleTaskWithRepeat(task.Config.ID, when, 3, 30))

	assert.Equal(t, taskEntity.TaskStatusScheduled, task.Context.Status)
	require.NotNil(t, task.Context.ScheduledAt)
	assert.WithinDuration(t, when, *task.Context.ScheduledAt, time.Second)
	assert.Equal(t, 3, task.Context.RepeatCount)
	assert.Equal(t, 3, task.Context.RepeatRemaining)
	assert.Equal(t, 30, task.Context.RepeatIntervalSec)
	assert.NoError(t, ts.Close())
}

func TestScheduleTaskWithRepeatRejectsInvalidTask(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	err := ts.ScheduleTaskWithRepeat("missing", time.Now().Add(time.Minute), 2, 10)
	assert.Error(t, err)
}

func TestCancelScheduleClearsRepeatFields(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "schedule_cancel_repeat", Name: "Schedule Cancel Repeat"})
	require.NoError(t, err)

	require.NoError(t, ts.ScheduleTaskWithRepeat(task.Config.ID, time.Now().Add(time.Minute), 3, 30))
	require.NoError(t, ts.CancelSchedule(task.Config.ID))

	assert.Equal(t, taskEntity.TaskStatusPending, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Nil(t, task.Context.ScheduledFromStatus)
	assert.Zero(t, task.Context.RepeatCount)
	assert.Zero(t, task.Context.RepeatRemaining)
	assert.Zero(t, task.Context.RepeatIntervalSec)
	assert.NoError(t, ts.Close())
}

func TestStartTaskImmediatelyClearsStaleRepeatFields(t *testing.T) {
	ts := newScheduledTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "start_clear_repeat", Name: "Start Clear Repeat"})
	require.NoError(t, err)
	task.ConfigureRepeat(3, 30)

	require.NoError(t, ts.StartTask(context.Background(), task.Config.ID))

	assert.Equal(t, taskEntity.TaskStatusRunning, task.Context.Status)
	assert.Zero(t, task.Context.RepeatCount)
	assert.Zero(t, task.Context.RepeatRemaining)
	assert.Zero(t, task.Context.RepeatIntervalSec)
	assert.NoError(t, ts.Close())
}

func TestCompleteTaskWithRepeatReschedulesUntilExhausted(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task, err := ts.CreateTask(taskEntity.TaskConfig{ID: "repeat_complete", Name: "Repeat Complete"})
	require.NoError(t, err)

	require.NoError(t, ts.ScheduleTaskWithRepeat(task.Config.ID, time.Now().Add(time.Minute), 2, 1))
	task.Start()
	ts.completeTask(task.Config.ID)

	assert.Equal(t, taskEntity.TaskStatusScheduled, task.Context.Status)
	require.NotNil(t, task.Context.ScheduledAt)
	assert.Equal(t, 2, task.Context.RepeatCount)
	assert.Equal(t, 1, task.Context.RepeatRemaining)
	assert.Equal(t, 1, task.Context.RepeatIntervalSec)

	task.Start()
	ts.completeTask(task.Config.ID)

	assert.Equal(t, taskEntity.TaskStatusCompleted, task.Context.Status)
	assert.Nil(t, task.Context.ScheduledAt)
	assert.Zero(t, task.Context.RepeatCount)
	assert.Zero(t, task.Context.RepeatRemaining)
	assert.Zero(t, task.Context.RepeatIntervalSec)
	assert.NoError(t, ts.Close())
}

func TestScheduleOperationsReturnStorageErrors(t *testing.T) {
	storageErr := fmt.Errorf("storage unavailable")
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"schedule_save_error": taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "schedule_save_error", Name: "Schedule Save Error"}),
		},
		runtimes: make(map[string]*taskRuntime),
		storage:  &failingTaskStorage{err: storageErr},
	}

	err := ts.ScheduleTask("schedule_save_error", time.Now().Add(time.Minute))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save scheduled task state")

	ts.tasks["schedule_save_error"] = taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "schedule_save_error", Name: "Schedule Save Error"})
	err = ts.ScheduleTaskWithRepeat("schedule_save_error", time.Now().Add(time.Minute), 2, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save")

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "cancel_save_error", Name: "Cancel Save Error"})
	task.Schedule(time.Now().Add(time.Minute))
	ts.tasks["cancel_save_error"] = task
	err = ts.CancelSchedule("cancel_save_error")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save cancelled schedule state")
}

// ==================== 阶段状态机：entity 助手方法 ====================

func TestSyncPhase_HelpersOnFreshTask(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_fresh"})
	assert.Equal(t, taskEntity.SyncPhaseInit, task.Context.SyncPhase)
	assert.False(t, task.HasFullSyncEverCompleted(), "fresh task must not be considered completed")
	assert.False(t, task.FullSyncIncomplete(), "fresh task is neither completed nor incomplete")
}

func TestSyncPhase_MarkFullSyncStartedThenCompleted(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_full"})

	task.MarkFullSyncStarted("mysql-bin.000001:120")
	assert.Equal(t, taskEntity.SyncPhaseFullStarted, task.Context.SyncPhase)
	assert.Equal(t, "mysql-bin.000001:120", task.Context.FullSyncStartPosition)
	require.NotNil(t, task.Context.FullSyncStartedAt)
	assert.Nil(t, task.Context.FullSyncCompletedAt)
	assert.True(t, task.FullSyncIncomplete(), "FULL_STARTED is incomplete")
	assert.False(t, task.HasFullSyncEverCompleted(), "FULL_STARTED is not completed")

	task.MarkFullSyncCompleted()
	assert.Equal(t, taskEntity.SyncPhaseFullCompleted, task.Context.SyncPhase)
	require.NotNil(t, task.Context.FullSyncCompletedAt)
	assert.True(t, task.HasFullSyncEverCompleted(), "after FULL_COMPLETED, must be ever-completed")
	assert.False(t, task.FullSyncIncomplete(), "FULL_COMPLETED is not incomplete anymore")
}

func TestSyncPhase_MarkFullSyncFailedKeepsIncompleteFlag(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_fail"})
	task.MarkFullSyncStarted("mysql-bin.000002:999")
	task.MarkFullSyncFailed("simulated DB error")

	assert.Equal(t, taskEntity.SyncPhaseFullFailed, task.Context.SyncPhase)
	assert.Equal(t, "simulated DB error", task.Context.FullSyncFailedReason)
	assert.True(t, task.FullSyncIncomplete(), "FULL_FAILED still counts as incomplete (must rerun)")
	assert.False(t, task.HasFullSyncEverCompleted(), "FULL_FAILED must not pretend to be completed")
}

func TestSyncPhase_MarkIncrementalStartedPreservesCompletion(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_incr"})
	task.MarkFullSyncStarted("mysql-bin.000003:1")
	task.MarkFullSyncCompleted()
	task.MarkIncrementalStarted()

	assert.Equal(t, taskEntity.SyncPhaseIncrementalStarted, task.Context.SyncPhase)
	assert.True(t, task.HasFullSyncEverCompleted(), "INCREMENTAL_STARTED also satisfies HasFullSyncEverCompleted")
	assert.False(t, task.FullSyncIncomplete())
}

func TestSyncPhase_UpdateIncrementalPosition(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_pos"})
	task.MarkFullSyncCompleted()
	task.MarkIncrementalStarted()

	task.UpdateIncrementalPosition("mysql-bin.000004:777")
	assert.Equal(t, "mysql-bin.000004:777", task.Context.LastIncrementalPosition)
	assert.Equal(t, taskEntity.SyncPhaseIncrementalStarted, task.Context.SyncPhase, "updating position should not change phase")
}

func TestSyncPhase_ResetWipesAllPhaseFields(t *testing.T) {
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "phase_reset"})
	task.MarkFullSyncStarted("mysql-bin.000005:1")
	task.MarkFullSyncCompleted()
	task.MarkIncrementalStarted()
	task.UpdateIncrementalPosition("mysql-bin.000005:888")

	task.ResetSyncPhase()
	assert.Equal(t, taskEntity.SyncPhaseInit, task.Context.SyncPhase)
	assert.Empty(t, task.Context.FullSyncStartPosition)
	assert.Empty(t, task.Context.LastIncrementalPosition)
	assert.Empty(t, task.Context.FullSyncFailedReason)
	assert.Nil(t, task.Context.FullSyncStartedAt)
	assert.Nil(t, task.Context.FullSyncCompletedAt)
	assert.False(t, task.HasFullSyncEverCompleted())
	assert.False(t, task.FullSyncIncomplete())
}

// ==================== 阶段状态机：service 持久化 helper ====================

func TestUpdateSyncPhase_PersistsAndIsNoopForUnknownTask(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "persist_phase", Name: "Persist Phase"})
	ts.tasks[task.Config.ID] = task

	ts.updateSyncPhase("persist_phase", func(tk *taskEntity.SyncTask) {
		tk.MarkFullSyncStarted("mysql-bin.000010:5")
	})

	assert.Equal(t, taskEntity.SyncPhaseFullStarted, task.Context.SyncPhase)
	assert.Equal(t, "mysql-bin.000010:5", task.Context.FullSyncStartPosition)

	assert.NotPanics(t, func() {
		ts.updateSyncPhase("does_not_exist", func(_ *taskEntity.SyncTask) {
			t.Fatal("mutator should not run for missing task")
		})
	})
}

// ==================== executeSync 入口门禁 + 暂停 bug ====================

// TestExecuteSync_IncrementalRequiresFullSync 覆盖修复 5：
// 纯 INCREMENTAL 模式下，若任务从未完成过全量，必须直接标 FAILED 而不是订阅 binlog。
func TestExecuteSync_IncrementalRequiresFullSync(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "incr_without_full",
		Name: "Incremental Without Full",
		Mode: taskEntity.SyncModeIncremental,
	})
	task.Start() // 状态 RUNNING，否则 failTaskUnlessCancelled 会忽略
	ts.tasks[task.Config.ID] = task

	ts.executeSync(context.Background(), task.Config.ID, &taskRuntime{})

	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status,
		"INCREMENTAL without prior FULL must fail-fast, not silently start a stale subscriber")
	assert.Contains(t, task.Context.ErrorStack, "requires a previously completed full sync")
}

// TestExecuteSync_IncrementalAllowedAfterFullCompleted 覆盖修复 5 的肯定路径：
// 一旦阶段为 FULL_COMPLETED，INCREMENTAL 必须放行进入 executeIncrementalSync。
// 这里用 nil runtime 触发 executeIncrementalSync 内部的早退保护，确认门禁本身已通过。
func TestExecuteSync_IncrementalAllowedAfterFullCompleted(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "incr_after_full",
		Name: "Incremental After Full",
		Mode: taskEntity.SyncModeIncremental,
	})
	task.Start()
	task.MarkFullSyncStarted("mysql-bin.000020:1")
	task.MarkFullSyncCompleted()
	ts.tasks[task.Config.ID] = task

	ts.executeSync(context.Background(), task.Config.ID, nil)

	// 门禁通过后进入 executeIncrementalSync，由于 runtime 为 nil 会把任务标 FAILED，
	// 但错误信息应来自"task runtime is nil"，证明门禁本身已放行。
	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
	assert.Contains(t, task.Context.ErrorStack, "runtime")
	assert.NotContains(t, task.Context.ErrorStack, "requires a previously completed full sync")
}

// TestExecuteSync_FullPauseDoesNotCompleteTask 覆盖暂停被错标 COMPLETED 的 P0 bug：
// 全量过程中用户暂停 → executeFullSync 入口的"早期停止检查"返回 errFullSyncStoppedByUser
// → executeSync 识别 sentinel 后既不能调用 completeTask 也不能把任务标 FAILED，
// 阶段也不能被推进到 FULL_COMPLETED。
//
// 这里复用 executeFullSync 入口的早停短路：不需要 mock SQL，只要 runtime 三件套非 nil
// 即可，校验通过后就会撞到 isTaskStopped 检查。
func TestExecuteSync_FullPauseDoesNotCompleteTask(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ts := newTestTaskService(t.TempDir())

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "full_paused",
		Name:         "Full Paused",
		Mode:         taskEntity.SyncModeFull,
		SourceSchema: "dummy",
	})
	task.Start()
	ts.tasks[task.Config.ID] = task

	// 模拟"用户在全量过程中按了暂停"
	task.Pause()

	ts.executeSync(context.Background(), task.Config.ID, &taskRuntime{
		sourceDB: db,
		targetDB: db,
		analyzer: &mockAnalyzer{},
	})

	assert.Equal(t, taskEntity.TaskStatusPaused, task.Context.Status,
		"paused full sync must NOT be marked COMPLETED (regression of pause-as-success bug)")
	assert.NotEqual(t, taskEntity.SyncPhaseFullCompleted, task.Context.SyncPhase,
		"paused full sync must NOT advance phase to FULL_COMPLETED")
	assert.NotEqual(t, taskEntity.SyncPhaseFullFailed, task.Context.SyncPhase,
		"paused full sync must NOT be flipped to FULL_FAILED either")
}

// TestErrFullSyncStoppedByUser_IsSentinel 守住 sentinel 的可识别性：
// errors.Is 必须对包装后的错误仍为 true，否则上层 switch 会把暂停误判为失败。
func TestErrFullSyncStoppedByUser_IsSentinel(t *testing.T) {
	wrapped := fmt.Errorf("syncDatabasePair: %w", errFullSyncStoppedByUser)
	assert.True(t, errors.Is(wrapped, errFullSyncStoppedByUser))

	other := fmt.Errorf("something else")
	assert.False(t, errors.Is(other, errFullSyncStoppedByUser))
}

// TestFormatBinlogPosition 覆盖小工具，避免空位点污染任务存档。
func TestFormatBinlogPosition(t *testing.T) {
	cases := []struct {
		name string
		in   mysqlPositionLike
		want string
	}{
		{"empty file returns empty", mysqlPositionLike{Name: "", Pos: 0}, ""},
		{"file+pos formatted", mysqlPositionLike{Name: "mysql-bin.000001", Pos: 4}, "mysql-bin.000001:4"},
		{"file with zero pos still formats", mysqlPositionLike{Name: "mysql-bin.000099", Pos: 0}, "mysql-bin.000099:0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := formatBinlogPosition(c.in.toMysql())
			assert.Equal(t, c.want, got)
		})
	}
}

// mysqlPositionLike 单测内联辅助：把测试参数转成 mysql.Position，避免在每个 case 内手写转换。
type mysqlPositionLike struct {
	Name string
	Pos  uint32
}

func (p mysqlPositionLike) toMysql() mysql.Position {
	return mysql.Position{Name: p.Name, Pos: p.Pos}
}

// ==================== 节流型位点回写 ====================

func TestThrottledPositionPersister_OnlyWritesAfterMinInterval(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "throttle"})
	ts.tasks[task.Config.ID] = task

	persist := ts.makeThrottledIncrementalPositionPersister(50 * time.Millisecond)

	persist("throttle", mysqlPositionLike{Name: "mysql-bin.000100", Pos: 1}.toMysql())
	assert.Equal(t, "mysql-bin.000100:1", task.Context.LastIncrementalPosition)

	// 立即再写一次（在 throttle 窗口内）应被丢弃，不更新存档
	persist("throttle", mysqlPositionLike{Name: "mysql-bin.000100", Pos: 2}.toMysql())
	assert.Equal(t, "mysql-bin.000100:1", task.Context.LastIncrementalPosition,
		"second call within throttle window must be dropped")

	// 等过节流窗口后再写应放行
	time.Sleep(80 * time.Millisecond)
	persist("throttle", mysqlPositionLike{Name: "mysql-bin.000100", Pos: 3}.toMysql())
	assert.Equal(t, "mysql-bin.000100:3", task.Context.LastIncrementalPosition)
}

func TestThrottledPositionPersister_IgnoresEmptyPosition(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{ID: "throttle_empty"})
	ts.tasks[task.Config.ID] = task

	persist := ts.makeThrottledIncrementalPositionPersister(time.Millisecond)
	persist("throttle_empty", mysqlPositionLike{Name: "", Pos: 0}.toMysql())
	assert.Empty(t, task.Context.LastIncrementalPosition, "empty position must not be written")
}

func TestStopSchedulerIsIdempotent(t *testing.T) {
	ts := newTestTaskService(t.TempDir())
	ts.StartScheduler()
	assert.NotPanics(t, func() {
		ts.StopScheduler()
		ts.StopScheduler()
	})
}
