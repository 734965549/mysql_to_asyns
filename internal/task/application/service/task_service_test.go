package service

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"mysql-to-async/internal/config"
	"mysql-to-async/internal/metadata/domain/entity"
	taskEntity "mysql-to-async/internal/task/domain/entity"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAnalyzer 是一个模拟的 IdentityAnalyzer
type mockAnalyzer struct{}

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

// newTestTaskService 创建一个使用自定义数据目录的测试任务服务
func newTestTaskService(dataDir string) *TaskService {
	storage := NewFileTaskStorage(dataDir)
	return &TaskService{
		tasks:   make(map[string]*taskEntity.SyncTask),
		storage: storage,
	}
}

func newDefaultConfig() *config.Config {
	return &config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: "data"},
	}
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

func TestCreateTask(t *testing.T) {
	dataDir := "./test_task_service_create"
	defer os.RemoveAll(dataDir)

	ts := NewTaskService(newDefaultConfig())

	taskConfig := taskEntity.TaskConfig{
		ID:           "test_task_1",
		Name:         "Test Task",
		SourceSchema: "source_db",
		TargetSchema: "target_db",
		Tables:       []string{"users", "orders"},
		Mode:         taskEntity.SyncModeFull,
	}

	task, err := ts.CreateTask(taskConfig)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, "test_task_1", task.Config.ID)

	// 验证任务已添加到服务中
	retrievedTask, exists := ts.GetTask("test_task_1")
	assert.True(t, exists)
	assert.Equal(t, task.Config.ID, retrievedTask.Config.ID)
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
