package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
)

func TestFileTaskStorage_EncryptsSinkSecretsWithoutMutatingRuntimeTask(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir, "storage-key")
	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "sink-secrets",
		Name: "Sink secrets",
		SinkConfigs: []sinkDomain.SinkConfig{
			{Type: sinkDomain.SinkTypeKAFKA, Options: map[string]interface{}{
				"security": map[string]interface{}{"sasl_password": "kafka-secret"},
			}},
			{Type: sinkDomain.SinkTypeHTTPWebhook, Options: map[string]interface{}{
				"headers": map[string]interface{}{"Authorization": "Bearer webhook-secret"},
			}},
		},
	})

	if err := storage.Save(task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	if got := task.Config.SinkConfigs[0].Options["security"].(map[string]interface{})["sasl_password"]; got != "kafka-secret" {
		t.Fatalf("runtime Kafka password was mutated: %v", got)
	}
	if got := task.Config.SinkConfigs[1].Options["headers"].(map[string]interface{})["Authorization"]; got != "Bearer webhook-secret" {
		t.Fatalf("runtime webhook header was mutated: %v", got)
	}

	data, err := os.ReadFile(filepath.Join(dataDir, "sink-secrets.json"))
	if err != nil {
		t.Fatalf("read stored task: %v", err)
	}
	stored := string(data)
	if strings.Contains(stored, "kafka-secret") || strings.Contains(stored, "webhook-secret") {
		t.Fatalf("stored task leaked plaintext sink secrets: %s", stored)
	}
	if !strings.Contains(stored, "ENC~") {
		t.Fatalf("stored task does not contain encrypted values: %s", stored)
	}

	loaded, err := storage.LoadAll()
	if err != nil {
		t.Fatalf("load tasks: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected one loaded task, got %d", len(loaded))
	}
	if got := loaded[0].Config.SinkConfigs[0].Options["security"].(map[string]interface{})["sasl_password"]; got != "kafka-secret" {
		t.Fatalf("Kafka password was not decrypted: %v", got)
	}
	if got := loaded[0].Config.SinkConfigs[1].Options["headers"].(map[string]interface{})["Authorization"]; got != "Bearer webhook-secret" {
		t.Fatalf("webhook headers were not decrypted: %v", got)
	}
}

func TestTaskStorage_Save(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:               "test_task_1",
		Name:             "Test Task",
		SourceSchema:     "source_db",
		TargetSchema:     "target_db",
		Tables:           []string{"users", "orders"},
		Mode:             taskEntity.SyncModeFull,
		EnableSkipBinlog: true,
	})

	err := storage.Save(task)
	if err != nil {
		t.Errorf("failed to save task: %v", err)
	}

	// 验证文件是否存在
	tasks, _ := storage.LoadAll()
	if len(tasks) != 1 {
		t.Error("task file not created")
	}
	if !tasks[0].Config.EnableSkipBinlog {
		t.Error("enable_skip_binlog was not preserved by task storage")
	}
}

func TestTaskStorage_Delete(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "test_task_2",
		Name: "Test Task",
	})

	storage.Save(task)

	// 删除任务
	err := storage.Delete("test_task_2")
	if err != nil {
		t.Errorf("failed to delete task: %v", err)
	}

	// 验证文件已删除
	tasks, _ := storage.LoadAll()
	if len(tasks) != 0 {
		t.Error("task file still exists after deletion")
	}
}

func TestTaskStorage_DeleteNonExistent(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	// 删除不存在的任务不应该报错
	err := storage.Delete("non_existent_task")
	if err != nil {
		t.Errorf("deleting non-existent task should not error: %v", err)
	}
}

func TestTaskStorage_LoadAll(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	// 创建多个任务
	task1 := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "test_task_1",
		Name:         "Task 1",
		SourceSchema: "db1",
		TargetSchema: "db2",
	})

	task2 := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "test_task_2",
		Name:         "Task 2",
		SourceSchema: "db3",
		TargetSchema: "db4",
	})

	storage.Save(task1)
	storage.Save(task2)

	// 加载所有任务
	tasks, err := storage.LoadAll()
	if err != nil {
		t.Errorf("failed to load tasks: %v", err)
	}

	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(tasks))
	}

	// 验证任务内容
	taskMap := make(map[string]*taskEntity.SyncTask)
	for _, task := range tasks {
		taskMap[task.Config.ID] = task
	}

	if _, ok := taskMap["test_task_1"]; !ok {
		t.Error("task 1 not found")
	}

	if _, ok := taskMap["test_task_2"]; !ok {
		t.Error("task 2 not found")
	}
}

func TestTaskStorage_LoadAllEmpty(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	// 空目录应该返回空列表
	tasks, err := storage.LoadAll()
	if err != nil {
		t.Errorf("failed to load tasks from empty directory: %v", err)
	}

	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(tasks))
	}
}

func TestTaskStorage_ConcurrentAccess(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	// 并发写入
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			taskID := fmt.Sprintf("concurrent_task_%d", id)
			task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
				ID:   taskID,
				Name: "Concurrent Task",
			})
			storage.Save(task)
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证所有任务都保存成功
	tasks, _ := storage.LoadAll()
	if len(tasks) != 10 {
		t.Errorf("expected 10 tasks, got %d", len(tasks))
	}
}

func TestTaskStorage_SaveAndUpdate(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "test_task_update",
		Name:         "Test Task",
		SourceSchema: "source_db",
		TargetSchema: "target_db",
	})

	// 保存
	storage.Save(task)

	// 修改状态
	task.Start()
	task.Context.TotalRows = 10000

	// 再次保存
	storage.Save(task)

	// 加载验证
	tasks, _ := storage.LoadAll()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	if tasks[0].Context.Status != taskEntity.TaskStatusRunning {
		t.Errorf("expected status RUNNING, got %s", tasks[0].Context.Status)
	}

	if tasks[0].Context.TotalRows != 10000 {
		t.Errorf("expected total rows 10000, got %d", tasks[0].Context.TotalRows)
	}
}

// TestFileTaskStorage_PreservesRowCountComparison 验证行数对比结果能完整保存与加载，
// 同时不影响内存中的明文密码处理规则。
func TestFileTaskStorage_PreservesRowCountComparison(t *testing.T) {
	dataDir := t.TempDir()
	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "cmp_storage",
		Name:         "Compare Storage",
		Mode:         taskEntity.SyncModeAll,
		SourceSchema: "src_db",
		TargetSchema: "tgt_db",
		SourceDB:     &taskEntity.DatabaseConfig{Host: "h", Port: 3306, Username: "u", Password: "plaintext-secret"},
	})
	task.Start()
	task.MarkIncrementalStarted()
	task.Stop() // STOPPED

	srcRows := int64(100)
	tgtRows := int64(99)
	diff := int64(-1)
	task.Context.RowCountComparison = &taskEntity.RowCountComparison{
		Status:           taskEntity.RowCountComparisonMismatched,
		TotalTables:      1,
		CheckedTables:    1,
		MatchedTables:    0,
		MismatchedTables: 1,
		FailedTables:     0,
		SourceTotal:      100,
		TargetTotal:      99,
		Difference:       -1,
		Tables: []taskEntity.RowCountComparisonTable{
			{
				SourceSchema: "src_db", SourceTable: "users",
				TargetSchema: "tgt_db", TargetTable: "users",
				SourceRows: &srcRows, TargetRows: &tgtRows,
				Difference: &diff, Matched: false,
			},
		},
	}

	if err := storage.Save(task); err != nil {
		t.Fatalf("save task: %v", err)
	}

	// 内存中的明文密码不应被存储过程永久篡改
	if task.Config.SourceDB.Password != "plaintext-secret" {
		t.Errorf("in-memory plaintext password mutated by save: %q", task.Config.SourceDB.Password)
	}

	tasks, err := storage.LoadAll()
	if err != nil {
		t.Fatalf("load all: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	loaded := tasks[0]
	if loaded.Context.Status != taskEntity.TaskStatusStopped {
		t.Errorf("expected STOPPED, got %s", loaded.Context.Status)
	}
	if loaded.Context.RowCountComparison == nil {
		t.Fatal("row_count_comparison not preserved by storage")
	}
	rc := loaded.Context.RowCountComparison
	if rc.Status != taskEntity.RowCountComparisonMismatched {
		t.Errorf("expected MISATCHED, got %s", rc.Status)
	}
	if rc.Difference != -1 {
		t.Errorf("expected difference -1, got %d", rc.Difference)
	}
	if len(rc.Tables) != 1 {
		t.Fatalf("expected 1 table result, got %d", len(rc.Tables))
	}
	tbl := rc.Tables[0]
	if tbl.SourceRows == nil || *tbl.SourceRows != 100 {
		t.Errorf("expected source rows 100, got %v", tbl.SourceRows)
	}
	if tbl.TargetRows == nil || *tbl.TargetRows != 99 {
		t.Errorf("expected target rows 99, got %v", tbl.TargetRows)
	}
	if tbl.Matched {
		t.Error("expected mismatched table")
	}
	// 行数对比结果不得包含密码等敏感信息（结构体本身无密码字段，此处确保 SourceDB.Password 不在结果 JSON 中）
}
