package service

import (
	"fmt"
	"testing"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
)

func TestTaskStorage_Save(t *testing.T) {
	dataDir := t.TempDir()

	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "test_task_1",
		Name:         "Test Task",
		SourceSchema: "source_db",
		TargetSchema: "target_db",
		Tables:       []string{"users", "orders"},
		Mode:         taskEntity.SyncModeFull,
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
