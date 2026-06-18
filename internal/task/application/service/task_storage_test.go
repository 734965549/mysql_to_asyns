package service

import (
	"fmt"
	"os"
	"testing"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
)

func TestTaskStorage_Save(t *testing.T) {
	dataDir := "./test_data"
	defer os.RemoveAll(dataDir)

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

	// 楠岃瘉鏂囦欢鏄惁瀛樺湪
	filePath := dataDir + "/test_task_1.json"
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("task file not created")
	}
}

func TestTaskStorage_Delete(t *testing.T) {
	dataDir := "./test_data"
	defer os.RemoveAll(dataDir)

	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:   "test_task_2",
		Name: "Test Task",
	})

	storage.Save(task)

	// 鍒犻櫎浠诲姟
	err := storage.Delete("test_task_2")
	if err != nil {
		t.Errorf("failed to delete task: %v", err)
	}

	// 楠岃瘉鏂囦欢宸插垹闄?
	filePath := dataDir + "/test_task_2.json"
	if _, err := os.Stat(filePath); err == nil {
		t.Error("task file still exists after deletion")
	}
}

func TestTaskStorage_DeleteNonExistent(t *testing.T) {
	dataDir := "./test_data"
	defer os.RemoveAll(dataDir)

	storage := NewFileTaskStorage(dataDir)

	// 鍒犻櫎涓嶅瓨鍦ㄧ殑浠诲姟涓嶅簲璇ユ姤閿?
	err := storage.Delete("non_existent_task")
	if err != nil {
		t.Errorf("deleting non-existent task should not error: %v", err)
	}
}

func TestTaskStorage_LoadAll(t *testing.T) {
	dataDir := "./test_data"
	defer os.RemoveAll(dataDir)

	storage := NewFileTaskStorage(dataDir)

	// 鍒涘缓澶氫釜浠诲姟
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

	// 鍔犺浇鎵€鏈変换鍔?
	tasks, err := storage.LoadAll()
	if err != nil {
		t.Errorf("failed to load tasks: %v", err)
	}

	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(tasks))
	}

	// 楠岃瘉浠诲姟鍐呭
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
	dataDir := "./test_data_empty"
	defer os.RemoveAll(dataDir)

	storage := NewFileTaskStorage(dataDir)

	// 绌虹洰褰曞簲璇ヨ繑鍥炵┖鍒楄〃
	tasks, err := storage.LoadAll()
	if err != nil {
		t.Errorf("failed to load tasks from empty directory: %v", err)
	}

	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(tasks))
	}
}

func TestTaskStorage_ConcurrentAccess(t *testing.T) {
	dataDir := "./test_data_concurrent"
	defer os.RemoveAll(dataDir)

	storage := NewFileTaskStorage(dataDir)

	// 骞跺彂鍐欏叆
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

	// 绛夊緟鎵€鏈塯oroutine瀹屾垚
	for i := 0; i < 10; i++ {
		<-done
	}

	// 楠岃瘉鎵€鏈変换鍔￠兘淇濆瓨鎴愬姛
	tasks, _ := storage.LoadAll()
	if len(tasks) != 10 {
		t.Errorf("expected 10 tasks, got %d", len(tasks))
	}
}

func TestTaskStorage_SaveAndUpdate(t *testing.T) {
	dataDir := "./test_data"
	defer os.RemoveAll(dataDir)

	storage := NewFileTaskStorage(dataDir)

	task := taskEntity.NewSyncTask(taskEntity.TaskConfig{
		ID:           "test_task_update",
		Name:         "Test Task",
		SourceSchema: "source_db",
		TargetSchema: "target_db",
	})

	// 淇濆瓨
	storage.Save(task)

	// 淇敼鐘舵€?
	task.Start()
	task.Context.TotalRows = 10000

	// 鍐嶆淇濆瓨
	storage.Save(task)

	// 鍔犺浇楠岃瘉
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


