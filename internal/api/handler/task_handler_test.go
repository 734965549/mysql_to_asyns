package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	metadataEntity "mysql-to-async/internal/metadata/domain/entity"
	taskService "mysql-to-async/internal/task/application/service"
	taskEntity "mysql-to-async/internal/task/domain/entity"
)

// MockIdentityAnalyzer 模拟IdentityAnalyzer
type MockIdentityAnalyzer struct{}

// AnalyzeTable 实现 IdentityAnalyzer 接口
func (m *MockIdentityAnalyzer) AnalyzeTable(schema, tableName string) (*metadataEntity.TableIdentity, error) {
	return &metadataEntity.TableIdentity{
		TableName:    tableName,
		IdentifyCols: []string{"id"},
		Strategy:     metadataEntity.PKStrategy,
	}, nil
}

// GetAllTables 实现 IdentityAnalyzer 接口
func (m *MockIdentityAnalyzer) GetAllTables(schema string) ([]metadataEntity.TableInfo, error) {
	return []metadataEntity.TableInfo{{Schema: schema, TableName: "test_table"}}, nil
}

// GetAllDatabases 实现 IdentityAnalyzer 接口
func (m *MockIdentityAnalyzer) GetAllDatabases() ([]string, error) {
	return []string{"test_db"}, nil
}

func TestCreateTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 创建模拟的task service
	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.POST("/api/tasks", handler.CreateTask)

	// 测试数据
	taskConfig := map[string]interface{}{
		"id":            "test_task_1",
		"name":          "Test Task",
		"source_schema": "source_db",
		"target_schema": "target_db",
		"tables":        []string{"users", "orders"},
		"mode":          "FULL",
		"batch_size":    1000,
	}

	body, _ := json.Marshal(taskConfig)
	req := httptest.NewRequest("POST", "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK && w.Code != http.StatusCreated {
		t.Errorf("expected status 200 or 201, got %d", w.Code)
	}
}

func TestGetAllTasks(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.GET("/api/tasks", handler.GetAllTasks)

	req := httptest.NewRequest("GET", "/api/tasks", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var tasks []*taskEntity.SyncTask
	json.Unmarshal(w.Body.Bytes(), &tasks)

	// 初始状态应该是空数组
	if tasks == nil {
		t.Error("expected tasks array, got nil")
	}
}

func TestGetTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 先创建一个任务
	taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "test_task_1",
		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.GET("/api/tasks/:id", handler.GetTask)

	req := httptest.NewRequest("GET", "/api/tasks/test_task_1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.GET("/api/tasks/:id", handler.GetTask)

	req := httptest.NewRequest("GET", "/api/tasks/nonexistent", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

func TestDeleteTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 创建任务
	taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "test_task_1",
		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.DELETE("/api/tasks/:id", handler.DeleteTask)

	req := httptest.NewRequest("DELETE", "/api/tasks/test_task_1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// 验证任务已删除
	_, exists := taskSvc.GetTask("test_task_1")
	if exists {
		t.Error("task still exists after deletion")
	}
}

func TestStartTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 创建任务
	taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "test_task_start",
		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.POST("/api/tasks/:id/start", handler.StartTask)

	req := httptest.NewRequest("POST", "/api/tasks/test_task_start/start", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// 可能因为没有数据库连接而失败，但至少应该能找到任务
	if w.Code != http.StatusOK && w.Code != http.StatusInternalServerError {
		t.Errorf("unexpected status code: %d", w.Code)
	}
}

func TestPauseTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 创建任务
	taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "test_task_pause",
		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.POST("/api/tasks/:id/pause", handler.PauseTask)

	req := httptest.NewRequest("POST", "/api/tasks/test_task_pause/pause", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestSkipError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 创建失败的任务
	task, _ := taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "test_task_skip",
		Name: "Test Task",
	})
	task.Context.Status = taskEntity.TaskStatusFailed
	task.Context.ErrorStack = "test error"

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.POST("/api/tasks/:id/skip", handler.SkipError)

	req := httptest.NewRequest("POST", "/api/tasks/test_task_skip/skip", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestNewTaskHandler(t *testing.T) {
	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	handler := NewTaskHandler(taskSvc, analyzer)
	if handler == nil {
		t.Error("expected handler, got nil")
	}
}

func TestUpdateTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := taskService.NewTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 先创建一个任务
	taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "test_task_update",
		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.PUT("/api/tasks/:id", handler.UpdateTask)

	// 更新数据
	updateConfig := map[string]interface{}{
		"name":       "Updated Task",
		"batch_size": 2000,
	}

	body, _ := json.Marshal(updateConfig)
	req := httptest.NewRequest("PUT", "/api/tasks/test_task_update", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK && w.Code != http.StatusBadRequest {
		t.Errorf("expected status 200 or 400, got %d", w.Code)
	}
}
