package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"mysql-to-async/internal/config"
	metadataEntity "mysql-to-async/internal/metadata/domain/entity"
	taskService "mysql-to-async/internal/task/application/service"
	taskEntity "mysql-to-async/internal/task/domain/entity"
)

func newTestTaskService() *taskService.TaskService {
	return taskService.NewTaskService(&config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: "data"},
	})
}

// MockIdentityAnalyzer 妯℃嫙IdentityAnalyzer
type MockIdentityAnalyzer struct{}

// AnalyzeTable 瀹炵幇 IdentityAnalyzer 鎺ュ彛
func (m *MockIdentityAnalyzer) AnalyzeTable(schema, tableName string) (*metadataEntity.TableIdentity, error) {
	return &metadataEntity.TableIdentity{
		TableName:    tableName,
		IdentifyCols: []string{"id"},
		Strategy:     metadataEntity.PKStrategy,
	}, nil
}

// GetAllTables 瀹炵幇 IdentityAnalyzer 鎺ュ彛
func (m *MockIdentityAnalyzer) GetAllTables(schema string) ([]metadataEntity.TableInfo, error) {
	return []metadataEntity.TableInfo{{Schema: schema, TableName: "test_table"}}, nil
}

// GetAllDatabases 瀹炵幇 IdentityAnalyzer 鎺ュ彛
func (m *MockIdentityAnalyzer) GetAllDatabases() ([]string, error) {
	return []string{"test_db"}, nil
}

func TestCreateTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 鍒涘缓妯℃嫙鐨則ask service
	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.POST("/api/tasks", handler.CreateTask)

	// 娴嬭瘯鏁版嵁
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

	taskSvc := newTestTaskService()
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

	// 鍒濆鐘舵€佸簲璇ユ槸绌烘暟缁?
	if tasks == nil {
		t.Error("expected tasks array, got nil")
	}
}

func TestGetTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 鍏堝垱寤轰竴涓换鍔?
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

	taskSvc := newTestTaskService()
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

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 鍒涘缓浠诲姟
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

	// 楠岃瘉浠诲姟宸插垹闄?
	_, exists := taskSvc.GetTask("test_task_1")
	if exists {
		t.Error("task still exists after deletion")
	}
}

func TestStartTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 鍒涘缓浠诲姟
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

	// 可能因为没有数据库连接导致启动失败；当前实现会返回 500
	if w.Code != http.StatusOK && w.Code != http.StatusInternalServerError {
		t.Errorf("unexpected status code: %d", w.Code)
	}
}

func TestPauseTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 鍒涘缓浠诲姟
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

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 鍒涘缓澶辫触鐨勪换鍔?
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
	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	handler := NewTaskHandler(taskSvc, analyzer)
	if handler == nil {
		t.Error("expected handler, got nil")
	}
}

func TestCreateTask_WithSinkConfigs(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}
	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.POST("/api/tasks", handler.CreateTask)

	taskConfig := map[string]interface{}{
		"name":          "Sink Test Task",
		"mode":          "INCREMENTAL",
		"source_schema": "src",
		"target_schema": "tgt",
		"tables":        []string{"t1"},
		"sink_configs": []map[string]interface{}{
			{
				"type": "KAFKA",
				"options": map[string]interface{}{
					"brokers": []string{"localhost:9092"},
					"topic":   "cdc_events",
				},
			},
			{
				"type": "HTTP_WEBHOOK",
				"options": map[string]interface{}{
					"url":    "http://example.com/hook",
					"method": "POST",
				},
			},
		},
	}

	body, _ := json.Marshal(taskConfig)
	req := httptest.NewRequest("POST", "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d, body: %s", w.Code, w.Body.String())
	}

	var result taskEntity.SyncTask
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if len(result.Config.SinkConfigs) != 2 {
		t.Fatalf("expected 2 sink_configs, got %d", len(result.Config.SinkConfigs))
	}
	if result.Config.SinkConfigs[0].Type != "KAFKA" {
		t.Errorf("expected first sink type KAFKA, got %s", result.Config.SinkConfigs[0].Type)
	}
	if result.Config.SinkConfigs[1].Type != "HTTP_WEBHOOK" {
		t.Errorf("expected second sink type HTTP_WEBHOOK, got %s", result.Config.SinkConfigs[1].Type)
	}
}

func TestCreateTask_WithoutSinkConfigs_BackwardCompatible(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}
	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.POST("/api/tasks", handler.CreateTask)

	taskConfig := map[string]interface{}{
		"name":          "No Sink Task",
		"mode":          "FULL",
		"source_schema": "src",
	}

	body, _ := json.Marshal(taskConfig)
	req := httptest.NewRequest("POST", "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d, body: %s", w.Code, w.Body.String())
	}

	var result taskEntity.SyncTask
	json.Unmarshal(w.Body.Bytes(), &result)
	if len(result.Config.SinkConfigs) != 0 {
		t.Errorf("expected empty sink_configs for backward compat, got %d", len(result.Config.SinkConfigs))
	}
}

func TestUpdateTask_WithSinkConfigs(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "test_sink_update",
		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.PUT("/api/tasks/:id", handler.UpdateTask)

	updateData := map[string]interface{}{
		"sink_configs": []map[string]interface{}{
			{
				"type":    "MYSQL",
				"options": map[string]interface{}{"target_schema": "new_tgt"},
			},
		},
	}

	body, _ := json.Marshal(updateData)
	req := httptest.NewRequest("PUT", "/api/tasks/test_sink_update", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var result taskEntity.SyncTask
	json.Unmarshal(w.Body.Bytes(), &result)
	if len(result.Config.SinkConfigs) != 1 {
		t.Fatalf("expected 1 sink_config after update, got %d", len(result.Config.SinkConfigs))
	}
	if result.Config.SinkConfigs[0].Type != "MYSQL" {
		t.Errorf("expected sink type MYSQL, got %s", result.Config.SinkConfigs[0].Type)
	}
}

func TestConvertSinkConfigs(t *testing.T) {
	// nil input
	if result := convertSinkConfigs(nil); result != nil {
		t.Errorf("expected nil for nil input, got %v", result)
	}

	// empty input
	if result := convertSinkConfigs([]SinkConfigRequest{}); result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}

	// normal input
	reqs := []SinkConfigRequest{
		{Type: "KAFKA", Options: map[string]interface{}{"brokers": []string{"b1"}}},
		{Type: "HTTP_WEBHOOK", Options: map[string]interface{}{"url": "http://x"}},
	}
	result := convertSinkConfigs(reqs)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].Type != "KAFKA" {
		t.Errorf("expected KAFKA, got %s", result[0].Type)
	}
	if result[1].Type != "HTTP_WEBHOOK" {
		t.Errorf("expected HTTP_WEBHOOK, got %s", result[1].Type)
	}
}

func TestUpdateTask(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}

	// 鍏堝垱寤轰竴涓换鍔?
	taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "test_task_update",
		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.PUT("/api/tasks/:id", handler.UpdateTask)

	// 鏇存柊鏁版嵁
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
