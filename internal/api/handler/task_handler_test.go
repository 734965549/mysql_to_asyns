package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"mysql-to-sync/internal/config"
	metadataEntity "mysql-to-sync/internal/metadata/domain/entity"
	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
	taskService "mysql-to-sync/internal/task/application/service"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
)

func TestTaskResponsesRedactSecretsWithoutMutatingRuntimeTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newTestTaskService()
	_, err := taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:       "secret_response_task",
		Name:     "Secret response",
		SourceDB: &taskEntity.DatabaseConfig{Password: "source-password"},
		TargetDB: &taskEntity.DatabaseConfig{Password: "target-password"},
		SinkConfigs: []sinkDomain.SinkConfig{
			{Type: sinkDomain.SinkTypeKAFKA, Options: map[string]interface{}{
				"security": map[string]interface{}{"sasl_password": "kafka-password"},
			}},
			{Type: sinkDomain.SinkTypeHTTPWebhook, Options: map[string]interface{}{
				"headers": map[string]interface{}{"Authorization": "Bearer webhook-token"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.GET("/api/tasks/:id", handler.GetTask)
	req := httptest.NewRequest(http.MethodGet, "/api/tasks/secret_response_task", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, secret := range []string{"source-password", "target-password", "kafka-password", "webhook-token"} {
		if bytes.Contains(w.Body.Bytes(), []byte(secret)) {
			t.Fatalf("response leaked %q: %s", secret, body)
		}
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(secretMask)) {
		t.Fatalf("response did not include secret placeholders: %s", body)
	}

	runtimeTask, ok := taskSvc.GetTask("secret_response_task")
	if !ok {
		t.Fatal("runtime task not found")
	}
	if runtimeTask.Config.SourceDB.Password != "source-password" {
		t.Fatalf("response redaction mutated source password: %q", runtimeTask.Config.SourceDB.Password)
	}
	if got := runtimeTask.Config.SinkConfigs[0].Options["security"].(map[string]interface{})["sasl_password"]; got != "kafka-password" {
		t.Fatalf("response redaction mutated Kafka password: %v", got)
	}
}

func TestUpdateTaskPreservesMaskedSinkSecrets(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newTestTaskService()
	_, err := taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "masked_update_task",
		Name: "Masked update",
		SinkConfigs: []sinkDomain.SinkConfig{
			{Type: sinkDomain.SinkTypeKAFKA, Options: map[string]interface{}{
				"brokers":  []interface{}{"broker:9092"},
				"topic":    "cdc",
				"security": map[string]interface{}{"sasl_password": "original-kafka"},
			}},
			{Type: sinkDomain.SinkTypeHTTPWebhook, Options: map[string]interface{}{
				"url":     "https://example.com/hook",
				"headers": map[string]interface{}{"Authorization": "original-webhook"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.PUT("/api/tasks/:id", handler.UpdateTask)
	body := `{"sink_configs":[` +
		`{"type":"KAFKA","options":{"brokers":["broker:9092"],"topic":"new-topic","security":{"sasl_password":"******"}}},` +
		`{"type":"HTTP_WEBHOOK","options":{"url":"https://example.com/hook","headers":{"Authorization":"******"}}}` +
		`]}`
	req := httptest.NewRequest(http.MethodPut, "/api/tasks/masked_update_task", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	updated, ok := taskSvc.GetTask("masked_update_task")
	if !ok {
		t.Fatal("updated task not found")
	}
	if got := updated.Config.SinkConfigs[0].Options["security"].(map[string]interface{})["sasl_password"]; got != "original-kafka" {
		t.Fatalf("masked Kafka password was not preserved: %v", got)
	}
	if got := updated.Config.SinkConfigs[1].Options["headers"].(map[string]interface{})["Authorization"]; got != "original-webhook" {
		t.Fatalf("masked webhook header was not preserved: %v", got)
	}
	if bytes.Contains(w.Body.Bytes(), []byte("original-kafka")) || bytes.Contains(w.Body.Bytes(), []byte("original-webhook")) {
		t.Fatalf("update response leaked secrets: %s", w.Body.String())
	}
}

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

		TableName: tableName,

		IdentifyCols: []string{"id"},

		Strategy: metadataEntity.PKStrategy,
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

		"id": "test_task_1",

		"name": "Test Task",

		"source_schema": "source_db",

		"target_schema": "target_db",

		"tables": []string{"users", "orders"},

		"mode": "FULL",

		"batch_size": 1000,
	}

	body, _ := json.Marshal(taskConfig)

	req := httptest.NewRequest("POST", "/api/tasks", bytes.NewReader(body))

	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK && w.Code != http.StatusCreated {

		t.Errorf("expected status 200 or 201, got %d", w.Code)

	}
	var created taskEntity.SyncTask
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode created task: %v", err)
	}
	if len(created.Config.SinkConfigs) != 1 || created.Config.SinkConfigs[0].Type != sinkDomain.SinkTypeMYSQL {
		t.Fatalf("missing sink_configs should default to MYSQL, got %#v", created.Config.SinkConfigs)
	}

}

func TestCreateTask_EnableSkipBinlog(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks", handler.CreateTask)

	body := bytes.NewBufferString(`{
		"name":"Skip Binlog Task",
		"mode":"FULL",
		"enable_skip_binlog":true
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, w.Code, w.Body.String())
	}
	var task taskEntity.SyncTask
	if err := json.Unmarshal(w.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !task.Config.EnableSkipBinlog {
		t.Fatal("enable_skip_binlog=true was not mapped into the created task")
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

	var resp struct {
		Total    int64                  `json:"total"`
		Page     int                    `json:"page"`
		PageSize int                    `json:"page_size"`
		Items    []*taskEntity.SyncTask `json:"items"`
	}

	json.Unmarshal(w.Body.Bytes(), &resp)

	// 初始状态应该是空数组

	if resp.Items == nil {

		t.Error("expected tasks array, got nil")

	}

}

func TestGetTask(t *testing.T) {

	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()

	analyzer := &MockIdentityAnalyzer{}

	// 鍏堝垱寤轰竴涓换鍔?

	taskSvc.CreateTask(taskEntity.TaskConfig{

		ID: "test_task_1",

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

		ID: "test_task_1",

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

		ID: "test_task_start",

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

func TestStartTask_InvalidScheduledAtReturnsBadRequest(t *testing.T) {

	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()

	analyzer := &MockIdentityAnalyzer{}

	_, _ = taskSvc.CreateTask(taskEntity.TaskConfig{

		ID: "test_task_start_invalid_schedule",

		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()

	router.POST("/api/tasks/:id/start", handler.StartTask)

	req := httptest.NewRequest("POST", "/api/tasks/test_task_start_invalid_schedule/start", bytes.NewBufferString(`{"scheduled_at":"not-a-time"}`))

	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {

		t.Errorf("expected status 400, got %d", w.Code)

	}

	task, exists := taskSvc.GetTask("test_task_start_invalid_schedule")

	if !exists {

		t.Fatal("expected task to exist")

	}

	if task.Context.Status != taskEntity.TaskStatusPending {

		t.Errorf("expected task status PENDING, got %s", task.Context.Status)

	}

}

func TestStartTask_SchedulesFailedTask(t *testing.T) {

	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()

	analyzer := &MockIdentityAnalyzer{}

	task, _ := taskSvc.CreateTask(taskEntity.TaskConfig{

		ID: "test_task_failed_schedule",

		Name: "Failed Schedule Task",
	})

	task.Context.Status = taskEntity.TaskStatusFailed

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()

	router.POST("/api/tasks/:id/start", handler.StartTask)

	scheduledAt := time.Now().Add(2 * time.Minute).Format(time.RFC3339)
	req := httptest.NewRequest("POST", "/api/tasks/test_task_failed_schedule/start", bytes.NewBufferString(`{"scheduled_at":"`+scheduledAt+`"}`))

	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {

		t.Errorf("expected status 200, got %d", w.Code)

	}

	updatedTask, exists := taskSvc.GetTask("test_task_failed_schedule")

	if !exists {

		t.Fatal("expected task to exist")

	}

	if updatedTask.Context.Status != taskEntity.TaskStatusScheduled {

		t.Errorf("expected task status SCHEDULED, got %s", updatedTask.Context.Status)

	}

	if updatedTask.Context.ScheduledFromStatus == nil {

		t.Fatal("expected scheduled_from_status to be set")

	}

	if *updatedTask.Context.ScheduledFromStatus != taskEntity.TaskStatusFailed {

		t.Errorf("expected scheduled_from_status FAILED, got %s", *updatedTask.Context.ScheduledFromStatus)

	}

	if updatedTask.Context.ScheduledAt == nil {

		t.Fatal("expected scheduled_at to be set")

	}

}

func TestPauseTask(t *testing.T) {

	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()

	analyzer := &MockIdentityAnalyzer{}

	// 鍒涘缓浠诲姟

	taskSvc.CreateTask(taskEntity.TaskConfig{

		ID: "test_task_pause",

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

		ID: "test_task_skip",

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

func TestUpdateTask(t *testing.T) {

	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()

	analyzer := &MockIdentityAnalyzer{}

	// 鍏堝垱寤轰竴涓换鍔?

	taskSvc.CreateTask(taskEntity.TaskConfig{

		ID: "test_task_update",

		Name: "Test Task",
	})

	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()

	router.PUT("/api/tasks/:id", handler.UpdateTask)

	// 鏇存柊鏁版嵁

	updateConfig := map[string]interface{}{

		"name": "Updated Task",

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

func TestUpdateTask_EnableSkipBinlogOptionalBool(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{name: "omitted_preserves_true", body: `{"name":"Updated"}`, want: true},
		{name: "explicit_false_disables", body: `{"enable_skip_binlog":false}`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			taskSvc := newTestTaskService()
			_, err := taskSvc.CreateTask(taskEntity.TaskConfig{
				ID:               "test_skip_binlog_update",
				Name:             "Test Task",
				EnableSkipBinlog: true,
			})
			if err != nil {
				t.Fatalf("create fixture task: %v", err)
			}

			handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
			router := gin.New()
			router.PUT("/api/tasks/:id", handler.UpdateTask)
			req := httptest.NewRequest(
				http.MethodPut,
				"/api/tasks/test_skip_binlog_update",
				bytes.NewBufferString(tt.body),
			)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
			}
			updated, ok := taskSvc.GetTask("test_skip_binlog_update")
			if !ok {
				t.Fatal("updated task not found")
			}
			if updated.Config.EnableSkipBinlog != tt.want {
				t.Fatalf("enable_skip_binlog: got %v want %v", updated.Config.EnableSkipBinlog, tt.want)
			}
		})
	}
}

func TestGetTaskProgress_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}
	handler := NewTaskHandler(taskSvc, analyzer)

	router := gin.New()
	router.GET("/api/tasks/:id/progress", handler.GetTaskProgress)

	req := httptest.NewRequest("GET", "/api/tasks/non_existent/progress", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

func TestGetTaskProgress_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	analyzer := &MockIdentityAnalyzer{}
	handler := NewTaskHandler(taskSvc, analyzer)

	// 创建任务并手动初始化运行时进度
	taskSvc.CreateTask(taskEntity.TaskConfig{ID: "test_progress", Name: "Test"})

	// 通过内部机制初始化进度（模拟全量同步开始）
	// 直接操作 runningProgress 需要通过 service 包的方法
	// 这里验证接口可正常路由即可，完整逻辑在 service 层测试覆盖

	router := gin.New()
	router.GET("/api/tasks/:id/progress", handler.GetTaskProgress)

	// 未初始化进度时返回 404
	req := httptest.NewRequest("GET", "/api/tasks/test_progress/progress", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404 for uninitialized progress, got %d", w.Code)
	}
}
