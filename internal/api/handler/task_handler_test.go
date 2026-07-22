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

func TestCreateTaskPreservesSecretsWhenCloning(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newTestTaskService()
	sourceTask, err := taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:   "clone_source_task",
		Name: "Clone source",
		Mode: taskEntity.SyncModeFull,
		SourceDB: &taskEntity.DatabaseConfig{
			Host:     "source-host",
			Port:     3306,
			Database: "source_db",
			Username: "source-user",
			Password: "source-password",
		},
		TargetDB: &taskEntity.DatabaseConfig{
			Host:     "target-host",
			Port:     3306,
			Database: "target_db",
			Username: "target-user",
			Password: "target-password",
		},
	})
	if err != nil {
		t.Fatalf("create source task: %v", err)
	}

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks", handler.CreateTask)
	body := `{
		"name":"Cloned task",
		"mode":"FULL",
		"clone_from_task_id":"` + sourceTask.Config.ID + `",
		"source_db":{"host":"source-host","port":3306,"database":"source_db","username":"source-user","password":"******"},
		"target_db":{"host":"target-host","port":3306,"database":"target_db","username":"target-user","password":"******"}
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var created taskEntity.SyncTask
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if created.Config.SourceDB == nil || created.Config.SourceDB.Password != secretMask {
		t.Fatalf("response should redact source password, got %v", created.Config.SourceDB)
	}
	if created.Config.TargetDB == nil || created.Config.TargetDB.Password != secretMask {
		t.Fatalf("response should redact target password, got %v", created.Config.TargetDB)
	}

	cloned, ok := taskSvc.GetTask(created.Config.ID)
	if !ok {
		t.Fatal("cloned task not found")
	}
	if cloned.Config.SourceDB.Password != "source-password" {
		t.Fatalf("cloned source password not preserved: %q", cloned.Config.SourceDB.Password)
	}
	if cloned.Config.TargetDB.Password != "target-password" {
		t.Fatalf("cloned target password not preserved: %q", cloned.Config.TargetDB.Password)
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

func TestCreateTask_FullLoadV2Fields(t *testing.T) {
	gin.SetMode(gin.TestMode)

	taskSvc := newTestTaskService()
	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks", handler.CreateTask)

	body := bytes.NewBufferString(`{
		"name":"V2 Task",
		"mode":"FULL",
		"full_load_engine":"v2",
		"full_load_read_workers":8,
		"full_load_write_workers":6,
		"full_load_buffer_mb":256,
		"full_load_batch_bytes_mb":8,
		"full_load_commit_rows":20000,
		"full_load_commit_bytes_mb":64,
		"index_restore_worker_count":3
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
	cfg := task.Config
	if !cfg.UsesFullLoadV2() {
		t.Fatalf("full_load_engine not mapped: %q", cfg.FullLoadEngine)
	}
	if cfg.FullLoadReadWorkers != 8 || cfg.FullLoadWriteWorkers != 6 {
		t.Fatalf("workers not mapped: %d/%d", cfg.FullLoadReadWorkers, cfg.FullLoadWriteWorkers)
	}
	if cfg.FullLoadBufferMB != 256 || cfg.FullLoadBatchBytesMB != 8 {
		t.Fatalf("buffer/batch bytes not mapped: %d/%d", cfg.FullLoadBufferMB, cfg.FullLoadBatchBytesMB)
	}
	if cfg.FullLoadCommitRows != 20000 || cfg.FullLoadCommitBytesMB != 64 {
		t.Fatalf("commit fields not mapped: %d/%d", cfg.FullLoadCommitRows, cfg.FullLoadCommitBytesMB)
	}
	if cfg.IndexRestoreWorkerCount != 3 {
		t.Fatalf("index_restore_worker_count not mapped: %d", cfg.IndexRestoreWorkerCount)
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

// newHandlerTaskService 为 handler 测试创建独立的临时文件存储任务服务，避免污染 data/ 目录。
func newHandlerTaskService(t *testing.T) *taskService.TaskService {
	t.Helper()
	return taskService.NewTaskService(&config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()},
	})
}

// stageAllTaskAtIncremental 在服务中放入一个 RUNNING + ALL + INCREMENTAL_STARTED 的任务。
func stageAllTaskAtIncremental(t *testing.T, taskSvc *taskService.TaskService, taskID string) {
	t.Helper()
	task, err := taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:           taskID,
		Name:         "end-test",
		Mode:         taskEntity.SyncModeAll,
		SyncLevel:    taskEntity.SyncLevelTable,
		SourceSchema: "src_db",
		TargetSchema: "tgt_db",
		Tables:       []string{"users"},
		TargetTables: []string{"users"},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Start()
	task.MarkIncrementalStarted()
	// 通过 UpdateTask 把运行中状态写回服务映射
	if err := taskSvc.UpdateTask(task); err != nil {
		t.Fatalf("stage task: %v", err)
	}
}

// TestUpdateTaskHandler_RejectsStopped STOPPED 任务更新返回 409，且不改脏内存配置。
func TestUpdateTaskHandler_RejectsStopped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newHandlerTaskService(t)
	defer taskSvc.Close()
	stageAllTaskAtIncremental(t, taskSvc, "update_stopped")
	if err := taskSvc.EndTask("update_stopped"); err != nil {
		t.Fatalf("end task: %v", err)
	}

	original, ok := taskSvc.GetTask("update_stopped")
	if !ok {
		t.Fatal("task not found")
	}
	originalName := original.Config.Name

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.PUT("/api/tasks/:id", handler.UpdateTask)

	body, _ := json.Marshal(map[string]string{"name": "should-not-apply"})
	req := httptest.NewRequest(http.MethodPut, "/api/tasks/update_stopped", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}

	task, _ := taskSvc.GetTask("update_stopped")
	if task.Config.Name != originalName {
		t.Errorf("in-memory config mutated: got name %q, want %q", task.Config.Name, originalName)
	}
}

// TestEndTaskHandler_OK 结束合法任务返回 200。
func TestEndTaskHandler_OK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newHandlerTaskService(t)
	defer taskSvc.Close()
	stageAllTaskAtIncremental(t, taskSvc, "end_ok")

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks/:id/end", handler.EndTask)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/end_ok/end", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	task, ok := taskSvc.GetTask("end_ok")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Context.Status != taskEntity.TaskStatusStopped {
		t.Errorf("expected STOPPED, got %s", task.Context.Status)
	}
}

// TestEndTaskHandler_NotFound 结束不存在的任务返回 404。
func TestEndTaskHandler_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newHandlerTaskService(t)
	defer taskSvc.Close()

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks/:id/end", handler.EndTask)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/missing/end", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

// TestEndTaskHandler_Conlict 暂停状态（不允许结束）返回 409。
func TestEndTaskHandler_Conflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newHandlerTaskService(t)
	defer taskSvc.Close()

	task, err := taskSvc.CreateTask(taskEntity.TaskConfig{
		ID: "end_conflict", Mode: taskEntity.SyncModeAll, SourceSchema: "s", TargetSchema: "t",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Start()
	task.MarkIncrementalStarted()
	task.Pause() // 暂停 -> 非 RUNNING，不允许结束
	if err := taskSvc.UpdateTask(task); err != nil {
		t.Fatalf("stage task: %v", err)
	}

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks/:id/end", handler.EndTask)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/end_conflict/end", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

// TestRowCountComparisonHandler_Accepted STOPPED + ALL 任务启动核对返回 202。
// 后台 goroutine 会因无真实 DB 失败，但接口本身返回 202。
func TestRowCountComparisonHandler_Accepted(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newHandlerTaskService(t)
	defer taskSvc.Close()
	stageAllTaskAtIncremental(t, taskSvc, "cmp_ok")
	// 结束任务进入 STOPPED
	if err := taskSvc.EndTask("cmp_ok"); err != nil {
		t.Fatalf("end task: %v", err)
	}

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks/:id/row-count-comparison", handler.RowCountComparison)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/cmp_ok/row-count-comparison", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
}

// TestRowCountComparisonHandler_NotFound 不存在的任务返回 404。
func TestRowCountComparisonHandler_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newHandlerTaskService(t)
	defer taskSvc.Close()

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks/:id/row-count-comparison", handler.RowCountComparison)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/missing/row-count-comparison", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

// TestRowCountComparisonHandler_Conflict RUNNING 状态任务（不可对比）返回 409。
func TestRowCountComparisonHandler_Conflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newHandlerTaskService(t)
	defer taskSvc.Close()
	// RUNNING + ALL + INCREMENTAL_STARTED 状态不可对比（需 COMPLETED/STOPPED）
	stageAllTaskAtIncremental(t, taskSvc, "cmp_conflict")

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.POST("/api/tasks/:id/row-count-comparison", handler.RowCountComparison)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/cmp_conflict/row-count-comparison", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGetTaskReturnsRowCountComparison GET /api/tasks/:id 返回行数对比结果（脱敏后仍含结果）。
func TestGetTaskReturnsRowCountComparison(t *testing.T) {
	gin.SetMode(gin.TestMode)
	taskSvc := newHandlerTaskService(t)
	defer taskSvc.Close()

	// 创建一个 FULL 任务并完成全量，进入 COMPLETED（可对比状态）
	task, err := taskSvc.CreateTask(taskEntity.TaskConfig{
		ID:           "cmp_get",
		Name:         "cmp-get-test",
		Mode:         taskEntity.SyncModeFull,
		SyncLevel:    taskEntity.SyncLevelTable,
		SourceSchema: "src_db",
		TargetSchema: "tgt_db",
		Tables:       []string{"users"},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Start()
	task.MarkFullSyncCompleted()
	task.Complete()
	// COMPLETED 状态允许 UpdateTask
	if err := taskSvc.UpdateTask(task); err != nil {
		t.Fatalf("stage completed task: %v", err)
	}

	// 注入一个已完成的结果（COMPLETED 状态允许更新）
	task, _ = taskSvc.GetTask("cmp_get")
	srcRows := int64(100)
	tgtRows := int64(100)
	diff := int64(0)
	task.Context.RowCountComparison = &taskEntity.RowCountComparison{
		Status:        taskEntity.RowCountComparisonMatched,
		TotalTables:   1,
		CheckedTables: 1,
		MatchedTables: 1,
		SourceTotal:   100,
		TargetTotal:   100,
		Difference:    0,
		Tables: []taskEntity.RowCountComparisonTable{
			{
				SourceSchema: "src_db", SourceTable: "users",
				TargetSchema: "tgt_db", TargetTable: "users",
				SourceRows: &srcRows, TargetRows: &tgtRows,
				Difference: &diff, Matched: true,
			},
		},
	}
	if err := taskSvc.UpdateTask(task); err != nil {
		t.Fatalf("update task with comparison: %v", err)
	}

	handler := NewTaskHandler(taskSvc, &MockIdentityAnalyzer{})
	router := gin.New()
	router.GET("/api/tasks/:id", handler.GetTask)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/cmp_get", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp taskEntity.SyncTask
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Context.RowCountComparison == nil {
		t.Fatal("row_count_comparison not returned")
	}
	if resp.Context.RowCountComparison.Status != taskEntity.RowCountComparisonMatched {
		t.Errorf("expected MATCHED, got %s", resp.Context.RowCountComparison.Status)
	}
}
