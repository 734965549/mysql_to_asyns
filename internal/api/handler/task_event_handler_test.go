package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"mysql-to-sync/internal/config"
	taskService "mysql-to-sync/internal/task/application/service"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
)

func newTestTaskServiceWithTempData(t *testing.T) *taskService.TaskService {
	t.Helper()
	dir := t.TempDir()
	return taskService.NewTaskService(&config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: filepath.Join(dir, "data")},
	})
}

func TestListTaskEvents_RedactsSecretsInResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := newTestTaskServiceWithTempData(t)
	taskID := "event_sanitize_task"
	_, err := svc.CreateTask(taskEntity.TaskConfig{
		ID:   taskID,
		Name: "Event sanitize",
		SourceDB: &taskEntity.DatabaseConfig{
			Host: "127.0.0.1", Port: 3306, Username: "root", Password: "db-secret",
		},
	})
	require.NoError(t, err)

	rec := svc.EventRecorder()
	require.NotNil(t, rec)
	rec.SetExecutionID(taskID, "exec-1")
	rec.Emit(taskEntity.TaskEvent{
		TaskID:      taskID,
		ExecutionID: "exec-1",
		Timestamp:   time.Now(),
		Severity:    taskEntity.EventSeverityWarn,
		Visibility:  taskEntity.EventVisibilityKey,
		Category:    taskEntity.EventCategoryTable,
		Code:        "WRITE_LOCK_RETRY",
		Message:     "retry dsn=user:db-secret@tcp(localhost)/db Authorization=Bearer tok-abc",
		Details: map[string]interface{}{
			"password": "plain-pwd",
			"nested": map[string]interface{}{
				"token": "nested-token",
			},
		},
	})
	rec.Close()

	h := NewTaskEventHandler(svc)
	router := gin.New()
	router.GET("/api/tasks/:id/events", h.ListTaskEvents)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/"+taskID+"/events", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.Bytes()
	for _, secret := range []string{"db-secret", "plain-pwd", "nested-token", "tok-abc"} {
		require.NotContains(t, string(body), secret, "response leaked secret")
	}
	require.Contains(t, string(body), "[REDACTED]")

	var resp struct {
		Events []taskEntity.TaskEvent `json:"events"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))
	require.NotEmpty(t, resp.Events)
}

func TestListTaskEvents_InvalidAfterSeq(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := newTestTaskServiceWithTempData(t)
	h := NewTaskEventHandler(svc)
	router := gin.New()
	router.GET("/api/tasks/:id/events", h.ListTaskEvents)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/t1/events?after_seq=bad", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.True(t, bytes.Contains(w.Body.Bytes(), []byte("invalid after_seq")))
}

func TestListTaskEventExecutions_OK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := newTestTaskServiceWithTempData(t)
	taskID := "event_exec_task"
	_, err := svc.CreateTask(taskEntity.TaskConfig{ID: taskID, Name: "Exec list"})
	require.NoError(t, err)

	rec := svc.EventRecorder()
	rec.SetExecutionID(taskID, "exec-a")
	rec.Emit(taskEntity.TaskEvent{
		TaskID: taskID, ExecutionID: "exec-a", Timestamp: time.Now(),
		Severity: taskEntity.EventSeverityInfo, Visibility: taskEntity.EventVisibilityKey,
		Category: taskEntity.EventCategoryLifecycle, Code: taskEntity.EventCodeTaskStarted,
		Message: "started",
	})
	rec.Close()

	h := NewTaskEventHandler(svc)
	router := gin.New()
	router.GET("/api/tasks/:id/event-executions", h.ListTaskEventExecutions)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/"+taskID+"/event-executions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "exec-a")
}
