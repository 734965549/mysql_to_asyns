package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	taskService "mysql-to-sync/internal/task/application/service"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
	"mysql-to-sync/pkg/taskevent"
)

// TaskEventHandler 任务事件 HTTP 处理器。
type TaskEventHandler struct {
	taskService *taskService.TaskService
}

// NewTaskEventHandler 创建处理器。
func NewTaskEventHandler(svc *taskService.TaskService) *TaskEventHandler {
	return &TaskEventHandler{taskService: svc}
}

// ListTaskEvents GET /api/tasks/:id/events
func (h *TaskEventHandler) ListTaskEvents(c *gin.Context) {
	taskID := c.Param("id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task id is required"})
		return
	}
	filter := port.TaskEventListFilter{TaskID: taskID}
	if v := c.Query("execution_id"); v != "" {
		filter.ExecutionID = v
	}
	if v := c.Query("after_seq"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid after_seq"})
			return
		}
		filter.AfterSeq = n
	}
	if v := c.Query("before_seq"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid before_seq"})
			return
		}
		filter.BeforeSeq = n
	}
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return
		}
		filter.Limit = n
	}
	if v := c.Query("min_severity"); v != "" {
		filter.MinSeverity = taskEntity.EventSeverity(v)
	}
	if v := c.Query("visibility"); v != "" {
		filter.Visibility = taskEntity.EventVisibility(v)
	}
	if v := c.Query("category"); v != "" {
		filter.Category = taskEntity.EventCategory(v)
	}
	if v := c.Query("code"); v != "" {
		filter.Code = v
	}
	if v := c.Query("source_table"); v != "" {
		filter.SourceTable = v
	}
	if v := c.Query("source_schema"); v != "" {
		filter.SourceSchema = v
	}

	events, err := h.taskService.ListTaskEvents(filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	for _, ev := range events {
		if ev == nil {
			continue
		}
		ev.Message, ev.Details = taskevent.SanitizeTaskEventFields(ev.Message, ev.Details)
	}
	c.JSON(http.StatusOK, gin.H{
		"task_id": taskID,
		"events":  events,
	})
}

// ListTaskEventExecutions GET /api/tasks/:id/event-executions
func (h *TaskEventHandler) ListTaskEventExecutions(c *gin.Context) {
	taskID := c.Param("id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task id is required"})
		return
	}
	executions, err := h.taskService.ListTaskEventExecutions(taskID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"task_id":    taskID,
		"executions": executions,
	})
}
