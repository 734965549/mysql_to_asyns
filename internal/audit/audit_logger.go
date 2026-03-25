package audit

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// EventType 事件类型
type EventType string

const (
	EventTypeSyncStart    EventType = "SYNC_START"
	EventTypeSyncComplete EventType = "SYNC_COMPLETE"
	EventTypeSyncFailed   EventType = "SYNC_FAILED"
	EventTypeTaskCreated  EventType = "TASK_CREATED"
	EventTypeTaskDeleted  EventType = "TASK_DELETED"
	EventTypeTaskPaused   EventType = "TASK_PAUSED"
	EventTypeTaskResumed  EventType = "TASK_RESUMED"
	EventTypeDataRead     EventType = "DATA_READ"
	EventTypeDataWrite    EventType = "DATA_WRITE"
	EventTypeDataUpdate   EventType = "DATA_UPDATE"
	EventTypeDataDelete   EventType = "DATA_DELETE"
	EventTypeError        EventType = "ERROR"
)

// Event 审计事件
type Event struct {
	Timestamp    time.Time              `json:"timestamp"`
	TaskID       string                 `json:"task_id"`
	EventType    EventType              `json:"event_type"`
	TableName    string                 `json:"table_name,omitempty"`
	Schema       string                 `json:"schema,omitempty"`
	Position     string                 `json:"position,omitempty"`
	Success      bool                   `json:"success"`
	ErrorMsg     string                 `json:"error_msg,omitempty"`
	Details      map[string]interface{} `json:"details,omitempty"`
	RowsAffected int64                  `json:"rows_affected,omitempty"`
}

// AuditLogger 审计日志器
type AuditLogger struct {
	logDir string
	mu     sync.Mutex
	file   *os.File
}

// NewAuditLogger 创建审计日志器
func NewAuditLogger(logDir string) *AuditLogger {
	// 确保日志目录存在
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("Warning: failed to create audit log directory: %v", err)
	}

	al := &AuditLogger{
		logDir: logDir,
	}

	// 打开或创建当天的日志文件
	al.rotateFile()

	return al
}

// rotateFile 轮转日志文件（按天）
func (al *AuditLogger) rotateFile() error {
	al.mu.Lock()
	defer al.mu.Unlock()

	// 关闭旧文件
	if al.file != nil {
		al.file.Close()
	}

	// 创建新文件（按天命名）
	today := time.Now().Format("2006-01-02")
	filePath := filepath.Join(al.logDir, fmt.Sprintf("audit_%s.log", today))

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open audit log file: %w", err)
	}

	al.file = file
	return nil
}

// Log 记录审计事件
func (al *AuditLogger) Log(event *Event) error {
	al.mu.Lock()
	defer al.mu.Unlock()

	// 检查是否需要轮转文件
	today := time.Now().Format("2006-01-02")
	currentFile := filepath.Base(al.file.Name())
	expectedFile := fmt.Sprintf("audit_%s.log", today)
	if currentFile != expectedFile {
		al.mu.Unlock()
		al.rotateFile()
		al.mu.Lock()
	}

	// 设置时间戳
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// 序列化事件
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal audit event: %w", err)
	}

	// 写入文件
	if _, err := al.file.WriteString(string(data) + "\n"); err != nil {
		return fmt.Errorf("failed to write audit event: %w", err)
	}

	return nil
}

// LogSyncStart 记录同步开始
func (al *AuditLogger) LogSyncStart(taskID, schema, tableName string) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeSyncStart,
		Schema:    schema,
		TableName: tableName,
		Success:   true,
	})
}

// LogSyncComplete 记录同步完成
func (al *AuditLogger) LogSyncComplete(taskID, schema, tableName string, rowsAffected int64) {
	al.Log(&Event{
		TaskID:       taskID,
		EventType:    EventTypeSyncComplete,
		Schema:       schema,
		TableName:    tableName,
		Success:      true,
		RowsAffected: rowsAffected,
	})
}

// LogSyncFailed 记录同步失败
func (al *AuditLogger) LogSyncFailed(taskID, schema, tableName, position, errorMsg string, details map[string]interface{}) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeSyncFailed,
		Schema:    schema,
		TableName: tableName,
		Position:  position,
		Success:   false,
		ErrorMsg:  errorMsg,
		Details:   details,
	})
}

// LogDataWrite 记录数据写入
func (al *AuditLogger) LogDataWrite(taskID, schema, tableName string, rowsAffected int64, success bool, errorMsg string) {
	al.Log(&Event{
		TaskID:       taskID,
		EventType:    EventTypeDataWrite,
		Schema:       schema,
		TableName:    tableName,
		Success:      success,
		RowsAffected: rowsAffected,
		ErrorMsg:     errorMsg,
	})
}

// LogDataUpdate 记录数据更新
func (al *AuditLogger) LogDataUpdate(taskID, schema, tableName string, success bool, errorMsg string, details map[string]interface{}) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeDataUpdate,
		Schema:    schema,
		TableName: tableName,
		Success:   success,
		ErrorMsg:  errorMsg,
		Details:   details,
	})
}

// LogDataDelete 记录数据删除
func (al *AuditLogger) LogDataDelete(taskID, schema, tableName string, success bool, errorMsg string) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeDataDelete,
		Schema:    schema,
		TableName: tableName,
		Success:   success,
		ErrorMsg:  errorMsg,
	})
}

// LogError 记录错误
func (al *AuditLogger) LogError(taskID, errorMsg string, details map[string]interface{}) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeError,
		Success:   false,
		ErrorMsg:  errorMsg,
		Details:   details,
	})
}

// LogTaskCreated 记录任务创建
func (al *AuditLogger) LogTaskCreated(taskID, taskName string) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeTaskCreated,
		Success:   true,
		Details:   map[string]interface{}{"task_name": taskName},
	})
}

// LogTaskDeleted 记录任务删除
func (al *AuditLogger) LogTaskDeleted(taskID string) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeTaskDeleted,
		Success:   true,
	})
}

// LogTaskPaused 记录任务暂停
func (al *AuditLogger) LogTaskPaused(taskID string) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeTaskPaused,
		Success:   true,
	})
}

// LogTaskResumed 记录任务恢复
func (al *AuditLogger) LogTaskResumed(taskID string) {
	al.Log(&Event{
		TaskID:    taskID,
		EventType: EventTypeTaskResumed,
		Success:   true,
	})
}

// Close 关闭审计日志器
func (al *AuditLogger) Close() error {
	al.mu.Lock()
	defer al.mu.Unlock()

	if al.file != nil {
		return al.file.Close()
	}
	return nil
}

// QueryOptions 查询选项
type QueryOptions struct {
	TaskID    string
	EventType EventType
	StartTime *time.Time
	EndTime   *time.Time
	Limit     int
}

// Query 查询审计日志
func (al *AuditLogger) Query(opts QueryOptions) ([]*Event, error) {
	al.mu.Lock()
	defer al.mu.Unlock()

	// 读取日志文件
	data, err := os.ReadFile(al.file.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read audit log: %w", err)
	}

	// 解析日志
	var events []*Event
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}

		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		// 过滤
		if opts.TaskID != "" && event.TaskID != opts.TaskID {
			continue
		}
		if opts.EventType != "" && event.EventType != opts.EventType {
			continue
		}
		if opts.StartTime != nil && event.Timestamp.Before(*opts.StartTime) {
			continue
		}
		if opts.EndTime != nil && event.Timestamp.After(*opts.EndTime) {
			continue
		}

		events = append(events, &event)

		// 限制数量
		if opts.Limit > 0 && len(events) >= opts.Limit {
			break
		}
	}

	return events, nil
}
