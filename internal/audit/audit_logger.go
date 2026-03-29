package audit // 声明当前文件属于audit包，用于审计日志管理

import ( // 导入外部包和标准库
	"encoding/json" // 导入encoding/json包，用于JSON编码解码
	"fmt" // 导入fmt包，用于格式化输入输出
	"log" // 导入log包，用于日志输出
	"os" // 导入os包，用于操作系统接口
	"path/filepath" // 导入path/filepath包，用于文件路径操作
	"strings" // 导入strings包，用于字符串操作
	"sync" // 导入sync包，用于并发控制
	"time" // 导入time包，用于时间处理
)

// EventType 事件类型，定义审计事件的类型
type EventType string // 定义事件类型为字符串类型

const ( // 定义常量
	EventTypeSyncStart    EventType = "SYNC_START" // 同步开始事件
	EventTypeSyncComplete EventType = "SYNC_COMPLETE" // 同步完成事件
	EventTypeSyncFailed   EventType = "SYNC_FAILED" // 同步失败事件
	EventTypeTaskCreated  EventType = "TASK_CREATED" // 任务创建事件
	EventTypeTaskDeleted  EventType = "TASK_DELETED" // 任务删除事件
	EventTypeTaskPaused   EventType = "TASK_PAUSED" // 任务暂停事件
	EventTypeTaskResumed  EventType = "TASK_RESUMED" // 任务恢复事件
	EventTypeDataRead     EventType = "DATA_READ" // 数据读取事件
	EventTypeDataWrite    EventType = "DATA_WRITE" // 数据写入事件
	EventTypeDataUpdate   EventType = "DATA_UPDATE" // 数据更新事件
	EventTypeDataDelete   EventType = "DATA_DELETE" // 数据删除事件
	EventTypeError        EventType = "ERROR" // 错误事件
)

// Event 审计事件结构体，用于记录审计日志事件
type Event struct { // 定义审计事件结构体
	Timestamp    time.Time              `json:"timestamp"` // 事件时间戳
	TaskID       string                 `json:"task_id"` // 任务ID
	EventType    EventType              `json:"event_type"` // 事件类型
	TableName    string                 `json:"table_name,omitempty"` // 表名（可选）
	Schema       string                 `json:"schema,omitempty"` // 模式名（可选）
	Position     string                 `json:"position,omitempty"` // 位置信息（可选）
	Success      bool                   `json:"success"` // 是否成功
	ErrorMsg     string                 `json:"error_msg,omitempty"` // 错误消息（可选）
	Details      map[string]interface{} `json:"details,omitempty"` // 详细信息（可选）
	RowsAffected int64                  `json:"rows_affected,omitempty"` // 影响行数（可选）
}

// AuditLogger 审计日志器结构体，用于管理审计日志
type AuditLogger struct { // 定义审计日志器结构体
	logDir string // 日志目录路径
	mu     sync.Mutex // 互斥锁，用于并发控制
	file   *os.File // 日志文件句柄
}

// NewAuditLogger 创建审计日志器函数
func NewAuditLogger(logDir string) *AuditLogger { // 创建审计日志器实例
	// 确保日志目录存在
	if err := os.MkdirAll(logDir, 0755); err != nil { // 创建日志目录，权限0755
		log.Printf("Warning: failed to create audit log directory: %v", err) // 输出警告日志
	}

	al := &AuditLogger{ // 创建审计日志器实例
		logDir: logDir, // 设置日志目录
	}

	// 打开或创建当天的日志文件
	al.rotateFile() // 轮转日志文件

	return al // 返回审计日志器实例
}

// rotateFile 轮转日志文件（按天）方法
func (al *AuditLogger) rotateFile() error { // 轮转日志文件
	al.mu.Lock() // 获取锁
	defer al.mu.Unlock() // 延迟释放锁

	// 关闭旧文件
	if al.file != nil { // 如果文件句柄不为空
		al.file.Close() // 关闭文件
	}

	// 创建新文件（按天命名）
	today := time.Now().Format("2006-01-02") // 获取当前日期
	filePath := filepath.Join(al.logDir, fmt.Sprintf("audit_%s.log", today)) // 构建文件路径

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) // 打开文件，追加模式
	if err != nil { // 如果打开失败
		return fmt.Errorf("failed to open audit log file: %w", err) // 返回错误
	}

	al.file = file // 设置文件句柄
	return nil // 返回nil
}

// Log 记录审计事件方法
func (al *AuditLogger) Log(event *Event) error { // 记录审计事件
	al.mu.Lock() // 获取锁
	defer al.mu.Unlock() // 延迟释放锁

	// 检查是否需要轮转文件
	today := time.Now().Format("2006-01-02") // 获取当前日期
	currentFile := filepath.Base(al.file.Name()) // 获取当前文件名
	expectedFile := fmt.Sprintf("audit_%s.log", today) // 期望的文件名
	if currentFile != expectedFile { // 如果文件名不匹配
		al.mu.Unlock() // 释放锁
		al.rotateFile() // 轮转文件
		al.mu.Lock() // 重新获取锁
	}

	// 设置时间戳
	if event.Timestamp.IsZero() { // 如果时间戳为零值
		event.Timestamp = time.Now() // 设置为当前时间
	}

	// 序列化事件
	data, err := json.Marshal(event) // 将事件序列化为JSON
	if err != nil { // 如果序列化失败
		return fmt.Errorf("failed to marshal audit event: %w", err) // 返回错误
	}

	// 写入文件
	if _, err := al.file.WriteString(string(data) + "\n"); err != nil { // 写入文件并添加换行符
		return fmt.Errorf("failed to write audit event: %w", err) // 返回错误
	}

	return nil // 返回nil
}

// LogSyncStart 记录同步开始方法
func (al *AuditLogger) LogSyncStart(taskID, schema, tableName string) { // 记录同步开始事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeSyncStart, // 设置事件类型为同步开始
		Schema:    schema, // 设置模式名
		TableName: tableName, // 设置表名
		Success:   true, // 设置成功标志
	})
}

// LogSyncComplete 记录同步完成方法
func (al *AuditLogger) LogSyncComplete(taskID, schema, tableName string, rowsAffected int64) { // 记录同步完成事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:       taskID, // 设置任务ID
		EventType:    EventTypeSyncComplete, // 设置事件类型为同步完成
		Schema:       schema, // 设置模式名
		TableName:    tableName, // 设置表名
		Success:      true, // 设置成功标志
		RowsAffected: rowsAffected, // 设置影响行数
	})
}

// LogSyncFailed 记录同步失败方法
func (al *AuditLogger) LogSyncFailed(taskID, schema, tableName, position, errorMsg string, details map[string]interface{}) { // 记录同步失败事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeSyncFailed, // 设置事件类型为同步失败
		Schema:    schema, // 设置模式名
		TableName: tableName, // 设置表名
		Position:  position, // 设置位置信息
		Success:   false, // 设置失败标志
		ErrorMsg:  errorMsg, // 设置错误消息
		Details:   details, // 设置详细信息
	})
}

// LogDataWrite 记录数据写入方法
func (al *AuditLogger) LogDataWrite(taskID, schema, tableName string, rowsAffected int64, success bool, errorMsg string) { // 记录数据写入事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:       taskID, // 设置任务ID
		EventType:    EventTypeDataWrite, // 设置事件类型为数据写入
		Schema:       schema, // 设置模式名
		TableName:    tableName, // 设置表名
		Success:      success, // 设置成功标志
		RowsAffected: rowsAffected, // 设置影响行数
		ErrorMsg:     errorMsg, // 设置错误消息
	})
}

// LogDataUpdate 记录数据更新方法
func (al *AuditLogger) LogDataUpdate(taskID, schema, tableName string, success bool, errorMsg string, details map[string]interface{}) { // 记录数据更新事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeDataUpdate, // 设置事件类型为数据更新
		Schema:    schema, // 设置模式名
		TableName: tableName, // 设置表名
		Success:   success, // 设置成功标志
		ErrorMsg:  errorMsg, // 设置错误消息
		Details:   details, // 设置详细信息
	})
}

// LogDataDelete 记录数据删除方法
func (al *AuditLogger) LogDataDelete(taskID, schema, tableName string, success bool, errorMsg string) { // 记录数据删除事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeDataDelete, // 设置事件类型为数据删除
		Schema:    schema, // 设置模式名
		TableName: tableName, // 设置表名
		Success:   success, // 设置成功标志
		ErrorMsg:  errorMsg, // 设置错误消息
	})
}

// LogError 记录错误方法
func (al *AuditLogger) LogError(taskID, errorMsg string, details map[string]interface{}) { // 记录错误事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeError, // 设置事件类型为错误
		Success:   false, // 设置失败标志
		ErrorMsg:  errorMsg, // 设置错误消息
		Details:   details, // 设置详细信息
	})
}

// LogTaskCreated 记录任务创建方法
func (al *AuditLogger) LogTaskCreated(taskID, taskName string) { // 记录任务创建事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeTaskCreated, // 设置事件类型为任务创建
		Success:   true, // 设置成功标志
		Details:   map[string]interface{}{"task_name": taskName}, // 设置任务名称详细信息
	})
}

// LogTaskDeleted 记录任务删除方法
func (al *AuditLogger) LogTaskDeleted(taskID string) { // 记录任务删除事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeTaskDeleted, // 设置事件类型为任务删除
		Success:   true, // 设置成功标志
	})
}

// LogTaskPaused 记录任务暂停方法
func (al *AuditLogger) LogTaskPaused(taskID string) { // 记录任务暂停事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeTaskPaused, // 设置事件类型为任务暂停
		Success:   true, // 设置成功标志
	})
}

// LogTaskResumed 记录任务恢复方法
func (al *AuditLogger) LogTaskResumed(taskID string) { // 记录任务恢复事件
	al.Log(&Event{ // 调用日志记录方法
		TaskID:    taskID, // 设置任务ID
		EventType: EventTypeTaskResumed, // 设置事件类型为任务恢复
		Success:   true, // 设置成功标志
	})
}

// Close 关闭审计日志器方法
func (al *AuditLogger) Close() error { // 关闭审计日志器
	al.mu.Lock() // 获取锁
	defer al.mu.Unlock() // 延迟释放锁

	if al.file != nil { // 如果文件句柄不为空
		return al.file.Close() // 关闭文件
	}
	return nil // 返回nil
}

// QueryOptions 查询选项结构体
type QueryOptions struct { // 定义查询选项结构体
	TaskID    string // 任务ID过滤条件
	EventType EventType // 事件类型过滤条件
	StartTime *time.Time // 开始时间过滤条件
	EndTime   *time.Time // 结束时间过滤条件
	Limit     int // 结果数量限制
}

// Query 查询审计日志方法
func (al *AuditLogger) Query(opts QueryOptions) ([]*Event, error) { // 查询审计日志
	al.mu.Lock() // 获取锁
	defer al.mu.Unlock() // 延迟释放锁

	// 读取日志文件
	data, err := os.ReadFile(al.file.Name()) // 读取日志文件内容
	if err != nil { // 如果读取失败
		return nil, fmt.Errorf("failed to read audit log: %w", err) // 返回错误
	}

	// 解析日志
	var events []*Event // 定义事件列表
	lines := strings.Split(string(data), "\n") // 按换行符分割内容
	for _, line := range lines { // 遍历每一行
		if line == "" { // 如果行为空
			continue // 跳过
		}

		var event Event // 定义事件变量
		if err := json.Unmarshal([]byte(line), &event); err != nil { // 解析JSON行
			continue // 跳过错误行
		}

		// 过滤
		if opts.TaskID != "" && event.TaskID != opts.TaskID { // 如果任务ID不匹配
			continue // 跳过
		}
		if opts.EventType != "" && event.EventType != opts.EventType { // 如果事件类型不匹配
			continue // 跳过
		}
		if opts.StartTime != nil && event.Timestamp.Before(*opts.StartTime) { // 如果时间早于开始时间
			continue // 跳过
		}
		if opts.EndTime != nil && event.Timestamp.After(*opts.EndTime) { // 如果时间晚于结束时间
			continue // 跳过
		}

		events = append(events, &event) // 添加到结果列表

		// 限制数量
		if opts.Limit > 0 && len(events) >= opts.Limit { // 如果设置了限制且达到数量
			break // 跳出循环
		}
	}

	return events, nil // 返回事件列表
}
