package application

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"mysql-to-async/internal/checkpoint"
	"mysql-to-async/internal/metadata/domain/service"
	"mysql-to-async/internal/sync/domain/sink"
	"mysql-to-async/pkg/binlog"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// IncrementalSyncService 增量同步服务
// 面向 Sink 接口编程，不直接依赖任何具体写入器。
type IncrementalSyncService struct {
	sourceDB      *sql.DB
	analyzer      service.IdentityAnalyzer
	checkpointMgr checkpoint.Manager
	subscriber    *binlog.Subscriber
	sinks         []sink.Sink
	normalizer    *EventNormalizer
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	auditChan     chan *AuditLog
	wg            sync.WaitGroup
}

// AuditLog 审计日志
type AuditLog struct {
	TaskID      string    `json:"task_id"`
	TableName   string    `json:"table_name"`
	EventType   string    `json:"event_type"`
	Error       string    `json:"error"`
	BeforeImage string    `json:"before_image"`
	AfterImage  string    `json:"after_image"`
	Timestamp   time.Time `json:"timestamp"`
	Success     bool      `json:"success"`
}

// NewIncrementalSyncService 创建增量同步服务
// sinks 由外部（TaskService / SinkFactory）创建并注入。
func NewIncrementalSyncService(
	sourceDB *sql.DB,
	analyzer service.IdentityAnalyzer,
	checkpointMgr checkpoint.Manager,
	sinks []sink.Sink,
) *IncrementalSyncService {
	return &IncrementalSyncService{
		sourceDB:      sourceDB,
		analyzer:      analyzer,
		checkpointMgr: checkpointMgr,
		sinks:         sinks,
		normalizer:    NewEventNormalizer(),
		auditChan:     make(chan *AuditLog, 1000),
	}
}

// Start 启动增量同步
func (s *IncrementalSyncService) Start(ctx context.Context, taskID string, config *SyncConfig) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// 打开所有 Sink
	for _, sk := range s.sinks {
		if err := sk.Open(ctx); err != nil {
			return fmt.Errorf("open sink %s failed: %w", sk.Type(), err)
		}
		log.Printf("[Task %s] Sink %s opened", taskID, sk.Type())
	}

	sourceDBs := config.SourceDatabases
	if len(sourceDBs) == 0 && config.SourceSchema != "" {
		sourceDBs = []string{config.SourceSchema}
	}

	// 获取保存的位点
	pos, err := s.checkpointMgr.GetPosition(ctx, taskID)
	if err != nil {
		log.Printf("Warning: failed to get checkpoint: %v", err)
	}

	// 创建Binlog订阅器
	s.subscriber = binlog.NewSubscriber(&binlog.SubscriberConfig{
		Host:      config.SourceHost,
		Port:      config.SourcePort,
		Username:  config.SourceUsername,
		Password:  config.SourcePassword,
		Database:  config.SourceSchema,
		Databases: sourceDBs,
		Tables:    config.Tables,
		ServerID:  config.ServerID,
	})

	// 添加事件处理器（面向 Sink 接口）
	s.subscriber.AddHandler(&syncEventHandler{service: s, taskID: taskID})

	// 启动审计日志处理
	s.wg.Add(1)
	go s.processAuditLogs()

	// 启动订阅
	return s.subscriber.Start(ctx, pos)
}

// Stop 停止增量同步
func (s *IncrementalSyncService) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.subscriber != nil {
		s.subscriber.Stop()
	}
	// 关闭所有 Sink
	ctx := context.Background()
	for _, sk := range s.sinks {
		if err := sk.Close(ctx); err != nil {
			log.Printf("[IncrementalSyncService] Close sink %s error: %v", sk.Type(), err)
		}
	}

	// 安全关闭审计通道
	s.mu.Lock()
	if s.auditChan != nil {
		close(s.auditChan)
		s.auditChan = nil
	}
	s.mu.Unlock()

	s.wg.Wait() // 等待审计日志处理协程退出
}

// processAuditLogs 处理审计日志
func (s *IncrementalSyncService) processAuditLogs() {
	defer s.wg.Done()
	for auditLog := range s.auditChan {
		// 这里可以写入数据库或发送到日志系统
		if !auditLog.Success {
			log.Printf("[AUDIT] Sync failed - Task: %s, Table: %s, Event: %s, Error: %s",
				auditLog.TaskID, auditLog.TableName, auditLog.EventType, auditLog.Error)
		}
	}
}

// addAuditLog 添加审计日志
func (s *IncrementalSyncService) addAuditLog(log *AuditLog) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.auditChan == nil {
		return // 通道已关闭
	}

	select {
	case s.auditChan <- log:
	default:
		// 通道满了，丢弃
	}
}

// SyncConfig 同步配置
type SyncConfig struct {
	TaskID          string
	SourceHost      string
	SourcePort      int
	SourceUsername  string
	SourcePassword  string
	SourceSchema    string
	TargetSchema    string
	SourceDatabases []string
	TargetDatabases []string
	Tables          []string
	BatchSize       int
	ServerID        uint32
}

// syncEventHandler 同步事件处理器（面向 Sink 接口）
type syncEventHandler struct {
	service *IncrementalSyncService
	taskID  string
}

// OnEvent 处理Binlog事件：标准化 → 写入所有 Sink → 提交 checkpoint
func (h *syncEventHandler) OnEvent(event *binlog.BinlogEvent) error {
	// 将 BinlogEvent 标准化为 ChangeEvent 列表
	changeEvents, err := h.service.normalizer.Normalize(h.taskID, event)
	if err != nil {
		log.Printf("[Task %s] Normalize event failed: %v", h.taskID, err)
		return err
	}

	ctx := context.Background()

	// 将每个 ChangeEvent 写入所有 Sink
	for _, ce := range changeEvents {
		for _, sk := range h.service.sinks {
			if writeErr := sk.Write(ctx, ce); writeErr != nil {
				log.Printf("[Task %s] Sink %s write failed for %s.%s: %v",
					h.taskID, sk.Type(), ce.SourceSchema, ce.SourceTable, writeErr)

				h.service.addAuditLog(&AuditLog{
					TaskID:    h.taskID,
					TableName: ce.SourceTable,
					EventType: ce.EventType,
					Timestamp: time.Now(),
					Success:   false,
					Error:     writeErr.Error(),
				})
				return writeErr
			}
		}
	}

	// Flush 所有 Sink
	for _, sk := range h.service.sinks {
		if flushErr := sk.Flush(ctx); flushErr != nil {
			log.Printf("[Task %s] Sink %s flush failed: %v", h.taskID, sk.Type(), flushErr)
			return flushErr
		}
	}

	// Sink 全部确认后才提交 checkpoint
	if cpErr := h.service.checkpointMgr.SavePosition(ctx, h.taskID, event.Position); cpErr != nil {
		log.Printf("[Task %s] Failed to save checkpoint at %s:%d: %v",
			h.taskID, event.Position.Name, event.Position.Pos, cpErr)
		return fmt.Errorf("save checkpoint failed: %w", cpErr)
	}

	// 记录成功审计日志
	h.service.addAuditLog(&AuditLog{
		TaskID:    h.taskID,
		TableName: event.Table,
		EventType: string(event.EventType),
		Timestamp: time.Now(),
		Success:   true,
	})

	return nil
}

// GetPosition 获取当前位置
func (s *IncrementalSyncService) GetPosition() mysql.Position {
	if s.subscriber == nil {
		return mysql.Position{}
	}
	return s.subscriber.GetPosition()
}

// GetLag 获取延迟（与主库的位置差）
func (s *IncrementalSyncService) GetLag() (uint64, error) {
	if s.subscriber == nil {
		return 0, nil
	}

	masterPos, err := s.subscriber.GetMasterPosition()
	if err != nil {
		return 0, err
	}

	currentPos := s.subscriber.GetPosition()

	// 简单计算位置差（实际生产中可能需要更精确的计算）
	if masterPos.Name == currentPos.Name {
		if masterPos.Pos > currentPos.Pos {
			return uint64(masterPos.Pos - currentPos.Pos), nil
		}
		return 0, nil
	}

	// 不同的binlog文件，返回最大值表示延迟较大
	return ^uint64(0), nil
}
