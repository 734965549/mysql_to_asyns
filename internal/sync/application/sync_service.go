package application

import (
	"context"

	"database/sql"

	"fmt"

	"mysql-to-async/internal/checkpoint"

	"mysql-to-async/internal/metadata/domain/entity"

	"mysql-to-async/internal/metadata/domain/service"

	"mysql-to-async/internal/sync/infrastructure/writer"

	"mysql-to-async/pkg/binlog"

	"mysql-to-async/pkg/logger"

	"sync"

	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// IncrementalSyncService 增量同步服务

type IncrementalSyncService struct {
	sourceDB *sql.DB

	targetDB *sql.DB

	analyzer service.IdentityAnalyzer

	checkpointMgr checkpoint.Manager

	subscriber *binlog.Subscriber

	writers map[string]*writer.BufferedWriter

	identities map[string]*entity.TableIdentity

	targetSchemas map[string]string

	mu sync.RWMutex

	ctx context.Context

	cancel context.CancelFunc

	auditChan chan *AuditLog

	wg sync.WaitGroup
}

// AuditLog 审计日志

type AuditLog struct {
	TaskID string `json:"task_id"`

	TableName string `json:"table_name"`

	EventType string `json:"event_type"`

	Error string `json:"error"`

	BeforeImage string `json:"before_image"`

	AfterImage string `json:"after_image"`

	Timestamp time.Time `json:"timestamp"`

	Success bool `json:"success"`
}

// NewIncrementalSyncService 创建增量同步服务

func NewIncrementalSyncService(

	sourceDB, targetDB *sql.DB,

	analyzer service.IdentityAnalyzer,

	checkpointMgr checkpoint.Manager,

) *IncrementalSyncService {

	return &IncrementalSyncService{

		sourceDB: sourceDB,

		targetDB: targetDB,

		analyzer: analyzer,

		checkpointMgr: checkpointMgr,

		writers: make(map[string]*writer.BufferedWriter),

		identities: make(map[string]*entity.TableIdentity),

		targetSchemas: make(map[string]string),

		auditChan: make(chan *AuditLog, 1000),
	}

}

// Start 启动增量同步

func (s *IncrementalSyncService) Start(ctx context.Context, taskID string, config *SyncConfig) error {

	s.ctx, s.cancel = context.WithCancel(ctx)

	// 构建数据库映射

	dbMapping := make(map[string]string)

	sourceDBs := config.SourceDatabases

	if len(sourceDBs) == 0 && config.SourceSchema != "" {

		sourceDBs = []string{config.SourceSchema}

	}

	for i, src := range sourceDBs {

		tgt := src

		// 尝试从TargetDatabases获取映射

		if i < len(config.TargetDatabases) && config.TargetDatabases[i] != "" {

			tgt = config.TargetDatabases[i]

		} else if len(config.SourceDatabases) == 0 && config.TargetSchema != "" {

			// 如果是单库模式(SourceDatabases为空)，使用TargetSchema

			tgt = config.TargetSchema

		}

		dbMapping[src] = tgt

	}

	// 初始化表的标识信息和写入器

	for srcDB, tgtDB := range dbMapping {

		tables := config.Tables

		// 如果未指定表（库级别同步），获取该库所有表

		if len(tables) == 0 {

			allTables, err := s.analyzer.GetAllTables(srcDB)

			if err != nil {

				return err

			}

			tables = make([]string, 0, len(allTables))

			for _, t := range allTables {

				tables = append(tables, t.TableName)

			}

		}

		for _, tableName := range tables {

			identity, err := s.analyzer.AnalyzeTable(srcDB, tableName)

			if err != nil {

				return err

			}

			key := fmt.Sprintf("%s.%s", srcDB, tableName)

			// 创建写入器（使用TargetSchema确保数据写入正确的目标库）

			bw := writer.NewBufferedWriterWithSchema(

				s.targetDB,

				identity,

				config.BatchSize,

				500*time.Millisecond,

				tgtDB,
			)

			s.mu.Lock()

			s.identities[key] = identity

			s.targetSchemas[key] = tgtDB

			s.writers[key] = bw

			s.mu.Unlock()

		}

	}

	// 获取保存的位点

	pos, err := s.checkpointMgr.GetPosition(ctx, taskID)

	if err != nil {

		logger.Warn("failed to get checkpoint: %v", err)

	}

	// 创建Binlog订阅器

	s.subscriber = binlog.NewSubscriber(&binlog.SubscriberConfig{

		Host: config.SourceHost,

		Port: config.SourcePort,

		Username: config.SourceUsername,

		Password: config.SourcePassword,

		Database: config.SourceSchema,

		Databases: sourceDBs,

		Tables: config.Tables,

		ServerID: config.ServerID,
	})

	// 添加事件处理器

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

	// 关闭所有写入器

	for _, w := range s.writers {

		w.Close()

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

			logger.Error("[AUDIT] Sync failed - Task: %s, Table: %s, Event: %s, Error: %s",

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

// getWriter 获取写入器

func (s *IncrementalSyncService) getWriter(tableName string) *writer.BufferedWriter {

	s.mu.RLock()

	defer s.mu.RUnlock()

	return s.writers[tableName]

}

// getIdentity 获取表标识

func (s *IncrementalSyncService) getIdentity(tableName string) *entity.TableIdentity {

	s.mu.RLock()

	defer s.mu.RUnlock()

	return s.identities[tableName]

}

// getTargetSchema 获取目标数据库名

func (s *IncrementalSyncService) getTargetSchema(key string) string {

	s.mu.RLock()

	defer s.mu.RUnlock()

	return s.targetSchemas[key]

}

// SyncConfig 同步配置

type SyncConfig struct {
	TaskID string

	SourceHost string

	SourcePort int

	SourceUsername string

	SourcePassword string

	SourceSchema string

	TargetSchema string

	SourceDatabases []string

	TargetDatabases []string

	Tables []string

	BatchSize int

	ServerID uint32
}

// syncEventHandler 同步事件处理器

type syncEventHandler struct {
	service *IncrementalSyncService

	taskID string
}

// OnEvent 处理Binlog事件

func (h *syncEventHandler) OnEvent(event *binlog.BinlogEvent) error {

	key := fmt.Sprintf("%s.%s", event.Schema, event.Table)

	w := h.service.getWriter(key)

	if w == nil {

		return nil // 不是我们需要同步的表

	}

	identity := h.service.getIdentity(key)

	if identity == nil {

		return nil

	}

	targetSchema := h.service.getTargetSchema(key)

	var err error

	switch event.EventType {

	case binlog.EventTypeInsert:

		err = h.handleInsert(event, w)

	case binlog.EventTypeUpdate:

		err = h.handleUpdate(event, w, identity, targetSchema)

	case binlog.EventTypeDelete:

		err = h.handleDelete(event, w, identity, targetSchema)

	}

	// 保存位点

	if err == nil {

		ctx := context.Background()

		if cpErr := h.service.checkpointMgr.SavePosition(ctx, h.taskID, event.Position); cpErr != nil {

			err = fmt.Errorf("save checkpoint failed: %w", cpErr)

			logger.Error("[Task %s] Failed to save checkpoint for %s.%s at %s:%d: %v",

				h.taskID, event.Schema, event.Table, event.Position.Name, event.Position.Pos, cpErr)

		}

	}

	// 记录审计日志

	auditLog := &AuditLog{

		TaskID: h.taskID,

		TableName: event.Table,

		EventType: string(event.EventType),

		Timestamp: time.Now(),

		Success: err == nil,
	}

	if err != nil {

		auditLog.Error = err.Error()

	}

	h.service.addAuditLog(auditLog)

	return err

}

// handleInsert 处理INSERT事件

func (h *syncEventHandler) handleInsert(event *binlog.BinlogEvent, w *writer.BufferedWriter) error {

	for _, row := range event.Rows {

		if err := w.Write(row); err != nil {

			return err

		}

	}

	return w.Flush()

}

// handleUpdate 处理UPDATE事件

func (h *syncEventHandler) handleUpdate(event *binlog.BinlogEvent, w *writer.BufferedWriter, identity *entity.TableIdentity, targetSchema string) error {

	batchWriter := writer.NewBatchWriterWithSchema(h.service.targetDB, identity, 1000, targetSchema)

	for i, row := range event.Rows {

		var err error

		if identity.Strategy == entity.FullColumnsStrategy && i < len(event.BeforeImage) {

			// 无主键表：使用 before image 作为 WHERE 条件

			err = batchWriter.UpdateWithBeforeImage(context.Background(), row, event.BeforeImage[i])

		} else {

			// 有主键/唯一键表：直接使用 row (after image) 即可

			err = batchWriter.Update(context.Background(), row)

		}

		if err != nil {

			return err

		}

	}

	return nil

}

// handleDelete 处理DELETE事件

func (h *syncEventHandler) handleDelete(event *binlog.BinlogEvent, w *writer.BufferedWriter, identity *entity.TableIdentity, targetSchema string) error {

	batchWriter := writer.NewBatchWriterWithSchema(h.service.targetDB, identity, 1000, targetSchema)

	for _, row := range event.Rows {

		if err := batchWriter.Delete(context.Background(), row); err != nil {

			return err

		}

	}

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
