package application

import (
	"context"

	"database/sql"

	"fmt"

	"mysql-to-sync/internal/checkpoint"

	"mysql-to-sync/internal/metadata/domain/entity"

	"mysql-to-sync/internal/metadata/domain/service"

	"mysql-to-sync/internal/metrics"

	"mysql-to-sync/internal/sync/infrastructure/writer"

	"mysql-to-sync/pkg/binlog"

	"mysql-to-sync/pkg/logger"

	"sync"

	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// PositionPersister 位点回写回调：每次成功落库 + 写完 checkpoint 后被调用，由调用方（task service）决定如何
// 把位点冗余持久化到任务存档（用于离线审计 / 阶段状态判断）。回调内部应自带节流，避免每个 binlog 事件都触发存储写。
// 回调失败不会反向影响增量同步本身。
type PositionPersister func(taskID string, pos mysql.Position)

// IncrementalSyncService 增量同步服务

// IncrementalSyncService replays ROW binlog events into the target database.
//
// It owns binlog subscription, target writers, table identities, and incremental
// checkpoint writes. It does not own task lifecycle; TaskService starts/stops it
// and optionally receives throttled position snapshots for task archives.
type IncrementalSyncService struct {
	sourceDB *sql.DB

	targetDB *sql.DB

	writeConn *sql.Conn // 专用写入连接，已禁用外键检查

	analyzer service.IdentityAnalyzer

	checkpointMgr checkpoint.Manager

	// positionPersister 可选位点回写回调，由 task service 注入用于把增量位点冗余到任务存档。
	// nil 时不做任何回写，行为与历史版本一致。
	positionPersister PositionPersister

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

// SetPositionPersister 注入位点回写回调。允许在 Start 前调用，重复调用会覆盖旧值。
// 传 nil 可以清除已注入的回调。
func (s *IncrementalSyncService) SetPositionPersister(p PositionPersister) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.positionPersister = p
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

	// 获取专用写入连接并关闭外键检查，避免有外键的表在增量同步时报错
	writeConn, err := s.targetDB.Conn(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to get write connection: %w", err)
	}
	s.writeConn = writeConn
	if _, err := writeConn.ExecContext(s.ctx, "SET SESSION FOREIGN_KEY_CHECKS=0"); err != nil {
		writeConn.Close()
		s.writeConn = nil
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
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

			// 创建写入器（使用专用连接确保外键检查已关闭）

			bw := writer.NewBufferedWriterWithConn(

				writeConn,

				identity,

				config.BatchSize,

				500*time.Millisecond,

				tgtDB,
			)

			// 修复 10/11：增量路径开启 upsert 语义。
			// 增量 INSERT 事件可能重复到达（全量期间的变更要由增量回放追平），
			// 必须用 ON DUPLICATE KEY UPDATE 保证幂等；无主键表内部会自动退化为 INSERT IGNORE。
			bw.EnableUpsert()

			s.mu.Lock()

			s.identities[key] = identity

			s.targetSchemas[key] = tgtDB

			s.writers[key] = bw

			s.mu.Unlock()

		}

	}

	// 获取保存的位点

	pos, err := s.checkpointMgr.GetPosition(s.ctx, taskID)
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

	return s.subscriber.Start(s.ctx, pos)

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

	// 恢复外键检查并关闭写入连接
	if s.writeConn != nil {
		s.writeConn.ExecContext(context.Background(), "SET SESSION FOREIGN_KEY_CHECKS=1")
		s.writeConn.Close()
		s.writeConn = nil
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

// snapshotPositionPersister 在写锁外取一份 persister 的"现值"，避免每事件路径都持锁。
// 返回值可能为 nil（未注入或被清除），调用方判空。
func (s *IncrementalSyncService) snapshotPositionPersister() PositionPersister {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positionPersister
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

	// 修复 9/14：无主键 / 无唯一键表事件埋点 + 显式告警（按事件级，量级可控）
	if identity.Strategy == entity.FullColumnsStrategy {
		metrics.GetMetrics().IncrementIncrementalNoPKTableEvents()
		logger.Warn("[NoPK][Task %s] Incremental event on table %s.%s (event=%s, strategy=FullColumns): falling back to full-column WHERE + LIMIT 1; idempotency is best-effort, recommend adding a primary or unique key",
			h.taskID, event.Schema, event.Table, event.EventType)
	}

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

		} else if persist := h.service.snapshotPositionPersister(); persist != nil {

			// 节流回写到任务存档：由 task service 注入的 persister 自行决定写盘频率。
			persist(h.taskID, event.Position)

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

	batchWriter := writer.NewBatchWriterWithConn(h.service.writeConn, identity, 1000, targetSchema)

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

	batchWriter := writer.NewBatchWriterWithConn(h.service.writeConn, identity, 1000, targetSchema)

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
