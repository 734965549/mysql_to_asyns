package application

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"mysql-to-sync/internal/checkpoint"
	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/internal/metadata/domain/service"
	"mysql-to-sync/internal/sync/domain/sink"
	infraSink "mysql-to-sync/internal/sync/infrastructure/sink"
	"mysql-to-sync/pkg/binlog"
	"mysql-to-sync/pkg/logger"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// PositionPersister 位点回写回调：整事务成功 flush 并写完 checkpoint 后被调用，由调用方（task service）决定如何
// 把位点冗余持久化到任务存档（用于离线审计 / 阶段状态判断）。回调内部应自带节流，避免每个 binlog 事务都触发存储写。
// 回调失败不会反向影响增量同步本身。
type PositionPersister func(taskID string, pos mysql.Position)

// IncrementalSyncService replays ROW binlog events into the target database.
//
// It owns binlog subscription, target writers, table identities, and incremental
// checkpoint writes. It does not own task lifecycle; TaskService starts/stops it
// and optionally receives throttled position snapshots for task archives.
type IncrementalSyncService struct {
	sourceDB *sql.DB
	targetDB *sql.DB

	analyzer service.IdentityAnalyzer

	checkpointMgr checkpoint.Manager

	// positionPersister 可选位点回写回调，由 task service 注入用于把增量位点冗余到任务存档。
	// nil 时不做任何回写，行为与历史版本一致。
	positionPersister PositionPersister

	subscriber *binlog.Subscriber

	sinks []sink.Sink

	// identities 保留在 service 中，供 OnEvent 的 normalizer 使用。与 MySQLSink 内部 identities 独立。
	identities map[string]*entity.TableIdentity

	// tableHWMs 表级高水位，key 为 "schema.table"。由 P0-2 注入；未设置时不启用过滤。
	tableHWMs map[string]mysql.Position

	mu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	auditChan chan *AuditLog
	wg        sync.WaitGroup
}

// SetPositionPersister 注入位点回写回调。允许在 Start 前调用，重复调用会覆盖旧值。
// 传 nil 可以清除已注入的回调。
func (s *IncrementalSyncService) SetPositionPersister(p PositionPersister) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.positionPersister = p
}

// SetTableHWMs 注入表级 binlog 高水位。传 nil 可清除；P0-2 负责从任务存档加载并调用。
func (s *IncrementalSyncService) SetTableHWMs(hwms map[string]mysql.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if hwms == nil {
		s.tableHWMs = nil
		return
	}
	copied := make(map[string]mysql.Position, len(hwms))
	for key, pos := range hwms {
		copied[key] = pos
	}
	s.tableHWMs = copied
}

func (s *IncrementalSyncService) snapshotTableHWMs() map[string]mysql.Position {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tableHWMs
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
func NewIncrementalSyncService(
	sourceDB, targetDB *sql.DB,
	analyzer service.IdentityAnalyzer,
	checkpointMgr checkpoint.Manager,
) *IncrementalSyncService {
	return &IncrementalSyncService{
		sourceDB:      sourceDB,
		targetDB:      targetDB,
		analyzer:      analyzer,
		checkpointMgr: checkpointMgr,
		identities:    make(map[string]*entity.TableIdentity),
		auditChan:     make(chan *AuditLog, 1000),
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

	// 按源库解析表列表。库级同步时每个库的表集合可能不同，不能把
	// 第一个库的表名复用到其他库，否则事件会因缺少 identity 被跳过。
	tablesByDatabase, err := resolveTablesByDatabase(s.analyzer, dbMapping, config.Tables)
	if err != nil {
		return err
	}
	for srcDB, tables := range tablesByDatabase {

		// 分析表结构，填充 identities（供 OnEvent 的 normalizer 使用）。
		for _, tableName := range tables {
			identity, err := s.analyzer.AnalyzeTable(srcDB, tableName)
			if err != nil {
				return fmt.Errorf("analyze table %s.%s: %w", srcDB, tableName, err)
			}
			key := fmt.Sprintf("%s.%s", srcDB, tableName)
			s.identities[key] = identity
		}
	}

	if config.RequireNoPKTableHWM {
		if err := s.requireNoPKTableHWMs(); err != nil {
			return err
		}
	}

	// 创建 Sink 实例
	factoryDeps := infraSink.SinkDeps{
		TargetDB:  s.targetDB,
		Analyzer:  s.analyzer,
		BatchSize: config.BatchSize,
	}
	sinks, err := infraSink.NewSinks(config.SinkConfigs, factoryDeps)
	if err != nil {
		return fmt.Errorf("failed to create sinks: %w", err)
	}
	s.sinks = sinks

	// 打开所有 Sink
	for _, snk := range s.sinks {
		if err := snk.Open(s.ctx); err != nil {
			return fmt.Errorf("failed to open sink %s: %w", snk.Type(), err)
		}
	}

	// 对实现了 TablePreparer 的 Sink 调用 PrepareTables
	for _, snk := range s.sinks {
		if preparer, ok := snk.(sink.TablePreparer); ok {
			for srcDB, tables := range tablesByDatabase {
				mapping := map[string]string{srcDB: dbMapping[srcDB]}
				if err := preparer.PrepareTables(s.ctx, mapping, tables); err != nil {
					return fmt.Errorf("failed to prepare tables for sink %s and source %s: %w", snk.Type(), srcDB, err)
				}
			}
		}
	}

	// 获取保存的位点
	pos, err := s.checkpointMgr.GetPosition(s.ctx, taskID)
	if err != nil {
		logger.Warn("failed to get checkpoint: %v", err)
	}

	// 创建Binlog订阅器
	s.subscriber = binlog.NewSubscriber(&binlog.SubscriberConfig{
		Host:                config.SourceHost,
		Port:                config.SourcePort,
		Username:            config.SourceUsername,
		Password:            config.SourcePassword,
		Database:            config.SourceSchema,
		Databases:           sourceDBs,
		Tables:              config.Tables,
		ServerID:            config.ServerID,
		MaxTxnBufferedRows:  config.MaxTxnBufferedRows,
		MaxTxnBufferedBytes: config.MaxTxnBufferedBytes,
		TxnSpillDir:         config.TxnSpillDir,
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

	// 关闭所有 Sink
	for _, snk := range s.sinks {
		snk.Close(context.Background())
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

// getIdentity 获取表标识
func (s *IncrementalSyncService) getIdentity(tableName string) *entity.TableIdentity {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.identities[tableName]
}

func (s *IncrementalSyncService) getTableHWM(tableKey string) (mysql.Position, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.tableHWMs == nil {
		return mysql.Position{}, false
	}
	pos, ok := s.tableHWMs[tableKey]
	return pos, ok
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

	SourceHost     string
	SourcePort     int
	SourceUsername string
	SourcePassword string

	SourceSchema string
	TargetSchema string

	SourceDatabases []string
	TargetDatabases []string

	Tables []string

	BatchSize int
	ServerID  uint32

	// MaxTxnBufferedRows / MaxTxnBufferedBytes / TxnSpillDir 透传给 binlog Subscriber：
	// 内存软上限，超限溢写磁盘，避免超大事务硬失败形成毒事务。0 表示使用默认值。
	MaxTxnBufferedRows  int
	MaxTxnBufferedBytes int64
	TxnSpillDir         string

	SinkConfigs []sink.SinkConfig

	// RequireNoPKTableHWM 为 true 时（ALL + full_load_engine=v2），每张 FullColumnsStrategy 表必须有合法表级 HWM，否则拒绝启动。
	// V1 ALL 不设此标志：V1 全量不落盘表级 HWM，强校验会导致增量永久无法启动。
	RequireNoPKTableHWM bool
}

// syncEventHandler 同步事件处理器
type syncEventHandler struct {
	service *IncrementalSyncService
	taskID  string
	txnOpen bool
	// sinkSkipApply[i] 为 true 表示该 sink 已持久化本源事务位点，本事务跳过对其写入/提交。
	sinkSkipApply []bool
	// txnHasWrites 表示本事务至少有一条事件实际写入了 sink（未被表级 HWM 过滤）。
	txnHasWrites bool
}

// OnEvent 处理Binlog事件。只负责写入，不 flush、不推进 checkpoint。
// 同一源事务的全部事件在 OnTransactionCommit 中统一 flush、提交目标端事务，再保存 checkpoint。
func (h *syncEventHandler) OnEvent(event *binlog.BinlogEvent) error {
	if event == nil {
		return fmt.Errorf("binlog event is required")
	}
	key := fmt.Sprintf("%s.%s", event.Schema, event.Table)

	identity := h.service.getIdentity(key)
	if identity == nil {
		return nil // 不是我们需要同步的表
	}

	skipByHWM, err := h.eventSkipByHWM(event, identity)
	if err != nil {
		return err
	}
	if skipByHWM {
		h.addEventAudit(event, nil)
		return nil
	}

	if err := h.ensureSinkTransaction(commitPositionOf(event)); err != nil {
		return err
	}

	if !h.allSinksSkipped() {
		rowCount, rowCountErr := eventRowCount(event)
		if rowCountErr != nil {
			h.abortSinkTransaction()
			return rowCountErr
		}

		// 逐行处理：将每行转为 ChangeEvent 并写入尚未跳过的 Sink。
		for i := 0; i < rowCount; i++ {
			se := h.singleRowEvent(event, i)
			ce, err := ToChangeEvent(se, identity, h.taskID, i)
			if err != nil {
				h.abortSinkTransaction()
				return err
			}

			for idx, snk := range h.service.sinks {
				if h.sinkSkipApply[idx] {
					continue
				}
				if err := snk.Write(h.service.ctx, ce); err != nil {
					h.addEventAudit(event, err)
					h.abortSinkTransaction()
					return err
				}
			}
		}
		h.txnHasWrites = true
	}

	h.addEventAudit(event, nil)
	return nil
}

// OnTransactionCommit 在同一事务全部事件成功写入后，统一 flush、提交目标端事务，再推进 checkpoint。
//
// 顺序必须是 Flush → MarkAppliedTxn(目标事务内) → Commit → SavePosition：
// 若先推进 checkpoint 再 Commit，目标事务失败会造成丢数据。
// MySQL sink 在同一目标事务内持久化源位点；Commit 成功但 SavePosition 失败时，
// 重启重放会通过 HasAppliedTxn 跳过写入，避免无主键 INSERT 重复。
func (h *syncEventHandler) OnTransactionCommit(pos mysql.Position) error {
	defer h.resetTxnState()

	if !h.txnHasWrites || h.allSinksSkipped() {
		h.abortSinkTransaction()
		return h.saveExternalCheckpoint(pos)
	}

	hadTxn := h.txnOpen && h.anySinkNeedsWork()
	for idx, snk := range h.service.sinks {
		if h.sinkSkipApply[idx] {
			continue
		}
		if err := snk.Flush(h.service.ctx); err != nil {
			h.abortSinkTransaction()
			return fmt.Errorf("flush sinks on transaction commit failed: %w", err)
		}
	}

	if hadTxn {
		for idx, snk := range h.service.sinks {
			if h.sinkSkipApply[idx] {
				continue
			}
			if d, ok := snk.(sink.DurableTxnPositionSink); ok {
				if err := d.MarkAppliedTxn(h.service.ctx, h.taskID, pos); err != nil {
					h.abortSinkTransaction()
					return fmt.Errorf("mark applied txn position failed: %w", err)
				}
			}
		}

		// 先提交非 durable sink，最后提交 durable sink（如 MySQL），避免外部 sink 失败时 MySQL 已持久化 mark。
		for idx, snk := range h.service.sinks {
			if h.sinkSkipApply[idx] {
				continue
			}
			if _, isDurable := snk.(sink.DurableTxnPositionSink); isDurable {
				continue
			}
			if ts, ok := snk.(sink.TransactionalSink); ok {
				if err := ts.CommitTransaction(h.service.ctx); err != nil {
					h.abortSinkTransaction()
					return fmt.Errorf("commit sink transaction failed: %w", err)
				}
			}
		}
		for idx, snk := range h.service.sinks {
			if h.sinkSkipApply[idx] {
				continue
			}
			if _, ok := snk.(sink.DurableTxnPositionSink); !ok {
				continue
			}
			if ts, ok := snk.(sink.TransactionalSink); ok {
				if err := ts.CommitTransaction(h.service.ctx); err != nil {
					h.abortSinkTransaction()
					return fmt.Errorf("commit durable sink transaction failed: %w", err)
				}
			}
		}
	}

	if err := h.saveExternalCheckpoint(pos); err != nil {
		logger.Error("[Task %s] Failed to save checkpoint at %s:%d after sink commit "+
			"(MySQL durable txn mark should make no-PK replay skip inserts; PK/UK upsert remains idempotent): %v",
			h.taskID, pos.Name, pos.Pos, err)
		return err
	}
	return nil
}

func (h *syncEventHandler) saveExternalCheckpoint(pos mysql.Position) error {
	ctx := context.Background()
	if cpErr := h.service.checkpointMgr.SavePosition(ctx, h.taskID, pos); cpErr != nil {
		return fmt.Errorf("save checkpoint failed: %w", cpErr)
	}
	if persist := h.service.snapshotPositionPersister(); persist != nil {
		persist(h.taskID, pos)
	}
	return nil
}

func (h *syncEventHandler) ensureSinkTransaction(commitPos mysql.Position) error {
	h.initSinkSkipApply()

	if h.txnOpen {
		return nil
	}

	needsBegin := false
	for idx, snk := range h.service.sinks {
		if d, ok := snk.(sink.DurableTxnPositionSink); ok {
			applied, err := d.HasAppliedTxn(h.service.ctx, h.taskID, commitPos)
			if err != nil {
				return fmt.Errorf("check applied txn position for sink %s: %w", snk.Type(), err)
			}
			if applied {
				h.sinkSkipApply[idx] = true
				logger.Info("[Task %s] Skipping already-applied source transaction at %s:%d for sink %s (per-sink durable mark)",
					h.taskID, commitPos.Name, commitPos.Pos, snk.Type())
				continue
			}
		}
		if _, ok := snk.(sink.TransactionalSink); ok && !h.sinkSkipApply[idx] {
			needsBegin = true
		}
	}

	if needsBegin {
		for idx, snk := range h.service.sinks {
			if h.sinkSkipApply[idx] {
				continue
			}
			if ts, ok := snk.(sink.TransactionalSink); ok {
				if err := ts.BeginTransaction(h.service.ctx); err != nil {
					h.abortSinkTransaction()
					return fmt.Errorf("begin sink transaction: %w", err)
				}
			}
		}
	}
	h.txnOpen = true
	return nil
}

func (h *syncEventHandler) abortSinkTransaction() {
	h.initSinkSkipApply()
	for idx, snk := range h.service.sinks {
		if h.sinkSkipApply[idx] {
			continue
		}
		if ts, ok := snk.(sink.TransactionalSink); ok {
			if err := ts.RollbackTransaction(h.service.ctx); err != nil {
				logger.Error("[Task %s] Failed to rollback sink %s transaction: %v", h.taskID, snk.Type(), err)
			}
		}
	}
	h.txnOpen = false
}

func (h *syncEventHandler) initSinkSkipApply() {
	if h.sinkSkipApply != nil {
		return
	}
	h.sinkSkipApply = make([]bool, len(h.service.sinks))
}

func (h *syncEventHandler) allSinksSkipped() bool {
	if len(h.service.sinks) == 0 {
		return true
	}
	h.initSinkSkipApply()
	for idx := range h.service.sinks {
		if !h.sinkSkipApply[idx] {
			return false
		}
	}
	return true
}

func (h *syncEventHandler) anySinkNeedsWork() bool {
	return !h.allSinksSkipped()
}

func (h *syncEventHandler) resetTxnState() {
	h.txnOpen = false
	h.sinkSkipApply = nil
	h.txnHasWrites = false
}

func (h *syncEventHandler) addEventAudit(event *binlog.BinlogEvent, err error) {
	auditLog := &AuditLog{
		TaskID:    h.taskID,
		TableName: event.Table,
		EventType: string(event.EventType),
		Timestamp: time.Now(),
		Success:   err == nil,
	}
	if err != nil {
		auditLog.Error = err.Error()
	}
	h.service.addAuditLog(auditLog)
}

func commitPositionOf(event *binlog.BinlogEvent) mysql.Position {
	if event.CommitPosition.Name != "" || event.CommitPosition.Pos > 0 {
		return event.CommitPosition
	}
	return event.Position
}

// eventSkipByHWM 按表级 HWM 判断本条 no-PK 事件是否已被全量快照覆盖。
// PK/UK 表不受 HWM 影响；跨表事务中各表独立过滤，剩余事件仍在同一目标事务提交。
func (h *syncEventHandler) eventSkipByHWM(event *binlog.BinlogEvent, identity *entity.TableIdentity) (bool, error) {
	if identity == nil || identity.Strategy != entity.FullColumnsStrategy {
		return false, nil
	}
	hwms := h.service.snapshotTableHWMs()
	if len(hwms) == 0 {
		return false, nil
	}
	key := fmt.Sprintf("%s.%s", event.Schema, event.Table)
	hwm, ok := hwms[key]
	if !ok || hwm.Name == "" {
		return false, fmt.Errorf("missing or invalid table binlog HWM for no-PK table %s (fail-closed)", key)
	}
	if err := binlog.ValidatePosition(hwm); err != nil {
		return false, fmt.Errorf("invalid table binlog HWM for no-PK table %s: %w", key, err)
	}
	commitPos := commitPositionOf(event)
	return binlog.ComparePosition(commitPos, hwm) <= 0, nil
}

// requireNoPKTableHWMs 校验 identities 中每张无 PK/UK 表都有合法 HWM。
func (s *IncrementalSyncService) requireNoPKTableHWMs() error {
	hwms := s.snapshotTableHWMs()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var missing []string
	for key, id := range s.identities {
		if id == nil || id.Strategy != entity.FullColumnsStrategy {
			continue
		}
		hwm, ok := hwms[key]
		if !ok || hwm.Name == "" {
			missing = append(missing, key)
			continue
		}
		if err := binlog.ValidatePosition(hwm); err != nil {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("ALL+V2 mode requires valid table binlog HWM for no-PK table(s) %v (fail-closed)", missing)
}

// singleRowEvent 从多行事件中提取单行，用于 normalizer 逐行转换
func (h *syncEventHandler) singleRowEvent(event *binlog.BinlogEvent, idx int) *binlog.BinlogEvent {
	se := &binlog.BinlogEvent{
		Table:          event.Table,
		Schema:         event.Schema,
		EventType:      event.EventType,
		Timestamp:      event.Timestamp,
		Position:       event.Position,
		EventEndPos:    event.EventEndPos,
		CommitPosition: event.CommitPosition,
	}
	if idx < len(event.Rows) {
		se.Rows = []map[string]interface{}{event.Rows[idx]}
	}
	if idx < len(event.BeforeImage) {
		se.BeforeImage = []map[string]interface{}{event.BeforeImage[idx]}
	}
	return se
}

func eventRowCount(event *binlog.BinlogEvent) (int, error) {
	switch event.EventType {
	case binlog.EventTypeInsert:
		if len(event.Rows) == 0 {
			return 0, fmt.Errorf("INSERT event for %s.%s has no rows", event.Schema, event.Table)
		}
		return len(event.Rows), nil
	case binlog.EventTypeUpdate:
		if len(event.Rows) == 0 {
			return 0, fmt.Errorf("UPDATE event for %s.%s has no rows", event.Schema, event.Table)
		}
		return len(event.Rows), nil
	case binlog.EventTypeDelete:
		if len(event.BeforeImage) > 0 {
			return len(event.BeforeImage), nil
		}
		if len(event.Rows) > 0 {
			return len(event.Rows), nil
		}
		return 0, fmt.Errorf("DELETE event for %s.%s has no rows", event.Schema, event.Table)
	default:
		return 0, fmt.Errorf("unknown binlog event type: %s", event.EventType)
	}
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func resolveTablesByDatabase(analyzer service.IdentityAnalyzer, dbMapping map[string]string, configuredTables []string) (map[string][]string, error) {
	if analyzer == nil {
		return nil, fmt.Errorf("identity analyzer is required")
	}
	result := make(map[string][]string, len(dbMapping))
	for srcDB := range dbMapping {
		tables := uniqueStrings(configuredTables)
		if len(tables) == 0 {
			allTables, err := analyzer.GetAllTables(srcDB)
			if err != nil {
				return nil, fmt.Errorf("list tables for %s: %w", srcDB, err)
			}
			tables = make([]string, 0, len(allTables))
			for _, table := range allTables {
				tables = append(tables, table.TableName)
			}
			tables = uniqueStrings(tables)
		}
		result[srcDB] = tables
	}
	return result, nil
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
