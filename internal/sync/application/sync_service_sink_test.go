package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mysql-to-sync/internal/checkpoint"
	metadataEntity "mysql-to-sync/internal/metadata/domain/entity"
	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/pkg/binlog"
)

type recordingSink struct {
	typeValue sinkDomain.SinkType
	events    []*sinkDomain.ChangeEvent
	writeErr  error
	flushErr  error
	commitErr error
	flushes   int
	commits   int
	rollbacks int
	txnActive bool
	// calls 记录 Flush/Commit/Rollback 顺序，用于断言先 Commit 再 SavePosition 的编排。
	calls []string
}

type tableDiscoveryAnalyzer struct {
	tables map[string][]metadataEntity.TableInfo
	calls  []string
}

type failingPositionManager struct {
	checkpoint.Manager
}

type recordingPositionManager struct {
	checkpoint.Manager
	calls *[]string
}

func (f failingPositionManager) SavePosition(context.Context, string, mysql.Position) error {
	return errors.New("checkpoint unavailable")
}

func (r recordingPositionManager) SavePosition(ctx context.Context, taskID string, pos mysql.Position) error {
	if r.calls != nil {
		*r.calls = append(*r.calls, "save_position")
	}
	return r.Manager.SavePosition(ctx, taskID, pos)
}

func (a *tableDiscoveryAnalyzer) AnalyzeTable(string, string) (*metadataEntity.TableIdentity, error) {
	return &metadataEntity.TableIdentity{}, nil
}
func (a *tableDiscoveryAnalyzer) GetAllTables(schema string) ([]metadataEntity.TableInfo, error) {
	a.calls = append(a.calls, schema)
	return a.tables[schema], nil
}
func (a *tableDiscoveryAnalyzer) GetAllDatabases() ([]string, error) { return nil, nil }

func (s *recordingSink) Type() sinkDomain.SinkType  { return s.typeValue }
func (s *recordingSink) Open(context.Context) error { return nil }
func (s *recordingSink) Write(_ context.Context, event *sinkDomain.ChangeEvent) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	if s.txnActive {
		copied := *event
		s.events = append(s.events, &copied)
		return nil
	}
	s.events = append(s.events, event)
	return nil
}
func (s *recordingSink) Flush(context.Context) error {
	s.calls = append(s.calls, "flush")
	s.flushes++
	return s.flushErr
}
func (s *recordingSink) Close(context.Context) error { return nil }
func (s *recordingSink) BeginTransaction(context.Context) error {
	s.txnActive = true
	return nil
}
func (s *recordingSink) CommitTransaction(context.Context) error {
	s.calls = append(s.calls, "commit")
	if s.commitErr != nil {
		return s.commitErr
	}
	s.txnActive = false
	s.commits++
	return nil
}
func (s *recordingSink) RollbackTransaction(context.Context) error {
	s.calls = append(s.calls, "rollback")
	s.txnActive = false
	s.rollbacks++
	s.events = s.events[:0]
	return nil
}

func newSinkEventHandler(sinks ...sinkDomain.Sink) (*syncEventHandler, *checkpoint.MemoryCheckpointManager) {
	cp := checkpoint.NewMemoryCheckpointManager()
	service := &IncrementalSyncService{
		checkpointMgr: cp,
		sinks:         sinks,
		identities: map[string]*metadataEntity.TableIdentity{
			"db.users": {
				TableName:    "users",
				Strategy:     metadataEntity.PKStrategy,
				IdentifyCols: []string{"id"},
				HasPK:        true,
			},
		},
		ctx:       context.Background(),
		auditChan: make(chan *AuditLog, 10),
	}
	return &syncEventHandler{service: service, taskID: "task-1"}, cp
}

func TestSyncEventHandlerWritesAllRowsThenAdvancesCheckpoint(t *testing.T) {
	first := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	second := &recordingSink{typeValue: sinkDomain.SinkTypeHTTPWebhook}
	handler, cp := newSinkEventHandler(first, second)
	position := mysql.Position{Name: "mysql-bin.000001", Pos: 42}

	err := applyAndCommit(t, handler, position, &binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
		Rows:           []map[string]interface{}{{"id": int64(1)}, {"id": int64(2)}},
		Timestamp:      time.Now(),
		Position:       position,
		CommitPosition: position,
	})
	require.NoError(t, err)
	assert.Len(t, first.events, 2)
	assert.Len(t, second.events, 2)
	assert.Equal(t, int64(1), first.events[0].PrimaryKeys["id"])
	assert.Equal(t, 1, first.flushes, "flush once on transaction commit")
	assert.Equal(t, 1, second.flushes)
	assert.Equal(t, 1, first.commits)
	assert.Equal(t, 1, second.commits)
	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, position, saved)
}

func TestSyncEventHandlerDeleteUsesBeforeImages(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(recorder)
	position := mysql.Position{Name: "mysql-bin.000002", Pos: 88}

	err := applyAndCommit(t, handler, position, &binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeDelete,
		BeforeImage:    []map[string]interface{}{{"id": int64(7)}, {"id": int64(8)}},
		Timestamp:      time.Now(),
		Position:       position,
		CommitPosition: position,
	})
	require.NoError(t, err)
	require.Len(t, recorder.events, 2)
	assert.Equal(t, int64(7), recorder.events[0].Before["id"])
	assert.Nil(t, recorder.events[0].After)
	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, position, saved)
}

func TestSyncEventHandlerFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	t.Run("write failure", func(t *testing.T) {
		handler, cp := newSinkEventHandler(&recordingSink{writeErr: errors.New("delivery failed")})
		err := handler.OnEvent(&binlog.BinlogEvent{
			Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
			Rows:           []map[string]interface{}{{"id": int64(1)}},
			Position:       mysql.Position{Name: "mysql-bin.000001", Pos: 100},
			CommitPosition: mysql.Position{Name: "mysql-bin.000001", Pos: 100},
		})
		require.Error(t, err)
		saved, getErr := cp.GetPosition(context.Background(), "task-1")
		require.NoError(t, getErr)
		assert.Equal(t, mysql.Position{}, saved)
	})

	t.Run("flush failure", func(t *testing.T) {
		handler, cp := newSinkEventHandler(&recordingSink{flushErr: errors.New("flush failed")})
		pos := mysql.Position{Name: "mysql-bin.000001", Pos: 100}
		require.NoError(t, handler.OnEvent(&binlog.BinlogEvent{
			Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
			Rows:           []map[string]interface{}{{"id": int64(1)}},
			Position:       pos,
			CommitPosition: pos,
		}))
		err := handler.OnTransactionCommit(pos)
		require.Error(t, err)
		saved, getErr := cp.GetPosition(context.Background(), "task-1")
		require.NoError(t, getErr)
		assert.Equal(t, mysql.Position{}, saved)
	})
}

func TestSyncEventHandlerReturnsCheckpointFailureAfterDelivery(t *testing.T) {
	recorder := &recordingSink{}
	handler, memory := newSinkEventHandler(recorder)
	handler.service.checkpointMgr = failingPositionManager{Manager: memory}
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 100}
	err := applyAndCommit(t, handler, pos, &binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
		Rows:           []map[string]interface{}{{"id": int64(1)}},
		Position:       pos,
		CommitPosition: pos,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint unavailable")
	// Commit 已成功：不得 rollback；数据已落库，重启可能重放（PK/UK upsert 幂等）。
	assert.Equal(t, 1, recorder.commits)
	assert.Equal(t, 0, recorder.rollbacks)
	assert.Len(t, recorder.events, 1)
	saved, getErr := memory.GetPosition(context.Background(), "task-1")
	require.NoError(t, getErr)
	assert.Equal(t, mysql.Position{}, saved)
}

func TestSyncEventHandlerCommitFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	recorder := &recordingSink{commitErr: errors.New("commit failed")}
	handler, cp := newSinkEventHandler(recorder)
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 100}
	err := applyAndCommit(t, handler, pos, &binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
		Rows:           []map[string]interface{}{{"id": int64(1)}},
		Position:       pos,
		CommitPosition: pos,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	assert.Equal(t, 0, recorder.commits)
	assert.Equal(t, 1, recorder.rollbacks, "commit failure must abort/rollback")
	saved, getErr := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, getErr)
	assert.Equal(t, mysql.Position{}, saved, "commit failure must not advance checkpoint")
}

func TestSyncEventHandlerCommitsBeforeSavingCheckpoint(t *testing.T) {
	recorder := &recordingSink{}
	handler, memory := newSinkEventHandler(recorder)
	var order []string
	handler.service.checkpointMgr = recordingPositionManager{Manager: memory, calls: &order}
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 100}
	err := applyAndCommit(t, handler, pos, &binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
		Rows:           []map[string]interface{}{{"id": int64(1)}},
		Position:       pos,
		CommitPosition: pos,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"flush", "commit"}, recorder.calls)
	assert.Equal(t, []string{"save_position"}, order)
	assert.Equal(t, []string{"flush", "commit", "save_position"}, append(append([]string{}, recorder.calls...), order...))
}

func TestSyncEventHandlerMarksDurablePositionBeforeCommit(t *testing.T) {
	durable := &durableRecordingSink{}
	handler, memory := newSinkEventHandler(durable)
	var order []string
	handler.service.checkpointMgr = recordingPositionManager{Manager: memory, calls: &order}
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 100}
	err := applyAndCommit(t, handler, pos, &binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
		Rows:           []map[string]interface{}{{"id": int64(1)}},
		Position:       pos,
		CommitPosition: pos,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"flush", "mark", "commit"}, durable.calls)
	assert.Equal(t, []string{"save_position"}, order)
}

func TestSyncEventHandlerRejectsInvalidEventsWithoutCheckpoint(t *testing.T) {
	tests := []*binlog.BinlogEvent{
		nil,
		{Schema: "db", Table: "users", EventType: binlog.EventTypeInsert},
		{Schema: "db", Table: "users", EventType: binlog.EventTypeUpdate},
		{Schema: "db", Table: "users", EventType: binlog.EventTypeDelete},
		{Schema: "db", Table: "users", EventType: binlog.BinlogEventType("TRUNCATE")},
	}
	for i, event := range tests {
		handler, cp := newSinkEventHandler(&recordingSink{})
		err := handler.OnEvent(event)
		require.Error(t, err, "case %d", i)
		saved, getErr := cp.GetPosition(context.Background(), "task-1")
		require.NoError(t, getErr)
		assert.Equal(t, mysql.Position{}, saved)
	}
}

func TestEventRowCount(t *testing.T) {
	count, err := eventRowCount(&binlog.BinlogEvent{
		EventType:   binlog.EventTypeDelete,
		Rows:        []map[string]interface{}{{"id": 1}},
		BeforeImage: []map[string]interface{}{{"id": 1}, {"id": 2}},
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestResolveTablesByDatabaseDiscoversEachSourceIndependently(t *testing.T) {
	analyzer := &tableDiscoveryAnalyzer{tables: map[string][]metadataEntity.TableInfo{
		"db1": {{TableName: "users"}, {TableName: "orders"}},
		"db2": {{TableName: "audit"}},
	}}
	resolved, err := resolveTablesByDatabase(analyzer, map[string]string{"db1": "target1", "db2": "target2"}, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"db1", "db2"}, analyzer.calls)
	assert.Equal(t, []string{"users", "orders"}, resolved["db1"])
	assert.Equal(t, []string{"audit"}, resolved["db2"])
}

func TestResolveTablesByDatabaseUsesDeduplicatedConfiguredTables(t *testing.T) {
	analyzer := &tableDiscoveryAnalyzer{}
	resolved, err := resolveTablesByDatabase(analyzer, map[string]string{"db1": "target1", "db2": "target2"}, []string{"users", "users", "orders"})
	require.NoError(t, err)
	assert.Empty(t, analyzer.calls)
	assert.Equal(t, []string{"users", "orders"}, resolved["db1"])
	assert.Equal(t, []string{"users", "orders"}, resolved["db2"])
}
