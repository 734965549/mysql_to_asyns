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
	flushes   int
}

type tableDiscoveryAnalyzer struct {
	tables map[string][]metadataEntity.TableInfo
	calls  []string
}

type failingPositionManager struct {
	checkpoint.Manager
}

func (f failingPositionManager) SavePosition(context.Context, string, mysql.Position) error {
	return errors.New("checkpoint unavailable")
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
	s.events = append(s.events, event)
	return nil
}
func (s *recordingSink) Flush(context.Context) error {
	s.flushes++
	return s.flushErr
}
func (s *recordingSink) Close(context.Context) error { return nil }

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

	err := handler.OnEvent(&binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
		Rows:      []map[string]interface{}{{"id": int64(1)}, {"id": int64(2)}},
		Timestamp: time.Now(), Position: position,
	})
	require.NoError(t, err)
	assert.Len(t, first.events, 2)
	assert.Len(t, second.events, 2)
	assert.Equal(t, int64(1), first.events[0].PrimaryKeys["id"])
	assert.Equal(t, 1, first.flushes)
	assert.Equal(t, 1, second.flushes)
	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, position, saved)
}

func TestSyncEventHandlerDeleteUsesBeforeImages(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(recorder)
	position := mysql.Position{Name: "mysql-bin.000002", Pos: 88}

	err := handler.OnEvent(&binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeDelete,
		BeforeImage: []map[string]interface{}{{"id": int64(7)}, {"id": int64(8)}},
		Timestamp:   time.Now(), Position: position,
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
	tests := []struct {
		name  string
		sinks func() []sinkDomain.Sink
	}{
		{
			name: "write failure",
			sinks: func() []sinkDomain.Sink {
				return []sinkDomain.Sink{&recordingSink{writeErr: errors.New("delivery failed")}}
			},
		},
		{
			name: "flush failure",
			sinks: func() []sinkDomain.Sink {
				return []sinkDomain.Sink{&recordingSink{flushErr: errors.New("flush failed")}}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, cp := newSinkEventHandler(tt.sinks()...)
			err := handler.OnEvent(&binlog.BinlogEvent{
				Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
				Rows:     []map[string]interface{}{{"id": int64(1)}},
				Position: mysql.Position{Name: "mysql-bin.000001", Pos: 100},
			})
			require.Error(t, err)
			saved, getErr := cp.GetPosition(context.Background(), "task-1")
			require.NoError(t, getErr)
			assert.Equal(t, mysql.Position{}, saved)
		})
	}
}

func TestSyncEventHandlerReturnsCheckpointFailureAfterDelivery(t *testing.T) {
	recorder := &recordingSink{}
	handler, memory := newSinkEventHandler(recorder)
	handler.service.checkpointMgr = failingPositionManager{Manager: memory}
	err := handler.OnEvent(&binlog.BinlogEvent{
		Schema: "db", Table: "users", EventType: binlog.EventTypeInsert,
		Rows:     []map[string]interface{}{{"id": int64(1)}},
		Position: mysql.Position{Name: "mysql-bin.000001", Pos: 100},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint unavailable")
	assert.Len(t, recorder.events, 1)
	saved, getErr := memory.GetPosition(context.Background(), "task-1")
	require.NoError(t, getErr)
	assert.Equal(t, mysql.Position{}, saved)
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
