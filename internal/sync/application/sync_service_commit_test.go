package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metadataEntity "mysql-to-sync/internal/metadata/domain/entity"
	sinkDomain "mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/pkg/binlog"
)

func binlogEventWithCommitPos(schema, table string, eventType binlog.BinlogEventType, commitPos mysql.Position, rows ...map[string]interface{}) *binlog.BinlogEvent {
	event := &binlog.BinlogEvent{
		Schema:         schema,
		Table:          table,
		EventType:      eventType,
		Timestamp:      time.Now(),
		Position:       commitPos,
		CommitPosition: commitPos,
	}
	if eventType == binlog.EventTypeDelete {
		event.BeforeImage = rows
	} else {
		event.Rows = rows
	}
	return event
}

func applyAndCommit(t *testing.T, handler *syncEventHandler, pos mysql.Position, events ...*binlog.BinlogEvent) error {
	t.Helper()
	for _, event := range events {
		if err := handler.OnEvent(event); err != nil {
			return err
		}
	}
	return handler.OnTransactionCommit(pos)
}

func TestSyncEventHandlerUsesCommitPositionForCheckpoint(t *testing.T) {
	handler, cp := newSinkEventHandler(&recordingSink{})
	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 500}
	eventEndPos := mysql.Position{Name: "mysql-bin.000001", Pos: 420}

	err := applyAndCommit(t, handler, commitPos, &binlog.BinlogEvent{
		Schema:         "db",
		Table:          "users",
		EventType:      binlog.EventTypeInsert,
		Rows:           []map[string]interface{}{{"id": int64(1)}},
		Timestamp:      time.Now(),
		Position:       mysql.Position{Name: "mysql-bin.000001", Pos: 100},
		EventEndPos:    eventEndPos,
		CommitPosition: commitPos,
	})
	require.NoError(t, err)

	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, commitPos, saved)
}

func TestSyncEventHandlerHWMSkipStillAdvancesCheckpoint(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(recorder)
	handler.service.identities["db.audit"] = &metadataEntity.TableIdentity{
		TableName:    "audit",
		Strategy:     metadataEntity.FullColumnsStrategy,
		IdentifyCols: []string{"col_a", "col_b"},
	}
	handler.service.SetTableHWMs(map[string]mysql.Position{
		"db.audit": {Name: "mysql-bin.000001", Pos: 600},
	})

	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 500}
	err := applyAndCommit(t, handler, commitPos, binlogEventWithCommitPos(
		"db", "audit", binlog.EventTypeInsert, commitPos,
		map[string]interface{}{"col_a": "x", "col_b": "y"},
	))
	require.NoError(t, err)
	assert.Empty(t, recorder.events, "HWM skip rolls back buffered writes")
	assert.Equal(t, 0, recorder.flushes, "HWM skip must not flush")
	assert.Equal(t, 0, recorder.commits)
	assert.Equal(t, 1, recorder.rollbacks)

	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, commitPos, saved)
}

func TestSyncEventHandlerHWMDoesNotSkipAfterHWM(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(recorder)
	handler.service.identities["db.audit"] = &metadataEntity.TableIdentity{
		TableName:    "audit",
		Strategy:     metadataEntity.FullColumnsStrategy,
		IdentifyCols: []string{"col_a", "col_b"},
	}
	handler.service.SetTableHWMs(map[string]mysql.Position{
		"db.audit": {Name: "mysql-bin.000001", Pos: 400},
	})

	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 500}
	err := applyAndCommit(t, handler, commitPos, binlogEventWithCommitPos(
		"db", "audit", binlog.EventTypeInsert, commitPos,
		map[string]interface{}{"col_a": "x", "col_b": "y"},
	))
	require.NoError(t, err)
	require.Len(t, recorder.events, 1)
	assert.Equal(t, 1, recorder.flushes, "only flush on transaction commit")

	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, commitPos, saved)
}

func TestSyncEventHandlerMissingHWMFailsClosed(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, _ := newSinkEventHandler(recorder)
	handler.service.identities["db.audit"] = &metadataEntity.TableIdentity{
		TableName:    "audit",
		Strategy:     metadataEntity.FullColumnsStrategy,
		IdentifyCols: []string{"col_a", "col_b"},
	}
	// 映射非空但缺少本表：不得静默应用。
	handler.service.SetTableHWMs(map[string]mysql.Position{
		"db.other": {Name: "mysql-bin.000001", Pos: 100},
	})

	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 500}
	err := handler.OnEvent(binlogEventWithCommitPos(
		"db", "audit", binlog.EventTypeInsert, commitPos,
		map[string]interface{}{"col_a": "x", "col_b": "y"},
	))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail-closed")
	assert.Empty(t, recorder.events)
}

func TestRequireNoPKTableHWMs_FailsClosedOnMissing(t *testing.T) {
	svc := NewIncrementalSyncService(nil, nil, nil, nil)
	svc.identities["db.nopk"] = &metadataEntity.TableIdentity{
		TableName: "nopk",
		Strategy:  metadataEntity.FullColumnsStrategy,
	}
	svc.identities["db.pk"] = &metadataEntity.TableIdentity{
		TableName: "pk",
		Strategy:  metadataEntity.PKStrategy,
	}
	err := svc.requireNoPKTableHWMs()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db.nopk")
	assert.Contains(t, err.Error(), "ALL+V2")

	svc.SetTableHWMs(map[string]mysql.Position{
		"db.nopk": {Name: "mysql-bin.000001", Pos: 10},
	})
	require.NoError(t, svc.requireNoPKTableHWMs())
}

func TestSyncEventHandlerSameTransactionEventsShareCommitPosition(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(recorder)
	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 900}

	events := make([]*binlog.BinlogEvent, 0, 2)
	for _, row := range []map[string]interface{}{{"id": int64(1)}, {"id": int64(2)}} {
		events = append(events, binlogEventWithCommitPos("db", "users", binlog.EventTypeInsert, commitPos, row))
	}
	err := applyAndCommit(t, handler, commitPos, events...)
	require.NoError(t, err)

	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, commitPos, saved)
	require.Len(t, recorder.events, 2)
}

func TestSyncEventHandlerDoesNotAdvanceCheckpointUntilTransactionCommit(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(recorder)
	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 900}

	err := handler.OnEvent(binlogEventWithCommitPos(
		"db", "users", binlog.EventTypeInsert, commitPos,
		map[string]interface{}{"id": int64(1)},
	))
	require.NoError(t, err)
	require.Len(t, recorder.events, 1)

	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, mysql.Position{}, saved, "checkpoint must wait for OnTransactionCommit")

	require.NoError(t, handler.OnTransactionCommit(commitPos))
	saved, err = cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, commitPos, saved)
}

func TestSyncEventHandlerMidTransactionFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	failing := &failAfterNSink{failAfter: 2}
	handler, cp := newSinkEventHandler(failing)
	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 900}

	require.NoError(t, handler.OnEvent(binlogEventWithCommitPos(
		"db", "users", binlog.EventTypeInsert, commitPos,
		map[string]interface{}{"id": int64(1)},
	)))
	err := handler.OnEvent(binlogEventWithCommitPos(
		"db", "users", binlog.EventTypeInsert, commitPos,
		map[string]interface{}{"id": int64(2)},
	))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "second write failed")

	saved, getErr := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, getErr)
	assert.Equal(t, mysql.Position{}, saved, "partial transaction must not advance checkpoint")
	assert.Equal(t, 2, failing.writes)
	assert.Equal(t, 0, failing.flushes, "partial transaction must not flush or commit")
	assert.Equal(t, 0, failing.commits)
	assert.Equal(t, 1, failing.rollbacks)
}

func TestSyncEventHandlerHWMCrossTableTxnFiltersByTable(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(recorder)
	handler.service.identities["db.audit"] = &metadataEntity.TableIdentity{
		TableName: "audit", Strategy: metadataEntity.FullColumnsStrategy, IdentifyCols: []string{"a"},
	}
	handler.service.identities["db.ledger"] = &metadataEntity.TableIdentity{
		TableName: "ledger", Strategy: metadataEntity.FullColumnsStrategy, IdentifyCols: []string{"b"},
	}
	handler.service.SetTableHWMs(map[string]mysql.Position{
		"db.audit":  {Name: "mysql-bin.000001", Pos: 600},
		"db.ledger": {Name: "mysql-bin.000001", Pos: 400},
	})
	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 500}
	err := applyAndCommit(t, handler, commitPos,
		binlogEventWithCommitPos("db", "audit", binlog.EventTypeInsert, commitPos, map[string]interface{}{"a": "x"}),
		binlogEventWithCommitPos("db", "ledger", binlog.EventTypeInsert, commitPos, map[string]interface{}{"b": "y"}),
	)
	require.NoError(t, err)
	require.Len(t, recorder.events, 1, "only ledger above HWM should be applied")
	assert.Equal(t, "ledger", recorder.events[0].SourceTable)
	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, commitPos, saved)
}

func TestSyncEventHandlerHWMCrossTableTxnFiltersNoPKBelowHWMWithPKTable(t *testing.T) {
	recorder := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(recorder)
	handler.service.identities["db.users"] = handler.service.identities["db.users"]
	handler.service.identities["db.audit"] = &metadataEntity.TableIdentity{
		TableName: "audit", Strategy: metadataEntity.FullColumnsStrategy, IdentifyCols: []string{"a"},
	}
	handler.service.SetTableHWMs(map[string]mysql.Position{
		"db.audit": {Name: "mysql-bin.000001", Pos: 600},
	})
	commitPos := mysql.Position{Name: "mysql-bin.000001", Pos: 500}
	err := applyAndCommit(t, handler, commitPos,
		binlogEventWithCommitPos("db", "audit", binlog.EventTypeInsert, commitPos, map[string]interface{}{"a": "x"}),
		binlogEventWithCommitPos("db", "users", binlog.EventTypeInsert, commitPos, map[string]interface{}{"id": int64(1)}),
	)
	require.NoError(t, err)
	require.Len(t, recorder.events, 1, "no-PK rows at/below HWM must be filtered while PK rows apply")
	assert.Equal(t, "users", recorder.events[0].SourceTable)
	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, commitPos, saved)
}

func TestSyncEventHandlerMultiSinkReplayOnlyMissingSink(t *testing.T) {
	mysqlSink := &durableRecordingSink{recordingSink: recordingSink{typeValue: sinkDomain.SinkTypeMYSQL}}
	mysqlSink.applied = mysql.Position{Name: "mysql-bin.000001", Pos: 700}
	kafkaSink := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, cp := newSinkEventHandler(mysqlSink, kafkaSink)
	handler.service.identities["db.audit"] = &metadataEntity.TableIdentity{
		TableName: "audit", Strategy: metadataEntity.FullColumnsStrategy, IdentifyCols: []string{"a"},
	}
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 700}
	err := applyAndCommit(t, handler, pos, binlogEventWithCommitPos(
		"db", "audit", binlog.EventTypeInsert, pos, map[string]interface{}{"a": "x"},
	))
	require.NoError(t, err)
	require.Len(t, kafkaSink.events, 1)
	assert.Equal(t, 0, mysqlSink.writes, "MySQL durable mark must not skip other sinks")
	assert.Equal(t, 1, kafkaSink.commits)
	saved, err := cp.GetPosition(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, pos, saved)
}

func TestSyncEventHandlerNonDurableCommitsBeforeDurable(t *testing.T) {
	mysqlSink := &durableRecordingSink{recordingSink: recordingSink{typeValue: sinkDomain.SinkTypeMYSQL}}
	kafkaSink := &recordingSink{typeValue: sinkDomain.SinkTypeKAFKA}
	handler, _ := newSinkEventHandler(kafkaSink, mysqlSink)
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 100}
	err := applyAndCommit(t, handler, pos, binlogEventWithCommitPos(
		"db", "users", binlog.EventTypeInsert, pos, map[string]interface{}{"id": int64(1)},
	))
	require.NoError(t, err)
	assert.Equal(t, []string{"flush", "commit"}, kafkaSink.calls[:2])
	assert.Equal(t, "commit", kafkaSink.calls[1])
	assert.Equal(t, []string{"flush", "mark", "commit"}, mysqlSink.calls)
	assert.Equal(t, "commit", kafkaSink.calls[1], "kafka must commit before mysql durable sink")
}

func TestSyncEventHandlerDurableMarkSkipsNoPKReplayAfterCheckpointFailure(t *testing.T) {
	durable := &durableRecordingSink{}
	handler, memory := newSinkEventHandler(durable)
	handler.service.checkpointMgr = failingPositionManager{Manager: memory}
	handler.service.identities["db.audit"] = &metadataEntity.TableIdentity{
		TableName:    "audit",
		Strategy:     metadataEntity.FullColumnsStrategy,
		IdentifyCols: []string{"col_a", "col_b"},
	}
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 700}

	err := applyAndCommit(t, handler, pos, binlogEventWithCommitPos(
		"db", "audit", binlog.EventTypeInsert, pos,
		map[string]interface{}{"col_a": "x", "col_b": "y"},
	))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint unavailable")
	require.Len(t, durable.events, 1)
	assert.Equal(t, 1, durable.marks)
	assert.Equal(t, 1, durable.commits)

	// 模拟崩溃重放：外部 checkpoint 仍为空，但目标端 durable mark 已提交。
	handler2, memory2 := newSinkEventHandler(durable)
	handler2.service.identities["db.audit"] = handler.service.identities["db.audit"]
	beforeWrites := durable.writes
	err = applyAndCommit(t, handler2, pos, binlogEventWithCommitPos(
		"db", "audit", binlog.EventTypeInsert, pos,
		map[string]interface{}{"col_a": "x", "col_b": "y"},
	))
	require.NoError(t, err)
	assert.Equal(t, beforeWrites, durable.writes, "replay must not write no-PK insert again")
	assert.Equal(t, 1, durable.commits, "replay skip must not commit another sink txn")
	saved, getErr := memory2.GetPosition(context.Background(), "task-1")
	require.NoError(t, getErr)
	assert.Equal(t, pos, saved)
}

type durableRecordingSink struct {
	recordingSink
	applied mysql.Position
	marks   int
	writes  int
}

func (s *durableRecordingSink) Write(ctx context.Context, event *sinkDomain.ChangeEvent) error {
	s.writes++
	return s.recordingSink.Write(ctx, event)
}

func (s *durableRecordingSink) HasAppliedTxn(_ context.Context, _ string, pos mysql.Position) (bool, error) {
	if s.applied.Name == "" {
		return false, nil
	}
	return binlog.ComparePosition(s.applied, pos) >= 0, nil
}

func (s *durableRecordingSink) MarkAppliedTxn(_ context.Context, _ string, pos mysql.Position) error {
	s.marks++
	s.applied = pos
	s.calls = append(s.calls, "mark")
	return nil
}

type failAfterNSink struct {
	writes    int
	flushes   int
	commits   int
	rollbacks int
	failAfter int
	txnActive bool
}

func (s *failAfterNSink) Type() sinkDomain.SinkType  { return sinkDomain.SinkTypeKAFKA }
func (s *failAfterNSink) Open(context.Context) error { return nil }
func (s *failAfterNSink) Write(_ context.Context, _ *sinkDomain.ChangeEvent) error {
	s.writes++
	if s.failAfter > 0 && s.writes >= s.failAfter {
		return errors.New("second write failed")
	}
	return nil
}
func (s *failAfterNSink) Flush(context.Context) error {
	s.flushes++
	return nil
}
func (s *failAfterNSink) Close(context.Context) error { return nil }
func (s *failAfterNSink) BeginTransaction(context.Context) error {
	s.txnActive = true
	return nil
}
func (s *failAfterNSink) CommitTransaction(context.Context) error {
	s.txnActive = false
	s.commits++
	s.flushes++
	return nil
}
func (s *failAfterNSink) RollbackTransaction(context.Context) error {
	s.txnActive = false
	s.rollbacks++
	return nil
}
