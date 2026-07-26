package binlog

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingHandler struct {
	events  []*BinlogEvent
	commits []mysql.Position
}

func (h *recordingHandler) OnEvent(event *BinlogEvent) error {
	cp := *event
	h.events = append(h.events, &cp)
	return nil
}

func (h *recordingHandler) OnTransactionCommit(pos mysql.Position) error {
	h.commits = append(h.commits, pos)
	return nil
}

type failingAfterNHandler struct {
	recordingHandler
	failAfter int
	seen      int
}

func (h *failingAfterNHandler) OnEvent(event *BinlogEvent) error {
	h.seen++
	if err := h.recordingHandler.OnEvent(event); err != nil {
		return err
	}
	if h.failAfter > 0 && h.seen >= h.failAfter {
		return fmt.Errorf("inject failure after event %d", h.seen)
	}
	return nil
}

func TestHandlerBuffersRowsUntilXID(t *testing.T) {
	sub := NewSubscriber(&SubscriberConfig{})
	rec := &recordingHandler{}
	sub.AddHandler(rec)
	h := &binlogHandler{subscriber: sub, currentFile: "mysql-bin.000001"}

	table := &schema.Table{
		Schema:  "db",
		Name:    "t",
		Columns: []schema.TableColumn{{Name: "id"}},
	}
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(1)}},
		Header: &replication.EventHeader{LogPos: 100, Timestamp: uint32(time.Now().Unix())},
	}))
	assert.Empty(t, rec.events, "rows must stay buffered until XID")

	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(2)}},
		Header: &replication.EventHeader{LogPos: 200},
	}))
	assert.Empty(t, rec.events)

	commit := mysql.Position{Name: "mysql-bin.000001", Pos: 300}
	require.NoError(t, h.OnXID(nil, commit))
	require.Len(t, rec.events, 2)
	require.Equal(t, []mysql.Position{commit}, rec.commits)
	for _, ev := range rec.events {
		assert.Equal(t, commit, ev.CommitPosition)
		assert.Equal(t, commit, ev.Position)
		assert.Equal(t, "mysql-bin.000001", ev.EventEndPos.Name)
	}
	assert.Equal(t, uint32(100), rec.events[0].EventEndPos.Pos)
	assert.Equal(t, uint32(200), rec.events[1].EventEndPos.Pos)
	assert.Equal(t, commit, sub.GetPosition())
}

func TestHandlerDoesNotCommitWhenMidTransactionEventFails(t *testing.T) {
	sub := NewSubscriber(&SubscriberConfig{})
	rec := &failingAfterNHandler{failAfter: 2}
	sub.AddHandler(rec)
	h := &binlogHandler{subscriber: sub, currentFile: "mysql-bin.000001"}

	table := &schema.Table{
		Schema:  "db",
		Name:    "t",
		Columns: []schema.TableColumn{{Name: "id"}},
	}
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(1)}},
		Header: &replication.EventHeader{LogPos: 100},
	}))
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(2)}},
		Header: &replication.EventHeader{LogPos: 200},
	}))

	commit := mysql.Position{Name: "mysql-bin.000001", Pos: 300}
	err := h.OnXID(nil, commit)
	require.Error(t, err)
	require.Len(t, rec.events, 2, "first event must be applied before second fails")
	assert.Empty(t, rec.commits, "commit callback must not run when a later event fails")
	assert.Equal(t, 1, h.buffer().inMemoryLen(), "failed event and remaining buffer must be retained")
	assert.Equal(t, uint32(200), h.buffer().events[0].EventEndPos.Pos)
}

func TestHandlerCommitsEmptyTransaction(t *testing.T) {
	sub := NewSubscriber(&SubscriberConfig{})
	rec := &recordingHandler{}
	sub.AddHandler(rec)
	h := &binlogHandler{subscriber: sub, currentFile: "mysql-bin.000001"}

	commit := mysql.Position{Name: "mysql-bin.000001", Pos: 400}
	require.NoError(t, h.OnXID(nil, commit))
	assert.Empty(t, rec.events)
	assert.Equal(t, []mysql.Position{commit}, rec.commits)
	assert.Equal(t, commit, sub.GetPosition())
}

func TestHandlerTracksRotateFileName(t *testing.T) {
	h := &binlogHandler{currentFile: "mysql-bin.000001"}
	require.NoError(t, h.OnRotate(nil, &replication.RotateEvent{
		NextLogName: []byte("mysql-bin.000002"),
		Position:    4,
	}))
	assert.Equal(t, "mysql-bin.000002", h.currentFile)
}

func TestHandlerSpillsOversizedTransactionAndFlushesOnXID(t *testing.T) {
	dir := t.TempDir()
	sub := NewSubscriber(&SubscriberConfig{})
	rec := &recordingHandler{}
	sub.AddHandler(rec)
	h := &binlogHandler{
		subscriber:  sub,
		currentFile: "mysql-bin.000001",
		txnBuf:      newTxnEventBuffer(1, 1<<20, dir),
	}
	table := &schema.Table{
		Schema:  "db",
		Name:    "t",
		Columns: []schema.TableColumn{{Name: "id"}},
	}
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(1)}, {int64(2)}},
		Header: &replication.EventHeader{LogPos: 100},
	}))
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(3)}, {int64(4)}},
		Header: &replication.EventHeader{LogPos: 200},
	}))
	assert.Equal(t, 0, h.buffer().inMemoryLen(), "memory buffer should be spilled")
	assert.Equal(t, 2, h.buffer().spillCount)

	commit := mysql.Position{Name: "mysql-bin.000001", Pos: 300}
	require.NoError(t, h.OnXID(nil, commit))
	require.Len(t, rec.events, 2)
	assert.Equal(t, []mysql.Position{commit}, rec.commits)
	assert.Equal(t, int64(1), rec.events[0].Rows[0]["id"])
	assert.Equal(t, int64(3), rec.events[1].Rows[0]["id"])
	assert.Equal(t, 0, h.buffer().spillCount)
	assert.Equal(t, 0, h.buffer().inMemoryLen())
}

func TestHandlerSpillsWhenEstimatedBytesExceeded(t *testing.T) {
	dir := t.TempDir()
	h := &binlogHandler{
		subscriber:  NewSubscriber(&SubscriberConfig{}),
		currentFile: "mysql-bin.000001",
		txnBuf:      newTxnEventBuffer(100_000, 64, dir),
	}
	t.Cleanup(func() { h.buffer().reset() })
	blob := make([]byte, 128)
	table := &schema.Table{
		Schema:  "db",
		Name:    "t",
		Columns: []schema.TableColumn{{Name: "payload"}},
	}
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{blob}},
		Header: &replication.EventHeader{LogPos: 100},
	}))
	assert.Equal(t, 0, h.buffer().inMemoryLen())
	assert.Equal(t, 1, h.buffer().spillCount)
}

func TestSubscriberStopDoesNotResetBufferWhileRunning(t *testing.T) {
	dir := t.TempDir()
	sub := NewSubscriber(&SubscriberConfig{TxnSpillDir: dir})
	h := &binlogHandler{
		subscriber:  sub,
		currentFile: "mysql-bin.000001",
		txnBuf:      newTxnEventBuffer(1, 1<<20, dir),
	}
	sub.eventHandler = h

	table := &schema.Table{
		Schema:  "db",
		Name:    "t",
		Columns: []schema.TableColumn{{Name: "id"}},
	}
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(1)}, {int64(2)}},
		Header: &replication.EventHeader{LogPos: 100},
	}))
	require.NotNil(t, h.buffer().spillFile)
	spillName := h.buffer().spillFile.Name()
	_, err := os.Stat(spillName)
	require.NoError(t, err, "spill file must exist before Stop")

	sub.running = true
	sub.Stop()

	assert.NotNil(t, h.buffer().spillFile, "Stop must not reset buffer while RunFrom may still be active")
	_, err = os.Stat(spillName)
	require.NoError(t, err, "Stop must not remove spill file")

	sub.closeTxnSpill()
	assert.Nil(t, h.buffer().spillFile)
	_, err = os.Stat(spillName)
	assert.True(t, os.IsNotExist(err), "Start defer path must remove spill file after RunFrom exits")
}

func TestHandlerOnPosSyncedFlushesNonXIDBoundary(t *testing.T) {
	sub := NewSubscriber(&SubscriberConfig{})
	rec := &recordingHandler{}
	sub.AddHandler(rec)
	h := &binlogHandler{subscriber: sub, currentFile: "mysql-bin.000001"}

	table := &schema.Table{
		Schema:  "db",
		Name:    "t",
		Columns: []schema.TableColumn{{Name: "id"}},
	}
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(1)}},
		Header: &replication.EventHeader{LogPos: 100},
	}))
	assert.Empty(t, rec.events)

	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 200}
	require.NoError(t, h.OnPosSynced(nil, pos, nil, false))
	require.Len(t, rec.events, 1)
	require.Equal(t, []mysql.Position{pos}, rec.commits)
}

func TestHandlerOnPosSyncedForceAdvancesCheckpointWithoutRows(t *testing.T) {
	sub := NewSubscriber(&SubscriberConfig{})
	rec := &recordingHandler{}
	sub.AddHandler(rec)
	h := &binlogHandler{subscriber: sub, currentFile: "mysql-bin.000001"}

	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 400}
	require.NoError(t, h.OnPosSynced(nil, pos, nil, true))
	assert.Empty(t, rec.events)
	require.Equal(t, []mysql.Position{pos}, rec.commits)
	assert.Equal(t, pos, sub.GetPosition())
}

func TestHandlerOnDDLFlushesBufferedRows(t *testing.T) {
	sub := NewSubscriber(&SubscriberConfig{})
	rec := &recordingHandler{}
	sub.AddHandler(rec)
	h := &binlogHandler{subscriber: sub, currentFile: "mysql-bin.000001"}

	table := &schema.Table{
		Schema:  "db",
		Name:    "t",
		Columns: []schema.TableColumn{{Name: "id"}},
	}
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(1)}},
		Header: &replication.EventHeader{LogPos: 100},
	}))

	ddlPos := mysql.Position{Name: "mysql-bin.000001", Pos: 500}
	require.NoError(t, h.OnDDL(nil, ddlPos, nil))
	require.Len(t, rec.events, 1)
	require.Equal(t, []mysql.Position{ddlPos}, rec.commits)
}

func TestParseAndFormatPosition(t *testing.T) {
	pos, err := ParsePosition("mysql-bin.000010:1234")
	require.NoError(t, err)
	assert.Equal(t, mysql.Position{Name: "mysql-bin.000010", Pos: 1234}, pos)
	assert.Equal(t, "mysql-bin.000010:1234", FormatPosition(pos))
}
