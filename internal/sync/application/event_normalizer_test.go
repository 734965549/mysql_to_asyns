package application

import (
	"testing"
	"time"

	"mysql-to-async/pkg/binlog"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestNewEventNormalizer(t *testing.T) {
	n := NewEventNormalizer()
	if n == nil {
		t.Fatal("NewEventNormalizer returned nil")
	}
}

func TestNormalize_NilEvent(t *testing.T) {
	n := NewEventNormalizer()
	_, err := n.Normalize("task1", nil)
	if err == nil {
		t.Fatal("expected error for nil event")
	}
}

func TestNormalize_Insert(t *testing.T) {
	n := NewEventNormalizer()
	ts := time.Now()
	event := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "testdb",
		EventType: binlog.EventTypeInsert,
		Rows: []map[string]interface{}{
			{"id": 1, "name": "alice"},
			{"id": 2, "name": "bob"},
		},
		Timestamp: ts,
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 100},
	}

	changes, err := n.Normalize("task1", event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("expected 2 change events, got %d", len(changes))
	}

	// 验证第一条
	ce := changes[0]
	if ce.TaskID != "task1" {
		t.Errorf("expected TaskID=task1, got %s", ce.TaskID)
	}
	if ce.SourceSchema != "testdb" {
		t.Errorf("expected SourceSchema=testdb, got %s", ce.SourceSchema)
	}
	if ce.SourceTable != "users" {
		t.Errorf("expected SourceTable=users, got %s", ce.SourceTable)
	}
	if ce.EventType != "INSERT" {
		t.Errorf("expected EventType=INSERT, got %s", ce.EventType)
	}
	if ce.BinlogFile != "mysql-bin.000001" {
		t.Errorf("expected BinlogFile=mysql-bin.000001, got %s", ce.BinlogFile)
	}
	if ce.BinlogPos != 100 {
		t.Errorf("expected BinlogPos=100, got %d", ce.BinlogPos)
	}
	if ce.After == nil {
		t.Error("expected After != nil for INSERT")
	}
	if ce.Before != nil {
		t.Error("expected Before == nil for INSERT")
	}
	if ce.After["id"] != 1 {
		t.Errorf("expected After[id]=1, got %v", ce.After["id"])
	}

	// 验证第二条
	if changes[1].After["name"] != "bob" {
		t.Errorf("expected second row name=bob, got %v", changes[1].After["name"])
	}
}

func TestNormalize_Update(t *testing.T) {
	n := NewEventNormalizer()
	event := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "testdb",
		EventType: binlog.EventTypeUpdate,
		Rows: []map[string]interface{}{
			{"id": 1, "name": "alice_new"},
		},
		BeforeImage: []map[string]interface{}{
			{"id": 1, "name": "alice_old"},
		},
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 200},
	}

	changes, err := n.Normalize("task2", event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change event, got %d", len(changes))
	}

	ce := changes[0]
	if ce.EventType != "UPDATE" {
		t.Errorf("expected EventType=UPDATE, got %s", ce.EventType)
	}
	if ce.Before == nil {
		t.Fatal("expected Before != nil for UPDATE")
	}
	if ce.After == nil {
		t.Fatal("expected After != nil for UPDATE")
	}
	if ce.Before["name"] != "alice_old" {
		t.Errorf("expected Before[name]=alice_old, got %v", ce.Before["name"])
	}
	if ce.After["name"] != "alice_new" {
		t.Errorf("expected After[name]=alice_new, got %v", ce.After["name"])
	}
}

func TestNormalize_Update_NoBeforeImage(t *testing.T) {
	n := NewEventNormalizer()
	event := &binlog.BinlogEvent{
		Table:       "users",
		Schema:      "testdb",
		EventType:   binlog.EventTypeUpdate,
		Rows:        []map[string]interface{}{{"id": 1, "name": "new"}},
		BeforeImage: nil,
		Timestamp:   time.Now(),
		Position:    mysql.Position{Name: "mysql-bin.000001", Pos: 300},
	}

	changes, err := n.Normalize("task3", event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change event, got %d", len(changes))
	}
	if changes[0].Before != nil {
		t.Error("expected Before == nil when no BeforeImage provided")
	}
}

func TestNormalize_Delete(t *testing.T) {
	n := NewEventNormalizer()
	event := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "testdb",
		EventType: binlog.EventTypeDelete,
		Rows:      []map[string]interface{}{{"id": 1, "name": "alice"}},
		BeforeImage: []map[string]interface{}{
			{"id": 1, "name": "alice"},
			{"id": 2, "name": "bob"},
		},
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000002", Pos: 400},
	}

	changes, err := n.Normalize("task4", event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// DELETE 以 BeforeImage 为准
	if len(changes) != 2 {
		t.Fatalf("expected 2 change events, got %d", len(changes))
	}

	ce := changes[0]
	if ce.EventType != "DELETE" {
		t.Errorf("expected EventType=DELETE, got %s", ce.EventType)
	}
	if ce.Before == nil {
		t.Fatal("expected Before != nil for DELETE")
	}
	if ce.After != nil {
		t.Error("expected After == nil for DELETE")
	}
}

func TestNormalize_UnknownEventType(t *testing.T) {
	n := NewEventNormalizer()
	event := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "testdb",
		EventType: "TRUNCATE",
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 500},
	}

	_, err := n.Normalize("task5", event)
	if err == nil {
		t.Fatal("expected error for unknown event type")
	}
}

func TestNormalize_EmptyRows(t *testing.T) {
	n := NewEventNormalizer()
	event := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "testdb",
		EventType: binlog.EventTypeInsert,
		Rows:      nil,
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 600},
	}

	changes, err := n.Normalize("task6", event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("expected 0 change events for empty rows, got %d", len(changes))
	}
}
