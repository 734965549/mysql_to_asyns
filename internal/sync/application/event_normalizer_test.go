package application

import (
	"testing"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/pkg/binlog"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestToChangeEvent_Insert(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
		HasPK:        true,
	}

	binlogEvent := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "mydb",
		EventType: binlog.EventTypeInsert,
		Rows: []map[string]interface{}{
			{"id": int64(1), "name": "alice"},
		},
		Timestamp: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 12345},
	}

	event, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.TaskID != "task_001" {
		t.Errorf("expected TaskID 'task_001', got %s", event.TaskID)
	}
	if event.SourceSchema != "mydb" {
		t.Errorf("expected SourceSchema 'mydb', got %s", event.SourceSchema)
	}
	if event.SourceTable != "users" {
		t.Errorf("expected SourceTable 'users', got %s", event.SourceTable)
	}
	if event.EventType != "INSERT" {
		t.Errorf("expected EventType 'INSERT', got %s", event.EventType)
	}
	if event.BinlogFile != "mysql-bin.000001" {
		t.Errorf("expected BinlogFile 'mysql-bin.000001', got %s", event.BinlogFile)
	}
	if event.BinlogPos != 12345 {
		t.Errorf("expected BinlogPos 12345, got %d", event.BinlogPos)
	}
	if event.Before != nil {
		t.Error("expected Before to be nil for INSERT")
	}
	if event.After == nil {
		t.Fatal("expected After to be non-nil for INSERT")
	}
	if event.After["id"] != int64(1) {
		t.Errorf("expected After.id=1, got %v", event.After["id"])
	}
	if event.After["name"] != "alice" {
		t.Errorf("expected After.name='alice', got %v", event.After["name"])
	}
	if event.PrimaryKeys["id"] != int64(1) {
		t.Errorf("expected PrimaryKeys.id=1, got %v", event.PrimaryKeys["id"])
	}
	if event.TraceID != "task_001:mysql-bin.000001:12345:0" {
		t.Errorf("expected TraceID 'task_001:mysql-bin.000001:12345:0', got %s", event.TraceID)
	}
}

func TestToChangeEvent_Update(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
		HasPK:        true,
	}

	binlogEvent := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "mydb",
		EventType: binlog.EventTypeUpdate,
		Rows: []map[string]interface{}{
			{"id": int64(1), "name": "alice_new"},
		},
		BeforeImage: []map[string]interface{}{
			{"id": int64(1), "name": "alice"},
		},
		Timestamp: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 12346},
	}

	event, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.EventType != "UPDATE" {
		t.Errorf("expected EventType 'UPDATE', got %s", event.EventType)
	}
	if event.Before == nil {
		t.Fatal("expected Before to be non-nil for UPDATE")
	}
	if event.Before["name"] != "alice" {
		t.Errorf("expected Before.name='alice', got %v", event.Before["name"])
	}
	if event.After == nil {
		t.Fatal("expected After to be non-nil for UPDATE")
	}
	if event.After["name"] != "alice_new" {
		t.Errorf("expected After.name='alice_new', got %v", event.After["name"])
	}
	if event.PrimaryKeys["id"] != int64(1) {
		t.Errorf("expected PrimaryKeys.id=1, got %v", event.PrimaryKeys["id"])
	}
}

func TestToChangeEvent_Delete(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
		HasPK:        true,
	}

	binlogEvent := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "mydb",
		EventType: binlog.EventTypeDelete,
		Rows: []map[string]interface{}{
			{"id": int64(1), "name": "alice"},
		},
		BeforeImage: []map[string]interface{}{
			{"id": int64(1), "name": "alice"},
		},
		Timestamp: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 12347},
	}

	event, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.EventType != "DELETE" {
		t.Errorf("expected EventType 'DELETE', got %s", event.EventType)
	}
	if event.Before == nil {
		t.Fatal("expected Before to be non-nil for DELETE")
	}
	if event.Before["name"] != "alice" {
		t.Errorf("expected Before.name='alice', got %v", event.Before["name"])
	}
	if event.After != nil {
		t.Error("expected After to be nil for DELETE")
	}
	if event.PrimaryKeys["id"] != int64(1) {
		t.Errorf("expected PrimaryKeys.id=1, got %v", event.PrimaryKeys["id"])
	}
}

func TestToChangeEvent_DeleteNoBeforeImage(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
		HasPK:        true,
	}

	binlogEvent := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "mydb",
		EventType: binlog.EventTypeDelete,
		Rows: []map[string]interface{}{
			{"id": int64(1), "name": "alice"},
		},
		Timestamp: time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 12347},
	}

	event, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.Before == nil {
		t.Fatal("expected Before to fallback to Rows for DELETE without BeforeImage")
	}
	if event.Before["name"] != "alice" {
		t.Errorf("expected Before.name='alice', got %v", event.Before["name"])
	}
}

func TestToChangeEvent_InsertEmptyRows(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
	}

	binlogEvent := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "mydb",
		EventType: binlog.EventTypeInsert,
		Rows:      []map[string]interface{}{},
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 100},
	}

	_, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err == nil {
		t.Fatal("expected error for INSERT with empty rows")
	}
}

func TestToChangeEvent_InvalidInput(t *testing.T) {
	identity := &entity.TableIdentity{IdentifyCols: []string{"id"}}
	tests := []struct {
		name     string
		event    *binlog.BinlogEvent
		identity *entity.TableIdentity
	}{
		{name: "nil event", identity: identity},
		{name: "nil identity", event: &binlog.BinlogEvent{}},
		{name: "empty update", identity: identity, event: &binlog.BinlogEvent{EventType: binlog.EventTypeUpdate}},
		{name: "unknown type", identity: identity, event: &binlog.BinlogEvent{EventType: binlog.BinlogEventType("DDL")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ToChangeEvent(tt.event, tt.identity, "task", 0)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestToChangeEvent_DeleteEmptyRows(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
	}

	binlogEvent := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "mydb",
		EventType: binlog.EventTypeDelete,
		Rows:      []map[string]interface{}{},
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 100},
	}

	_, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err == nil {
		t.Fatal("expected error for DELETE with empty rows")
	}
}

func TestToChangeEvent_CompositeKey(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "order_items",
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"order_id", "product_id"},
		HasPK:        true,
	}

	binlogEvent := &binlog.BinlogEvent{
		Table:     "order_items",
		Schema:    "mydb",
		EventType: binlog.EventTypeInsert,
		Rows: []map[string]interface{}{
			{"order_id": int64(100), "product_id": int64(200), "qty": int64(3)},
		},
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 200},
	}

	event, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(event.PrimaryKeys) != 2 {
		t.Errorf("expected 2 primary keys, got %d", len(event.PrimaryKeys))
	}
	if event.PrimaryKeys["order_id"] != int64(100) {
		t.Errorf("expected PrimaryKeys.order_id=100, got %v", event.PrimaryKeys["order_id"])
	}
	if event.PrimaryKeys["product_id"] != int64(200) {
		t.Errorf("expected PrimaryKeys.product_id=200, got %v", event.PrimaryKeys["product_id"])
	}
}

func TestToChangeEvent_FullColumnsStrategy(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "logs",
		Strategy:     entity.FullColumnsStrategy,
		IdentifyCols: []string{"message", "level"},
		HasPK:        false,
		HasUK:        false,
	}

	binlogEvent := &binlog.BinlogEvent{
		Table:     "logs",
		Schema:    "mydb",
		EventType: binlog.EventTypeUpdate,
		Rows: []map[string]interface{}{
			{"message": "hello", "level": "info"},
		},
		BeforeImage: []map[string]interface{}{
			{"message": "hello", "level": "debug"},
		},
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 300},
	}

	event, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.PrimaryKeys["message"] != "hello" {
		t.Errorf("expected PrimaryKeys.message='hello', got %v", event.PrimaryKeys["message"])
	}
	if event.PrimaryKeys["level"] != "info" {
		t.Errorf("expected PrimaryKeys.level='info', got %v", event.PrimaryKeys["level"])
	}
}

func TestChangeEventTraceID_MultiRowIndex(t *testing.T) {
	if got := ChangeEventTraceID("task_a", "mysql-bin.000002", 999, 3); got != "task_a:mysql-bin.000002:999:3" {
		t.Errorf("unexpected trace id: %s", got)
	}
}

func TestToChangeEvent_TraceIDPerRowIndex(t *testing.T) {
	identity := &entity.TableIdentity{
		TableName:    "users",
		Strategy:     entity.PKStrategy,
		IdentifyCols: []string{"id"},
		HasPK:        true,
	}
	binlogEvent := &binlog.BinlogEvent{
		Table:     "users",
		Schema:    "mydb",
		EventType: binlog.EventTypeInsert,
		Rows: []map[string]interface{}{
			{"id": int64(1)},
			{"id": int64(2)},
		},
		Timestamp: time.Now(),
		Position:  mysql.Position{Name: "mysql-bin.000001", Pos: 500},
	}

	first, err := ToChangeEvent(binlogEvent, identity, "task_001", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := ToChangeEvent(binlogEvent, identity, "task_001", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first.TraceID == second.TraceID {
		t.Fatalf("expected distinct trace ids, both=%s", first.TraceID)
	}
	if first.TraceID != "task_001:mysql-bin.000001:500:0" {
		t.Errorf("unexpected first trace id: %s", first.TraceID)
	}
	if second.TraceID != "task_001:mysql-bin.000001:500:1" {
		t.Errorf("unexpected second trace id: %s", second.TraceID)
	}
}

func TestSinkTypeConstants(t *testing.T) {
	if string(sink.SinkTypeMYSQL) != "MYSQL" {
		t.Errorf("expected MYSQL, got %s", sink.SinkTypeMYSQL)
	}
	if string(sink.SinkTypeKAFKA) != "KAFKA" {
		t.Errorf("expected KAFKA, got %s", sink.SinkTypeKAFKA)
	}
	if string(sink.SinkTypeHTTPWebhook) != "HTTP_WEBHOOK" {
		t.Errorf("expected HTTP_WEBHOOK, got %s", sink.SinkTypeHTTPWebhook)
	}
}

func TestSecretPaths(t *testing.T) {
	if paths := sink.SecretPaths(sink.SinkTypeKAFKA); len(paths) != 1 || paths[0] != "security.sasl_password" {
		t.Errorf("expected [security.sasl_password], got %v", paths)
	}
	if paths := sink.SecretPaths(sink.SinkTypeHTTPWebhook); len(paths) != 1 || paths[0] != "headers" {
		t.Errorf("expected [headers], got %v", paths)
	}
	if paths := sink.SecretPaths(sink.SinkTypeMYSQL); len(paths) != 0 {
		t.Errorf("expected empty for MYSQL, got %v", paths)
	}
}
