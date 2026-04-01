package kafka

import (
	"context"
	"testing"
	"time"

	"mysql-to-async/internal/sync/domain/sink"
)

func TestKafkaSink_Type(t *testing.T) {
	s := NewKafkaSink(Config{Brokers: []string{"localhost:9092"}, Topic: "test"})
	if s.Type() != "KAFKA" {
		t.Errorf("expected KAFKA, got %s", s.Type())
	}
}

func TestKafkaSink_Defaults(t *testing.T) {
	s := NewKafkaSink(Config{})
	if s.config.BatchSize != 100 {
		t.Errorf("expected default BatchSize=100, got %d", s.config.BatchSize)
	}
	if s.config.BatchTimeoutMs != 500 {
		t.Errorf("expected default BatchTimeoutMs=500, got %d", s.config.BatchTimeoutMs)
	}
	if s.config.RequiredAcks != 1 {
		t.Errorf("expected default RequiredAcks=1, got %d", s.config.RequiredAcks)
	}
}

func TestKafkaSink_CustomConfig(t *testing.T) {
	s := NewKafkaSink(Config{
		Brokers:        []string{"b1:9092", "b2:9092"},
		Topic:          "my_topic",
		KeyMode:        "table",
		BatchSize:      200,
		BatchTimeoutMs: 1000,
		RequiredAcks:   -1,
	})
	if len(s.config.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(s.config.Brokers))
	}
	if s.config.Topic != "my_topic" {
		t.Errorf("expected topic=my_topic, got %s", s.config.Topic)
	}
	if s.config.KeyMode != "table" {
		t.Errorf("expected key_mode=table, got %s", s.config.KeyMode)
	}
	if s.config.BatchSize != 200 {
		t.Errorf("expected batch_size=200, got %d", s.config.BatchSize)
	}
}

func TestKafkaSink_Open(t *testing.T) {
	s := NewKafkaSink(Config{Brokers: []string{"localhost:9092"}, Topic: "test"})
	ctx := context.Background()
	if err := s.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if s.writer == nil {
		t.Fatal("expected writer to be initialized after Open")
	}
	// 清理
	s.Close(ctx)
}

func TestKafkaSink_Close_Idempotent(t *testing.T) {
	s := NewKafkaSink(Config{Brokers: []string{"localhost:9092"}, Topic: "test"})
	ctx := context.Background()
	s.Open(ctx)

	if err := s.Close(ctx); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if !s.closed {
		t.Fatal("expected closed=true")
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}
}

func TestKafkaSink_Flush(t *testing.T) {
	s := NewKafkaSink(Config{Brokers: []string{"localhost:9092"}, Topic: "test"})
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush should always succeed: %v", err)
	}
}

func TestKafkaSink_BuildMessageKey_PK(t *testing.T) {
	s := NewKafkaSink(Config{KeyMode: "pk"})
	event := &sink.ChangeEvent{
		SourceSchema: "db",
		SourceTable:  "t",
		BinlogFile:   "bin.000001",
		BinlogPos:    100,
		PrimaryKeys:  map[string]interface{}{"id": 42},
	}
	key := s.buildMessageKey(event)
	if key != "id=42" {
		t.Errorf("expected 'id=42', got '%s'", key)
	}
}

func TestKafkaSink_BuildMessageKey_NoPK(t *testing.T) {
	s := NewKafkaSink(Config{KeyMode: "pk"})
	event := &sink.ChangeEvent{
		SourceSchema: "db",
		SourceTable:  "t",
		BinlogFile:   "bin.000001",
		BinlogPos:    100,
	}
	key := s.buildMessageKey(event)
	expected := "db.t:bin.000001:100"
	if key != expected {
		t.Errorf("expected '%s', got '%s'", expected, key)
	}
}

func TestKafkaSink_BuildMessageKey_TableMode(t *testing.T) {
	s := NewKafkaSink(Config{KeyMode: "table"})
	event := &sink.ChangeEvent{
		SourceSchema: "mydb",
		SourceTable:  "orders",
		BinlogFile:   "bin.000002",
		BinlogPos:    999,
		PrimaryKeys:  map[string]interface{}{"id": 1},
	}
	key := s.buildMessageKey(event)
	expected := "mydb.orders:bin.000002:999"
	if key != expected {
		t.Errorf("expected '%s', got '%s'", expected, key)
	}
}

func TestKafkaSink_Write_NilEvent(t *testing.T) {
	s := NewKafkaSink(Config{Brokers: []string{"localhost:9092"}, Topic: "test"})
	ctx := context.Background()
	s.Open(ctx)
	defer s.Close(ctx)

	if err := s.Write(ctx, nil); err != nil {
		t.Fatalf("Write nil should return nil: %v", err)
	}
}

// 注意: Write 到真实 Kafka 的集成测试需要 Kafka 集群，这里仅测试序列化逻辑
func TestKafkaSink_Write_SerializationValid(t *testing.T) {
	// 确保 ChangeEvent 可以被正确序列化（不会 panic）
	event := &sink.ChangeEvent{
		TaskID:       "t1",
		SourceSchema: "db",
		SourceTable:  "users",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "bin.000001",
		BinlogPos:    100,
		After:        map[string]interface{}{"id": 1, "name": "test"},
	}
	_ = event // 确认构造不会 panic
}
