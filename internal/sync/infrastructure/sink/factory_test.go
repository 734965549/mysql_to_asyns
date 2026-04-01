package sink

import (
	"context"
	"testing"
)

func TestNewSinkFactory(t *testing.T) {
	f := NewSinkFactory()
	if f == nil {
		t.Fatal("NewSinkFactory returned nil")
	}
}

func TestCreateSink_NilConfig(t *testing.T) {
	f := NewSinkFactory()
	_, err := f.CreateSink(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestCreateSink_UnsupportedType(t *testing.T) {
	f := NewSinkFactory()
	cfg := &SinkConfig{Type: "REDIS", Options: map[string]interface{}{}}
	_, err := f.CreateSink(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("expected error for unsupported sink type")
	}
}

func TestCreateSink_MySQL_NilDeps(t *testing.T) {
	f := NewSinkFactory()
	cfg := &SinkConfig{Type: SinkTypeMySQL, Options: map[string]interface{}{}}
	_, err := f.CreateSink(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("expected error for MySQL sink with nil deps")
	}
}

func TestCreateSink_Kafka_NoBrokers(t *testing.T) {
	f := NewSinkFactory()
	cfg := &SinkConfig{Type: SinkTypeKafka, Options: map[string]interface{}{}}
	_, err := f.CreateSink(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("expected error for Kafka sink without brokers")
	}
}

func TestCreateSink_Kafka_Success(t *testing.T) {
	f := NewSinkFactory()
	cfg := &SinkConfig{
		Type: SinkTypeKafka,
		Options: map[string]interface{}{
			"brokers": []interface{}{"localhost:9092"},
			"topic":   "test_topic",
		},
	}
	sk, err := f.CreateSink(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sk == nil {
		t.Fatal("expected non-nil sink")
	}
	if sk.Type() != "KAFKA" {
		t.Errorf("expected type KAFKA, got %s", sk.Type())
	}
}

func TestCreateSink_Webhook_NoURL(t *testing.T) {
	f := NewSinkFactory()
	cfg := &SinkConfig{Type: SinkTypeHTTPWebhook, Options: map[string]interface{}{}}
	_, err := f.CreateSink(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("expected error for Webhook sink without url")
	}
}

func TestCreateSink_Webhook_Success(t *testing.T) {
	f := NewSinkFactory()
	cfg := &SinkConfig{
		Type: SinkTypeHTTPWebhook,
		Options: map[string]interface{}{
			"url":        "http://localhost:8080/hook",
			"method":     "POST",
			"timeout_ms": 5000,
		},
	}
	sk, err := f.CreateSink(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sk == nil {
		t.Fatal("expected non-nil sink")
	}
	if sk.Type() != "HTTP_WEBHOOK" {
		t.Errorf("expected type HTTP_WEBHOOK, got %s", sk.Type())
	}
}

// --- option helper tests ---

func TestGetStringOption(t *testing.T) {
	opts := map[string]interface{}{"key": "value"}
	if v := getStringOption(opts, "key", "def"); v != "value" {
		t.Errorf("expected 'value', got '%s'", v)
	}
	if v := getStringOption(opts, "missing", "def"); v != "def" {
		t.Errorf("expected 'def', got '%s'", v)
	}
	if v := getStringOption(nil, "key", "def"); v != "def" {
		t.Errorf("expected 'def' for nil opts, got '%s'", v)
	}
}

func TestGetIntOption(t *testing.T) {
	opts := map[string]interface{}{
		"float":  float64(42),
		"int":    100,
		"int64":  int64(200),
		"string": "bad",
	}
	if v := getIntOption(opts, "float", 0); v != 42 {
		t.Errorf("expected 42, got %d", v)
	}
	if v := getIntOption(opts, "int", 0); v != 100 {
		t.Errorf("expected 100, got %d", v)
	}
	if v := getIntOption(opts, "int64", 0); v != 200 {
		t.Errorf("expected 200, got %d", v)
	}
	if v := getIntOption(opts, "string", 99); v != 99 {
		t.Errorf("expected default 99 for string type, got %d", v)
	}
	if v := getIntOption(opts, "missing", 10); v != 10 {
		t.Errorf("expected default 10, got %d", v)
	}
}

func TestGetStringSliceOption(t *testing.T) {
	opts := map[string]interface{}{
		"iface_slice":  []interface{}{"a", "b", "c"},
		"string_slice": []string{"x", "y"},
	}
	if v := getStringSliceOption(opts, "iface_slice"); len(v) != 3 || v[0] != "a" {
		t.Errorf("expected [a,b,c], got %v", v)
	}
	if v := getStringSliceOption(opts, "string_slice"); len(v) != 2 || v[0] != "x" {
		t.Errorf("expected [x,y], got %v", v)
	}
	if v := getStringSliceOption(opts, "missing"); v != nil {
		t.Errorf("expected nil for missing key, got %v", v)
	}
}

func TestGetStringMapOption(t *testing.T) {
	opts := map[string]interface{}{
		"iface_map":  map[string]interface{}{"k1": "v1"},
		"string_map": map[string]string{"k2": "v2"},
	}
	if v := getStringMapOption(opts, "iface_map"); len(v) != 1 || v["k1"] != "v1" {
		t.Errorf("expected {k1:v1}, got %v", v)
	}
	if v := getStringMapOption(opts, "string_map"); len(v) != 1 || v["k2"] != "v2" {
		t.Errorf("expected {k2:v2}, got %v", v)
	}
	if v := getStringMapOption(opts, "missing"); v != nil {
		t.Errorf("expected nil for missing key, got %v", v)
	}
}
