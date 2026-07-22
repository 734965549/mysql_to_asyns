package kafka

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mysql-to-sync/internal/sync/domain/sink"
)

func TestNewKafkaSink_Defaults(t *testing.T) {
	options := map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "mysql_cdc",
	}

	s := NewKafkaSink(options)

	assert.Equal(t, []string{"127.0.0.1:9092"}, s.brokers)
	assert.Equal(t, "mysql_cdc", s.topic)
	assert.Equal(t, "single_topic", s.routingMode)
	assert.Equal(t, "pk", s.keyMode)
	assert.Equal(t, 1000, s.batchSize)
	assert.Equal(t, 500, s.batchTimeoutMs)
	assert.Equal(t, 1, s.requiredAcks)
	assert.Nil(t, s.security)
}

func TestNewKafkaSink_CustomOptions(t *testing.T) {
	options := map[string]interface{}{
		"brokers":          []string{"kafka1:9092", "kafka2:9092"},
		"topic":            "my_cdc",
		"routing_mode":     "per_table",
		"topic_prefix":     "cdc",
		"key_mode":         "none",
		"batch_size":       float64(500),
		"batch_timeout_ms": float64(200),
		"required_acks":    float64(-1),
		"security": map[string]interface{}{
			"sasl_mechanism": "PLAIN",
			"sasl_username":  "user",
			"sasl_password":  "pass",
		},
	}

	s := NewKafkaSink(options)

	assert.Equal(t, []string{"kafka1:9092", "kafka2:9092"}, s.brokers)
	assert.Equal(t, "my_cdc", s.topic)
	assert.Equal(t, "per_table", s.routingMode)
	assert.Equal(t, "cdc", s.topicPrefix)
	assert.Equal(t, "none", s.keyMode)
	assert.Equal(t, 500, s.batchSize)
	assert.Equal(t, 200, s.batchTimeoutMs)
	assert.Equal(t, -1, s.requiredAcks)
	assert.NotNil(t, s.security)
	assert.Equal(t, "PLAIN", s.security["sasl_mechanism"])
}

func TestKafkaSink_Type(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})
	assert.Equal(t, sink.SinkTypeKAFKA, s.Type())
}

func TestKafkaSink_OpenFlushCloseLifecycle(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})
	require.NoError(t, s.Open(context.Background()))
	assert.NotNil(t, s.writer)
	err := s.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already open")
	require.NoError(t, s.Flush(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	err = s.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestKafkaSink_ResolveTopic_SingleTopic(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers":      []string{"127.0.0.1:9092"},
		"topic":        "mysql_cdc",
		"routing_mode": "single_topic",
	})

	event := &sink.ChangeEvent{
		SourceSchema: "db1",
		SourceTable:  "orders",
	}

	topic := s.resolveTopic(event)
	assert.Equal(t, "mysql_cdc", topic)
}

func TestKafkaSink_ResolveTopic_PerTable(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers":      []string{"127.0.0.1:9092"},
		"topic":        "mysql_cdc",
		"routing_mode": "per_table",
		"topic_prefix": "cdc",
	})

	event := &sink.ChangeEvent{
		SourceSchema: "db1",
		SourceTable:  "orders",
	}

	topic := s.resolveTopic(event)
	assert.Equal(t, "cdc.db1.orders", topic)
}

func TestKafkaSink_ResolveTopic_PerTable_NoPrefix(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers":      []string{"127.0.0.1:9092"},
		"topic":        "mysql_cdc",
		"routing_mode": "per_table",
	})

	event := &sink.ChangeEvent{
		SourceSchema: "db1",
		SourceTable:  "users",
	}

	assert.Equal(t, ".db1.users", s.resolveTopic(event))
	err := s.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "topic_prefix")
}

type fakeMessageWriter struct {
	messages   []kafkago.Message
	writeErr   error
	closeErr   error
	closeCount int
}

func (w *fakeMessageWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.messages = append(w.messages, messages...)
	return w.writeErr
}

func (w *fakeMessageWriter) Close() error {
	w.closeCount++
	return w.closeErr
}

func TestKafkaSink_Write(t *testing.T) {
	writer := &fakeMessageWriter{}
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "cdc",
	})
	s.writer = writer
	event := &sink.ChangeEvent{
		TaskID: "task", SourceSchema: "db", SourceTable: "users",
		EventType: "INSERT", BinlogPos: 12,
		PrimaryKeys: map[string]interface{}{"id": int64(9)},
		After:       map[string]interface{}{"id": int64(9)},
	}

	require.NoError(t, s.Write(context.Background(), event))
	require.Len(t, writer.messages, 1)
	assert.Equal(t, "cdc", writer.messages[0].Topic)
	assert.Equal(t, "9", string(writer.messages[0].Key))
	var decoded sink.ChangeEvent
	require.NoError(t, json.Unmarshal(writer.messages[0].Value, &decoded))
	assert.Equal(t, "task", decoded.TaskID)
}

func TestKafkaSink_WriteErrors(t *testing.T) {
	t.Run("not open", func(t *testing.T) {
		s := NewKafkaSink(map[string]interface{}{})
		err := s.Write(context.Background(), &sink.ChangeEvent{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not open")
	})
	t.Run("nil event", func(t *testing.T) {
		s := NewKafkaSink(map[string]interface{}{})
		s.writer = &fakeMessageWriter{}
		err := s.Write(context.Background(), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "event")
	})
	t.Run("writer error", func(t *testing.T) {
		s := NewKafkaSink(map[string]interface{}{"topic": "cdc"})
		s.writer = &fakeMessageWriter{writeErr: errors.New("broker unavailable")}
		err := s.Write(context.Background(), &sink.ChangeEvent{SourceSchema: "db", SourceTable: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "broker unavailable")
	})
}

func TestKafkaSink_Validate(t *testing.T) {
	valid := map[string]interface{}{"brokers": []string{"broker:9092"}, "topic": "cdc"}
	tests := []struct {
		name    string
		options map[string]interface{}
		wantErr string
	}{
		{name: "valid", options: valid},
		{name: "missing brokers", options: map[string]interface{}{"topic": "cdc"}, wantErr: "brokers"},
		{name: "missing topic", options: map[string]interface{}{"brokers": []string{"broker:9092"}}, wantErr: "topic"},
		{name: "invalid routing", options: map[string]interface{}{"brokers": []string{"broker:9092"}, "topic": "cdc", "routing_mode": "random"}, wantErr: "routing_mode"},
		{name: "invalid key mode", options: map[string]interface{}{"brokers": []string{"broker:9092"}, "topic": "cdc", "key_mode": "random"}, wantErr: "key_mode"},
		{name: "invalid batch size", options: map[string]interface{}{"brokers": []string{"broker:9092"}, "topic": "cdc", "batch_size": 0}, wantErr: "batch_size"},
		{name: "invalid acks", options: map[string]interface{}{"brokers": []string{"broker:9092"}, "topic": "cdc", "required_acks": 2}, wantErr: "required_acks"},
		{name: "missing sasl username", options: map[string]interface{}{"brokers": []string{"broker:9092"}, "topic": "cdc", "security": map[string]interface{}{"sasl_mechanism": "PLAIN", "sasl_password": "secret"}}, wantErr: "sasl_username"},
		{name: "missing sasl password", options: map[string]interface{}{"brokers": []string{"broker:9092"}, "topic": "cdc", "security": map[string]interface{}{"sasl_mechanism": "PLAIN", "sasl_username": "user"}}, wantErr: "sasl_password"},
		{name: "incomplete mtls", options: map[string]interface{}{"brokers": []string{"broker:9092"}, "topic": "cdc", "security": map[string]interface{}{"tls_enabled": true, "client_cert_path": "cert.pem"}}, wantErr: "configured together"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewKafkaSink(tt.options).Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestKafkaSink_CloseWriterErrorAndIdempotency(t *testing.T) {
	writer := &fakeMessageWriter{closeErr: errors.New("close failed")}
	s := NewKafkaSink(map[string]interface{}{})
	s.writer = writer
	err := s.Close(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
	require.NoError(t, s.Close(context.Background()))
	assert.Equal(t, 1, writer.closeCount)
}

func TestKafkaSink_BuildKey_PKMode(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers":  []string{"127.0.0.1:9092"},
		"topic":    "test",
		"key_mode": "pk",
	})

	event := &sink.ChangeEvent{
		SourceSchema: "db1",
		SourceTable:  "orders",
		BinlogPos:    12345,
		PrimaryKeys: map[string]interface{}{
			"id": float64(42),
		},
	}

	key := s.buildKey(event)
	assert.Equal(t, "42", key)
}

func TestKafkaSink_BuildKey_PKMode_MultiKey(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers":  []string{"127.0.0.1:9092"},
		"topic":    "test",
		"key_mode": "pk",
	})

	event := &sink.ChangeEvent{
		SourceSchema: "db1",
		SourceTable:  "orders",
		BinlogPos:    12345,
		PrimaryKeys: map[string]interface{}{
			"order_id": float64(100),
			"item_id":  float64(5),
		},
	}

	key := s.buildKey(event)
	assert.Equal(t, "5:100", key)
}

func TestKafkaSink_BuildKey_NoneMode(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers":  []string{"127.0.0.1:9092"},
		"topic":    "test",
		"key_mode": "none",
	})

	event := &sink.ChangeEvent{
		SourceSchema: "db1",
		SourceTable:  "orders",
		BinlogPos:    12345,
		PrimaryKeys: map[string]interface{}{
			"id": float64(42),
		},
	}

	key := s.buildKey(event)
	assert.Equal(t, "db1.orders:12345", key)
}

func TestKafkaSink_BuildKey_NoPrimaryKeys(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers":  []string{"127.0.0.1:9092"},
		"topic":    "test",
		"key_mode": "pk",
	})

	event := &sink.ChangeEvent{
		SourceSchema: "db1",
		SourceTable:  "orders",
		BinlogPos:    99999,
	}

	key := s.buildKey(event)
	assert.Equal(t, "db1.orders:99999", key)
}

func TestKafkaSink_BuildValue(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})

	eventTime := time.Date(2026, 7, 14, 10, 0, 0, 0, time.FixedZone("CST", 8*3600))
	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "UPDATE",
		EventTime:    eventTime,
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
		PrimaryKeys:  map[string]interface{}{"id": float64(42)},
		Before:       map[string]interface{}{"id": float64(42), "status": float64(0)},
		After:        map[string]interface{}{"id": float64(42), "status": float64(1)},
	}

	value, err := s.buildValue(event)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(value, &result)
	require.NoError(t, err)

	assert.Equal(t, "task_001", result["task_id"])
	assert.Equal(t, "db1", result["source_schema"])
	assert.Equal(t, "orders", result["source_table"])
	assert.Equal(t, "UPDATE", result["event_type"])
	assert.Equal(t, "mysql-bin.000001", result["binlog_file"])
	assert.Equal(t, float64(12345), result["binlog_pos"])
	assert.Contains(t, result["event_time"].(string), "2026-07-14")
}

func TestKafkaSink_BuildSASLMechanism_PLAIN(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})

	m, err := s.buildSASLMechanism("PLAIN", "user", "pass")
	require.NoError(t, err)
	assert.NotNil(t, m)
}

func TestKafkaSink_BuildSASLMechanism_SCRAM_SHA256(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})

	m, err := s.buildSASLMechanism("SCRAM-SHA-256", "user", "pass")
	require.NoError(t, err)
	assert.NotNil(t, m)
}

func TestKafkaSink_BuildSASLMechanism_SCRAM_SHA512(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})

	m, err := s.buildSASLMechanism("SCRAM-SHA-512", "user", "pass")
	require.NoError(t, err)
	assert.NotNil(t, m)
}

func TestKafkaSink_BuildSASLMechanism_Unsupported(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})

	_, err := s.buildSASLMechanism("GSSAPI", "user", "pass")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported SASL mechanism")
}

func TestKafkaSink_BuildTransport_NoSecurity(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})

	transport, err := s.buildTransport()
	require.NoError(t, err)
	assert.NotNil(t, transport)
	assert.Nil(t, transport.TLS)
}

func TestKafkaSink_BuildTransport_WithSASL(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
		"security": map[string]interface{}{
			"sasl_mechanism": "PLAIN",
			"sasl_username":  "user",
			"sasl_password":  "pass",
		},
	})

	transport, err := s.buildTransport()
	require.NoError(t, err)
	assert.NotNil(t, transport)
	assert.NotNil(t, transport.SASL)
	assert.Nil(t, transport.TLS)
}

func TestKafkaSink_BuildTransport_WithTLS_InvalidCA(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
		"security": map[string]interface{}{
			"tls_enabled":  true,
			"ca_cert_path": "/nonexistent/ca.pem",
		},
	})

	_, err := s.buildTransport()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read CA cert")
}

func TestKafkaSink_BuildTransport_WithTLSAndClientCertificate(t *testing.T) {
	certPath, keyPath := writeTestCertificate(t)
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
		"security": map[string]interface{}{
			"tls_enabled":          true,
			"ca_cert_path":         certPath,
			"client_cert_path":     certPath,
			"client_key_path":      keyPath,
			"insecure_skip_verify": true,
		},
	})
	require.NoError(t, s.Validate())
	transport, err := s.buildTransport()
	require.NoError(t, err)
	require.NotNil(t, transport.TLS)
	assert.NotNil(t, transport.TLS.RootCAs)
	assert.Len(t, transport.TLS.Certificates, 1)
	assert.True(t, transport.TLS.InsecureSkipVerify)
}

func TestKafkaSink_BuildTransport_InvalidCAPEM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-ca.pem")
	require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))
	s := NewKafkaSink(map[string]interface{}{
		"security": map[string]interface{}{"tls_enabled": true, "ca_cert_path": path},
	})
	_, err := s.buildTransport()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse CA")
}

func writeTestCertificate(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client-key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}), 0o600))
	return certPath, keyPath
}

func TestKafkaSink_Close_Idempotent(t *testing.T) {
	s := NewKafkaSink(map[string]interface{}{
		"brokers": []string{"127.0.0.1:9092"},
		"topic":   "test",
	})

	err := s.Close(nil)
	assert.NoError(t, err)

	err = s.Close(nil)
	assert.NoError(t, err)
}

func TestGetStringSlice(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		key      string
		expected []string
	}{
		{
			name:     "string_slice",
			input:    map[string]interface{}{"brokers": []string{"a:9092", "b:9092"}},
			key:      "brokers",
			expected: []string{"a:9092", "b:9092"},
		},
		{
			name:     "interface_slice",
			input:    map[string]interface{}{"brokers": []interface{}{"a:9092", "b:9092"}},
			key:      "brokers",
			expected: []string{"a:9092", "b:9092"},
		},
		{
			name:     "missing_key",
			input:    map[string]interface{}{},
			key:      "brokers",
			expected: nil,
		},
		{
			name:     "wrong_type",
			input:    map[string]interface{}{"brokers": "not-a-slice"},
			key:      "brokers",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getStringSlice(tt.input, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetIntWithDefault(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		key      string
		def      int
		expected int
	}{
		{
			name:     "float64",
			input:    map[string]interface{}{"n": float64(42)},
			key:      "n",
			def:      100,
			expected: 42,
		},
		{
			name:     "int",
			input:    map[string]interface{}{"n": 10},
			key:      "n",
			def:      100,
			expected: 10,
		},
		{
			name:     "missing",
			input:    map[string]interface{}{},
			key:      "n",
			def:      100,
			expected: 100,
		},
		{
			name:     "wrong_type",
			input:    map[string]interface{}{"n": "string"},
			key:      "n",
			def:      100,
			expected: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getIntWithDefault(tt.input, tt.key, tt.def)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSortedKeys(t *testing.T) {
	m := map[string]interface{}{
		"b": 1,
		"a": 2,
		"c": 3,
	}
	keys := sortedKeys(m)
	assert.Equal(t, []string{"a", "b", "c"}, keys)
}

func TestKafkaSink_TxnBufferHardLimitByEvents(t *testing.T) {
	writer := &fakeMessageWriter{}
	s := NewKafkaSink(map[string]interface{}{
		"brokers":                 []string{"127.0.0.1:9092"},
		"topic":                   "cdc",
		"max_txn_buffered_events": 2,
		"max_txn_buffered_bytes":  float64(1 << 20),
	})
	s.writer = writer
	require.NoError(t, s.BeginTransaction(context.Background()))

	event := &sink.ChangeEvent{
		TaskID: "task", SourceSchema: "db", SourceTable: "users",
		EventType: "INSERT", BinlogPos: 1,
		PrimaryKeys: map[string]interface{}{"id": int64(1)},
		After:       map[string]interface{}{"id": int64(1)},
	}
	require.NoError(t, s.Write(context.Background(), event))
	event.BinlogPos = 2
	event.After = map[string]interface{}{"id": int64(2)}
	require.NoError(t, s.Write(context.Background(), event))
	event.BinlogPos = 3
	event.After = map[string]interface{}{"id": int64(3)}
	err := s.Write(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hard limit")
	assert.Empty(t, writer.messages, "rejected event must not flush early")
	require.NoError(t, s.RollbackTransaction(context.Background()))
}

func TestKafkaSink_TxnBufferHardLimitByBytes(t *testing.T) {
	writer := &fakeMessageWriter{}
	s := NewKafkaSink(map[string]interface{}{
		"brokers":                 []string{"127.0.0.1:9092"},
		"topic":                   "cdc",
		"max_txn_buffered_events": 1000,
		"max_txn_buffered_bytes":  float64(64),
	})
	s.writer = writer
	require.NoError(t, s.BeginTransaction(context.Background()))

	big := make([]byte, 80)
	for i := range big {
		big[i] = 'x'
	}
	err := s.Write(context.Background(), &sink.ChangeEvent{
		TaskID: "task", SourceSchema: "db", SourceTable: "users",
		EventType: "INSERT", BinlogPos: 1,
		After: map[string]interface{}{"payload": string(big)},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "hard limit")
	require.NoError(t, s.RollbackTransaction(context.Background()))
}
