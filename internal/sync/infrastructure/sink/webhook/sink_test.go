package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mysql-to-sync/internal/sync/domain/sink"
)

func TestNewWebhookSink_Defaults(t *testing.T) {
	options := map[string]interface{}{
		"url": "http://127.0.0.1:9000/cdc/event",
	}

	s := NewWebhookSink(options)

	assert.Equal(t, "http://127.0.0.1:9000/cdc/event", s.urlStr)
	assert.Equal(t, "POST", s.method)
	assert.Equal(t, 3000, s.timeoutMs)
	assert.Equal(t, 3, s.retryTimes)
	assert.Equal(t, 500, s.retryBackoffMs)
	assert.Empty(t, s.headers)
}

func TestNewWebhookSink_CustomOptions(t *testing.T) {
	options := map[string]interface{}{
		"url":              "https://example.com/webhook",
		"method":           "PUT",
		"timeout_ms":       float64(5000),
		"retry_times":      float64(5),
		"retry_backoff_ms": float64(1000),
		"headers": map[string]interface{}{
			"Authorization": "Bearer token123",
			"X-Custom":      "value",
		},
	}

	s := NewWebhookSink(options)

	assert.Equal(t, "https://example.com/webhook", s.urlStr)
	assert.Equal(t, "PUT", s.method)
	assert.Equal(t, 5000, s.timeoutMs)
	assert.Equal(t, 5, s.retryTimes)
	assert.Equal(t, 1000, s.retryBackoffMs)
	assert.Equal(t, "Bearer token123", s.headers["Authorization"])
	assert.Equal(t, "value", s.headers["X-Custom"])
}

func TestWebhookSink_Type(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{
		"url": "http://127.0.0.1:9000/cdc/event",
	})
	assert.Equal(t, sink.SinkTypeHTTPWebhook, s.Type())
}

func TestWebhookSink_Open_EmptyURL(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{})
	err := s.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "url is required")
}

func TestWebhookSink_Open_InvalidScheme(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{
		"url": "ftp://127.0.0.1:9000/cdc/event",
	})
	err := s.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported url scheme")
}

func TestWebhookSink_Open_InvalidURL(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{
		"url": "://invalid",
	})
	err := s.Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid webhook url")
}

func TestWebhookSink_ValidateOptions(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]interface{}
		want    string
	}{
		{name: "valid", options: map[string]interface{}{"url": "https://example.com/hook"}},
		{name: "missing host", options: map[string]interface{}{"url": "http:///hook"}, want: "host"},
		{name: "invalid method", options: map[string]interface{}{"url": "https://example.com/hook", "method": "BAD METHOD"}, want: "method"},
		{name: "invalid timeout", options: map[string]interface{}{"url": "https://example.com/hook", "timeout_ms": 0}, want: "timeout_ms"},
		{name: "invalid retries", options: map[string]interface{}{"url": "https://example.com/hook", "retry_times": -1}, want: "retry_times"},
		{name: "invalid backoff", options: map[string]interface{}{"url": "https://example.com/hook", "retry_backoff_ms": -1}, want: "retry_backoff_ms"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewWebhookSink(tt.options).Validate()
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestWebhookSink_Write_Success(t *testing.T) {
	var receivedBody []byte
	var receivedMethod string
	var receivedContentType string
	var receivedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedContentType = r.Header.Get("Content-Type")
		receivedAuth = r.Header.Get("Authorization")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookSink(map[string]interface{}{
		"url": server.URL,
		"headers": map[string]interface{}{
			"Authorization": "Bearer test-token",
		},
	})
	err := s.Open(context.Background())
	require.NoError(t, err)

	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
		PrimaryKeys:  map[string]interface{}{"id": float64(42)},
		After:        map[string]interface{}{"id": float64(42), "status": float64(1)},
	}

	err = s.Write(context.Background(), event)
	require.NoError(t, err)

	assert.Equal(t, "POST", receivedMethod)
	assert.Equal(t, "application/json", receivedContentType)
	assert.Equal(t, "Bearer test-token", receivedAuth)

	var parsed map[string]interface{}
	err = json.Unmarshal(receivedBody, &parsed)
	require.NoError(t, err)
	assert.Equal(t, "task_001", parsed["task_id"])
	assert.Equal(t, "INSERT", parsed["event_type"])
	assert.Equal(t, float64(12345), parsed["binlog_pos"])
}

func TestWebhookSink_Write_RetryThenSuccess(t *testing.T) {
	var callCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		if count < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookSink(map[string]interface{}{
		"url":              server.URL,
		"retry_times":      float64(3),
		"retry_backoff_ms": float64(1),
	})
	err := s.Open(context.Background())
	require.NoError(t, err)

	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
	}

	err = s.Write(context.Background(), event)
	require.NoError(t, err)
	assert.Equal(t, int32(3), callCount.Load())
}

func TestWebhookSink_Write_RetryExhausted(t *testing.T) {
	var callCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	s := NewWebhookSink(map[string]interface{}{
		"url":              server.URL,
		"retry_times":      float64(2),
		"retry_backoff_ms": float64(1),
	})
	err := s.Open(context.Background())
	require.NoError(t, err)

	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
	}

	err = s.Write(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed after")
	assert.Contains(t, err.Error(), "retries")
	assert.Equal(t, int32(3), callCount.Load())
}

func TestWebhookSink_Write_ContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	s := NewWebhookSink(map[string]interface{}{
		"url":              server.URL,
		"retry_times":      float64(5),
		"retry_backoff_ms": float64(100),
	})
	err := s.Open(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err = s.Write(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled")
}

func TestWebhookSink_Write_NotOpen(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{
		"url": "http://127.0.0.1:9000/cdc/event",
	})

	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
	}

	err := s.Write(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not open")
}

func TestWebhookSink_Write_NilEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	s := NewWebhookSink(map[string]interface{}{"url": server.URL})
	require.NoError(t, s.Open(context.Background()))
	err := s.Write(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "event")
}

func TestWebhookSink_Write_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	s := NewWebhookSink(map[string]interface{}{
		"url": server.URL, "timeout_ms": 5, "retry_times": 0,
	})
	require.NoError(t, s.Open(context.Background()))
	err := s.Write(context.Background(), &sink.ChangeEvent{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Client.Timeout")
}

func TestWebhookSink_Flush(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{
		"url": "http://127.0.0.1:9000/cdc/event",
	})
	err := s.Flush(context.Background())
	assert.NoError(t, err)
}

func TestWebhookSink_Close_Idempotent(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{
		"url": "http://127.0.0.1:9000/cdc/event",
	})

	err := s.Close(context.Background())
	assert.NoError(t, err)

	err = s.Close(context.Background())
	assert.NoError(t, err)
}

func TestWebhookSink_Write_AfterClose(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{
		"url": "http://127.0.0.1:9000/cdc/event",
	})
	err := s.Open(context.Background())
	require.NoError(t, err)

	err = s.Close(context.Background())
	require.NoError(t, err)

	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
	}

	err = s.Write(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not open")
}

func TestWebhookSink_Write_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	s := NewWebhookSink(map[string]interface{}{
		"url":              server.URL,
		"retry_times":      float64(0),
		"retry_backoff_ms": float64(1),
	})
	err := s.Open(context.Background())
	require.NoError(t, err)

	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
	}

	err = s.Write(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 502")
}

func TestWebhookSink_Write_ConnectionRefused(t *testing.T) {
	s := NewWebhookSink(map[string]interface{}{
		"url":              "http://127.0.0.1:1/cdc/event",
		"timeout_ms":       float64(100),
		"retry_times":      float64(1),
		"retry_backoff_ms": float64(1),
	})
	err := s.Open(context.Background())
	require.NoError(t, err)

	event := &sink.ChangeEvent{
		TaskID:       "task_001",
		SourceSchema: "db1",
		SourceTable:  "orders",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		BinlogFile:   "mysql-bin.000001",
		BinlogPos:    12345,
	}

	err = s.Write(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed after")
}

func TestWebhookSink_Write_DifferentMethods(t *testing.T) {
	tests := []struct {
		name   string
		method string
	}{
		{"POST", "POST"},
		{"PUT", "PUT"},
		{"PATCH", "PATCH"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var receivedMethod string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedMethod = r.Method
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			s := NewWebhookSink(map[string]interface{}{
				"url":    server.URL,
				"method": tt.method,
			})
			err := s.Open(context.Background())
			require.NoError(t, err)

			event := &sink.ChangeEvent{
				TaskID:       "task_001",
				SourceSchema: "db1",
				SourceTable:  "orders",
				EventType:    "INSERT",
				EventTime:    time.Now(),
			}

			err = s.Write(context.Background(), event)
			require.NoError(t, err)
			assert.Equal(t, tt.method, receivedMethod)
		})
	}
}

func TestGetStringMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		key      string
		expected map[string]string
	}{
		{
			name: "string_map",
			input: map[string]interface{}{
				"headers": map[string]interface{}{
					"Authorization": "Bearer token",
					"X-Custom":      "value",
				},
			},
			key: "headers",
			expected: map[string]string{
				"Authorization": "Bearer token",
				"X-Custom":      "value",
			},
		},
		{
			name:     "missing_key",
			input:    map[string]interface{}{},
			key:      "headers",
			expected: nil,
		},
		{
			name: "wrong_type",
			input: map[string]interface{}{
				"headers": "not-a-map",
			},
			key:      "headers",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getStringMap(tt.input, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}
