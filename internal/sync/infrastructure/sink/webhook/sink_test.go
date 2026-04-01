package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"mysql-to-async/internal/sync/domain/sink"
)

func TestWebhookSink_Type(t *testing.T) {
	s := NewWebhookSink(Config{URL: "http://localhost"})
	if s.Type() != "HTTP_WEBHOOK" {
		t.Errorf("expected HTTP_WEBHOOK, got %s", s.Type())
	}
}

func TestWebhookSink_Defaults(t *testing.T) {
	s := NewWebhookSink(Config{URL: "http://localhost"})
	if s.config.Method != "POST" {
		t.Errorf("expected default method POST, got %s", s.config.Method)
	}
	if s.config.TimeoutMs != 3000 {
		t.Errorf("expected default timeout 3000, got %d", s.config.TimeoutMs)
	}
	if s.config.RetryTimes != 3 {
		t.Errorf("expected default retries 3, got %d", s.config.RetryTimes)
	}
}

func TestWebhookSink_OpenClose(t *testing.T) {
	s := NewWebhookSink(Config{URL: "http://localhost"})
	ctx := context.Background()

	if err := s.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if s.client == nil {
		t.Fatal("expected client to be initialized after Open")
	}

	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !s.closed {
		t.Fatal("expected closed=true after Close")
	}

	// 幂等性验证
	if err := s.Close(ctx); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}
}

func TestWebhookSink_Write_Success(t *testing.T) {
	var received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookSink(Config{
		URL:        server.URL,
		RetryTimes: 1,
		TimeoutMs:  2000,
	})
	ctx := context.Background()
	if err := s.Open(ctx); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close(ctx)

	event := &sink.ChangeEvent{
		TaskID:       "task1",
		SourceSchema: "db",
		SourceTable:  "users",
		EventType:    "INSERT",
		EventTime:    time.Now(),
		After:        map[string]interface{}{"id": 1},
	}

	if err := s.Write(ctx, event); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if received.Load() != 1 {
		t.Errorf("expected 1 request, got %d", received.Load())
	}
}

func TestWebhookSink_Write_NilEvent(t *testing.T) {
	s := NewWebhookSink(Config{URL: "http://localhost"})
	ctx := context.Background()
	s.Open(ctx)
	if err := s.Write(ctx, nil); err != nil {
		t.Fatalf("Write nil should succeed: %v", err)
	}
}

func TestWebhookSink_Write_Retry(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookSink(Config{
		URL:        server.URL,
		RetryTimes: 3,
		TimeoutMs:  2000,
	})
	ctx := context.Background()
	s.Open(ctx)
	defer s.Close(ctx)

	event := &sink.ChangeEvent{
		TaskID:      "task1",
		SourceTable: "t",
		EventType:   "INSERT",
		EventTime:   time.Now(),
	}

	if err := s.Write(ctx, event); err != nil {
		t.Fatalf("Write should succeed after retries: %v", err)
	}
	if callCount.Load() != 3 {
		t.Errorf("expected 3 attempts (2 failures + 1 success), got %d", callCount.Load())
	}
}

func TestWebhookSink_Write_AllRetriesFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	s := NewWebhookSink(Config{
		URL:        server.URL,
		RetryTimes: 1,
		TimeoutMs:  1000,
	})
	ctx := context.Background()
	s.Open(ctx)
	defer s.Close(ctx)

	event := &sink.ChangeEvent{
		TaskID:      "task1",
		SourceTable: "t",
		EventType:   "INSERT",
		EventTime:   time.Now(),
	}

	err := s.Write(ctx, event)
	if err == nil {
		t.Fatal("expected error when all retries fail")
	}
}

func TestWebhookSink_Write_CustomHeaders(t *testing.T) {
	var gotHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := NewWebhookSink(Config{
		URL:        server.URL,
		RetryTimes: 0,
		TimeoutMs:  2000,
		Headers:    map[string]string{"X-Custom": "test-value"},
	})
	ctx := context.Background()
	s.Open(ctx)
	defer s.Close(ctx)

	event := &sink.ChangeEvent{TaskID: "t1", EventType: "INSERT", EventTime: time.Now()}
	if err := s.Write(ctx, event); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if gotHeader != "test-value" {
		t.Errorf("expected custom header 'test-value', got '%s'", gotHeader)
	}
}

func TestWebhookSink_Flush(t *testing.T) {
	s := NewWebhookSink(Config{URL: "http://localhost"})
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush should always succeed: %v", err)
	}
}
