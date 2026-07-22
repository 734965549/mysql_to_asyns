package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"mysql-to-sync/internal/metrics"
	"mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/pkg/logger"
)

const (
	defaultMaxTxnBufferedEvents = 100_000
	defaultMaxTxnBufferedBytes  = 256 << 20 // 256 MiB
)

type WebhookSink struct {
	mu     sync.RWMutex
	client *http.Client
	closed bool

	urlStr         string
	method         string
	timeoutMs      int
	headers        map[string]string
	retryTimes     int
	retryBackoffMs int

	maxTxnEvents int
	maxTxnBytes  int64

	txnActive bool
	txnBytes  int64
	// txnBuffer 暂存同一源事务内的事件；超 maxTxnEvents/maxTxnBytes 硬上限时拒绝，避免绕过 subscriber spill 后 OOM。
	txnBuffer []*sink.ChangeEvent
}

func NewWebhookSink(options map[string]interface{}) *WebhookSink {
	headers := getStringMap(options, "headers")
	if headers == nil {
		headers = make(map[string]string)
	}

	return &WebhookSink{
		urlStr:         getString(options, "url"),
		method:         getStringWithDefault(options, "method", "POST"),
		timeoutMs:      getIntWithDefault(options, "timeout_ms", 3000),
		headers:        headers,
		retryTimes:     getIntWithDefault(options, "retry_times", 3),
		retryBackoffMs: getIntWithDefault(options, "retry_backoff_ms", 500),
		maxTxnEvents:   getIntWithDefault(options, "max_txn_buffered_events", defaultMaxTxnBufferedEvents),
		maxTxnBytes:    getInt64WithDefault(options, "max_txn_buffered_bytes", defaultMaxTxnBufferedBytes),
	}
}

func (s *WebhookSink) Type() sink.SinkType {
	return sink.SinkTypeHTTPWebhook
}

func (s *WebhookSink) Validate() error {
	if s.urlStr == "" {
		return fmt.Errorf("webhook url is required")
	}
	parsedURL, err := url.ParseRequestURI(s.urlStr)
	if err != nil {
		return fmt.Errorf("invalid webhook url: %w", err)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("unsupported url scheme %q, must be http or https", parsedURL.Scheme)
	}
	if parsedURL.Host == "" {
		return fmt.Errorf("webhook url host is required")
	}
	if _, err := http.NewRequest(s.method, s.urlStr, nil); err != nil {
		return fmt.Errorf("invalid webhook method: %w", err)
	}
	if s.timeoutMs <= 0 {
		return fmt.Errorf("timeout_ms must be greater than 0")
	}
	if s.retryTimes < 0 {
		return fmt.Errorf("retry_times must not be negative")
	}
	if s.retryBackoffMs < 0 {
		return fmt.Errorf("retry_backoff_ms must not be negative")
	}
	if s.maxTxnEvents <= 0 {
		return fmt.Errorf("max_txn_buffered_events must be greater than 0")
	}
	if s.maxTxnBytes <= 0 {
		return fmt.Errorf("max_txn_buffered_bytes must be greater than 0")
	}
	return nil
}

func (s *WebhookSink) Open(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("webhook sink is closed")
	}
	if s.client != nil {
		return fmt.Errorf("webhook sink is already open")
	}
	if err := s.Validate(); err != nil {
		return err
	}

	s.client = &http.Client{
		Timeout: time.Duration(s.timeoutMs) * time.Millisecond,
	}

	logger.Info("[WebhookSink] Opened client to url=%s method=%s timeout=%dms retry_times=%d retry_backoff=%dms",
		s.urlStr, s.method, s.timeoutMs, s.retryTimes, s.retryBackoffMs)

	return nil
}

func (s *WebhookSink) Write(ctx context.Context, event *sink.ChangeEvent) error {
	if event == nil {
		return fmt.Errorf("change event is required")
	}

	s.mu.Lock()
	client := s.client
	txnActive := s.txnActive
	if client == nil {
		s.mu.Unlock()
		return fmt.Errorf("webhook sink is not open")
	}
	if txnActive {
		approx := estimateChangeEventBytes(event)
		if len(s.txnBuffer)+1 > s.maxTxnEvents || s.txnBytes+approx > s.maxTxnBytes {
			metrics.GetMetrics().IncrementIncrementalSinkTxnBufferLimit()
			err := fmt.Errorf("webhook sink transaction buffer exceeded hard limit (events=%d+%d bytes=%d+%d max_events=%d max_bytes=%d): "+
				"raise max_txn_buffered_events/max_txn_buffered_bytes or reduce source transaction size",
				len(s.txnBuffer), 1, s.txnBytes, approx, s.maxTxnEvents, s.maxTxnBytes)
			s.mu.Unlock()
			return err
		}
		copied := *event
		if event.PrimaryKeys != nil {
			copied.PrimaryKeys = cloneMap(event.PrimaryKeys)
		}
		if event.Before != nil {
			copied.Before = cloneMap(event.Before)
		}
		if event.After != nil {
			copied.After = cloneMap(event.After)
		}
		s.txnBuffer = append(s.txnBuffer, &copied)
		s.txnBytes += approx
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	return s.deliver(ctx, client, event)
}

func (s *WebhookSink) BeginTransaction(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return fmt.Errorf("webhook sink is not open")
	}
	if s.txnActive {
		return fmt.Errorf("webhook sink transaction already active")
	}
	s.txnActive = true
	s.txnBuffer = s.txnBuffer[:0]
	s.txnBytes = 0
	return nil
}

func (s *WebhookSink) CommitTransaction(ctx context.Context) error {
	s.mu.Lock()
	client := s.client
	events := append([]*sink.ChangeEvent(nil), s.txnBuffer...)
	s.txnBuffer = s.txnBuffer[:0]
	s.txnBytes = 0
	s.txnActive = false
	s.mu.Unlock()

	for _, event := range events {
		if err := s.deliver(ctx, client, event); err != nil {
			return fmt.Errorf("commit webhook sink transaction: %w", err)
		}
	}
	return nil
}

func (s *WebhookSink) RollbackTransaction(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txnBuffer = s.txnBuffer[:0]
	s.txnBytes = 0
	s.txnActive = false
	return nil
}

func (s *WebhookSink) deliver(ctx context.Context, client *http.Client, event *sink.ChangeEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serialize event: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= s.retryTimes; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(s.retryBackoffMs) * time.Millisecond
			logger.Warn("[WebhookSink] Retry attempt %d/%d after %v for %s",
				attempt, s.retryTimes, backoff, s.urlStr)

			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, s.method, s.urlStr, bytes.NewReader(body))
		if err != nil {
			lastErr = fmt.Errorf("create request: %w", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		for k, v := range s.headers {
			req.Header.Set(k, v)
		}
		if event.TraceID != "" && req.Header.Get("Idempotency-Key") == "" {
			req.Header.Set("Idempotency-Key", event.TraceID)
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("send request: %w", err)
			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		lastErr = fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return fmt.Errorf("webhook write failed after %d retries: %w", s.retryTimes+1, lastErr)
}

func (s *WebhookSink) Flush(ctx context.Context) error {
	return nil
}

func (s *WebhookSink) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	if s.client != nil {
		s.client.CloseIdleConnections()
	}
	s.client = nil

	return nil
}

func getString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

func getStringWithDefault(m map[string]interface{}, key string, defaultVal string) string {
	v := getString(m, key)
	if v == "" {
		return defaultVal
	}
	return v
}

func getIntWithDefault(m map[string]interface{}, key string, defaultVal int) int {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	case json.Number:
		n, err := val.Int64()
		if err != nil {
			return defaultVal
		}
		return int(n)
	}
	return defaultVal
}

func getInt64WithDefault(m map[string]interface{}, key string, defaultVal int64) int64 {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int:
		return int64(val)
	case int64:
		return val
	case json.Number:
		n, err := val.Int64()
		if err != nil {
			return defaultVal
		}
		return n
	}
	return defaultVal
}

func cloneMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func estimateChangeEventBytes(event *sink.ChangeEvent) int64 {
	if event == nil {
		return 0
	}
	var total int64
	total += int64(len(event.TaskID) + len(event.SourceSchema) + len(event.SourceTable) + len(event.EventType) + len(event.BinlogFile) + len(event.TraceID))
	total += estimateMapBytes(event.PrimaryKeys)
	total += estimateMapBytes(event.Before)
	total += estimateMapBytes(event.After)
	return total
}

func estimateMapBytes(m map[string]interface{}) int64 {
	var total int64
	for k, v := range m {
		total += int64(len(k))
		switch x := v.(type) {
		case nil:
		case []byte:
			total += int64(len(x))
		case string:
			total += int64(len(x))
		default:
			total += 64
		}
	}
	return total
}

func getStringMap(m map[string]interface{}, key string) map[string]string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	switch val := v.(type) {
	case map[string]string:
		return val
	case map[string]interface{}:
		result := make(map[string]string, len(val))
		for k, iv := range val {
			if s, ok := iv.(string); ok {
				result[k] = s
			}
		}
		return result
	}
	return nil
}
