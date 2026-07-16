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

	"mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/pkg/logger"
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
	s.mu.RLock()
	client := s.client
	s.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("webhook sink is not open")
	}
	if event == nil {
		return fmt.Errorf("change event is required")
	}

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
