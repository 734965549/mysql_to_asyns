package linttasklog

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var allowedTaskLoggerFiles = map[string]struct{}{
	"internal/task/application/service/task_event_recorder.go": {},
}

var taskLoggerPattern = regexp.MustCompile(`logger\.(Warn|Error)\([^)]*\[Task`)

func TestFullloadUsesEventSinkForTaskBusinessLogs(t *testing.T) {
	root := findRepoRoot(t)
	dir := filepath.Join(root, "internal", "sync", "fullload")
	var violations []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		if taskLoggerPattern.MatchString(string(data)) {
			rel, _ := filepath.Rel(root, path)
			violations = append(violations, filepath.ToSlash(rel))
		}
		return nil
	})
	if len(violations) > 0 {
		t.Fatalf("fullload files still use logger.Warn/Error with [Task (use EventSink.Emit): %v", violations)
	}
}

func TestTaskServiceAllowedTaskLoggerFiles(t *testing.T) {
	root := findRepoRoot(t)
	dir := filepath.Join(root, "internal", "task", "application", "service")
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if _, ok := allowedTaskLoggerFiles[rel]; ok {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		if taskLoggerPattern.MatchString(string(data)) {
			// 存量路径允许；本测仅确保扫描逻辑可运行。
			t.Logf("task service still has [Task logger at %s (migrate in later batches)", rel)
		}
		return nil
	})
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	t.Fatal("go.mod not found")
	return ""
}
