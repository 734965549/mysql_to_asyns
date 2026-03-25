package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogLevel 日志级别
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

var levelNames = map[LogLevel]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	ERROR: "ERROR",
}

// Logger 日志器
type Logger struct {
	mu          sync.Mutex
	level       LogLevel
	consoleOut  io.Writer
	fileOut     *os.File
	logger      *log.Logger
	fileLogger  *log.Logger
	enableColor bool
}

var defaultLogger *Logger
var once sync.Once

// Config 日志配置
type Config struct {
	Level      string
	Console    bool
	File       bool
	FilePath   string
	EnableColor bool
}

// Init 初始化日志器
func Init(cfg *Config) error {
	var err error
	once.Do(func() {
		defaultLogger, err = newLogger(cfg)
	})
	return err
}

// newLogger 创建新的日志器
func newLogger(cfg *Config) (*Logger, error) {
	l := &Logger{
		level:       parseLevel(cfg.Level),
		consoleOut:  os.Stdout,
		enableColor: cfg.EnableColor,
	}

	// 控制台logger
	l.logger = log.New(l.consoleOut, "", 0)

	// 文件logger
	if cfg.File {
		if cfg.FilePath == "" {
			cfg.FilePath = "logs/app.log"
		}
		
		// 确保目录存在
		dir := filepath.Dir(cfg.FilePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		file, err := os.OpenFile(cfg.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
		l.fileOut = file
		l.fileLogger = log.New(file, "", 0)
	}

	// 如果禁用控制台输出，设置为discard
	if !cfg.Console {
		l.consoleOut = io.Discard
		l.logger = log.New(io.Discard, "", 0)
	}

	return l, nil
}

// parseLevel 解析日志级别
func parseLevel(level string) LogLevel {
	switch level {
	case "debug", "DEBUG":
		return DEBUG
	case "info", "INFO":
		return INFO
	case "warn", "WARN":
		return WARN
	case "error", "ERROR":
		return ERROR
	default:
		return DEBUG
	}
}

// formatMessage 格式化日志消息
func (l *Logger) formatMessage(level LogLevel, format string, args ...interface{}) string {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	levelName := levelNames[level]
	msg := fmt.Sprintf(format, args...)
	return fmt.Sprintf("[%s] [%s] %s", timestamp, levelName, msg)
}

// log 写入日志
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	msg := l.formatMessage(level, format, args...)

	// 写入控制台（带颜色）
	if l.consoleOut != io.Discard {
		if l.enableColor {
			colorCode := getColorCode(level)
			resetCode := "\033[0m"
			l.logger.Println(colorCode + msg + resetCode)
		} else {
			l.logger.Println(msg)
		}
	}

	// 写入文件（不带颜色）
	if l.fileLogger != nil {
		l.fileLogger.Println(msg)
	}
}

// getColorCode 获取颜色代码
func getColorCode(level LogLevel) string {
	switch level {
	case DEBUG:
		return "\033[36m" // 青色
	case INFO:
		return "\033[32m" // 绿色
	case WARN:
		return "\033[33m" // 黄色
	case ERROR:
		return "\033[31m" // 红色
	default:
		return "\033[0m"
	}
}

// Close 关闭日志器
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	if l.fileOut != nil {
		return l.fileOut.Close()
	}
	return nil
}

// 全局日志函数

// Debug 调试日志
func Debug(format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.log(DEBUG, format, args...)
	}
}

// Info 信息日志
func Info(format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.log(INFO, format, args...)
	}
}

// Warn 警告日志
func Warn(format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.log(WARN, format, args...)
	}
}

// Error 错误日志
func Error(format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.log(ERROR, format, args...)
	}
}

// Fatal 致命错误日志
func Fatal(format string, args ...interface{}) {
	Error(format, args...)
	os.Exit(1)
}

// Close 关闭默认日志器
func Close() error {
	if defaultLogger != nil {
		return defaultLogger.Close()
	}
	return nil
}