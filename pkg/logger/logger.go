package logger // 声明当前文件属于logger包，用于日志功能

import ( // 导入外部包
	"fmt" // 导入fmt包，用于格式化字符串
	"io" // 导入io包，用于输入输出接口
	"log" // 导入log包，用于基础日志功能
	"os" // 导入os包，用于操作系统接口
	"path/filepath" // 导入path/filepath包，用于文件路径操作
	"sync" // 导入sync包，用于并发控制
	"time" // 导入time包，用于时间处理
)

// LogLevel 日志级别类型定义
type LogLevel int // 定义日志级别类型为int

const ( // 常量定义
	DEBUG LogLevel = iota // DEBUG级别，值为0，使用iota自动递增
	INFO // INFO级别，值为1
	WARN // WARN级别，值为2
	ERROR // ERROR级别，值为3
)

var levelNames = map[LogLevel]string{ // 定义日志级别名称映射
	DEBUG: "DEBUG", // DEBUG级别对应字符串"DEBUG"
	INFO:  "INFO", // INFO级别对应字符串"INFO"
	WARN:  "WARN", // WARN级别对应字符串"WARN"
	ERROR: "ERROR", // ERROR级别对应字符串"ERROR"
}

// Logger 日志器结构体
type Logger struct { // 定义Logger结构体
	mu          sync.Mutex // 互斥锁，用于保证线程安全
	level       LogLevel // 日志级别，低于此级别的日志不输出
	consoleOut  io.Writer // 控制台输出接口
	fileOut     *os.File // 日志文件句柄
	logger      *log.Logger // 控制台日志器
	fileLogger  *log.Logger // 文件日志器
	enableColor bool // 是否启用颜色输出
}

var defaultLogger *Logger // 默认日志器实例
var once sync.Once // 确保只初始化一次的sync.Once

// Config 日志配置结构体
type Config struct { // 定义日志配置结构体
	Level      string // 日志级别字符串
	Console    bool // 是否输出到控制台
	File       bool // 是否输出到文件
	FilePath   string // 日志文件路径
	EnableColor bool // 是否启用颜色输出
}

// Init 初始化日志器函数
func Init(cfg *Config) error { // 初始化日志器
	var err error // 定义错误变量
	once.Do(func() { // 确保只执行一次
		defaultLogger, err = newLogger(cfg) // 创建新的日志器实例
	})
	return err // 返回错误
}

// newLogger 创建新的日志器函数
func newLogger(cfg *Config) (*Logger, error) { // 创建新日志器
	l := &Logger{ // 创建Logger实例
		level:       parseLevel(cfg.Level), // 解析日志级别
		consoleOut:  os.Stdout, // 设置控制台输出为标准输出
		enableColor: cfg.EnableColor, // 设置是否启用颜色
	}

	// 控制台logger
	l.logger = log.New(l.consoleOut, "", 0) // 创建控制台日志器

	// 文件logger
	if cfg.File { // 如果启用文件日志
		if cfg.FilePath == "" { // 如果未设置文件路径
			cfg.FilePath = "logs/app.log" // 使用默认路径
		}
		
		// 确保目录存在
		dir := filepath.Dir(cfg.FilePath) // 获取文件所在目录
		if err := os.MkdirAll(dir, 0755); err != nil { // 创建目录
			return nil, fmt.Errorf("failed to create log directory: %w", err) // 返回错误
		}

		file, err := os.OpenFile(cfg.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666) // 打开文件
		if err != nil { // 如果打开文件失败
			return nil, fmt.Errorf("failed to open log file: %w", err) // 返回错误
		}
		l.fileOut = file // 设置文件句柄
		l.fileLogger = log.New(file, "", 0) // 创建文件日志器
	}

	// 如果禁用控制台输出，设置为discard
	if !cfg.Console { // 如果禁用控制台输出
		l.consoleOut = io.Discard // 设置为丢弃输出
		l.logger = log.New(io.Discard, "", 0) // 创建丢弃日志器
	}

	return l, nil // 返回日志器实例和nil错误
}

// parseLevel 解析日志级别函数
func parseLevel(level string) LogLevel { // 解析日志级别字符串
	switch level { // 根据字符串匹配级别
	case "debug", "DEBUG": // 如果是debug
		return DEBUG // 返回DEBUG级别
	case "info", "INFO": // 如果是info
		return INFO // 返回INFO级别
	case "warn", "WARN": // 如果是warn
		return WARN // 返回WARN级别
	case "error", "ERROR": // 如果是error
		return ERROR // 返回ERROR级别
	default: // 默认情况
		return DEBUG // 返回DEBUG级别
	}
}

// formatMessage 格式化日志消息方法
func (l *Logger) formatMessage(level LogLevel, format string, args ...interface{}) string { // 格式化日志消息
	timestamp := time.Now().Format("2006-01-02 15:04:05.000") // 获取当前时间戳
	levelName := levelNames[level] // 获取日志级别名称
	msg := fmt.Sprintf(format, args...) // 格式化消息内容
	return fmt.Sprintf("[%s] [%s] %s", timestamp, levelName, msg) // 返回格式化后的消息
}

// log 写入日志方法
func (l *Logger) log(level LogLevel, format string, args ...interface{}) { // 写入日志
	if level < l.level { // 如果日志级别低于当前级别
		return // 直接返回，不输出
	}

	l.mu.Lock() // 获取互斥锁
	defer l.mu.Unlock() // 延迟释放锁

	msg := l.formatMessage(level, format, args...) // 格式化消息

	// 写入控制台（带颜色）
	if l.consoleOut != io.Discard { // 如果控制台输出未禁用
		if l.enableColor { // 如果启用颜色
			colorCode := getColorCode(level) // 获取颜色代码
			resetCode := "\033[0m" // 重置颜色代码
			l.logger.Println(colorCode + msg + resetCode) // 输出带颜色的日志
		} else { // 如果未启用颜色
			l.logger.Println(msg) // 输出不带颜色的日志
		}
	}

	// 写入文件（不带颜色）
	if l.fileLogger != nil { // 如果文件日志器存在
		l.fileLogger.Println(msg) // 输出到文件
	}
}

// getColorCode 获取颜色代码函数
func getColorCode(level LogLevel) string { // 获取日志级别对应的颜色代码
	switch level { // 根据级别匹配颜色
	case DEBUG: // DEBUG级别
		return "\033[36m" // 返回青色代码
	case INFO: // INFO级别
		return "\033[32m" // 返回绿色代码
	case WARN: // WARN级别
		return "\033[33m" // 返回黄色代码
	case ERROR: // ERROR级别
		return "\033[31m" // 返回红色代码
	default: // 默认情况
		return "\033[0m" // 返回重置代码
	}
}

// Close 关闭日志器方法
func (l *Logger) Close() error { // 关闭日志器
	l.mu.Lock() // 获取互斥锁
	defer l.mu.Unlock() // 延迟释放锁
	
	if l.fileOut != nil { // 如果文件句柄存在
		return l.fileOut.Close() // 关闭文件
	}
	return nil // 返回nil
}

// 全局日志函数

// Debug 调试日志函数
func Debug(format string, args ...interface{}) { // 输出DEBUG级别日志
	if defaultLogger != nil { // 如果默认日志器存在
		defaultLogger.log(DEBUG, format, args...) // 调用日志器的log方法
	}
}

// Info 信息日志函数
func Info(format string, args ...interface{}) { // 输出INFO级别日志
	if defaultLogger != nil { // 如果默认日志器存在
		defaultLogger.log(INFO, format, args...) // 调用日志器的log方法
	}
}

// Warn 警告日志函数
func Warn(format string, args ...interface{}) { // 输出WARN级别日志
	if defaultLogger != nil { // 如果默认日志器存在
		defaultLogger.log(WARN, format, args...) // 调用日志器的log方法
	}
}

// Error 错误日志函数
func Error(format string, args ...interface{}) { // 输出ERROR级别日志
	if defaultLogger != nil { // 如果默认日志器存在
		defaultLogger.log(ERROR, format, args...) // 调用日志器的log方法
	}
}

// Fatal 致命错误日志函数
func Fatal(format string, args ...interface{}) { // 输出致命错误日志
	Error(format, args...) // 先输出ERROR级别日志
	os.Exit(1) // 退出程序
}

// Close 关闭默认日志器函数
func Close() error { // 关闭默认日志器
	if defaultLogger != nil { // 如果默认日志器存在
		return defaultLogger.Close() // 调用日志器的Close方法
	}
	return nil // 返回nil
}
