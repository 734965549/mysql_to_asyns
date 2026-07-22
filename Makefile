.PHONY: build run test clean docker help

# 变量
APP_NAME=mysql-to-sync
VERSION=1.0.0
BUILD_TIME=
GO_VERSION=

# Windows
ifeq (,Windows_NT)
	EXECUTABLE=.exe
else
	EXECUTABLE=
endif

# 默认目标
.DEFAULT_GOAL := help

# 构建应用
build:
	@echo "Building ..."
	@go build -ldflags "-X main.Version= -X main.BuildTime=" -o 
	@echo "Build complete: "

# 运行应用
run:
	@go run main.go

# 运行测试
test:
	@echo "Running tests..."
	@go test -v -cover ./...

# 生成测试覆盖率报告
test-coverage:
	@echo "Generating test coverage report..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# 代码检查
lint:
	@echo "Running linters..."
	@go vet ./...
	@go fmt ./...

# 清理
clean:
	@echo "Cleaning..."
	@rm -f 
	@rm -f coverage.out coverage.html
	@echo "Clean complete"

# Docker构建
docker-build:
	@echo "Building Docker image..."
	@docker build -t : .

# Docker运行
docker-run:
	@echo "Running Docker container..."
	@docker run -p 8080:8080 :

# 前端安装依赖
web-install:
	@echo "Installing frontend dependencies..."
	@cd web && npm install

# 前端开发
web-dev:
	@echo "Starting frontend development server..."
	@cd web && npm run dev

# 前端构建
web-build:
	@echo "Building frontend..."
	@cd web && npm run build

# 全部构建（前后端）
build-all: build web-build
	@echo "Full build complete"

# codebase-memory 知识图谱索引（Windows 请用 PowerShell 脚本）
index-codebase:
	@powershell -NoProfile -ExecutionPolicy Bypass -File scripts/index-codebase.ps1

# 开发模式（同时启动前后端）
dev:
	@echo "Starting development environment..."
	@go run main.go & cd web && npm run dev

# 帮助
help:
	@echo "MySQL-to-Async Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build          Build the application"
	@echo "  run            Run the application"
	@echo "  test           Run tests"
	@echo "  test-coverage  Generate test coverage report"
	@echo "  lint           Run code linters"
	@echo "  clean          Clean build artifacts"
	@echo "  docker-build   Build Docker image"
	@echo "  docker-run     Run Docker container"
	@echo "  web-install    Install frontend dependencies"
	@echo "  web-dev        Start frontend dev server"
	@echo "  web-build      Build frontend"
	@echo "  build-all      Build both frontend and backend"
	@echo "  index-codebase Refresh codebase-memory graph (use scripts/index-codebase.ps1 on Windows)"
	@echo "  dev            Start development environment"
	@echo "  help           Show this help message"
	@echo ""
	@echo "Version: "
	@echo "Go Version: "
