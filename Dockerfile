# 构建阶段
FROM golang:1.24-alpine AS builder

WORKDIR /app

# 安装依赖
RUN apk add --no-cache git

# 复制go mod文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 构建应用
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o mysql-to-async .

# 运行阶段
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# 复制构建产物
COPY --from=builder /app/mysql-to-async .
COPY --from=builder /app/etc ./etc

# 创建必要的目录
RUN mkdir -p /app/data /app/logs/audit

# 暴露端口
EXPOSE 8081

# 运行应用
CMD ["./mysql-to-async"]
