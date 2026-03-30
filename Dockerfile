FROM golang:1.24-alpine AS backend-builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o mysql-to-async .


FROM node:20-alpine AS frontend-builder

WORKDIR /frontend

COPY web/package.json web/package-lock.json ./
RUN npm ci

COPY web/ ./
RUN npm run build


FROM alpine:3.20 AS backend

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=backend-builder /app/mysql-to-async ./
COPY --from=backend-builder /app/etc ./etc

RUN mkdir -p /app/data /app/logs/audit

EXPOSE 8080

CMD ["./mysql-to-async"]


FROM nginx:1.27-alpine AS frontend

COPY docker/nginx/default.conf /etc/nginx/conf.d/default.conf
COPY --from=frontend-builder /frontend/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
