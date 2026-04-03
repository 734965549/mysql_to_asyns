# Kubernetes 部署说明

本目录包含 `mysql-to-async` 的基础部署清单：

- `configmap.yaml`：非敏感配置（环境变量）
- `secret.yaml`：敏感配置（用户名/密码）
- `deployment.yaml`：应用 Deployment
- `service.yaml`：对外访问 Service（默认 `NodePort`）
- `ingress.yaml`：域名访问入口（需集群已安装 Ingress Controller）

## 1. 部署前准备

请先修改以下内容：

1. `deployment.yaml` 中镜像地址
   - `image: your-registry/mysql-to-async:latest`
2. `secret.yaml` 中密码账号及加密密钥
   - `MYSQL_TO_ASYNC_DATASOURCE_USERNAME`
   - `MYSQL_TO_ASYNC_DATASOURCE_PASSWORD`
   - `MYSQL_TO_ASYNC_TARGET_USERNAME`
   - `MYSQL_TO_ASYNC_TARGET_PASSWORD`
   - `MYSQL_TO_ASYNC_SECURITY_ENCRYPT_KEY`（任务密码 AES-256-GCM 加密密钥，建议 32 字节；留空则不加密）
3. 如有需要，调整 `configmap.yaml` 中数据库名、Redis 地址等参数

## 2. 应用清单

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/ingress.yaml
```

## 3. 查看状态

```bash
kubectl get pods -l app=mysql-to-async
kubectl get svc mysql-to-async
kubectl logs -f deploy/mysql-to-async
```

## 4. 访问方式（NodePort）

当前 `service.yaml` 使用：

- `type: NodePort`
- `nodePort: 30080`

访问地址：

- `http://<任意节点IP>:30080/health`
- `http://<任意节点IP>:30080/api/health`
- `http://<任意节点IP>:30080/api/tasks`

## 5. 如果你想改成 ClusterIP

将 `service.yaml` 里的：

- `type: NodePort`
- `nodePort: 30080`

改为：

- `type: ClusterIP`
- 删除 `nodePort` 字段

然后重新应用：

```bash
kubectl apply -f k8s/service.yaml
```

ClusterIP 只允许集群内访问，通常需要配合 Ingress 或网关对外暴露。

## 6. 使用 Ingress 走域名访问

`ingress.yaml` 默认配置：

- `ingressClassName: nginx`
- 域名：`mysql-to-async.local`
- 后端服务：`mysql-to-async:8080`

应用 Ingress：

```bash
kubectl apply -f k8s/ingress.yaml
kubectl get ingress mysql-to-async
```

如果你在本机测试，可在 hosts 中加入（将 IP 改为你的 Ingress 对外地址）：

```text
<INGRESS_IP> mysql-to-async.local
```

访问：

- `http://mysql-to-async.local/health`
- `http://mysql-to-async.local/api/health`
- `http://mysql-to-async.local/api/tasks`
