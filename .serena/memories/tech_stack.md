# Tech stack
- Windows/PowerShell workspace; repository root must resolve exactly to `D:\Epan\BaiduNetdiskDownload\go\mysql_to_asyns` before Serena/Codebase Memory work.
- Backend: Go 1.24, Gin 1.9, go-mysql, go-sql-driver/mysql, Redis v9, Prometheus, cron v3, kafka-go.
- Tests: standard `testing`, testify, go-sqlmock; use miniredis for Redis persistence behavior.
- Frontend: Vue 3.5, Vue Router 4.6, Arco Design Vue 2.55, Vite 8, npm.
- Configuration/deployment: TOML plus env overrides; Docker/Kubernetes assets under `docker/`, `k8s/`, `etc/`.