# CRT Airflow（P3.8）

## 文件

- `dags/crt_daily_batch.py`：每日批链路。
- `dags/crt_weekly_vacuum.py`：人工触发的 Delta VACUUM（默认 7 天保留期）。

## 两种运行模式

- **当前模式**：Airflow 2.10.5 + Postgres + LocalExecutor 容器组；Web UI 在 `localhost:8080`，
  两个 DAG 暂停。scheduler 通过 Docker socket 提交一次性 Spark 容器，已实际验证。
- **宿主 venv 备用**：Python 3.11 + SQLite + SequentialExecutor，适合应急调试。

两种模式都要求 Airflow worker 能执行项目根目录的 `infra/scripts/run_warehouse.sh`；用 `CRT_PROJECT_ROOT`
指向项目根目录。`StreamApp` 作为常驻服务留在 DAG 外。

## 当前 Docker 运行方式

```bash
docker compose -f infra/airflow/docker-compose.yml up -d
docker compose -f infra/airflow/docker-compose.yml ps -a
curl -fsS http://localhost:8080/health
```

如需回到宿主 venv 备用模式：

当前仓库仍保留 **Python 3.11 venv + SQLite + SequentialExecutor** 备用入口：

```bash
# 首次安装（已执行）
python3.11 -m venv .airflow-venv
.airflow-venv/bin/pip install 'apache-airflow==2.10.5' \
  --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt'

# 启动（当前已在后台运行）
infra/airflow/run_local.sh webserver --port 8080
infra/airflow/run_local.sh scheduler
```

浏览器打开：

```text
http://localhost:8080
```

本地默认账号（仅开发环境）：

```text
用户名：admin
密码：admin
```

当前两个 DAG **已暂停**，在 UI 里手工打开 `crt_daily_batch` 后才会按 `@daily` 调度。

## Docker Compose 备选

仓库同时保留 `docker-compose.yml`（Airflow 2.10.5 + Postgres + LocalExecutor + Web UI）。本次 Docker Hub
基础层拉取出现 `unexpected EOF`，所以当前没有使用该方案；网络恢复后可执行：

```bash
docker compose -f infra/airflow/docker-compose.yml up -d
docker compose -f infra/airflow/docker-compose.yml ps
```

停止本机 venv 服务：

```bash
infra/airflow/stop_local.sh
```

停止 Docker 备选：

```bash
docker compose -f infra/airflow/docker-compose.yml down
# 会删除 Airflow 元数据库卷：
# docker compose -f infra/airflow/docker-compose.yml down -v
```

## 手动触发参数

`crt_daily_batch` 支持 `dag_run.conf`：

```json
{"dt": "2026-10-19", "days": 2}
```

`dt` 可省略；省略时 GateApp 从 ODS 最大 `dt` 推导。

## 注意

- `gate_dr` 使用 `--wait`，G1（上游未到）由 Gate 内部等待/重试，超时后退出 1；Airflow 不再重复重试。
- G2 不阻断 DAG，但 `ops/partition_status` 为 `pass_with_limitations`，`publish_final=false`。
- `crt_weekly_vacuum` 默认不调度；先确认 `verify/*` 通过、保留期符合预期，再手动触发。
