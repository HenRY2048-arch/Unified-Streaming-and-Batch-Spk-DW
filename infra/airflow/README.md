# CRT Airflow（P3.8）

## 文件

- `dags/crt_daily_batch.py`：每日批链路。
- `dags/crt_weekly_vacuum.py`：人工触发的 Delta VACUUM（默认 7 天保留期）。

## 部署假设

Airflow 用 **LocalExecutor + Postgres**，不引入 Celery/Redis broker；`StreamApp` 作为常驻服务留在 DAG 外。
Airflow worker 需要能执行项目根目录的 `infra/scripts/run_warehouse.sh`：可以是宿主机 venv，也可以是挂载
Docker socket 的 Airflow 容器。用 `CRT_PROJECT_ROOT` 指向项目根目录（默认
`/home/henry/projects/Community_Real_Time`）。

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
