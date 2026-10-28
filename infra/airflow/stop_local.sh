#!/usr/bin/env bash
# 停止本机 Airflow webserver/scheduler（按实际 venv 进程匹配，避免依赖启动时的 shell PID）。
set -u

for pattern in \
  "/.airflow-venv/bin/airflow webserver" \
  "/.airflow-venv/bin/airflow scheduler"
do
  pids="$(pgrep -f "$pattern" || true)"
  if [ -n "$pids" ]; then
    kill -TERM $pids 2>/dev/null || true
  fi
done
