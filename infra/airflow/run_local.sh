#!/usr/bin/env bash
# 以项目内 Python 3.11 venv 运行 Airflow（本机调试模式）。
set -euo pipefail

ROOT="/home/henry/projects/Community_Real_Time"
export AIRFLOW_HOME="$ROOT/.airflow-home"
# Airflow 的 task 子进程会按 PATH 找 airflow；必须把 venv/bin 放到最前面。
export PATH="$ROOT/.airflow-venv/bin:$PATH"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/infra/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Shanghai
export AIRFLOW__WEBSERVER__SECRET_KEY=crt-airflow-local-secret
export CRT_PROJECT_ROOT="$ROOT"

exec "$ROOT/.airflow-venv/bin/airflow" "$@"
