#!/usr/bin/env bash
# 单次标准观测编排：构建 → 中间件 → 流 → 生成器 → 排空 → 批 → 对账 → Gate → 观测。
#
# 必填：
#   TARGET_DT=YYYY-MM-DD  Gate 的目标完整事件天
#
# 常用可选：
#   PROFILE=observation-normal|observation-incidents|observation-peak
#   DAYS=3
#   RUN_NAME=自定义名称
#   SKIP_BUILD=true
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

TARGET_DT="${TARGET_DT:-auto}"
PROFILE="${PROFILE:-observation-normal}"
DAYS="${DAYS:-3}"
RUN_NAME="${RUN_NAME:-${PROFILE}-$(date +%Y%m%d-%H%M%S)}"
SKIP_BUILD="${SKIP_BUILD:-false}"
STREAM_RESET="${STREAM_RESET:-false}"
START_AIRFLOW="${START_AIRFLOW:-true}"

[[ -n "${DURATION_SECONDS:-}" ]] && export CRT_RUN_DURATION_SECONDS="$DURATION_SECONDS"
[[ -n "${TARGET_EVENT_DAYS:-}" ]] && export CRT_TARGET_EVENT_DAYS="$TARGET_EVENT_DAYS"
[[ -n "${MAX_EVENTS:-}" ]] && export CRT_MAX_EVENTS="$MAX_EVENTS"

COMPOSE=(docker compose -f infra/docker-compose.yml)
AIRFLOW_COMPOSE=(docker compose -f infra/airflow/docker-compose.yml)

echo "[observation] run=$RUN_NAME profile=$PROFILE target_dt=$TARGET_DT days=$DAYS"

if [[ "$SKIP_BUILD" != "true" ]]; then
  mvn -o -s RealTime/settings.xml -B -DskipTests package
  (
    cd RealTime
    mvn -o -s settings.xml -B -DskipTests package
  )
  "${COMPOSE[@]}" --profile generator build generator
fi

infra/scripts/bootstrap_runtime.sh

if [[ "$START_AIRFLOW" == "true" ]]; then
  echo "[observation] starting Airflow"
  "${AIRFLOW_COMPOSE[@]}" up -d
  airflow_ready=false
  for _ in $(seq 1 60); do
    if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
      airflow_ready=true
      break
    fi
    sleep 2
  done
  [[ "$airflow_ready" == true ]] || {
    "${AIRFLOW_COMPOSE[@]}" ps -a >&2 || true
    echo "[observation] Airflow did not become healthy" >&2
    exit 1
  }
  docker exec crt-airflow-airflow-scheduler-1 airflow dags pause crt_daily_batch >/dev/null
  docker exec crt-airflow-airflow-scheduler-1 airflow dags pause crt_weekly_vacuum >/dev/null
fi

cleanup_stream() {
  "${COMPOSE[@]}" stop stream >/dev/null 2>&1 || true
  "${COMPOSE[@]}" rm -f stream >/dev/null 2>&1 || true
}
trap cleanup_stream EXIT

cleanup_stream
"${COMPOSE[@]}" rm -f generator >/dev/null 2>&1 || true

echo "[observation] starting stream"
CRT_STREAM_RESET="$STREAM_RESET" "${COMPOSE[@]}" --profile stream up -d stream

ready=false
for _ in $(seq 1 90); do
  if "${COMPOSE[@]}" logs stream 2>/dev/null | grep -q '\[StreamApp\] started'; then
    ready=true
    break
  fi
  sleep 2
done
[[ "$ready" == true ]] || {
  "${COMPOSE[@]}" logs --tail 150 stream >&2 || true
  echo "[observation] stream did not become ready" >&2
  exit 1
}

echo "[observation] starting generator"
CRT_GENERATOR_PROFILE="$PROFILE" CRT_RUN_NAME="$RUN_NAME" \
  "${COMPOSE[@]}" --profile generator up generator

echo "[observation] generator finished; waiting 15s for ODS drain"
sleep 15

cleanup_stream

if [[ "$TARGET_DT" == "auto" ]]; then
  TARGET_DT="$(
    infra/scripts/run_shell.sh /opt/crt/infra/scripts/latest_complete_dt.scala 2>&1 \
      | tr -d '\r' \
      | grep -o 'LATEST_COMPLETE_DT=[0-9-]*' \
      | tail -1 \
      | cut -d= -f2
  )"
fi
[[ "$TARGET_DT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || {
  echo "[observation] cannot determine complete TARGET_DT (got '$TARGET_DT')" >&2
  exit 1
}
echo "[observation] resolved target_dt=$TARGET_DT"

echo "[observation] batch"
infra/scripts/run_warehouse.sh com.hub.app.BatchApp --step all --days "$DAYS"

echo "[observation] verify"
infra/scripts/run_warehouse.sh com.hub.app.VerifyApp --days "$DAYS"

echo "[observation] gate target=$TARGET_DT"
infra/scripts/run_warehouse.sh \
  com.hub.app.GateApp --stage all --dt "$TARGET_DT" --days "$DAYS"

echo "[observation] report"
CRT_GENERATOR_RUN_NAME="$RUN_NAME" infra/scripts/observe_warehouse.sh

"${COMPOSE[@]}" rm -f generator >/dev/null 2>&1 || true
echo "[observation] completed: $RUN_NAME"
