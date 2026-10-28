#!/usr/bin/env bash
# 数仓整体观测入口：容器/输入/Redis/生成器运行清单 + Spark 全链路报表。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "================================================================================"
echo "1. 容器状态"
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/airflow/docker-compose.yml ps -a 2>/dev/null || true

echo "================================================================================"
echo "2. Kafka offsets"
for topic in content_events content_groundtruth; do
  echo "[$topic]"
  docker exec crt-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:19092 --topic "$topic" 2>/dev/null || echo "  missing"
done

echo "================================================================================"
echo "3. Redis Tier0 投影（不是真值）"
docker exec crt-redis redis-cli HGETALL crt:online 2>/dev/null || true
echo -n "heat_top_size="
docker exec crt-redis redis-cli ZCARD crt:heat:top 2>/dev/null || true

echo "================================================================================"
echo "4. 最近一次生成器运行清单"
manifest_pattern="${CRT_GENERATOR_RUN_NAME:-*}*.json"
echo "pattern=$manifest_pattern"
find RealTime/data_files/run_manifest -maxdepth 1 -type f -name '*.json' \
  -printf '%T@ %f\n' 2>/dev/null | sort -nr | head -5 || true
latest_manifest="$(find RealTime/data_files/run_manifest -maxdepth 1 -type f -name "$manifest_pattern" \
  -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)"
if [[ -n "$latest_manifest" ]]; then
  echo "$latest_manifest"
  cat "$latest_manifest"
else
  echo "no generator run manifest"
fi

echo "================================================================================"
echo "5. Delta 全链路观测"
infra/scripts/run_shell.sh /opt/crt/infra/scripts/observe_warehouse.scala
