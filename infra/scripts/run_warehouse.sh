#!/usr/bin/env bash
# 在 crt-spark 容器里跑数仓入口（统一 jar 列表、驱动参数与 topic 覆盖）。
#
# 用法：
#   infra/scripts/run_warehouse.sh com.hub.app.BatchApp --step all
#   CRT_TOPIC_EVENTS=content_events_p3b infra/scripts/run_warehouse.sh com.hub.app.StreamApp --reset
#
# 说明：jar 全部来自 infra/jars（宿主 mvn 预下载、容器内只读挂载），
#       数仓自身是薄 jar（/opt/crt/warehouse/crt-warehouse-0.1.0.jar）。
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
jars_dir=/opt/crt/infra/jars
jar_list="$(printf "%s," \
  "$jars_dir/delta-spark_2.12-3.2.1.jar" \
  "$jars_dir/delta-storage-3.2.1.jar" \
  "$jars_dir/hadoop-aws-3.3.4.jar" \
  "$jars_dir/aws-java-sdk-bundle-1.12.262.jar" \
  "$jars_dir/antlr4-runtime-4.9.3.jar" \
  "$jars_dir/spark-sql-kafka-0-10_2.12-3.5.3.jar" \
  "$jars_dir/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" \
  "$jars_dir/kafka-clients-3.4.1.jar" \
  "$jars_dir/commons-pool2-2.12.0.jar" \
  "$jars_dir/jedis-5.1.2.jar" \
  "$jars_dir/json-20231013.jar" | sed 's/,$//')"

class="${1:?用法: run_warehouse.sh <main-class> [args...]}"
shift

exec docker compose -f "$here/../docker-compose.yml" --profile tools run --rm \
  --entrypoint /opt/spark/bin/spark-submit spark \
  --master 'local[2]' --driver-memory 2g \
  --driver-java-options "-Dkafka.topic.events=${CRT_TOPIC_EVENTS:-content_events} -Dkafka.topic.truth=${CRT_TOPIC_TRUTH:-content_groundtruth}" \
  --jars "$jar_list" \
  --class "$class" \
  /opt/crt/warehouse/crt-warehouse-0.1.0.jar "$@"
