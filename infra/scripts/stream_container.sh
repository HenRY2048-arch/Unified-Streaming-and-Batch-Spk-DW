#!/usr/bin/env bash
# 在 crt-stream 容器内启动 StreamApp；CRT_STREAM_RESET=true 时先清本作业 checkpoint。
set -euo pipefail

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

app_args=()
if [[ "${CRT_STREAM_RESET:-false}" == "true" ]]; then
  app_args+=("--reset")
fi

exec /opt/spark/bin/spark-submit \
  --master 'local[2]' --driver-memory 2g \
  --driver-java-options "-Dkafka.topic.events=${CRT_TOPIC_EVENTS:-content_events} -Dkafka.topic.truth=${CRT_TOPIC_TRUTH:-content_groundtruth}" \
  --jars "$jar_list" \
  --class com.hub.app.StreamApp \
  /opt/crt/warehouse/crt-warehouse-0.1.0.jar "${app_args[@]}"
