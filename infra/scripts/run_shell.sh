#!/usr/bin/env bash
# 在 crt-spark 容器里用 spark-shell 执行一个 .scala 脚本（用于验收/排查）。
# 用法：infra/scripts/run_shell.sh /opt/crt/infra/scripts/verify_p3_batch2.scala
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
jars_dir=/opt/crt/infra/jars
jar_list="/opt/crt/warehouse/crt-warehouse-0.1.0.jar,$(printf "%s," \
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

script="${1:?用法: run_shell.sh <容器内脚本路径>}"

exec docker compose -f "$here/../docker-compose.yml" --profile tools run --rm \
  --entrypoint /opt/spark/bin/spark-shell spark \
  --master 'local[2]' --driver-memory 2g \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://seaweedfs:8333 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.access.key=crtadmin \
  --conf spark.hadoop.fs.s3a.secret.key=crtadmin123 \
  --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
  --jars "$jar_list" \
  -i "$script"
