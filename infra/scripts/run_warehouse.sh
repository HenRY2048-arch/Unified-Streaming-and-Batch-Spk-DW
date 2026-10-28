#!/usr/bin/env bash
# =============================================================================
# Airflow / 手工命令的统一 Spark 启动壳。
#
# 这层只做三件事：
#   1) 固定 Spark 运行期依赖（infra/jars）；
#   2) 把第一个参数当作 Java/Scala main class；
#   3) 把剩余参数原样传给这个 main class。
#
# 它不实现任何 Gate 规则；Gate 规则在 com.hub.app.GateApp ->
# com.hub.verify.GateRunner 里。
# =============================================================================

# 用法：
#   infra/scripts/run_warehouse.sh com.hub.app.BatchApp --step all
#   infra/scripts/run_warehouse.sh com.hub.app.GateApp --stage all --days 2
#   CRT_TOPIC_EVENTS=content_events_p3b infra/scripts/run_warehouse.sh com.hub.app.StreamApp --reset
#
# 参数约定：
#   $1 = main class（必填）
#   $2... = 传给 main class 的原始参数

# -e：任一命令失败立即退出。
# -u：引用未定义变量时报错。
# -o pipefail：管道中任一命令失败，整个管道失败。
set -euo pipefail

# 取脚本所在目录的绝对路径；后面用它定位 docker-compose.yml。
here="$(cd "$(dirname "$0")" && pwd)"

# 容器内预下载依赖的挂载目录（宿主 infra/jars -> 容器 /opt/crt/infra/jars）。
jars_dir=/opt/crt/infra/jars

# 把 Spark 运行期需要的 jar 拼成 spark-submit --jars 接受的逗号分隔列表。
# 这些 jar 由宿主 Maven 预下载；容器内只读挂载，避免容器联网解析。
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

# 第一个位置参数是入口类；缺失时 shell 会打印用法并退出。
class="${1:?用法: run_warehouse.sh <main-class> [args...]}"

# 移除第一个参数，剩下的 "$@" 原样交给入口类。
shift

# 用一次性 Spark 容器执行：
#   --profile tools       启用 compose 里的 spark 服务；
#   run --rm              运行完自动删容器；
#   spark-submit          真正的 Spark 提交器；
#   --master local[2]     单机 2 线程，够本地小数据；
#   --driver-memory 2g    控制 WSL2 内存；
#   --driver-java-options 让 Config 读到 topic 覆盖；
#   --jars                运行期依赖；
#   --class               GateApp / BatchApp 等入口类；
#   挂载的薄 jar          数仓代码本身。
exec docker compose -f "$here/../docker-compose.yml" --profile tools run --rm \
  --entrypoint /opt/spark/bin/spark-submit spark \
  --master 'local[2]' --driver-memory 2g \
  --driver-java-options "-Dkafka.topic.events=${CRT_TOPIC_EVENTS:-content_events} -Dkafka.topic.truth=${CRT_TOPIC_TRUTH:-content_groundtruth}" \
  --jars "$jar_list" \
  --class "$class" \
  /opt/crt/warehouse/crt-warehouse-0.1.0.jar "$@"
