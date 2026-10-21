package com.hub.stream

import com.hub.common.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Kafka 读取：只负责"把原始消息拉进来"，解析交给 [[OdsWriter]]（契约解析在 `common.Contract`）。
 *
 * 保留 `partition/offset/timestamp` 三列：ODS 里落一份，出问题时能定位到"哪条消息、何时进的"。
 * `failOnDataLoss=false`：本地会反复重建 topic，读不到旧 offset 不应让作业挂掉。
 */
object KafkaSource {

  def read(spark: SparkSession, topic: String): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Config.kafkaBootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", Config.startingOffsets)
      .option("maxOffsetsPerTrigger", Config.maxOffsetsPerTrigger)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr(
        "CAST(value AS STRING) AS value",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
        "timestamp AS kafka_ts"
      )
}
