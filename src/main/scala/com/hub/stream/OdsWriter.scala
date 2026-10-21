package com.hub.stream

import com.hub.common.{Contract, DeltaIO, Lake}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * 流①：Kafka → ODS（**证据层唯一入口**）。
 *
 * 三条纪律：
 *  1) ODS **不改形状**：`items` 原样保留（展开是 DWD 的事）；
 *  2) 分区按 `dt`（事件时间，GMT+8）；
 *  3) 不做业务去重——ODS 是"原始证据"，去重/纠偏留给批侧（批为真值）。
 */
object OdsWriter {

  private def odsMeta(df: DataFrame): DataFrame =
    df.withColumn("_kafka_partition", col("kafka_partition"))
      .withColumn("_kafka_offset", col("kafka_offset"))
      .withColumn("_ingest_ts", current_timestamp())

  def writeEvents(raw: DataFrame): StreamingQuery =
    raw.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val parsed = odsMeta(Contract.parseEvents(batch))
        DeltaIO.write(parsed, Lake.odsEvents, Seq("dt"), "append")
        println(s"[ODS] events batch=$batchId rows=${parsed.count()}")
        System.out.flush()
      }
      .option("checkpointLocation", Lake.checkpoint("stream/ods_events"))
      .queryName("ods_events")
      .start()

  def writeTruth(raw: DataFrame): StreamingQuery =
    raw.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val parsed = odsMeta(Contract.parseTruth(batch))
        DeltaIO.write(parsed, Lake.odsTruth, Seq("dt"), "append")
        println(s"[ODS] truth batch=$batchId rows=${parsed.count()}")
        System.out.flush()
      }
      .option("checkpointLocation", Lake.checkpoint("stream/ods_truth"))
      .queryName("ods_truth")
      .start()
}
