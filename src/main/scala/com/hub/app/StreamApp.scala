package com.hub.app

import com.hub.common.{Config, Lake, Spark}
import com.hub.stream.{KafkaSource, OdsWriter, Tier0}
import org.apache.spark.sql.SparkSession

/**
 * 入口①：流式作业（一个 Spark 应用跑三条查询）。
 *
 *   Kafka(events) → ODS/events
 *   Kafka(truth)  → ODS/groundtruth
 *   ODS/events    → Tier0（热度/作弊/在线）
 */
object StreamApp {

  def main(args: Array[String]): Unit = {
    val spark = Spark.session("crt-stream-ods")
    println(Config.summary)

    // 本地开发常用：`--reset` 只清本作业自己的 checkpoint（解决"上次被 kill、checkpoint 被占用"）
    if (args.contains("--reset")) resetCheckpoints(spark)

    val events = KafkaSource.read(spark, Config.topicEvents)
    val truth = KafkaSource.read(spark, Config.topicTruth)

    val q1 = OdsWriter.writeEvents(events)
    val q2 = OdsWriter.writeTruth(truth)
    val q3 = Tier0.start(spark)

    println(s"[StreamApp] started: ${q1.name} / ${q2.name} / ${q3.name}（Ctrl-C 停止）")
    spark.streams.awaitAnyTermination()
  }

  private def resetCheckpoints(spark: SparkSession): Unit = {
    val hc = spark.sparkContext.hadoopConfiguration
    Seq(
      Lake.checkpoint("stream/ods_events"),
      Lake.checkpoint("stream/ods_truth"),
      Lake.checkpoint("stream/tier0")
    ).foreach { p =>
      val path = new org.apache.hadoop.fs.Path(p)
      val fs = path.getFileSystem(hc)
      if (fs.exists(path)) {
        fs.delete(path, true)
        println(s"[StreamApp] checkpoint 已重置：$p")
      }
    }
    System.out.flush()
  }
}
