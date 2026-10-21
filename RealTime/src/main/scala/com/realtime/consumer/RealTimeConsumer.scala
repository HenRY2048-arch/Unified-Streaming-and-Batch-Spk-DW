package com.realtime.consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RealTimeConsumer {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession (本地运行模式)
    val spark = SparkSession.builder()
      .appName("RealTimeConsumer")
      .master("local[*]") // 本地多线程运行
      .getOrCreate()

    import spark.implicits._

    // 2. 定义 Schema (严格对齐 RealTime_Mock_Generator_Design_Doc.md)
    val schema = StructType(Seq(
      StructField("event_id", StringType),
      StructField("user_id", LongType),
      StructField("event_type", StringType),
      StructField("ts", StringType),
      StructField("session_id", StringType),
      StructField("platform", StringType),
      StructField("item_id", LongType),
      StructField("target_user_id", LongType),
      StructField("duration_sec", DoubleType),
      StructField("comment_text", StringType),
      StructField("content_len", IntegerType)
    ))

    // 3. 连接 Kafka 读取流数据
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.10.102:9092,192.168.10.103:9092,192.168.10.104:9092")
      .option("subscribe", "content_events")
      .option("startingOffsets", "latest") // 仅消费启动后的新数据
      .load()

    // 4. 将 Kafka 的 binary value 转换为 JSON 并解析
    val parsedStream = kafkaStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json($"json_str", schema).as("data"))
      .select("data.*")

    // 5. 打印到控制台 (用完即弃，不保存到任何持久化存储)
    val query = parsedStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false") // 不截断长字符串
      .start()

    // 阻塞直到手动停止
    query.awaitTermination()
  }
}
