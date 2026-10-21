package com.hub.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * 事件契约（v2）与种子格式的**唯一定义处**。
 *
 * 契约正本：`project_specs/mock_generator/Mock_Source_Spec.md` §4。这里只做"文档 → 代码"的翻译，
 * 不改形状：`items` 在 ODS **原样保留**，展开是 DWD 的事。
 *
 * 约定：字段缺失＝不适用（生成器用 `@JsonInclude(NON_NULL)` 不输出空字段）。
 */
object Contract {

  val exposureItems: ArrayType = ArrayType(
    StructType(Seq(
      StructField("item_id", LongType),
      StructField("position", IntegerType),
      StructField("rank_score", DoubleType)
    ))
  )

  /** 超集事件 schema（一条 JSON = 一条事件）。 */
  val eventSchema: StructType = StructType(Seq(
    StructField("event_id", StringType),
    StructField("user_id", LongType),
    StructField("event_type", StringType),
    StructField("ts", StringType),
    StructField("session_id", StringType),          // `user_registered` 不适用 → 缺失
    StructField("platform", StringType),
    StructField("schema_version", StringType),
    StructField("item_id", LongType),
    StructField("target_user_id", LongType),
    StructField("duration_sec", DoubleType),
    StructField("comment_text", StringType),
    StructField("channel", StringType),
    StructField("batch_id", StringType),
    StructField("position", IntegerType),
    StructField("source_item_id", LongType),
    StructField("source_user_id", LongType),
    StructField("author_id", LongType),
    StructField("category", StringType),
    StructField("tags", StringType),
    StructField("content_len", IntegerType),
    StructField("tag", StringType),
    StructField("published_ts", StringType),
    StructField("items", exposureItems),
    // v2：`user_registered` 携带的用户属性
    StructField("interest_tags", ArrayType(StringType)),
    StructField("level", IntegerType),
    StructField("is_creator", BooleanType),
    StructField("primary_category", StringType),
    StructField("post_rate_level", StringType)
  ))

  /** 归因真值（独立 topic，不参与事件流计算）。 */
  val truthSchema: StructType = StructType(Seq(
    StructField("truth_id", StringType),
    StructField("event_id", StringType),
    StructField("user_id", LongType),
    StructField("conversion_type", StringType),
    StructField("item_id", LongType),
    StructField("target_user_id", LongType),
    StructField("channel", StringType),
    StructField("batch_id", StringType),
    StructField("position", IntegerType),
    StructField("session_id", StringType),
    StructField("ts", StringType)
  ))

  // ---- 种子（`|` 分隔、无表头）----
  val seedUserSchema: StructType = StructType(Seq(
    StructField("user_id", LongType),
    StructField("interest_tags", StringType),       // 逗号分隔，可空＝冷启动
    StructField("level", IntegerType),
    StructField("is_creator", BooleanType),
    StructField("primary_category", StringType),
    StructField("post_rate_level", StringType)
  ))

  val seedItemSchema: StructType = StructType(Seq(
    StructField("item_id", LongType),
    StructField("author_id", LongType),
    StructField("category", StringType),
    StructField("tags", StringType),
    StructField("content_len", IntegerType),
    StructField("published_ts", StringType)
  ))

  val seedEdgeSchema: StructType = StructType(Seq(
    StructField("follower", LongType),
    StructField("followee", LongType),
    StructField("ts", StringType)
  ))

  /** Kafka value（JSON 字符串）→ 结构化事件；`ts` 同时解析成 timestamp 并派生 `dt/hour`。 */
  def parseEvents(raw: DataFrame): DataFrame = {
    val meta = Seq("kafka_partition", "kafka_offset", "kafka_ts").filter(raw.columns.contains).map(col)
    val parsed = raw
      .select(from_json(col("value").cast("string"), eventSchema).as("e") +: meta: _*)
      .select(col("e.*") +: meta: _*)
    withEventTime(parsed)
  }

  def parseTruth(raw: DataFrame): DataFrame = {
    val meta = Seq("kafka_partition", "kafka_offset", "kafka_ts").filter(raw.columns.contains).map(col)
    val parsed = raw
      .select(from_json(col("value").cast("string"), truthSchema).as("t") +: meta: _*)
      .select(col("t.*") +: meta: _*)
    withEventTime(parsed)
  }

  /**
   * 派生时间列：`ts` 是 ISO-8601（GMT+8），Spark 按 `spark.sql.session.timeZone` 解析。
   * `dt` / `hour` 是**分区键**，必须与报表口径一致（本项目统一 GMT+8）。
   */
  def withEventTime(df: DataFrame): DataFrame = {
    val ts = col("ts").cast("timestamp")
    df.withColumn("event_ts", ts)
      .withColumn("dt", date_format(ts, "yyyy-MM-dd"))
      .withColumn("hour", date_format(ts, "HH"))
  }

  /** 读种子 CSV（`|` 分隔、无表头）。 */
  def readSeed(spark: org.apache.spark.sql.SparkSession, path: String, schema: StructType): DataFrame =
    spark.read.option("delimiter", "|").option("header", "false").schema(schema).csv(path)
}
