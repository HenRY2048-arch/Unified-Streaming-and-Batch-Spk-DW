package com.hub.common

import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Delta 读写封装的**唯一入口**。
 *
 * 两个纪律写在这里，而不是靠每个作业自己记得：
 *  1) 批侧"按窗口重算"一律 `replaceWhere`（或 MERGE），不允许 append 出一堆重复行；
 *  2) `replaceWhere`/`DELETE` **只改日志**，物理文件靠 `VACUUM` 回收 —— 重算后要么显式调用 `vacuum`，
 *     要么由调度器统一收口（见 Warehouse_Design §5.1）。
 */
object DeltaIO {

  def exists(spark: SparkSession, path: String): Boolean =
    try DeltaTable.isDeltaTable(spark, path) catch { case _: Throwable => false }

  def read(spark: SparkSession, path: String): DataFrame =
    spark.read.format("delta").load(path)

  def readIfExists(spark: SparkSession, path: String): Option[DataFrame] =
    if (exists(spark, path)) Some(read(spark, path)) else None

  /** 全量覆盖（建表或重建）。 */
  def write(df: DataFrame, path: String, partitionBy: Seq[String] = Nil, mode: String = "overwrite"): Unit = {
    // mergeSchema：允许"加列"式演进（开发期新增派生列不必删表）；不做类型转换，避免悄悄改语义
    val w = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    val withParts = if (partitionBy.nonEmpty) w.partitionBy(partitionBy: _*) else w
    withParts.save(path)
  }

  /** 按谓词重算一个（或一组）分区：批为真值的落地形式。 */
  def overwriteWhere(df: DataFrame, path: String, predicate: String): Unit =
    df.write.format("delta").mode("overwrite")
      .option("replaceWhere", predicate)
      .option("mergeSchema", "true")
      .save(path)

  /** MERGE：适合"按主键增量合并"（如 dim_user 的 first_seen 只前进不回退）。 */
  def merge(spark: SparkSession, path: String, source: DataFrame, condition: String): DeltaMergeBuilder =
    DeltaTable.forPath(spark, path).as("t").merge(source.as("s"), condition)

  /** 回收历史文件（`hours=0` 表示立即回收；调用方需自己保证当前版本已结算）。 */
  def vacuum(spark: SparkSession, path: String, hours: Double = 0.0): Long = {
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    DeltaTable.forPath(spark, path).vacuum(hours).count()
  }

  /** 表的基本信息（行数 + 文件数 + 版本数），用于每批验收。 */
  def describe(spark: SparkSession, path: String): (Long, Long, Long) = {
    val rows = spark.read.format("delta").load(path).count()
    val detail = spark.sql(s"DESCRIBE DETAIL delta.`$path`")
    val nf = detail.select("numFiles").collect().head.getLong(0)
    val versions = spark.sql(s"DESCRIBE HISTORY delta.`$path`").count()
    (rows, nf, versions)
  }
}
