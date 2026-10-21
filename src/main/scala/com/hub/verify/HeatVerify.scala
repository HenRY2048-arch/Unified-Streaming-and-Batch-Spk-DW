package com.hub.verify

import com.hub.common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * V-02 热度对账：**流侧快照** vs **批侧重算**。
 *
 * 对账口径（缺一不可）：
 *  1) 只读 `tier0/heat_snapshot`（**不读 Redis**：缓存会 TTL、且"读到的不是同一时刻"）；
 *  2) 同一组 `(dt,hour,item,window)` 取 `computed_at` **最新**的一条作为流侧值；
 *  3) 用**同一个 `computed_at` 作为事件时间锚点**，调 `HeatMath` 从 ODS 重算批侧值。
 *  这样比较的才是"流近似 vs 同一时刻的真值"，而不是两套代码的差异。
 */
object HeatVerify {

  def run(spark: SparkSession, sinceDt: String): DataFrame = {
    val snap = DeltaIO.read(spark, Lake.tier0Heat).filter(col("dt") >= sinceDt)
    if (snap.limit(1).count() == 0) {
      return spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        org.apache.spark.sql.types.StructType(Seq(
          org.apache.spark.sql.types.StructField("dt", org.apache.spark.sql.types.StringType),
          org.apache.spark.sql.types.StructField("hour", org.apache.spark.sql.types.StringType),
          org.apache.spark.sql.types.StructField("item_id", org.apache.spark.sql.types.LongType),
          org.apache.spark.sql.types.StructField("window_minutes", org.apache.spark.sql.types.IntegerType),
          org.apache.spark.sql.types.StructField("heat_stream", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("heat_batch", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("abs_diff", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("rel_diff", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("computed_at", org.apache.spark.sql.types.TimestampType))))
    }

    val latest = snap.groupBy("dt", "hour", "item_id", "window_minutes")
      .agg(max("computed_at").as("computed_at"))
    val streamSide = snap
      .join(latest, Seq("dt", "hour", "item_id", "window_minutes", "computed_at"))
      .select(col("dt"), col("hour"), col("item_id"), col("window_minutes"),
        col("heat").as("heat_stream"), col("computed_at"))

    // 按"每个锚点 × 每个窗口"重算批侧值（锚点数量 = 微批数量，很少）
    val anchors = streamSide.select("computed_at").distinct().collect()
      .map(_.getTimestamp(0)).sortBy(_.getTime)
    val ods = DeltaIO.read(spark, Lake.odsEvents)

    val batchSide = anchors.flatMap { anchor =>
      val from = new java.sql.Timestamp(anchor.getTime - 24L * 3600 * 1000)
      val recent = ods.filter(col("event_ts") > lit(from) && col("event_ts") <= lit(anchor))
      Config.heatWindowsMinutes.map { w =>
        HeatMath.heatFor(recent, anchor, w)
          .select(col("item_id"), col("window_minutes"), col("heat").as("heat_batch"),
            lit(anchor).as("computed_at"))
      }
    }

    if (batchSide.isEmpty) {
      return streamSide.withColumn("heat_batch", lit(null).cast("double"))
        .withColumn("abs_diff", lit(null).cast("double"))
        .withColumn("rel_diff", lit(null).cast("double"))
    }

    streamSide.join(batchSide.reduce(_ unionByName _), Seq("item_id", "window_minutes", "computed_at"), "inner")
      .withColumn("abs_diff", abs(col("heat_stream") - col("heat_batch")))
      .withColumn("rel_diff",
        when(col("heat_batch") > 0, col("abs_diff") / col("heat_batch")).otherwise(lit(null)))
      .select("dt", "hour", "item_id", "window_minutes",
        "heat_stream", "heat_batch", "abs_diff", "rel_diff", "computed_at")
  }

  def write(spark: SparkSession, sinceDt: String): Unit = {
    val out = run(spark, sinceDt)
    if (DeltaIO.exists(spark, Lake.verifyHeat)) {
      DeltaIO.overwriteWhere(out, Lake.verifyHeat, s"dt >= '$sinceDt'")
    } else {
      DeltaIO.write(out, Lake.verifyHeat, Seq("dt", "hour"), "overwrite")
    }
    println(s"[V-02] rows=${out.count()}")
  }
}
