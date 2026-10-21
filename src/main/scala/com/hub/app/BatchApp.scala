package com.hub.app

import com.hub.batch.{DimBuilder, DwdBuilder, DwsBuilder, SemanticDict}
import com.hub.common._
import org.apache.spark.sql.functions._

/**
 * 入口②：批作业。
 *
 * 用法：
 *   --step dim            只重建维表
 *   --step dwd            只重建明细（默认最近 2 天）
 *   --step all            维表 + 明细
 *   --days  N             重算窗口（覆盖 batch.recompute.days）
 *
 * 窗口的起点由 **ODS 的最大 dt** 推出来（而不是机器当前日期）：这样重放历史数据时，
 * "最近 N 天"跟着数据走，不会因为今天没跑就空转。
 */
object BatchApp {

  def main(args: Array[String]): Unit = {
    var step = "all"
    var days = Config.recomputeDays
    var i = 0
    while (i < args.length - 1) {
      args(i) match {
        case "--step" => step = args(i + 1); i += 2
        case "--days" => days = args(i + 1).toInt; i += 2
        case _        => i += 1
      }
    }

    val spark = Spark.session("crt-batch")
    println(Config.summary)

    val ods = DeltaIO.read(spark, Lake.odsEvents)
    val maxDt = ods.agg(max("dt")).collect().head.getString(0)
    // 统一用 date_format 转成 yyyy-MM-dd 字符串（Date 类型直接 getString 会 ClassCastException）
    val sinceDt = ods
      .select(date_format(date_sub(to_date(lit(maxDt)), days - 1), "yyyy-MM-dd").as("d"))
      .collect().head.getString(0)
    println(s"[BatchApp] step=$step days=$days maxDt=$maxDt since=$sinceDt")

    if (step == "all" || step == "dim") DimBuilder.buildAll(spark)
    if (step == "all" || step == "dwd") DwdBuilder.buildAll(spark, sinceDt)
    if (step == "all" || step == "dws") DwsBuilder.buildAll(spark, sinceDt)
    if (step == "all" || step == "semantic") SemanticDict.build(spark)

    println("[BatchApp] done")
    spark.stop()
  }
}
