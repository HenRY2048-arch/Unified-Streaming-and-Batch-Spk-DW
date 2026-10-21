package com.hub.app

import com.hub.common._
import com.hub.verify.{AttributionVerify, HeatVerify}
import org.apache.spark.sql.functions._

/**
 * 入口③：对账（V-01 归因 / V-02 热度）。
 *
 * 用法：`VerifyApp --days 2`（窗口起点由 ODS 最大 dt 推出，与 BatchApp 一致）
 */
object VerifyApp {

  def main(args: Array[String]): Unit = {
    var days = Config.recomputeDays
    var i = 0
    while (i < args.length - 1) {
      args(i) match {
        case "--days" => days = args(i + 1).toInt; i += 2
        case _        => i += 1
      }
    }

    val spark = Spark.session("crt-verify")
    println(Config.summary)

    val ods = DeltaIO.read(spark, Lake.odsEvents)
    val maxDt = ods.agg(max("dt")).collect().head.getString(0)
    val sinceDt = ods.select(date_format(date_sub(to_date(lit(maxDt)), days - 1), "yyyy-MM-dd").as("d"))
      .collect().head.getString(0)
    println(s"[VerifyApp] days=$days maxDt=$maxDt since=$sinceDt")

    AttributionVerify.write(spark, sinceDt)
    HeatVerify.write(spark, sinceDt)

    println("=" * 80)
    println("V-01 明细：")
    DeltaIO.read(spark, Lake.verifyAttribution).orderBy("dt", "conversion_type").collect()
      .foreach { r =>
        val rate = Option(r.getAs[Double]("match_rate")).map(v => f"$v%.3f").getOrElse("-")
        println(f"  ${r.getString(0)} ${r.getString(1)}%-8s total=${r.getLong(2)}%4d " +
          f"matched=${r.getLong(3)}%4d rate=$rate")
      }

    println("V-02 分布：")
    val heat = DeltaIO.read(spark, Lake.verifyHeat)
    if (heat.limit(1).count() > 0) {
      val q = heat.filter(col("rel_diff").isNotNull).stat.approxQuantile("rel_diff", Array(0.5, 0.95, 1.0), 0.001)
      println(f"  rel_diff 中位=${q(0)}%.6f / p95=${q(1)}%.6f / 最大=${q(2)}%.6f（行数=${heat.count()}）")
    } else {
      println("  （无对账行：Tier0 尚未产出快照）")
    }
    println("=" * 80)
    spark.stop()
  }
}
