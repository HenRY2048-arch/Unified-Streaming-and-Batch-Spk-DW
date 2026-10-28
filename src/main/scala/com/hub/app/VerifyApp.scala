// 对账入口所在包；它属于 app 入口层，不是 Gate 规则本身。
package com.hub.app

// 统一配置、Delta IO、Lake 路径和 SparkSession 工厂。
import com.hub.common._
// 两个对账生产者：V-01 归因、V-02 热度。
import com.hub.verify.{AttributionVerify, HeatVerify}
// DataFrame 聚合、日期函数和列表达式。
import org.apache.spark.sql.functions._

/**
 * 入口③：对账（V-01 归因 / V-02 热度）。
 *
 * 调用链：
 *   Airflow verify task
 *     -> VerifyApp
 *       -> AttributionVerify.write()
 *       -> HeatVerify.write()
 *       -> 打印 verify/attribution 和 verify/heat 的摘要
 *
 * 这个入口不跑 Gate；Gate 只读它产出的 verify/ 表。
 */
object VerifyApp {

  def main(args: Array[String]): Unit = {
    // 默认重算窗口来自 warehouse.properties 的 batch.recompute.days。
    var days = Config.recomputeDays
    // 手写参数扫描游标。
    var i = 0
    // 扫描参数：当前只支持 --days。
    while (i < args.length - 1) {
      args(i) match {
        // --days N：重算最近 N 个事件天。
        case "--days" => days = args(i + 1).toInt; i += 2
        // 其它参数跳过，保持兼容性。
        case _        => i += 1
      }
    }

    // 创建对账用 SparkSession（应用名 crt-verify）。
    val spark = Spark.session("crt-verify")
    // 打印配置摘要，确认 S3/Kafka/Redis 配置和重算天数。
    println(Config.summary)

    // 读取 ODS 事件表，用于推最大事件日期。
    val ods = DeltaIO.read(spark, Lake.odsEvents)
    // 取 ODS 最大 dt；对账窗口以数据日期为准，不用机器当天。
    val maxDt = ods.agg(max("dt")).collect().head.getString(0)
    // sinceDt = maxDt - (days - 1)，得到 N 天的起始日期。
    val sinceDt = ods.select(date_format(date_sub(to_date(lit(maxDt)), days - 1), "yyyy-MM-dd").as("d"))
      .collect().head.getString(0)
    // 打印本次窗口，排查时先看这行。
    println(s"[VerifyApp] days=$days maxDt=$maxDt since=$sinceDt")

    // 先写 V-01：truth 左连接 DWD，生成 verify/attribution。
    AttributionVerify.write(spark, sinceDt)
    // 再写 V-02：流侧快照 vs 批侧重算，生成 verify/heat。
    HeatVerify.write(spark, sinceDt)

    // 打印分隔线，便于看控制台。
    println("=" * 80)
    println("V-01 明细：")
    // 读取 V-01 表，按日期和转化类型排序后逐行打印。
    DeltaIO.read(spark, Lake.verifyAttribution).orderBy("dt", "conversion_type").collect()
      .foreach { r =>
        // match_rate 可能为 null（没有 joined 行）；这里格式化为 "-"。
        val rate = Option(r.getAs[Double]("match_rate")).map(v => f"$v%.3f").getOrElse("-")
        // 打印日期、类型、total、matched、rate；列序号来自 AttributionVerify 的输出 schema。
        println(f"  ${r.getString(0)} ${r.getString(1)}%-8s total=${r.getLong(2)}%4d " +
          f"matched=${r.getLong(3)}%4d rate=$rate")
      }

    // 打印 V-02 摘要。
    println("V-02 分布：")
    // 读取热度对账表。
    val heat = DeltaIO.read(spark, Lake.verifyHeat)
    // 只有在存在数据时才做分位数；否则打印“无快照”。
    if (heat.limit(1).count() > 0) {
      // 忽略 rel_diff 为 null 的行，计算 p50/p95/max。
      val q = heat.filter(col("rel_diff").isNotNull).stat.approxQuantile("rel_diff", Array(0.5, 0.95, 1.0), 0.001)
      // 打印中位、p95、最大和总行数。
      println(f"  rel_diff 中位=${q(0)}%.6f / p95=${q(1)}%.6f / 最大=${q(2)}%.6f（行数=${heat.count()}）")
    } else {
      // 10-18 这类没有 Tier0 快照的日期会走这里。
      println("  （无对账行：Tier0 尚未产出快照）")
    }
    // 结束分隔线。
    println("=" * 80)
    // 释放 SparkSession。
    spark.stop()
  }
}
