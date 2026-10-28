// 维护入口所在包。
package com.hub.app

// Config、Spark、Lake、DeltaIO 等公共能力。
import com.hub.common._

/**
 * 维护入口：按保留期回收 Delta 历史文件。
 *
 * 为什么单独成一个入口：
 *   - VACUUM 会删除物理文件，不能放进每日 Gate 主链；
 *   - 默认保留 7 天（168h），由周维护/人工触发；
 *   - 先确认 verify/ 表和 ops/partition_status 没有 blocked，再执行。
 */
object VacuumApp {

  def main(args: Array[String]): Unit = {
    // 默认保留 168 小时；可用 --hours 覆盖。
    var hours = Config.getDouble("vacuum.retention.hours", 168.0)
    // 参数扫描游标。
    var i = 0
    // 只识别 --hours；其他参数跳过。
    while (i < args.length - 1) {
      args(i) match {
        // --hours N：保留 N 小时。
        case "--hours" => hours = args(i + 1).toDouble; i += 2
        // 未识别参数跳过。
        case _ => i += 1
      }
    }

    // 创建维护用 SparkSession。
    val spark = Spark.session("crt-vacuum")
    // 打印配置和保留期，先让人确认这次会删什么范围。
    println(Config.summary)
    println(s"[VacuumApp] retentionHours=$hours")

    // 需要维护的表清单：名称 -> 实际 S3 路径。
    // 只列 Delta 表；Redis 不参与 VACUUM。
    val paths = Seq(
      "ods/events" -> Lake.odsEvents,
      "ods/groundtruth" -> Lake.odsTruth,
      "tier0/heat_snapshot" -> Lake.tier0Heat,
      "tier0/cheat_events" -> Lake.tier0Cheat,
      "dwd/exposure" -> Lake.dwdExposure,
      "dwd/click" -> Lake.dwdClick,
      "dwd/consume" -> Lake.dwdConsume,
      "dwd/interaction" -> Lake.dwdInteraction,
      "dwd/relation" -> Lake.dwdRelation,
      "dwd/publish" -> Lake.dwdPublish,
      "dwd/session" -> Lake.dwdSession,
      "dws/funnel_hourly" -> Lake.dwsFunnelHourly,
      "dws/channel_hourly" -> Lake.dwsChannelHourly,
      "dws/content_daily" -> Lake.dwsContentDaily,
      "dws/author_daily" -> Lake.dwsAuthorDaily,
      "dws/relation_daily" -> Lake.dwsRelationDaily,
      "dws/session_daily" -> Lake.dwsSessionDaily,
      "dws/user_daily" -> Lake.dwsUserDaily,
      "dws/quality_daily" -> Lake.dwsQualityDaily,
      "dws/position_daily" -> Lake.dwsPositionDaily,
      "dws/retention_daily" -> Lake.dwsRetentionDaily,
      "dws/cold_start_daily" -> Lake.dwsColdStartDaily,
      "verify/attribution" -> Lake.verifyAttribution,
      "verify/heat" -> Lake.verifyHeat
    )

    // 逐表检查：存在才 VACUUM，缺失只打印跳过。
    paths.foreach { case (name, path) =>
      if (DeltaIO.exists(spark, path)) {
        // DeltaIO.vacuum 内部会临时关闭保留期检查，并返回回收文件数。
        val removed = DeltaIO.vacuum(spark, path, hours)
        println(s"[Vacuum] $name removed=$removed")
      } else {
        println(s"[Vacuum] $name skipped (missing)")
      }
    }
    // 释放 SparkSession。
    spark.stop()
  }
}
