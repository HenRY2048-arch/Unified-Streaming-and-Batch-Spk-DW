package com.hub.app

import com.hub.common._

/**
 * 维护入口：按保留期回收 Delta 历史文件。
 *
 * 这是**会改物理文件**的动作，默认只由周维护/人工触发；不要放进每日主链。
 * `--hours 168` 表示保留 7 天；小于默认保留期时需显式确认（由调用方负责）。
 */
object VacuumApp {

  def main(args: Array[String]): Unit = {
    var hours = Config.getDouble("vacuum.retention.hours", 168.0)
    var i = 0
    while (i < args.length - 1) {
      args(i) match {
        case "--hours" => hours = args(i + 1).toDouble; i += 2
        case _ => i += 1
      }
    }

    val spark = Spark.session("crt-vacuum")
    println(Config.summary)
    println(s"[VacuumApp] retentionHours=$hours")

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

    paths.foreach { case (name, path) =>
      if (DeltaIO.exists(spark, path)) {
        val removed = DeltaIO.vacuum(spark, path, hours)
        println(s"[Vacuum] $name removed=$removed")
      } else {
        println(s"[Vacuum] $name skipped (missing)")
      }
    }
    spark.stop()
  }
}
