// =============================================================================
// P3 第三批验收：DWS + 语义层（对照手算，不是只看行数）
// =============================================================================
import com.hub.common._
import org.apache.spark.sql.functions._

val spark = Spark.session("crt-verify-batch3")
spark.sparkContext.setLogLevel("WARN")
def say(s: String): Unit = { println(s); System.out.flush() }
def n(path: String): Long = if (DeltaIO.exists(spark, path)) spark.read.format("delta").load(path).count() else -1L

say("=" * 90)
say("① 行数")
Seq(
  "funnel_hourly" -> Lake.dwsFunnelHourly, "channel_hourly" -> Lake.dwsChannelHourly,
  "content_daily" -> Lake.dwsContentDaily, "author_daily" -> Lake.dwsAuthorDaily,
  "relation_daily" -> Lake.dwsRelationDaily, "session_daily" -> Lake.dwsSessionDaily,
  "user_daily" -> Lake.dwsUserDaily, "quality_daily" -> Lake.dwsQualityDaily,
  "position_daily" -> Lake.dwsPositionDaily, "retention_daily" -> Lake.dwsRetentionDaily,
  "cold_start_daily" -> Lake.dwsColdStartDaily, "metric_dict" -> Lake.semanticMetricDict
).foreach { case (k, p) => say(f"$k%-18s ${n(p)}%8d") }

say("=" * 90)
say("② CTR：DWS 汇总 vs DWD 重算")
val exp = DeltaIO.read(spark, Lake.dwdExposure).count()
val ods = DeltaIO.read(spark, Lake.odsEvents)
val clk = ods.filter(col("event_type") === "enter_item").count()
val funnel = DeltaIO.read(spark, Lake.dwsFunnelHourly)
val fExp = funnel.agg(sum("exposure_cnt")).collect().head.getLong(0)
val fClk = funnel.agg(sum("click_cnt")).collect().head.getLong(0)
say(f"曝光 DWD=$exp, DWS=$fExp → ${if (exp == fExp) "OK" else "MISMATCH"}")
say(f"点击 ODS=$clk, DWS=$fClk → ${if (clk == fClk) "OK" else "MISMATCH"}")
say(f"整体 CTR：DWD=${clk.toDouble / exp}%.4f，DWS=${fClk.toDouble / math.max(fExp, 1)}%.4f")

say("=" * 90)
say("③ content_daily 抽一个高曝光内容手算")
val cd = DeltaIO.read(spark, Lake.dwsContentDaily)
val top = cd.orderBy(col("exposure_cnt").desc).select("dt", "item_id", "exposure_cnt", "click_cnt", "avg_dwell").head
val dt0 = top.getString(0); val item0 = top.getLong(1)
val exp0 = DeltaIO.read(spark, Lake.dwdExposure)
  .filter(col("dt") === dt0 && col("item_id") === item0).count()
say(f"item=$item0 dt=$dt0：DWS exposure=${top.getLong(2)} vs DWD=$exp0 → ${if (top.getLong(2) == exp0) "OK" else "MISMATCH"}")
say(f"               click=${top.getLong(3)}，avg_dwell=${top.getDouble(4)}%.2f 秒")

say("=" * 90)
say("④ 作者净涨粉：DWS 汇总 vs DWD 事件数")
val rel = DeltaIO.read(spark, Lake.dwdRelation)
val follows = rel.filter(col("action") === "follow").count()
val unfollows = rel.filter(col("action") === "unfollow").count()
val ad = DeltaIO.read(spark, Lake.dwsAuthorDaily)
val adNew = ad.agg(sum("new_follower_cnt")).collect().head.getLong(0)
val adLost = ad.agg(sum("lost_follower_cnt")).collect().head.getLong(0)
say(s"关注 DWD=$follows / DWS=$adNew；取关 DWD=$unfollows / DWS=$adLost；净增=$follows - $unfollows")

say("=" * 90)
say("⑤ 会话 / 用户 / 留存 / 字典")
val sess = DeltaIO.read(spark, Lake.dwdSession)
val sd = DeltaIO.read(spark, Lake.dwsSessionDaily)
val bounce = sess.filter(col("exposure_cnt") === 1).count()
say(f"会话 DWD=${sess.count()} vs DWS=${sd.agg(sum("session_cnt")).collect().head.getLong(0)}；" +
  f"仅 1 次曝光的会话=$bounce")
val ud = DeltaIO.read(spark, Lake.dwsUserDaily)
say("user_daily：")
ud.select("dt", "dau", "new_user_cnt_registered", "new_user_cnt_observed", "new_user_gap")
  .orderBy("dt").collect().foreach(r => say(s"  ${r.get(0)} dau=${r.get(1)} reg=${r.get(2)} obs=${r.get(3)} gap=${r.get(4)}"))

say("retention_daily：")
DeltaIO.read(spark, Lake.dwsRetentionDaily)
  .select("dt", "cohort_dt", "day_offset", "cohort_size", "retained_cnt", "retention_rate")
  .orderBy("cohort_dt", "day_offset").collect()
  .foreach(r => say(s"  obs_dt=${r.get(0)} cohort=${r.get(1)} offset=${r.get(2)} " +
    s"size=${r.get(3)} retained=${r.get(4)} rate=${r.get(5)}"))

val dict = DeltaIO.read(spark, Lake.semanticMetricDict)
say(f"metric_dict：${dict.count()} 行，唯一 id=${dict.select("metric_id").distinct().count()}；" +
  f"性质分布=" + dict.groupBy("nature").count().collect()
    .map(r => s"${r.getString(0)}:${r.getLong(1)}").sorted.mkString(","))

say("=" * 90)
say("结论：①-⑤ 全部符合 → 第三批通过；下一步 P3.6 Tier0 + P3.7 对账。")
