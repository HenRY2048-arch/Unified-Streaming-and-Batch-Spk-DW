// Gate 规则执行器所在包。
package com.hub.verify

// Config、Contract、DeltaIO、Lake 等公共能力。
import com.hub.batch.SemanticDict
import com.hub.common._
// Hadoop FileSystem/Path：把 Gate 报告写到 S3A。
import org.apache.hadoop.fs.{FileSystem, Path}
// Spark SQL 的 DataFrame、Row、SparkSession 类型。
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
// DataFrame 表达式函数，如 col/filter/agg/when。
import org.apache.spark.sql.functions._
// Spark SQL 类型，用于手工构造 manifest/status schema。
import org.apache.spark.sql.types._

// 写 gate-report.json / gate-report.md 的输出流和编码。
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
// Gate 结果时间戳。
import java.sql.Timestamp
// Try：报告写失败时不让主流程崩掉。
import scala.util.Try

/**
 * Gate v1 执行器：DR / IV / CV / QL / RC / FX。
 *
 * 阅读顺序建议：
 *   1) 先看 GateResult 和 run()，理解“检查组如何被选择”；
 *   2) 再按 readiness -> exposureInvariant -> summaryInvariants
 *      -> semanticChecks -> qualityChecks -> reconcileChecks -> fixtureChecks
 *      看每类检查；
 *   3) 最后看 writeManifest/writePartitionStatus/writeReports/overall。
 *
 * 重要：G0/G1/G2/G3/G4 不是类或文件，只是 GateResult.action 的字符串值。
 */
object GateRunner {

  // 证据结论：检查通过。
  val PASS = "pass"
  // 证据结论：可用但有限制。
  val LIMIT = "pass_with_limitations"
  // 证据结论：需要人确认/审批。
  val NEED = "needs_confirmation"
  // 证据结论：硬错误，不允许下游使用。
  val BLOCK = "blocked"
  // 证据结论：上游数据还没到，需要等待。
  val WAIT = "wait"

  /**
   * 一条 Gate 检查的完整结果。
   *
   * status 是证据结论；action 才是处置动作（G0–G4）。
   */
  final case class GateResult(
      // 所属阶段：dr/dwd/dws/verify/fx。
      stage: String,
      // 门禁编号，例如 IV-1-01。
      gateId: String,
      // 类别：DR/IV/CV/QL/RC/FX。
      category: String,
      // 层级：1/2/3/4。
      level: Int,
      // 作用日期。
      dt: String,
      // 证据结论：pass/pass_with_limitations/needs_confirmation/blocked/wait。
      status: String,
      // 实际观测值；有些门是计数，有些是比例。
      metricValue: Option[Double],
      // 阈值的人类可读描述。
      threshold: String,
      // 处置动作：G0/G1/G1h/G2/G3/G4。
      action: String,
      // 受影响的表名或表范围。
      table: String,
      // 排查细节。
      detail: String = "",
      // 是否需要人工审批；默认安全自动。
      approval: String = "safe_auto")

  // 本次运行使用的策略版本；用于事后回溯阈值。
  private val policyVersion: String = Config.get("gate.policy.version", "gate-v1")
  // QL 告警容差：3 个百分点。
  private val qlTol: Double = Config.getDouble("gate.ql.tolerance.pp", 3.0) / 100.0
  // QL 降级容差：5 个百分点。
  private val qlDegrade: Double = Config.getDouble("gate.ql.degrade.pp", 5.0) / 100.0
  // 单日小样本阈值：少于 200 条消费时用 7 日滚动。
  private val qlSmallSample: Long = Config.getLong("gate.ql.small.sample", 200L)
  // QL 滚动窗口天数。
  private val qlRollingDays: Int = Config.getInt("gate.ql.rolling.days", 7)
  // V-02 当前允许的相对差。
  private val v02Rel: Double = Config.getDouble("gate.v02.rel.diff", 1e-9)
  // V-01 精确率下限。
  private val rcMatch: Double = Config.getDouble("gate.rc.match.rate", 0.99)
  // V-01 召回率下限。
  private val rcRecall: Double = Config.getDouble("gate.rc.recall", 0.999)

  /**
   * 执行某个 stage 的全部检查。
   *
   * stage 不是“只跑某一类”，而是分层累积：
   *   dr     -> DR
   *   dwd    -> DR + IV-1
   *   dws    -> DR + IV-1 + IV-2 + CV + QL
   *   verify -> 上述 + RC
   *   all    -> 上述 + FX
   */
  def run(spark: SparkSession, stage: String, targetDtOpt: Option[String], days: Int): Seq[GateResult] = {
    // 目标日期：显式 --dt 优先，否则从 ODS 最大 dt 推。
    val targetDt = targetDtOpt.getOrElse(maxDt(spark, Lake.odsEvents))
    // 窗口起点：targetDt - (days - 1)。
    val sinceDt = shiftDays(spark, targetDt, -(days - 1))

    // DR：所有 stage 都先做就绪检查。
    val dr = readiness(spark, targetDt, sinceDt)
    // IV-1：只要不是只跑 dr，就检查曝光展开守恒。
    val iv1 = if (stage == "dr") Nil else exposureInvariant(spark, targetDt)
    // IV-2：dwd 阶段不跑，dws/verify/all 才检查主题层守恒。
    val iv2 = if (stage == "dwd") Nil else summaryInvariants(spark, targetDt)
    // CV：只有 dws/all 才检查口径结构。
    val cv = if (Set("dws", "all").contains(stage)) semanticChecks(spark, targetDt) else Nil
    // QL：只有 dws/all 才检查质量分布。
    val ql = if (Set("dws", "all").contains(stage)) qualityChecks(spark, targetDt) else Nil
    // RC：只有 verify/all 才检查对账。
    val rc = if (Set("verify", "all").contains(stage)) reconcileChecks(spark, targetDt) else Nil
    // FX：只有 fx/all 才检查生成器夹具自检。
    val fx = if (Set("fx", "all").contains(stage)) fixtureChecks(spark, targetDt) else Nil

    // 按 stage 组装最终结果；注意累积关系。
    stage match {
      case "dr"     => dr
      case "dwd"    => dr ++ iv1
      case "dws"    => dr ++ iv1 ++ iv2 ++ cv ++ ql
      case "verify" => dr ++ iv1 ++ iv2 ++ cv ++ ql ++ rc
      case "fx"     => dr ++ fx
      case _        => dr ++ iv1 ++ iv2 ++ cv ++ ql ++ rc ++ fx
    }
  }

  // ===========================================================================
  // DR：数据就绪
  // ===========================================================================
  private def readiness(spark: SparkSession, dt: String, sinceDt: String): Seq[GateResult] = {
    // 读取证据层 ODS 事件表。
    val ods = read(spark, Lake.odsEvents)
    // 只保留目标日期。
    val target = ods.filter(col("dt") === dt)
    // 目标日期事件数。
    val n = target.count()
    // 目标日期最大事件时间。
    val maxTs = target.agg(max(col("event_ts"))).collect().head.getTimestamp(0)
    // 目标日期的下一天 00:00 对应的 epoch ms；用 Spark 时区计算。
    val nextDay = spark.sql(s"select to_timestamp(date_add(to_date('$dt'), 1)) t").head.getTimestamp(0).getTime
    // 水位是否越过目标日：历史日缺水位要区分处理。
    val watermarkOk = maxTs != null && maxTs.getTime >= nextDay
    // 全 ODS 的最大 dt，用来判断目标日是否为历史日。
    val maxDt = ods.agg(max(col("dt"))).collect().head.getString(0)
    // 目标日 < 最大 dt 说明它不是当前日。
    val historical = dt < maxDt
    // event_id 去重比例；重复通常来自 Kafka 重放。
    val dedupRatio = if (n == 0) 0.0 else target.select("event_id").distinct().count().toDouble / n
    // 目标日之前最近 7 个事件日的事件数，用于量级判断。
    val histSince = shiftDays(spark, dt, -7)
    val hist = ods.filter(col("dt") >= histSince && col("dt") < dt)
      .groupBy("dt").count().select("count").collect()
      .map(_.getLong(0)).sorted
    // 历史中位数；无历史时记 0。
    val histMedian = if (hist.isEmpty) 0.0 else hist(hist.length / 2).toDouble
    // 目标日事件量是否落在中位 0.5x–2x；无历史时先放行。
    val volumeOk = histMedian == 0 || (n >= histMedian * 0.5 && n <= histMedian * 2.0)

    Seq(
      // DR-1-01：分区存在性；不存在就 blocked/G0。
      GateResult("dr", "DR-1-01", "DR", 1, dt, if (n > 0) PASS else BLOCK, Some(n.toDouble), "> 0",
        if (n > 0) "G4" else "G0", "ods/events", "目标日期分区存在"),
      // DR-2-01：非空；不存在/空分区都 blocked/G0。
      GateResult("dr", "DR-2-01", "DR", 2, dt, if (n > 0) PASS else BLOCK, Some(n.toDouble), "> 0",
        if (n > 0) "G4" else "G0", "ods/events", s"事件数=$n"),
      // DR-3-01：当前日缺水位则 wait；历史日缺水位则 limitation。
      GateResult("dr", "DR-3-01", "DR", 3, dt,
        if (watermarkOk) PASS else if (historical) LIMIT else WAIT,
        Option(maxTs).map(_.getTime.toDouble), s">= $nextDay",
        if (watermarkOk) "G4" else if (historical) "G2" else "G1", "ods/events",
        s"max_event_ts=${Option(maxTs).map(_.toString).getOrElse("null")}${if (historical) "（历史日缺水位，按限制处理）" else ""}"),
      // DR-4-01：事件量是否在近 7 天中位的合理区间。
      GateResult("dr", "DR-4-01", "DR", 4, dt, if (volumeOk) PASS else LIMIT, Some(n.toDouble),
        s"median7=$histMedian", if (volumeOk) "G4" else "G2", "ods/events", "事件量相对近 7 天中位"),
      // DR-4-02：去重比例是否正常。
      GateResult("dr", "DR-4-02", "DR", 4, dt,
        if (dedupRatio >= 0.98) PASS else LIMIT, Some(dedupRatio), ">= 0.98",
        if (dedupRatio >= 0.98) "G4" else "G2", "ods/events", "event_id 去重比例")
    )
  }

  // ===========================================================================
  // IV：守恒
  // ===========================================================================
  private def exposureInvariant(spark: SparkSession, dt: String): Seq[GateResult] = {
    // ODS 先去重，避免 Kafka 重放把 Σitems 放大。
    val ods = read(spark, Lake.odsEvents).filter(col("dt") === dt).dropDuplicates("event_id")
    // 每条 exposure_batch 的 items 数相加，得到理论曝光条数。
    val expected = ods.filter(col("event_type") === "exposure_batch")
      .agg(sum(size(col("items")))).collect().head.getLong(0)
    // DWD 实际展开后的行数。
    val actual = read(spark, Lake.dwdExposure).filter(col("dt") === dt).count()
    // IV-1-01：两边必须完全相等，否则 blocked/G0。
    Seq(GateResult("dwd", "IV-1-01", "IV", 1, dt,
      if (actual == expected) PASS else BLOCK, Some(actual.toDouble), s"= $expected",
      if (actual == expected) "G4" else "G0", "dwd/exposure", s"展开=$actual, Σitems=$expected"))
  }

  private def summaryInvariants(spark: SparkSession, dt: String): Seq[GateResult] = {
    // DWD 四张主事实表的当天条数。
    val exposure = read(spark, Lake.dwdExposure).filter(col("dt") === dt).count()
    val click = read(spark, Lake.dwdClick).filter(col("dt") === dt).count()
    val consume = read(spark, Lake.dwdConsume).filter(col("dt") === dt).count()
    val interact = read(spark, Lake.dwdInteraction).filter(col("dt") === dt).count()
    // DWS 漏斗表（小时粒度）当天数据。
    val funnel = read(spark, Lake.dwsFunnelHourly).filter(col("dt") === dt)
    // 漏斗四列分别求和。
    val fe = funnel.agg(sum("exposure_cnt")).collect().head.getLong(0)
    val fc = funnel.agg(sum("click_cnt")).collect().head.getLong(0)
    val fs = funnel.agg(sum("consume_cnt")).collect().head.getLong(0)
    val fi = funnel.agg(sum("interact_cnt")).collect().head.getLong(0)
    // 作者层当天表，用于净增和曝光加总。
    val author = read(spark, Lake.dwsAuthorDaily).filter(col("dt") === dt)
    // 逐行检查 net = new - lost；统计违反行数。
    val badNet = author.filter(col("net_follower_cnt") =!= col("new_follower_cnt") - col("lost_follower_cnt")).count()
    // 作者曝光加总。
    val authorExp = author.agg(sum("exposure_cnt")).collect().head.getLong(0)

    Seq(
      // IV-2-01：DWS 漏斗四项与 DWD 四表逐项相等。
      GateResult("dws", "IV-2-01", "IV", 2, dt,
        if (fe == exposure && fc == click && fs == consume && fi == interact) PASS else BLOCK,
        Some((fe + fc + fs + fi).toDouble), "funnel = dwd",
        if (fe == exposure && fc == click && fs == consume && fi == interact) "G4" else "G0",
        "dws/funnel_hourly", s"exposure=$fe/$exposure click=$fc/$click consume=$fs/$consume interact=$fi/$interact"),
      // IV-2-02：作者净增结构恒等式。
      GateResult("dws", "IV-2-02", "IV", 2, dt, if (badNet == 0) PASS else BLOCK, Some(badNet.toDouble),
        "= 0", if (badNet == 0) "G4" else "G0", "dws/author_daily", s"net != new - lost rows=$badNet"),
      // IV-2-03：作者曝光加总必须回到 DWD 曝光总数。
      GateResult("dws", "IV-2-03", "IV", 2, dt, if (authorExp == exposure) PASS else BLOCK,
        Some(authorExp.toDouble), s"= $exposure", if (authorExp == exposure) "G4" else "G0",
        "dws/author_daily", "作者曝光加总 = DWD 曝光")
    )
  }

  // ===========================================================================
  // CV：口径自洽
  // ===========================================================================
  private def semanticChecks(spark: SparkSession, dt: String): Seq[GateResult] = {
    // 三张带比率/人数口径的 DWS 表。
    val funnel = read(spark, Lake.dwsFunnelHourly).filter(col("dt") === dt)
    val content = read(spark, Lake.dwsContentDaily).filter(col("dt") === dt)
    val retention = read(spark, Lake.dwsRetentionDaily).filter(col("dt") === dt)
    // CV-1-01：所有比率必须在 [0,1]；统计越界行数。
    val bad = funnel.filter(
      (col("ctr") < 0 || col("ctr") > 1) ||
      (col("consume_rate") < 0 || col("consume_rate") > 1) ||
      (col("interact_rate") < 0 || col("interact_rate") > 1)).count() +
      content.filter(col("bounce_rate") < 0 || col("bounce_rate") > 1).count() +
      retention.filter(col("retention_rate") < 0 || col("retention_rate") > 1).count()
    // CV-2-01：人数口径不能大于人次口径。
    val badUv = funnel.filter(col("exposure_uv") > col("exposure_cnt")).count() +
      content.filter(col("exposure_uv") > col("exposure_cnt")).count() +
      content.filter(col("interact_state_cnt") > col("interact_flow_cnt")).count()
    // CV-3-01：语义字典必须有 40 个唯一 metric_id。
    val dict = read(spark, Lake.semanticMetricDict)
    val ids = dict.select("metric_id").distinct().count()
    val dictRows = dict.count()
    val badDictRows = dict.filter(
      col("metric_id").isNull || !col("metric_id").rlike("^[A-Z]-[0-9]{2}$") ||
        col("name").isNull || trim(col("name")) === "" ||
        col("definition").isNull || trim(col("definition")) === "" ||
        col("numerator").isNull || trim(col("numerator")) === "" ||
        col("denominator").isNull || trim(col("denominator")) === "" ||
        col("grain").isNull || trim(col("grain")) === "" ||
        col("source_fields").isNull || trim(col("source_fields")) === "" ||
        col("nature").isNull || trim(col("nature")) === "" ||
        col("limitations").isNull || trim(col("limitations")) === "" ||
        col("query_table").isNull || trim(col("query_table")) === "" ||
        col("query_mode").isNull || trim(col("query_mode")) === "" ||
        !col("query_mode").isin(
          "materialized", "compute_on_demand", "stream_snapshot", "verification", "redis_only") ||
        col("read_only").isNull || col("read_only") =!= true ||
        col("contract_version").isNull)
      .count()

    Seq(
      // 比率越界即 blocked。
      GateResult("dws", "CV-1-01", "CV", 1, dt, if (bad == 0) PASS else BLOCK, Some(bad.toDouble),
        "= 0", if (bad == 0) "G4" else "G0", "dws/*", s"比率越界行=$bad"),
      // 人数/人次越界即 blocked。
      GateResult("dws", "CV-2-01", "CV", 2, dt, if (badUv == 0) PASS else BLOCK, Some(badUv.toDouble),
        "= 0", if (badUv == 0) "G4" else "G0", "dws/*", s"人数/人次越界行=$badUv"),
      // 字典必须恰好 40 行、40 个唯一编号，且关键元数据非空。
      GateResult("dws", "CV-3-01", "CV", 3, dt,
        if (dictRows == 40 && ids == 40 && badDictRows == 0) PASS else BLOCK,
        Some(ids.toDouble), "rows=40 & unique=40 & metadata complete",
        if (dictRows == 40 && ids == 40 && badDictRows == 0) "G4" else "G0",
        "semantic/metric_dict",
        s"rows=$dictRows unique metric_id=$ids bad_metadata_rows=$badDictRows")
    )
  }

  // ===========================================================================
  // QL：质量分布
  // ===========================================================================
  private def qualityChecks(spark: SparkSession, dt: String): Seq[GateResult] = {
    // QL 的滚动基线与运行参数 days 解耦，固定使用配置的 7 日窗口。
    val rollingSinceDt = shiftDays(spark, dt, -(qlRollingDays - 1))
    val q = read(spark, Lake.dwsQualityDaily)
      .filter(col("dt") >= rollingSinceDt && col("dt") <= dt)
    val target = q.filter(col("dt") === dt)
    def aggregateStats(frame: DataFrame): org.apache.spark.sql.Row =
      frame.agg(
        sum("consume_cnt").as("consume_cnt"),
        sum("bounce_cnt").as("bounce_cnt"),
        sum("short_stay_cnt").as("short_stay_cnt"),
        sum("extended_stay_cnt").as("extended_stay_cnt")).collect().head

    def longValue(row: org.apache.spark.sql.Row, name: String): Long =
      Option(row.getAs[Any](name)).collect { case n: Number => n.longValue }.getOrElse(0L)

    def rates(row: org.apache.spark.sql.Row): Seq[Double] = {
      val n = longValue(row, "consume_cnt")
      if (n == 0) Seq(0.0, 0.0, 0.0)
      else Seq(
        longValue(row, "bounce_cnt").toDouble / n,
        longValue(row, "short_stay_cnt").toDouble / n,
        longValue(row, "extended_stay_cnt").toDouble / n)
    }

    val baselines = Seq(0.20, 0.14, 0.005)
    def deviations(row: org.apache.spark.sql.Row): Seq[Double] =
      rates(row).zip(baselines).map { case (actual, base) => actual - base }

    // 目标日的 consume/bounce/short/extended 汇总。
    val total = aggregateStats(target)
    // 目标日消费片段数；无数据时为 0。
    val n = longValue(total, "consume_cnt")
    // 小样本时用窗口总量，降低单日噪声。
    val base = if (n < qlSmallSample) aggregateStats(q) else total
    // 基线消费数。
    val nn = longValue(base, "consume_cnt")
    val baseRates = rates(base)
    val bounce = baseRates(0)
    val short = baseRates(1)
    val extended = baseRates(2)
    // D1/E1 区间重叠，因此用可观测总量 20%/14%/0.5% 作为基线。
    val currentDevs = deviations(total)
    val dev = deviations(base).map(math.abs).max
    val previous = q.filter(col("dt") < dt).groupBy("dt")
      .agg(sum("consume_cnt").as("consume_cnt"), sum("bounce_cnt").as("bounce_cnt"),
        sum("short_stay_cnt").as("short_stay_cnt"), sum("extended_stay_cnt").as("extended_stay_cnt"))
      .orderBy(desc("dt")).limit(1).collect().headOption
    val continuousDeviation = previous.exists { prev =>
      val p = deviations(prev)
      val pn = longValue(prev, "consume_cnt")
      pn > 0 && Seq(currentDevs, p).transpose.exists { case Seq(cur, old) =>
        math.abs(cur) > qlTol && math.abs(old) > qlTol && Math.signum(cur) == Math.signum(old)
      }
    }
    // QL 不允许 G0：无目标日样本或偏差过大只降级。
    val status =
      if (n == 0 || dev > qlDegrade || continuousDeviation) LIMIT else PASS
    val action = if (status == LIMIT) "G2" else if (dev > qlTol) "G3" else "G4"

    // 留存 cohort 质量：只看 registered、day_offset=1。
    val ret = read(spark, Lake.dwsRetentionDaily)
      .filter(col("cohort_type") === "registered" && col("day_offset") === 1)
    // 目标 cohort 的规模。
    val cohort = ret.filter(col("cohort_dt") === dt).select("cohort_size").collect().headOption.map(_.getLong(0))
    // 历史 cohort 规模中位数。
    val hist = ret.filter(col("cohort_dt") >= rollingSinceDt && col("cohort_dt") < dt)
      .groupBy("cohort_dt").agg(max("cohort_size").as("n"))
      .select("n").collect().map(_.getLong(0)).sorted
    // cohort 变化分级：>50% G2，>30% G3，否则 G4。
    val cohortStatus = (cohort, if (hist.isEmpty) None else Some(hist(hist.length / 2))) match {
      case (Some(c), Some(m)) if m > 0 =>
        val d = math.abs(c.toDouble - m) / m
        if (d > 0.5) (LIMIT, "G2", d) else if (d > 0.3) (PASS, "G3", d) else (PASS, "G4", d)
      case _ => (PASS, "G4", 0.0)
    }

    // 渠道占比变化：当天相对最近 7 日中位数，而不是只对比前一天。
    val ch = read(spark, Lake.dwsChannelHourly)
      .filter(col("dt") >= rollingSinceDt && col("dt") <= dt)
    val dailyCounts = ch.groupBy("dt", "channel").agg(sum("exposure_cnt").as("cnt"))
    val dailyTotals = dailyCounts.groupBy("dt").agg(sum("cnt").as("total_cnt"))
    val dailyShares = dailyCounts.join(dailyTotals, Seq("dt"))
      .withColumn("share", when(col("total_cnt") > 0, col("cnt") / col("total_cnt"))
        .otherwise(lit(null).cast("double")))
    val curRows = dailyShares.filter(col("dt") === dt)
      .select("channel", "share").collect()
    val cur = curRows.map(r => r.getString(0) -> r.getDouble(1)).toMap
    val histRows = dailyShares.filter(col("dt") < dt)
      .groupBy("channel")
      .agg(percentile_approx(col("share"), lit(0.5), lit(1000)).as("median_share"))
      .collect()
    val old = histRows.map(r => r.getString(0) -> r.getDouble(1)).toMap
    val deltas =
      if (old.isEmpty) Seq.empty
      else (cur.keySet ++ old.keySet)
        .map(k => math.abs(cur.getOrElse(k, 0.0) - old.getOrElse(k, 0.0))).toSeq
    val chDelta = if (deltas.isEmpty) 0.0 else deltas.max

    Seq(
      // QL-1-01：秒退/短停留/长驻留分布。
      GateResult("dws", "QL-1-01", "QL", 1, dt, status, Some(dev), "20%/14%/0.5% ±3pp",
        action, "dws/quality_daily",
        f"bounce=$bounce%.3f short=$short%.3f extended=$extended%.3f n=$nn target_n=$n continuous=$continuousDeviation"),
      // QL-2-01：cohort 规模相对近 7 日中位。
      GateResult("dws", "QL-2-01", "QL", 2, dt, cohortStatus._1, Some(cohortStatus._3), "相对 7 日中位",
        cohortStatus._2, "dws/retention_daily", "registered cohort_size"),
      // QL-2-02：渠道占比变化。
      GateResult("dws", "QL-2-02", "QL", 2, dt, if (chDelta > 0.10) LIMIT else if (chDelta > 0.05) PASS else PASS,
        Some(chDelta), "渠道占比变化", if (chDelta > 0.10) "G2" else if (chDelta > 0.05) "G3" else "G4",
        "dws/channel_hourly", f"max_abs_delta=$chDelta%.3f")
    )
  }

  // ===========================================================================
  // RC：与标准答案对账
  // ===========================================================================
  private def reconcileChecks(spark: SparkSession, dt: String): Seq[GateResult] = {
    // V-01 输出表。
    val va = read(spark, Lake.verifyAttribution).filter(col("dt") === dt)
    // total/joined/matched/missing 汇总。
    val agg = va.agg(sum("total_cnt"), sum("joined_cnt"), sum("matched_cnt"), sum("missing_cnt")).collect().head
    val total = Option(agg.get(0)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val joined = Option(agg.get(1)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val matched = Option(agg.get(2)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val missing = Option(agg.get(3)).map(_.asInstanceOf[Long]).getOrElse(0L)
    // 精确率：matched/joined。
    val matchRate = if (joined == 0) 0.0 else matched.toDouble / joined
    // 召回率：joined/total。
    val recall = if (total == 0) 0.0 else joined.toDouble / total
    // 无 truth 直接 blocked；精确率+missing=0 才 pass；
    // 精确率高但 missing>0 且召回达标时记 limitation；否则 blocked。
    val rcStatus = if (total == 0) BLOCK else if (matchRate >= rcMatch && missing == 0) PASS
      else if (matchRate >= rcMatch && recall >= rcRecall) LIMIT else BLOCK

    // V-02 输出表。
    val heat = read(spark, Lake.verifyHeat).filter(col("dt") === dt)
    // 该日期是否有快照。
    val heatEmpty = heat.limit(1).count() == 0
    // 最大相对差；空表时先填 0，避免 Row.getDouble(null) 崩。
    val maxRel = if (heatEmpty) 0.0 else heat.agg(max("rel_diff")).collect().head.getDouble(0)
    // NULL rel_diff 说明批侧为 0 或没有可比行，也应按限制处理。
    val nullRel = if (heatEmpty) 0L else heat.filter(col("rel_diff").isNull).count()
    // V-02 是 G3：偏差超限只降级/告警，不阻断批侧。
    val heatOk = !heatEmpty && nullRel == 0 && maxRel <= v02Rel
    val heatStatus = if (heatOk) PASS else LIMIT
    val heatDetail =
      if (heatEmpty) "no snapshot for this dt"
      else s"max_rel_diff=$maxRel null_rel_diff_rows=$nullRel"

    Seq(
      // RC-2-01：V-01 精确率/召回率。
      GateResult("verify", "RC-2-01", "RC", 2, dt, rcStatus, Some(matchRate), "match>=0.99 & missing=0",
        if (rcStatus == PASS) "G4" else if (rcStatus == LIMIT) "G2" else "G0", "verify/attribution",
        s"total=$total joined=$joined matched=$matched missing=$missing recall=$recall"),
      // RC-2-02：V-02 热度对账。
      GateResult("verify", "RC-2-02", "RC", 2, dt, heatStatus, Some(maxRel), s"rel_diff<=$v02Rel",
        "G3", "verify/heat", heatDetail)
    )
  }

  // ===========================================================================
  // FX：仿真器夹具自检
  // ===========================================================================
  private def fixtureChecks(spark: SparkSession, dt: String): Seq[GateResult] = {
    // 消费明细：检查 ts 差与 duration_sec 是否自洽。
    val consume = read(spark, Lake.dwdConsume).filter(col("dt") === dt)
    val consumeRows = consume.count()
    val q = if (consumeRows == 0) 0.0
      else consume.select(abs(unix_timestamp(col("leave_ts")) - unix_timestamp(col("enter_ts")) - col("duration_sec")).as("d"))
        .stat.approxQuantile("d", Array(0.5), 0.001).head
    // 运行期注入用户文件行数；文件不存在记 0。
    val injected = Try(spark.read.text(Config.seedDir + "/dim_user_injected.csv").count()).getOrElse(0L)
    // ODS 中 distinct 注册用户数（不是事件条数，避免重放重复）。
    val registered = read(spark, Lake.odsEvents).filter(col("event_type") === "user_registered")
      .select("user_id").distinct().count()
    // 目标日契约完整性：信封、枚举、事件特有必填字段和平台枚举。
    val validEventTypes = Seq(
      "app_open", "exposure_batch", "enter_item", "leave_item", "like", "fav", "share",
      "comment", "dislike", "follow", "unfollow", "dm", "enter_profile", "click_tag",
      "item_published", "user_registered")
    val events = read(spark, Lake.odsEvents).filter(col("dt") === dt)
    val badEnvelope =
      col("event_id").isNull || col("user_id").isNull || col("event_type").isNull ||
        col("ts").isNull || col("schema_version").isNull ||
        (col("event_type") =!= "user_registered" && col("session_id").isNull)
    val badEnums =
      !col("event_type").isin(validEventTypes: _*) ||
        !col("schema_version").isin("v1", "v2") ||
        col("platform").isNull || !col("platform").isin("ios", "android", "web")
    val badSpecific =
      (col("event_type") === "exposure_batch" &&
        (col("channel").isNull || col("batch_id").isNull ||
          col("items").isNull || size(col("items")) === 0)) ||
      (col("event_type") === "enter_item" &&
        (col("item_id").isNull || col("channel").isNull ||
          col("batch_id").isNull || col("position").isNull)) ||
      (col("event_type") === "leave_item" &&
        (col("item_id").isNull || col("duration_sec").isNull)) ||
      (col("event_type").isin("like", "fav", "share", "comment", "dislike") &&
        col("item_id").isNull) ||
      (col("event_type") === "comment" && col("comment_text").isNull) ||
      (col("event_type").isin("follow", "unfollow", "dm", "enter_profile") &&
        col("target_user_id").isNull) ||
      (col("event_type") === "dm" && col("comment_text").isNull) ||
      (col("event_type") === "click_tag" && col("tag").isNull) ||
      (col("event_type") === "item_published" &&
        (col("item_id").isNull || col("author_id").isNull || col("category").isNull ||
          col("content_len").isNull || col("published_ts").isNull)) ||
      (col("event_type") === "user_registered" &&
        (col("level").isNull || col("is_creator").isNull))
    val badContract = events.filter(badEnvelope || badEnums || badSpecific).count()

    Seq(
      // FX-1：停留自洽，只看中位数。
      GateResult("fx", "FX-1-01", "FX", 1, dt,
        if (consumeRows > 0 && q < 1.0) PASS else LIMIT, Some(q), "< 1s", "G4", "dwd/consume",
        f"median_ts_diff=$q%.3f consume_rows=$consumeRows"),
      // FX-2：累计 CSV 与 ODS distinct 注册用户；差异属于 G4 观察。
      GateResult("fx", "FX-2-01", "FX", 2, dt, if (injected == registered) PASS else LIMIT,
        Some((injected - registered).toDouble), "injected = registered", "G4", "dim_user_injected.csv",
        s"file=$injected, events=$registered"),
      // FX-3：契约必填字段完整性。
      GateResult("fx", "FX-3-01", "FX", 3, dt,
        if (events.count() > 0 && badContract == 0) PASS else LIMIT,
        Some(badContract.toDouble), "= 0", "G4", "ods/events",
        s"contract_invalid_rows=$badContract")
    )
  }

  // ===========================================================================
  // 输出层
  // ===========================================================================

  /**
   * 写 ops/run_manifest：一行一条 Gate 结果，append-only，作为审计轨迹。
   */
  def writeManifest(spark: SparkSession, runId: String, results: Seq[GateResult],
                    contractHash: String, fingerprint: String): Unit = {
    // manifest 的显式 schema；所有字段都要稳定，供 agent 读取。
    val schema = StructType(Seq(
      StructField("run_id", StringType), StructField("stage", StringType),
      StructField("gate_id", StringType), StructField("class", StringType),
      StructField("level", IntegerType), StructField("dt", StringType),
      StructField("status", StringType), StructField("metric_value", DoubleType),
      StructField("threshold", StringType), StructField("action_taken", StringType),
      StructField("approval", StringType), StructField("publish_final", BooleanType),
      StructField("policy_version", StringType), StructField("contract_hash", StringType),
      StructField("input_fingerprint", StringType), StructField("table", StringType),
      StructField("ts", TimestampType), StructField("detail", StringType)))
    // 同一次运行所有行共用时间戳。
    val now = new Timestamp(System.currentTimeMillis())
    // GateResult -> Row；metricValue 为 None 时写 null。
    val rows = results.map(r => Row(runId, r.stage, r.gateId, r.category, r.level, r.dt, r.status,
      r.metricValue.map(Double.box).orNull, r.threshold, r.action, r.approval,
      r.status == PASS, policyVersion, contractHash, fingerprint, r.table, now, r.detail))
    // 转成 DataFrame。
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
    // manifest 是追加审计，不覆盖历史。
    if (DeltaIO.exists(spark, Lake.opsRunManifest)) DeltaIO.write(df, Lake.opsRunManifest, Seq("dt"), "append")
    else DeltaIO.write(df, Lake.opsRunManifest, Seq("dt"), "overwrite")
  }

  /**
   * 写 ops/partition_status：dt × table 的最终信任状态。
   */
  def writePartitionStatus(spark: SparkSession, runId: String, results: Seq[GateResult],
                           contractHash: String, inputFingerprint: String): Unit = {
    // 分区状态表的 schema。
    val schema = StructType(Seq(
      StructField("dt", StringType), StructField("table", StringType),
      StructField("status", StringType), StructField("limitations", StringType),
      StructField("policy_version", StringType), StructField("contract_hash", StringType),
      StructField("input_fingerprint", StringType), StructField("run_id", StringType),
      StructField("publish_final", BooleanType), StructField("updated_at", TimestampType)))
    val now = new Timestamp(System.currentTimeMillis())
    val dt = results.head.dt

    // 通配结果展开为具体表；否则下游无法按 dt + table 精确查询。
    val direct = results.flatMap { r =>
      expandTable(r.table).map(t => t -> r)
    }.groupBy(_._1).map { case (t, rs) => t -> rs.map(_._2) }

    def statusRank(s: String): Int = s match {
      case BLOCK => 4
      case NEED  => 3
      case LIMIT | WAIT => 2
      case _ => 1
    }

    def worst(ss: Seq[String]): String =
      ss.map(s => if (s == WAIT) LIMIT else s).sortBy(statusRank).lastOption.getOrElse(PASS)

    val rows = canonicalTables.map { table =>
      val directRs = direct.getOrElse(table, Nil)
      val upstreamTables =
        if (table == "ods/events") Nil
        else if (table == "ods/groundtruth") Seq("ods/events")
        else if (table == "verify/heat") Seq("ods/events", "tier0/heat_snapshot")
        else if (table == "verify/attribution") Seq("ods/events", "ods/groundtruth")
        else if (table == "semantic/metric_dict") Nil
        else Seq("ods/events")
      val upstreamStatuses = upstreamTables.map { up =>
        val rs = direct.getOrElse(up, Nil)
        worst(rs.map(_.status))
      }
      val directStatus = worst(directRs.map(_.status))
      val inherited = worst(upstreamStatuses)
      // 没有专用检查的表不能默认 pass，只能继承上游并显式标记为“无专用门禁”。
      val noDirectStatus = if (directRs.isEmpty) LIMIT else PASS
      val status = worst(Seq(noDirectStatus, directStatus, inherited))
      val ownLimits = directRs.filter(_.status != PASS).map(r => s"${r.gateId}: ${r.detail}")
      val inheritedLimits = upstreamStatuses.filter(_ != PASS).map(s => s"upstream: $s")
      val noDirectGate =
        if (directRs.isEmpty) Seq("no dedicated gate for this table; status inherited from upstream")
        else Nil
      val limits = (ownLimits ++ inheritedLimits ++ noDirectGate).distinct.mkString("; ")
      Row(dt, table, status, limits, policyVersion, contractHash, inputFingerprint, runId,
        status == PASS, now)
    }
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
    // 一个 dt 的全量状态一次替换，避免半途运行留下旧表状态。
    if (DeltaIO.exists(spark, Lake.opsPartitionStatus)) {
      DeltaIO.overwriteWhere(df, Lake.opsPartitionStatus, s"dt = '$dt'")
    } else {
      DeltaIO.write(df, Lake.opsPartitionStatus, Seq("dt"), "overwrite")
    }
  }

  /**
   * 写 gate-report.json 和 gate-report.md；两者由同一批 GateResult 生成。
   */
  def writeReports(spark: SparkSession, runId: String, results: Seq[GateResult],
                   contractHash: String, inputFingerprint: String): Unit = {
    // 报告目录按 run_id 隔离。
    val dir = new Path(s"${Lake.opsReports}/run_id=$runId")
    // 使用 Spark 的 Hadoop 配置访问 S3A。
    val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
    // 报告写失败只打印警告，不影响主 Gate 结论；Try 包住整个写文件段。
    Try {
      // 创建目录。
      fs.mkdirs(dir)
      // JSON 报告写流。
      val json = new OutputStreamWriter(fs.create(new Path(dir, "gate-report.json")), StandardCharsets.UTF_8)
      // JSON 头和 run_id。
      val generatedAt = new Timestamp(System.currentTimeMillis()).toString
      val limitations = results.filter(_.status != PASS)
        .map(r => s"${r.gateId}: ${r.detail}").distinct
      val approvals = results.filter(_.approval != "safe_auto")
        .map(r => s"${r.gateId}: ${r.approval}").distinct
      json.write("{\"run_id\":\"" + esc(runId) +
        "\",\"generated_at\":\"" + esc(generatedAt) +
        "\",\"policy_version\":\"" + esc(policyVersion) +
        "\",\"contract_hash\":\"" + esc(contractHash) +
        "\",\"input_fingerprint\":\"" + esc(inputFingerprint) +
        "\",\"overall\":\"" + esc(overall(results)) + "\",\"gates\":[")
      // 每条 GateResult 写成 JSON 对象，拼接成数组。
      json.write(results.map { r =>
        val mv = r.metricValue.map(_.toString).getOrElse("null")
        "{\"id\":\"" + esc(r.gateId) + "\",\"class\":\"" + esc(r.category) +
          "\",\"level\":" + r.level + ",\"table\":\"" + esc(r.table) +
          "\",\"status\":\"" + esc(r.status) + "\",\"metric_value\":" + mv +
          ",\"threshold\":\"" + esc(r.threshold) + "\",\"action\":\"" + esc(r.action) +
          "\",\"approval\":\"" + esc(r.approval) + "\",\"detail\":\"" + esc(r.detail) + "\"}"
      }.mkString(","))
      // JSON 收尾。
      json.write("],\"limitations\":[" +
        limitations.map(x => "\"" + esc(x) + "\"").mkString(",") +
        "],\"approvals_required\":[" +
        approvals.map(x => "\"" + esc(x) + "\"").mkString(",") + "]}")
      json.close()
      // Markdown 报告写流。
      val md = new OutputStreamWriter(fs.create(new Path(dir, "gate-report.md")), StandardCharsets.UTF_8)
      // Markdown 表头。
      md.write(s"# Gate report $runId\n\n| gate | status | action | detail |\n|---|---|---|---|\n")
      // 每条 GateResult 一行。
      results.foreach(r => md.write(s"| ${r.gateId} | ${r.status} | ${r.action} | ${r.detail} |\n"))
      md.close()
    }.recover { case e => println(s"[Gate] report write skipped: ${e.getMessage}") }
  }

  /**
   * 汇总总体状态：blocked > needs_confirmation > wait > limitation > pass。
   */
  def overall(results: Seq[GateResult]): String =
    if (results.exists(_.status == BLOCK)) BLOCK
    else if (results.exists(_.status == NEED)) NEED
    else if (results.exists(_.status == WAIT)) WAIT
    else if (results.exists(_.status == LIMIT)) LIMIT
    else PASS

  // 契约指纹：schema 字符串 hash，用于判断口径是否变过。
  def contractHash: String = {
    val material = Seq(
      s"event_schema=${Contract.eventSchema.treeString}",
      s"truth_schema=${Contract.truthSchema.treeString}",
      s"seed_user=${Contract.seedUserSchema.treeString}",
      s"seed_item=${Contract.seedItemSchema.treeString}",
      s"seed_edge=${Contract.seedEdgeSchema.treeString}",
      s"metrics=${SemanticDict.definitionFingerprint}",
      s"heat_windows=${Config.heatWindowsMinutes.mkString(",")}",
      s"heat_weights=${Config.heatWeight.toSeq.sortBy(_._1).mkString(",")}",
      s"heat_dwell=${Config.heatDwellFactor}/${Config.heatDwellCapSeconds}",
      s"logic_version=${Config.get("contract.logic.version", "crt-contract-v1")}"
    ).mkString("\n")
    java.security.MessageDigest.getInstance("SHA-256")
      .digest(material.getBytes(StandardCharsets.UTF_8))
      .map(b => f"${b & 0xff}%02x").mkString
  }

  // 输入指纹：行数、distinct event_id、offset 边界和最大入湖时间，用于判断输入是否变化。
  def fingerprint(spark: SparkSession, dt: String): String = {
    val ods = read(spark, Lake.odsEvents).filter(col("dt") === dt)
    val r = ods.agg(
      count(lit(1)).as("rows"),
      countDistinct(col("event_id")).as("distinct_events"),
      min(col("_kafka_offset")).as("min_offset"),
      max(col("_kafka_offset")).as("max_offset"),
      max(col("_ingest_ts")).as("max_ingest_ts")).collect().head
    val material = s"dt=$dt;rows=${r.getLong(0)};distinct=${r.getLong(1)};" +
      s"minOffset=${Option(r.get(2)).map(_.toString).getOrElse("null")};" +
      s"maxOffset=${Option(r.get(3)).map(_.toString).getOrElse("null")};" +
      s"maxIngest=${Option(r.get(4)).map(_.toString).getOrElse("null")}"
    java.security.MessageDigest.getInstance("SHA-256")
      .digest(material.getBytes(StandardCharsets.UTF_8))
      .map(b => f"${b & 0xff}%02x").mkString
  }

  // 读表；表不存在时返回空 DataFrame，让后续检查尽量给出 blocked 而不是直接崩。
  private def read(spark: SparkSession, path: String): DataFrame =
    DeltaIO.readIfExists(spark, path).getOrElse(spark.emptyDataFrame)

  // 取某张表的最大 dt；空表直接报错，提示先跑上游。
  private def maxDt(spark: SparkSession, path: String): String = {
    val df = read(spark, path)
    val r = df.agg(max("dt")).collect().head
    Option(r.getString(0)).getOrElse(sys.error(s"empty table: $path"))
  }

  // 日期平移：用于计算 sinceDt。
  private def shiftDays(spark: SparkSession, dt: String, days: Int): String =
    spark.sql(s"select date_format(date_add(to_date('$dt'), $days), 'yyyy-MM-dd') d").head.getString(0)

  // JSON 字符串转义，避免 detail 里的引号/换行破坏报告。
  private def esc(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")

  private val canonicalDwsTables = Seq(
    "dws/funnel_hourly", "dws/channel_hourly", "dws/content_daily", "dws/author_daily",
    "dws/relation_daily", "dws/session_daily", "dws/user_daily", "dws/quality_daily",
    "dws/position_daily", "dws/retention_daily", "dws/cold_start_daily")

  private val canonicalTables: Seq[String] = Seq(
    "ods/events", "ods/groundtruth",
    "tier0/heat_snapshot", "tier0/cheat_events",
    "dim/user", "dim/item", "dim/edge",
    "dwd/exposure", "dwd/click", "dwd/consume", "dwd/interaction",
    "dwd/relation", "dwd/publish", "dwd/session") ++
    canonicalDwsTables ++ Seq(
    "verify/attribution", "verify/heat", "semantic/metric_dict")

  private def expandTable(table: String): Seq[String] =
    if (table == "dws/*") canonicalDwsTables
    else if (table == "verify/*") Seq("verify/attribution", "verify/heat")
    else if (canonicalTables.contains(table)) Seq(table)
    else Seq.empty
}
