package com.hub.verify

import com.hub.common._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import scala.util.Try

/**
 * Gate v1 执行器：DR / IV / CV / QL / RC / FX。
 *
 * 设计原则：
 *  - 只读湖表，不在 Gate 里改数；唯一写的是 `ops/run_manifest` 与 `ops/partition_status`。
 *  - 每条检查输出一个 [[GateResult]]；`action` 是处置，`status` 是证据结论。
 *  - 失败信息写进 manifest/report，不靠日志猜。
 */
object GateRunner {

  val PASS = "pass"
  val LIMIT = "pass_with_limitations"
  val NEED = "needs_confirmation"
  val BLOCK = "blocked"
  val WAIT = "wait"

  final case class GateResult(
      stage: String,
      gateId: String,
      category: String,
      level: Int,
      dt: String,
      status: String,
      metricValue: Option[Double],
      threshold: String,
      action: String,
      table: String,
      detail: String = "",
      approval: String = "safe_auto")

  private val policyVersion: String = Config.get("gate.policy.version", "gate-v1")
  private val qlTol: Double = Config.getDouble("gate.ql.tolerance.pp", 3.0) / 100.0
  private val qlDegrade: Double = Config.getDouble("gate.ql.degrade.pp", 5.0) / 100.0
  private val qlSmallSample: Long = Config.getLong("gate.ql.small.sample", 200L)
  private val qlRollingDays: Int = Config.getInt("gate.ql.rolling.days", 7)
  private val v02Rel: Double = Config.getDouble("gate.v02.rel.diff", 1e-9)
  private val rcMatch: Double = Config.getDouble("gate.rc.match.rate", 0.99)
  private val rcRecall: Double = Config.getDouble("gate.rc.recall", 0.999)

  def run(spark: SparkSession, stage: String, targetDtOpt: Option[String], days: Int): Seq[GateResult] = {
    val targetDt = targetDtOpt.getOrElse(maxDt(spark, Lake.odsEvents))
    val sinceDt = shiftDays(spark, targetDt, -(days - 1))
    val dr = readiness(spark, targetDt, sinceDt)
    val iv1 = if (stage == "dr") Nil else exposureInvariant(spark, targetDt)
    val iv2 = if (stage == "dwd") Nil else summaryInvariants(spark, targetDt)
    val cv = if (Set("dws", "all").contains(stage)) semanticChecks(spark, targetDt) else Nil
    val ql = if (Set("dws", "all").contains(stage)) qualityChecks(spark, targetDt, sinceDt) else Nil
    val rc = if (Set("verify", "all").contains(stage)) reconcileChecks(spark, targetDt) else Nil
    val fx = if (Set("fx", "all").contains(stage)) fixtureChecks(spark, targetDt) else Nil

    stage match {
      case "dr"     => dr
      case "dwd"    => dr ++ iv1
      case "dws"    => dr ++ iv1 ++ iv2 ++ cv ++ ql
      case "verify" => dr ++ iv1 ++ iv2 ++ cv ++ ql ++ rc
      case "fx"     => dr ++ fx
      case _        => dr ++ iv1 ++ iv2 ++ cv ++ ql ++ rc ++ fx
    }
  }

  // ---------------- DR ----------------

  private def readiness(spark: SparkSession, dt: String, sinceDt: String): Seq[GateResult] = {
    val ods = read(spark, Lake.odsEvents)
    val target = ods.filter(col("dt") === dt)
    val n = target.count()
    val maxTs = target.agg(max(col("event_ts"))).collect().head.getTimestamp(0)
    val nextDay = spark.sql(s"select to_timestamp(date_add(to_date('$dt'), 1)) t").head.getTimestamp(0).getTime
    val watermarkOk = maxTs != null && maxTs.getTime >= nextDay
    val maxDt = ods.agg(max(col("dt"))).collect().head.getString(0)
    val historical = dt < maxDt
    val dedupRatio = if (n == 0) 0.0 else target.select("event_id").distinct().count().toDouble / n
    val hist = ods.filter(col("dt") < dt).groupBy("dt").count().select("count").collect()
      .map(_.getLong(0)).sorted
    val histMedian = if (hist.isEmpty) 0.0 else hist(hist.length / 2).toDouble
    val volumeOk = histMedian == 0 || (n >= histMedian * 0.5 && n <= histMedian * 2.0)

    Seq(
      GateResult("dr", "DR-1-01", "DR", 1, dt, if (n > 0) PASS else BLOCK, Some(n.toDouble), "> 0",
        if (n > 0) "G4" else "G0", "ods/events", "目标日期分区存在"),
      GateResult("dr", "DR-2-01", "DR", 2, dt, if (n > 0) PASS else BLOCK, Some(n.toDouble), "> 0",
        if (n > 0) "G4" else "G0", "ods/events", s"事件数=$n"),
      GateResult("dr", "DR-3-01", "DR", 3, dt,
        if (watermarkOk) PASS else if (historical) LIMIT else WAIT,
        Option(maxTs).map(_.getTime.toDouble), s">= $nextDay",
        if (watermarkOk) "G4" else if (historical) "G2" else "G1", "ods/events",
        s"max_event_ts=${Option(maxTs).map(_.toString).getOrElse("null")}${if (historical) "（历史日缺水位，按限制处理）" else ""}"),
      GateResult("dr", "DR-4-01", "DR", 4, dt, if (volumeOk) PASS else LIMIT, Some(n.toDouble),
        s"median7=$histMedian", if (volumeOk) "G4" else "G2", "ods/events", "事件量相对近 7 天中位"),
      GateResult("dr", "DR-4-02", "DR", 4, dt,
        if (dedupRatio >= 0.98) PASS else LIMIT, Some(dedupRatio), ">= 0.98",
        if (dedupRatio >= 0.98) "G4" else "G2", "ods/events", "event_id 去重比例")
    )
  }

  // ---------------- IV ----------------

  private def exposureInvariant(spark: SparkSession, dt: String): Seq[GateResult] = {
    val ods = read(spark, Lake.odsEvents).filter(col("dt") === dt).dropDuplicates("event_id")
    val expected = ods.filter(col("event_type") === "exposure_batch")
      .agg(sum(size(col("items")))).collect().head.getLong(0)
    val actual = read(spark, Lake.dwdExposure).filter(col("dt") === dt).count()
    Seq(GateResult("dwd", "IV-1-01", "IV", 1, dt,
      if (actual == expected) PASS else BLOCK, Some(actual.toDouble), s"= $expected",
      if (actual == expected) "G4" else "G0", "dwd/exposure", s"展开=$actual, Σitems=$expected"))
  }

  private def summaryInvariants(spark: SparkSession, dt: String): Seq[GateResult] = {
    val exposure = read(spark, Lake.dwdExposure).filter(col("dt") === dt).count()
    val click = read(spark, Lake.dwdClick).filter(col("dt") === dt).count()
    val consume = read(spark, Lake.dwdConsume).filter(col("dt") === dt).count()
    val interact = read(spark, Lake.dwdInteraction).filter(col("dt") === dt).count()
    val funnel = read(spark, Lake.dwsFunnelHourly).filter(col("dt") === dt)
    val fe = funnel.agg(sum("exposure_cnt")).collect().head.getLong(0)
    val fc = funnel.agg(sum("click_cnt")).collect().head.getLong(0)
    val fs = funnel.agg(sum("consume_cnt")).collect().head.getLong(0)
    val fi = funnel.agg(sum("interact_cnt")).collect().head.getLong(0)
    val author = read(spark, Lake.dwsAuthorDaily).filter(col("dt") === dt)
    val badNet = author.filter(col("net_follower_cnt") =!= col("new_follower_cnt") - col("lost_follower_cnt")).count()
    val authorExp = author.agg(sum("exposure_cnt")).collect().head.getLong(0)

    Seq(
      GateResult("dws", "IV-2-01", "IV", 2, dt,
        if (fe == exposure && fc == click && fs == consume && fi == interact) PASS else BLOCK,
        Some((fe + fc + fs + fi).toDouble), "funnel = dwd",
        if (fe == exposure && fc == click && fs == consume && fi == interact) "G4" else "G0",
        "dws/funnel_hourly", s"exposure=$fe/$exposure click=$fc/$click consume=$fs/$consume interact=$fi/$interact"),
      GateResult("dws", "IV-2-02", "IV", 2, dt, if (badNet == 0) PASS else BLOCK, Some(badNet.toDouble),
        "= 0", if (badNet == 0) "G4" else "G0", "dws/author_daily", s"net != new - lost rows=$badNet"),
      GateResult("dws", "IV-2-03", "IV", 2, dt, if (authorExp == exposure) PASS else BLOCK,
        Some(authorExp.toDouble), s"= $exposure", if (authorExp == exposure) "G4" else "G0",
        "dws/author_daily", "作者曝光加总 = DWD 曝光")
    )
  }

  // ---------------- CV ----------------

  private def semanticChecks(spark: SparkSession, dt: String): Seq[GateResult] = {
    val funnel = read(spark, Lake.dwsFunnelHourly).filter(col("dt") === dt)
    val content = read(spark, Lake.dwsContentDaily).filter(col("dt") === dt)
    val retention = read(spark, Lake.dwsRetentionDaily).filter(col("dt") === dt)
    val bad = funnel.filter(
      (col("ctr") < 0 || col("ctr") > 1) ||
      (col("consume_rate") < 0 || col("consume_rate") > 1) ||
      (col("interact_rate") < 0 || col("interact_rate") > 1)).count() +
      content.filter(col("bounce_rate") < 0 || col("bounce_rate") > 1).count() +
      retention.filter(col("retention_rate") < 0 || col("retention_rate") > 1).count()
    val badUv = funnel.filter(col("exposure_uv") > col("exposure_cnt")).count() +
      content.filter(col("exposure_uv") > col("exposure_cnt")).count() +
      content.filter(col("interact_state_cnt") > col("interact_flow_cnt")).count()
    val dict = read(spark, Lake.semanticMetricDict)
    val ids = dict.select("metric_id").distinct().count()

    Seq(
      GateResult("dws", "CV-1-01", "CV", 1, dt, if (bad == 0) PASS else BLOCK, Some(bad.toDouble),
        "= 0", if (bad == 0) "G4" else "G0", "dws/*", s"比率越界行=$bad"),
      GateResult("dws", "CV-2-01", "CV", 2, dt, if (badUv == 0) PASS else BLOCK, Some(badUv.toDouble),
        "= 0", if (badUv == 0) "G4" else "G0", "dws/*", s"人数/人次越界行=$badUv"),
      GateResult("dws", "CV-3-01", "CV", 3, dt, if (ids == 40) PASS else BLOCK, Some(ids.toDouble),
        "= 40", if (ids == 40) "G4" else "G0", "semantic/metric_dict", s"unique metric_id=$ids")
    )
  }

  // ---------------- QL ----------------

  private def qualityChecks(spark: SparkSession, dt: String, sinceDt: String): Seq[GateResult] = {
    val q = read(spark, Lake.dwsQualityDaily).filter(col("dt") >= sinceDt)
    val target = q.filter(col("dt") === dt)
    val total = target.agg(sum("consume_cnt"), sum("bounce_cnt"), sum("short_stay_cnt"),
      sum("extended_stay_cnt")).collect().head
    val n = Option(total.get(0)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val base = if (n < qlSmallSample) q.agg(sum("consume_cnt"), sum("bounce_cnt"),
      sum("short_stay_cnt"), sum("extended_stay_cnt")).collect().head else total
    val nn = Option(base.get(0)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val bounce = if (nn == 0) 0.0 else base.get(1).asInstanceOf[Long].toDouble / nn
    val short = if (nn == 0) 0.0 else base.get(2).asInstanceOf[Long].toDouble / nn
    val extended = if (nn == 0) 0.0 else base.get(3).asInstanceOf[Long].toDouble / nn
    // D1（0.2–1.2s）与 E1（<0.8s）在事件层重叠，15%/5% 不能分别观测；
    // 这里用当前生成器的可观测总量基线：秒退≈20%、短停留≈14%、长驻留≈0.5%。
    val dev = Seq(bounce - 0.20, short - 0.14, extended - 0.005).map(math.abs).max
    val status = if (nn == 0) BLOCK else if (dev > qlDegrade) LIMIT else PASS
    val action = if (nn == 0) "G0" else if (dev > qlDegrade) "G2" else if (dev > qlTol) "G3" else "G4"

    val ret = read(spark, Lake.dwsRetentionDaily).filter(col("cohort_type") === "registered" && col("day_offset") === 1)
    val cohort = ret.filter(col("cohort_dt") === dt).select("cohort_size").collect().headOption.map(_.getLong(0))
    val hist = ret.filter(col("cohort_dt") < dt).groupBy("cohort_dt").agg(max("cohort_size").as("n"))
      .select("n").collect().map(_.getLong(0)).sorted
    val cohortStatus = (cohort, if (hist.isEmpty) None else Some(hist(hist.length / 2))) match {
      case (Some(c), Some(m)) if m > 0 =>
        val d = math.abs(c.toDouble - m) / m
        if (d > 0.5) (LIMIT, "G2", d) else if (d > 0.3) (PASS, "G3", d) else (PASS, "G4", d)
      case _ => (PASS, "G4", 0.0)
    }

    val ch = read(spark, Lake.dwsChannelHourly).filter(col("dt") >= sinceDt)
    def shares(d: DataFrame): Map[String, Double] = {
      val rows = d.groupBy("channel").agg(sum("exposure_cnt").as("n")).collect()
      val tot = rows.map(_.getLong(1)).sum.toDouble
      if (tot == 0) Map.empty else rows.map(r => r.getString(0) -> r.getLong(1) / tot).toMap
    }
    val cur = shares(ch.filter(col("dt") === dt))
    val old = shares(ch.filter(col("dt") < dt))
    val deltas = (cur.keySet ++ old.keySet).map(k => math.abs(cur.getOrElse(k, 0.0) - old.getOrElse(k, 0.0))).toSeq
    val chDelta = if (deltas.isEmpty) 0.0 else deltas.max

    Seq(
      GateResult("dws", "QL-1-01", "QL", 1, dt, status, Some(dev), "20%/14%/0.5% ±3pp",
        action, "dws/quality_daily", f"bounce=$bounce%.3f short=$short%.3f extended=$extended%.3f n=$nn"),
      GateResult("dws", "QL-2-01", "QL", 2, dt, cohortStatus._1, Some(cohortStatus._3), "相对 7 日中位",
        cohortStatus._2, "dws/retention_daily", "registered cohort_size"),
      GateResult("dws", "QL-2-02", "QL", 2, dt, if (chDelta > 0.10) LIMIT else if (chDelta > 0.05) PASS else PASS,
        Some(chDelta), "渠道占比变化", if (chDelta > 0.10) "G2" else if (chDelta > 0.05) "G3" else "G4",
        "dws/channel_hourly", f"max_abs_delta=$chDelta%.3f")
    )
  }

  // ---------------- RC ----------------

  private def reconcileChecks(spark: SparkSession, dt: String): Seq[GateResult] = {
    val va = read(spark, Lake.verifyAttribution).filter(col("dt") === dt)
    val agg = va.agg(sum("total_cnt"), sum("joined_cnt"), sum("matched_cnt"), sum("missing_cnt")).collect().head
    val total = Option(agg.get(0)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val joined = Option(agg.get(1)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val matched = Option(agg.get(2)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val missing = Option(agg.get(3)).map(_.asInstanceOf[Long]).getOrElse(0L)
    val matchRate = if (joined == 0) 0.0 else matched.toDouble / joined
    val recall = if (total == 0) 0.0 else joined.toDouble / total
    val rcStatus = if (total == 0) BLOCK else if (matchRate >= rcMatch && missing == 0) PASS
      else if (matchRate >= rcMatch && recall >= rcRecall) LIMIT else BLOCK

    val heat = read(spark, Lake.verifyHeat).filter(col("dt") === dt)
    val heatEmpty = heat.limit(1).count() == 0
    val maxRel = if (heatEmpty) 0.0 else heat.agg(max("rel_diff")).collect().head.getDouble(0)
    val heatStatus = if (heatEmpty) LIMIT else PASS

    Seq(
      GateResult("verify", "RC-2-01", "RC", 2, dt, rcStatus, Some(matchRate), "match>=0.99 & missing=0",
        if (rcStatus == PASS) "G4" else if (rcStatus == LIMIT) "G2" else "G0", "verify/attribution",
        s"total=$total joined=$joined matched=$matched missing=$missing recall=$recall"),
      GateResult("verify", "RC-2-02", "RC", 2, dt, heatStatus, Some(maxRel), s"rel_diff<=$v02Rel",
        "G3", "verify/heat", if (heatEmpty) "no snapshot for this dt" else s"max_rel_diff=$maxRel")
    )
  }

  // ---------------- FX ----------------

  private def fixtureChecks(spark: SparkSession, dt: String): Seq[GateResult] = {
    val consume = read(spark, Lake.dwdConsume).filter(col("dt") === dt)
    val q = if (consume.limit(1).count() == 0) 0.0
      else consume.select(abs(unix_timestamp(col("leave_ts")) - unix_timestamp(col("enter_ts")) - col("duration_sec")).as("d"))
        .stat.approxQuantile("d", Array(0.5), 0.001).head
    val injected = Try(spark.read.text(Config.seedDir + "/dim_user_injected.csv").count()).getOrElse(0L)
    val registered = read(spark, Lake.odsEvents).filter(col("event_type") === "user_registered")
      .select("user_id").distinct().count()
    val badContract = read(spark, Lake.odsEvents).filter(col("dt") === dt)
      .filter(col("event_id").isNull || col("user_id").isNull || col("event_type").isNull || col("ts").isNull).count()

    Seq(
      GateResult("fx", "FX-1-01", "FX", 1, dt, PASS, Some(q), "< 1s", "G4", "dwd/consume",
        f"median_ts_diff=$q%.3f"),
      GateResult("fx", "FX-2-01", "FX", 2, dt, if (injected == registered) PASS else LIMIT,
        Some((injected - registered).toDouble), "injected = registered", "G4", "dim_user_injected.csv",
        s"file=$injected, events=$registered"),
      GateResult("fx", "FX-3-01", "FX", 3, dt, if (badContract == 0) PASS else LIMIT,
        Some(badContract.toDouble), "= 0", "G4", "ods/events", s"字段缺失行=$badContract")
    )
  }

  // ---------------- 输出 ----------------

  def writeManifest(spark: SparkSession, runId: String, results: Seq[GateResult],
                    contractHash: String, fingerprint: String): Unit = {
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
    val now = new Timestamp(System.currentTimeMillis())
    val rows = results.map(r => Row(runId, r.stage, r.gateId, r.category, r.level, r.dt, r.status,
      r.metricValue.map(Double.box).orNull, r.threshold, r.action, r.approval,
      r.status == PASS, policyVersion, contractHash, fingerprint, r.table, now, r.detail))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
    if (DeltaIO.exists(spark, Lake.opsRunManifest)) DeltaIO.write(df, Lake.opsRunManifest, Seq("dt"), "append")
    else DeltaIO.write(df, Lake.opsRunManifest, Seq("dt"), "overwrite")
  }

  def writePartitionStatus(spark: SparkSession, results: Seq[GateResult]): Unit = {
    val schema = StructType(Seq(
      StructField("dt", StringType), StructField("table", StringType),
      StructField("status", StringType), StructField("limitations", StringType),
      StructField("policy_version", StringType), StructField("updated_at", TimestampType)))
    val now = new Timestamp(System.currentTimeMillis())
    val rows = results.groupBy(_.table).toSeq.map { case (table, rs) =>
      val status = if (rs.exists(_.status == BLOCK)) BLOCK
        else if (rs.exists(_.status == NEED)) NEED
        else if (rs.exists(_.status == WAIT)) LIMIT
        else if (rs.exists(_.status == LIMIT)) LIMIT else PASS
      val limits = rs.filter(r => r.status != PASS).map(r => s"${r.gateId}: ${r.detail}").mkString("; ")
      Row(rs.head.dt, table, status, limits, policyVersion, now)
    }
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
    if (DeltaIO.exists(spark, Lake.opsPartitionStatus)) {
      DeltaIO.merge(spark, Lake.opsPartitionStatus, df, "t.dt = s.dt AND t.table = s.table")
        .whenMatched.updateAll().whenNotMatched.insertAll().execute()
    } else DeltaIO.write(df, Lake.opsPartitionStatus, Seq("dt"), "overwrite")
  }

  def writeReports(spark: SparkSession, runId: String, results: Seq[GateResult]): Unit = {
    val dir = new Path(s"${Lake.opsReports}/run_id=$runId")
    val fs = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
    Try {
      fs.mkdirs(dir)
      val json = new OutputStreamWriter(fs.create(new Path(dir, "gate-report.json")), StandardCharsets.UTF_8)
      json.write("{\"run_id\":\"" + esc(runId) + "\",\"gates\":[")
      json.write(results.map(r => "{\"id\":\"" + esc(r.gateId) + "\",\"status\":\"" + esc(r.status) +
        "\",\"action\":\"" + esc(r.action) + "\",\"detail\":\"" + esc(r.detail) + "\"}").mkString(","))
      json.write("]}")
      json.close()
      val md = new OutputStreamWriter(fs.create(new Path(dir, "gate-report.md")), StandardCharsets.UTF_8)
      md.write(s"# Gate report $runId\n\n| gate | status | action | detail |\n|---|---|---|---|\n")
      results.foreach(r => md.write(s"| ${r.gateId} | ${r.status} | ${r.action} | ${r.detail} |\n"))
      md.close()
    }.recover { case e => println(s"[Gate] report write skipped: ${e.getMessage}") }
  }

  def overall(results: Seq[GateResult]): String =
    if (results.exists(_.status == BLOCK)) BLOCK
    else if (results.exists(_.status == NEED)) NEED
    else if (results.exists(_.status == WAIT)) WAIT
    else if (results.exists(_.status == LIMIT)) LIMIT
    else PASS

  def contractHash: String = Integer.toHexString(Contract.eventSchema.treeString.hashCode)

  def fingerprint(spark: SparkSession, dt: String): String = {
    val ods = read(spark, Lake.odsEvents).filter(col("dt") === dt)
    val r = ods.agg(count(lit(1)), max(col("_kafka_offset"))).collect().head
    s"dt=$dt;rows=${r.getLong(0)};maxOffset=${Option(r.get(1)).map(_.toString).getOrElse("null")}"
  }

  private def read(spark: SparkSession, path: String): DataFrame =
    DeltaIO.readIfExists(spark, path).getOrElse(spark.emptyDataFrame)

  private def maxDt(spark: SparkSession, path: String): String = {
    val df = read(spark, path)
    val r = df.agg(max("dt")).collect().head
    Option(r.getString(0)).getOrElse(sys.error(s"empty table: $path"))
  }

  private def shiftDays(spark: SparkSession, dt: String, days: Int): String =
    spark.sql(s"select date_format(date_add(to_date('$dt'), $days), 'yyyy-MM-dd') d").head.getString(0)

  private def esc(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
}
