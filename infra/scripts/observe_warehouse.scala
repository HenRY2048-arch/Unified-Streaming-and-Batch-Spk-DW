// 数仓全链路观测快照：一次性检查本次运行产出的层级、行数、日期范围、对账和 Gate 状态。
// 运行：infra/scripts/run_shell.sh /opt/crt/infra/scripts/observe_warehouse.scala
import com.hub.common._
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.json.{JSONArray, JSONObject}

val spark = Spark.session("crt-observe-warehouse")
spark.sparkContext.setLogLevel("WARN")

val observationId = "observation-" +
  new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Timestamp(System.currentTimeMillis()))
val report = new JSONObject()
report.put("observation_id", observationId)
report.put("generated_at", new Timestamp(System.currentTimeMillis()).toString)
report.put("timezone", Config.sessionTimeZone)

def say(s: String): Unit = {
  println(s)
  System.out.flush()
}

def tableStats(name: String, path: String): JSONObject = {
  val item = new JSONObject()
  item.put("table", name)
  if (!DeltaIO.exists(spark, path)) {
    item.put("exists", false)
    item.put("rows", -1L)
    return item
  }
  val df = DeltaIO.read(spark, path)
  item.put("exists", true)
  item.put("rows", df.count())
  if (df.columns.contains("dt") && df.columns.contains("event_ts")) {
    val r = df.agg(min("dt").as("min_dt"), max("dt").as("max_dt"),
      min("event_ts").as("min_ts"), max("event_ts").as("max_ts")).head()
    item.put("min_dt", Option(r.getAs[String]("min_dt")).getOrElse(""))
    item.put("max_dt", Option(r.getAs[String]("max_dt")).getOrElse(""))
    item.put("min_event_ts", Option(r.getTimestamp(2)).map(_.toString).getOrElse(""))
    item.put("max_event_ts", Option(r.getTimestamp(3)).map(_.toString).getOrElse(""))
  } else if (df.columns.contains("dt")) {
    val r = df.agg(min("dt").as("min_dt"), max("dt").as("max_dt")).head()
    item.put("min_dt", Option(r.getAs[String]("min_dt")).getOrElse(""))
    item.put("max_dt", Option(r.getAs[String]("max_dt")).getOrElse(""))
  }
  item
}

def addTables(section: String, tables: Seq[(String, String)]): Unit = {
  val arr = new JSONArray()
  say("=" * 100)
  say(section)
  tables.foreach { case (name, path) =>
    val item = tableStats(name, path)
    arr.put(item)
    if (item.getBoolean("exists")) {
      val range =
        if (item.has("min_dt")) s" dt=${item.getString("min_dt")}..${item.getString("max_dt")}"
        else ""
      say(f"  $name%-28s rows=${item.getLong("rows")}%10d$range")
    } else {
      say(f"  $name%-28s MISSING")
    }
  }
  report.put(section, arr)
}

say("=" * 100)
say(s"OBSERVATION $observationId")
say(Config.summary)

addTables("SOURCE_AND_SERVICE", Seq(
  "ods/events" -> Lake.odsEvents,
  "ods/groundtruth" -> Lake.odsTruth,
  "tier0/heat_snapshot" -> Lake.tier0Heat,
  "tier0/cheat_events" -> Lake.tier0Cheat))

addTables("DIM", Seq(
  "dim/user" -> Lake.dimUser,
  "dim/item" -> Lake.dimItem,
  "dim/edge" -> Lake.dimEdge))

addTables("DWD", Seq(
  "dwd/exposure" -> Lake.dwdExposure,
  "dwd/click" -> Lake.dwdClick,
  "dwd/consume" -> Lake.dwdConsume,
  "dwd/interaction" -> Lake.dwdInteraction,
  "dwd/relation" -> Lake.dwdRelation,
  "dwd/publish" -> Lake.dwdPublish,
  "dwd/session" -> Lake.dwdSession))

addTables("DWS", Seq(
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
  "dws/cold_start_daily" -> Lake.dwsColdStartDaily))

addTables("VERIFY_AND_CONTRACT", Seq(
  "semantic/metric_dict" -> Lake.semanticMetricDict,
  "verify/attribution" -> Lake.verifyAttribution,
  "verify/heat" -> Lake.verifyHeat,
  "ops/run_manifest" -> Lake.opsRunManifest,
  "ops/partition_status" -> Lake.opsPartitionStatus))

// 事件类型分布：判断本轮真实包含了哪些行为与缺陷。
if (DeltaIO.exists(spark, Lake.odsEvents)) {
  val arr = new JSONArray()
  say("=" * 100)
  say("ODS EVENT TYPES")
  DeltaIO.read(spark, Lake.odsEvents).groupBy("event_type").count().orderBy(desc("count")).collect()
    .foreach { r =>
      val e = new JSONObject()
      e.put("event_type", Option(r.getString(0)).getOrElse("<null>"))
      e.put("count", r.getLong(1))
      arr.put(e)
      say(f"  ${e.getString("event_type")}%-24s ${r.getLong(1)}%10d")
    }
  report.put("ods_event_types", arr)
}

// V-01：truth 为分母，精确率和召回率分开看。
if (DeltaIO.exists(spark, Lake.verifyAttribution)) {
  val r = DeltaIO.read(spark, Lake.verifyAttribution)
    .agg(sum("total_cnt").as("total"), sum("joined_cnt").as("joined"),
      sum("matched_cnt").as("matched"), sum("missing_cnt").as("missing"),
      min("match_rate").as("min_match"), min("recall").as("min_recall")).head()
  val v1 = new JSONObject()
  Seq("total", "joined", "matched", "missing").zipWithIndex.foreach { case (k, i) =>
    v1.put(k, Option(r.get(i)).map(_.asInstanceOf[Long]).getOrElse(0L))
  }
  v1.put("min_match_rate", Option(r.getAs[Double]("min_match")).map(Double.box).orNull)
  v1.put("min_recall", Option(r.getAs[Double]("min_recall")).map(Double.box).orNull)
  report.put("v01", v1)
  say("=" * 100)
  say("V-01 " + v1.toString)
}

// V-02：当前是定义一致性回归，报告真实分位数。
if (DeltaIO.exists(spark, Lake.verifyHeat)) {
  val heat = DeltaIO.read(spark, Lake.verifyHeat)
  val v2 = new JSONObject()
  v2.put("rows", heat.count())
  val q = heat.filter(col("rel_diff").isNotNull)
    .stat.approxQuantile("rel_diff", Array(0.5, 0.95, 1.0), 0.001)
  if (q.length == 3) {
    v2.put("rel_diff_p50", q(0))
    v2.put("rel_diff_p95", q(1))
    v2.put("rel_diff_max", q(2))
  }
  report.put("v02", v2)
  say("=" * 100)
  say("V-02 " + v2.toString)
}

// 最新一次 Gate 的逐门结论。
if (DeltaIO.exists(spark, Lake.opsRunManifest)) {
  val manifest = DeltaIO.read(spark, Lake.opsRunManifest)
  val latest = manifest.orderBy(desc("ts")).select("run_id").limit(1).head().getString(0)
  val gates = manifest.filter(col("run_id") === latest).orderBy("gate_id")
  val arr = new JSONArray()
  say("=" * 100)
  say(s"LATEST GATE run_id=$latest")
  gates.select("gate_id", "status", "action_taken", "table", "detail").collect().foreach { r =>
    val e = new JSONObject()
    e.put("gate_id", r.getString(0))
    e.put("status", r.getString(1))
    e.put("action", r.getString(2))
    e.put("table", Option(r.getString(3)).getOrElse(""))
    e.put("detail", Option(r.getString(4)).getOrElse(""))
    arr.put(e)
    say(f"  ${r.getString(0)}%-10s ${r.getString(1)}%-22s ${r.getString(2)}%-3s ${r.getString(3)}")
  }
  report.put("latest_gate_run_id", latest)
  report.put("latest_gate", arr)
}

// 最新 dt 的逐表信任状态。
if (DeltaIO.exists(spark, Lake.opsPartitionStatus)) {
  val status = DeltaIO.read(spark, Lake.opsPartitionStatus)
  val latestDt = status.agg(max("dt")).head().getString(0)
  val arr = new JSONArray()
  say("=" * 100)
  say(s"PARTITION STATUS dt=$latestDt")
  status.filter(col("dt") === latestDt).orderBy("table").collect().foreach { r =>
    val e = new JSONObject()
    e.put("table", r.getString(r.fieldIndex("table")))
    e.put("status", r.getString(r.fieldIndex("status")))
    e.put("publish_final", r.getBoolean(r.fieldIndex("publish_final")))
    e.put("limitations", Option(r.getString(r.fieldIndex("limitations"))).getOrElse(""))
    arr.put(e)
    say(f"  ${e.getString("table")}%-28s ${e.getString("status")}%-22s publish_final=${e.getBoolean("publish_final")}")
  }
  report.put("partition_status_dt", latestDt)
  report.put("partition_status", arr)
}

// Redis 是缓存投影，不属于 Delta 观测报告；由 infra/scripts/observe_warehouse.sh 单独采集。

val outPath = new Path(s"${Lake.opsReports}/observations/$observationId.json")
val fs = outPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
fs.mkdirs(outPath.getParent)
val stream = fs.create(outPath, true)
try stream.write(report.toString(2).getBytes(StandardCharsets.UTF_8))
finally stream.close()
say("=" * 100)
say(s"OBSERVATION_JSON $outPath")

spark.stop()
System.exit(0)
