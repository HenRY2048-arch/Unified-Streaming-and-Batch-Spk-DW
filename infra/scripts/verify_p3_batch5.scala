// =============================================================================
// P3.8 验收：Gate 信任表 + 前置修正后的关键表
// =============================================================================
import com.hub.common._
import org.apache.spark.sql.functions._

val spark = Spark.session("crt-verify-p3b5")
spark.sparkContext.setLogLevel("WARN")
def say(s: String): Unit = { println(s); System.out.flush() }
def n(path: String): Long = if (DeltaIO.exists(spark, path)) spark.read.format("delta").load(path).count() else -1L

say("=" * 90)
say("① 前置修正表")
Seq(
  "dwd/click" -> Lake.dwdClick,
  "dwd/relation" -> Lake.dwdRelation,
  "dws/quality_daily" -> Lake.dwsQualityDaily,
  "dws/retention_daily" -> Lake.dwsRetentionDaily,
  "semantic/metric_dict" -> Lake.semanticMetricDict
).foreach { case (k, p) => say(f"$k%-24s ${n(p)}%8d") }

say("=" * 90)
say("② retention cohort 类型 / 档位")
if (DeltaIO.exists(spark, Lake.dwsRetentionDaily)) {
  DeltaIO.read(spark, Lake.dwsRetentionDaily)
    .groupBy("cohort_type", "day_offset").count().orderBy("cohort_type", "day_offset")
    .collect().foreach(r => say(s"  ${r.getString(0)} offset=${r.getInt(1)} rows=${r.getLong(2)}"))
}

say("=" * 90)
say("③ 字典 limitations / 性质")
if (DeltaIO.exists(spark, Lake.semanticMetricDict)) {
  val d = DeltaIO.read(spark, Lake.semanticMetricDict)
  say(s"rows=${d.count()}，有 limitations（非 —）：${d.filter(col("limitations") =!= "—").count()}")
  d.groupBy("nature").count().orderBy("nature").collect()
    .foreach(r => say(s"  ${r.getString(0)} -> ${r.getLong(1)}"))
}

say("=" * 90)
say("④ Gate 信任表")
if (DeltaIO.exists(spark, Lake.opsRunManifest)) {
  val m = DeltaIO.read(spark, Lake.opsRunManifest)
  say(s"run_manifest rows=${m.count()} runs=${m.select("run_id").distinct().count()}")
  m.groupBy("status", "action_taken").count().orderBy("status", "action_taken").collect()
    .foreach(r => say(s"  ${r.getString(0)} / ${r.getString(1)} -> ${r.getLong(2)}"))
} else say("  run_manifest missing")
if (DeltaIO.exists(spark, Lake.opsPartitionStatus)) {
  val p = DeltaIO.read(spark, Lake.opsPartitionStatus)
  say(s"partition_status rows=${p.count()}")
  p.select("dt", "table", "status", "limitations").orderBy(desc("dt"), asc("table")).collect()
    .take(20).foreach(r => say(s"  ${r.getString(0)} ${r.getString(1)} ${r.getString(2)} ${r.getString(3)}"))
} else say("  partition_status missing")

say("=" * 90)
say("⑤ 对账")
if (DeltaIO.exists(spark, Lake.verifyAttribution)) {
  val a = DeltaIO.read(spark, Lake.verifyAttribution)
    .agg(sum("total_cnt").as("total"), sum("joined_cnt").as("joined"),
      sum("matched_cnt").as("matched"), sum("missing_cnt").as("missing"),
      min("match_rate").as("min_rate"), min("recall").as("min_recall")).head
  say(s"  V-01 total=${a.getLong(0)} joined=${a.getLong(1)} matched=${a.getLong(2)} " +
    s"missing=${a.getLong(3)} min_rate=${a.getDouble(4)} min_recall=${a.getDouble(5)}")
}
if (DeltaIO.exists(spark, Lake.verifyHeat)) {
  val h = DeltaIO.read(spark, Lake.verifyHeat)
  val q = h.filter(col("rel_diff").isNotNull).stat.approxQuantile("rel_diff", Array(0.5, 0.95, 1.0), 0.001)
  say(f"  V-02 rows=${h.count()} rel_diff p50=${q(0)}%.6f p95=${q(1)}%.6f max=${q(2)}%.6f")
}
say("=" * 90)
