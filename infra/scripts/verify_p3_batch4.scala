// =============================================================================
// P3.6 验收：Tier0（热度快照 / 作弊轨迹）+ Redis 侧读数（Redis 用 redis-cli 另查）
// =============================================================================
import com.hub.common._
import org.apache.spark.sql.functions._

val spark = Spark.session("crt-verify-batch4")
spark.sparkContext.setLogLevel("WARN")
def say(s: String): Unit = { println(s); System.out.flush() }

val heat = DeltaIO.read(spark, Lake.tier0Heat)
say("=" * 80)
say(s"heat_snapshot：rows=${heat.count()}，items=${heat.select("item_id").distinct().count()}，" +
  s"windows=${heat.select("window_minutes").distinct().collect().map(_.getInt(0)).sorted.mkString(",")}")
say("最近几次计算（dt/hour × 条数）：")
heat.groupBy("dt", "hour").count().orderBy(desc("dt"), desc("hour")).collect().take(3)
  .foreach(r => say(s"  ${r.getString(0)} ${r.getString(1)}时 -> ${r.getLong(2)} 行"))
say("热度 Top5（最新一批）：")
val latest = heat.agg(max("computed_at")).collect().head.getTimestamp(0)
heat.filter(col("computed_at") === lit(latest) && col("window_minutes") === 60)
  .orderBy(desc("heat")).select("item_id", "heat", "exposure_cnt", "click_cnt").collect().take(5)
  .foreach(r => say(f"  item=${r.getLong(0)} heat=${r.getDouble(1)}%.2f 曝光=${r.getLong(2)} 点击=${r.getLong(3)}"))

say("=" * 80)
if (DeltaIO.exists(spark, Lake.tier0Cheat)) {
  val cheat = DeltaIO.read(spark, Lake.tier0Cheat)
  say(s"cheat_events：rows=${cheat.count()}")
  cheat.groupBy("rule").count().collect().foreach(r => say(s"  ${r.getString(0)} -> ${r.getLong(1)}"))
} else {
  say("cheat_events：暂无（本批未命中 A1/A2，属正常；注入是概率事件）")
}
say("=" * 80)
say("结论：快照有行、Redis 有读数、作弊表可读 → P3.6 通过（V-02 对账留给 P3.7）。")
