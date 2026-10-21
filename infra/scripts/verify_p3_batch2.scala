// =============================================================================
// P3 第二批验收脚本（ODS / DIM / DWD）
// 用法：docker compose ... run --rm --entrypoint /opt/spark/bin/spark-shell spark \
//         --jars ... -i /opt/crt/infra/scripts/verify_p3_batch2.scala
// 判据（不通过就别往下做 DWS）：
//   ① ODS 有数据；② 曝光展开条数 == Σ items；③ 停留差中位为毫秒级；
//   ④ dim_user 有 seed/registered/inferred 三类；⑤ 会话数 > 0 且每会话步数合理。
// =============================================================================
import com.hub.common._
import org.apache.spark.sql.functions._

val spark = Spark.session("crt-verify-batch2")
spark.sparkContext.setLogLevel("WARN")

def count(path: String): Long =
  if (DeltaIO.exists(spark, path)) spark.read.format("delta").load(path).count() else -1L

println("=" * 90)
println("行数")
println("=" * 90)
Seq(
  "ods/events" -> Lake.odsEvents,
  "ods/groundtruth" -> Lake.odsTruth,
  "dim/user" -> Lake.dimUser,
  "dim/item" -> Lake.dimItem,
  "dim/edge" -> Lake.dimEdge,
  "dwd/exposure" -> Lake.dwdExposure,
  "dwd/consume" -> Lake.dwdConsume,
  "dwd/interaction" -> Lake.dwdInteraction,
  "dwd/relation" -> Lake.dwdRelation,
  "dwd/publish" -> Lake.dwdPublish,
  "dwd/session" -> Lake.dwdSession
).foreach { case (name, path) => println(f"$name%-20s ${count(path)}%10d") }

println("=" * 90)
println("不变量")
println("=" * 90)
val ods = DeltaIO.read(spark, Lake.odsEvents)
val expectedExposure = ods.filter(col("event_type") === "exposure_batch")
  .agg(sum(size(col("items")))).collect().head.getLong(0)
val exposureRows = count(Lake.dwdExposure)
println(s"曝光展开：dwd/exposure=$exposureRows，Σitems=$expectedExposure → ${if (exposureRows == expectedExposure) "OK" else "MISMATCH"}")

val cons = DeltaIO.read(spark, Lake.dwdConsume)
// 注意：unix_timestamp 只到秒，会把亚秒差抹成 0~1 秒的噪声；这里用 double 秒
val diff = cons.select(abs(
  col("leave_ts").cast("double") - col("enter_ts").cast("double") - col("duration_sec")).as("d"))
val q = diff.stat.approxQuantile("d", Array(0.5, 0.95), 0.001)
println(f"停留差（ts差 − duration_sec）：中位 ${q(0)}%.3f 秒 / p95 ${q(1)}%.3f 秒")

val du = DeltaIO.read(spark, Lake.dimUser)
println(s"dim_user 来源构成：seed=${du.filter(col("source") === "seed").count()}，" +
  s"registered=${du.filter(col("source") === "registered").count()}，" +
  s"inferred=${du.filter(col("source") === "inferred").count()}")
println(s"dim_user 有 first_seen 的：${du.filter(col("first_seen_ts").isNotNull).count()}")

println("U-02 注册口径（按 registered_ts 的日期）：")
du.filter(col("registered_ts").isNotNull)
  .groupBy(date_format(col("registered_ts"), "yyyy-MM-dd").as("dt"))
  .count().orderBy("dt").collect().take(5)
  .foreach(r => println(s"  ${r.getString(0)} -> ${r.getLong(1)}"))

val sess = DeltaIO.read(spark, Lake.dwdSession)
println(s"会话总数：${sess.count()}；每会话事件数中位/最大：")
sess.selectExpr("percentile_approx(event_cnt, 0.5) AS p50", "max(event_cnt) AS mx").show(false)

println("=" * 90)
println("结论：以上 5 项都符合判据 → 第二批通过；否则先修再进 DWS。")
println("=" * 90)
System.out.flush()
