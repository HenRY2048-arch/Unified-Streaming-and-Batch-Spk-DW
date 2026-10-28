// 小工具：确认 ODS 是否已写入（排查"流作业有没有跑通"最快的办法）
import com.hub.common._
import org.apache.spark.sql.functions._

val spark = Spark.session("crt-check-ods")
spark.sparkContext.setLogLevel("WARN")

for (p <- Seq(Lake.odsEvents, Lake.odsTruth)) {
  val ok = DeltaIO.exists(spark, p)
  val n = if (ok) spark.read.format("delta").load(p).count() else -1L
  println(s"CHECK $p exists=$ok rows=$n")
  if (ok) {
    val df = spark.read.format("delta").load(p)
    println("  BY_DAY:")
    df.groupBy("dt").agg(
      count(lit(1)).as("rows"),
      countDistinct("event_id").as("distinct_events"),
      min("event_ts").as("min_ts"),
      max("event_ts").as("max_ts")
    ).orderBy("dt").collect().foreach(r =>
      println(s"    ${r.getString(0)} rows=${r.getLong(1)} distinct=${r.getLong(2)} " +
        s"min=${r.getTimestamp(3)} max=${r.getTimestamp(4)}"))
    if (df.columns.contains("event_type")) {
      println("  BY_EVENT_TYPE:")
      df.groupBy("event_type").count().orderBy(desc("count")).collect()
        .foreach(r => println(s"    ${Option(r.getString(0)).getOrElse("<null>")} ${r.getLong(1)}"))
    }
  }
  System.out.flush()
}
System.exit(0)
