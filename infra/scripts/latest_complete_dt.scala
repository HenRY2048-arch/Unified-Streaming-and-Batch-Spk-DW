// 输出 ODS 中已经越过下一天 00:00 水位的最大事件天，供 Gate/观测脚本自动选目标日。
import com.hub.common._
import org.apache.spark.sql.functions._

val spark = Spark.session("crt-latest-complete-dt")
spark.sparkContext.setLogLevel("WARN")

val ods = DeltaIO.read(spark, Lake.odsEvents)
val overallMaxDt = ods.agg(max("dt").as("max_dt")).head().getString(0)
val daily = ods.groupBy("dt").count()
  // 下一天分区已经出现，说明目标日已越过水位。不能用目标日分区自身的 max(ts)：
  // 那个值必然落在本日内，不能证明午夜后的数据已经到达。
  .filter(col("dt") < lit(overallMaxDt))
  .orderBy(desc("dt"))

val rows = daily.limit(1).collect()
if (rows.isEmpty) {
  println("LATEST_COMPLETE_DT=<none>")
  System.exit(2)
}
println(s"LATEST_COMPLETE_DT=${rows.head.getString(0)}")
System.out.flush()
System.exit(0)
