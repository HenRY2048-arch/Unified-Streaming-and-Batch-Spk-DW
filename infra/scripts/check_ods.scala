// 小工具：确认 ODS 是否已写入（排查"流作业有没有跑通"最快的办法）
import com.hub.common._

val spark = Spark.session("crt-check-ods")
spark.sparkContext.setLogLevel("WARN")

for (p <- Seq(Lake.odsEvents, Lake.odsTruth)) {
  val ok = DeltaIO.exists(spark, p)
  val n = if (ok) spark.read.format("delta").load(p).count() else -1L
  println(s"CHECK $p exists=$ok rows=$n")
  System.out.flush()
}
System.exit(0)
