import com.hub.common._
import org.apache.spark.sql.functions._

val spark = Spark.session("crt-diag-v01")
spark.sparkContext.setLogLevel("WARN")

val truth = DeltaIO.read(spark, Lake.odsTruth)
  .filter(col("conversion_type") === "like")
  .select(col("event_id"), col("item_id").as("t_item"), col("channel").as("t_channel"), col("batch_id").as("t_batch"))
val inter = DeltaIO.read(spark, Lake.dwdInteraction)
  .filter(col("action") === "like")
  .select(col("event_id"), col("item_id").as("w_item"), col("channel").as("w_channel"), col("batch_id").as("w_batch"))

println("truth(like) 行数=" + truth.count() + "，interaction(like) 行数=" + inter.count())
val j = truth.join(inter, Seq("event_id"), "inner")
println("join 行数=" + j.count())
j.select("event_id", "t_item", "w_item", "t_channel", "w_channel", "t_batch", "w_batch")
  .limit(5).collect().foreach(r => println(r.toString))
System.out.flush()
System.exit(0)
