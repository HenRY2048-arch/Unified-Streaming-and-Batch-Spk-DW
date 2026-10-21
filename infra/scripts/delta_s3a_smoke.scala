// =============================================================================
// Delta over S3A 冒烟脚本（SeaweedFS S3 网关）
// 运行方式（见 infra/README.md §S3A 冒烟）：
//   docker compose -f infra/docker-compose.yml --profile tools run --rm \
//     --entrypoint /opt/spark/bin/spark-shell spark -i /opt/crt/infra/scripts/delta_s3a_smoke.scala ...
// 目的：在写 P3 代码之前，把"批为真值"依赖的四件事验穿——
//   ① 建 Delta 表 / 写入 / 读回；② 按分区 replaceWhere 幂等重写（旧文件真的被删）；
//   ③ MERGE + 时间旅行 + VACUUM（列/删/读日志）；④ 流式写 Delta + checkpoint 落 S3A。
// 判据：全部 PASS 才算"Delta over S3A 可用"；任何 FAIL 都要看是 S3A committer 还是 SeaweedFS 的能力缺口。
// =============================================================================

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

spark.sparkContext.setLogLevel("WARN")

val lake = "s3a://crt-lake"
val root = s"$lake/smoke/delta_s3a"          // 主表（分区 dt/hour）
val bulkPath = s"$lake/smoke/bulk"           // 大文件写入（触发 multipart 上传）
val mergePath = s"$lake/smoke/merge_target"  // MERGE 目标表
val streamPath = s"$lake/smoke/stream_rate"  // 流式写入目标表
val ckPath = s"$lake/smoke/ck/rate_delta"    // 流 checkpoint（落在 S3A 上）

val dtA = "2026-10-16"
val dtB = "2026-10-17"
val vacPath = s"$lake/smoke/vacuum_test"   // VACUUM 专用表（自建历史，不依赖其他用例）

val hconf = spark.sparkContext.hadoopConfiguration

def listFiles(path: String): Seq[(String, Long)] = {
  val p = new Path(path)
  val fs = p.getFileSystem(hconf)
  if (!fs.exists(p)) Seq.empty
  else {
    val it = fs.listFiles(p, true)
    val b = Seq.newBuilder[(String, Long)]
    while (it.hasNext) {
      val st = it.next()
      if (st.isFile) b += ((st.getPath.toString, st.getLen))
    }
    b.result()
  }
}

def partFiles(dir: String): Seq[String] =
  listFiles(dir).map(_._1).filterNot(_.contains("/_delta_log/")).sorted

// 用反引号包住路径的 SQL 引用（避免在插值字符串里转义反引号）
def deltaRef(path: String): String = "delta.`" + path + "`"

val results = scala.collection.mutable.ListBuffer.empty[(String, Boolean, String)]

def check(name: String)(body: => String): Unit = {
  try {
    val msg = body
    results += ((name, true, msg))
    println(s"[PASS] $name :: $msg")
  } catch {
    case e: Throwable =>
      val msg = s"${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}".take(400)
      results += ((name, false, msg))
      println(s"[FAIL] $name :: $msg")
  }
}

println("=" * 100)
println(s"Spark ${spark.version} | Scala ${scala.util.Properties.versionString}")
println(s"S3A endpoint  = ${hconf.get("fs.s3a.endpoint")} | path-style=${hconf.get("fs.s3a.path.style.access")} | ssl=${hconf.get("fs.s3a.connection.ssl.enabled")}")
println(s"row committer = ${hconf.get("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "(default)")} | s3a.committer=${hconf.get("fs.s3a.committer.name", "(default)")}")
println(s"multipart.size= ${hconf.get("fs.s3a.multipart.size", "(default 64M)")}")
println("=" * 100)

// ---------------------------------------------------------------------------
// T0 清理上一轮冒烟产物（只删 smoke/ 前缀，保证可重复）
// ---------------------------------------------------------------------------
check("T0 清理 smoke/ 前缀") {
  val smoke = new Path(s"$lake/smoke/")
  // 注意：Path 会吃掉结尾斜杠，所以守卫按 "/smoke" 判定
  require(smoke.toString.endsWith("/smoke"), s"拒绝删除非 smoke 前缀路径：$smoke")
  val f = smoke.getFileSystem(hconf)
  val existed = listFiles(s"$lake/smoke").size
  val ok = f.delete(smoke, true)
  s"deleted=${if (ok) "ok" else "no-op"}, previousObjects=$existed"
}

// ---------------------------------------------------------------------------
// T1 S3 连通 + 列表（列桶根）
// ---------------------------------------------------------------------------
check("T1 s3a 连通/列表") {
  val top = listFiles(s"$lake/")
  s"lake root entries=${top.size}, sample=${top.take(3).map(_._1).mkString(",")}"
}

// ---------------------------------------------------------------------------
// T2 建 Delta 表 + 写入（分区 dt/hour）
// ---------------------------------------------------------------------------
check("T2 建表写入(overwrite, 分区 dt/hour)") {
  val df = spark.range(0, 100)
    .withColumn("dt", lit(dtA)).withColumn("hour", lit("13"))
    .withColumn("item_id", (col("id") + 200000).cast("long"))
    .withColumn("v", lit(1L))
    .withColumn("tag", lit("base"))          // 固定表 schema，供 T5 的分区重写复用
    .select("dt", "hour", "item_id", "v", "tag")
  df.write.format("delta").mode("overwrite").partitionBy("dt", "hour").save(root)
  val n = spark.read.format("delta").load(root).count()
  s"rows written=$n, files=${partFiles(root).size}"
}

// ---------------------------------------------------------------------------
// T3 读回（DataFrame API + Spark SQL 两条路径）
// ---------------------------------------------------------------------------
check("T3 读回(DF API + SQL)") {
  val nDf = spark.read.format("delta").load(root).count()
  val nSql = spark.sql(s"SELECT count(*) AS c FROM ${deltaRef(root)}").collect()(0).getLong(0)
  s"dfApi=$nDf, sql=$nSql"
}

// ---------------------------------------------------------------------------
// T4 大文件写入（触发 multipart 上传：单文件 > fs.s3a.multipart.size）
// ---------------------------------------------------------------------------
check("T4 大文件写入(multipart)") {
  // 载荷必须是"不可压缩"的高熵字符串，否则 parquet 一压就缩到 1MB，multipart 根本不会被触发
  val bulk = spark.range(0, 2000000)
    .selectExpr("id", "md5(concat(cast(id as string), cast(rand() as string))) as payload")
    .repartition(1)
  bulk.write.format("delta").mode("overwrite").save(bulkPath)
  val fs = listFiles(bulkPath).filter(_._1.endsWith(".parquet"))
  val rows = spark.read.format("delta").load(bulkPath).count()
  val maxMB = if (fs.isEmpty) 0.0 else fs.map(_._2).max / 1048576.0
  s"rows=$rows, parquetFiles=${fs.size}, maxFileMB=${maxMB.formatted("%.1f")}"
}

// ---------------------------------------------------------------------------
// T5 分区幂等重写（replaceWhere：INIT → A → B，每次旧文件必须消失、行数稳定）
// ---------------------------------------------------------------------------
check("T5 replaceWhere 幂等重写") {
  def rewrite(tag: String) = {
    val df = spark.range(0, 50)
      .withColumn("dt", lit(dtB)).withColumn("hour", lit("13"))
      .withColumn("item_id", (col("id") + 300000).cast("long"))
      .withColumn("v", lit(2L))
      .withColumn("tag", lit(tag))
      .select("dt", "hour", "item_id", "v", "tag")
    df.write.format("delta").mode("overwrite")
      .option("replaceWhere", s"dt = '$dtB'").save(root)
  }
  rewrite("INIT"); rewrite("A"); rewrite("B")
  val dfB = spark.read.format("delta").load(root).filter(col("dt") === dtB)
  val rowsB = dfB.count()
  val tags = dfB.select("tag").distinct().collect().map(_.getString(0)).sorted.mkString(",")
  val refFiles = spark.sql(s"DESCRIBE DETAIL ${deltaRef(root)}").select("numFiles").collect()(0).getLong(0)
  val onS3 = listFiles(root).count(_._1.endsWith(".parquet"))
  require(tags == "B", s"重写后 tag=$tags（应为 B）")
  require(rowsB == 50, s"重写后行数=$rowsB（应为 50，未翻倍）")
  // 注：Delta 的 replaceWhere 只在日志里移除旧文件，物理回收交给 VACUUM（见 T13）
  s"rows=$rowsB, tag=$tags, deltaRefFiles=$refFiles, s3Parquet=$onS3（差值＝待 VACUUM 回收的历史）"
}

// ---------------------------------------------------------------------------
// T6 MERGE（批侧"幂等重算"的另一种落地形式）
// ---------------------------------------------------------------------------
check("T6 MERGE(update + insert)") {
  val base = spark.range(0, 20).withColumn("v", lit(1L)).selectExpr("id", "v")
  base.write.format("delta").mode("overwrite").save(mergePath)
  val src = spark.range(0, 30).withColumn("v", lit(9L)).selectExpr("id", "v")
  DeltaTable.forPath(spark, mergePath).as("t")
    .merge(src.as("s"), "t.id = s.id")
    .whenMatched.updateAll()
    .whenNotMatched.insertAll()
    .execute()
  val df = spark.read.format("delta").load(mergePath)
  val total = df.count()
  val v9 = df.filter(col("v") === 9L).count()
  require(total == 30, s"MERGE 后行数=$total（应 30）")
  require(v9 == 30, s"MERGE 后 v=9 行数=$v9（应 30）")
  s"rows=$total, updated+inserted(v=9)=$v9"
}

// ---------------------------------------------------------------------------
// T7 时间旅行 + 提交历史（_delta_log 可列、可读）
// ---------------------------------------------------------------------------
check("T7 时间旅行 + DESCRIBE HISTORY") {
  val v0 = spark.read.format("delta").option("versionAsOf", 0).load(root).count()
  val logFiles = listFiles(s"$root/_delta_log").filter(_._1.endsWith(".json")).size
  val hist = spark.sql(s"DESCRIBE HISTORY ${deltaRef(root)}").count()
  require(v0 == 100, s"version 0 行数=$v0（应 100）")
  s"version0Rows=$v0, jsonLogFiles=$logFiles, historyVersions=$hist"
}

// ---------------------------------------------------------------------------
// T8 list-after-write 一致性（S3A 上"列"必须能立刻看到"写"的结果）
// ---------------------------------------------------------------------------
check("T8 list-after-write 一致性") {
  val refFiles = spark.sql(s"DESCRIBE DETAIL ${deltaRef(root)}").select("numFiles").collect()(0).getLong(0)
  val onDisk = listFiles(root).count(_._1.endsWith(".parquet"))
  val justWritten = partFiles(root).filter(_.contains(s"dt=$dtB"))
  require(justWritten.nonEmpty, "刚提交的分区在 S3 列表里看不到 → list-after-write 不成立")
  require(refFiles <= onDisk, s"Delta 引用数($refFiles) 大于 S3 实际 parquet 数($onDisk)")
  s"justWrittenFiles=${justWritten.size}, deltaRefFiles=$refFiles, s3Parquet=$onDisk"
}

// ---------------------------------------------------------------------------
// T9 VACUUM（列旧版本文件 + 真删；SeaweedFS 的 DELETE 能力）
// ---------------------------------------------------------------------------
check("T9 VACUUM(RETAIN 0 HOURS)") {
  // 自建历史：v0 写入 → v1 全量覆盖（v0 的文件成为陈旧文件）→ VACUUM 必须真删掉它们
  spark.range(0, 50).selectExpr("id", "cast(1 as bigint) as v")
    .write.format("delta").mode("overwrite").save(vacPath)
  val stale = listFiles(vacPath).filter(_._1.endsWith(".parquet")).map(_._1).toSet
  spark.range(0, 60).selectExpr("id", "cast(2 as bigint) as v")
    .write.format("delta").mode("overwrite").save(vacPath)
  val before = listFiles(vacPath).size
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
  // 注意：Delta 3.2.1 的 vacuum() 返回的是"待删列表"的空表（不是删除计数），
  // 因此判据必须落在"S3 上对象数下降 + 陈旧文件消失 + 当前版本可读"上。
  val removed = DeltaTable.forPath(spark, vacPath).vacuum(0.0).count()
  val after = listFiles(vacPath).size
  val rowsAfter = spark.read.format("delta").load(vacPath).count()
  val staleLeft = listFiles(vacPath).map(_._1).toSet.intersect(stale)
  require(after < before, s"VACUUM 后对象数未下降（before=$before, after=$after）")
  require(staleLeft.isEmpty, s"陈旧文件仍有 ${staleLeft.size} 个残留在 S3 上 → DELETE 未真正生效")
  require(rowsAfter == 60, s"VACUUM 后当前版本行数=$rowsAfter（应 60）")
  s"objectsBefore=$before, objectsAfter=$after (Δ=${before - after}), staleResidual=${staleLeft.size}, currentRows=$rowsAfter, vacuumReturnedRows=$removed"
}

// ---------------------------------------------------------------------------
// T10 流式写 Delta + checkpoint 落 S3A（P3 流① 的形状）
// ---------------------------------------------------------------------------
check("T10 流式写 Delta（checkpoint 在 S3A）") {
  val q = spark.readStream.format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .writeStream.format("delta")
    .option("checkpointLocation", ckPath)
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start(streamPath)
  try {
    Thread.sleep(25000)
    val prog = q.lastProgress
    val batches = if (prog == null) -1L else prog.batchId
    q.stop()
    val rows = spark.read.format("delta").load(streamPath).count()
    val ckFiles = listFiles(ckPath).size
    require(rows > 0, "流式写入后目标表为空")
    require(ckFiles > 0, "checkpoint 目录为空")
    s"streamedRows=$rows, lastBatchId=$batches, checkpointFiles=$ckFiles"
  } finally {
    if (q.isActive) q.stop()
  }
}

// ---------------------------------------------------------------------------
// T11 SQL DELETE（批侧按窗口重算时的另一种写法）
// ---------------------------------------------------------------------------
check("T11 SQL DELETE") {
  val before = spark.read.format("delta").load(root).count()
  spark.sql(s"DELETE FROM ${deltaRef(root)} WHERE dt = '$dtB'")
  val after = spark.read.format("delta").load(root).count()
  val hist = spark.sql(s"DESCRIBE HISTORY ${deltaRef(root)}").count()
  require(after == before - 50, s"DELETE 后行数=$after（应为 ${before - 50}）")
  s"rowsBefore=$before, rowsAfter=$after, historyVersions=$hist"
}

// ---------------------------------------------------------------------------
// T12 流式续跑（同一 checkpoint 重启）：P3 流① 靠它做"崩溃后不重不丢"
// ---------------------------------------------------------------------------
check("T12 流式续跑(同一 checkpoint 重启)") {
  val df1 = spark.read.format("delta").load(streamPath)
  val rows1 = df1.count()
  val versions1 = spark.sql(s"DESCRIBE HISTORY ${deltaRef(streamPath)}").count()
  val q = spark.readStream.format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .writeStream.format("delta")
    .option("checkpointLocation", ckPath)
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start(streamPath)
  try {
    Thread.sleep(20000)
  } finally {
    if (q.isActive) q.stop()
  }
  val df2 = spark.read.format("delta").load(streamPath)
  val rows2 = df2.count()
  val distinctPairs = df2.select("timestamp", "value").distinct().count()
  val versions2 = spark.sql(s"DESCRIBE HISTORY ${deltaRef(streamPath)}").count()
  require(rows2 > rows1, s"重启后行数未增长（$rows1 → $rows2）")
  require(distinctPairs == rows2, s"重启后出现重复行：rows=$rows2, distinct=$distinctPairs（幂等性不成立）")
  s"rowsBefore=$rows1, rowsAfter=$rows2, versionsBefore=$versions1, versionsAfter=$versions2, dupRows=${rows2 - distinctPairs}"
}

// ---------------------------------------------------------------------------
// T13 VACUUM 回收历史文件（与 Delta 引用对齐）——"批为真值"的空间回收动作
// ---------------------------------------------------------------------------
check("T13 VACUUM 回收历史(与引用对齐)") {
  val refFiles = spark.sql(s"DESCRIBE DETAIL ${deltaRef(root)}").select("numFiles").collect()(0).getLong(0)
  val before = listFiles(root).count(_._1.endsWith(".parquet"))
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
  DeltaTable.forPath(spark, root).vacuum(0.0)
  val after = listFiles(root).count(_._1.endsWith(".parquet"))
  val rowsNow = spark.read.format("delta").load(root).count()
  require(after == refFiles, s"VACUUM 后 S3 parquet 数($after) != Delta 引用数($refFiles)")
  require(rowsNow == 100, s"VACUUM 后行数=$rowsNow（应 100，当前版本不受影响）")
  s"refFiles=$refFiles, s3Before=$before, s3After=$after, reclaimed=${before - after}, rows=$rowsNow"
}

// ---------------------------------------------------------------------------
// 汇总
// ---------------------------------------------------------------------------
println("\n" + "=" * 100)
println("冒烟汇总")
println("=" * 100)
results.foreach { case (n, ok, m) => println(f"${if (ok) "PASS" else "FAIL"}%-4s | $n%-32s | $m") }
val failed = results.count(p => !p._2)
println("-" * 100)
println(s"总计 ${results.size} 项，PASS ${results.size - failed}，FAIL $failed")
println(s"结论：${if (failed == 0) "Delta over S3A 可用 → 可以进 P3" else "存在阻塞项 → 需先解决再进 P3"}")
println("=" * 100)

System.out.flush()
System.exit(if (failed == 0) 0 else 1)
