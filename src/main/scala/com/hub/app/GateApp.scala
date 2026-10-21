package com.hub.app

import com.hub.common.{Config, Spark}
import com.hub.verify.GateRunner

import java.util.UUID

/**
 * 入口④：Gate 执行器（P3.8）。
 *
 * 用法：
 *   GateApp --stage all
 *   GateApp --stage dws --dt 2026-10-18 --days 2
 *   GateApp --stage dr --wait
 *
 * `--wait`：DR-3 未到水位时在进程内等待（默认 5 分钟 × 3 次），Airflow 只看到最终 0/1；
 * 不加 `--wait` 且结果为 wait 时退出码 2，便于手工诊断。
 */
object GateApp {

  def main(args: Array[String]): Unit = {
    var stage = "all"
    var dt: Option[String] = None
    var days = Config.recomputeDays
    var wait = false
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--stage" if i + 1 < args.length => stage = args(i + 1); i += 2
        case "--dt" if i + 1 < args.length => dt = Some(args(i + 1)); i += 2
        case "--days" if i + 1 < args.length => days = args(i + 1).toInt; i += 2
        case "--wait" => wait = true; i += 1
        case _ => i += 1
      }
    }

    val spark = Spark.session("crt-gate")
    println(Config.summary)
    println(s"[GateApp] stage=$stage dt=${dt.getOrElse("max")} days=$days wait=$wait")

    val runId = "gate-" + UUID.randomUUID().toString.replace("-", "").take(12)
    var results = GateRunner.run(spark, stage, dt, days)
    var attempts = 0
    val maxAttempts = if (wait) Config.getInt("gate.dr3.retries", 3) else 0
    val waitMinutes = Config.getInt("gate.dr3.wait.minutes", 5)

    while (wait && GateRunner.overall(results) == GateRunner.WAIT && attempts < maxAttempts) {
      attempts += 1
      println(s"[GateApp] wait: ${waitMinutes}min (attempt $attempts/$maxAttempts)")
      System.out.flush()
      Thread.sleep(waitMinutes.toLong * 60L * 1000L)
      results = GateRunner.run(spark, stage, dt, days)
    }

    results.foreach { r =>
      println(f"[GATE] ${r.gateId}%-10s ${r.status}%-22s ${r.action}%-3s ${r.table}%-24s ${r.detail}")
    }

    val targetDt = results.headOption.map(_.dt).getOrElse(dt.getOrElse("unknown"))
    GateRunner.writeManifest(spark, runId, results, GateRunner.contractHash,
      GateRunner.fingerprint(spark, targetDt))
    GateRunner.writePartitionStatus(spark, results)
    GateRunner.writeReports(spark, runId, results)

    val overall = GateRunner.overall(results)
    println(s"[GateApp] runId=$runId overall=$overall")
    System.out.flush()
    spark.stop()

    overall match {
      case GateRunner.BLOCK => System.exit(1)
      case GateRunner.WAIT if !wait => System.exit(2)
      case GateRunner.WAIT => System.exit(1)
      case _ => System.exit(0)
    }
  }
}
