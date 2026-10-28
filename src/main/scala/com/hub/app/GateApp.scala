// Gate 入口所在包。
package com.hub.app

// Config 提供重算天数和等待参数；Spark 提供统一 SparkSession。
import com.hub.common.{Config, Spark}
// GateRunner 包含真正的 20 条检查和结果汇总逻辑。
import com.hub.verify.GateRunner

// UUID 用来给每次 Gate 运行生成唯一 runId。
import java.util.UUID

/**
 * 入口④：Gate 执行器（P3.8）。
 *
 * 调用链：
 *   Airflow BashOperator
 *     -> infra/scripts/run_warehouse.sh
 *       -> GateApp
 *         -> GateRunner.run()
 *         -> 写 manifest/partition_status/reports
 *         -> 映射退出码
 *
 * GateApp 不实现具体断言；断言在 GateRunner.scala。
 */
object GateApp {

  def main(args: Array[String]): Unit = {
    // 默认跑全部检查组。
    var stage = "all"
    // 默认不指定日期；GateRunner 会从 ODS 最大 dt 推导。
    var dt: Option[String] = None
    // 默认重算窗口来自 warehouse.properties。
    var days = Config.recomputeDays
    // 是否使用 Gate 内部等待模式；只有 DR-3 需要。
    var wait = false
    // 参数扫描游标。
    var i = 0

    // 扫描所有命令行参数。
    while (i < args.length) {
      args(i) match {
        // --stage dr|dwd|dws|verify|fx|all
        case "--stage" if i + 1 < args.length => stage = args(i + 1); i += 2
        // --dt YYYY-MM-DD；用于指定目标事件天。
        case "--dt" if i + 1 < args.length => dt = Some(args(i + 1)); i += 2
        // --days N；决定输入/对账窗口；QL 质量基线固定按配置的 7 日窗口。
        case "--days" if i + 1 < args.length => days = args(i + 1).toInt; i += 2
        // --wait；DR-3 未到水位时在进程内循环等待。
        case "--wait" => wait = true; i += 1
        // 其它参数忽略，保持向后兼容。
        case _ => i += 1
      }
    }

    // 创建 Gate 用 SparkSession。
    val spark = Spark.session("crt-gate")
    // 打印配置摘要，先确认路径、时区、重算窗口。
    println(Config.summary)
    // 打印本次参数，排查“到底跑了哪天/哪个 stage”。
    println(s"[GateApp] stage=$stage dt=${dt.getOrElse("max")} days=$days wait=$wait")

    // 每次运行生成唯一 runId；所有 manifest 行共用它。
    val runId = "gate-" + UUID.randomUUID().toString.replace("-", "").take(12)

    // 第一次执行 GateRunner：根据 stage 选择检查组并返回 GateResult 列表。
    var results = GateRunner.run(spark, stage, dt, days)
    // 已等待次数。
    var attempts = 0
    // 只有 --wait 才读取重试次数；否则 0。
    val maxAttempts = if (wait) Config.getInt("gate.dr3.retries", 3) else 0
    // 每次等待分钟数。
    val waitMinutes = Config.getInt("gate.dr3.wait.minutes", 5)

    // 如果总体是 wait 且处于 --wait 模式，就在进程内等待后重跑。
    while (wait && GateRunner.overall(results) == GateRunner.WAIT && attempts < maxAttempts) {
      attempts += 1
      println(s"[GateApp] wait: ${waitMinutes}min (attempt $attempts/$maxAttempts)")
      System.out.flush()
      Thread.sleep(waitMinutes.toLong * 60L * 1000L)
      results = GateRunner.run(spark, stage, dt, days)
    }

    // 打印每条 GateResult 的一行摘要。
    results.foreach { r =>
      println(f"[GATE] ${r.gateId}%-10s ${r.status}%-22s ${r.action}%-3s ${r.table}%-24s ${r.detail}")
    }

    // 取结果中的 dt；若结果为空，退回参数或 unknown。
    val targetDt = results.headOption.map(_.dt).getOrElse(dt.getOrElse("unknown"))

    // 口径与输入指纹在本次运行内只计算一次，清单/状态/报告共用。
    val contractHash = GateRunner.contractHash
    val inputFingerprint = GateRunner.fingerprint(spark, targetDt)

    // 写一行一条 gate 的审计清单。
    GateRunner.writeManifest(spark, runId, results, contractHash, inputFingerprint)
    // 按 dt × table 写完整的逐表信任状态。
    GateRunner.writePartitionStatus(spark, runId, results, contractHash, inputFingerprint)
    // 写人读 JSON/MD 报告。
    GateRunner.writeReports(spark, runId, results, contractHash, inputFingerprint)

    // 汇总全部结果的总体状态。
    val overall = GateRunner.overall(results)
    println(s"[GateApp] runId=$runId overall=$overall")
    System.out.flush()
    // 先释放 Spark，再决定进程退出码。
    spark.stop()

    // 退出码约定：
    //   blocked -> 1
    //   wait 且非 --wait -> 2（手工诊断）
    //   wait 且 --wait 超时 -> 1
    //   其它（pass / limitation / G3） -> 0
    overall match {
      case GateRunner.BLOCK => System.exit(1)
      case GateRunner.WAIT if !wait => System.exit(2)
      case GateRunner.WAIT => System.exit(1)
      case _ => System.exit(0)
    }
  }
}
