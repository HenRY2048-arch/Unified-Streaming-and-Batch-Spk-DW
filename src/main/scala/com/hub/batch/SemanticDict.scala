package com.hub.batch

import com.hub.common.{DeltaIO, Lake}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 语义层：`semantic/metric_dict`（**由代码生成，单一来源**）。
 *
 * 为什么用代码而不是手写表：口径一变，字典必须跟着变。写在这里，`Metric_Definitions.md` §10 是它的
 * 人类可读镜像；两者不一致时，以本文件的 `definition/numerator/denominator` 为准并回写文档。
 *
 * `nature`（性质）是给 agent/看板的护栏：
 *   业务（可直接解释）/ 风控（安全信号）/ 验收（真值＝注入器）/ 自检（数仓可信度）。
 */
object SemanticDict {

  private case class M(
      id: String, name: String, definition: String, numerator: String, denominator: String,
      grain: String, dims: String, tier: Int, source: String, recomputable: Boolean,
      truth: String, nature: String)

  private val metrics: Seq[M] = Seq(
    M("F-01", "曝光量", "一次曝光一条（items 展开）", "count(曝光)", "—",
      "dt,hour × channel × item × author × category × position", "channel,item,author,category,position",
      1, "dwd/exposure", true, "—", "业务"),
    M("F-02", "曝光UV", "同一天同一用户只计一次", "count(distinct user_id)", "—",
      "dt,hour × channel × category", "channel,category", 1, "dwd/exposure", true, "—", "业务"),
    M("F-03", "点击量", "一次进入计一次（未闭环的消费也算）", "count(enter_item)", "—",
      "dt,hour × channel × category", "channel,category", 1, "dwd/click", true, "—", "业务"),
    M("F-04", "CTR", "点击 ÷ 曝光（分母必须是曝光）", "click_cnt", "exposure_cnt",
      "dt,hour × channel/position/item/author", "channel,position,item,author",
      1, "dws/funnel_hourly", true, "—", "业务"),
    M("F-05", "渠道效率", "各渠道曝光占比 / CTR / 关注转化", "channel 聚合", "全量曝光",
      "dt,hour × channel", "channel", 1, "dws/channel_hourly", true, "—", "业务"),
    M("F-06", "分发漏斗", "曝光→点击→消费→互动逐级率", "下一级计数", "上一级计数",
      "dt × channel × category", "channel,category", 2, "dws/funnel_hourly", true, "—", "业务"),
    M("F-07", "内容热度", "Σ w_e×count_e（1h/24h 滚动窗 + 停留折算）", "加权事件数", "—",
      "item × 1h/24h", "author,category,channel", 0, "tier0/heat_snapshot", true, "批侧热度重算", "业务"),
    M("F-08", "位置偏差", "位置–CTR 曲线 / rank_score 相关性", "click_cnt", "exposure_cnt",
      "dt × channel × position", "channel,position", 2, "dws/position_daily", true, "—", "业务"),
    M("C-01", "平均停留", "消费片段停留均值（另给中位数）", "sum(duration_sec)", "consume_cnt",
      "dt × item/author/channel", "item,author,channel", 2, "dwd/consume", true, "—", "业务"),
    M("C-02", "完读率", "duration ÷ 预计阅读时长（**推断量**）", "duration_sec", "content_len÷77.5",
      "dt × item/author/category", "item,author,category", 2, "dwd/consume + dim/item", true, "—", "业务"),
    M("C-03", "负反馈率", "dislike + 秒退(0.2–1.2s) ÷ 曝光（含注入噪声）", "负反馈事件", "曝光量",
      "dt × item/author/channel", "item,author,channel", 2, "dwd/consume + dwd/interaction", true, "Q-02 注入对照", "业务"),
    M("I-01", "互动量", "flow=事件数；state=(user,item) 去重", "count / count(distinct)", "—",
      "dt × item/author/type", "item,author,type", 2, "dwd/interaction", true, "—", "业务"),
    M("I-02", "互动率", "state 型 ÷ 曝光UV；flow 型 ÷ 曝光量", "互动数", "曝光UV / 曝光量",
      "dt × item/author", "item,author", 2, "dws/content_daily", true, "—", "业务"),
    M("I-03", "点击→互动转化率", "互动 ÷ 点击", "互动数", "click_cnt",
      "dt × item/category", "item,category", 2, "dws/content_daily", true, "—", "业务"),
    M("I-04", "收藏率", "收藏 ÷ 曝光UV", "fav_cnt", "exposure_uv",
      "dt × item/author", "item,author", 2, "dws/content_daily", true, "—", "业务"),
    M("I-05", "私信量", "私信事件数（弱保留）", "count(dm)", "—",
      "dt × channel", "channel", 2, "dwd/relation", true, "—", "业务"),
    M("R-01", "粉丝来源构成", "按渠道/来源内容/来源用户计数", "follow 计数", "—",
      "dt × author × channel", "author,channel,source_item,source_user", 2, "dws/relation_daily", true, "—", "业务"),
    M("R-02", "关注转化率", "关注 ÷ 曝光（另给 ÷ 点击）", "follow_cnt", "exposure_cnt",
      "dt × author × channel", "author,channel", 2, "dws/channel_hourly / relation_daily", true, "—", "业务"),
    M("R-03", "关注转化途径", "last-touch：来源构成 × 转化率", "命中的来源", "关注数",
      "dt × author × channel × source_item", "author,channel,source_item", 2, "dwd/relation", true, "ods/groundtruth", "业务"),
    M("R-04", "取关率/净涨粉", "unfollow÷follow；net=follow−unfollow", "净增", "关注数",
      "dt × author", "author", 2, "dws/author_daily", true, "—", "业务"),
    M("R-05", "粉丝活跃度", "新粉在关注后 7 天内的互动率", "有关注后互动的粉丝", "当日新粉",
      "dt × author", "author", 2, "dws/author_daily", true, "—", "业务"),
    M("R-06", "进主页量", "路径中间步", "count(enter_profile)", "—",
      "dt × author", "author", 2, "ods/events", true, "—", "业务"),
    M("A-01", "作者曝光/点击/互动", "按作者聚合", "对应计数", "—",
      "dt × author", "author,category", 2, "dws/author_daily", true, "—", "业务"),
    M("A-02", "作者涨粉/掉粉", "按作者聚合（净额）", "follow/unfollow", "—",
      "dt × author", "author", 2, "dws/author_daily", true, "—", "业务"),
    M("A-03", "作者产出", "发帖数与频率", "count(item_published)", "—",
      "dt × author", "author,category", 2, "dws/author_daily", true, "—", "业务"),
    M("A-04", "内容冷启动", "发布后 24h 的曝光/CTR/互动", "24h 窗口内计数", "—",
      "dt × item", "item,author,category", 2, "dws/cold_start_daily", true, "—", "业务"),
    M("S-01", "会话数", "distinct session_id", "count(distinct session_id)", "—",
      "dt", "—", 2, "dws/session_daily", true, "—", "业务"),
    M("S-02", "会话时长", "end_ts − start_ts（事件时间轴）", "duration_sec", "会话数",
      "dt", "—", 2, "dws/session_daily", true, "—", "业务"),
    M("S-03", "会话长度分布", "每会话事件数分布", "event_cnt 分布", "—",
      "dt", "—", 2, "dwd/session", true, "—", "业务"),
    M("S-04", "每会话曝光/点击/消费/转化", "按会话聚合", "对应计数", "会话数",
      "dt × session", "session", 2, "dwd/session", true, "—", "业务"),
    M("S-05", "会话跳出", "仅 1 次曝光的会话占比", "bounce_session_cnt", "session_cnt",
      "dt", "—", 2, "dws/session_daily", true, "—", "业务"),
    M("U-01", "DAU", "有会话的用户数", "count(distinct user_id)", "—",
      "dt", "—", 2, "dws/user_daily", true, "—", "业务"),
    M("U-02", "新增用户", "注册口径（主）/ 观测口径（自检）", "新增用户数", "—",
      "dt × 来源", "source", 2, "dws/user_daily + dim/user", true, "user_registered", "业务"),
    M("U-03", "留存/回访", "registered 用注册日、seed 用首见日；1/3/7 日回访率", "retained_cnt", "cohort_size",
      "dt × cohort_dt × cohort_type", "cohort,cohort_type", 2, "dws/retention_daily", true, "—", "业务"),
    M("U-04", "在线活跃", "近 N 分钟有会话的用户数", "count(distinct user_id)", "—",
      "滚动 N 分钟", "—", 0, "stream 状态", true, "批侧重算", "业务"),
    M("Q-01", "作弊判定(A1/A2)", "A1 五连评 / A2 五秒十五赞", "判定为作弊的事件", "—",
      "事件级", "user,item", 0, "tier0/cheat_events", true, "注入器", "风控"),
    M("Q-02", "秒退/短停留/长内容短停留/极长驻留", "各类计数 ÷ consume_cnt；short_stay 对 5% 基线", "异常计数", "consume_cnt",
      "dt × category/author", "category,author", 2, "dws/quality_daily", true, "注入器", "验收"),
    M("Q-03", "异常分布", "按类目/作者分布", "异常计数", "—",
      "dt × category/author", "category,author", 2, "dws/quality_daily", true, "注入器", "验收"),
    M("V-01", "归因对账一致率", "数仓 last-touch vs 真值（精确率/召回率）", "match_rate / recall", "joined_cnt / total_cnt",
      "dt × conversion_type", "conversion_type", 2, "verify/attribution", true, "ods/groundtruth", "自检"),
    M("V-02", "热度对账", "流近似 vs 批真值的偏差", "abs/rel diff", "—",
      "dt,hour × item", "item", 2, "verify/heat", true, "批侧热度重算", "自检")
  )

  /** 给 agent 的限制说明：口径本身没错，但解释时必须带上这些边界。 */
  private def limitationsOf(id: String): String = id match {
    case "C-02" => "推断量，非真值；无“读完”事件"
    case "C-03" => "含 D1 秒退注入；超速不重复计入"
    case "F-07" => "停留分不要求曝光；长驻留注入可放大"
    case "R-05" => "关注后 7 天窗口可能右截断"
    case "A-04" => "发布后 24h 窗口可能右截断"
    case "U-03" => "registered 用注册日、seed 用首见日；窗口可能未满"
    case "U-02" => "注册/观测双口径，缺口可为负"
    case "Q-02" => "short_stay 与 long_content_short_stay 为包含关系，不可相加"
    case "R-01" => "DWS 仅物化 source_item_top，完整来源需读 DWD"
    case "V-01" => "自检指标；精确率与召回率分开，不得当业务量"
    case "V-02" => "自检指标；当前为定义一致性，不是近似误差"
    case "I-01" => "flow/state 两种口径不可混用"
    case _ => "—"
  }

  private val schema = StructType(Seq(
    StructField("metric_id", StringType),
    StructField("name", StringType),
    StructField("definition", StringType),
    StructField("numerator", StringType),
    StructField("denominator", StringType),
    StructField("grain", StringType),
    StructField("dims", StringType),
    StructField("tier", IntegerType),
    StructField("source_fields", StringType),
    StructField("recomputable", BooleanType),
    StructField("truth_source", StringType),
    StructField("nature", StringType),
    StructField("limitations", StringType)
  ))

  def build(spark: SparkSession): Unit = {
    val rows = metrics.map(m => Row(m.id, m.name, m.definition, m.numerator, m.denominator,
      m.grain, m.dims, m.tier, m.source, m.recomputable, m.truth, m.nature, limitationsOf(m.id)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
    DeltaIO.write(df, Lake.semanticMetricDict, Nil, "overwrite")
    println(s"[SEMANTIC] metric_dict rows=${df.count()}（业务 " +
      s"${metrics.count(_.nature == "业务")} / 风控 ${metrics.count(_.nature == "风控")} / " +
      s"验收 ${metrics.count(_.nature == "验收")} / 自检 ${metrics.count(_.nature == "自检")}）")
  }
}
