package com.hub.batch

import com.hub.common._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * 主题域聚合（批）：DWD → DWS。
 *
 * 口径要点（与 `Metric_Definitions.md` §10 对齐）：
 *  - **分母永远是曝光**（`dwd/exposure`），旧实现用 `enter_item` 当分母是错的；
 *  - `I-01/I-02` 出双口径：flow=事件数、state=`distinct(user,item)`；state 型分母用曝光 UV；
 *  - `U-02` 出双口径：注册口径（`registered_ts`）与观测口径（`first_seen_dt`）；
 *  - `C-03` 保留注入噪声（D1/E1），另给 `Q-02` 的注入计数做对照；
 *  - 每张表按 `dt` 幂等重算（`replaceWhere`）。
 */
object DwsBuilder {

  def buildAll(spark: SparkSession, sinceDt: String): Unit = {
    val exposure = DeltaIO.read(spark, Lake.dwdExposure).filter(col("dt") >= sinceDt)
    val click = DeltaIO.read(spark, Lake.dwdClick).filter(col("dt") >= sinceDt)
    val consume = DeltaIO.read(spark, Lake.dwdConsume).filter(col("dt") >= sinceDt)
    val interaction = DeltaIO.read(spark, Lake.dwdInteraction).filter(col("dt") >= sinceDt)
    val relation = DeltaIO.read(spark, Lake.dwdRelation).filter(col("dt") >= sinceDt)
    val publish = DeltaIO.read(spark, Lake.dwdPublish).filter(col("dt") >= sinceDt)
    val sessionAll = DeltaIO.read(spark, Lake.dwdSession)
    val session = sessionAll.filter(col("dt") >= sinceDt)
    val items = DeltaIO.read(spark, Lake.dimItem).select("item_id", "author_id", "category", "content_len")
    val users = DeltaIO.read(spark, Lake.dimUser)
    val maxDt = sessionAll.agg(max(col("dt"))).collect().head.getString(0)

    // 点击（一次进入）：DWD 已按 event_id 去重，未闭环的消费也算点击。
    val clicks = click.select("user_id", "session_id", "item_id", "author_id", "category",
      "channel", "position", "ts", "dt", "hour")

    write(spark, funnelHourly(exposure, clicks, consume, interaction), Lake.dwsFunnelHourly,
      Seq("dt", "hour"), sinceDt, "funnel_hourly")
    write(spark, channelHourly(exposure, clicks, relation), Lake.dwsChannelHourly,
      Seq("dt", "hour"), sinceDt, "channel_hourly")
    write(spark, contentDaily(exposure, clicks, consume, interaction, items), Lake.dwsContentDaily,
      Seq("dt"), sinceDt, "content_daily")
    write(spark, authorDaily(spark, exposure, clicks, interaction, relation, publish, items),
      Lake.dwsAuthorDaily, Seq("dt"), sinceDt, "author_daily")
    write(spark, relationDaily(exposure, relation), Lake.dwsRelationDaily, Seq("dt"), sinceDt, "relation_daily")
    write(spark, sessionDaily(session), Lake.dwsSessionDaily, Seq("dt"), sinceDt, "session_daily")
    write(spark, userDaily(users, session, sinceDt), Lake.dwsUserDaily, Seq("dt"), sinceDt, "user_daily")
    write(spark, qualityDaily(consume, items), Lake.dwsQualityDaily, Seq("dt"), sinceDt, "quality_daily")
    write(spark, positionDaily(exposure, clicks), Lake.dwsPositionDaily, Seq("dt"), sinceDt, "position_daily")
    write(spark, retentionDaily(spark, sessionAll, users, sinceDt, maxDt), Lake.dwsRetentionDaily, Seq("dt"), sinceDt, "retention_daily")
    write(spark, coldStartDaily(publish, exposure, clicks, interaction), Lake.dwsColdStartDaily,
      Seq("dt"), sinceDt, "cold_start_daily")
  }

  private def write(spark: SparkSession, df: DataFrame, path: String,
                    parts: Seq[String], sinceDt: String, name: String): Unit = {
    if (DeltaIO.exists(spark, path)) {
      DeltaIO.overwriteWhere(df, path, s"dt >= '$sinceDt'")
    } else {
      DeltaIO.write(df, path, parts, "overwrite")
    }
    println(s"[DWS] $name rows=${df.count()}")
  }

  /** 安全除法：分母为 0 时给 null（而不是 Infinity/NaN）。 */
  private def ratio(num: String, den: String): Column =
    when(col(den) > 0, col(num) / col(den)).otherwise(lit(null).cast("double"))

  // ---------------- F-01~F-06 / I-02 ----------------

  private def funnelHourly(exposure: DataFrame, clicks: DataFrame,
                           consume: DataFrame, interaction: DataFrame): DataFrame = {
    val keys = Seq("dt", "hour", "channel", "category")
    val e = exposure.groupBy(keys.map(col): _*)
      .agg(count(lit(1)).as("exposure_cnt"), countDistinct(col("user_id")).as("exposure_uv"))
    val c = clicks.groupBy(keys.map(col): _*).agg(count(lit(1)).as("click_cnt"))
    val s = consume.groupBy(keys.map(col): _*).agg(count(lit(1)).as("consume_cnt"))
    val i = interaction.groupBy(keys.map(col): _*).agg(count(lit(1)).as("interact_cnt"))

    e.join(c, keys, "full_outer").join(s, keys, "full_outer").join(i, keys, "full_outer")
      .na.fill(0, Seq("exposure_cnt", "exposure_uv", "click_cnt", "consume_cnt", "interact_cnt"))
      .withColumn("ctr", ratio("click_cnt", "exposure_cnt"))
      .withColumn("consume_rate", ratio("consume_cnt", "exposure_cnt"))
      .withColumn("interact_rate", ratio("interact_cnt", "exposure_cnt"))
  }

  // ---------------- F-05 / R-02（渠道） ----------------

  private def channelHourly(exposure: DataFrame, clicks: DataFrame, relation: DataFrame): DataFrame = {
    val keys = Seq("dt", "hour", "channel")
    val e = exposure.groupBy(keys.map(col): _*)
      .agg(count(lit(1)).as("exposure_cnt"), countDistinct(col("user_id")).as("exposure_uv"))
    val c = clicks.groupBy("dt", "hour", "channel").agg(count(lit(1)).as("click_cnt"))
    val f = relation.filter(col("action") === "follow")
      .withColumn("hour", date_format(col("ts"), "HH"))
      .groupBy("dt", "hour", "channel").agg(count(lit(1)).as("follow_cnt"))

    e.join(c, keys, "full_outer").join(f, keys, "full_outer")
      .na.fill(0, Seq("exposure_cnt", "exposure_uv", "click_cnt", "follow_cnt"))
      .withColumn("ctr", ratio("click_cnt", "exposure_cnt"))
      .withColumn("follow_cvr", ratio("follow_cnt", "exposure_cnt"))       // 关注 ÷ 曝光
      .withColumn("follow_cvr_click", ratio("follow_cnt", "click_cnt"))    // 关注 ÷ 点击
  }

  // ---------------- F-* / C-01~03 / I-01 / I-04（内容） ----------------

  private def contentDaily(exposure: DataFrame, clicks: DataFrame, consume: DataFrame,
                           interaction: DataFrame, items: DataFrame): DataFrame = {
    val e = exposure.groupBy("dt", "item_id")
      .agg(count(lit(1)).as("exposure_cnt"), countDistinct(col("user_id")).as("exposure_uv"))
    val c = clicks.groupBy("dt", "item_id").agg(count(lit(1)).as("click_cnt"))
    val s = consume.groupBy("dt", "item_id").agg(
      count(lit(1)).as("consume_cnt"),
      avg(col("duration_sec")).as("avg_dwell"),
      sum(when(col("duration_sec").between(0.2, 1.2), lit(1)).otherwise(lit(0))).as("bounce_cnt"))
    val i = interaction.groupBy("dt", "item_id").agg(
      sum(when(col("action") === "like", 1).otherwise(0)).as("like_cnt"),
      sum(when(col("action") === "fav", 1).otherwise(0)).as("fav_cnt"),
      sum(when(col("action") === "comment", 1).otherwise(0)).as("comment_cnt"),
      sum(when(col("action") === "share", 1).otherwise(0)).as("share_cnt"),
      sum(when(col("action") === "dislike", 1).otherwise(0)).as("dislike_cnt"),
      countDistinct(struct(col("user_id"), col("item_id"))).as("interact_state_cnt"),
      count(lit(1)).as("interact_flow_cnt"))

    e.join(c, Seq("dt", "item_id"), "full_outer")
      .join(s, Seq("dt", "item_id"), "full_outer")
      .join(i, Seq("dt", "item_id"), "full_outer")
      .join(broadcast(items.select("item_id", "author_id", "category")), Seq("item_id"), "left")
      .na.fill(0)
      .withColumn("avg_dwell",
        when(col("consume_cnt") > 0, col("avg_dwell")).otherwise(lit(null).cast("double")))
      .withColumn("ctr", ratio("click_cnt", "exposure_cnt"))
      .withColumn("bounce_rate", ratio("bounce_cnt", "exposure_cnt"))
      .withColumn("interact_rate_state", ratio("interact_state_cnt", "exposure_uv"))
      .withColumn("interact_rate_flow", ratio("interact_flow_cnt", "exposure_cnt"))
  }

  // ---------------- A-01~A-03 / R-04 / R-05（作者） ----------------

  private def authorDaily(spark: SparkSession, exposure: DataFrame, clicks: DataFrame,
                          interaction: DataFrame, relation: DataFrame, publish: DataFrame,
                          items: DataFrame): DataFrame = {
    val e = exposure.groupBy("dt", "author_id")
      .agg(count(lit(1)).as("exposure_cnt"), countDistinct(col("user_id")).as("exposure_uv"))
    val c = clicks.groupBy("dt", "author_id").agg(count(lit(1)).as("click_cnt"))
    val i = interaction.groupBy("dt", "author_id").agg(count(lit(1)).as("interact_cnt"))
    val p = publish.groupBy("dt", "author_id").agg(
      count(lit(1)).as("publish_cnt"), first(col("category"), ignoreNulls = true).as("category"))
    val nf = relation.filter(col("action") === "follow")
      .select(col("dt"), col("target_user_id").as("author_id"))
      .groupBy("dt", "author_id").agg(count(lit(1)).as("new_follower_cnt"))
    val lf = relation.filter(col("action") === "unfollow")
      .select(col("dt"), col("target_user_id").as("author_id"))
      .groupBy("dt", "author_id").agg(count(lit(1)).as("lost_follower_cnt"))

    // R-05：新粉在关注后 7 天内是否与该作者的内容产生过互动
    val follows = relation.filter(col("action") === "follow")
      .select(col("dt"), col("target_user_id").as("author_id"),
        col("user_id").as("follower"), col("ts").as("follow_ts"))
    val acts = interaction.select(col("user_id").as("follower"), col("author_id"), col("ts").as("i_ts"))
    val activeFollowers = follows
      .join(acts, Seq("follower", "author_id"))
      .filter(col("i_ts") > col("follow_ts") &&
        col("i_ts") <= col("follow_ts") + expr("INTERVAL 7 DAYS"))
      .select("dt", "author_id", "follower").distinct()
      .groupBy("dt", "author_id").agg(count(lit(1)).as("new_follower_active_cnt"))

    val base = e.join(c, Seq("dt", "author_id"), "full_outer")
      .join(i, Seq("dt", "author_id"), "full_outer")
      .join(p, Seq("dt", "author_id"), "full_outer")
      .join(nf, Seq("dt", "author_id"), "full_outer")
      .join(lf, Seq("dt", "author_id"), "full_outer")
      .join(activeFollowers, Seq("dt", "author_id"), "left")
      .na.fill(0)
      .withColumn("net_follower_cnt", col("new_follower_cnt") - col("lost_follower_cnt"))
      .withColumn("ctr", ratio("click_cnt", "exposure_cnt"))
      .withColumn("new_follower_active_rate", ratio("new_follower_active_cnt", "new_follower_cnt"))

    // 截至当日（含）的累计粉丝数：按窗口内每个 dt 从边表算一次（量小，直接循环）
    val edge = DeltaIO.read(spark, Lake.dimEdge)
    val dts = base.select("dt").distinct().collect().map(_.getString(0)).sorted
    val followerEnd = dts.map { d =>
      val dayEnd = date_add(to_date(lit(d)), 1)
      edge.filter(col("valid_from") < dayEnd)
        .filter(col("valid_to").isNull || col("valid_to") >= dayEnd)
        .groupBy(col("followee").as("author_id"))
        .agg(count(lit(1)).as("follower_end_cnt"))
        .withColumn("dt", lit(d))
    }.reduceOption(_ unionByName _)

    followerEnd match {
      case Some(fe) => base.join(fe, Seq("dt", "author_id"), "left")
      case None => base.withColumn("follower_end_cnt", lit(0L))
    }
  }

  // ---------------- R-01~R-03（粉丝来源） ----------------

  private def relationDaily(exposure: DataFrame, relation: DataFrame): DataFrame = {
    val follows = relation.filter(col("action") === "follow")

    val nf = follows.groupBy(col("dt"), col("target_user_id").as("author_id"), col("channel"))
      .agg(count(lit(1)).as("new_follower_cnt"))
    val exp = exposure.groupBy("dt", "author_id", "channel")
      .agg(count(lit(1)).as("exposure_cnt"))

    val bySource = follows.filter(col("source_item_id").isNotNull)
      .groupBy(col("dt"), col("target_user_id").as("author_id"), col("channel"), col("source_item_id"))
      .agg(count(lit(1)).as("cnt"))
    val w = Window.partitionBy("dt", "author_id", "channel").orderBy(col("cnt").desc)
    val topSource = bySource.withColumn("rn", row_number().over(w)).filter(col("rn") === 1)
      .select(col("dt"), col("author_id"), col("channel"),
        col("source_item_id").cast("string").as("source_item_top"))

    nf.join(exp, Seq("dt", "author_id", "channel"), "left")
      .join(topSource, Seq("dt", "author_id", "channel"), "left")
      .na.fill(0, Seq("exposure_cnt"))
      .withColumn("follow_cvr", ratio("new_follower_cnt", "exposure_cnt"))
  }

  // ---------------- S-01~S-05（会话） ----------------

  private def sessionDaily(session: DataFrame): DataFrame =
    session.groupBy("dt").agg(
      count(lit(1)).as("session_cnt"),
      avg(col("duration_sec")).as("avg_duration"),
      avg(col("exposure_cnt")).as("avg_exposure"),
      avg(col("click_cnt")).as("avg_click"),
      avg(col("consume_cnt")).as("avg_consume"),
      sum(when(col("exposure_cnt") === 1, 1).otherwise(0)).as("bounce_session_cnt"))

  // ---------------- U-01 / U-02（双口径） ----------------

  private def userDaily(users: DataFrame, session: DataFrame, sinceDt: String): DataFrame = {
    val dau = session.groupBy("dt").agg(countDistinct(col("user_id")).as("dau"))
    val reg = users.filter(col("registered_ts").isNotNull)
      .groupBy(date_format(col("registered_ts"), "yyyy-MM-dd").as("dt"))
      .agg(countDistinct(col("user_id")).as("new_user_cnt_registered"))
      .filter(col("dt") >= sinceDt)
    val obs = users.filter(col("first_seen_dt").isNotNull && col("source") =!= "seed")
      .groupBy(col("first_seen_dt").as("dt"))
      .agg(countDistinct(col("user_id")).as("new_user_cnt_observed"))
      .filter(col("dt") >= sinceDt)

    dau.join(reg, Seq("dt"), "full_outer").join(obs, Seq("dt"), "full_outer")
      .na.fill(0)
      .withColumn("new_user_gap", col("new_user_cnt_registered") - col("new_user_cnt_observed"))
  }

  // ---------------- Q-02 / Q-03（质量与风控） ----------------

  private def qualityDaily(consume: DataFrame, items: DataFrame): DataFrame =
    consume.join(broadcast(items.select("item_id", "content_len")), Seq("item_id"), "left")
      .groupBy("dt", "category", "author_id").agg(
        count(lit(1)).as("consume_cnt"),
        sum(when(col("duration_sec").between(0.2, 1.2), 1).otherwise(0)).as("bounce_cnt"),
        sum(when(col("duration_sec") < 0.8, 1).otherwise(0)).as("short_stay_cnt"),
        sum(when(col("duration_sec") < 0.8 && col("content_len") > 2000, 1).otherwise(0))
          .as("long_content_short_stay_cnt"),
        sum(when(col("duration_sec").between(1800, 3600), 1).otherwise(0)).as("extended_stay_cnt"))

  // ---------------- F-08（位置） ----------------

  private def positionDaily(exposure: DataFrame, clicks: DataFrame): DataFrame = {
    val e = exposure.groupBy("dt", "channel", "position")
      .agg(count(lit(1)).as("exposure_cnt"), avg(col("rank_score")).as("avg_rank_score"))
    val c = clicks.groupBy("dt", "channel", "position").agg(count(lit(1)).as("click_cnt"))
    e.join(c, Seq("dt", "channel", "position"), "full_outer")
      .na.fill(0, Seq("exposure_cnt", "click_cnt"))
      .withColumn("ctr", ratio("click_cnt", "exposure_cnt"))
  }

  // ---------------- U-03（留存） ----------------

  private def retentionDaily(spark: SparkSession, sessionAll: DataFrame, users: DataFrame,
                             sinceDt: String, maxDt: String): DataFrame = {
    import org.apache.spark.sql.Encoders
    // 注册用户以注册日为 cohort；种子/推断用户以首见日为 cohort。
    val cohort = users.select(
      col("user_id"),
      when(col("source") === "registered" && col("registered_ts").isNotNull,
        date_format(col("registered_ts"), "yyyy-MM-dd"))
        .otherwise(col("first_seen_dt")).as("cohort_dt"),
      when(col("source") === "registered" && col("registered_ts").isNotNull, lit("registered"))
        .otherwise(lit("seed")).as("cohort_type"))
      .filter(col("cohort_dt").isNotNull)
    val cohortSize = cohort.groupBy("cohort_dt", "cohort_type")
      .agg(count(lit(1)).as("cohort_size"))
    val act = sessionAll.select(col("user_id"), col("dt")).distinct()
    val offsets = spark.createDataset(Seq(1, 3, 7))(Encoders.scalaInt).toDF("day_offset")

    val pairs = cohort
      .crossJoin(offsets)
      .withColumn("dt", date_format(date_add(to_date(col("cohort_dt")), col("day_offset")), "yyyy-MM-dd"))
      .filter(col("dt") >= sinceDt)
      .withColumn("is_mature", col("dt") <= lit(maxDt))

    // 左连接：零留存的 cohort 也要出行（否则"留存=0"会被静默吞掉，图表会缺格）
    pairs.join(act.withColumn("active", lit(1)), Seq("user_id", "dt"), "left")
      .groupBy("cohort_dt", "cohort_type", "dt", "day_offset", "is_mature")
      .agg(count(col("active")).as("retained_cnt"))
      .join(cohortSize, Seq("cohort_dt", "cohort_type"), "left")
      .withColumn("retention_rate", ratio("retained_cnt", "cohort_size"))
  }

  // ---------------- A-04（冷启动 24h） ----------------

  private def coldStartDaily(publish: DataFrame, exposure: DataFrame,
                             clicks: DataFrame, interaction: DataFrame): DataFrame = {
    val pubs = publish.select(col("dt"), col("item_id"), col("author_id"), col("category"),
      col("published_ts"))
    val exp = exposure.select(col("item_id"), col("ts"), col("user_id"))
    val inWindow = (ts: String, ref: String) =>
      col(ts) >= col(ref) && col(ts) <= col(ref) + expr("INTERVAL 24 HOURS")

    val e = pubs.join(exp, Seq("item_id"))
      .filter(inWindow("ts", "published_ts"))
      .groupBy("dt", "item_id").agg(
        count(lit(1)).as("exposure_cnt_24h"),
        countDistinct(col("user_id")).as("exposure_uv_24h"))

    val c = pubs.join(clicks.select(col("item_id"), col("ts")), Seq("item_id"))
      .filter(inWindow("ts", "published_ts"))
      .groupBy("dt", "item_id").agg(count(lit(1)).as("click_cnt_24h"))

    val i = pubs.join(interaction.select(col("item_id"), col("ts")), Seq("item_id"))
      .filter(inWindow("ts", "published_ts"))
      .groupBy("dt", "item_id").agg(count(lit(1)).as("interact_cnt_24h"))

    pubs.join(e, Seq("dt", "item_id"), "left")
      .join(c, Seq("dt", "item_id"), "left")
      .join(i, Seq("dt", "item_id"), "left")
      .na.fill(0, Seq("exposure_cnt_24h", "exposure_uv_24h", "click_cnt_24h", "interact_cnt_24h"))
      .withColumn("ctr_24h", ratio("click_cnt_24h", "exposure_cnt_24h"))
  }
}
