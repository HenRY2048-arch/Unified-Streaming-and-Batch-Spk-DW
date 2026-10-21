package com.hub.batch

import com.hub.common._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 维度层（批）：全部**全量重建 + 幂等覆盖**。
 *
 * 量级很小（千级用户/内容、万级边），全量重建换来的是"任何一次重跑都得到同一结果"，
 * 比增量 MERGE 更好调试；等真的到百万级再换 MERGE（接口不变）。
 *
 * 三条语义：
 *  - `dim_user`：**快照 ∪ 事件**。`registered_ts` 是注册真值，`first_seen_ts = min(ts)` 是观测真值，
 *    且 **只前进不回退**（`least(新, 旧)`）——迟到数据不会把"首次出现"改晚。
 *  - `dim_item`：**只追加**（种子 + `item_published`），不做 SCD2。
 *  - `dim_edge`：种子边 + `follow/unfollow` 重建，带 `valid_from/valid_to`（`valid_to=null` 表示仍有效）。
 */
object DimBuilder {

  private def seed(name: String): String = s"${Config.seedDir}/$name"

  def buildAll(spark: SparkSession): Unit = {
    buildUser(spark)
    buildItem(spark)
    buildEdge(spark)
  }

  // ---------------- dim/user ----------------

  private def buildUser(spark: SparkSession): Unit = {
    // 与 DWD 同一纪律：按 event_id 去重（重放会产生重复事件）
    val ods = DeltaIO.read(spark, Lake.odsEvents).dropDuplicates("event_id")

    val seedUsers = Contract.readSeed(spark, seed("dim_user_profile.csv"), Contract.seedUserSchema)
      .withColumn("interest_tags",
        when(col("interest_tags").isNull || col("interest_tags") === "", array().cast("array<string>"))
          .otherwise(split(col("interest_tags"), ",")))
      .withColumn("source", lit("seed"))
      .withColumn("registered_ts", lit(null).cast("timestamp"))

    val registered = ods.filter(col("event_type") === "user_registered")
      .select(col("user_id"), col("interest_tags"), col("level"), col("is_creator"),
        col("primary_category"), col("post_rate_level"),
        lit("registered").as("source"), col("event_ts").as("registered_ts"))

    // 同一 user_id 以"注册事件"优先，其次种子
    val attrs = registered.unionByName(seedUsers)
      .withColumn("_pref", when(col("source") === "registered", 0).otherwise(1))
      .withColumn("_rn", row_number().over(
        Window.partitionBy("user_id").orderBy(col("_pref"), col("registered_ts").desc_nulls_last)))
      .filter(col("_rn") === 1)
      .drop("_rn", "_pref")

    val observed = ods.groupBy("user_id").agg(min("event_ts").as("obs_fs"))

    // 旧表的 first_seen 必须参与"取最早"，否则迟到数据会把首次出现改晚
    val existing = DeltaIO.readIfExists(spark, Lake.dimUser)
      .map(_.select(col("user_id"), col("first_seen_ts").as("old_fs")))
      .getOrElse(spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(Seq(StructField("user_id", LongType), StructField("old_fs", TimestampType)))))

    val out = attrs
      .join(observed, Seq("user_id"), "full_outer")
      .join(existing, Seq("user_id"), "left")
      .withColumn("source", coalesce(col("source"), lit("inferred")))
      .withColumn("first_seen_ts",
        coalesce(least(col("obs_fs"), col("old_fs")), col("obs_fs"), col("old_fs")))
      .withColumn("first_seen_dt", date_format(col("first_seen_ts"), "yyyy-MM-dd"))
      .withColumn("updated_dt", current_date())
      .select("user_id", "interest_tags", "level", "is_creator", "primary_category", "post_rate_level",
        "source", "registered_ts", "first_seen_ts", "first_seen_dt", "updated_dt")

    DeltaIO.write(out, Lake.dimUser, Nil, "overwrite")
    println(s"[DIM] user rows=${out.count()}")
  }

  // ---------------- dim/item ----------------

  private def buildItem(spark: SparkSession): Unit = {
    val ods = DeltaIO.read(spark, Lake.odsEvents)

    val seedItems = Contract.readSeed(spark, seed("dim_item.csv"), Contract.seedItemSchema)
      .withColumn("tags", when(col("tags").isNull || col("tags") === "", array().cast("array<string>"))
        .otherwise(split(col("tags"), ",")))
      .withColumn("source", lit("seed"))

    val published = ods.filter(col("event_type") === "item_published")
      .select(col("item_id"), col("author_id"), col("category"),
        split(col("tags"), ",").as("tags"), col("content_len"),
        col("published_ts"), lit("published").as("source"))

    val out = seedItems.unionByName(published)
      .withColumn("_pref", when(col("source") === "seed", 0).otherwise(1))
      .withColumn("_rn", row_number().over(Window.partitionBy("item_id").orderBy("_pref")))
      .filter(col("_rn") === 1)
      .drop("_rn", "_pref")
      .select("item_id", "author_id", "category", "tags", "content_len", "published_ts", "source")

    DeltaIO.write(out, Lake.dimItem, Nil, "overwrite")
    println(s"[DIM] item rows=${out.count()}")
  }

  // ---------------- dim/edge ----------------

  private def buildEdge(spark: SparkSession): Unit = {
    val ods = DeltaIO.read(spark, Lake.odsEvents)

    val seedEdges = Contract.readSeed(spark, seed("dim_follow_seed.csv"), Contract.seedEdgeSchema)
      .select(col("follower"), col("followee"), col("ts").cast("timestamp").as("ts"), lit("open").as("kind"))

    val follows = ods.filter(col("event_type") === "follow")
      .select(col("user_id").as("follower"), col("target_user_id").as("followee"),
        col("event_ts").as("ts"), lit("open").as("kind"))

    val unfollows = ods.filter(col("event_type") === "unfollow")
      .select(col("user_id").as("follower"), col("target_user_id").as("followee"),
        col("event_ts").as("ts"), lit("close").as("kind"))

    val all = seedEdges.unionByName(follows).unionByName(unfollows)
    val w = Window.partitionBy("follower", "followee").orderBy("ts")

    val out = all
      .withColumn("next_ts", lead(col("ts"), 1).over(w))
      .withColumn("next_kind", lead(col("kind"), 1).over(w))
      .filter(col("kind") === "open")
      .withColumn("valid_from", col("ts"))
      .withColumn("valid_to", when(col("next_kind") === "close", col("next_ts"))
        .otherwise(lit(null).cast("timestamp")))
      .select("follower", "followee", "valid_from", "valid_to")
      .dropDuplicates("follower", "followee", "valid_from")

    DeltaIO.write(out, Lake.dimEdge, Nil, "overwrite")
    println(s"[DIM] edge rows=${out.count()}")
  }
}
