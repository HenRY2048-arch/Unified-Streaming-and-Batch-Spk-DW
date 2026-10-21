package com.hub.stream

import com.hub.common.Config
import redis.clients.jedis.Jedis

/**
 * Tier0 的 Redis 写入口（**缓存，不是真值**）。
 *
 * 与旧实现的两个区别（旧版是反面教材）：
 *  1) 只写"聚合后的少量行"（热度 top-N、在线人数），**不 collect 明细**；
 *  2) 走 pipeline 批量写，避免逐行逐字段的同步往返。
 *
 * 语义：key 带 TTL（默认 15 分钟），过期即回源重算——Redis 丢数据不是数据丢失。
 */
object RedisSink {

  private def withJedis[T](f: Jedis => T): T = {
    val jedis = new Jedis(Config.redisHost, Config.redisPort)
    try f(jedis) finally jedis.close()
  }

  /** 热度：每个 item 一个 hash + 一个排行榜 zset。 */
  def writeHeat(rows: Seq[(Long, Double, Double)], computedAt: Long): Unit = withJedis { jedis =>
    val pipe = jedis.pipelined()
    rows.foreach { case (itemId, heat1h, heat24h) =>
      val key = s"${Config.redisKeyPrefix}heat:$itemId"
      pipe.hset(key, "heat_1h", f"$heat1h%.4f")
      pipe.hset(key, "heat_24h", f"$heat24h%.4f")
      pipe.hset(key, "computed_at", computedAt.toString)
      pipe.expire(key, Config.redisTtlSeconds)
      pipe.zadd(s"${Config.redisKeyPrefix}heat:top", heat1h, itemId.toString)
    }
    pipe.expire(s"${Config.redisKeyPrefix}heat:top", Config.redisTtlSeconds)
    pipe.sync()
  }

  /** 在线活跃：近 N 分钟有会话的用户数。 */
  def writeOnline(activeUsers: Long, computedAt: Long): Unit = withJedis { jedis =>
    val key = s"${Config.redisKeyPrefix}online"
    val pipe = jedis.pipelined()
    pipe.hset(key, "active_users", activeUsers.toString)
    pipe.hset(key, "window_minutes", Config.onlineMinutes.toString)
    pipe.hset(key, "computed_at", computedAt.toString)
    pipe.expire(key, Config.redisTtlSeconds)
    pipe.sync()
  }
}
