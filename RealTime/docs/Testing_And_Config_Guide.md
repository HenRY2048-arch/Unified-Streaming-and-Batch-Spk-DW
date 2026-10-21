# Mock 数据源 —— 测试流程与配置指南

> 归属：`/RealTime`（模拟数据源脚本）。
> 配套：链路与算法原理见 `Pipeline_And_Architecture.md`；权威契约见 `project_specs/mock_generator/Mock_Source_Spec.md`。
> 版本：2026-10-16

---

## 1. 前置条件

| 项 | 要求 |
|---|---|
| 系统 | WSL2（Ubuntu）/ Linux；Docker Desktop 可用 |
| JDK | 17（脚本编译目标 1.8，用 17 构建没问题） |
| Maven | 3.9+（仓库内 `RealTime/settings.xml` 已配阿里云镜像） |
| 端口 | `9092`（Kafka EXTERNAL）空闲 |

> 若在受限沙箱里跑 `docker` 可能因权限被拒；本机正常 shell 直接可用（必要时把用户加入 `docker` 组）。

---

## 2. 启动中间件（Kafka）

在仓库根目录执行：

```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
```

`crt-kafka` 状态为 `healthy` 即就绪。建 topic（脚本也会自动建，手动建可固定分区数）：

```bash
docker exec crt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic content_events --partitions 3 --replication-factor 1
docker exec crt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic content_groundtruth --partitions 3 --replication-factor 1
```

---

## 3. 构建

```bash
cd RealTime
mvn -s settings.xml -B clean package
```

产物：`RealTime/target/mock-generator-1.0-SNAPSHOT.jar`。

---

## 4. 启动脚本

```bash
cd RealTime
java -jar target/mock-generator-1.0-SNAPSHOT.jar
```

启动会打印配置摘要，便于确认参数已生效：

```
[Config] bootstrap=localhost:9092 topic=content_events truthTopic=content_groundtruth strategy=hybrid costTier=standard loadTier=steady
Generator is running. loadTier=steady
```

前台跑：`Ctrl+C` 停止（触发收尾钩子，打印 `[Cost] …`）。后台跑：

```bash
nohup java -jar target/mock-generator-1.0-SNAPSHOT.jar > generator.log 2>&1 &
ps -ef | grep mock-generator | grep -v grep
kill -15 <PID>
```

---

## 5. 验证

条数（各分区最新 offset 之和）：

```bash
docker exec crt-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic content_events
docker exec crt-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic content_groundtruth
```

抽样主事件 / 真值：

```bash
docker exec crt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic content_events --from-beginning --max-messages 5 --timeout-ms 10000
docker exec crt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic content_groundtruth --from-beginning --max-messages 5 --timeout-ms 10000
```

看事件构成（先落盘再统计）：

```bash
docker exec crt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic content_events --from-beginning --max-messages 5000 --timeout-ms 20000 > /tmp/events.jsonl
grep -o '"event_type":"[a-z_]*"' /tmp/events.jsonl | sort | uniq -c | sort -rn
grep '"event_type":"exposure_batch"' /tmp/events.jsonl | grep -o '"channel":"[a-z_]*"' | sort | uniq -c
```

看会话是否成链：

```bash
sid=$(grep -m1 '"event_type":"app_open"' /tmp/events.jsonl | grep -o '"session_id":"[^"]*"' | cut -d'"' -f4)
grep "$sid" /tmp/events.jsonl | grep -o '"event_type":"[a-z_]*"' | tr '\n' ' '; echo
```

期望形如：`app_open exposure_batch enter_item leave_item like … exposure_batch enter_item leave_item …`。

---

## 6. 重置与清理

清空 topic（保留结构）：

```bash
docker exec crt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --delete --topic content_events
docker exec crt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --delete --topic content_groundtruth
sleep 3
docker exec crt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic content_events --partitions 3 --replication-factor 1
docker exec crt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic content_groundtruth --partitions 3 --replication-factor 1
```

重置种子（换规模/换参数后想让种子重算）：

```bash
rm -rf RealTime/data_files
```

停中间件：

```bash
docker compose -f infra/docker-compose.yml down
docker compose -f infra/docker-compose.yml down -v
```

> 种子只要 `.seed_version` 与代码里的 `SEED_VERSION` 一致就会**复用**；改种子算法后**务必删 `data_files`**，否则会读到旧格式。

---

## 7. 配置机制

优先级（高 → 低）：

```
-Dkey=value               命令行单项覆盖（最灵活，便于做实验）
  > -Dconfig.file=<path>  整份外部配置文件
    > classpath:config.properties（RealTime/src/main/resources/config.properties，随 jar 打包）
      > 代码内置默认值
```

三种用法：

```bash
java -jar target/mock-generator-1.0-SNAPSHOT.jar
java -Dmock.load.tier=ramp -Dmock.distribution.strategy=random -jar target/mock-generator-1.0-SNAPSHOT.jar
java -Dconfig.file=/abs/path/pressure_peak.properties -jar target/mock-generator-1.0-SNAPSHOT.jar
```

---

## 8. 配置项全表

### 8.1 连接与目录

| key | 默认 | 说明 |
|---|---|---|
| `kafka.bootstrap.servers` | `localhost:9092` | Kafka 地址（容器 `crt-kafka` 的 EXTERNAL） |
| `kafka.topic` | `content_events` | 主事件流 topic |
| `kafka.acks` | `1` | 生产者 acks |
| `mock.truth.topic` | `content_groundtruth` | 归因真值 topic |
| `data.dir` | `data_files` | 种子输出目录（相对工作目录或绝对路径） |
| `mock.schema.version` | `v1` | 事件契约版本（写进每条事件） |

### 8.2 规模与种子

| key | 默认 | 说明 |
|---|---|---|
| `mock.user.count` | `1000` | 用户数 |
| `mock.item.initial.count` | `500` | 初始内容数（5 类均分） |
| `mock.tag.coverage` | `0.8` | 有画像用户占比（其余冷启动） |
| `mock.creator.ratio` | `0.15` | 创作者占比 |
| `mock.graph.avg.outdegree` | `10` | 图种子平均出度 |
| `mock.graph.cross.cluster.ratio` | `0.08` | 跨簇边占比 |

### 8.3 分发

| key | 默认 | 说明 |
|---|---|---|
| `mock.distribution.strategy` | `hybrid` | `hybrid` / `random` / `interest` / `graph`（后三者锁定单一渠道） |
| `mock.cost.tier` | `standard` | `cheap` / `standard` / `heavy`（重档多 3 倍采样再排序） |
| `mock.batch.size` | `8` | 一次推送的候选数 |
| `mock.emit.exposure` | `true` | 是否产 `exposure_batch`（关闭可显著降量） |
| `mock.batch.rank.score` | `true` | 批次候选是否带 `rank_score` |
| `mock.channel.weight.recommend` | `0.45` | 渠道基线权重（recommend） |
| `mock.channel.weight.follow` | `0.35` | 渠道基线权重（follow_feed）；explore 为余量 |

### 8.4 会话 / 点击 / 停留 / 互动

| key | 默认 | 说明 |
|---|---|---|
| `mock.session.max.steps` | `12` | 会话最大步数 |
| `mock.click.base` | `0.55` | 位置 0 的点击概率 |
| `mock.click.position.decay` | `0.55` | 每后移一位的衰减系数 |
| `mock.click.interest.boost` | `1.6` | 兴趣匹配的点击加成 |
| `mock.dwell.base.seconds` | `2.0` | 停留基础秒数 |
| `mock.dwell.chars.per.sec.min` / `.max` | `45` / `110` | 阅读速度区间（字符/秒） |
| `mock.engage.like` / `.fav` / `.share` / `.comment` / `.dislike` | `0.06` / `0.03` / `0.012` / `0.006` / `0.010` | 每次消费的互动概率 |
| `mock.follow.prob` | `0.02` | 从"正在看的内容"关注作者的概率 |
| `mock.unfollow.prob` | `0.004` | 取关概率 |
| `mock.item.publish.rate` | `0.01` | 每次迭代触发"作者产出"的概率 |

### 8.5 压力档位

| key | 默认 | 说明 |
|---|---|---|
| `mock.load.tier` | `steady` | `steady` / `peak` / `ramp` / `skew` |
| `mock.eps.steady.min` / `.max` | `30` / `50` | 稳态每秒生成量 |
| `mock.eps.peak.min` / `.max` | `150` / `200` | 洪峰每秒生成量 |
| `mock.peak.interval.minutes` | `10` | 洪峰周期 |
| `mock.peak.duration.seconds` | `30` | 洪峰持续 |
| `mock.eps.ramp.start` / `.end` / `mock.eps.ramp.minutes` | `30` / `200` / `10` | 斜坡档 |
| `mock.eps.skew.base.min` / `.max` | `20` / `40` | 倾斜档基线 |
| `mock.eps.skew.burst.min` / `.max` / `mock.eps.skew.burst.prob` | `300` / `600` / `0.02` | 倾斜档突发 |

### 8.6 模拟时钟与新用户注入（P1.6 / 契约 v2）

| key | 默认 | 说明 |
|---|---|---|
| `mock.clock.think.min.seconds` / `.max` | `2` / `20` | 会话内思考时间（事件秒），推进用户时钟 |
| `mock.clock.file` | `data_files/.sim_clock` | 事件时间落盘：重启从 `max(真实现在, 落盘值)` 续跑，**不倒退** |
| `mock.clock.resume` | `true` | 关掉则重启回到真实现在（不推荐，会造成事件时间回退） |
| `mock.session.max.open` | `30` | **并发会话上限**（时钟对齐关键参数：并发越多，单会话越久才轮到下一步） |
| `mock.session.timeout.minutes` | `30` | 会话超时（事件时间），同时作为"过期会话收口"阈值 |
| `mock.session.resume.weight` | `0.85` | 已有会话的推进权重（越低越常开新会话） |
| `mock.user.registered.per.day` | `30` | 新用户注入速率（**事件天**口径）；`0` = 关闭 |
| `mock.user.first.open.delay.min.minutes` / `.max` | `0` / `180` | 注册 → 首次打开的间隔（U-03 次留基线） |
| `mock.user.id.inject.base` | `300001` | 注入用户 id 起始段（种子 100001–101000 之外） |
| `mock.user.injected.file` | `data_files/dim_user_injected.csv` | 已注入用户 append 落盘；**重启不重发 id** |

**验证口诀（实测口径）**：跑 45s → 消费 topic →
① `user_registered` 条数应等于 `dim_user_injected.csv` 行数（曾因提前 return 丢过一半）；
② `leave_ts − enter_ts` 与 `duration_sec` 之差中位应为毫秒级（最大 90s = B1 乱序上限）；
③ 事件时间回退 p95 应在 20 分钟内；④ 每事件天约 1,200–2,000 条事件（1 事件天 ≈100 真实秒）。

---

## 9. 常见测试配方

| 目的 | 命令（在 `RealTime/` 下） |
|---|---|
| 只验证契约字段 | `java -Dmock.emit.exposure=true -jar target/mock-generator-1.0-SNAPSHOT.jar` 跑 10s 后抽样 |
| 降量快跑 | `java -Dmock.emit.exposure=false -Dmock.item.publish.rate=0 -jar target/mock-generator-1.0-SNAPSHOT.jar` |
| 稳态 / 洪峰压测 | `java -Dmock.load.tier=peak -jar target/mock-generator-1.0-SNAPSHOT.jar` |
| **跨天窗口**（A-04 冷启动 / U-03 留存 / R-05 粉丝活跃） | 跑 **≥3 分钟**（默认约 1 事件天/100 秒）；核对事件时间跨度 > 1 天、每天 1k+ 事件 |
| **新用户链路** | `java -Dmock.user.registered.per.day=60 -jar target/mock-generator-1.0-SNAPSHOT.jar` 跑 45s；核对 `dim_user_injected.csv` 行数 == topic 里 `user_registered` 条数 |
| **时钟自检**（改时钟/并发参数后必跑） | 跑 45s，按 §8.6 的"验证口诀"核对：停留自洽（中位毫秒级）、回退 p95（<20 分钟）、注册一致性 |
| 斜坡增长 | `java -Dmock.load.tier=ramp -jar target/mock-generator-1.0-SNAPSHOT.jar` |
| 突发热点 | `java -Dmock.load.tier=skew -jar target/mock-generator-1.0-SNAPSHOT.jar` |
| 纯探索策略 | `java -Dmock.distribution.strategy=random -jar target/mock-generator-1.0-SNAPSHOT.jar` |
| 分发成本对比 | 分别用 `cheap` / `standard` / `heavy` 跑，比较收尾 `[Cost]` 的 `rankMs` |
| 放大规模 | `java -Dmock.user.count=3000 -Dmock.item.initial.count=1500 -jar target/mock-generator-1.0-SNAPSHOT.jar`（先删 `data_files`） |
| 生成全量用户种子 | 删 `data_files` 后跑几秒（种子在启动时一次性生成） |

---

## 10. 故障排查

| 现象 | 排查 |
|---|---|
| `Connection refused` / 一直重连 | 容器是否在跑；9092 是否被占 |
| 宿主进程连不上容器 Kafka | EXTERNAL 的 `advertised.listeners` 必须是 `localhost:9092`（见 `infra/docker-compose.yml`） |
| 种子字段对不上 | `.seed_version` 与代码 `SEED_VERSION` 不一致 → **删 `data_files`** 重新生成 |
| `Failed to load dimension data` | 看 `data.dir` 是否可写；首次运行会先种种子 |
| 事件量比预期小 | 是否关了 `mock.emit.exposure`；压力档是否 `steady` |
| 渠道占比不是 45/35/20 | 正常：空候选池会回落 `explore`，成熟度上升会提高关注流权重 |
| 停留时长全是 0 | 不应发生（已改为采样）；若出现，检查是否在用**旧 jar** |
| 日志太吵 | 已内置 `resources/simplelogger.properties` 降到 WARN |
| `docker` 权限被拒 | 把当前用户加入 `docker` 组并重新登录，或用 `sudo` |

---

## 11. 命令速查

```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml down
cd RealTime && mvn -s settings.xml -B clean package
cd RealTime && java -jar target/mock-generator-1.0-SNAPSHOT.jar
docker exec crt-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic content_events
docker exec crt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic content_events --from-beginning --max-messages 5 --timeout-ms 10000
rm -rf RealTime/data_files
```
