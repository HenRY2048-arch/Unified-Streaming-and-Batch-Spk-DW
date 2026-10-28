# 整体链路搭建、运行与调试手册

> 地位：项目的**部署 / 运行 / 排错**主文档。口径以 `Metric_Definitions.md` 为准，门禁以 `Gate_Grading_Scheme.md` 为准，表结构以 `Warehouse_Tables.md` 为准。
> 版本：v1.2（2026-10-28）｜状态：**P3.9 已完成新端到端验收；10-29 Gate 为 pass_with_limitations**

---

## 0. 一页图

```
[宿主 WSL2：命令入口]
  bootstrap_runtime.sh / run_warehouse.sh / observe_warehouse.sh
  Maven 构建产物 → target/crt-warehouse-0.1.0.jar

[Docker]
  Mock Generator 容器 ──► kafka:19092
  Airflow/PG 容器 ──► Docker socket ──► Spark 一次性容器
                                                     │
                                                     ▼
[Docker: crt-kafka] ──► [Spark: 流①] ──► ods/events（证据层）
        │                    │
        │                    └──────────► ods/groundtruth（[真相层]）
        │                                      │
        │                                      │ 只用于 V-01，不参与指标计算
        │                                      ▼
        │                              verify/attribution
        │                                      ▲
        │                                      │ dwd/interaction + dwd/relation
        │                                      │
        ├──► [Spark: 流②] Tier0（输入只读 ods/events；每微批重算最近 24h）
        │       ├─ F-07 HeatMath 1h/24h → tier0/heat_snapshot + Redis heat
        │       ├─ Q-01 A1/A2 → tier0/cheat_events（跨批可重复，查询去重）
        │       └─ U-04 近15min distinct user → Redis crt:online
        │                                      │
        │                                      ▼
        │                              verify/heat（V-02）
        │
        └──► [Spark: 批] DIM → DWD → DWS → semantic/metric_dict
                                      │
                                      ▼
                             GateApp（20 条 Gate）
                                      │
                    ┌─────────────────┼──────────────────┐
                    ▼                 ▼                  ▼
            ops/run_manifest   ops/partition_status   ops/reports
                                      │
                                      ▼
                             看板 / data agent（外部只读）

[存储] crt-s3 / SeaweedFS：s3a://crt-lake/（所有 Delta 表）
[缓存] crt-redis：Tier0 秒级缓存（非真值，TTL 900s）
[控制面] Airflow：只调度批/对账/Gate；StreamApp 常驻，不塞进每日 DAG
```

核心分工：**流负责“此刻的近似”，批负责“最终真值”，Gate 负责“这批数据能不能用”**。data agent 不在这条链路里，只读数仓的读契约。

---

## 1. 组件装在哪里

### 1.1 宿主（WSL2）

| 组件 / 文件 | 位置 | 用途 |
|---|---|---|
| 仓库根 | `/home/henry/projects/Community_Real_Time` | 源码、文档、脚本 |
| 数仓薄 jar | `target/crt-warehouse-0.1.0.jar` | `spark-submit` 的 `--class` 入口 |
| 运行期依赖 | `infra/jars/*.jar` | Delta / Hadoop-AWS / Kafka / Jedis 等，容器只读挂载 |
| 生成器 | `RealTime/` | `mvn -s settings.xml -B clean package` 后运行 fat jar |
| 生成器种子/状态 | `RealTime/data_files/` | 种子 CSV、`.sim_clock`、`dim_user_injected.csv` |
| 启动脚本 | `infra/scripts/run_warehouse.sh`、`run_shell.sh` | 统一 jar 列表、驱动内存、Spark 容器入口 |
| Airflow DAG | `infra/airflow/dags/` | `crt_daily_batch.py`、`crt_weekly_vacuum.py` |
| Airflow venv | `.airflow-venv/`（gitignore） | Python 3.11 + Airflow 2.10.5 |
| Airflow 运行状态 | Docker volumes + `infra/airflow/logs/` | Postgres 元数据库、日志、Web UI/scheduler 状态 |
| Airflow 启停 | `infra/airflow/run_local.sh`、`stop_local.sh` | 本机 Web UI `8080` / scheduler |
| 环境编排 | `infra/docker-compose.yml` | Kafka / Redis / SeaweedFS / 按需 Spark |

### 1.2 Docker 容器

| 容器 | 镜像 | 端口 | 卷 / 关键点 |
|---|---|---|---|
| `crt-kafka` | `apache/kafka:4.1.0` | `9092` EXT / `19092` INT / `9093` CTRL | `kafka-data`；宿主生成器连 `localhost:9092`，Spark 容器连 `kafka:19092` |
| `crt-redis` | `redis/redis-stack-server:latest` | `6379` | `redis-data`；**仅 Tier0 缓存**，maxmemory 512MB / allkeys-lru |
| `crt-s3` | `chrislusf/seaweedfs:latest` | `8333` S3 / `9333` master / `8888` filer | `seaweedfs-data`；S3 path-style，lake root `s3a://crt-lake/` |
| `crt-spark-run-*` | `apache/spark:3.5.3` | 临时 | `run_warehouse.sh` 用 `compose run --rm` 创建；挂载 `RealTime`、`infra`、`target` |

Spark 容器内的挂载点：

```
/opt/crt/RealTime      ← 宿主 RealTime（种子、生成器）
/opt/crt/infra         ← 宿主 infra（jar、脚本、compose）
/opt/crt/warehouse     ← 宿主 target（数仓薄 jar，只读）
```

### 1.3 湖上的目录

| 路径 | 写入方 | 说明 |
|---|---|---|
| `s3a://crt-lake/ods/events/` | 流① | 契约事件，含嵌套 `items` |
| `s3a://crt-lake/ods/groundtruth/` | 流① | 归因真值，独立 topic |
| `s3a://crt-lake/tier0/heat_snapshot/` | 流② | 热度快照，供 V-02；append-only |
| `s3a://crt-lake/tier0/cheat_events/` | 流② | A1/A2 作弊轨迹；append-only，查询去重 |
| `s3a://crt-lake/tier0/cheat_events/` | 流② | A1/A2 作弊轨迹 |
| `s3a://crt-lake/dim/{user,item,edge}/` | 批 | 维表 |
| `s3a://crt-lake/dwd/{exposure,click,consume,interaction,relation,publish,session}/` | 批 | 明细 |
| `s3a://crt-lake/dws/*` | 批 | 11 张主题表 |
| `s3a://crt-lake/verify/{attribution,heat}/` | 批 | V-01/V-02 |
| `s3a://crt-lake/semantic/metric_dict/` | 批 | 40 指标字典 + `limitations` |
| `s3a://crt-lake/ops/run_manifest/` | GateApp | 每次 Gate 执行的清单 |
| `s3a://crt-lake/ops/partition_status/` | GateApp | `dt × table` 的信任状态 |
| `s3a://crt-lake/ops/reports/run_id=.../` | GateApp | `gate-report.json` / `gate-report.md` |

### 1.4 Tier0 到底是什么

**Tier0 不是一张表，也不是 Kafka 消费者，而是流①之后的第二个 Spark Structured Streaming 作业（流②）**。
它只读 `ods/events`，做三类秒级服务计算，输出到 Redis 和/或 Delta：

```
                         ┌──────────────────────────────────────────────┐
                         │ Tier0（流②，Spark Structured Streaming）      │
                         │ 输入：ods/events（不直读 Kafka）              │
                         │ 当前实现：每个微批重算最近 24h（非状态化增量）  │
                         └───────────────┬──────────────────────────────┘
                                         │
          ┌──────────────────────────────┼──────────────────────────────┐
          ▼                              ▼                              ▼
 F-07 内容热度                    Q-01 作弊判定                  U-04 在线活跃
 HeatMath 1h / 24h               A1: 5 连评                    近 15 分钟有事件的
 曝光0.1/点击1/赞3/藏4/评6/转8   A2: 5s 内 15 赞且 ≥10 内容     distinct user_id
 停留 0.02×秒（单段上限 30）       │                              │
          │                              │                              │
          ├─► tier0/heat_snapshot         └─► tier0/cheat_events         └─► Redis
          │   （Delta，供 V-02）              （Delta，append-only）         crt:online
          └─► Redis: crt:heat:<id> / crt:heat:top（TTL 900s，缓存）
```

| 问题 | 答案 |
|---|---|
| Tier0 的输入是什么？ | 只读 **ODS/events**，不是 Kafka；这样批与流同源，V-02 比的是纯算法差 |
| 为什么叫 Tier0？ | 指“秒级服务层”，不是指标分级里的单独存储层；承载 **F-07 / Q-01 / U-04** |
| 输出在哪里？ | 热度快照 `tier0/heat_snapshot`、作弊轨迹 `tier0/cheat_events`、Redis 缓存 |
| 什么是真值？ | 批侧 DWS / HeatMath 重算才是真值；Tier0 是近似，Redis 只是缓存 |
| 刷新方式？ | 当前每个微批重算最近 24h；不是状态化增量，量级大时应改为增量状态 |
| Tier0 失败会怎样？ | 批链路仍可跑；Redis 可能过期、热度快照缺失；V-02 记 limitation，不把 Redis 当真值 |
| 作弊轨迹会重复吗？ | 会跨微批重复 append；查询/门禁必须按 `rule + user_id + item_id + window_start` 去重 |

---

## 2. 启动、停止与构建

### 2.1 构建数仓

```bash
mvn -o -s RealTime/settings.xml -B -DskipTests package
# → target/crt-warehouse-0.1.0.jar
```

首次需要联网拉依赖；之后 `-o` 可离线。Spark/Delta/Kafka 连接器是 `provided`，运行时由 `run_warehouse.sh` 的 `--jars` 提供。

### 2.2 起中间件

```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
```

**启动顺序**：Kafka / Redis / SeaweedFS 先起，再起生成器或 Spark。`crt-s3` 不在运行时的典型表现是“写 S3 卡住”，不是立刻报错。

### 2.3 起生成器

```bash
cd RealTime
mvn -s settings.xml -B clean package
java -jar target/mock-generator-1.0-SNAPSHOT.jar
```

生成器默认以容器运行，配置 `RealTime/src/main/resources/config.properties`；profile 通过
`CRT_GENERATOR_PROFILE` 选择，`-Dkey=value` 仍可覆盖。它会持续写 `content_events` 与 `content_groundtruth`。

### 2.4 起流作业

```bash
infra/scripts/run_warehouse.sh com.hub.app.StreamApp --reset
```

一个 Spark 应用跑三条查询：Kafka→ODS events、Kafka→ODS truth、ODS→Tier0。`--reset` 只清本作业 checkpoint；若上次被 kill，先确认没有残留 `crt-spark-run-*`。

### 2.5 停止

```bash
docker compose -f infra/docker-compose.yml down      # 保留卷
# 不要随意 down -v；会清空 Kafka/Redis/S3 数据卷
```

---

## 3. 端到端 runbook

### 3.1 每日批（当前可手工执行）

```bash
# 1) 维度 + 明细 + 主题 + 语义（按最近 N 个事件天）
infra/scripts/run_warehouse.sh com.hub.app.BatchApp --step all --days 3

# 2) 对账
infra/scripts/run_warehouse.sh com.hub.app.VerifyApp --days 3

# 3) Gate（目标完整事件天；历史日缺水位会记 G2）
infra/scripts/run_warehouse.sh com.hub.app.GateApp --stage all --dt 2026-10-18 --days 3

# 4) 抽查信任表
infra/scripts/run_shell.sh /opt/crt/infra/scripts/verify_p3_batch5.scala
```

### 3.2 Airflow（Docker LocalExecutor + Postgres）

- DAG：`infra/airflow/dags/crt_daily_batch.py`，链路 `DR → dim → dwd → IV → dws → semantic → CV/QL → verify → RC → partition_status`。
- 维护：`crt_weekly_vacuum.py`，默认 `schedule=None`，人工触发。
- Airflow worker 需要能执行 `infra/scripts/run_warehouse.sh`；默认项目根由 `CRT_PROJECT_ROOT` 指定。
- 当前使用 Docker `LocalExecutor + Postgres`，Web UI 为 `http://localhost:8080`，账号 `admin/admin`。
- `docker compose -f infra/airflow/docker-compose.yml up -d` 启动；两个 DAG 当前暂停，避免自动触发 Spark。
- Airflow 容器通过 Docker socket 提交一次性 Spark 容器；已从 scheduler 容器内实际执行 `run_warehouse.sh` 验证。
- 宿主 venv + SQLite 的 `run_local.sh` 保留为备用调试方式。

### 3.3 VACUUM（不要放进每日主链）

```bash
infra/scripts/run_warehouse.sh com.hub.app.VacuumApp --hours 168
```

默认保留 7 天；先确认 `verify/*` 通过、`ops/partition_status` 没有 blocked，再人工触发。`replaceWhere`/`DELETE` 只改 Delta 日志，物理回收靠它。

---

## 4. 配置与优先级

| 配置 | 默认 | 说明 |
|---|---|---|
| `lake.root` | `s3a://crt-lake` | Delta 湖根 |
| `fs.s3a.endpoint` | `http://seaweedfs:8333` | 容器内 S3 端点；宿主是 `localhost:8333` |
| `fs.s3a.path.style.access` | `true` | SeaweedFS 必须 path-style |
| `kafka.bootstrap.servers` | `kafka:19092` | Spark 容器内 listener |
| `redis.host` / `redis.port` | `redis` / `6379` | Tier0 缓存 |
| `batch.recompute.days` | `2` | BatchApp 默认重算窗口 |
| `mock.data.dir` | `/opt/crt/RealTime/data_files` | Spark 容器内种子目录 |
| `gate.*` | `gate-v1` 默认 | Gate 阈值、等待、匹配率见 `warehouse.properties` |
| `vacuum.retention.hours` | `168` | VACUUM 保留期 |

优先级：classpath `warehouse.properties` → `-Dconfig.file=<path>` → 任意 `-Dkey=value`。

**容易踩的坑**：所有 `fs.*` 参数必须以 `spark.hadoop.` 前缀进入 SparkSession；直接传 `fs.s3a.endpoint` 会被静默忽略，S3A 会去连真实 AWS，在无网环境里挂住。

---

## 5. 表与信任

| 层 | 表 | 用途 |
|---|---|---|
| 证据 | `ods/events`、`ods/groundtruth` | 原始事件 / 真值；不去重、不展开 |
| 明细 | `dwd/*` | 曝光展开、点击、消费配对、互动、关系、发布、会话 |
| 主题 | `dws/*` | 漏斗/渠道/内容/作者/关系/会话/用户/质量/位置/留存/冷启动 |
| 语义 | `semantic/metric_dict` | 40 指标 + `nature` + `limitations` |
| 对账 | `verify/attribution`、`verify/heat` | V-01 精确率/召回率、V-02 热度偏差 |
| 信任 | `ops/run_manifest`、`ops/partition_status`、`ops/reports` | 运行清单、分区状态、人/机报告 |

`ops/partition_status` 是看板和 agent 的入口：查一行 `dt + table` 就知道能不能用、有什么限制；`ops/run_manifest` 保留每次 Gate 的原始值与处置。

---

## 6. 调试地图

### 6.1 容器 / 存储

| 症状 | 先查 | 处理 |
|---|---|---|
| 写 S3 卡住、不报错 | `docker compose ps` 是否有 `crt-s3` | `docker compose up -d seaweedfs` |
| 容器内解析不到 `seaweedfs` | `docker exec crt-spark-run-* getent hosts seaweedfs` | 确认在同一 compose 网络 |
| S3 鉴权失败 | `infra/seaweedfs/s3.json`（由 `s3.example.json` 复制）与 `warehouse.properties` | access/secret 对齐；path-style |
| Redis 读不到 | `docker exec crt-redis redis-cli ping` | 起 Redis；Tier0 TTL 900s 是正常过期 |
| Kafka 有 topic 没数据 | `kafka-get-offsets.sh` 看 offset | 生成器是否在跑；topic 名是否一致 |

### 6.2 Spark / checkpoint

| 症状 | 先查 | 处理 |
|---|---|---|
| `batch N doesn't exist` | 是否有残留 `crt-spark-run-*` | `docker rm -f` 残留；`StreamApp --reset` |
| `ClassNotFoundException: commons-pool2` | `run_warehouse.sh` 的 jar 列表 | 补 `infra/jars/commons-pool2-*.jar` |
| `NoSuchMethodError: ExpressionSet` | Spark/Delta 版本 | 必须 Spark 3.5.3 + Delta 3.2.1 |
| Spark UI 看不到 | `run_warehouse.sh` 是 `compose run`，默认不发布端口 | 加 `--service-ports` 或看 stdout/容器日志 |
| 想提高日志 | `run_warehouse.sh` 的 `--driver-java-options` | 加 `-Dspark.log.level=INFO` |

### 6.3 数据口径

| 症状 | 先查 | 说明 |
|---|---|---|
| IV-1 曝光不守恒 | ODS 是否重放重复 | Gate 对 ODS 先 `dropDuplicates("event_id")`；DWD 已是去重口径 |
| 点击列 `category` 解析失败 | `DwsBuilder` 的 `clicks` 投影 | 必须带 `author_id/category`；本次已修 |
| DR-3 一直 wait | `max(event_ts)` 与目标日 | 历史日缺水位 → G2；当前日才 G1 等待 |
| QL-1 严重偏离 | D1/E1 注入区间 | 两者重叠，按 20%/14%/0.5% 可观测总量看 |
| V-01 突然下降 | 交互类 `channel` | 必须由 `batch_id` 反查曝光批次 |
| V-02 无行 | 该 dt 是否有 Tier0 快照 | 无快照记 G3 limitation，不阻断批侧 |
| FX-2 文件/事件不一致 | `dim_user_injected.csv` 是累计文件 | ODS 受 Kafka 保留窗口影响；按 G4 观察 |
| 留存 0 | `is_mature` | 窗口未满时为 `false`，0 不代表真实零留存 |

### 6.4 常用检查脚本

```bash
infra/scripts/run_shell.sh /opt/crt/infra/scripts/check_ods.scala
infra/scripts/run_shell.sh /opt/crt/infra/scripts/verify_p3_batch2.scala
infra/scripts/run_shell.sh /opt/crt/infra/scripts/verify_p3_batch3.scala
infra/scripts/run_shell.sh /opt/crt/infra/scripts/verify_p3_batch4.scala
infra/scripts/run_shell.sh /opt/crt/infra/scripts/verify_p3_batch5.scala   # P3.8 信任表 + 对账
```

---

## 7. 本次验收记录（2026-10-20）

> 运行环境补充（2026-10-21 起）：Airflow 2.10.5 本机 venv 的上次验收入口为 `http://localhost:8080`，
> 使用 SQLite + SequentialExecutor；两个 DAG 已暂停，避免自动触发 Spark。

### 7.1 前置

- `crt-kafka`、`crt-redis` 已在运行；`crt-s3` 当时未运行，已启动 `seaweedfs`。
- `mvn -o -s RealTime/settings.xml -B -DskipTests package` 通过。
- DWS 首次因 `dwd/click` 投影漏 `category` 失败；修复后 3 天重建通过。

### 7.2 实际跑过的命令

```bash
infra/scripts/run_warehouse.sh com.hub.app.BatchApp --step all --days 3
infra/scripts/run_warehouse.sh com.hub.app.VerifyApp --days 3
infra/scripts/run_warehouse.sh com.hub.app.GateApp --stage all --dt 2026-10-18 --days 3
infra/scripts/run_shell.sh /opt/crt/infra/scripts/verify_p3_batch5.scala
```

### 7.3 行数

| 层 | 结果 |
|---|---|
| DIM | `user 1101 / item 538 / edge 10097` |
| DWD | `exposure 36,044 / click 4,202 / consume 3,185 / interaction 1,037 / relation 241 / publish 106 / session 1,920` |
| DWS | `funnel 826 / channel 160 / content 1,601 / author 416 / relation 82 / session 3 / user 3 / quality 347 / position 72 / retention 18 / cold_start 106` |
| 语义 | `metric_dict 40`；`limitations` 非 `—` 12 条 |

### 7.4 对账

| 指标 | 结果 |
|---|---|
| V-01 | `total=1252 / joined=1252 / matched=1252 / missing=0`；精确率、召回率均 1.0 |
| V-02 | `1,947` 行；`rel_diff` 中位/p95/max 均 0 |

### 7.5 Gate（目标 10-18）

| 检查 | 结果 |
|---|---|
| DR-1/2/4-01 | pass |
| DR-3 | `pass_with_limitations`（G2：历史日 max_event_ts=15:58，缺水位） |
| DR-4-02 | `pass_with_limitations`（G2：ODS 有重放重复） |
| IV-1/2-01/2-02/2-03 | pass |
| CV-1/2/3 | pass（40 个唯一指标） |
| QL-1-01 | pass（bounce 0.178 / short 0.127 / extended 0.004，n=901） |
| QL-2-01 | pass |
| QL-2-02 | `pass_with_limitations`（G2：渠道占比日间 max_abs_delta=0.377） |
| RC-2-01 | pass（500/500/0） |
| RC-2-02 | `pass_with_limitations`（G3：10-18 无 Tier0 快照） |
| FX-1/3 | pass；FX-2 G4 观察（累计 CSV 与 ODS 保留窗口 distinct 数不一致） |

**总结果：`pass_with_limitations`，退出码 0。**

### 7.6 信任表

- `ops/run_manifest`：80 行 / 4 次运行（含修复前失败的 blocked 记录，属于审计轨迹）。
- `ops/partition_status`：26 行；10-18/10-19 的 `dt × table` 状态可查。
- 10-19 是部分日：DR-3 记限制，不应把当天当完整日报。

### 7.7 P3.9 新基线（2026-10-28）

- ODS：13,807 事件 / 1,069 真值，覆盖 10-28～10-30；
- DIM：user 1,133 / item 590 / edge 10,092；
- DWD：exposure 32,293 / click 3,781 / consume 2,872 / interaction 977 /
  relation 228 / publish 90 / session 1,610；
- DWS 11 表与 semantic 40 行全部产出；
- 完整事件天 10-29 的 Gate 为 `pass_with_limitations`，20 条结果全部写入；
- V-01：521/521/0，精确率、召回率均为 1；
- V-02：4,538 行，`p50=0 / p95≈7.69% / max≈97.69%`，原因是快照生成后仍到达迟到事件；
- `ops/partition_status`：28 张规范表；仅 `semantic/metric_dict` 为 `publish_final=true`；
- 统一观测 JSON：`s3a://crt-lake/ops/reports/observations/`。

详细操作见 `Full_Observation_Runbook.md`。

---

## 8. 已知限制与下一步

1. **Airflow 容器化已验证执行边界**：Docker `LocalExecutor + Postgres` 已运行，scheduler 可提交 Spark；尚未做人工 DAG 全链路重试/告警演练。
2. **V-02 新基线非零**：10-29 的迟到事件导致流快照后批侧继续补齐，`p50=0 / p95≈7.69% / max≈97.69%`，Gate 记 G3 limitation。
3. **ODS 有重放重复**：DWD 已按 `event_id` 去重；ODS 的 raw 行数不能直接当曝光口径。
4. **D1/E1 区间重叠**：15%/5% 是注入参数，不是可分别观测的指标；QL 用 20%/14% 总量。
5. **渠道占比波动大**：QL-2-02 当前 5pp/10pp 偏紧，10-18 实测 37.7pp，按 G2 limitation；等 7 日基线成熟再校准。
6. **FX-2**：`dim_user_injected.csv` 是累计运行态文件，Kafka/ODS 有保留窗口，不能要求每天绝对相等；保持 G4 观察。
7. **VACUUM 未执行**：本次验收只读；物理回收等人工确认。
8. **data agent 未开始**：本轮只交付字典、`partition_status`、manifest、`contract_hash` 四个读契约。
9. **P3.9 已重跑**：Batch → Verify → Gate 完成；Gate 整体为 `pass_with_limitations`，`partition_status` 28 行逐表状态已生成。
10. **生成器状态日志从升级点起生效**：`graph_state.log` / `interaction_state.log` 无法自动补出升级前的历史运行状态。

---

## 9. data agent 的读契约（摘要）

agent 只读：

| 读什么 | 回答什么 |
|---|---|
| `semantic/metric_dict` | 指标、口径、粒度、`nature`、`limitations`、`query_table/query_mode/read_only/contract_version` |
| `ops/partition_status` | 这一天这张规范表能不能用、有什么限制；无专用门禁时继承上游并显式标注 |
| `ops/run_manifest` + `gate-report.json` | 为什么是这个结论、哪条门给的、哪版策略 |
| `contract_hash` / `policy_version` | 口径和策略是否变过 |

需要明细时读 `dwd/*`，每次引用带 `metric_id + dt + 表名`；**不直接读 `ods/*`**。

---

## 10. 变更记录

| 日期 | 变更 |
|---|---|
| 2026-10-20 | 首版：组件落点、启动顺序、端到端 runbook、配置优先级、调试地图、P3.8 验收记录与已知限制 |
| 2026-10-24 | 补 Airflow 本机 venv 启停、SQLite/SequentialExecutor 运行模式、Docker Compose 备选和 DAG 暂停状态；同步 README/AGENTS 当前事实 |
| 2026-10-24 | **P3.9 代码加固**：Gate 阈值/7 日滚动、逐表信任传播、综合 contract hash、结构化报告、语义查询字段、生成器状态恢复、Tier0 去重；代码与离线测试通过，运行验收待重跑 |
| 2026-10-28 | **P3.9 新基线 + 全容器观测**：恢复空 Docker 环境；生成器增加运行计划/坏数据故障/运行清单并容器化；Spark 空 ODS 等待、会话池收尾和 Gate QL 行读取修复；Docker Airflow 提交 Spark 验证；新增整体观测脚本与教学手册；10-29 Gate 为 pass_with_limitations |
