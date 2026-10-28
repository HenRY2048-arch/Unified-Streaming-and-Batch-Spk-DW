# 数仓（T 层）设计文档

> 地位：数仓侧的**实现设计**。
> 口径以 `Metric_Definitions.md` 为准；数据定义以 `project_specs/mock_generator/Mock_Source_Spec.md` 为准；框架级约束见 `Project_Global_Spec.md`。
> 版本：v1.2 ｜ 2026-10-28 ｜ 状态：**P3.1–P3.9 已完成并形成新运行基线**

---

## 0. 定位与顺序

- 上游：生成器（两个 Kafka topic：`content_events` / `content_groundtruth`）；下游：创作者看板（主）、**data agent**、推荐系统（**仅概念**）。
- 顺序：数据源稳定 ✅ → 口径冻结 ✅ → 本文档 ✅ → 逐表落地 ✅ → **P3.1–P3.8 已验收** → **P3.9 读契约加固与新基线完成**。
- 本文档**不含**指标口径（见 `Metric_Definitions.md`），只讲"怎么落"。

---

## 1. 第 1 批：定调决策

| 项 | 决策 | 理由 |
|---|---|---|
| **批流切片** | **A 方案**：流侧只落 ODS + Tier0 服务；**DWD / DWS / 归因 / 对账全部批** | 批是**唯一真值**，最简单一致；避免 B 方案的双代码与状态维护 |
| **引擎** | Spark **3.5.3**（镜像内置 **Java 11**）+ Delta **3.2.1** | 升级；附带收益：3.5 有**原生 `session_window`**，旧版 S-01/02/03 的"滚动窗近似"问题消失。**版本必须精确配对**（见 §5.1） |
| **运行形态** | Spark **容器化** | 环境干净、镜像自带 JDK（宿主不装 JDK8） |
| **存储** | **S3（SeaweedFS）+ Delta**，lake root ＝ `s3a://crt-lake/` | MinIO 仓库已在 Docker Hub 消失（404）、quay 401 → 改用 **SeaweedFS**（Apache-2.0，单容器 S3 网关）；**path-style**；凭证见 `infra/seaweedfs/s3.json` |
| **服务缓存** | Redis Stack（容器），**仅作 Tier0 缓存** | Redis 不是真值（TTL 到期回源重批） |
| **调度** | **Airflow**（2026-10-20 定） | `LocalExecutor + Postgres`，薄 DAG 触发批作业与 Gate；流作业独立运行 |

### 1.1 "批流一体"的准确表述（避免自欺）

- A 方案是「**存储一体 + 语义一体 + 真值唯一**」，**不是**"同一份代码两种执行"。
- Tier0 的流逻辑与批侧对应指标是**两套实现**，靠 **`V-02` 热度对账**约束一致。
- 代价可接受：**Tier0 只有 3 个指标**（内容热度 / 在线活跃 / 作弊）。

### 1.2 服务时效 ≠ 分析时效

| 需求 | 提供方 | 时效 |
|---|---|---|
| 秒级"看"（热榜/实时排序/下游实时特征） | 流② Tier0 → Redis | 秒级 |
| 分析（趋势/对比/下钻） | 批侧 DWS | 分钟–小时 |
| 口径统一 / 历史回溯 / 对账 | 批侧（唯一真值） | 批量 |

| 注：本项目是**小体量验证机制**，不是真扛量；"现实体量"不改变上述分工。

---

## 2. 链路拓扑

```
Kafka(content_events) ──► [流① 解析] ──► ods/events（证据层，含嵌套 items）
                                             │
                         ┌───────────────────┴────────────────────┐
                         ▼                                        ▼
              [流②] ODS → Tier0                         [批] ODS → DIM/DWD
              输入只读 ods/events；每微批重算最近24h       曝光展开、配对、渠道回填
                 │                                           │
                 ├─ F-07 HeatMath 1h/24h ─► heat_snapshot     ▼
                 │                         + Redis heat    DWS（11 张主题表）
                 ├─ Q-01 A1/A2 ───────────► cheat_events
                 └─ U-04 近15min ─────────► Redis crt:online
                                                              │
                                                              ▼
                                                   semantic/metric_dict（40 指标）

Kafka(content_groundtruth) ──► [流① 真值解析] ──► ods/groundtruth（[真相层]）
                                                              │
                                                              │ 只用于 V-01，不参与指标计算
                                                              ▼
                     dwd/interaction + dwd/relation ──► verify/attribution（V-01）
                                                              ▲
                     tier0/heat_snapshot + HeatMath ──► verify/heat（V-02）
                                                              │
                                                              ▼
                                      GateApp（DR/IV/CV/QL/RC/FX）
                                                              │
                         ┌────────────────────────────────────┼────────────────────┐
                         ▼                                    ▼                    ▼
                 ops/run_manifest                  ops/partition_status       ops/reports
                                                              │
                                                              ▼
                                                     看板 / data agent（外部只读）
```

实现要点：

- **一个 Spark 应用跑三条流查询**（events→ODS、truth→ODS、ODS→Tier0），共享一个 SparkSession → 内存可控、职责分离。
- **流②从 ODS 出发**（不直读 Kafka）：保证「**证据层唯一**」——批与流同源，使 `V-02` 比较的是**纯算法差**。
  > 旧架构"回读 Delta"是坏味道，因有 **8 个消费者**；此处只有 **1 个小消费者**。
- `CheatDetector`（流状态机）保留，且**须落库**（旧版只进 memory）。

**Tier0 的三个输出（不要把它当成一张表）**：

| 输出 | 指标 | 落点 | 语义 |
|---|---|---|---|
| 热度快照 | F-07 | `tier0/heat_snapshot`（Delta）+ Redis top-N | 1h/24h 窗口、`HeatMath` 加权 + 停留折算；**近似**，供 V-02 |
| 作弊轨迹 | Q-01 | `tier0/cheat_events`（Delta） | A1/A2 规则命中；跨微批可能重复，查询按 `rule+user+item+window` 去重 |
| 在线活跃 | U-04 | Redis `crt:online` | 近 15 分钟 distinct user；缓存 TTL 900s，不是精确在线 |

Tier0 当前**没有状态化增量**：每个微批重算最近 24h，换来与批侧同定义；真实体量下应改为增量状态。

---

## 3. 第 2 批：结构

### 3.1 总线矩阵（业务过程 × 一致性维度）

| 业务过程 | 粒度 | dt/时 | user | item | author | category/tags | channel | session | position |
|---|---|---|---|---|---|---|---|---|---|
| 曝光 | 一次曝光（逐条） | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| 点击/进入 | 一次 | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| 消费 | 一次消费（带停留） | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | — |
| 互动 | 一次动作（分类型） | ✔ | ✔ | ✔ | ✔ | ✔ | — | ✔ | — |
| 关系变更 | 一条边 | ✔ | ✔(follower) | — | ✔(followee) | — | ✔(来源) | ✔ | — |
| 交流 dm | 一条消息 | ✔ | ✔ | — | — | — | — | ✔ | — |
| 内容产出 | 一条内容 | ✔ | — | ✔ | ✔ | ✔ | — | — | — |
| 会话/旅途 | 一个会话 | ✔ | ✔ | — | — | — | — | ✔ | — |
| 作弊/异常（横切） | 一次判定 | ✔ | ✔ | ✔ | ✔ | ✔ | — | ✔ | — |

**组织原则**：

- **行 → DWD 事实表 + DWS 聚合表**；**列 → 一致性维度**（**必须同名同义**，这是 agent 能正确下钻的前提）。
- 主题域同时是「**归因分治**」与「**批流分级**」的边界。

### 3.2 目录契约（Delta on S3）

> lake root ＝ **`s3a://crt-lake/`**（SeaweedFS S3 网关，path-style）。下表路径均相对 lake root。

```
<lake-root>/
  ods/events/dt=YYYY-MM-DD/          原始解析事件（含嵌套 items）
  ods/groundtruth/dt=YYYY-MM-DD/     归因真值

  tier0/heat_snapshot/dt=.../hour=.../  流侧热度快照（**流②按微批落**，供 V-02 对账）
  tier0/cheat_events/dt=.../hour=.../   流侧 A1/A2 作弊轨迹（**流② append**，查询去重）

  dim/user/                          用户维（快照）
  dim/item/                          内容维（append-only）
  dim/edge/                          关注边（t0 种子 + follow/unfollow 事件重建）

  dwd/exposure/                      逐条曝光（由 items 展开）
  dwd/click/                         逐条点击（未闭环的 enter_item 也算）
  dwd/consume/                       消费片段（enter↔leave 配对 + 停留）
  dwd/interaction/                   互动动作（like/fav/share/comment/dislike）
  dwd/relation/                      关系与交流（follow/unfollow/dm/enter_profile）
  dwd/publish/                       内容产出（item_published）
  dwd/session/                       会话/旅途（由事件聚合）

  dws/funnel_hourly/                 分发漏斗（曝光→点击→消费→互动）
  dws/channel_hourly/                渠道效率
  dws/content_daily/                 内容效率与消费质量
  dws/author_daily/                  创作者中心
  dws/relation_daily/                粉丝来源 / 关注转化 / 取关
  dws/session_daily/                 会话与旅途
  dws/user_daily/                    活跃 / 新用户
  dws/quality_daily/                 质量与风控（秒退/短停留/长驻留/异常分布）
  dws/position_daily/                位置偏差 / 排序质量

  verify/attribution/                V-01 归因对账
  verify/heat/                       V-02 热度对账
  semantic/metric_dict/              指标字典（元数据表）

  ops/run_manifest/                  每次运行的 Gate 清单（Delta，按 run_id/dt）
  ops/partition_status/              分区信任状态（Delta，dt × 规范表 → status/limitations/contract_hash）
  ops/reports/                       人读的 gate-report.md / 机器读的 gate-report.json
```

- **分区键**：一律 `dt`（日期）；`dws/*_hourly` 另加 `hour`。
- **重写语义**：按 `dt(+hour)` 分区，用 Delta `replaceWhere`/`MERGE` 做**幂等重算**（"批为真值"的落地形式）。

### 3.3 曝光展开（分层的第一个真实职责）

- **ODS**：`from_json` 后 `items` 是 `array<struct<item_id,position,rank_score>>`，**原样保留**（不改形状）。
- **DWD**：`explode(items)` → **一行一条曝光**，字段含 `batch_id / channel / user_id / session_id / ts / item_id / position / rank_score`。
- 这是"**曝光为根**"口径的**物理基石**（否则 `F-01..F-06` 无从计算）。

### 3.4 代码结构

```
com/hub/
  common/   Config（配置）｜Contract（契约 v2）｜Lake（路径+SparkSession）｜DeltaIO（写/覆盖/合并/VACUUM）｜HeatMath（热度公式，流批共用）
  stream/   KafkaSource｜OdsWriter（流①）｜Tier0（流②：热度/作弊/在线）｜RedisSink
  batch/    DimBuilder｜DwdBuilder（曝光展开/配对等）｜DwsBuilder（11 张表）｜SemanticDict（40 指标元数据）
  verify/   AttributionVerify（V-01）｜HeatVerify（V-02）｜GateRunner（Gate v1 执行器）
  app/      StreamApp｜BatchApp（--step dim|dwd|dws|semantic|all）｜VerifyApp｜GateApp｜VacuumApp
```

> 三个主入口共用 `common` 的契约模型与配置，**契约只定义一次**；Gateway/Vacuum 是 P3.8 新增的运行入口。
> 状态（2026-10-20）：21 个文件**已落地并编译通过**；旧 14 个文件已移出（备份在 `/tmp/crt_legacy_warehouse_2026-10-17/`）。
> P3.8 新增 `GateRunner` / `GateApp` / `VacuumApp`，现有三入口职责不变。

---

## 4. 旧架构处置（改造 vs 删除 vs 重写）

| 旧组件 | 处置 | 原因 |
|---|---|---|
| `KafkaReader` | **改**（配置化 + 双 topic） | 地址/topic 变 |
| `DataCleanser` | **重写** | 新契约 20+ 字段、嵌套 `items`、`schema_version` |
| ODS 落湖 | **改**（→ S3（SeaweedFS）+ 分区 `dt`） | 存储落点换 |
| `DataEnricher`（DWD） | **重写**（移到**批侧** + **曝光展开** + 维表/作者/来源） | 新契约 + 新职责 |
| `enrichedStream` 回读 | **删除** | A 方案下 DWD 只批 |
| 8 个流聚合 | **重写为批**（并入 `batch/dws`） | 口径全变 + 回退批 |
| memory sink / `--test` 导出 | **删除或改造** | 旧的是调试用；新出口是 DWS 表 |
| `NewUserAggregator` / `BloomFilterService` | **删除** | 用维表反连接替代；Bloom 是过时补丁 |
| `CheatDetector` | **保留 + 改**（加落库 Sink、配置化） | 流侧真需求 |
| `RedisSink` | **改**（新主机 + 新 key；Redis 降为 **Tier0 缓存**） | 定位变了 |
| `DeltaSink` | **改**（S3A / Delta 3.2.1 / 配置化） | 存储与版本 |
| `AppConfig` | **重写**（配置外置，照生成器做法） | 硬编码 |
| `RealTimeApp` | **拆分为三入口** | 架构变了 |
| `build.sbt` | **改**（Spark 3.5.x / Delta 3.x） | 升级 |

**"落后点"补救清单**：① 入湖 → S3（SeaweedFS）✅；② Redis → 容器 ✅；③ 路径硬编码 → 配置化 ✅；
④ Spark 版本 → 升级 ✅；⑤ **新增**：调度、语义层、对账、曝光展开（旧架构没有）。

---

## 5. 运行与资源

- 容器（**已就绪**）：`crt-kafka`、`crt-redis`（Redis Stack）、`crt-s3`（SeaweedFS）；`crt-spark`（`apache/spark:3.5.3`，按需 `--profile tools`）。
- **S3A 参数**（Spark 侧）：`fs.s3a.endpoint=http://seaweedfs:8333`、`fs.s3a.path.style.access=true`、
  `fs.s3a.access.key=crtadmin`、`fs.s3a.secret.key=crtadmin123`、`fs.s3a.connection.ssl.enabled=false`、
  `fs.s3a.endpoint.region=us-east-1`。
- **Delta over S3A 已验证（2026-10-16，14/14）**：建表 / 写入 / 读回 / `replaceWhere` / `MERGE` / `DELETE` /
  时间旅行 / `VACUUM` 真删 / 流式写 + checkpoint 落 S3A / **checkpoint 重启续跑零重复**。
  脚本 `infra/scripts/delta_s3a_smoke.scala`，结论与复现命令见 `infra/README.md` §5。
- **Java**：Spark 官方镜像内置 **Java 11**（实测 `11.0.24`）→ 编译目标取 **JVM 8/11**；宿主 Java 17 仅用于构建。

### 5.1 两个必须遵守的落地约束（冒烟带出来的）

1. **版本精确配对**：`delta-spark_2.12 3.2.1` 编译自 **Spark 3.5.3**；放 Spark 3.5.1 上 MERGE 会
   `NoSuchMethodError: ExpressionSet.$plus$plus`。**改 Delta 版本必须同步改镜像 tag**。
2. **空间回收靠 VACUUM**：`replaceWhere` / `DELETE` 只在 `_delta_log` 里移除引用，**物理文件留在 S3 上**
   （实测引用 4 个 vs S3 上 16 个）。批侧按窗口重算后需配 `VACUUM`，否则存储单调膨胀。

> 依赖获取方式：`infra/jars/pom.xml` + 宿主 mvn 下载 → 挂载进容器（避免容器内联网解析）。

### 5.2 构建与运行（**Maven + scalac**，不使用 sbt）

环境已容器化（容器内只有 Java 11，没有 sbt），因此数仓代码统一走 Maven：

```bash
mvn -s RealTime/settings.xml -B clean package     # → target/crt-warehouse-0.1.0.jar（薄 jar）
```

- 依赖分两类：**编译期**（`pom.xml`，Spark/Delta/Kafka 连接器全部 `provided`）与**运行期**
  （`infra/jars/*.jar`，由 `spark-submit` 的 jars 参数挂进容器）。
- `build.sbt` 保留给 IDE，但**版本与 pom 保持一致**，且不是正式构建入口。
- 运行时示例见 `infra/README.md` §5.3（把 `--master`/`--jars`/`-i` 换成 `--class com.hub.app.StreamApp` 即可）。
- 资源：WSL2 内存 7.58GB（`.wslconfig` 已写 11GB，待重启生效）；实测 Kafka ~683M、Redis ~7M；Spark 限 3G。
- 调度：**Airflow Docker `LocalExecutor + Postgres`**；scheduler 经 Docker socket 提交 Spark；流作业留在 DAG 外。

---

## 6. 已定与待定

**已定**

1. **调度器 ＝ Airflow**（2026-10-20）：`LocalExecutor + Postgres`，DAG 只调 `run_warehouse.sh` 与 Gate 入口；
   不引入 Celery/Redis broker；`StreamApp` 作为常驻服务留在 DAG 外，Airflow 只检查 ODS 新鲜度。
   当前运行在 Docker `LocalExecutor + Postgres`；宿主 venv/SQLite 只保留为备用调试方式，详见 `infra/airflow/README.md`。
2. **查询层 ＝ Spark SQL 为主**（零新增组件），**DuckDB 作为后续轻量查询 / agent 层**；
3. **存储落点 ＝ S3（SeaweedFS）**，lake root ＝ `s3a://crt-lake/`，path-style（参数见 §5）；
4. **`Delta over S3A` 冒烟通过**（14/14）：committer 用默认（`fs.s3a.committer.name=file`）、multipart 生效、
   VACUUM 真删、流式 checkpoint 重启零重复（见 §5）。
5. **V-02 的流侧热度快照落 Delta**（2026-10-17 拍板）：流②每微批把
   `(dt, hour, item_id, window_minutes, heat, computed_at)` **追加**到 `tier0/heat_snapshot/`；
   批侧对账时读该表，**不直读 Redis**（Redis 是缓存、会 TTL 过期，且"读到的不是同一时刻"）。
6. **P3.1–P3.7 已落地**（2026-10-20）：ODS → 维表 → DWD → DWS（11 张）→ 语义层（40 行）→ Tier0 → 对账；
   三批验收的数字见 `AGENTS.md` §5.0。
7. **P3.8 运行验收完成**（2026-10-20）：10-18 Gate `pass_with_limitations`；V-01 1,252/1,252/0，
   V-02 1,947 行 rel0；`ops/run_manifest` / `partition_status` 可读；详见 `Deployment_And_Operations.md` §7。
8. **Gate v1 已冻结**（2026-10-20）：阈值/等待/降级取值见 `Gate_Grading_Scheme.md` §5/§8；
   三个语义口径（热度不要求曝光、负反馈保留混合、留存按注册日/首见日 + 1/3/7）见 `Metric_Definitions.md` §10.11。
9. **P3.8 前置修正**：补 `dwd/click`、`enter_profile` 进 `dwd/relation`、V-01 加 `missing_cnt/recall`、
   Q-02 拆 `short_stay`/`long_content_short_stay`、字典加 `limitations`、U-03 cohort 与 `user_daily` 重跑口径
   （详见 `Gate_Grading_Scheme.md` §5.1）。
10. **P3.9 新基线完成**（2026-10-28）：全容器工作负载、逐表信任、V-02 阈值执行、整体观测入口和教学手册落地；
    10-29 Gate `pass_with_limitations`，V-01 521/521/0，V-02 因迟到事件非零。

**待定**

1. **V-02 未来门槛**：Tier0 改为状态化增量后，再根据实测分布校准 `p95/max`。
2. 逐表字段的最终列名（已按实现回写 `Warehouse_Tables.md`，后续只在语义需要时调整）。

---

## 7. 变更记录

| 日期 | 变更 |
|---|---|
| 2026-10-16 | 首版：定调（A 方案 / Spark 3.5.x 容器化 / MinIO）；总线矩阵；目录契约；曝光展开；代码结构；旧组件处置 |
| 2026-10-16 | 补环境后修订：**MinIO 镜像不可用** → 存储改「可配置 lake root（当前本地 FS）」；补 **调度＝DolphinScheduler**、**查询层＝Spark SQL（+DuckDB 后续）**；记入 Spark 镜像**内置 Java 11** |
| 2026-10-16 | **对象存储落定 SeaweedFS（S3）**：`s3a://crt-lake/`、path-style、S3A 参数入 §5；新增 `Warehouse_Tables.md`（逐表 DDL）；待办收敛为「Delta over S3A 冒烟」 |
| 2026-10-16 | **Delta over S3A 冒烟通过（14/14）**：引擎定版 **Spark 3.5.3 + Delta 3.2.1**（精确配对，§5.1）；§5 补"验收清单 + 两条硬约束（版本配对 / VACUUM 回收）"；清除 MinIO 残留；待定项改为"列名微调 + **V-02 流侧热度快照落哪**" |
| 2026-10-17 | **V-02 拍板：流侧热度快照落 Delta**（`tier0/heat_snapshot/`，流②按微批追加；批侧对账读它，不读 Redis）；§2 拓扑与 §3.2 目录契约同步；待定项只剩"列名微调" |
| 2026-10-20 | 对齐实现：§3.4 换成**实际 18 个类**（含 `HeatMath`、`Tier0`、`RedisSink`、三入口）；§6 补"P3.1–P3.7 已落地"，待定收敛为 **P3.8 编排 + Gate 四待定 + 三语义口径 + 列名微调** |
| 2026-10-20 | **P3.8 设计定稿**：调度器改 **Airflow（LocalExecutor + Postgres）**；目录契约补 `dwd/click`、`ops/run_manifest`、`ops/partition_status`、`ops/reports`；§6 写入 P3.8 最小闭环、Gate v1 冻结与前置修正（`dwd/click`、R-06、V-01 召回、Q-02 拆分、字典 `limitations`） |
| 2026-10-20 | **P3.8 运行验收**：10-18 Gate `pass_with_limitations`（DR-3 历史日缺水位、ODS 重放、渠道占比、无 Tier0 快照为限制）；V-01 1,252/1,252/0，V-02 1,947 行 rel0；验收记录与调试手册见 `Deployment_And_Operations.md` |
| 2026-10-24 | **P3.9 Data Agent 读契约加固（代码）**：逐表 `partition_status` + 上游限制传播；综合 SHA-256 `contract_hash`；语义字典查询字段；Gate 阈值与 7 日窗口真实执行；生成器状态恢复；Tier0 ODS 去重。离线编译/测试通过，完整 Gate 待重跑 |
| 2026-10-28 | **P3.9 新基线 + 全容器运行**：Generator/Spark/Airflow 容器化；Tier0 等待空 ODS、生成器运行计划/故障注入、观测入口和教学手册落地；10-29 Gate `pass_with_limitations`，V-02 因迟到事件非零 |
