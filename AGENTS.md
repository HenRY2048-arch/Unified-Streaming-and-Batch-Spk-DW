# AGENTS.md —— 交接书（给下一个会话窗口）

> 目的：**让新窗口只读本文 + 几份规约，就能无损接续**。上一个窗口的上下文已近耗尽，因此本文件承载"目标 / 决策 / 进度 / 风险 / 下一步 / 协作方式"。
> 维护约定：每完成一个节点，更新 §4（进度）与 §11（变更记录）。

## 0. 60 秒速览

- **项目**："微博式内容社区"的**批流一体实时数仓**，个人练手项目（非生产级，但要求合理）。
- **当前阶段**：生成器 v2 ✅、存储冒烟 ✅、**数仓 P3.1–P3.9 全部落地并完成新运行验收**
  （ODS → 维表 → 明细 → 主题 → 语义层 → Tier0 → 对账 → Gate）；10-29 为 `pass_with_limitations`。
  **全链路工作负载已完成容器化验证，整体观测入口和教学手册已落地。**
  **P4 Data Agent 已进入架构 brainstorm，尚未开始工程实现。**
- **代码现状**：21 个 Scala 文件（`common/stream/batch/verify/app`），构建走 **Maven + scalac**；
  旧 14 个文件已移出到 `/tmp/crt_legacy_warehouse_2026-10-17/`。
- **文档现状**：口径 v1.3、生成器正本 v0.11、指标实现说明 v8、**Gate 分级方案 v3.2**（独立成文）；
  Data Agent 有独立 brainstorm 记录，尚不是实现方案。
- **下一个动作**：基于新观测基线继续 Data Agent 的需求与架构收敛；未获用户确认前不创建 Agent 工程。
  当前读契约仍是 `semantic/metric_dict`、`ops/partition_status`、`ops/run_manifest`/`gate-report.json`、`contract_hash`。
- **当前服务**：Airflow Docker `LocalExecutor + Postgres`，入口 `http://localhost:8080`（`admin/admin`），
  `crt_daily_batch` / `crt_weekly_vacuum` 已暂停；Kafka/Redis/SeaweedFS 正在运行。DAG 源码在 `infra/airflow/dags/`。
- ⚠️ **协作约定**：动手（改代码/跑任务）前先说一声；docker / 联网 / 沙箱外写入直接申请权限。

## 1. 项目目标（从头至今）

### 1.1 最初目标

通读并解构一个既有的 Spark Structured Streaming"准实时数仓"学习项目（以代码为唯一事实，输出解构文档）。

### 1.2 现在的目标（**业务背景与链路框架整体换血**）

1. **业务改造**：由"微博式社交社区"钉向**以内容分发为主角**（社交关系是被分析对象 + 分发渠道）；**作者回归**；曝光成为"根"；**不建应用层**，业务真实性由 mock 生成器承担。
2. **架构改造**：从**纯准实时**（所有指标走流 + 每指标一个 sink）改为**批流一体**——**批为真值、流为近似**；指标**分级**：单实体计数→OLTP、秒级→流、其余→批。
3. **归因改造**：统一证据层（事件 + 会话 + **来源**）+ 分治口径层；**旅程归因**（会话级 + 窗口、批侧）；**归因真值另存**（独立 topic）用于**对账**。

### 1.3 长期规划（影响指标设计的约束）

- 数仓改造完成后要加 **data agent**（数仓产出既服务**看板**，也服务 **agent**）。
- **推荐系统仅"概念存在"**，长期不开发；但指标/证据层要**为其预留形状**（不建服务）。
- 因此指标设计的判据是：**① 有下游 ② 服务数仓自身（重跑/对账）③ 服务长期离线分析**——三者不沾＝废指标。

## 2. 通读协议（怎么理解这个项目）

**按序读，不要跳读。** 读代码时牢记：**`src/main/scala/` 是当前数仓实现**；改造前的历史快照在 `项目解构与链路基线.md`，不要把两者混用。

| 顺序 | 读什么 | 作用 |
|---|---|---|
| 1 | **本文件** | 全景与交接 |
| 2 | `project_specs/overall_spec/Project_Global_Spec.md` | **框架级规约**（定位/业务/主题域/批流一体/分级/归因框架/变更代价）——**冲突时以此为准** |
| 3 | `project_specs/overall_spec/Metric_Definitions.md` | **指标口径（v1.3 已冻结）**：9 类编号、Tier、边界（数仓 vs 应用层）、非废指标判据 |
| 4 | `project_specs/overall_spec/Warehouse_Design.md` | **T 层设计**：A 方案、总线矩阵、目录契约、曝光展开、代码结构、旧组件处置 |
| 5 | `project_specs/overall_spec/Warehouse_Tables.md` | **逐表 DDL**（DIM/ODS/DWD/DWS/verify/semantic）+ **Tier0 热度参数** |
| 6 | `project_specs/mock_generator/Mock_Source_Spec.md` | **生成器唯一正本**：实体/行为语义/超集 Schema/算法/真值/配置 |
| 6.1 | `project_specs/mock_generator/samples/SCHEMA.md` | **样本字段字典**：CSV（无表头）、JSONL 事件、truth 字段 |
| 7 | `project_specs/mock_generator/Generator_Refactor_Checklist.md` | 生成侧改造清单（**A–F 组 100% 完成**，可当"已完成事项"读） |
| 8 | `RealTime/`（源码）+ `RealTime/docs/` | **生成器工程**：链路与架构说明、测试与配置指南 |
| 9 | `src/main/scala/**`（**21 个文件**）+ `pom.xml` | **数仓新实现**：`common/stream/batch/verify/app`；旧 14 文件已移出（见 §5.3 映射） |
| 9.1 | `project_specs/overall_spec/Metric_Implementation_Guide.md` | **指标怎么算、怎么读、能不能当门禁**（讲人话版） |
| 9.2 | `project_specs/overall_spec/Gate_Grading_Scheme.md` | **门禁如何分级**（v3.2：Status × Action × Approval + 六类 × 层级 + 注册表） |
| 9.3 | `project_specs/overall_spec/Deployment_And_Operations.md` | **整体链路搭建 / 启动 / runbook / 调试 / 验收记录** |
| 9.3.1 | `project_specs/overall_spec/Full_Observation_Runbook.md` | **从空环境到整体观测报告的逐步教学手册**（Docker 恢复、生成器 profile、批/对账/Gate） |
| 9.4 | `project_specs/data_agent/Data_Agent_Discussion.md` | **Data Agent 架构 brainstorm**：需求、边界、候选技术栈、RAG 定位和开放问题；非实现契约 |
| 10 | `项目解构与链路基线.md` | **历史快照**（改造前），理解"改什么、为什么改" |
| 11 | `infra/README.md`、`infra/docker-compose.yml` | 本地环境：容器、端口、凭证、启停与验证 |

**明确忽略（勿作现行前提）**：

- `project_specs/_deprecated/**`（旧核心规约、旧指标体系、Drift 规约、旧 mock 三件套、两份草案）——见其 `README.md`
- `venv/`、`.venv/`、`.airflow-venv/`、`.airflow-home/`、`Untitled.ipynb`、`.ipynb_checkpoints/`、`.virtual_documents/`、`data-quality/`（噪音）
- `RealTime/` 内 IDE 产物（`.metals/ .trae/ .vscode/ .bloop/ target/`）

## 3. 决策台账（按时间顺序）

| # | 决策 | 结论 | 去向 |
|---|---|---|---|
| 1 | 业务定位 | 微博式社区，但**主角＝内容分发** | Global_Spec §1/§2 |
| 2 | 是否自建真实 App | **不建**；业务真实性由 mock 生成器承担 | Global_Spec §1 |
| 3 | 计算范式 | **批流一体**（批为真值、流为近似）；只用 Spark，**不引入 Flink** | Global_Spec §4 |
| 4 | 分级 | 单实体计数→**OLTP**；秒级→**流**；其余→**批** | Global_Spec §5.2 |
| 5 | 作者 | **回归**（作者 ≡ 用户；`dim_user` 带创作者属性；**不设 dim_author**） | Mock_Source_Spec §2.2 |
| 6 | 图 | 有限有向（N=1000、出度 10、分群+优先连接、**偏向同簇创作者**）；**存三处**；`dim_user` **不含边** | Mock_Source_Spec §2.5 |
| 7 | 曝光 | **新增批次曝光 `exposure_batch`**，带 `channel/position`；**曝光＝一切漏斗的分母** | Mock_Source_Spec §4 |
| 8 | 来源 | 结果事件带 `source_item_id / source_user_id / channel` | Mock_Source_Spec §4.2 |
| 9 | `content_len` | **归维表**，不由事件注入；E1＝短停留（2026-10-20 澄清：注入不按内容长度筛选，见 #39） | Mock_Source_Spec §4.5 |
| 10 | 归因真值 | **另存独立 topic `content_groundtruth`**（不进事件流，否则校验＝抄答案） | Mock_Source_Spec §5 |
| 11 | 分发 70/30 | 升级为**动态比例**（成熟度 × 渠道反馈 EMA），基线 45/35/20 | Mock_Source_Spec §6.2 |
| 12 | 策略/成本/压力 | 全部**可配**（`hybrid｜random｜interest｜graph`、`cheap｜standard｜heavy`、`steady｜peak｜ramp｜skew`） | Mock_Source_Spec §6 |
| 13 | 停留时长 | 改为**按内容长度/阅读速度采样**（旧版 wall-clock 导致恒为 0） | Mock_Source_Spec §6.7 |
| 14 | like/dislike | **状态型**：对 (user,item) 一次性、**互斥**、**不可逆**（不做 `unlike`） | Mock_Source_Spec §3.3 / Metric_Definitions §9.1 |
| 15 | 指标口径 | **冻结 v1.0**；曝光为分母；`I-01` 同时给 flow/state 两种口径 | Metric_Definitions |
| 16 | 批流切片 | **A 方案**：流侧只落 ODS + Tier0；**DWD/DWS/归因/对账全批** | Warehouse_Design §1 |
| 17 | 一致性的准确表述 | A ＝ **存储一体 + 语义一体 + 真值唯一**（Tier0 与批侧是两套实现，靠 `V-02` 对账） | Warehouse_Design §1.1 |
| 18 | 流服务读什么 | **从 ODS 读**（不直读 Kafka）→ 证据层唯一，`V-02` 比的是纯算法差 | Warehouse_Design §2 |
| 19 | 引擎/形态 | **Spark 3.5.x**（原生 `session_window`）+ **容器化**（镜像内置 **Java 11**） | Warehouse_Design §1 |
| 20 | 存储 | **S3（SeaweedFS，path-style）**；lake root ＝ `s3a://crt-lake/`（MinIO 仓库已消失） | Warehouse_Design §1/§3.2/§5 |
| 21 | 代码结构 | `common/ stream/ batch/ verify/ app/`（**三入口**） | Warehouse_Design §3.4 |
| 22 | 调度 | **Airflow**（2026-10-20 改；原 DolphinScheduler）——`LocalExecutor + Postgres`，薄 DAG，流作业留在 DAG 外 | Warehouse_Design §6 |
| 23 | 查询层 | **Spark SQL 为主**；**DuckDB** 作为后续轻量/agent 层 | Warehouse_Design §6 |
| 24 | 旅程刻度 | 会话级 + 归因窗口、放批侧；**不做终身旅程**、**不做多触达归因**（只 last-touch） | Metric_Definitions §7 |
| 25 | 新用户判定 | **不用 BloomFilter**（老版在 driver 串行查 Redis、概率误判、Redis 被清则全量误判、且**不存时间**）；改为**累积维表 + 幂等重算**：`first_seen = min(ts)` MERGE 进 `dim_user` | Metric_Definitions §10.11；Mock_Source_Spec §6.9 |
| 26 | 生成器引入 **模拟时钟** | 全局时钟**只由模型活动推进**（不绑真实时间，可复现）；**用户时钟**承载事件时间戳（会话开始锚定全局）；`leave_ts−enter_ts ≡ duration_sec`；并发会话上限 30 + 过期收口；**`DistributionService` 新鲜度必须同用模拟时钟**；事件时间落盘续跑 | Mock_Source_Spec §6.8 |
| 27 | 生成器引入 **新用户注入** | 新增 `user_registered` 事件（**不带 `session_id`**）；id 取种子外高位段 + `dim_user_injected.csv` **append 落盘**（重启不重发 id）；属性分布与种子一致 | Mock_Source_Spec §6.9 |
| 28 | `★★★` 三项排期 | `A-04` 冷启动 / `R-05` 粉丝活跃度 / `U-03` 留存 **与其它指标同步实施**（不再挂"后期"）→ 新增 `dws/retention_daily`、`dws/cold_start_daily` | Metric_Definitions §8/§10 |
| 29 | 停留/会话时长口径 | 停留**以 `leave_item.duration_sec`（模型真值）为准**，ts 差仅交叉校验；会话时长以事件时间轴为准 | Mock_Source_Spec §3.2；Warehouse_Tables §3 |
| 30 | V-02 流侧热度快照 | **落 Delta**：`tier0/heat_snapshot`（流②按微批 append，`computed_at` 取最新）；批侧对账**不读 Redis** | Warehouse_Design §6；Warehouse_Tables §4.1 |
| 31 | 数仓 vs 下游的边界 | **数仓只做"客观、可复现、可对账的描述"**（计数/比率/分布/切分）；**"为什么/该怎么做"留给下游**。漏斗属描述、不越界；越界的是"打分/建议/因果/个性化"。三条纪律：只输出计数与比率、归因只做 last-touch、字典带"性质"列 | Metric_Implementation_Guide §0.5 |
| 32 | Gate 分级体系 | **三维定义**：类别（DR 就绪/IV 守恒/CV 口径/QL 质量/RC 对账/**FX 夹具自检**）× 层级（每类 3–4 级）× 处置（**G0 阻断 / G1 等待 / G2 降级 / G3 告警 / G4 观察**）。规则：**只有 IV/CV 允许 G0**；QL/FX 默认只告警/观察；**FX 不进每日阻断链路**，只做生成器变更回归 | Metric_Implementation_Guide §7 |
| 33 | Gate 方案独立成文 | 抽为 **`Gate_Grading_Scheme.md`**（权威）；Guide §7 保留摘要+指针 | Gate_Grading_Scheme |
| 34 | P3.8 最小闭环 | Gate 执行器 + `ops/run_manifest` + `ops/partition_status` + `gate-report.{json,md}` + Airflow `daily_batch`；VACUUM 周维护/人工批准；manifest/partition_status 落 **Delta** | Warehouse_Design §6；Gate §6 |
| 35 | Gate v1 冻结 | QL 用可观测基线 20%/14%/0.5%，±3pp 告警 / ±5pp 或连续 2 天同向偏离降级（<200 用 7 日滚动）；DR-3 当前日 5min×3、历史日缺水位 G2；G2 仅 QL/DR-4 且 `publish_final=false`；V-02 当前 `rel_diff≤1e-9`，无快照 G3；V-01 精确率≥0.99 且 `missing_cnt=0`/召回≥0.999；另 DR-4-02=0.98、QL-2=30%/50% 与 5pp/10pp | Gate §5/§8 |
| 36 | 三个语义口径 | 热度**不要求曝光**才计停留分；负反馈保留混合口径（`dislike + bounce`，超速不重复计）；留存 cohort 改注册日/首见日 + `cohort_type`，档位 1/3/7 | Metric_Definitions §10.11；Guide v8 |
| 37 | P3.8 前置修正 | 补 `dwd/click`；`enter_profile` 进 `dwd/relation`；V-01 加 `missing_cnt/recall`；Q-02 拆 `short_stay`/`long_content_short_stay`；字典加 `limitations`；U-03 cohort 与 `user_daily` 重跑口径 | Gate §5.1；Warehouse_Tables |
| 38 | data agent 边界 | **不嵌数仓链路**；数仓只交付读契约（字典/`partition_status`/manifest/`contract_hash`），agent 独立开发；当前已进入架构讨论，仍未实现 | Gate 附录 C；Data Agent Discussion |
| 39 | E1 / 短停留语义 | E1 注入为 `<0.8s` 短停留（**不按 `content_len` 筛选**）；“长内容短停留”只作 Q-02 诊断；QL 5% 基线对 `short_stay` | Mock_Source_Spec §6.5；Metric_Definitions §10.11 |
| 40 | Airflow 运行时 | **Docker `LocalExecutor + Postgres` 已运行**；UI `localhost:8080`，DAG 暂停；scheduler 通过 Docker socket 提交一次性 Spark 容器，已验证 | `infra/airflow/README.md`；Deployment §3.2 |
| 41 | Data agent 前置加固（P3.9） | Gate 真正执行 V-02 阈值与 7 日滚动；`partition_status` 逐表化并传播上游限制；`contract_hash` 覆盖事件/真值/种子/语义字典/热度公式；生成器恢复互动与关注图状态；Tier0 先按 `event_id` 去重；线事件契约统一为 `v2` | Gate §5/§8；Tables §6/§7.1；Mock Source §6.9 |
| 42 | 观测控制面与全容器运行 | 生成器增加运行计划、显式 chaos、坏数据/重复/丢弃/暂停注入、运行清单和容器镜像；Spark/Airflow/Generator 均容器化；新增 `bootstrap_runtime.sh`、`observe_warehouse.*` 与 `Full_Observation_Runbook.md` | Mock Source v0.11；Deployment v1.2 |

## 4. 进度看板

| 阶段 | 内容 | 状态 |
|---|---|---|
| **P0 环境** | Kafka / Redis Stack / **SeaweedFS（S3）** 容器 + Spark 3.5.3 镜像（与 Delta 配对） | ✅ |
| **P1 生成器改造** | P1.0 配置外置 → P1.1 实体维表 → P1.2 事件契约 → P1.3 行为核心 → P1.4 策略/成本/真值 → P1.5 收尾（+ 行为语义补丁） | ✅ 清单 100% |
| **P2 数据源稳定** | 契约冻结、算法稳定、样本产出（`samples/`） | ✅ |
| **P2.5 存储冒烟** | **Delta over S3A 冒烟 14/14 通过**（脚本 `infra/scripts/delta_s3a_smoke.scala`；结论见 `infra/README.md` §5） | ✅ |
| **P1.6 生成器 v2** | 模拟时钟 + 新用户注入（`user_registered`，无 `session_id`）+ id 落盘防重 + 关闭时 flush | ✅（实测验见 §5.2） |
| **P3.0 骨架** | Maven + scalac 构建路径 ✅（`pom.xml`，薄 jar）；`common/{Config,Contract,Lake,DeltaIO}` ✅；14 个旧文件已移出 | ✅ |
| **P3.1–3.3 第二批** | 流①→ODS ✅（4,425 事件 / 254 真值）；维表 ✅（user 1031 / item 533 / edge 10021）；DWD ✅（曝光 10,530 = Σitems、消费 919、会话 541） | ✅ |
| **P3.4–3.5 第三批** | DWS 11 张表 ✅ + `semantic/metric_dict` 40 行 ✅；曝光/点击/关注/会话四项对照全部 OK | ✅ |
| **P3.6 Tier0** | 热度 1h/24h → Redis + `tier0/heat_snapshot`（1,947 行 / 536 内容）；在线活跃 → Redis（`active_users=20`）；作弊 A1/A2 → `tier0/cheat_events`（13 行） | ✅ |
| **P3.7 对账** | `verify/{AttributionVerify,HeatVerify}` + `app/VerifyApp`；**V-01 六类转化 × 3 天全部 100%**；V-02 1,947 行（rel_diff=0，见 §6 说明） | ✅ |
| **P3.8 编排** | **运行验收通过（10-18）**：Gate `pass_with_limitations`；V-01 1,252/1,252/0；V-02 1,947 行 rel0；manifest/partition_status 可读 | ✅（带已知限制） |
| **P3.9 Data Agent 读契约加固** | V-02 阈值/7 日滚动真正执行；逐表信任+上游传播；SHA-256 综合 `contract_hash`；语义字典查询接口；生成器状态恢复；Tier0 去重 | ✅ 代码 + 新端到端验收 |
| **P3 数仓（T 层）** | P3.1–P3.9 已完成；10-29 Gate `pass_with_limitations`，V-01 521/521/0，V-02 非零但 G3 | ✅ 新基线形成 |
| **Airflow 运行环境** | Docker LocalExecutor + Postgres；两个 DAG 已暂停；scheduler→Spark 已验证 | ✅ |
| **观测控制面** | 生成器 profile/时长/故障注入/运行清单；统一 `observe_warehouse.sh` 与结构化观测 JSON | ✅ |
| **P4 完善** | Data Agent 架构 brainstorm 已开始；不嵌数仓链路，禁止执行重跑/修复 | 🚧 讨论中，未实现 |

## 5. 当前系统事实（速查）

### 5.0 数仓代码与构建（**2026-10-17 起**）

- **构建：Maven + scalac**（`pom.xml`），不用 sbt；`mvn -s RealTime/settings.xml -B clean package` → `target/crt-warehouse-0.1.0.jar`（薄 jar，首次需联网，之后 `-o` 可离线）。
- **旧代码**：14 个文件已移出到 `/tmp/crt_legacy_warehouse_2026-10-17/`（可恢复），仓库内不再保留旧指标逻辑。
- **现有 21 个源文件**：
  - `common/`：`Config`（配置）、`Contract`（契约 v2 全字段）、`Lake`（路径 + SparkSession）、`DeltaIO`（读写/覆盖/合并/VACUUM）、`HeatMath`（流批共用热度公式）；
  - `stream/`：`KafkaSource`、`OdsWriter`（流①）、`Tier0`（流②：热度/作弊/在线）、`RedisSink`；
  - `batch/`：`DimBuilder`、`DwdBuilder`、`DwsBuilder`（11 张表）、`SemanticDict`（40 指标元数据）；
  - `verify/`：`AttributionVerify`（V-01）、`HeatVerify`（V-02）、`GateRunner`（Gate v1 执行器）；
  - `app/`：`StreamApp`（三条查询）、`BatchApp`（`--step dim|dwd|dws|semantic|all`）、`VerifyApp`、`GateApp`、`VacuumApp`。
- **P3.8 已写代码**：Gate 执行器（20 条注册表 + Gate v1 定值）、`ops/run_manifest` / `ops/partition_status`、
  Airflow DAG（`infra/airflow/dags/`）、周维护 VACUUM；前置修正（`dwd/click`、R-06、V-01、Q-02、留存、`limitations`）已落。
- **P3.9 已完成并运行验收**：V-02 阈值/7 日窗口、逐表信任传播、SHA-256 综合 `contract_hash`、
  语义字典 Data Agent 查询字段、生成器互动/关注状态恢复、A1/A2 真值、Tier0 `event_id` 去重、线契约 `v2`。
- **观测控制面（2026-10-28）**：生成器支持三停止条件、随机种子、显式 chaos、坏 JSON/缺字段/非法枚举/错类型/重复/丢弃/暂停注入；
  `RealTime/config/observation-*.properties` 提供 normal/incidents/peak；每次运行写 `run_manifest/<run_id>.json`。
- **单次编排**：`infra/scripts/run_observation.sh` 一次完成构建 → 环境 → stream → generator → Batch → Verify → Gate → observe；
  `TARGET_DT=auto` 通过 ODS 下一天分区自动选目标日。常驻流作业已成为 compose `stream` 服务。
- **全容器运行**：Kafka/Redis/SeaweedFS/Spark/Generator/Airflow 均为容器；宿主只保留薄命令入口。
- **运行脚本**（都在 `infra/scripts/`）：`run_warehouse.sh <class> [args]`、`run_shell.sh <scala 脚本>`、
  `verify_p3_batch2.scala` / `verify_p3_batch3.scala` / `verify_p3_batch4.scala`（三批验收）、
  `check_ods.scala`（ODS 自检）、`diag_v01.scala`（归因定点诊断）。

**第二批实测（2026-10-17）**：`ODS 4,425 / 真值 254`；`dim_user 1,031（seed 1000 + registered 24 + inferred 7）`、
`dim_item 533`、`dim_edge 10,021`；`dwd/exposure 10,530 == Σitems`；`dwd/consume 919`（停留差中位 **0.003s**）；
`dwd/session 541`；U-02 注册口径 `2026-10-18: 23 / 2026-10-19: 1`。

**第三批实测（2026-10-17）**：DWS 行数 `funnel 276 / channel 46 / content 684 / author 219 / relation 18 /
session 2 / user 2 / quality 130 / position 48 / retention 4 / cold_start 33`；字典 **40 行**（业务 35 / 风控 1 / 验收 2 / 自检 2）。
对照：曝光 DWD=10,530 == DWS=10,530；点击 1,212 == 1,212（整体 CTR 0.1151 两处一致）；
关注 21 / 取关 1 两处一致；会话 541 一致；留存表含**零留存行**（offset=7 → 0.0）。

**P3.6 实测（2026-10-17）**：`tier0/heat_snapshot` **1,947 行 / 536 内容 / 窗口 60+1440**；
Redis：`crt:online.active_users=20`、`crt:heat:top` 有排行榜、每个 item 有 `crt:heat:<id>` hash（TTL 900s）；
`tier0/cheat_events` **13 行（A1=11 / A2=2）**。
✅ **语义点已定（2026-10-20）**：热度**不要求曝光**才计停留分；权重冻结为 `heat-v1`（`Warehouse_Tables.md` §7），
字典标注“可能出现曝光 0、仅凭停留上榜”的限制。

**P3.8 实测（2026-10-20）**：3 天重建后 `dim_user 1101 / dim_item 538 / dim_edge 10097`；
DWD `exposure 36,044 / click 4,202 / consume 3,185 / interaction 1,037 / relation 241 / publish 106 / session 1,920`；
DWS 11 表全部产出；V-01 **1,252/1,252/0**（6 类 × 3 天）、V-02 **1,947 行 rel_diff=0**；
10-18 Gate `pass_with_limitations`（退出 0），10-19 为部分日；`run_manifest` 80 行/4 次运行，
`partition_status` 26 行。详见 `Deployment_And_Operations.md` §7。

**P3.9 全容器观测基线（2026-10-28）**：ODS `13,807 / truth 1,069`，覆盖 10-28～10-30；
DIM `user 1,133 / item 590 / edge 10,092`；DWD `exposure 32,293 / click 3,781 / consume 2,872 /
interaction 977 / relation 228 / publish 90 / session 1,610`；11 张 DWS 与 40 行字典全部生成。
完整事件天 10-29 Gate `pass_with_limitations`：V-01 521/521/0；V-02 4,538 行，
`p50=0 / p95≈7.69% / max≈97.69%`，说明流快照后仍有迟到事件，按 RC-2-02 G3 记录。

### 5.1 环境

| 项 | 值 |
|---|---|
| 宿主 | WSL2 Ubuntu 26.04，12 核，内存硬顶 **7.58GB**（`.wslconfig` 已写 11GB，**待重启生效**） |
| Docker | Docker Desktop 29.7.2 + compose v5.4.0；**沙箱内无 docker 权限、无 sudo、网络受限** → 相关命令**必须走沙箱外（向用户申请）** |
| 容器 | `crt-kafka`（`apache/kafka:4.1.0`，9092 EXT / 19092 INT）、`crt-redis`（Redis Stack，6379）、`crt-s3`（`chrislusf/seaweedfs`，8333/9333/8888） |
| Spark | 镜像 `apache/spark:3.5.3`（**内置 Java 11**，与 Delta 3.2.1 精确配对），按需 `docker compose --profile tools …` |
| Airflow | **Docker LocalExecutor + Postgres**；入口 `http://localhost:8080`；账号 `admin/admin`；DAG 暂停；scheduler→Spark 已验证 |
| Generator | `crt-mock-generator:0.1.0` 容器；profile 由 `CRT_GENERATOR_PROFILE` 选择；运行清单落 `RealTime/data_files/run_manifest/` |
| S3 | 容器内 `http://seaweedfs:8333` ／ 宿主 `http://localhost:8333`；**path-style**；首次复制 `infra/seaweedfs/s3.example.json` → `s3.json`（实际文件已 gitignore） |
| lake root | **`s3a://crt-lake/`**（桶已建；aws-cli 验证建桶/上传/鉴权通过） |
| Kafka topic | `content_events`、`content_groundtruth`（各 3 分区，RF=1） |

**当前 Airflow 入口**：

- UI：`http://localhost:8080`（`admin` / `admin`）
- 启动：`docker compose -f infra/airflow/docker-compose.yml up -d`
- 停止：`docker compose -f infra/airflow/docker-compose.yml down`
- 备用宿主 venv：`infra/airflow/run_local.sh` / `stop_local.sh`
- 运行状态：`.airflow-venv/` + `.airflow-home/`（均已 gitignore）
- Docker 备选：`infra/airflow/docker-compose.yml`；本次 Docker Hub 拉取 `unexpected EOF`，未采用

### 5.2 生成器（`/RealTime`，已完成 + P1.6 v2）

- 构建 `mvn -s settings.xml -B clean package`；运行 `java -jar target/mock-generator-1.0-SNAPSHOT.jar`。
- 配置 `src/main/resources/config.properties`；**优先级：`-Dkey=value` > `-Dconfig.file` > classpath > 内置默认**。
- 种子：**1000 用户 / 500 内容 / 10000 关注边**（`.seed_version` 版本化重建，落在 `data_files/`）。
- 实测 **约 70 EPS**；样本见 `project_specs/mock_generator/samples/`。
- **P1.6 实测（2026-10-17，`-Dmock.user.registered.per.day=40`）**：
  - 时钟：1 事件天 ≈ **100 真实秒**；`leave_ts−enter_ts` 与 `duration_sec` 之差 **中位 0.003s / p95 ≈60s / 最大 90s**（90s 恰为 B1 乱序上限）；
  - 跨用户时间线：回退 **p95 ≈18 分钟 / p99 ≈27 分钟**（长尾极端值 <1% 来自乱序与注册补齐滞后）；
  - 会话形态：每会话 `enter` 中位 3、最大 11–12；会话时长中位 ≈90s；
  - 新用户：注册速率精确（7.45 事件小时 × 40/天 = 12 人）、id 唯一且从 300001 起、**全部无 `session_id`**、属性齐备、**CSV 行数 == topic 注册事件数（12/12）**、重启不重发 id；
  - 事件天密度：约 1,200–2,000 事件/天。

### 5.3 数仓实现现状（旧代码已移出，新实现已落地）

旧版 14 个文件（`RealTimeApp` 的 8 条流查询、Bloom 判新、memory sink、`D:/spark` 硬编码等）
已按 `Warehouse_Design.md` §4 处置表移出，备份在 `/tmp/crt_legacy_warehouse_2026-10-17/`。新实现的对应关系：

| 旧组件 | 现在由谁承担 |
|---|---|
| `KafkaReader` | `stream/KafkaSource`（配置化、双 topic） |
| `DataCleanser` | `common/Contract`（契约 v2 解析） |
| `DeltaSink` | `common/DeltaIO`（S3A + Delta 3.2.1，含 `replaceWhere`/MERGE/VACUUM） |
| `DataEnricher` | `batch/DwdBuilder`（曝光展开 + 广播维表） |
| 8 个流聚合 | `batch/DwsBuilder`（11 张主题表，批侧） |
| `NewUserAggregator` + `BloomFilterService` | `DimBuilder` 的 `first_seen` + U-02 双口径 |
| `CheatDetector` | `stream/Tier0`（A1/A2，落 `tier0/cheat_events`） |
| `RedisSink` | `stream/RedisSink`（只写聚合后的 top-N，pipeline + TTL） |
| `AppConfig` / `RealTimeApp` | `common/Config` + `app/{StreamApp,BatchApp,VerifyApp}` |

### 5.4 Data Agent 当前交接状态（**先读讨论记录再行动**）

- **阶段**：架构 brainstorm；没有 Agent 工程、服务、图数据库 Schema 或 RAG 索引落地。
- **必读**：`project_specs/data_agent/Data_Agent_Discussion.md`。
- **当前工作约束**：
  - 不做 Text2SQL；
  - 不做重跑、修复、补数、回滚等执行动作；
  - 不把 shadow execution 当作当前需求；
  - “数据能不能信”的 Agent 输出只作参考，Gate/数据事实仍是权威；
  - 事故与工作记录需要持久化，但必须与权威数据隔离；
  - 历史根因和最终证据由人事后确认，Agent 不生成已裁决事实。
- **候选技术方向（未冻结）**：LangGraph 负责工作流，Neo4j 负责血缘/事故关系，DuckDB 作为 Delta 的轻量只读探针，
  Airflow/Delta/Spark/日志是事实源；PostgreSQL、Object Storage、RAG 和 GraphRAG 仍待后续裁剪。
- **当前缺口**：具体数据范围、问题路由契约、事故记录模型、知识索引和 RAG 边界均未定。
- **禁止误入**：不要因为讨论了 LangGraph/Neo4j/DuckDB 就默认开始建工程；下一步必须先由用户确认需求与架构范围。

## 6. 风险与坑

1. ~~`Delta over S3A` 未验证~~ → **已消除（2026-10-16，14/14 通过）**：默认 committer 可用、multipart 生效、VACUUM 真删、流式 checkpoint 重启零重复。
   **新增两条硬约束**：① **Delta 与 Spark 补丁号必须精确配对**（3.2.1 ↔ 3.5.3；3.5.1 上 MERGE 会 `NoSuchMethodError`）；
   ② **`replaceWhere`/`DELETE` 只改日志，物理回收靠 `VACUUM`**（批侧重算后必须配 VACUUM）。
2. **旧数仓代码与新契约不兼容** → P3 是"改造 + 新建"，不是"小修"。
3. **权限**：docker / 联网 / 沙箱外写入**都要向用户申请**（已口头授权"一切权限直接向我申请"）。
4. **内存 7.58GB 硬顶**；`.wslconfig` 已写但**未生效**（需 `wsl --shutdown`，会中断会话）。
5. **容器内 Java 11** → 编译目标取 **JVM 8/11**，否则 jar 在 Spark 容器里跑不起来。
6. **实际渠道占比偏离基线**（正常现象）：空候选池回落到 `explore`；成熟度上升会提高关注流权重。
7. **like/dislike 不可逆**是**显式模型假设**（要撤回＝契约变更）。
8. **上一个窗口上下文近满** → 细节以**文档**为准；冲突时以 `Project_Global_Spec.md` 为准。
9. **事件时间的三条不变量**（改了就会静默错，P1.6 踩过）：① 时间戳取自**用户时钟**；② 全局时钟**只由模型活动推进**，
   不绑真实时间（否则进度随机器速度波动、跨用户回退十几小时）；③ **并发会话必须限流**（`mock.session.max.open`），
   否则单个会话要很久才轮到下一步、被全局时钟甩开。
10. **关停要 flush**：`timeout/kill` 时 main 的 `finally` 不保证执行 → 必须在 shutdown hook 里 flush/close，
    并先置 `stopping` 标志让主循环退出；另注意"过期会话收口"这类提前 return **不能丢掉已生成的事件**
    （曾因此丢掉近一半 `user_registered`：CSV 已落盘、topic 没收到）。
11. **S3A 参数必须带 `spark.hadoop.` 前缀**（写进 `SparkSession.builder.config`）。直接传 `fs.s3a.endpoint`
    会被静默忽略 → S3A 去连真实 AWS，在无网环境里**挂住而不是报错**（排查起来非常费时）。
12. **spark-shell 里的会话是预先建好的**：`builder.config(...)` 会被忽略；`Spark.session` 现在会把
    Hadoop 参数补写到活动会话上，但 Delta 扩展仍需 `--conf` 显式传入（`run_shell.sh` 已处理）。
13. **被 kill 的流作业会污染 checkpoint**（`CONCURRENT_STREAM_LOG_UPDATE`）：`StreamApp --reset` 只清本作业
    自己的 checkpoint；同时先确认没有残留的 `crt-spark-run-*` 容器。
14. **本地容器可能静默停掉**：本次 `crt-s3` 就不在运行（`docker compose ps` 才发现），表现为"写 S3 卡住"。
    排查顺序：`docker compose ps` → 容器内 `getent hosts seaweedfs` + `/dev/tcp` 探活。
15. **批层必须按 `event_id` 去重**：Kafka 至少一次 + checkpoint 重置/重放会让 ODS 出现重复事件；
    DWD 按 `event_id` 去重；DIM 用 `row_number`/主键去重（效果等价，不去重会把 `like` 从 208 放大到 416）。
16. **V-02 新基线已非零**：虽然流批共用 `HeatMath`，但当前 Tier0 快照形成后，乱序事件仍可能继续进入 ODS；
    同锚点批侧重算会包含这些迟到事件，因此 10-29 出现 `p95≈7.69% / max≈97.69%`。Gate 按 G3 limitation 记录，
    不能再用“必然为 0”描述当前运行事实。
17. **交互类事件的 `channel` 靠 `batch_id` 反查曝光批次**：事件本身不带渠道；不补这一步，
    V-01 的交互类命中率会是 0（关注类则天然带 channel，所以"只有关注对得上"是个典型假象）。
18. **留存 cohort 已修（P3.8）**：改为注册日/首见日 + `cohort_type`；修复前生成的 `dws/retention_daily` 需重跑 DWS 后再用。
19. **`user_daily` 已加窗口过滤（P3.8）**：reg/obs 先过滤到 `sinceDt` 再写 `replaceWhere`；仍需运行验收确认谓词外行已消除。
20. **Airflow 当前是 Docker 模式**：LocalExecutor + Postgres，scheduler 经 Docker socket 提交 Spark；两个 DAG 已暂停。
    宿主 venv/SQLite 仅保留为备用调试路径。
21. **Docker Hub 直连仍可能失败**：本次先通过可用镜像源拉取并重打标准标签，再用 compose 启动；
    镜像源只是本机网络故障下的恢复路径，不写死到 compose。
22. **生成器状态恢复从 P3.9 起生效**：`graph_state.log` / `interaction_state.log` 只记录新运行产生的变更；
    首次启用前已有的历史互动与运行期关注无法自动补写，需要从 ODS 回放或重建数据。
23. **P3.9 已重跑端到端**：10-29 Gate `pass_with_limitations`；旧 10-18 数字仅作历史基线。
24. **当前工作树尚未提交**：P3.9 代码、Airflow 文件、Data Agent 讨论文档均在 dirty worktree 中。
    新会话必须保留这些改动，禁止 `git reset --hard` / `checkout --`；先看 `git status` 和 `git diff`。
25. **正式观测生成器 run 的清单 `stop_reason=null`**：这是修复前会话池收尾异常留下的现场；
    修复后 `generator-fix-smoke` 已以 `target_event_days` 正常退出。数据语义未变，但重做全新基线时应重新生成主 run。
26. **观测报告表行数是全历史当前表快照**：`observe_warehouse.scala` 报告每表总行数，并在有 `dt` 时给日期范围；
    阅读时要结合 Gate 目标日，不能把总行数当单日指标。
27. **`publish_final` 很严格**：除 semantic 字典外，本轮所有表因至少继承一条上游 limitation 而为 false；
    Data Agent 必须解释限制，不能把 `pass_with_limitations` 简写成 pass。

## 7. 下一步（按执行边界）

> **P3.1–P3.9 已完成**（三批验收 + P3.8 编排 + P3.9 读契约加固；数字见 §5.0）。
> **P3.9 新基线（2026-10-28）**：完整事件天 10-29 Gate 返回 `pass_with_limitations`（退出 0）；
> V-01 521/521/0，V-02 4,538 行且因迟到数据呈非零差异；`partition_status` 28 张规范表全部逐表输出。
> ① Gate 执行器（20 条注册表 + Gate v1 定值，写 manifest/report，退出码 0/1；G1 内部等待）；
> ② `ops/run_manifest` + `ops/partition_status`（Delta）；
> ③ Airflow `daily_batch` DAG（`DR → dim → dwd → IV → dws → semantic → CV/QL → verify → RC → partition_status`）；
> ④ 周维护/人工批准的 VACUUM。
>
> **新基线限制**：10-29 DR-3 历史日差最后 55 秒水位（G2）；DR-4-01 历史样本不足（G2）；
> retention registered cohort 样本不足（QL-2-01 G2）；V-02 迟到数据差异（RC-2-02 G3）；
> 累计注入用户文件与当前 ODS 保留窗口不一致（FX-2 G4）。
> **观测控制面已完成**：生成器 profile、运行时长、显式故障/坏数据、运行清单、容器镜像、
> `observe_warehouse.sh` 和 `Full_Observation_Runbook.md` 均已落地。
> **可执行下一步**：基于 `pass_with_limitations` 新基线继续 Data Agent 需求与架构收敛；
> 若要形成“完全干净的单一生成器 run”，应在保留代码修复的前提下清空本轮测试数据后重跑。
> **Data Agent 下一步**：只继续讨论需求、边界与架构，先读 §5.4 和
> `project_specs/data_agent/Data_Agent_Discussion.md`；未获用户确认前不创建工程。
> Airflow Docker 已运行，但两个 DAG 仍暂停。手工触发示例：
> `{"dt":"2026-10-29","days":3}`。

⚠️ **协作约定不变：动手（改代码/跑任务）前先说一声；docker / 联网 / 沙箱外写入直接申请权限。**

## 8. 如何与用户对齐（**协作约定，务必遵守**）

### 8.1 用户是谁、在意什么

- **个人练手项目**（非生产级），但要求**合理、站得住**；他反复强调"合理性"与"为应用层托底"。
- 他关心**业务合理性**：曾专门指出"同一帖子又赞又踩、可反复点赞"不合理，要求**从源头（生成器）修**。
- 他关心**上下文卫生**：主动要求清理噪音文档、归档旧文档、并写这份交接书。

### 8.2 沟通方式（他明确表达过的偏好）

- **讨论式、多轮 brainstorm**，不是"下命令—执行"；他喜欢**先听我的推荐值/倾向**，再自己权衡拍板（"你先给推荐值，我再针对性权衡"）。
- **不要过度纠正/评判**：他原话"我们是探讨，不是评判某事某物"、"不要抛过多的纠正"。→ 有分歧时**陈述事实与 tradeoff**，让他选。
- 他会**自己 review 文档**并给出修正；所以**结论必须落成文档**，不要只留在对话里。

### 8.3 我的表达规范（已被验证有效）

1. **先给结论**，再给理由与代价（tradeoff），最后给"需要你定的点"。
2. 不确定的一律标 **「待定」**；**不要自行拍板**；可以做合理假设，但**必须显式说明**。
3. 每个决策**落到文档**；文档要**收束**（不散落、不重复）——他反感文档分散。
4. 涉及删除/归档：**先列清单给他决议**，再执行；执行后**报告删了什么、可否恢复**。
5. 用**表格**承载映射/对照（他接受度高），避免长段落。

### 8.4 授权与权限

- docker / 联网 / 沙箱外写入 → **直接申请**（`require_escalated` + 简短说明），不要因怕打扰而跳过。
- 破坏性操作（`rm`、重建 topic、清数据）→ 说明目标与影响后再做；能用**移动/可恢复**方式就不用删。
- **动手（改代码/跑任务）前先确认**：他会说"可以开始"。

### 8.5 事实与冲突处理

- **代码是唯一事实**；文档与代码冲突 → 以代码为准，并**回写文档**。
- 文档之间冲突 → 以 `Project_Global_Spec.md`（框架）→ `Metric_Definitions.md`（口径）→ `Mock_Source_Spec.md`（数据）为序。
- 引用旧文档前先确认它不在 `_deprecated/`。

## 9. 文档地图（权威性）

| 文档 | 状态 |
|---|---|
| `AGENTS.md`（本文） | ✅ 交接入口 |
| `project_specs/overall_spec/Project_Global_Spec.md` | ✅ 框架级规约（**最高权威**） |
| `project_specs/overall_spec/Metric_Definitions.md` | ✅ 指标口径（v1.3 冻结；热度/负反馈/留存/E1/QL 总量基线已定） |
| `project_specs/overall_spec/Metric_Implementation_Guide.md` | ✅ **指标实现说明（给"人"看）**：40 指标逐个"定位/怎么算/读哪列/坑"+ Gate v1 摘要；**设计门禁前先读这份** |
| `project_specs/overall_spec/Gate_Grading_Scheme.md` | ✅ **Gate 分级方案 v3.2（权威）**：三层模型 + 六类 × 层级 + 20 条注册表 + Gate v1 定值 + 双产物/逐表 `partition_status` |
| `project_specs/overall_spec/Warehouse_Design.md` | ✅ T 层设计（v1.2；P3.9 已完成运行验收） |
| `project_specs/overall_spec/Warehouse_Tables.md` | ✅ 逐表 DDL + Tier0 热度参数（v1.2：P3.9 语义查询字段、逐表信任与上游限制传播） |
| `project_specs/overall_spec/Deployment_And_Operations.md` | ✅ **交付主文档**：组件落点、启动/停止、runbook、配置、调试地图、P3.8 与 P3.9 验收记录 |
| `project_specs/overall_spec/Full_Observation_Runbook.md` | ✅ **逐步教学手册**：一次运行状态机、每阶段命令/预期状态/通过条件、profile、自动选日、整体观测、重置与排错 |
| `project_specs/data_agent/Data_Agent_Discussion.md` | 🧭 Data Agent brainstorm 记录；非实现方案、非冻结契约 |
| `project_specs/mock_generator/Mock_Source_Spec.md` | ✅ 生成器唯一正本（v0.11；契约 v2 + 运行计划 + chaos/坏数据配置 + 状态恢复） |
| `project_specs/mock_generator/samples/`（`README.md` + `SCHEMA.md`） | ✅ 契约样本 + 逐文件字段字典（CSV 无表头） |
| `project_specs/mock_generator/Generator_Refactor_Checklist.md` | ✅ 生成侧清单（A–F 全完成） |
| `RealTime/docs/Pipeline_And_Architecture.md` | ✅ 生成器链路与架构说明 |
| `RealTime/docs/Testing_And_Config_Guide.md` | ✅ 生成器测试与配置指南 |
| `infra/README.md`、`infra/docker-compose.yml`、`infra/seaweedfs/s3.example.json` | ✅ 环境与凭证模板（实际 `s3.json` 不提交） |
| `infra/airflow/` | ✅ DAG + 本机启停脚本 + Docker Compose 备选；UI 上次已验证，当前进程需确认，DAG 暂停 |
| `RealTime/**`（生成器源码）、`src/**`（数仓新实现，21 文件） | ✅ 代码事实 |
| `项目解构与链路基线.md` | 🕰️ 历史快照（改造前，**旧代码已移出**） |
| `README.md` | ✅ 入口页（2026-10-28 同步 P3.9、Data Agent 讨论和 Airflow 状态） |
| `project_specs/_deprecated/**` | ⛔ 弃用，勿引用 |
| （已移出仓库）`my_resume.md` 等 | 🗑️ 在 `/tmp/crt_removed_2026-10-16/` |

## 10. 红线（不可越）

- **不建应用层**；业务真实性由生成器承担。
- **先冻结契约，再改 T 层**（契约已冻结）。
- **批为真值、流为近似**；批必须能**幂等重写**已写分区、**可按窗口重跑**。
- **不做终身旅程**；**不做多触达归因**（只 last-touch）。
- **不引用** `_deprecated/**`；`README.md` 已于 2026-10-20 重写为入口页。
- **未定项先问用户**，别自行拍板。

## 11. 变更记录

| 日期 | 变更 |
|---|---|
| 2026-10-16 | 首版（接管说明）：环境、生成器文档集结、A 组草案 |
| 2026-10-16 | 多次增量：契约冻结、P1.1–P1.5、行为语义补丁、口径冻结、P3 设计、逐表 DDL、环境补齐（SeaweedFS） |
| 2026-10-16 | **整体重写为交接书**（Delete + Add）：此前版本已累积过期/自相矛盾段落（如 §3 并存"已完成"与"未开始"、§5 仍写"本地 FS"）；新增 §1 目标、§3 决策台账、§6 风险、§8 协作约定 |
| 2026-10-16 | **`Delta over S3A` 冒烟通过（14/14）**：新增 P2.5 节点；引擎定版 **Spark 3.5.3 + Delta 3.2.1**（镜像 3.5.1 → 3.5.3，因补丁号必须配对）；新增 `infra/jars/pom.xml`、`infra/scripts/delta_s3a_smoke.scala`、`infra/README.md` §5；§6 风险 1 消除并改写为两条硬约束；P3 状态改为"进行中" |
| 2026-10-16 | **口径收束 + 契约 v2 定案**：`Metric_Definitions.md` 升 **v1.1**（新增 §10 实现级设计表：40 指标 × 口径/粒度/证据/性质 + 7 条裁定记录）；`Mock_Source_Spec.md` 升 **v0.7**（§6.8 模拟时钟、§6.9 新用户注入、§3.2 停留语义澄清、§7 v2 配置键）；`Warehouse_Tables.md` 更新 `dim/user` 列、`dwd/consume` 与 `dwd/session` 时长口径、`dws/user_daily` 双口径，新增 `dws/retention_daily`、`dws/cold_start_daily`；新增决策 25–29 与 **P1.6 前置节点** |
| 2026-10-17 | **P1.6 完成**：模拟时钟（全局时钟=模型活动、用户时钟承载 ts、并发上限 30、过期收口、落盘续跑）+ 新用户注入（`user_registered` 无 `session_id`、id 落盘 append、注册后首次打开）+ 关停 flush/停止标志。实测：1 事件天 ≈100 真实秒；停留自洽中位 0.003s；跨用户回退 p95 18min/p99 27min；注册 12/12 一致；重启不重发 id。`Mock_Source_Spec.md` §6.8/§7 按最终实现重写 |
| 2026-10-17 | **P3.0 完成**：构建改为 **Maven + scalac**（`pom.xml`，薄 jar 27KB，可离线）；新增 `common/{Config,Contract,Lake,DeltaIO}`（契约 v2 全字段、S3A/Delta 会话工厂、`replaceWhere`/MERGE/VACUUM 纪律）；14 个旧文件移出到 `/tmp/crt_legacy_warehouse_2026-10-17/`；`build.sbt` 同步版本但不再是正式入口；V-02 落 `tier0/heat_snapshot` 已写入设计 |
| 2026-10-17 | **P3.1–3.3 完成**：`stream/{KafkaSource,OdsWriter}`、`batch/{DimBuilder,DwdBuilder}`、`app/{StreamApp,BatchApp}`；验收 `exposure == Σitems`、停留差中位 0.003s、`dim_user` 三类来源齐全；新增 `infra/scripts/{run_warehouse.sh,run_shell.sh,verify_p3_batch2.scala,check_ods.scala}`；修 4 个坑（S3A 前缀、commons-pool2、Kafka 元数据列被丢、checkpoint 污染/容器残留 + SeaweedFS 停摆） |
| 2026-10-17 | **P3.4–3.5 完成**：`batch/DwsBuilder`（11 张表，含 U-02 双口径、U-03 留存、A-04 冷启动、R-05 新粉活跃）+ `batch/SemanticDict`（40 指标元数据，含"性质"列）；`BatchApp` 支持 `--step dim / dwd / dws / semantic / all`；验收脚本 `verify_p3_batch3.scala` 四项对照全 OK；两处修正：DWD 补 `hour` 派生列、**留存表改左连接**（零留存 cohort 不再消失）；`DeltaIO` 写入开启"加列合并" |
| 2026-10-17 | **P3.6 完成**：`stream/{Tier0,RedisSink}`；`StreamApp` 第三条查询（ODS→Tier0）；热度 1h/24h 落 `tier0/heat_snapshot` + Redis（TTL 900s）、在线活跃落 Redis、A1/A2 作弊落 `tier0/cheat_events`（13 行）；新增 `verify_p3_batch4.scala`；踩坑：**上次运行的 spark 容器残留会占着 checkpoint**（`batch N doesn't exist`），`--reset` 现含 tier0，并需先 `docker rm -f` 残留容器 |
| 2026-10-17 | **P3.7 完成**：`common/HeatMath`（流批共用热度公式）+ `verify/{AttributionVerify,HeatVerify}` + `app/VerifyApp`；V-01 六类转化 × 3 天 **100%**（链路修复：交互类 `channel` 由 `batch_id` 反查曝光批次）；DIM/DWD 增 `dropDuplicates("event_id")`（重放去重）；V-02 1,947 行、rel_diff=0（定义一致性检查，见 §6 第 16 条） |
| 2026-10-18 | **`Metric_Implementation_Guide.md` 升 v3（729 行）**：新增 **§1 宏观链路**（一条事件→指标的七阶段、批跑顺序与影响面、指标来源地图）；**公式全部改写为文字**；**正文零表名/字段名**（校验：附录前 0 次），表结构集中到附录 A；新增 FAQ、排错地图、按可靠程度排序的 Gate 清单；40/40 指标均有"它算什么/怎么算/能信吗/Gate"四段 |
| 2026-10-20 | **Guide v4 + 决策 31**：新增 §0.5「数仓与下游的边界」（以分发漏斗为例判定"是否抢功"）；答复用户 4 处补充疑问（点击是否去重、渠道取值与来源、作者发布概率算法、归因拆维度）；确认漏斗属"描述统计"、不越界，越界形态为打分/建议/因果/个性化 |
| 2026-10-20 | **Guide v5 + 决策 32**：§7 重写为 **Gate 分级体系**（G0–G4 处置 × DR/IV/CV/QL/RC/FX 六类 × 层级阶梯 + 20 条注册表 + 调度器退出码映射）；采纳用户建议，把"停留自洽"从硬门禁下调为 **FX-G4**（模拟器自洽 ≠ 生产数据质量）；新增决策 32 |
| 2026-10-20 | **Gate 方案独立成文（决策 33）**：新增 `Gate_Grading_Scheme.md`（G0–G4 × 六类 × 层级 + 20 条注册表 + manifest 字段 + 调度器映射 + agent 可调边界/参考起点 + 6 条待定项）；Guide §7 收敛为摘要+指针 |
| 2026-10-20 | **交接级文档对齐**：`AGENTS.md` §0/§5.0/§5.1/§5.3/§7 按实际进度重写（18 个源文件、旧代码处置映射、Spark 3.5.3、下一步=P3.8）；`Warehouse_Design` §3.4/§6 换成实际类与"P3.1–3.7 已落地"；`Warehouse_Tables` 回写实现字段（ODS 元数据列、`hour`、verify 分项、字典 `nature`）；`Metric_Definitions` 标注 40 指标已实现；`infra/README` 新增 §5.5 数仓构建与运行 |
| 2026-10-20 | **Gate 方案 v2**（对照 HSADL 后合并）：引入**证据结论（pass / pass_with_limitations / needs_confirmation / blocked）× 处置动作（G0/G1/G1h/G2/G3/G4）× 审批级别（safe_auto / needs_approval(action_id)）**；新增双产物（`gate-report.md` + `.json`）、`ops/partition_status` 分区信任标记、**附录 C data agent 读契约**（负责什么/按什么负责/怎么读/七条硬规则）；HSADL 对照降为附录 A |
| 2026-10-20 | **P3.8 设计定稿 + Gate v1 冻结（决策 34–39）**：调度器改 **Airflow（LocalExecutor + Postgres）**；Gate v1 冻结 QL ±3pp/±5pp、DR-3 5min×3、G2 仅 QL/DR-4、V-02 1e-9、V-01 精确率+召回率；语义口径定稿（热度不要求曝光、负反馈 `dislike+bounce`、留存注册日/首见日 + 1/3/7、E1 短停留）；前置修正 `dwd/click`、R-06、Q-02 拆分、字典 `limitations`；`Warehouse_Design`/`Metric_Definitions`/`Guide`/`Warehouse_Tables`/`Mock_Source_Spec`/`Pipeline_And_Architecture` 同步升版/回写 |
| 2026-10-20 | **P3.8 代码落地（编译通过）**：新增 `verify/GateRunner`、`app/GateApp`、`app/VacuumApp`；DWD 新增 `dwd/click`、`enter_profile` 入 relation；DWS 点击改读 DWD、Q-02 拆分、U-03 cohort 修正、`user_daily` 窗口过滤；V-01 改左连接精确率/召回率；字典新增 `limitations`；新增 Airflow `crt_daily_batch`/`crt_weekly_vacuum` DAG 与 `infra/airflow/README.md`；`mvn -o -DskipTests package` 通过 |
| 2026-10-20 | **P3.8 运行验收完成**：修复 DWS 点击投影漏 `category`；10-18 Gate `pass_with_limitations`（退出 0）；V-01 1,252/1,252/0、V-02 1,947 行 rel0；运行校准 DR-3 历史日 G2、QL 基线 20%/14%/0.5%、V-02 无快照 G3；新增 `Deployment_And_Operations.md`（组件落点/runbook/调试地图/验收记录）；新增 `verify_p3_batch5.scala` |
| 2026-10-20 | **README 重写**：旧版（250 用户 / SBT / 28 指标 / RedisBloom）已备份到 `/tmp/crt_readme_old_2026-10-20.md`；新版改为当前入口页，链接 `AGENTS`、`Deployment_And_Operations`、口径/Gate/表结构，并列出快速开始与已知限制 |
| 2026-10-20 | **样本字段字典 + 真相层图**：新增 `samples/SCHEMA.md`（CSV 无表头、JSONL 逐字段、truth 字段）；README / Deployment / Warehouse_Design / Pipeline / Mock_Source_Spec 的链路图补上 `content_groundtruth → ods/groundtruth → V-01` 真相层旁路 |
| 2026-10-20 | **Tier0 显式化**：Deployment/README/Warehouse_Design/Pipeline 图拆出 Tier0 的三条输出（F-07 热度、Q-01 作弊、U-04 在线）；补充 Tier0 输入只读 ODS、每微批重算 24h、Redis 非真值、作弊跨批去重等说明 |
| 2026-10-21 | **Gate/Airflow 代码注释**：为 `GateRunner`、`GateApp`、`VerifyApp`、`AttributionVerify`、`HeatVerify`、`VacuumApp`、两个 DAG、`run_warehouse.sh` 增加行级中文注释；逻辑未改，Scala 编译与 Python AST 检查通过 |
| 2026-10-24 | **Airflow 本机环境 + 交付文档对齐**：Airflow 2.10.5 venv/SQLite/SequentialExecutor 已运行在 `localhost:8080`，DAG 暂停；新增 `run_local.sh`/`stop_local.sh` 与 Docker Compose 备选；README/Deployment/AGENTS 更新实际状态；完善 `.gitignore`、`s3.example.json`、样本字段字典和真相层/Tier0 链路图 |
| 2026-10-24 | **P3.9 Data Agent 读契约加固（代码）**：V-02 阈值和 7 日滚动实际生效；`partition_status` 改为逐表输出、全量替换单日状态并继承 ODS 限制；`contract_hash` 扩为 SHA-256 综合指纹；语义字典新增 `query_table/query_mode/read_only/contract_version`；生成器恢复互动/关注图状态并让 A1/A2 产真值；Tier0 计算前按 `event_id` 去重；线事件契约统一为 `v2`。数仓/生成器离线编译与状态冒烟通过，端到端 Gate 待重跑 |
| 2026-10-24 | 新增 **Data Agent 讨论记录**：收口当前痛点、用户工作倾向、候选定位、功能/读写边界、链路角色和开放问题；明确为 brainstorm，不代表实现或冻结决策 |
| 2026-10-27 | **Data Agent 讨论补充**：记录 LangGraph + Neo4j 之外的候选技术栈、DuckDB/Delta 的轻量探针定位、Neo4j 图/全文/向量检索边界、历史案例的纠错作用、“分诊”的问题路由含义，以及自动保存工作记录与人工确认分层；知识索引仍后置 |
| 2026-10-28 | **交接文档收口**：修正生成器/Gate 文档版本，明确 P3.9 仍待端到端重跑；新增 §5.4 Data Agent 交接状态，记录“讨论已开始、工程未启动、禁止执行动作”；Data Agent 讨论文档增加新会话入口与持久化边界，README 增加讨论入口和当前限制 |
| 2026-10-28 | **P3.9 新基线 + 全容器观测控制面**：恢复空 Docker 环境；生成器新增运行时长/事件天/最大事件数、随机种子、显式 chaos、坏数据/重复/丢弃/暂停、运行清单和容器 profile；修复 Tier0 空 ODS 等待、生成器会话池收尾、Gate QL 行读取；Docker Airflow 提交 Spark 已验证；新增整体观测脚本与教学手册；10-29 Gate `pass_with_limitations`，V-01 521/521/0，V-02 因迟到数据非零 |
