# Mock 数据源 —— 链路与架构说明

> 归属：`/RealTime`（模拟数据源脚本）。
> 配套：算法与契约的**权威定义**见 `project_specs/mock_generator/Mock_Source_Spec.md`；本文侧重**说明性**叙述。
> 版本：2026-10-16（2026-10-20 事实回写：P1.6 模拟时钟、P3 已落地）

---

## 1. 环境架构

```
┌──────────────────────────── WSL2 (Ubuntu) ──────────────────────────────┐
│                                                                         │
│  [本机进程] Mock Generator (Java)           [Docker] crt-kafka           │
│   ① 启动时生成种子（缺失/版本不符时）         apache/kafka:4.1.0          │
│   ├──► data_files/*.csv + .seed_version      · KRaft 单节点（无 ZK）     │
│   ② 会话旅途产事件                            · EXTERNAL :9092（本机用）  │
│   ├──► content_events（行为流）               · INTERNAL kafka:19092     │
│   └──► content_groundtruth（归因真值）        · CONTROLLER :9093         │
│                                                                         │
│  [Docker] crt-redis       [Docker] crt-s3（SeaweedFS）                   │
│   Tier0 缓存（非真值）      s3a://crt-lake/（Delta 表）                   │
└─────────────────────────────────────────────────────────────────────────┘
          │ content_events                    │ content_groundtruth
          ▼                                   ▼
   [流①] ODS/events（证据层）          [流①] ODS/groundtruth（真相层）
          │                                   │
   ┌──────┴───────────────┐                    │ 只用于 V-01
   ▼                      ▼                    ▼
[流②] Tier0           [批] DIM→DWD→DWS→semantic
 输入只读 ods/events       │
 每微批重算最近 24h        ├─► dwd/interaction + dwd/relation
 ├─ F-07 1h/24h           │        │
 │  ─► heat_snapshot       │        ▼
 │  ─► Redis heat           │   verify/attribution（V-01）◄─ ods/groundtruth
 ├─ Q-01 A1/A2 ─► cheat_events
 └─ U-04 15min ─► Redis online
          │
          └─ heat_snapshot ─► verify/heat（V-02）
                                      │
                                      ▼
                     GateApp → ops/run_manifest / partition_status / reports
```

- **全容器运行**：Kafka/Redis/SeaweedFS、生成器和 Spark 均由 `infra/docker-compose.yml` 启动；
  宿主 jar 仅作为本地调试备用，连接 EXTERNAL `localhost:9092`。
- **当前运行方式（2026-10-28）**：生成器也提供 `crt-mock-generator:0.1.0` 容器，默认通过 INTERNAL
  `kafka:19092` 发送；`RealTime/config/observation-{normal,incidents,peak}.properties` 可选择观测 profile。
- **两个 topic**：主事件流 `content_events`、归因真值 `content_groundtruth`（各 3 分区、RF=1）。
- **数据目录**：`data.dir`（默认 `RealTime/data_files`），存放三份种子 CSV。
- 环境细节与启停见 `infra/README.md`；操作步骤见 `Testing_And_Config_Guide.md`。

---

## 2. 链路（数据流）

```
[种子生成]  →  dim_user_profile.csv / dim_item.csv / dim_follow_seed.csv   （t0 静态快照）
                        │
                        ▼  启动时载入内存（DataManager）
[运行期]  会话旅途：IDLE → 曝光 → 点击 → 消费 → 互动/关注 → …
                        │
        ├──────────────┐
        ▼              ▼
  content_events   content_groundtruth
（行为流：曝光/点击/消费/互动）  （[真相层] 每次转化的真实来源路径）
        │              │
        ▼              ▼
  ods/events      ods/groundtruth
        │              │
        ├─► Tier0 ─► Redis + heat_snapshot ─► verify/heat（V-02）
        │
        └─► 批：DIM → DWD → DWS → semantic ─► verify/attribution（V-01）◄─ truth
                                                   │
                                                   ▼
                                    GateApp → ops/run_manifest / partition_status
```

要点：

- 生成器**只负责产数据**，不参与计算；数仓侧 P3.1–3.8 已落地（见 §7 契约冻结）。
- **真值与事件分流**：真值走独立 topic —— 若混进事件流，归因校验就变成了"抄答案"（见 §5.6）。

---

## 3. 数据结构

### 3.1 种子文件（`|` 分隔，无表头）

| 文件 | 列 | 说明 |
|---|---|---|
| `dim_user_profile.csv` | `user_id` \| `interest_tags`（逗号分隔，**可空＝冷启动**） \| `level` \| `is_creator` \| `primary_category` \| `post_rate_level` | 用户＝潜在创作者（**作者 ≡ 用户**，不设 `dim_author`） |
| `dim_item.csv` | `item_id` \| `author_id` \| `category` \| `tags`（逗号分隔） \| `content_len` \| `published_ts` | 内容；`content_len` **归维表**，不由事件注入 |
| `dim_follow_seed.csv` | `follower` \| `followee` \| `ts` | 有向关注边（t0 图种子） |
| `.seed_version` | 版本号 | 种子格式版本；不一致时**自动重建**全部种子 |

### 3.2 主事件流（`content_events`）

**通用信封（所有事件共有）**

| 字段 | 说明 |
|---|---|
| `event_id` / `user_id` / `event_type` / `ts` / `session_id` / `platform` | `ts`＝ISO-8601（GMT+8，`SSSX`）；`platform ∈ {ios, android, web}` |
| `schema_version` | 线事件契约版本（P3.9 起当前 `v2`；旧数据可为 `v1`） |

**来源 / 上下文字段（结果事件携带）**：`channel`（`follow_feed`/`recommend`/`explore`/…）、`batch_id`、`position`、`source_item_id`、`source_user_id`。

**点事件**

| `event_type` | 特有字段 |
|---|---|
| `app_open` | — |
| `enter_item` | `item_id`, `channel`, `batch_id`, `position`, `source_item_id?`, `source_user_id?` |
| `leave_item` | `item_id`, `duration_sec` |
| `like`/`fav`/`share`/`dislike` | `item_id`, `batch_id?` |
| `comment` | `item_id`, `comment_text` |
| `follow`/`unfollow` | `target_user_id`, `channel`, `source_item_id?`, `source_user_id?` |
| `dm` | `target_user_id`, `comment_text` |
| `enter_profile` | `target_user_id`, `source_item_id?` |
| `click_tag` | `tag`（必填）, `item_id?` |
| `item_published` | `item_id`, `author_id`, `category`, `tags`, `content_len`, `published_ts` |

**批次事件 `exposure_batch`**（一行＝一次推送）：`batch_id`、`channel`、`items: [{item_id, position, rank_score?}]`。

> 落地约定：ODS 以 JSON 字符串承载**嵌套**；**DWD 再展开成逐条曝光**（展开属 T 层职责）。

### 3.3 归因真值（`content_groundtruth`）

| 字段 | 说明 |
|---|---|
| `truth_id` | 真值记录 id |
| `event_id` | **对应哪个转化事件**（可回连主事件流） |
| `user_id` / `conversion_type`（`like`/`fav`/`share`/`comment`/`follow`） | 谁、什么转化 |
| `item_id` / `target_user_id?` | 对什么（关注时含被关注者） |
| `channel` / `batch_id` / `position` | **它来自哪一次曝光、什么渠道、什么位置** |
| `session_id` / `ts` | 会话与时间 |

---

## 4. 超集 Schema 特征

全项目**只有一个事件 POJO**（`EventLog`），用"超集 Schema"承载所有事件类型：

1. **一个信封 + 可选字段**：所有事件共享信封；各类型只填自己的字段。
2. **`@JsonInclude(NON_NULL)`**：不适用的字段**直接不出现在 JSON 里**（而非 `null`），报文更小、更干净。
3. **单一 topic 承载多类型**：下游只需一个反序列化器，按 `event_type` 分支处理。
4. **批次事件用嵌套**：`exposure_batch.items[]` 是数组（`ExposureItem`），这是唯一"非点状"的形态。
5. **向前兼容**：新增字段不影响旧消费者；`schema_version` 用于大改时的区分。

**消费方约定**：

- 字段**缺失＝不适用**，不是"数据缺了"；
- 必须按 `event_type` 分支，不要假设某字段总在；
- 解析嵌套（`items`）时按数组处理。

---

## 5. 脚本算法原理与特征

### 5.1 种子生成（`MasterDataSeeder`）

| 对象 | 算法 |
|---|---|
| **用户**（1000） | 兴趣标签覆盖 **80%**（1–3 个，取自 11 个标签；其余 20% 冷启动）；`level` 正态（5/20/50/20/5）；创作者占比 **~15%**，创作者带 `primary_category`（取其标签所属类目）与 `post_rate_level`（HIGH 20% / MEDIUM 50% / LOW 30%） |
| **内容**（500） | 5 类各 100；`author_id` 取自**同类目创作者**；`tags` 取该类目的 1–3 个；`content_len ∈ [50,5000]`；`published_ts` 在近 30 天内 |
| **社交图**（10000 边） | 按"簇"（创作者按主类目、普通用户按首个兴趣标签、其余归 `COLD`）→ **60% 指向同簇创作者**（让关注流有内容可派）、再按**优先连接**（入度加权）选边、8% 跨簇；出度固定 10 → 入度呈**幂律**（top 用户入度可上百） |

### 5.2 会话旅途状态机（`MockGenerator`）★核心

每个用户被推进**一步**（`generateNext()`）：

| 状态 | 动作 |
|---|---|
| `IDLE` | 起会话 → 产 `app_open`，进入 `BROWSING` |
| `BROWSING` | 分发一批候选 → 产 `exposure_batch` → **点击判定** → 命中则产 `enter_item` 并进入 `CONSUMING`；未命中继续浏览 |
| `CONSUMING` | 产 `leave_item`（**采样停留时长**）→ 依次判定 **互动**（like/fav/share/comment/dislike）、**关注**（真正改图）、**取关**、**进主页 / 点标签 / 私信** → 回到 `BROWSING` |

**关键特征**：

- **因果链**：曝光 → 点击 → 消费 → 互动/关注，事件之间可对上（不再是独立抽样）。
- **会话有界**：步数上限 `mock.session.max.steps`。
- **优先推进活跃会话**（85%）：否则随机选用户会让绝大多数用户只被抽中一次、会话推进不起来。

### 5.3 分发控制器（`DistributionService`）

- **渠道采样**：按动态权重抽 `recommend / follow_feed / explore`。
- **动态比例（C3）**：基线 `recommend 45% / follow_feed 35% / explore 20%`（可配）
  × **用户成熟度**（`activityScore/12`，向关注流偏移）× **渠道反馈 EMA**（近期转化好则权重上升）。
- **候选池**：关注流＝**图邻居作者**的内容（抽 8 个邻居，避免全量拼接）；推荐＝用户某个兴趣标签下的内容；探索＝全量内容。
  > 候选池为空时**回落到探索**并记为 `explore` —— 这是观测到的实际占比偏离基线的主因。
- **排序（D1）**：`0.40×新鲜度 + 0.35×兴趣匹配 + 0.15×作者影响力 + 0.10×噪声`。
- **点击**：位置偏置 `base × decay^position`，兴趣匹配再乘 `interest.boost`；**可能一次都不点**。
- **策略可切（C4）**：`hybrid | random | interest | graph`（后三者锁定单一渠道）。
- **成本档（C5）**：`cheap`（不排序）| `standard` | `heavy`（多采样 3 倍再排序）；收尾打印 `batches/scored/rankMs`。

### 5.4 停留 / 互动 / 关注模型

- **停留时长**：`base + content_len / 阅读速度 × 抖动`（叠加 D1/E1/C 异常）。
  > 旧实现用 wall-clock 差值 → 事件毫秒级产生 → 正常停留恒为 0，**已废弃**。
- **互动**：每次消费按概率触发；概率受"**兴趣匹配** + **停留时长**"调制。
- **关注**：从"**正在看的内容**"关注其作者（概率受兴趣/停留调制），并**真正改图**（`addFollow`）；
  事件带 `channel/source_item_id/source_user_id` → 支撑**关注转化途径**的归因。
- **取关**：低频，`removeFollow`。

### 5.5 混沌注入（沿用）

| 模式 | 触发 | 规则 |
|---|---|---|
| **A1 刷屏评论** | 全局计数 `%1000==0` | 同一用户对同一内容连发 5 条 `comment`（`ts` 各 +1s） |
| **A2 疯狂点赞** | 全局计数 `%1000==500` | 同一用户 5s 内对 15 篇不同内容 `like` |
| **D1 秒退** | 停留采样 **15%** | `duration_sec ∈ [0.2, 1.2]` |
| **E1 短停留** | 停留采样 **5%** | `duration_sec < 0.8`（不按内容长度筛选；长内容短停留由 T 层 Q-02 诊断） |
| **C 极长驻留** | 停留采样 **0.5%** | `duration_sec ∈ [1800, 3600]` |
| **B1 乱序** | 任意事件 **10%** | `ts` 回拨 30–90s |

### 5.6 归因真值（D2）

每次转化（互动/关注）在产出主事件的同时，**额外产出一条真值**（独立 topic），记录它**真正来自**哪一次曝光、哪个位置、什么渠道。
数仓可用它**校验**自己从事件里重建的归因（last-touch / Markov 等谁更接近真相）。

> **不进主事件流**：否则归因校验等于抄答案。

### 5.7 流量整形与压力档位（C6 / E3）

`mock.load.tier`：`steady`（30–50）| `peak`（每 N 分钟洪峰 M 秒 → 150–200）| `ramp`（30→200 线性）| `skew`（低频 + 偶发大突发）；阈值全部可配。

### 5.8 关键特征与已知简化

**特征**：因果链完整、会话有界、图由旅途演化、停留时长可解释、真值可对账、策略/成本/压力全可配。

**已知简化（有意保留）**：

1. **`ts` 已改由模拟时钟推进（P1.6）**：全局时钟只由模型活动推进，用户时钟承载事件时间戳；1 个事件天 ≈100 真实秒。它适合跨天窗口分析，但不是真实墙钟时间。
2. **实际渠道占比会偏离基线**（空池回落 + 成熟度上升）。
3. **图演化只在内存**，落地的只有 t0 种子 + `follow/unfollow` 事件（数仓据此重建）。
4. 混沌注入的触发是"全局计数"，与用户无关（保留旧行为）。

---

## 6. 类与模块职责

| 类 | 职责 |
|---|---|
| `Main` | 入口：加载配置 → 种子 → 载入 → 双发送器 + 流量整形主循环；收尾打印成本 |
| `AppConfig` | 配置中心：`-Dkey` > `-Dconfig.file` > classpath `config.properties` > 内置默认 |
| `TrafficShaper` | 压力档位 → 每秒生成量 |
| `MasterDataSeeder` | 种子生成（用户/内容/图）+ 版本化重建 |
| `DataManager` | 载入种子；内存索引（by id/author/tag）；图的查改；`nextItemId` |
| `MockGenerator` | 会话旅途状态机；产出事件与真值 |
| `DistributionService` | 分发控制器（渠道/比例/候选/排序/点击/成本） |
| `KafkaSender` | 单 topic 生产者（主事件 / 真值各一实例） |
| `EventLog` / `ExposureItem` | 事件 POJO（超集 Schema）/ 批次内候选 |
| `GroundTruth` / `GeneratedBatch` | 真值 POJO / 一次生成的结果容器 |
| `User` / `Item` | 实体（含运行期 `activityScore`，不落维表） |

运行期状态：

- `dim_user_injected.csv`：新用户 id 与属性追加日志，防止重启重复发号；
- `graph_state.log`：`follow/unfollow` 边变更日志，启动时在种子图上重放；
- `interaction_state.log`：`like/dislike/fav` 状态日志，启动时恢复一次性与互斥语义。

P3.9 起 A1/A2 混沌事件也写独立归因真值；A2 保证 15 条点赞落在 15 篇不同且尚未表态的内容上。

---

## 7. 契约与变更规则

- 事件契约**已冻结**（`Mock_Source_Spec.md` §4）。**消费方（数仓）以此为唯一依据**。
- 顺序铁律：**改生成器 → 冻结契约 → 改 T 层**；契约变更须同步 `schema_version` 与文档。
- 种子格式变更须**提升 `.seed_version`**（否则会读到旧格式）。
