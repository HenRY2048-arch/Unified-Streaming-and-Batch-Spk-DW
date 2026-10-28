# Mock 数据源规约（Mock Source Spec）

> **本文件是模拟数据源（生成器）文档的唯一现行正本。**
> 它**集结**了：实体与维表、**行为定义**、事件契约（超集 Schema）、算法（会话/分发/混沌/流量）、归因真值、配置项。
> 替代并收编：旧 `RealTime_Mock_Generator_{Design,Implementation,User_Guide}.md`、`Event_Contract_Draft.md`、`Entity_Dimension_Draft.md`。
> 版本：v0.11（2026-10-28）
> 状态：**契约 v1 已冻结；契约 v2 已落地（P1.6 完成）** ｜ 更新：见 §10 变更记录（本文件持续更新）
> v2 增量：新增**新用户注入**（`user_registered`，**不带 `session_id`**，见 §6.9）与**模拟时钟**（§6.8）；
> 澄清**停留时长语义**（§3.2）；口径后果见 `Metric_Definitions.md` §10.11。
> 说明性文档（属生成器工程）：`RealTime/docs/Pipeline_And_Architecture.md`（链路与架构）、`RealTime/docs/Testing_And_Config_Guide.md`（测试与配置）。

---

## 0. 范围与定位

- 本规约只描述**数据源侧**（生成器 `/RealTime`）；**T 层（数仓侧）不在其内**。
- 生成器是**业务的唯一事实来源**，也是**事件契约的源头**。
- 顺序铁律：**改生成器 → 冻结契约 → 改 T 层**。

---

## 1. 定位

一个"微博式内容社区"的**行为流模拟器**：以"**会话旅途 + 分发循环 + 有限有向图**"产生**有因果链**的行为事件，
并**另存归因真值**供数仓校验。主角是**内容分发**。

---

## 2. 实体与维表

### 2.1 ER 形状

```
           产出 (1..*)
  User ───────────────────► Item
    ▲ │                      ▲
    │ │ 关注 (有向边)         │ 行为：曝光/点击/消费/互动
    └─┘                      │
    └────────────────────────┘
    └──── 行为 ──► User        (关注/取关/私信/进主页)
  Session：一次会话内行为的容器（行为自带 session_id）
```

### 2.2 作者 ≡ 用户（**已确认**）

创作是**用户**的一种能力/角色，**一个 ID 空间**；因此**不设 `dim_author`**，只在 `dim_user` 上增加**创作者属性**。
（若两套实体，则关注图连不到内容，"关注流"无从派发。）

### 2.3 实体与维表清单

| 实体 | 键 | 属性 | 变化性 | 落地形态 |
|---|---|---|---|---|
| **User** | `user_id` | `interest_tags`, `level`；**创作者属性**：`is_creator` / `primary_category` / `post_rate_level` | 基本静态 | `dim_user_profile.csv`（**种子**） |
| **Item** | `item_id` | `author_id`, `category`, `tag(s)`, `content_len`, `published_ts` | **会新增**（不修改） | 初始**快照** + 新增**走事件** |
| **Tag / Category** | tag / category | 两级分类（沿用旧字典） | 静态 | 内置配置 |
| **Follow edge** | (`follower`,`followee`) | `ts` | **会增删**（低频） | 初始**边表种子** + 变更**走事件** |
| **Session** | `session_id` | user、起止 | 运行期 | **不落维表**（事件自带） |

> **红线（已确认）**：`dim_user` **只放慢变属性**，**不放关注/粉丝边**；**粉丝数 / 关注数是"度量"而非维属性**
> （由边派生、按周期快照）。理由：一次关注会**同时改两处**（关注者的 following、被关注者的 followers），
> 把可变边塞进维表，会把最热的表变成最频繁被重写的表——**存储与下游都受损**。

### 2.4 维表"变更"的暴露方式（**已确认**）

- **静态实体（User / Tag）→ 快照**（定期重写 CSV，T 层热更新）；
- **动态实体（Item 新增、Follow 边）→ 事件**（走 Kafka，T 层增量维护）。
- `dim_item` ＝ **只追加的缓慢变化维**（新内容即新行），**不做 SCD2**（编辑/软删留作**可选未来扩展**）。
- **图（关注边）属"动态实体"** → 一律走事件（见 §2.5），**不通过改维表表达**。

### 2.5 图（有限有向）

**规模与种子**

- 节点 ＝ 用户（**N = 1000**）；边 ＝ **有向关注**（`A → B`：A 关注 B）。
- 平均出度 **d ≈ 10**，**幂律（长尾）**。
- 种子模型 ＝ **分群 + 簇内优先连接 + 少量跨簇随机边**（长出"同好聚集 + 少数大 V"的结构）。
- 种子**以边表给出**（`follower, followee, ts`）；"按用户列关注/粉丝列表"是**等价**写法。

**图存在三处（各司其职）**

| 位置 | 用途 | 形态 |
|---|---|---|
| 生成器**内存图** | 分发（关注流候选）+ 派生粉丝数/影响力 | 运行时结构，**不对外暴露** |
| 数仓**边表** | 创作者看板（粉丝 / 关注转化） | 由种子 + `follow/unfollow` 事件重建，带时效（`valid_from` / `valid_to`） |
| （可选）**当前态快照** | 快速校验 | 批侧 MERGE 得出，**不是真值** |

**演化**：**只由 `follow / unfollow` 事件增量改边**（红线：不跑独立演化模型）。

### 2.6 作者产出

- 作者按速率产出**新内容**，形态＝**事件**（`item_published`，走 Kafka）。
- 初始内容池以**快照种子**给出（t0 存量）。

---

## 3. 行为定义（行为 → 事件 → 派生量）

> 本节是**行为视角**（"用户做了什么"），与 §4 的 **Schema 视角**互补。

### 3.1 行为清单

| 行为 | 主体 → 客体 | 产出事件 | 备注 |
|---|---|---|---|
| 打开 App / 建会话 | 用户 → 系统 | `app_open` | 会话起点 |
| **浏览推送（曝光）** | 用户 → 内容 | `exposure_batch` | 一次推送一行；**分发的根** |
| 点击进入 | 用户 → 内容 | `enter_item` | 带来源/批次/位置 |
| **消费 / 停留** | 用户 → 内容 | `enter_item` + `leave_item` | **停留＝派生量**（见 §3.2） |
| 互动：点赞 / 收藏 / 分享 | 用户 → 内容 | `like` / `fav` / `share` | 目标＝**被消费的 item** |
| 互动：评论 | 用户 → 内容 | `comment`（带 `comment_text`） | |
| 负反馈：不感兴趣 | 用户 → 内容 | `dislike` | 建议纳入"负反馈"语义 |
| 关注 / 取关 | 用户 → 用户 | `follow` / `unfollow` | **改图边** |
| 私信 | 用户 → 用户 | `dm` | |
| 进入他人主页 | 用户 → 用户 | `enter_profile` | 关注转化的中间步 |
| 点标签 | 用户 → 标签 | `click_tag` | 目标「待定」：tag vs item |
| 发布内容（作者行为） | 用户 → 内容 | `item_published` | 供给侧 |

### 3.2 派生量（**不由事件直接产生**）

| 派生量 | 由什么算出 | 说明 |
|---|---|---|
| **停留时长 `duration_sec`** | `leave_item` 事件字段（模型采样值） | ⚠️ **澄清（2026-10-16 实测）**：`duration_sec` 由**停留模型采样**（`base + content_len/阅读速度 × 抖动`，叠加 D1/E1/C），**并非 `leave_ts − enter_ts`**；实测 ts 差中位 0.965s 而 `duration_sec` 中位 24.03s。**T 层口径以事件字段为准**；引入**模拟时钟**（§6.8）后两者才会收敛并可互证 |
| 会话时长 / 事件数 | 同一 `session_id` 的时序 | S 域指标 |
| 曝光→点击→消费→互动 转化 | 会话旅途上的事件序列 | 归因/旅程 |

> 约定：**凡是"算出来的"不放进事件流**；事件只记"发生的事实"。
> 例外说明：`duration_sec` 是**一次消费的模型真值**（当前实现把它写在 `leave_item` 上），因此它属于"事件字段"而非"事后派生"。

### 3.3 行为语义：**状态型** vs **动作型**（P1.3 补丁）

行为按"**一次性 / 针对性**"分两类，直接决定统计口径是否良定义：

| 类型 | 行为 | 规则 |
|---|---|---|
| **状态型**（对 (user,item) 一次性） | `like`、`dislike`、`fav`、`follow` | `like` 与 `dislike` **互斥**；`dislike` 时不 `fav`；已发生则**不再重复**；`follow` 可经 `unfollow` 撤回（唯一有撤回的行为） |
| **动作型**（可重复） | `comment`、`share`、`dm`、`enter_item`、`exposure_batch`、`enter_profile`、`click_tag` | 可多次发生 |

**为什么重要（统计良定义性）**：

- 帖子**累计点赞数**（截至 T）＝ like 事件数 ＝ **distinct 点赞用户数**（因去重）→ 单调递增（无撤回）；
- 窗口**新增点赞数** ＝ 窗口内 like 事件数（不重叠窗口**可加和**）；
- **用户正/负反馈** ＝ 该 (user,item) 的 `stance ∈ {LIKE, DISLIKE, NONE}`，唯一且明确；
- **点赞率**的分母应为**曝光 UV**。

**不做撤回**（无 `unlike`/`unfav`）—— 这是**显式模型假设**：`stance` 为**吸收态**（一次定、互斥、终身不变）。
若将来要加，属**契约变更**（需 `schema_version` 升级），且数仓要从"求和"改为"净额/状态"计算。

**针对性**：`like/dislike/fav/comment/share/click_tag` 都打在被消费的 `item` 上；`enter_profile/follow` 指向该内容的作者；`dm` 优先发**关注的人**（其次内容作者），不再随机。

---

## 4. 事件契约（超集 Schema）【已冻结 2026-10-16】

### 4.1 通用信封

| 字段 | 说明 |
|---|---|
| `event_id` / `user_id` / `event_type` / `ts` / `session_id` / `platform` | 全部事件共有（`ts`＝ISO-8601，GMT+8，`SSSX`） |
| `schema_version` | 线事件契约版本；P3.9 起生成器统一输出 **`v2`**，历史样本/旧 ODS 中的 `v1` 仍可读取 |

> `platform` 取值固定为 `{ios, android, web}`，**不扩展**。

### 4.2 来源 / 上下文字段（结果事件携带）

| 字段 | 说明 |
|---|---|
| `channel` | 分发渠道：`follow_feed` / `recommend` / `explore` / `profile` / `list` / `topic`（可扩展） |
| `batch_id` | 关联到哪一次曝光批次 |
| `position` | 在批次中的位置 |
| `source_item_id` | 从哪条内容进入 |
| `source_user_id` | 从谁的主页/列表进入 |

### 4.3 点事件

| `event_type` | 特有字段 |
|---|---|
| `app_open` | — |
| `enter_item` | `item_id`, `channel`, `batch_id`, `position`, `source_item_id?`, `source_user_id?` |
| `leave_item` | `item_id`, `duration_sec` |
| `like` / `fav` / `share` / `dislike` | `item_id`, `batch_id?` |
| `comment` | `item_id`, `comment_text` |
| `follow` / `unfollow` | `target_user_id`, `channel`, `source_item_id?`, `source_user_id?` |
| `dm` | `target_user_id`, `comment_text` |
| `enter_profile` | `target_user_id`, `source_item_id?` |
| `click_tag` | `tag`（必填） + `item_id`（可选，来源内容） |
| `item_published` | `item_id`, `author_id`, `category`, `tags`, `content_len`, `published_ts` |
| **`user_registered`**（v2 新增） | `user_id`（新用户）, 属性 `interest_tags` / `level` / `is_creator` / `primary_category` / `post_rate_level`；**不带 `session_id`**（账户事件发生在会话之前，"不适用"即不出现该字段） |

### 4.4 批次事件

**`exposure_batch`**（一行一次推送）：`batch_id`、`user_id`、`session_id`、`ts`、`platform`、`channel`、
`items: [{item_id, position, rank_score?}]`（`position` 必带；`rank_score` 由**配置开关**控制，默认开）。
承载：**ODS 以 JSON 字符串承载嵌套；DWD 展开成逐条曝光**（展开属 T 层）。

### 4.5 与旧契约的增删改

- **新增**：`exposure_batch`、`item_published`；`channel / batch_id / position / source_item_id / source_user_id`。
- **修改**：`enter_item` 带来源；互动目标改为**被消费的 item**；`follow/unfollow` 落图并带来源、target 不能是自己；
  **`content_len` 归维表**（不再由事件注入），E1＝"**短停留（<0.8s）**"；长内容短停留由 T 层 Q-02 诊断。
- **保留**：`event_id / user_id / event_type / ts / session_id / platform / item_id / target_user_id / duration_sec / comment_text`。
- **v2 新增**：`user_registered`（新用户注册，携带全部用户属性、**无 `session_id`**）。

---

## 5. 归因真值

- **另存、不进主事件流**（否则校验失去意义）。
- 形态：**独立 Kafka topic**（**已确认**）→ 数仓按现有"一路入湖"接成**真值表**，与"从事件算出的归因"**对账**。
- 内容：每次转化（互动 / 关注）的**真实来源路径**。

```
content_events ─────► ods/events ─────► DIM/DWD/DWS（指标计算）
content_groundtruth ► ods/groundtruth ─► verify/attribution（V-01）
                                      ▲ 只用于对账，不参与指标计算
```

---

## 6. 算法

### 6.1 会话管理

- `session_id` 格式 `sess_{ms}_{user_id}`；`app_open` 强制新建；**30 分钟无活动超时**。
- **修复**：混沌 burst 也要走会话更新（消除 `session_id = null`）。

### 6.2 分发（**策略可插拔 + 参数可配**）

- 控制器输入＝用户 + **内存图** + 内容池；输出＝候选集与排序。
- **候选来源与加权**：
  - **关注流**：候选 ＝ 该用户**所关注者**产出的内容 → 按 **内容新鲜度 + 互动热度** 加权；
  - **推荐**：候选 ＝ 按 `interest_tags` / 分类匹配的内容 → `粉丝数`（**从图派生**的作者影响力）+ 热度参与打分；
  - **探索**：候选 ＝ 全量内容池，均匀随机。
- **动态比例（⑧）**：基线 **`recommend 45% / follow_feed 35% / explore 20%`**，
  由 **用户成熟度 + 渠道近期反馈**（主）、**内容新鲜度**（辅）驱动偏移。
- **策略可切换**（随机 / 兴趣 / 图 / 混合），权重与阈值**走配置**、不硬编码。
- **性能**：消除旧实现每次调用 `new Random()` 等低效；分发成本**可观测**。

### 6.3 作者产出

- 按速率产出新内容 → `item_published` 事件；内容池动态增长。

### 6.4 图的种子与演化

- 种子：一次生成；演化：**只由 `follow/unfollow` 事件**（见 §2.5）。

### 6.5 混沌注入（沿用旧规则）

| 模式 | 触发 | 规则 |
|---|---|---|
| **A1 刷屏评论** | 全局计数 `%1000==0` | 同一用户对同一内容连发 **5 条** `comment`（`ts` 各 +1s） |
| **A2 疯狂点赞** | 全局计数 `%1000==500` | 同一用户 **5s 内**对 **15 篇不同内容** `like` |
| **D1 秒退** | `leave_item` **15%** | `duration_sec ∈ [0.2, 1.2]` |
| **E1 短停留** | `leave_item` **5%** | `duration_sec < 0.8`；**不按内容长度筛选**。`content_len > 2000` 的“长内容短停留”由 T 层 Q-02 作诊断，不作为注入条件 |
| **C 极长驻留** | `leave_item` **0.5%** | `duration_sec ∈ [1800, 3600]` |
| **B1 乱序** | 任意事件 **10%** | `ts` 回拨 **30–90s** |

### 6.6 流量整形与压力档位

**负载档位（可配）**

- `steady`：**30–50 EPS**（沿用）；
- `peak`：每 10 分钟洪峰 30s → **150–200 EPS**（沿用）；
- `ramp`：**30 → 200 EPS 线性上升**；
- `skew`：单渠道 / 单批次集中（模拟爆款热点）。

**分发成本档位（可配）**：`cheap`（纯随机）/ `standard`（兴趣匹配）/ `heavy`（图 + 打分）。

> 两者构成"**不同压力下的测试**"的二维矩阵。

### 6.7 停留 / 点击 / 互动模型（P1.3）

- **停留时长**：`duration = base + content_len / 阅读速度 × 抖动`（阅读速度可配），叠加 D1/E1/C 异常；
  **不再由 wall-clock 差值决定**（旧实现导致正常停留恒为 0）。
- **点击**：位置偏置（`base × decay^position`）+ 兴趣加成；**可能不点**。
- **互动**：分两类（见 §3.3）——**状态型**：`like`/`dislike` 一次性且互斥、`fav` 一次性；
  **动作型**：`comment`/`share` 可重复。概率受"兴趣匹配 + 停留时长"调制。
- **关注**：从**正在看的内容**关注其作者（概率受兴趣/停留调制），并**真正改图**。
- **会话**：一次会话＝一串**有序动作**，受 `mock.session.max.steps` 约束（30 分钟超时退居次要）。

---

### 6.8 模拟时钟（v2）——为什么、怎么转、会看到什么

**一句话**：事件时间不再读墙上的钟，而是由"用户真的花了多少时间"累积出来。

**为什么必须换**：原来所有 `ts` 都取自 `System.currentTimeMillis()`，而 `duration_sec` 是停留模型的采样值——同一份数据里存在两套互不推导的时间。实测：`enter_item → leave_item` 的 ts 差中位 **0.97 秒**，模型给出的 `duration_sec` 中位却是 **24 秒**。后果是三件事：会话时长/形态失真（S 域）、"发布后 24h / 次日留存 / 关注后 7 天"这类窗口在本地跑不出来、批流对账时说不清差异来自算法还是时间。

**怎么转（三层，缺一不可）**

| 层 | 规则 | 解决什么 |
|---|---|---|
| **全局时钟** | `+ = 停留时长 + 思考时间`，**不看真实时间** | 进度可复现，不随机器快慢漂移 |
| **用户时钟** | 每个用户一条自己的时间线，**事件 `ts` 取自它** | `leave_ts − enter_ts ≡ duration_sec`（别人的停留插不进来） |
| **会话锚点** | 起会话时把用户时钟对齐到 `max(自己, 全局)` | 用户之间的日期不会越拉越开 |
| **并发上限 + 收口** | 同时最多 30 个会话；落后全局超过 30 分钟就收口 | 会话几秒内走完，长尾偏差压在分钟级 |
| **落盘** | `data_files/.sim_clock` 记最后一次事件时间 | 重启从 `max(真实现在, 落盘值)` 继续，**不倒退** |

`timestamp` 仍是 **ISO-8601（GMT+8，`SSSX`）**——**schema 不变**，变的只是"时间从哪来"。与混沌 `B1`（ts 回拨 30–90 秒）兼容：回拨仍在事件时间上发生，watermark/迟到语义不变。

**你会看到什么（实测，85 EPS）**

- **1 个事件天 ≈ 100 真实秒**；每个事件天约 **1,200–2,000 条事件**（够日粒度指标看出形状）；
- `|ts差 − duration_sec|` 中位 **0.003 秒**（90 秒的尾巴是 B1 乱序注入，不是时钟问题）；
- 会话 `enter` 中位 3 次、最长 11–12 次；会话时长中位 ≈90 秒；
- 跨用户时间线回退 p95 ≈**18 分钟**、p99 ≈**27 分钟**（<1% 的长尾来自乱序与注册补齐滞后）。

**三个别再踩回去的坑**

1. 把全局时钟绑在真实时间上 → 进度随机器速度波动，且跨用户时间线会回退十几小时（实测 19h）。
2. 全局时钟取"所有用户模型活动之和"却不限制并发 → 单个会话要每小时才走一步（实测 500 并发时）。
3. 用"只读"的 `peek` 判断时间对齐 → 读到过期时间，收口判断失效。

**自检**：跑 45 秒后按下文 §8.6 的"验证口诀"核对三项即可（注册数一致性、停留自洽、事件时间回退分位数）。

**已知取舍**：事件时间的进度由模型活动决定，所以"一个事件天里装多少事件"和"每用户回访间隔"是一对互相拉扯的量——想要更密的天就得跑久一点，想要更快的天就得接受更薄的天。选运行点时心里有数即可。

**启用后的口径收益**：`enter_ts → leave_ts` 与 `duration_sec` 自洽（可互为校验）；会话时长/形态（S-02/S-03/S-04）恢复语义；跨天窗口（A-04/U-03/R-05）可算。

### 6.9 新用户注入（v2）——让"新增 / 留存 / 粉丝活跃"有真实起点

**一句话**：新用户不再"凭空出现在维表里"，而是**注册时发一条事件**，跟作者发内容完全对称。

**为什么必须注入**：用户池由 `mock.user.count`（默认 1000）固定，运行期没有任何创建用户的路径（样本实测：事件里的 user_id 全部 ∈ 种子）。于是 `U-02 新增用户`永远为 0、`U-03 留存`没有 cohort、`R-05 粉丝活跃度`没有"新粉"可观察。

**怎么注入**

| 项 | 做法 | 备注 |
|---|---|---|
| 事件 | `user_registered`（§4.3），**不带 `session_id`** | 账户事件发生在会话之前；字段缺失＝不适用 |
| 速率 | `mock.user.registered.per.day`（默认 **30**，按**事件天**计） | 与实际事件时间挂钩，跑得越久注册越多 |
| 属性 | 与种子**同一套分布**采样（标签覆盖 80%、level 正态、创作者 ~15%） | 否则新老 cohort 行为系统性不同，U-01/U-03 会出现解释不清的偏差 |
| 首次打开 | 注册后按 `mock.user.first.open.delay`（默认 0–180 事件分钟）**保证一次 `app_open`** | 不给这次开机，新用户只是"进了池子"，实测只有约 19% 会被随机抽到 |
| id 空间 | 种子区间之外（100001–101000 之外，从 **300001** 起） | 与种子天然不冲突 |

**重启安全**：已注入用户会 **append 到 `data_files/dim_user_injected.csv`**，生成器启动时先加载它 → 重启**不会重复发同一个 user_id**（否则会凭空多出一批"假新用户"）。这个文件是运行期状态，不是种子，不要手工改。

**下游（T 层）会看到什么**

- `dim_user` 从"只读快照"升级为"**快照 ∪ 事件**"：列 `source(seed/registered/inferred)`、`registered_ts`、`first_seen_ts`、`first_seen_dt`、`updated_dt`；
- `U-02` 出**双口径**：注册口径（`user_registered` 去重）与观测口径（`first_seen = min(ts)`），两者差值＝埋点缺口；
- 判新**不再使用 BloomFilter**：状态放在湖表里，幂等可重算（见 `Metric_Definitions.md` §10.11 第 7 条）。

**自检（跑 45 秒）**

1. `dim_user_injected.csv` 的行数 == topic 里 `user_registered` 的条数（曾因提前 return 丢过一半）；
2. 所有 `user_registered` 都**没有** `session_id`，且 5 个属性字段齐备；
3. 注册用户里 ≥90% 能在注册时刻之后找到一次 `app_open`（其余是运行结束时刚注册、还没到开机时间）。

## 7. 配置项（可配清单）

**已外置（P1.0 完成）**：`RealTime/src/main/resources/config.properties`
（覆盖优先级：`-Dconfig.file=/abs/x.properties` → classpath `config.properties` → 内置默认值）

| key | 默认 | 说明 |
|---|---|---|
| `kafka.bootstrap.servers` | `localhost:9092` | 连容器 `crt-kafka` 的 EXTERNAL listener |
| `kafka.topic` | `content_events` | 事件主题 |
| `kafka.acks` | `1` | 生产者 acks |
| `data.dir` | `data_files` | 维表种子输出目录（**原硬编码 `/home/hyr/data_files`**） |
| `mock.user.count` | `250` | 用户规模（P1.1 起将调整） |
| `mock.item.count` | `100` | 内容规模（P1.1 起将调整） |

**v2 新增（P1.6 已实现）**

| key | 默认 | 说明 |
|---|---|---|
| `mock.clock.think.min.seconds` / `max` | `2` / `20` | 会话内思考时间（事件秒），推进用户时钟 |
| `mock.clock.file` | `data_files/.sim_clock` | 事件时间落盘（重启续跑） |
| `mock.clock.resume` | `true` | 起点取 `max(真实现在, 落盘值)`，避免事件时间倒退 |
| `mock.session.max.open` | `30` | 并发会话上限（时钟对齐的关键参数） |
| `mock.session.timeout.minutes` | `30` | 会话超时（事件时间）；也是"过期会话收口"阈值 |
| `mock.session.resume.weight` | `0.85` | 已有会话的推进权重（越高越深挖当前会话；越低越常开新会话） |
| `mock.user.registered.per.day` | `30` | 新用户注入速率（**事件天**口径）；0 = 关闭 |
| `mock.user.first.open.delay.min.minutes` / `max` | `0` / `180` | 注册 → 首次打开的间隔（决定 U-03 次留基线） |
| `mock.user.id.inject.base` | `300001` | 注入用户 id 起始段（种子 100001–101000 之外） |
| `mock.user.injected.file` | `data_files/dim_user_injected.csv` | 已注入用户 append 落盘（重启不重发 id） |
| `mock.graph.state.file` | `data_files/graph_state.log` | 运行期关注增删日志；重启在种子图上重放 |
| `mock.interaction.state.file` | `data_files/interaction_state.log` | like/dislike/fav 状态日志；重启恢复一次性与互斥语义 |

**v0.11 观测控制面（2026-10-28）**

| key | 默认 | 说明 |
|---|---|---|
| `mock.run.name` | `default` | 本次运行名称，用于日志和运行清单 |
| `mock.run.duration.seconds` | `0` | 真实墙钟运行上限；0 = 不按墙钟停止 |
| `mock.run.target.event.days` | `0` | 模拟事件时间目标天数；0 = 不按事件天停止 |
| `mock.run.max.events` | `0` | 主事件数上限；0 = 不限 |
| `mock.random.seed` | `20261028` | 组件随机种子 |
| `mock.run.manifest.dir` | `data_files/run_manifest` | 每次运行写一个 JSON 清单 |
| `mock.chaos.*` | 见配置正本 | A1/A2、D1/E1、极长驻留、乱序的显式开关、比例和区间 |
| `mock.data_defect.*` | 关闭 | 坏 JSON、缺字段、非法枚举、错类型、重复、丢弃注入 |
| `mock.fault.pause.*` | `0` | 运行中人为暂停发送的墙钟时刻与时长 |

**调参指南（按目标选旋钮）**

| 你想要 | 调哪个 | 顺带影响 |
|---|---|---|
| 事件天走得更快（少等一会就能跨天） | 提高 `mock.eps.*`（或 `mock.load.tier=peak`） | 每天事件数大致不变，但会更快覆盖多天 |
| 每天事件更密（日粒度指标更稳） | 调小 `mock.clock.think.*`、缩短 `mock.session.max.steps`，或提高 EPS 后跑更久 | 会话变短/每天跨得慢 |
| 会话更长、更深 | 提高 `mock.session.max.steps`、`mock.session.resume.weight` | 单用户每天会话数变少 |
| 新用户更多 | 提高 `mock.user.registered.per.day` | 注册事件变多（`dim_user` 增长） |
| 新用户更快出现行为 | 调小 `mock.user.first.open.delay.max.minutes` | 次留基线偏高 |
| 想复现同一次运行 | 目前**没有随机种子配置**（待补）；可复制 `data_files/` 与 `.sim_clock` 作为近似起点 | — |

**构建与运行**

```bash
cd RealTime
mvn -s settings.xml -B clean package          # 产物：target/mock-generator-1.0-SNAPSHOT.jar
java -jar target/mock-generator-1.0-SNAPSHOT.jar
```

**尚待外置（后续）**：**分发策略与权重**、**动态比例的驱动规则**、图规模与种子模型、作者产出速率、
投喂长度、**压力档位**、混沌注入开关与概率、语料与两级分类。

---

## 8. 待定项与冻结状态

**契约（§4）状态：v1 ✅ 已冻结（2026-10-16）；v2 ✅ 已落地（P1.6 完成，2026-10-17；增量＝`user_registered` + 模拟时钟语义）**。此前九项待定均已定案：

| 项 | 定值 |
|---|---|
| ① 图规模 / 出度 / 模型 | **N = 1000**，**d ≈ 10**（幂律），分群 + 簇内优先连接 + 少量跨簇边 |
| ② 创作者属性 | `is_creator` / `primary_category` / `post_rate_level`（粉丝数、影响力 **派生不存**） |
| ③ `dim_item` | **严格 append-only**，不做 SCD2（编辑/软删留作可选扩展） |
| ④ `channel` 取值域 | `follow_feed` / `recommend` / `explore` / `profile` / `list` / `topic` |
| ⑤ `click_tag` 目标 | `tag`（必） + `item_id`（可选） |
| ⑥ 批次 `items` | `position` 必带；`rank_score` 带（配置开关，默认开） |
| ⑦ `platform` / 版本 | `platform` 保持三者；**新增 `schema_version`** |
| ⑧ 动态比例驱动 | 用户成熟度 + 渠道近期反馈（主）、内容新鲜度（辅） |
| ⑨ 基线比例 / 档位 | `recommend 45% / follow_feed 35% / explore 20%`；负载 4 档 / 成本 3 档 |

**仍待定的只是"实施期可调的参数"（不入契约）**：创作者占比（10–20% 区间内取值）、作者产出速率、
投喂长度、初始内容池规模、混沌注入概率、各档位的具体 EPS 与时长。

**已修（P1.3/P1.6）**：

- 正常停留不再恒为 ≈0：改为按内容长度/阅读速度采样，并由用户时钟推进；`leave_ts−enter_ts` 与 `duration_sec` 自洽。
- `source_*` 覆盖由会话旅途补全：关注/进主页等从当前消费内容产生来源。

---

## 9. 补充（沿用旧文档的素材）

- **两级分类**（沿用）：时尚美妆[美妆,穿搭]、生活记录[旅行,美食,宠物萌宠]、知识成长[科技,学习]、
  身心健康[健身运动,情感]、家庭生活[母婴育儿,生活方式]。
- **语料**（沿用）：评论 `["写得真好","学到了","干货满满","绝绝子","收藏了","踩坑了","不太适合我"]`；
  私信 `["怎么买？","链接有吗？","多少钱？","求私信资料"]`。
- **维表种子**（**P1.1 已实现**）：用户 **1000**、内容 **500**（5 类各 100）、兴趣标签覆盖 **80%**（其余 20% 冷启动）、
  level 正态（5/20/50/20/5）、`content_len ∈ [50,5000]`、**创作者占比 ~15%**；**内容带 `author_id`**；
  关注边种子 **10000 条**（出度 10，入度幂律）。

**种子文件格式（落地契约，供 T 层解析）** —— `data.dir` 下，`|` 分隔、**无表头**：

| 文件 | 列 |
|---|---|
| `dim_user_profile.csv` | `user_id` \| `interest_tags`（逗号分隔，可空＝冷启动） \| `level` \| `is_creator` \| `primary_category` \| `post_rate_level` |
| `dim_item.csv` | `item_id` \| `author_id` \| `category` \| `tags`（逗号分隔） \| `content_len` \| `published_ts` |
| `dim_follow_seed.csv` | `follower` \| `followee` \| `ts`（有向边：follower 关注 followee） |
| `.seed_version` | 种子格式版本（格式变更时自动重建） |

---

## 10. 变更记录（Changelog）

| 日期 | 变更 |
|---|---|
| 2026-10-16 | 首版：收编两份草案与三份旧文档；确立"作者≡用户"、"静态快照/动态事件"、"真值走独立 Kafka topic"、"`content_len` 归维表"；新增 §3 行为定义 |
| 2026-10-16 | **v0.2**：图定型（存三处 / 边表种子 / 内存图派发 / `dim_user` 不含边）；分发消费图（关注流加权 + 粉丝数信号 + 动态比例）；压力档位（4 负载 × 3 成本）；九项待定定案；**§4 事件契约冻结** |
| 2026-10-16 | **v0.3（P1.2 落地）**：契约在生成器实现（`schema_version`/`channel`/`batch_id`/`position`/`source_*`/`category`/`tags`/`published_ts`、`exposure_batch`、`item_published`）；burst 会话修复；契约微调 `item_published.tag` → `tags`；记入两项 P1.3 待修（停留时长恒 0、`source_*` 覆盖有限） |
| 2026-10-16 | **v0.4（P1.3 落地）**：行为改为**会话旅途**（`IDLE→BROWSING→CONSUMING`，事件成因果链）；分发控制器真实现（D1 排序函数 + C3 动态比例 + A4 关注流取自图）；关注**真正改图**（A3/A5/B6）；**停留时长模型**（内容长度/阅读速度 + 抖动，叠加 D1/E1/C）；图种子 v3（**偏向同簇创作者**） |
| 2026-10-16 | **v0.5（P1.4/P1.5 落地）**：分发**策略可切**（`hybrid/random/interest/graph`）与**成本档可配可观测**（`cheap/standard/heavy`）；**压力档位**（`steady/peak/ramp/skew`，阈值可配）；**归因真值另存**为独立 topic `content_groundtruth`；配置支持 `-Dkey=value` 覆盖；日志降噪、移除死依赖。样本见 `samples/` |
| 2026-10-16 | **v0.6（行为语义补丁）**：新增 §3.3——`like`/`dislike`/`fav` 改为**状态型**（一次性；`like` 与 `dislike` **互斥**；`dislike` 时不 `fav`），`comment`/`share` 仍为动作型；`dm` 目标改为"关注的人 / 内容作者"（不再随机）。目的：让"帖子点赞数（累计/窗口）"与"用户正负反馈"**统计良定义**。不做撤回（`unlike`） |
| 2026-10-16 | **v0.7（契约 v2 定案，待 P1.6 实现）**：新增 §6.8 **模拟时钟**（`mock.clock.speedup`，需同步改 `DistributionService` 新鲜度）与 §6.9 **新用户注入**（`user_registered`，**无 `session_id`**，id 高位段 + `dim_user_injected.csv` 落盘防重启重发）；§3.2 **澄清停留语义**（`duration_sec` 为模型真值、非 ts 差，实测 ts 差中位 0.965s vs duration 中位 24.03s）；§7 补 v2 配置键。目的：让 `U-02/U-03/A-04/R-05` 具备"何时第一次见 / 跨天窗口"的语义 |
| 2026-10-17 | **v0.8（P1.6 落地）**：§6.8 按**最终实现**重写——全局时钟只由模型活动推进（不绑真实时间）、事件时间戳取**用户时钟**、会话开始锚定全局、并发会话上限 30、过期会话收口、落盘续跑；`leave_ts−enter_ts ≡ duration_sec`（实测中位 0.003s）。§7 的 v2 配置表替换为实际键（`mock.session.max.open` 等，移除未采用的 `speedup`/`gap.*`）。实测：1 事件天 ≈100 真实秒、注册速率精确、id 重启不重发、CSV 与 topic 注册事件数一致 |
| 2026-10-20 | **v0.9（文档对齐）**：契约 v2 标记为**已落地**；E1 明确为“短停留（<0.8s，不按内容长度筛选）”，长内容短停留由 T 层 Q-02 诊断；§8 的“已知待修”改为已修 |
| 2026-10-24 | **v0.10（P3.9）**：线 `schema_version` 统一为 `v2`；新增关注图/互动状态追加日志并在重启恢复；A2 固定选 15 篇未表态内容；A1/A2 同时产出归因真值。状态日志从本次升级起生效，升级前历史变更不自动回填 |
| 2026-10-28 | **v0.11（观测控制面）**：运行计划支持墙钟时长、模拟事件天、最大事件数三种停止条件；随机主种子、显式 chaos 参数、坏数据/重复/丢弃/发送暂停注入、运行 JSON 清单和容器化 profile 落地。默认不注入坏数据，既有契约字段不变 |
