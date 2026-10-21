# 数仓表清单与字段（逐表）

> 地位：`Warehouse_Design.md` 的**落地细化**（表 / 列 / 粒度 / 分区 / 来源）。
> 口径以 `Metric_Definitions.md` 为准；数据定义以 `Mock_Source_Spec.md` 为准。
> 版本：v1.1 ｜ 2026-10-20 ｜ 状态：**P3.8 前置修正已登记；落地时列名仍可能微调**

## 0. 约定

- **lake root** ＝ `s3a://crt-lake/`；下表路径均相对它。
- **分区**：`dt`（日期 `yyyy-MM-dd`）；小时表另加 `hour`（`00`–`23`）。
- **重写**：批作业按 `dt(+hour)` 用 Delta `replaceWhere` / `MERGE` **幂等重算**。
- **时间语义**：`ts` 为事件时间；`dt` 由 `ts` 派生（GMT+8）。

## 1. DIM（维度）

| 表 | 粒度 | 分区 | 列 | 来源 |
|---|---|---|---|---|
| `dim/user` | `user_id` | 无（小表） | `user_id, interest_tags(array), level, is_creator, primary_category, post_rate_level, source(seed/registered/inferred), registered_ts, first_seen_ts, first_seen_dt, updated_dt` | 种子 `dim_user_profile.csv` **∪** `user_registered` 事件（v2）；`first_seen_ts = min(ts)` over `ods/events`，MERGE 时取 `LEAST(旧,新)`——**单调不回退** |
| `dim/item` | `item_id` | 无 | `item_id, author_id, category, tags(array), content_len, published_ts, source(seed/published)` | 种子 `dim_item.csv` + `item_published` 事件（**append-only**） |
| `dim/edge` | 一条边（带时效） | 无 | `follower, followee, valid_from, valid_to`（`valid_to = null` 表示仍有效） | 种子 `dim_follow_seed.csv` + `follow / unfollow` 事件重建 |

> 约束：`dim_user` **不含**关注 / 粉丝边（粉丝数、关注数是**度量**，不是维属性）。
>
> v2 语义（判新用户不用 BloomFilter）：**注册口径**＝`registered_ts`（事件真值，种子为 `null`）；
> **观测口径**＝`first_seen_ts/first_seen_dt`（`min(ts)`，幂等可重算）。事件里出现但维表缺属性 → `source=inferred`、属性置空、
> 下一批维表回填自愈。`updated_dt` 用于标注"流侧读到的维表版本（滞后 ≤ 1 天）"。

## 2. ODS（贴源，保留原始形态）

| 表 | 粒度 | 分区 | 列 | 来源 |
|---|---|---|---|---|
| `ods/events` | 一条 Kafka 事件 | `dt` | 契约全字段 + `items: array<struct<item_id,position,rank_score>>`（**原样保留，不展开**）；v2 含 `user_registered`（**无 `session_id`**，字段缺失＝不适用）；**另加三列元数据**：`_kafka_partition, _kafka_offset, _ingest_ts`（定位/审计用） | topic `content_events` |
| `ods/groundtruth` | 一条真值 | `dt` | `truth_id, event_id, user_id, conversion_type, item_id, target_user_id, channel, batch_id, position, session_id, ts` | topic `content_groundtruth` |

## 3. DWD（明细，**批侧产出**）

| 表 | 粒度 | 分区 | 列 | 关键变换 |
|---|---|---|---|---|
| `dwd/exposure` | **一条曝光** | `dt` | `batch_id, user_id, session_id, ts, channel, item_id, position, rank_score, author_id, category` | `explode(ods.events.items)` + join `dim/item` 补 `author_id / category` |
| `dwd/click` | **一次进入** | `dt` | `event_id, user_id, session_id, ts, item_id, author_id, category, channel, batch_id, position, hour` | 过滤 `enter_item`；按 `event_id` 去重；**未闭环的消费也算点击** |
| `dwd/consume` | **一次消费片段** | `dt` | `enter_event_id, leave_event_id, user_id, session_id, item_id, author_id, category, channel, batch_id, position, enter_ts, leave_ts, duration_sec, hour` | `enter_item` ↔ `leave_item` **按会话 + item 配对**；⚠️ `duration_sec` **取 `leave_item` 事件字段**（模型真值），`leave_ts − enter_ts` 仅作交叉校验；`hour` 由 `enter_ts` 派生（供小时聚合） |
| `dwd/interaction` | 一次互动动作 | `dt` | `event_id, user_id, session_id, ts, item_id, author_id, category, action(like/fav/share/comment/dislike), channel, batch_id, hour` | 过滤 `event_type ∈ {like,fav,share,comment,dislike}`；**`channel` 由 `batch_id` 反查曝光批次**（事件本身不带渠道） |
| `dwd/relation` | 一次关系 / 交流事件 | `dt` | `event_id, user_id, session_id, ts, action(follow/unfollow/dm/enter_profile), target_user_id, channel, source_item_id, source_user_id` | 过滤 `event_type ∈ {follow,unfollow,dm,enter_profile}`（P3.8 前置修正） |
| `dwd/publish` | 一条内容发布 | `dt` | `event_id, item_id, author_id, category, tags, content_len, published_ts` | 过滤 `item_published` |
| `dwd/session` | **一个会话** | `dt` | `session_id, user_id, start_ts, end_ts, duration_sec, dwell_sum_sec, exposure_cnt, click_cnt, consume_cnt, interact_cnt, follow_cnt, event_cnt` | 由 `ods/events` 按 `session_id` 聚合；`duration_sec = end_ts − start_ts`（**事件时间轴**，模拟时钟启用后与 `dwell_sum_sec` 收敛，二者互证） |

## 4. DWS（主题域聚合，批侧）

| 表 | 粒度 | 分区 | 主要列 | 对应指标 |
|---|---|---|---|---|
| `dws/funnel_hourly` | `dt,hour,channel,category` | `dt,hour` | `exposure_cnt, exposure_uv, click_cnt, consume_cnt, interact_cnt, ctr, consume_rate, interact_rate` | F-01~F-06, I-02/I-03 |
| `dws/channel_hourly` | `dt,hour,channel` | `dt,hour` | `exposure_cnt, click_cnt, ctr, follow_cnt, follow_cvr` | F-05 |
| `dws/content_daily` | `dt,item_id` | `dt` | `author_id, category, exposure_cnt, exposure_uv, click_cnt, ctr, consume_cnt, avg_dwell, bounce_cnt, bounce_rate, like_cnt, fav_cnt, comment_cnt, share_cnt, dislike_cnt` | F-*, C-01/02/03, I-01/04 |
| `dws/author_daily` | `dt,author_id` | `dt` | `category, publish_cnt, exposure_cnt, click_cnt, ctr, interact_cnt, new_follower_cnt, lost_follower_cnt, net_follower_cnt, follower_end_cnt, new_follower_active_rate` | A-01~A-03, R-04, **R-05**（新粉 7 日内互动率） |
| `dws/relation_daily` | `dt,author_id,channel` | `dt` | `new_follower_cnt, follow_cvr, source_item_top`（来源内容 top） | R-01/R-02/R-03 |
| `dws/session_daily` | `dt` | `dt` | `session_cnt, avg_duration, avg_exposure, avg_click, avg_consume, bounce_session_cnt` | S-01~S-05 |
| `dws/user_daily` | `dt` | `dt` | `dau, new_user_cnt_registered, new_user_cnt_observed, new_user_gap` | U-01；**U-02 双口径**（注册口径 / 观测口径 / 两者差值＝埋点缺口） |
| `dws/retention_daily` | `dt,cohort_dt` | `dt` | `cohort_type(seed/registered), day_offset(1/3/7), cohort_size, retained_cnt, retention_rate, is_mature` | **U-03**（注册日用注册日、种子用首见日；主口径只看 registered） |
| `dws/cold_start_daily` | `dt,item_id` | `dt` | `author_id, category, published_ts, exposure_cnt_24h, click_cnt_24h, ctr_24h, interact_cnt_24h` | **A-04**（发布后 24h 事件时间窗口） |
| `dws/quality_daily` | `dt,category,author_id` | `dt` | `consume_cnt, bounce_cnt, short_stay_cnt, long_content_short_stay_cnt, extended_stay_cnt`（**join `dim/item` 取 `content_len`**） | Q-02/Q-03 |
| `dws/position_daily` | `dt,channel,position` | `dt` | `exposure_cnt, click_cnt, ctr, avg_rank_score` | F-08 |

> `retention_daily.is_mature`：`cohort_dt + day_offset <= 数据最大事件日期`；`false` 表示窗口未满，
> 此时留存 0 不代表真实零留存。

## 5. 自我验证（批侧）

| 表 | 粒度 | 分区 | 列 | 说明 |
|---|---|---|---|---|
| `verify/attribution` | `dt,conversion_type` | `dt` | `total_cnt, joined_cnt, matched_cnt, missing_cnt, channel_match_cnt, item_match_cnt, match_rate, recall` | 数仓重建归因 vs `ods/groundtruth`（**V-01**）；`total_cnt` 来自 truth，`missing_cnt` 记录未进入 DWD 的转化 |
| `verify/heat` | `dt,hour,item_id` | `dt,hour` | `heat_stream, heat_batch, abs_diff, rel_diff` | 流侧近似 vs 批侧真值（**V-02**）；`heat_stream` 一律来自 `tier0/heat_snapshot`，不读 Redis |

### 4.1 Tier0 流侧快照（服务层，**非真值**）

| 表 | 粒度 | 分区 | 列 | 写入方 |
|---|---|---|---|---|
| `tier0/heat_snapshot` | `item_id × 计算窗口` | `dt,hour` | `item_id, window_minutes(60/1440), heat, computed_at, batch_id` | **流②**（每微批追加；同一 `dt/hour` 可多次写入，取 `computed_at` 最新的一条作对账口径） |
| `tier0/cheat_events` | 一次作弊判定 | `dt,hour` | `rule(A1/A2), user_id, item_id, window_start, cnt, dt, hour` | **流②**（append-only；跨微批会重复，查询按 `rule+user_id+item_id+window_start` 去重） |

> 语义：这是"流侧近似值的时间快照"，写入时**不做更新**（append-only），批侧对账按 `computed_at` 取最新一条。
> Redis 只做秒级读缓存（TTL 过期回源），**不作为对账依据**。

### 4.2 Tier0 Redis 缓存

| Key | 类型 | 字段 | 说明 |
|---|---|---|---|
| `crt:heat:<item_id>` | Hash | `heat_1h, heat_24h, computed_at` | 单个内容热度；TTL 900s |
| `crt:heat:top` | ZSet | member=`item_id`, score=`heat_1h` | 热榜 top-N（只写聚合后的条目） |
| `crt:online` | Hash | `active_users, window_minutes, computed_at` | 近 15 分钟活跃用户近似 |

Tier0 输入**只读 `ods/events`**，不读 DWD、不直读 Kafka；三个输出分别是 F-07 热度、Q-01 作弊、U-04 在线。

## 6. 语义层（托底 agent）

| 表 / 文件 | 内容 |
|---|---|
| `semantic/metric_dict` | `metric_id, name, definition, numerator, denominator, grain, dims, tier, source_fields, recomputable, truth_source, nature, limitations`（`nature` = 业务/风控/验收/自检；`limitations` = 推断量/注入噪声/右截断/自检性质） |
| `Metric_Definitions.md` | 人类可读的指标口径（与元数据表一一对应） |

## 7. Tier0 热度参数（`heat-v1`，已冻结）

公式：**热度 = `Σ w_e × count_e + 0.02 × Σ min(dwell_sec, 30)`**，
`e ∈ {曝光, 点击, 赞, 藏, 评, 转}`；窗口 `(anchor − minutes, anchor]`。

| 项 | 取值 | 说明 |
|---|---|---|
| **窗口** | **1h 与 24h 两档滚动窗** | 1h ＝ 实时热度；24h ＝ 趋势 |
| **权重 `w_e`** | 曝光 `0.1` ／ 点击 `1` ／ 赞 `3` ／ 藏 `4` ／ 评 `6` ／ 转 `8` | 转发 > 评论 > 收藏 > 点赞 > 点击 > 曝光 |
| **停留折算** | `0.02 × 秒`，单次上限 `30` | 防止极长驻留灌水 |
| **衰减** | **窗口本身即衰减**（不做额外指数衰减） | 简单、可解释、可对账 |
| **归一化** | 不归一化 | 保留原值便于 `V-02` 对账 |
| **粒度** | `item`（可上卷 `author / category / channel`） | — |
| **可配** | 窗口 / 权重 / 上限全部走配置 | 与"策略可配"原则一致 |

### 7.1 编排与信任表（P3.8）

| 表 | 粒度 | 分区 | 主要列 | 说明 |
|---|---|---|---|---|
| `ops/run_manifest` | 一条 gate 执行 | `run_id`/`dt` | `run_id, stage, gate_id, class, level, dt, status, metric_value, threshold, action_taken, approval, publish_final, policy_version, contract_hash, input_fingerprint, ts, detail` | 每次运行的 Gate 清单；支撑按段跳过与事后追责 |
| `ops/partition_status` | `dt × table` | `dt` | `dt, table, status(pass/pass_with_limitations/needs_confirmation/blocked), limitations, policy_version, updated_at` | 看板/agent 的“这张表今天能不能用” |
| `ops/reports`（文件） | 一次运行 | `run_id` | `gate-report.md`（人读）、`gate-report.json`（机器读） | 两份由同一份 manifest 生成 |

## 8. 待定

1. 列名 / 类型仍可能微调（**已按实现回写**：ODS 三个元数据列、`consume/interaction` 的 `hour`、验证表的两个分项、字典的 `nature`；P3.8 前置修正新增 `dwd/click`、`limitations`、`cohort_type/is_mature`）；
2. `dim/edge` 是否需要"当前态快照"表（供快速查询）—— 待性能需要时再加；
3. ~~V-02 的流侧热度快照落哪~~ → **已定（2026-10-17）**：`tier0/heat_snapshot`（见 §4.1），append-only，
   批侧对账按 `computed_at` 取最新一条；**不读 Redis**；
4. **P3.8 编排相关表**（`ops/run_manifest`、`ops/partition_status`）由 `GateApp` 首次运行建表，字段设计见 `Gate_Grading_Scheme.md` §6；
5. **VACUUM** 已由 `VacuumApp` 封装（默认 168h/周维护/人工批准），尚未跑运行验收。

> ✅ **Delta over S3A 已冒烟通过（2026-10-16，14/14）**：见 `Warehouse_Design.md` §5 / `infra/README.md` §5。
> 落地时注意：**`replaceWhere` / `DELETE` 只改日志，物理回收靠 `VACUUM`**——批侧按 `dt(+hour)` 幂等重算后要配 VACUUM。
