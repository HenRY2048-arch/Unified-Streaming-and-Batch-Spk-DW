# samples 字段字典

> 原始 CSV 保持**`|` 分隔、无表头**，与生成器和 T 层解析一致；不要直接给 CSV 加表头。
> JSONL 每行一个 JSON 对象；字段缺失＝该事件不适用。

## 1. CSV

### `dim_user_profile.csv`（种子用户）

| 列 | 类型 | 说明 |
|---|---|---|
| `user_id` | Long | 用户 id；种子为 100001–101000 |
| `interest_tags` | String | 逗号分隔；空串＝冷启动 |
| `level` | Int | 1–5 |
| `is_creator` | Boolean | `true/false`；创作者≈15% |
| `primary_category` | String | 创作者主类目；非创作者可为空 |
| `post_rate_level` | String | `HIGH/MEDIUM/LOW`；非创作者可为空 |

### `dim_item.csv`（种子内容）

| 列 | 类型 | 说明 |
|---|---|---|
| `item_id` | Long | 内容 id；种子从 200001 起 |
| `author_id` | Long | 作者用户 id |
| `category` | String | 5 大类之一 |
| `tags` | String | 逗号分隔，1–3 个 |
| `content_len` | Int | 字符数，50–5000 |
| `published_ts` | String | ISO-8601（GMT+8，`SSSX`） |

### `dim_follow_seed.csv`（关注边种子）

| 列 | 类型 | 说明 |
|---|---|---|
| `follower` | Long | 关注者 |
| `followee` | Long | 被关注者 |
| `ts` | String | 边建立的事件时间 |

### `dim_user_injected.csv`（运行期注入用户）

列与 `dim_user_profile.csv` 相同；**append 落盘**，重启时加载以避免重复发 id。

## 2. `events_sample.jsonl`（主事件流）

### 2.1 通用信封

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | String | 事件唯一 id（批侧去重键） |
| `user_id` | Long | 用户 |
| `event_type` | String | 见 §2.3 |
| `ts` | String | ISO-8601（GMT+8，`SSSX`） |
| `session_id` | String | 会话 id；`user_registered` 不出现 |
| `platform` | String | `ios/android/web` |
| `schema_version` | String | 当前 `v1` |

### 2.2 上下文与可选字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `item_id` | Long | 内容 |
| `target_user_id` | Long | 关注/私信/进主页目标 |
| `duration_sec` | Double | 仅 `leave_item`；模型采样停留 |
| `comment_text` | String | 评论/私信文本 |
| `channel` | String | `follow_feed/recommend/explore/profile/list/topic` |
| `batch_id` | String | 曝光批次 id |
| `position` | Int | 批次内位置 |
| `source_item_id` | Long | 来源内容 |
| `source_user_id` | Long | 来源用户 |
| `author_id` | Long | 内容作者（`item_published`） |
| `category` | String | 内容类目（`item_published`） |
| `tags` | String | 逗号分隔（`item_published`） |
| `content_len` | Int | 字符数（`item_published`） |
| `tag` | String | `click_tag` 目标标签 |
| `published_ts` | String | 发布时间（`item_published`） |
| `items` | Array<Struct> | `exposure_batch` 的候选：`item_id Long, position Int, rank_score Double` |
| `interest_tags` | Array<String> | `user_registered` 属性 |
| `level` | Int | `user_registered` 属性 |
| `is_creator` | Boolean | `user_registered` 属性 |
| `primary_category` | String | `user_registered` 属性 |
| `post_rate_level` | String | `user_registered` 属性 |

### 2.3 事件类型与特有字段

| `event_type` | 特有字段 |
|---|---|
| `app_open` | — |
| `exposure_batch` | `channel`, `batch_id`, `items[]` |
| `enter_item` | `item_id`, `channel`, `batch_id`, `position` |
| `leave_item` | `item_id`, `duration_sec` |
| `like/fav/share/dislike` | `item_id`, `batch_id` |
| `comment` | `item_id`, `comment_text` |
| `follow/unfollow` | `target_user_id`, `channel`, `source_item_id/source_user_id?` |
| `dm` | `target_user_id`, `comment_text` |
| `enter_profile` | `target_user_id`, `source_item_id?` |
| `click_tag` | `tag`, `item_id?` |
| `item_published` | `item_id`, `author_id`, `category`, `tags`, `content_len`, `published_ts` |
| `user_registered` | `interest_tags`, `level`, `is_creator`, `primary_category`, `post_rate_level`；**无 `session_id`** |

## 3. `groundtruth_sample.jsonl`（真相层）

每行一条归因真值；**不进主事件流**，只用于 V-01。

| 字段 | 类型 | 说明 |
|---|---|---|
| `truth_id` | String | 真值记录 id |
| `event_id` | String | 对应主事件 id，可回连 |
| `user_id` | Long | 转化用户 |
| `conversion_type` | String | `like/fav/share/comment/dislike/follow` |
| `item_id` | Long | 转化内容 |
| `target_user_id` | Long | 关注时＝被关注者 |
| `channel` | String | 真实来源渠道 |
| `batch_id` | String | 来源曝光批次 |
| `position` | Int | 来源位置 |
| `session_id` | String | 来源会话 |
| `ts` | String | 转化事件时间 |

## 4. 快速自检

```bash
head -1 project_specs/mock_generator/samples/dim_user_profile.csv
head -1 project_specs/mock_generator/samples/events_sample.jsonl
head -1 project_specs/mock_generator/samples/groundtruth_sample.jsonl
```

原始样本只用于契约核对；正式数据由 `RealTime/` 生成器写入 Kafka。
