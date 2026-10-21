> ⛔【已并入】本文件内容已收编进 `project_specs/mock_generator/Mock_Source_Spec.md`（生成器文档唯一现行正本）。
> 仅作历史参考，**不要作为现行依据**。

# 事件契约草案（v0.1 · 待评审）

> 状态：**草案**，供修改。本草案只定**形状**（有哪些事件、共有/特有字段、两种形态）；
> 字段类型、取值域、长度等多为「待定」。
> 前置：本契约**冻结后**才动 T 层。
> 日期：2026-10-16
> **已确认（2026-10-16）**：原则 4（事件保持"真实"、归因真值另存）、`content_len` 归维表、
> **归因真值走独立 Kafka topic**。

---

## 0. 设计原则

1. **因果链**：曝光 → 点击 → 消费 → 互动/关注 必须能**对上**。
2. **两种形态**：**点事件**（一次动作）＋**批次事件**（一次推送的曝光列表）。
3. **来源即根**：每个"结果事件"都带**来源 / 渠道**（归因的根）。
4. **事件保持"真实"**：主事件流只带**可重建**的线索（来源、批次、位置、会话、时序），
   **不**直接给出完整因果链；**归因真值另存**（供校验，见改造清单 D2）——否则校验失去意义。**（已确认）**
5. **作者是维表属性**，不进事件（由 `item → author` 维表解析）。

---

## 1. 通用信封（所有事件共有）

| 字段 | 说明 | 相对旧契约 |
|---|---|---|
| `event_id` | 唯一 ID | 保留 |
| `user_id` | 行为发起者 | 保留 |
| `event_type` | 事件类型 | 保留（类型集合变化见 §3/§4） |
| `ts` | 事件时间（ISO-8601，GMT+8，`SSSX`） | 保留 |
| `session_id` | 会话 | 保留（**修** burst 的 `null`） |
| `platform` | 终端 | 保留 |

---

## 2. 新增的"来源 / 上下文"字段（由结果事件携带）

| 字段 | 说明 | 出现在 |
|---|---|---|
| `channel` | 分发渠道：`follow_feed` / `recommend` / `explore` / `profile` / `list` / …（取值域「待定」） | 曝光批次、`enter_item`、`follow` 等 |
| `batch_id` | 关联到**哪一次曝光批次** | `enter_item`、互动、`follow` |
| `position` | 在批次中的位置 | 曝光批次内、`enter_item` |
| `source_item_id` | 从**哪条内容**进入 | `enter_item`、`follow`（从内容进主页） |
| `source_user_id` | 从**谁的主页/列表**进入 | `enter_item`、`follow` |

> 取值域、是否强制、是否可空 ——「待定」。

---

## 3. 事件类型（点事件）

| `event_type` | 特有字段 | 相对旧契约 |
|---|---|---|
| `app_open` | — | 保留 |
| `enter_item` | `item_id`, `channel`, `batch_id`, `position`, `source_item_id?`, `source_user_id?` | **改**：增来源上下文 |
| `leave_item` | `item_id`, `duration_sec` | 保留 |
| `like` / `fav` / `share` / `dislike` | `item_id`, `batch_id?` | **改**：目标须为**被消费的 item**（不再随机取） |
| `comment` | `item_id`, `comment_text` | 保留 |
| `follow` / `unfollow` | `target_user_id`, `channel`, `source_item_id?`, `source_user_id?` | **改**：target 必须在图中、不能是自己；带来源 |
| `dm` | `target_user_id`, `comment_text` | 保留 |
| `enter_profile` | `target_user_id`, `source_item_id?` | 保留 + 来源 |
| `click_tag` | `tag` 或 `item_id`「待定」 | 保留 |

---

## 4. 事件类型（批次事件）

**`exposure_batch`** —— 一行表示一次推送：

| 字段 | 说明 |
|---|---|
| `batch_id` | 本次推送 ID |
| `user_id` / `session_id` / `ts` / `platform` | 同信封 |
| `channel` | 本次推送的渠道 |
| `items` | 列表：`[{ item_id, position }]`（是否附带预估值「待定」） |

> 承载方式：**ODS 用 JSON 字符串承载嵌套**；**DWD 展开成逐条曝光**（展开属 T 层职责）。

---

## 5. 与旧契约的增删改（汇总）

**新增**

- 事件：`exposure_batch`；
- 字段：`channel`、`batch_id`、`position`、`source_item_id`、`source_user_id`。

**修改**

- `enter_item` 携带来源上下文；
- 互动（`like/fav/share/dislike`）目标改为**被消费的 item**；
- `follow/unfollow` 落到图上、带来源、target 不能是自己；
- **`content_len` 的定位**：归**维表**（item 属性），**不再由事件注入**；
  E1（超速滑动）改为"**选中长内容 + 极短停留**"来生成，而不是覆盖 `content_len`。**（已确认）**
  —— `content_len` 的初衷是"用内容长度推算阅读时长/速度"，既然内容本身属维表，长度也归维表。

**保留**

- `event_id / user_id / event_type / ts / session_id / platform / item_id / target_user_id / duration_sec / comment_text`。

**另存（不进主事件流）**

- **归因真值**：每次转化（互动 / 关注）的**真实来源路径**，单独输出，供数仓归因**校验**。
  **落地形态（已确认）：独立 Kafka topic** —— 数仓按现有"一路入湖"的方式（`Kafka → 轻转换 → 入湖`）
  把它接成一张**真值表**，与"从事件算出的归因"做对账。

---

## 6. 待定

- 字段类型与长度；
- `channel` 取值域；
- `click_tag` 的目标（tag vs item）；
- 批次 `items` 是否附带预估值；
- `platform` 是否扩展；
- 是否需要契约**版本字段**（`schema_version`）。
