> ⛔【已弃用 · DEPRECATED】本文件是**失败的旧应用层**（Drift Community Web 应用）对应的数仓规约，
> 该应用不落地、事件层为 no-op stub。项目**不建应用层**，业务真实性由模拟数据源脚本承担。
> 仅作历史素材，**不要引用为现行契约**。

# Drift Community 用户行为埋点与维表口径文档

> 用途：统一事件埋点、维度表字段与数据类型口径，便于后续数仓建模与 EtLT 改造。
> 事实来源：`src/main/java`、`src/main/resources/db/migration` 当前代码。

## 1. 事件总线

### 1.1 当前状态

- 事件统一通过 `KafkaProducerService.sendEvent(topic, event)` 发送。
- 当前 `KafkaProducerService` 是 **no-op stub**，只序列化并记录日志，不真正写入 Kafka。
- 事件主题约定为：`content_events`。

### 1.2 事件统一结构

对应 Java 类：`src/main/java/com/drift/community/dto/ContentEvent.java`

| 字段 | Java 类型 | 建议数仓类型 | 是否可空 | 说明 |
| --- | --- | --- | --- | --- |
| event_id | String | STRING | 否 | `evt_` + 32 位 UUID（无横线） |
| user_id | Long | BIGINT | 否 | 当前登录用户 ID |
| event_type | String | STRING | 否 | 事件类型，见 1.3 |
| ts | String | STRING/TIMESTAMP | 否 | ISO-8601 带时区，如 `2026-09-05T10:00:00+08:00` |
| session_id | String | STRING | 否 | `sess_{epochMillis}_{userId}` |
| platform | String | STRING | 否 | 当前固定为 `web` |
| item_id | Long | BIGINT | 可空 | 帖子 ID；社交事件可为 null |
| target_user_id | Long | BIGINT | 可空 | 目标用户 ID；关注/取关/私信/进主页 |
| content | String | TEXT | 可空 | 评论/私信文本；图片私信统一记录为 `[图片]` |
| duration_sec | Double | DOUBLE | 可空 | `leave_item` 使用，单位秒，可带小数 |

示例：

```json
{
  "event_id": "evt_3f8c2e6a7d1f4b9aa9c4f4c6c4c8c1a2",
  "user_id": 10000000000000001,
  "event_type": "like",
  "ts": "2026-09-05T10:00:00+08:00",
  "session_id": "sess_1725501600000_10000000000000001",
  "platform": "web",
  "item_id": 20000000000000002,
  "target_user_id": null,
  "content": null,
  "duration_sec": null
}
```

## 1.3 事件类型清单

| event_type | 触发位置 | item_id | target_user_id | content | duration_sec |
| --- | --- | --- | --- | --- | --- |
| app_open | 登录成功处理器 | null | null | null | null |
| enter_item | 登录用户查看非本人帖子 | 帖子 ID | null | null | null |
| leave_item | 帖子详情页离开时 sendBeacon | 帖子 ID | null | null | 停留秒数 |
| like | 点赞帖子 | 帖子 ID | null | null | null |
| fav | 收藏帖子 | 帖子 ID | null | null | null |
| dislike | 踩帖子 | 帖子 ID | null | null | null |
| comment | 发表评论 | 帖子 ID | null | 评论文本 | null |
| share | 分享帖子 | 帖子 ID | null | null | null |
| click_tag | 点击标签 | null | null | 标签名 | null |
| follow | 关注用户 | null | 目标用户 ID | null | null |
| unfollow | 取关用户 | null | 目标用户 ID | null | null |
| dm | 发送私信 | null | 接收用户 ID | 文本或 `[图片]` | null |
| enter_profile | 浏览他人主页 | null | 目标用户 ID | null | null |

## 2. 核心维表字段与数据类型

## 2.1 用户维表：`users`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| id | BIGINT | 雪花 ID，主键 |
| username | VARCHAR(50) | 邮箱，唯一 |
| password_hash | VARCHAR(255) | BCrypt，数仓一般不消费 |
| display_name | VARCHAR(50) | 昵称，唯一 |
| bio | TEXT | 简介 |
| interest_tags | VARCHAR(255) | 逗号分隔兴趣标签 |
| animal_name | VARCHAR(20) | 对外匿名动物名 |
| is_admin | BOOLEAN | 是否管理员 |
| show_profile | BOOLEAN | 隐私：是否展示主页 |
| show_following | BOOLEAN | 隐私：是否展示关注 |
| show_followers | BOOLEAN | 隐私：是否展示粉丝 |
| allow_follow | BOOLEAN | 是否允许关注 |
| allow_be_discovered | BOOLEAN | 是否允许被推荐 |
| allow_dm | BOOLEAN | 是否允许私信 |
| algorithm_mode | VARCHAR(20) | `DEFAULT` / `REAL_WORLD` |
| last_h3_cell_id | BIGINT | 预留字段，当前不活跃 |
| restricted_until | TIMESTAMP | 限制截止时间，可空 |
| created_at | TIMESTAMP | 注册时间 |
| updated_at | TIMESTAMP | 更新时间 |

## 2.2 帖子维表：`posts`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| id | BIGINT | 雪花 ID，主键 |
| author_id | BIGINT | 作者 ID |
| title | VARCHAR(200) | 标题 |
| content | TEXT | Quill HTML 正文 |
| content_len | INT | 正文长度 |
| category | VARCHAR(20) | 分类；`__UNDECIDED__` 表示用户主动未选 |
| paper_style | VARCHAR(20) | 纸张样式 |
| paper_color | VARCHAR(10) | 纸张颜色 |
| is_published | BOOLEAN | 当前基本恒为 true |
| anonymous_comments | BOOLEAN | 作者评论是否显示为“作者” |
| blurred | BOOLEAN | 是否超级模糊 |
| pinned | BOOLEAN | 是否置顶 |
| deleted | BOOLEAN | 是否软删除 |
| has_poll | BOOLEAN | 是否带投票 |
| frozen_until | TIMESTAMP | 冻结截止时间，可空 |
| like_count | INT | 点赞数 |
| fav_count | INT | 收藏数 |
| dislike_count | INT | 踩数 |
| top_reaction_emoji | VARCHAR | 热门表情 ASCII |
| top_reaction_count | BIGINT | 热门表情数量 |
| hot_score | DOUBLE | 热度分 |
| h3_cell_id | BIGINT | 发帖 IP 对应海洋 H3 格 |
| hot_reward_progress | TEXT | 热度奖励进度 JSON |
| created_at | TIMESTAMP | 发布时间 |
| updated_at | TIMESTAMP | 更新时间 |

## 2.3 标签维表：`tags`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| id | BIGINT | 雪花 ID，主键 |
| name | VARCHAR(20) | 标签名，唯一 |
| usage_count | INT | 使用次数 |
| is_hot | BOOLEAN | 是否热门 |
| is_managed | BOOLEAN | 是否平台管理标签 |
| created_at | TIMESTAMP | 创建时间 |

## 2.4 帖子-标签桥接表：`post_tags`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| post_id | BIGINT | 帖子 ID，复合主键之一 |
| tag_id | BIGINT | 标签 ID，复合主键之一 |

## 2.5 评论维表：`comments`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| id | BIGINT | 雪花 ID，主键 |
| post_id | BIGINT | 帖子 ID |
| user_id | BIGINT | 评论者 ID |
| content | VARCHAR(500) | 评论文本 |
| parent_comment_id | BIGINT | 父评论 ID，可空 |
| like_count | INT | 点赞数 |
| dislike_count | INT | 踩数 |
| deleted | BOOLEAN | 是否删除 |
| frozen_until | TIMESTAMP | 冻结截止时间，可空 |
| created_at | TIMESTAMP | 评论时间 |

## 2.6 钱包维表：`wallets`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| user_id | BIGINT | 用户 ID，主键 |
| balance | BIGINT | 海龟币余额 |
| shards_count | INT | 碎玻璃数量 |
| today_post_reward_count | INT | 今日发帖奖励次数 |
| today_comment_reward_count | INT | 今日评论奖励次数 |
| last_checkin_date | DATE | 最后签到日期 |
| checkin_streak | INT | 连续签到天数 |
| cumulative_days | INT | 累计签到天数 |
| last_reward_reset_at | TIMESTAMP | 每日奖励重置时间 |
| updated_at | TIMESTAMP | 更新时间 |

## 2.7 钱包流水事实表：`wallet_transactions`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| id | BIGINT | 雪花 ID，主键 |
| user_id | BIGINT | 用户 ID |
| type | VARCHAR(32) | 流水类型 |
| amount | BIGINT | 正入负出 |
| balance_after | BIGINT | 变动后余额 |
| related_id | BIGINT | 关联实体 ID，可空 |
| remark | VARCHAR(255) | 备注 |
| created_at | TIMESTAMP | 流水时间 |

常见 `type`：

`REGISTER_BONUS`、`CHECKIN`、`POST_REWARD`、`COMMENT_REWARD`、`HOT_REWARD`、`BUY_BOTTLE`、`POST_ADDITIONAL_FEE`、`ACCELERATE`、`ACCELERATE_RECOVERY`、`RECYCLE_SHARD`、`TRANSFER_SEND`、`TRANSFER_RECEIVE`

## 2.8 用户交易请求表：`transfer_requests`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| id | BIGINT | 雪花 ID，主键 |
| from_user_id | BIGINT | 发起用户 |
| to_user_id | BIGINT | 接收用户 |
| amount | BIGINT | 转账总额 |
| tax | BIGINT | 海岸税 |
| final_amount | BIGINT | 税后到账金额 |
| status | VARCHAR(20) | `PENDING` / `SENDER_CONFIRMED` / `COMPLETED` / `CANCELLED` / `EXPIRED` |
| created_at | TIMESTAMP | 发起时间 |
| confirmed_at | TIMESTAMP | 确认完成时间，可空 |
| expires_at | TIMESTAMP | 超时时间 |

## 2.9 商店库存维表：`shop_inventory`

| 列 | 类型 | 说明 |
| --- | --- | --- |
| bottle_type | VARCHAR(20) | 瓶型，主键 |
| current_stock | INT | 当前库存 |
| max_stock | INT | 最大库存 |
| price | BIGINT | 价格 |
| last_restock_at | TIMESTAMP | 最后补货时间 |

瓶型取值：

- `GLASS_SMALL` / `GLASS_MEDIUM` / `GLASS_LARGE`
- `PLASTIC_SMALL` / `PLASTIC_MEDIUM` / `PLASTIC_LARGE`

## 3. 数仓改造建议

1. 事件主题统一为 `content_events`，字段结构保持与 `ContentEvent` 一致。
2. 事件表 `user_id/item_id/target_user_id` 建议使用 BIGINT，不要因为前端 API 将 Long 序列化为字符串而改成 STRING。
3. `ts` 保留 ISO-8601 字符串入库，再解析为事件时间。
4. 维度表保留原始 ID 字段，业务显示名如昵称、标签名、分类名可在 DIM 层做多语言映射。
5. `posts.category = '__UNDECIDED__'` 是网站主动未选择分类的保留值，不应与 `NULL` 或数仓侧 `unknown` 混淆。
6. 图片私信在事件层统一替换为 `[图片]`，避免超大 payload。
7. 当前 Kafka 仍为 no-op，正式数仓接入时需要先在 `KafkaProducerService` 实现真实 Producer。
