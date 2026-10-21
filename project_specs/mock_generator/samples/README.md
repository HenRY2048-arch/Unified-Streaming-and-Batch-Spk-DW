# samples/ —— 新契约样本（生成于 2026-10-16）

本目录是**新脚本**（P1.0–P1.5 之后）产出的样本，**对应现行事件契约**（`../Mock_Source_Spec.md`）。
旧的示例产物（旧 `kafka_data_sample.jsonl` 与两份旧示例维表）**已按指示清除**。

> **字段结构见 [`SCHEMA.md`](./SCHEMA.md)**。原始 CSV 保持 `|` 分隔、无表头（与生成器/T 层契约一致），
> 不要直接给 CSV 加表头；JSONL 每行一个 JSON，缺失字段＝该事件不适用。

## 产出方式

```bash
cd RealTime
mvn -s settings.xml -B clean package
java -jar target/mock-generator-1.0-SNAPSHOT.jar     # 跑约 10 秒
```

（本机 Kafka 由 `infra/docker-compose.yml` 起的容器 `crt-kafka` 提供。）

## 内容

| 文件 | 内容 | 行数 |
|---|---|---|
| `dim_user_profile.csv` | 用户种子：`user_id\|interest_tags\|level\|is_creator\|primary_category\|post_rate_level` | 1000 |
| `dim_item.csv` | 内容种子：`item_id\|author_id\|category\|tags\|content_len\|published_ts` | 500 |
| `dim_follow_seed.csv` | 关注边种子：`follower\|followee\|ts`（有限有向图的 t0） | 10000 |
| `.seed_version` | 种子格式版本 | 3 |
| `events_sample.jsonl` | **主事件流**切片（每行一条事件 JSON；含 `user_registered`） | 600 |
| `groundtruth_sample.jsonl` | **归因真值**切片（独立 topic，供归因对账） | 40 |
| `dim_user_injected.csv` | **运行期注入用户**（append 落盘，格式同 `dim_user_profile.csv`；`T 层`可用它做交叉校验） | 18 |
| `SCHEMA.md` | 上述 CSV / JSONL 的逐文件字段、类型与事件特有字段 | — |

## 注意

- 事件含新契约字段（`schema_version` / `channel` / `batch_id` / `position` / `source_*` / `items` / `item_published` 等）；
  行为是**会话旅途**（曝光 → 点击 → 消费 → 互动/关注）。
- **v2（P1.6）新增**：`user_registered`（**不带 `session_id`**，带用户属性）；事件时间由**模拟时钟**推进，
  `leave_ts − enter_ts` 与 `duration_sec` 自洽（差中位毫秒级）。
- **真值不在 `events_sample.jsonl` 里** —— 否则归因校验等于抄答案（见 `../Mock_Source_Spec.md` §5）。
- 这是**一次约 10 秒运行**的切片，不是全量；要正式数据请自行跑脚本（可配 `mock.load.tier` 等档位）。
