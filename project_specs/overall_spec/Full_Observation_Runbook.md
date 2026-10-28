# 数仓全链路观测手册

> 本文回答一个具体问题：**从当前状态开始，每执行一次观测，按什么顺序做什么，
> 每一步应该看到什么，才能确认数仓真的完整跑完。**
>
> 适用对象：本机 WSL2 + Docker Desktop。所有运行负载都在容器中，宿主只保留命令入口。
>
> 本文不修改任何指标口径。口径以 `Metric_Definitions.md` 为准。

---

## 1. 两种运行模式

### 1.1 追加观测

保留现有 Kafka、S3、Redis 和生成器状态，在已有日期后继续生成一批数据。

- 适合日常开发和 Data Agent 调试；
- 每次使用新的 `RUN_NAME`；
- 流作业默认**不重置 checkpoint**；
- Batch 仍按最近 N 天幂等重算；
- 会保留历史运行和测试数据。

### 1.2 干净基线

清空 Kafka、S3、Redis，生成一组全新数据。

- 适合形成第一份正式演示基线；
- 会删除所有容器数据卷；
- 建议先把 `RealTime/data_files/` 移动到带时间戳的备份目录；
- 不要在只需要调试 Agent 时重复做干净基线。

---

## 2. 全过程状态机

一次标准观测应当依次经过：

```text
STOPPED
  → BUILD_READY
  → INFRA_READY
  → STREAM_RUNNING
  → GENERATING
  → GENERATOR_FINISHED
  → ODS_DRAINED
  → BATCH_DONE
  → VERIFY_DONE
  → GATE_DONE
  → OBSERVED
  → STOPPED
```

| 状态 | 判定条件 |
|---|---|
| `STOPPED` | 没有 `crt-stream` / `crt-generator`；中间件可运行或未运行 |
| `BUILD_READY` | 两个 jar 和生成器镜像均存在且可构建 |
| `INFRA_READY` | Kafka、Redis healthy；SeaweedFS 可访问；topic 和 bucket 存在 |
| `STREAM_RUNNING` | `crt-stream` 为 Up，日志出现 `[StreamApp] started` |
| `GENERATING` | `crt-generator` 为 Up；Kafka offset 持续增长 |
| `GENERATOR_FINISHED` | generator 退出码为 0；运行清单 `stop_reason` 非空 |
| `ODS_DRAINED` | 生成器退出后等待流作业继续消费 15–30 秒，日志无持续新批次 |
| `BATCH_DONE` | DIM 3 表、DWD 7 表、DWS 11 表、semantic 40 行全部产出 |
| `VERIFY_DONE` | V-01、V-02 表存在且行数可解释 |
| `GATE_DONE` | 目标日 20 条 Gate 全部执行；manifest/status/report 已写出 |
| `OBSERVED` | 产生 `ops/reports/observations/*.json`，控制台打印完整汇总 |
| `STOPPED` | stream/generator 已停止；Airflow 可按需保留，DAG 保持暂停 |

---

## 3. 每次运行前必须确认的四个参数

建议每次先设置：

```bash
cd /home/henry/projects/Community_Real_Time

RUN_NAME="observation-normal-$(date +%Y%m%d-%H%M%S)"
PROFILE="observation-normal"
DAYS=3
```

| 参数 | 含义 | 建议 |
|---|---|---|
| `RUN_NAME` | 本次输入运行名 | 每次唯一，便于找生成器清单 |
| `PROFILE` | 生成器行为 profile | 正常用 normal，故障诊断用 incidents |
| `DAYS` | Batch/Verify 重算窗口 | 至少覆盖观测目标和其 cohort 回看 |
| `TARGET_DT` | Gate 目标完整事件天 | 首选 `auto`，由 ODS 水位自动解析 |

如需临时覆盖 profile 的停止条件：

```bash
DURATION_SECONDS=120
TARGET_EVENT_DAYS=1.5
MAX_EVENTS=5000
```

未设置的参数保持 profile 原值。

运行前快速检查：

```bash
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/airflow/docker-compose.yml ps -a
```

开始新观测前，stream/generator 不应还在跑：

```bash
docker compose -f infra/docker-compose.yml stop stream generator 2>/dev/null || true
docker compose -f infra/docker-compose.yml rm -f stream generator 2>/dev/null || true
```

预期：命令后 `crt-stream`、`crt-generator` 不存在。

---

## 4. 推荐方式：一条命令跑完整观测

### 4.1 正常运行

```bash
cd /home/henry/projects/Community_Real_Time

PROFILE=observation-normal \
RUN_NAME="observation-normal-$(date +%Y%m%d-%H%M%S)" \
DAYS=3 \
TARGET_DT=auto \
infra/scripts/run_observation.sh
```

默认 `STREAM_RESET=false`，续用现有 checkpoint。干净环境首次运行或明确要重放 Kafka 时加：

```bash
STREAM_RESET=true
```

脚本默认同时启动并检查容器化 Airflow，然后暂停两个 DAG。只想做手工 Spark 流程时可加：

```bash
START_AIRFLOW=false
```

该脚本依次执行：

1. 构建两个 jar；
2. 构建生成器镜像；
3. 恢复/检查 Kafka、Redis、SeaweedFS、topic、bucket；
4. 启动 `crt-stream`；
5. 启动 `crt-generator`，直到它按 profile 自行结束；
6. 等待 ODS 排空并停止 stream；
7. 自动解析最新完整事件天；
8. Batch → Verify → Gate；
9. 生成统一观测报告。

跳过重复构建：

```bash
SKIP_BUILD=true PROFILE=observation-normal TARGET_DT=auto \
  infra/scripts/run_observation.sh
```

### 4.2 故障数据观测

```bash
PROFILE=observation-incidents \
RUN_NAME="observation-incidents-$(date +%Y%m%d-%H%M%S)" \
DAYS=3 \
TARGET_DT=auto \
infra/scripts/run_observation.sh
```

profile 会注入：

- 少量坏 JSON、缺字段、非法枚举、错类型；
- 重复消息；
- 丢弃事件；
- 一次 8 秒发送暂停；
- 更高的乱序比例。

### 4.3 压力观测

```bash
PROFILE=observation-peak \
RUN_NAME="observation-peak-$(date +%Y%m%d-%H%M%S)" \
DAYS=3 \
TARGET_DT=auto \
infra/scripts/run_observation.sh
```

---

## 5. 逐步执行 SOP

下面是一次观测的完整教学流程。若一条命令脚本失败，按这一节定位停在哪一步。

### 5.1 第 1 步：构建

```bash
cd /home/henry/projects/Community_Real_Time

mvn -o -s RealTime/settings.xml -B -DskipTests package

(
  cd RealTime
  mvn -o -s settings.xml -B -DskipTests package
)

docker compose -f infra/docker-compose.yml --profile generator build generator
```

预期：

```text
target/crt-warehouse-0.1.0.jar
RealTime/target/mock-generator-1.0-SNAPSHOT.jar
crt-mock-generator:0.1.0
```

通过条件：

```bash
test -f target/crt-warehouse-0.1.0.jar
test -f RealTime/target/mock-generator-1.0-SNAPSHOT.jar
docker image inspect crt-mock-generator:0.1.0 >/dev/null
```

常见失败：

- Maven 离线依赖缺失：去掉 `-o` 联网构建；
- 容器构建找不到 jar：先完成生成器 `package`。

### 5.2 第 2 步：恢复基础环境

```bash
infra/scripts/bootstrap_runtime.sh
```

脚本会：

1. 启动 Kafka、Redis、SeaweedFS；
2. 等 Kafka/Redis healthy；
3. 幂等创建两个 topic；
4. 幂等创建 `crt-lake` bucket。

预期：

```text
crt-kafka ... healthy
crt-redis ... healthy
crt-s3 ... Up
content_events
content_groundtruth
BUCKET crt-lake ready
```

通过条件：

```bash
docker inspect -f '{{.State.Health.Status}}' crt-kafka   # healthy
docker inspect -f '{{.State.Health.Status}}' crt-redis   # healthy
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8333/
```

SeaweedFS 根路径返回 `403` 是正常现象，说明鉴权生效。

### 5.3 第 3 步：启动容器化 Airflow

即使本次手工执行批任务，也建议启动 Airflow，便于查看运行和后续切换调度：

```bash
docker compose -f infra/airflow/docker-compose.yml up -d
```

等待：

```bash
for i in $(seq 1 60); do
  curl -fsS http://localhost:8080/health && break
  sleep 2
done
```

预期健康信息包含：

```text
"metadatabase":{"status":"healthy"}
"scheduler":{"status":"healthy"}
```

确认 DAG 导入且暂停：

```bash
docker exec crt-airflow-airflow-scheduler-1 airflow dags list-import-errors
docker exec crt-airflow-airflow-scheduler-1 airflow dags list
```

预期：

```text
No data found
crt_daily_batch      True
crt_weekly_vacuum    True
```

`True` 表示 paused，防止自动调度与本轮手工流程抢跑。

### 5.4 第 4 步：启动流作业

首次运行或明确需要从 Kafka 最早 offset 重放：

```bash
CRT_STREAM_RESET=true \
  docker compose -f infra/docker-compose.yml --profile stream up -d stream
```

已有有效 checkpoint、只想续跑：

```bash
CRT_STREAM_RESET=false \
  docker compose -f infra/docker-compose.yml --profile stream up -d stream
```

等待启动：

```bash
timeout 180 bash -c \
  'until docker compose -f infra/docker-compose.yml logs stream 2>/dev/null | grep -q "\[StreamApp\] started"; do sleep 2; done'
```

预期日志：

```text
[Tier0] waiting for s3a://crt-lake/ods/events
[StreamApp] started: ods_events / ods_truth / tier0
```

通过条件：

```bash
docker compose -f infra/docker-compose.yml ps stream
```

预期 `crt-stream` 为 `Up`。

注意：`waiting for ODS/events` 是空环境正常状态，不是故障。第一步真实事件到达后 Tier0 会继续。

### 5.5 第 5 步：启动生成器

先生成唯一运行名：

```bash
RUN_NAME="observation-normal-$(date +%Y%m%d-%H%M%S)"
echo "$RUN_NAME"
```

启动：

```bash
CRT_RUN_NAME="$RUN_NAME" \
CRT_GENERATOR_PROFILE=observation-normal \
  docker compose -f infra/docker-compose.yml --profile generator up generator
```

该命令会占用当前终端，直到生成器：

- 达到 `mock.run.duration.seconds`；
- 或达到 `mock.run.target.event.days`；
- 或达到 `mock.run.max.events`；
- 或收到人工停止信号。

预期开头：

```text
[RunPlan] name=<RUN_NAME> durationSec=300 targetEventDays=2.0 maxEvents=0
[RunManifest] runId=<RUN_NAME>-<timestamp>
```

预期结尾：

```text
[RunManifest] data_files/run_manifest/<run_id>.json
[Cost] batches=... scored=... rankMs=... registeredUsers=...
```

通过条件：

1. `docker compose ... ps -a generator` 显示 `Exited (0)`；
2. 最新运行清单 `stop_reason` 不为 `null`；
3. `generated_events > 0`；
4. `generated_truths > 0`；
5. incidents profile 下 `injections` 中出现对应计数。

查看：

```bash
cat RealTime/data_files/run_manifest/"${RUN_NAME}"*.json
```

### 5.6 第 6 步：确认输入仍在流处理

生成器结束后不要立刻杀流。

检查 offset：

```bash
for topic in content_events content_groundtruth; do
  echo "[$topic]"
  docker exec crt-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:19092 --topic "$topic"
done
```

检查最近日志：

```bash
docker compose -f infra/docker-compose.yml logs --since 2m stream | tail -100
```

预期出现：

```text
[ODS] events batch=...
[ODS] truth batch=...
[Tier0] batch=...
```

建议等待 15–30 秒，再连续查看两次日志；如果没有新的 ODS/Tier0 batch，视为排空。

### 5.7 第 7 步：停止流作业

```bash
docker compose -f infra/docker-compose.yml stop stream
docker compose -f infra/docker-compose.yml rm -f stream
```

预期：

```text
crt-stream ... Stopped
crt-stream ... Removed
```

此时 `STOPPED` 只表示流停止；中间件和 Airflow 应继续运行。

### 5.8 第 8 步：执行批层

```bash
infra/scripts/run_warehouse.sh \
  com.hub.app.BatchApp --step all --days 3
```

预期顺序：

```text
[DIM] user rows=...
[DIM] item rows=...
[DIM] edge rows=...
[DWD] exposure rows=...
[DWD] click rows=...
[DWD] consume rows=...
[DWD] interaction rows=...
[DWD] relation rows=...
[DWD] publish rows=...
[DWD] session rows=...
[DWS] funnel_hourly rows=...
[DWS] channel_hourly rows=...
[DWS] content_daily rows=...
[DWS] author_daily rows=...
[DWS] relation_daily rows=...
[DWS] session_daily rows=...
[DWS] user_daily rows=...
[DWS] quality_daily rows=...
[DWS] position_daily rows=...
[DWS] retention_daily rows=...
[DWS] cold_start_daily rows=...
[SEMANTIC] metric_dict rows=40
[BatchApp] done
```

通过条件：

| 层 | 预期 |
|---|---|
| DIM | 3 张表均打印行数 |
| DWD | 7 张表均打印行数 |
| DWS | 11 张表均打印行数 |
| semantic | 恰好 40 行 |
| 进程 | 退出码 0，最后打印 `[BatchApp] done` |

### 5.9 第 9 步：执行对账

```bash
infra/scripts/run_warehouse.sh \
  com.hub.app.VerifyApp --days 3
```

预期：

```text
[V-01] rows=...
[V-02] rows=...
================...
V-01 明细：
  <dt> <type> total=... matched=... rate=...
V-02 分布：
  rel_diff 中位=... / p95=... / 最大=...
```

通过条件：

- `verify/attribution` 存在；
- `verify/heat` 存在；
- V-01 每个转化类型都有 total/matched；
- V-02 可以是全 0，也可以因迟到事件非零；非零不等于 Batch 失败。

### 5.10 第 10 步：自动选择完整事件天

```bash
infra/scripts/run_shell.sh \
  /opt/crt/infra/scripts/latest_complete_dt.scala
```

预期：

```text
LATEST_COMPLETE_DT=YYYY-MM-DD
```

选取规则：

- 取 ODS 最大 `dt`；
- 比它更早的最近一天视为已越过水位；
- 最大 `dt` 本身按部分日处理。

如果输出 `<none>`，说明只有一天数据，应继续生成到出现下一天。

### 5.11 第 11 步：执行 Gate

将上一步日期写入：

```bash
TARGET_DT=2026-10-29   # 替换为 LATEST_COMPLETE_DT

infra/scripts/run_warehouse.sh \
  com.hub.app.GateApp --stage all --dt "$TARGET_DT" --days 3
```

预期打印 20 条 `[GATE]`：

- DR 5 条；
- IV 4 条；
- CV 3 条；
- QL 3 条；
- RC 2 条；
- FX 3 条。

最后：

```text
[GateApp] runId=gate-... overall=pass
```

或：

```text
[GateApp] runId=gate-... overall=pass_with_limitations
```

两者退出码都为 0。`blocked` 才退出 1。

通过条件：

1. 20 条都有 status/action；
2. `ops/run_manifest` 增加 20 行；
3. `ops/partition_status` 覆盖 28 张规范表；
4. `pass_with_limitations` 的限制能在 detail/limitations 中解释；
5. `publish_final=false` 的表不得被下游当成定稿数据。

### 5.12 第 12 步：生成整体观测报告

```bash
CRT_GENERATOR_RUN_NAME="$RUN_NAME" \
  infra/scripts/observe_warehouse.sh
```

预期控制台按顺序输出：

```text
1. 容器状态
2. Kafka offsets
3. Redis Tier0 投影
4. 最近一次生成器运行清单
5. Delta 全链路观测
SOURCE_AND_SERVICE
DIM
DWD
DWS
VERIFY_AND_CONTRACT
ODS EVENT TYPES
V-01 {...}
V-02 {...}
LATEST GATE run_id=...
PARTITION STATUS dt=...
OBSERVATION_JSON s3a://...
```

预期文件：

```text
s3a://crt-lake/ops/reports/observations/observation-<timestamp>.json
```

完成后应有：

```bash
find RealTime/data_files/run_manifest -name "${RUN_NAME}*.json" -print
```

### 5.13 第 13 步：收尾

本流程结束时：

- `crt-stream` 不存在；
- `crt-generator` 为 Exited(0) 或已移除；
- Kafka、Redis、SeaweedFS 可以继续运行；
- Airflow 可以继续运行；
- DAG 必须保持暂停；
- 最新观测 JSON 已生成。

确认：

```bash
docker compose -f infra/docker-compose.yml ps -a
docker compose -f infra/airflow/docker-compose.yml ps -a
docker exec crt-airflow-airflow-scheduler-1 airflow dags list
```

---

## 6. 生成器 profile 与故障注入

### 6.1 profile

| profile | 目的 |
|---|---|
| `observation-normal` | 正常行为，约两个事件天，最多 300 秒 |
| `observation-incidents` | 坏数据、重复、丢弃、暂停、较高乱序 |
| `observation-peak` | 洪峰负载 |

### 6.2 停止条件

| 配置 | 行为 |
|---|---|
| `mock.run.duration.seconds` | 真实墙钟到达即停 |
| `mock.run.target.event.days` | 模拟事件时间到达即停 |
| `mock.run.max.events` | 生成事件数到达即停 |

任一满足即停止；三者全 0 时持续运行。

### 6.3 行为异常

| 配置 | 默认 | 含义 |
|---|---:|---|
| `mock.chaos.enabled` | true | 总开关 |
| `mock.chaos.a1.*` | 1000 / 5 条 | 刷屏评论 |
| `mock.chaos.a2.*` | 1000 / 15 条 | 疯狂点赞 |
| `mock.chaos.d1.*` | 0.15 | 秒退 |
| `mock.chaos.e1.*` | 0.05 | 短停留 |
| `mock.chaos.extended.*` | 0.005 | 极长驻留 |
| `mock.chaos.out_of_order.*` | 0.10 | 乱序 |

### 6.4 坏数据与传输故障

| 配置 | 含义 |
|---|---|
| `mock.data_defect.enabled` | 总开关，默认关闭 |
| `mock.data_defect.rate` | 坏数据命中率 |
| `mock.data_defect.modes` | `invalid_json,missing_field,invalid_enum,wrong_type` |
| `mock.data_defect.duplicate.rate` | 重复消息 |
| `mock.data_defect.drop.rate` | 丢弃事件 |
| `mock.fault.pause.after.seconds` | 运行多久后暂停 |
| `mock.fault.pause.duration.seconds` | 暂停多久 |

坏数据只进入主事件流，不污染 `content_groundtruth`。

---

## 7. 每次运行应得到的产物

| 产物 | 位置 | 说明 |
|---|---|---|
| 输入运行清单 | `RealTime/data_files/run_manifest/<run_id>.json` | profile、seed、停止原因、注入次数 |
| Kafka offset | 容器输出 | 输入规模 |
| ODS | `ods/events`、`ods/groundtruth` | 证据层与真值 |
| Tier0 | `tier0/heat_snapshot`、`tier0/cheat_events`、Redis | 流侧近似与缓存 |
| DIM | `dim/user,item,edge` | 维度 |
| DWD | `dwd/*` 7 表 | 明细 |
| DWS | `dws/*` 11 表 | 主题指标 |
| 字典 | `semantic/metric_dict` | 40 指标和限制 |
| 对账 | `verify/attribution,heat` | V-01 / V-02 |
| Gate | `ops/run_manifest`、`ops/partition_status` | 逐门与逐表结论 |
| 报告 | `ops/reports/run_id=...`、`ops/reports/observations/*.json` | 人读与机器读 |

---

## 8. 观测结果如何判读

### 8.1 `pass`

所有证据满足当前策略，`publish_final=true`。可正常用于下游。

### 8.2 `pass_with_limitations`

数据可用于分析或调试，但不能当“已定稿”结果。

- 必须阅读 `limitations`；
- `publish_final=false`；
- Data Agent 引用时必须原样说明限制。

### 8.3 `blocked`

硬错误，不能继续使用该分区。典型原因：

- IV 守恒失败；
- CV 口径越界；
- 字典结构错误；
- V-01 低于下限。

### 8.4 `wait`

上游数据水位未到。当前日常用 `--wait`；历史日应按 limitation 或重新补数处理。

### 8.5 V-02 非零如何理解

V-02 比较“当时流快照”和“后来基于完整 ODS 的同锚点重算”。

- 全 0：快照时输入已经完整；
- 非 0：快照后仍到达了本应早于锚点的事件；
- 当前 Gate 对 V-02 超出阈值只记 G3，不阻断批侧。

---

## 9. 常见失败与恢复

| 现象 | 判断 | 处理 |
|---|---|---|
| `DELTA_SCHEMA_NOT_SET` | Tier0 启动早于 ODS 首次成表 | 当前版本会自动等待；检查 `stream.ods.wait.seconds` |
| `crt-stream` 启动后退出 | 看容器日志最后 100 行 | checkpoint、Kafka、S3A 或 jar 缺失 |
| checkpoint 冲突 | 上次流进程未干净退出 | 确认无其他 stream 后 `CRT_STREAM_RESET=true` |
| generator 连接失败 | Kafka 未 healthy | 先运行 `bootstrap_runtime.sh` |
| generator 一直不结束 | profile 三个停止条件都为 0 | 人工 stop，或改 profile |
| generator 运行清单 `stop_reason=null` | 旧版本或异常退出留下的清单 | 当前版本会写非空；重新运行新版本 |
| Kafka offset 增长但 ODS 不增长 | stream 未运行或 checkpoint 已消费 | `docker compose logs stream` |
| S3 写入卡住 | SeaweedFS 不在线或 bucket 缺失 | `docker compose ps`，再跑 bootstrap |
| Batch 找不到 ODS | 流没有落湖 | 先解决 stream，再重跑 Batch |
| Gate QL 类型异常 | 旧 jar | 重新 Maven 构建，确认使用最新 jar |
| Redis 热榜为空 | 停止流超过 15 分钟后 TTL 到期 | 正常；Delta 真值仍在 |
| `partition_status` 大多 false | 上游 limitation 被传播 | 查 `limitations`，不是简单理解为“全坏” |

---

## 10. 干净基线流程

这是一项破坏性操作，只在明确需要时执行。

### 10.1 备份生成器状态

```bash
backup="RealTime/data_files.backup-$(date +%Y%m%d-%H%M%S)"
mv RealTime/data_files "$backup"
echo "$backup"
```

### 10.2 删除运行时数据卷

```bash
docker compose -f infra/docker-compose.yml down -v
docker compose -f infra/airflow/docker-compose.yml down -v
```

### 10.3 重建并运行

```bash
infra/scripts/bootstrap_runtime.sh

PROFILE=observation-normal \
RUN_NAME="baseline-$(date +%Y%m%d-%H%M%S)" \
DAYS=3 \
TARGET_DT=auto \
STREAM_RESET=true \
infra/scripts/run_observation.sh
```

---

## 11. Docker 环境完全丢失后的恢复

如果镜像也需要恢复，而 Docker Hub 直连超时，可用本机当前可用镜像源：

```bash
docker pull docker.1ms.run/apache/kafka:4.1.0
docker tag docker.1ms.run/apache/kafka:4.1.0 apache/kafka:4.1.0

docker pull docker.1ms.run/redis/redis-stack-server:latest
docker tag docker.1ms.run/redis/redis-stack-server:latest redis/redis-stack-server:latest

docker pull docker.1ms.run/chrislusf/seaweedfs:latest
docker tag docker.1ms.run/chrislusf/seaweedfs:latest chrislusf/seaweedfs:latest

docker pull docker.1ms.run/apache/spark:3.5.3
docker tag docker.1ms.run/apache/spark:3.5.3 apache/spark:3.5.3

docker pull docker.1ms.run/apache/airflow:2.10.5
docker tag docker.1ms.run/apache/airflow:2.10.5 apache/airflow:2.10.5

docker pull docker.1ms.run/library/postgres:16-alpine
docker tag docker.1ms.run/library/postgres:16-alpine postgres:16-alpine
```

然后回到第 5.2 节。

---

## 12. 2026-10-28 已验证基线

| 项 | 结果 |
|---|---|
| 容器 | Kafka/Redis/SeaweedFS/Airflow 运行；Generator 正常退出；Stream 最终停止 |
| ODS | 13,807 事件 / 1,069 真值 |
| 日期 | 10-28～10-30；10-29 为可观测完整事件天 |
| DIM | 1,133 / 590 / 10,092 |
| DWD | 32,293 / 3,781 / 2,872 / 977 / 228 / 90 / 1,610 |
| DWS | 11 表全部生成 |
| semantic | 40 行 |
| Gate | 10-29 `pass_with_limitations` |
| V-01 | 521/521/0，精确率和召回率 1.0 |
| V-02 | 4,538 行，p50=0，p95≈7.69%，max≈97.69% |
| partition_status | 28 张规范表 |
| 观测 JSON | `s3a://crt-lake/ops/reports/observations/observation-20261028-061519.json` |

主 run 的生成器清单显示 `stop_reason=null`，这是首次运行暴露并修复的会话池收尾缺陷留下的现场。
修复后的短时回归 run 已正常以 `target_event_days` 停止；后续新运行不应再出现 null。

一键编排也已用 `DURATION_SECONDS=1` 做过完整回归：
`orchestrator-smoke-2` 从 Stream 启动一直执行到 Gate 和观测报告，最终
`overall=pass_with_limitations`，运行清单 `stop_reason=duration_seconds`。
