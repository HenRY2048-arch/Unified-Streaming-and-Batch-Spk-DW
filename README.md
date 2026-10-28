# 内容社区批流一体实时数仓

一个个人练手项目：用模拟生成器提供**带因果链的行为流 + 归因真值**，搭一套**批为真值、流为近似**的内容社区数仓；后续 data agent 作为独立消费者接入。

> 当前状态（2026-10-28）：**P3.9 已完成一次新的端到端运行验收**；完整事件天 10-29 的 Gate 为
> `pass_with_limitations`，V-01 521/521/0，V-02 显示迟到数据导致的非零差异（p95 7.69%，最大 97.69%）。
> Kafka/Redis/SeaweedFS、Spark、Mock Generator 和 Airflow 均已通过容器运行验证。
> Data Agent 已进入架构 brainstorm，尚未开始工程实现。

## 从这里开始

| 文档 | 用途 |
|---|---|
| [AGENTS.md](./AGENTS.md) | **交接入口**：目标、决策、进度、风险、下一步 |
| [Deployment_And_Operations.md](./project_specs/overall_spec/Deployment_And_Operations.md) | **整体链路搭建 / 启动 / runbook / 调试 / 验收记录** |
| [Full_Observation_Runbook.md](./project_specs/overall_spec/Full_Observation_Runbook.md) | **从空环境到整体观测报告的逐步教学手册** |
| [Project_Global_Spec.md](./project_specs/overall_spec/Project_Global_Spec.md) | 框架级规约（最高权威） |
| [Metric_Definitions.md](./project_specs/overall_spec/Metric_Definitions.md) | 40 指标口径（v1.3 冻结） |
| [Gate_Grading_Scheme.md](./project_specs/overall_spec/Gate_Grading_Scheme.md) | Gate v3.2：Status × Action × Approval + 20 条注册表 |
| [Warehouse_Design.md](./project_specs/overall_spec/Warehouse_Design.md) | T 层设计、目录契约、代码结构 |
| [Warehouse_Tables.md](./project_specs/overall_spec/Warehouse_Tables.md) | 逐表字段 + Tier0 热度参数 |
| [Data_Agent_Discussion.md](./project_specs/data_agent/Data_Agent_Discussion.md) | Data Agent 需求、边界、候选技术栈与开放问题；brainstorm |
| [Mock_Source_Spec.md](./project_specs/mock_generator/Mock_Source_Spec.md) | 生成器唯一正本（契约 v2） |
| [samples/SCHEMA.md](./project_specs/mock_generator/samples/SCHEMA.md) | CSV / JSONL 样本字段字典 |

## 架构

```
Mock Generator ──► Kafka(content_events / content_groundtruth)
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
   流① Kafka→ODS                流② ODS→Tier0
   (S3 + Delta)                 (热度/作弊/在线 → Redis + heat_snapshot)
          │
          ▼
   批：DIM → DWD → DWS → semantic/metric_dict
          │
          ▼
   verify/attribution + verify/heat → GateApp → ops/run_manifest + partition_status
```

- **批**：DIM / DWD / DWS / 语义层 / 对账全部批侧，按 `dt` 幂等重写。
- **流**：只保留内容热度（F-07）、作弊判定（Q-01）、在线活跃（U-04）。
- **真值**：归因真值走独立 topic，V-01 用 last-touch 对账。
- **调度**：Airflow 已切换为 Docker `LocalExecutor + Postgres`，入口 `http://localhost:8080`（`admin/admin`）；
  当前两个 DAG 已暂停，手工触发。Airflow worker 通过 Docker socket 提交一次性 Spark 容器。

## 快速开始

```bash
# 单次完整流程：构建 → 中间件 → 流 → 生成器 → Batch → Verify → Gate → 观测
PROFILE=observation-normal \
RUN_NAME="observation-normal-$(date +%Y%m%d-%H%M%S)" \
DAYS=3 \
TARGET_DT=auto \
infra/scripts/run_observation.sh
```

逐步命令、每阶段预期状态、通过条件和失败恢复见
[Full_Observation_Runbook.md](./project_specs/overall_spec/Full_Observation_Runbook.md)。
组件落点、配置和调试地图见
[Deployment_And_Operations.md](./project_specs/overall_spec/Deployment_And_Operations.md)。

本地只读观测界面：

```bash
infra/scripts/run_observability.sh
# http://localhost:8501
```

它读取 observation JSON、`dws/content_daily`、`dws/channel_hourly` 和
`ops/partition_status`，不触发数仓任务。

## 截至2026-10-28的边界与已知限制

- 不建应用层；不做个性化推荐服务；推荐系统仅为概念预留证据层。
- 不做终身旅程、不做多触达归因（只 last-touch）。
- Airflow 已以 Docker `LocalExecutor + Postgres` 运行，并验证 scheduler 可提交 Spark；DAG 仍暂停，避免自动抢跑。
- 新的 10-29 完整事件天 Gate 为 `pass_with_limitations`；10-28 起步不完整，10-30 被水位截断。
- ODS 可能含 Kafka 重放重复；DWD 按 `event_id` 去重后才进入指标口径。
- P3.9 已重跑。V-02 因流快照后仍有迟到事件到达，不再是全零：
  p50=0、p95≈7.69%、max≈97.69%，Gate 按 G3 limitation 记录。
- Data Agent 当前不做 Text2SQL，也不执行重跑、修复或补数；架构与数据范围仍未冻结。

## 目录

```text
src/main/scala/com/hub/     # 数仓：common / stream / batch / verify / app
RealTime/                   # Mock 生成器（Java）
infra/                      # compose、jar、脚本、Airflow DAG
project_specs/              # 框架、口径、表、Gate、运行手册
project_specs/data_agent/   # Data Agent brainstorm（未实现）
```
