# 内容社区批流一体实时数仓

一个个人练手项目：用模拟生成器提供**带因果链的行为流 + 归因真值**，搭一套**批为真值、流为近似**的内容社区数仓；后续 data agent 作为独立消费者接入。

> 当前状态（2026-10-20）：P3.1–P3.8 已完成；完整事件天 10-18 的 Gate 结果为 `pass_with_limitations`，V-01 1,252/1,252/0，V-02 1,947 行 rel_diff=0。

## 从这里开始

| 文档 | 用途 |
|---|---|
| [AGENTS.md](./AGENTS.md) | **交接入口**：目标、决策、进度、风险、下一步 |
| [Deployment_And_Operations.md](./project_specs/overall_spec/Deployment_And_Operations.md) | **整体链路搭建 / 启动 / runbook / 调试 / 验收记录** |
| [Project_Global_Spec.md](./project_specs/overall_spec/Project_Global_Spec.md) | 框架级规约（最高权威） |
| [Metric_Definitions.md](./project_specs/overall_spec/Metric_Definitions.md) | 40 指标口径（v1.3 冻结） |
| [Gate_Grading_Scheme.md](./project_specs/overall_spec/Gate_Grading_Scheme.md) | Gate v3.1：Status × Action × Approval + 20 条注册表 |
| [Warehouse_Design.md](./project_specs/overall_spec/Warehouse_Design.md) | T 层设计、目录契约、代码结构 |
| [Warehouse_Tables.md](./project_specs/overall_spec/Warehouse_Tables.md) | 逐表字段 + Tier0 热度参数 |
| [Mock_Source_Spec.md](./project_specs/mock_generator/Mock_Source_Spec.md) | 生成器唯一正本（契约 v2） |

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
- **调度**：Airflow（LocalExecutor + Postgres）DAG 源码在 `infra/airflow/dags/`，运行环境待部署。

## 快速开始

```bash
# 1) 构建薄 jar
mvn -o -s RealTime/settings.xml -B -DskipTests package

# 2) 中间件
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps

# 3) 生成器（宿主进程）
cd RealTime && mvn -s settings.xml -B clean package
java -jar target/mock-generator-1.0-SNAPSHOT.jar

# 4) 流作业
infra/scripts/run_warehouse.sh com.hub.app.StreamApp --reset

# 5) 批 + 对账 + Gate（示例：完整事件天 2026-10-18）
infra/scripts/run_warehouse.sh com.hub.app.BatchApp --step all --days 3
infra/scripts/run_warehouse.sh com.hub.app.VerifyApp --days 3
infra/scripts/run_warehouse.sh com.hub.app.GateApp --stage all --dt 2026-10-18 --days 3
```

更细的组件落点、端口、挂载、配置优先级、常见错误与验收数字见
[Deployment_And_Operations.md](./project_specs/overall_spec/Deployment_And_Operations.md)。

## 截至2026-10-19的边界与已知限制

- 不建应用层；不做个性化推荐服务；推荐系统仅为概念预留证据层。
- 不做终身旅程、不做多触达归因（只 last-touch）。
- 当前 Airflow 只有 DAG 源码，未部署/未做真实调度验收。
- 10-18 无 Tier0 快照，V-02 对该天记 G3 limitation；10-19 为部分事件天，DR-3 记 G2。
- ODS 可能含 Kafka 重放重复；DWD 按 `event_id` 去重后才进入指标口径。

## 目录

```text
src/main/scala/com/hub/     # 数仓：common / stream / batch / verify / app
RealTime/                   # Mock 生成器（Java）
infra/                      # compose、jar、脚本、Airflow DAG
project_specs/              # 框架、口径、表、Gate、运行手册
```
