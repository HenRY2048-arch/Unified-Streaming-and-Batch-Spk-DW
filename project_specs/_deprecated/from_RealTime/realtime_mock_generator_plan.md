# RealTime Mock Generator 开发计划

本项目旨在开发一个基于 Java 8 的模拟数据生成器，模拟小红书社区行为流并推送到 Kafka。

## 1. 模块拆解

### 1.1 基础架构与配置 (Infrastructure & Config)
- **Maven 项目初始化**: 配置 `pom.xml`，引入 `kafka-clients`, `jackson`, `javafaker` 等核心依赖。
- **Kafka 生产者封装**: 封装 Kafka 发送逻辑，确保每条消息携带 `user_id` 或 `session_id` 作为 Key。

### 1.2 维表种子生成 (Master Data Seeding)
- **自动路径管理**: 脚本启动时检查 `/home/hyr/data_files` 目录，若不存在则自动创建。
- **维表检查与生成**: 在上述目录下检查 `dim_user_profile.csv` 和 `dim_item.csv` 是否存在，若缺失则执行生成算法。
- **用户画像生成**: 生成 250 条数据，包含 `interest_tags`（20% 覆盖率）和正态分布的 `level`。
- **笔记内容生成**: 生成 100 条数据，涵盖 5 大类目，`content_len` 均匀分布。

### 1.3 数据模型 (Data Models)
- **Event POJO**: 包含 11 个核心字段的“超集 Schema”设计。
- **时间语义处理**: 使用 `ZonedDateTime` 处理 ISO 8601 格式 (`YYYY-MM-DDTHH:mm:ss.sssZ`) 及 CST 时区。

### 1.4 行为模拟核心 (The Vibe Logic)
- **会话管理**: 维护 `session_id`，支持 30 分钟超时或 `app_open` 更新。
- **推荐算法实现**: 70% 兴趣匹配 + 30% 随机探索逻辑。
- **状态机控制**: 维护 13 种事件权重分布，处理 `(user, item)` 的进入/离开状态。

### 1.5 异常注入引擎 (Anomaly Engine)
- **刷屏/疯狂行为 (A1/A2)**: 基于全局计数器的批量事件注入。
- **停留时长异常 (D1/E1/C)**: 模拟秒退、超速滑动或极长驻留。
- **时间乱序 (B1)**: 10% 概率注入延迟时间戳。

### 1.6 流速与运行控制 (Traffic Shaping)
- **流量控制器**: 实现常规模式 (30-50 EPS) 与洪峰模式 (150-200 EPS) 的切换。
- **主程序入口**: 整合所有模块，执行主循环。

## 2. 开发顺序

1.  **Phase 1**: 项目初始化与 `pom.xml` 配置。
2.  **Phase 2**: 实现维表种子生成逻辑并验证 CSV 产出。
3.  **Phase 3**: 定义 Event POJO 及时间工具类。
4.  **Phase 4**: 实现基础行为流（Session, 70/30 算法, 13 种事件权重）。
5.  **Phase 5**: 集成异常注入引擎。
6.  **Phase 6**: 实现流速控制逻辑并对接 Kafka 发送。
7.  **Phase 7**: 打包与全链路验证。

## 3. 验证步骤

1.  **目录验证**: 检查启动后是否在指定路径生成了 `dim_user_profile.csv` 和 `dim_item.csv`。
2.  **数据格式验证**: 随机抽样生成的 JSON 消息，验证 `ts` 是否符合 ISO 8601，`event_id` 是否唯一。
3.  **业务逻辑验证**: 统计事件权重分布，验证是否接近设计要求的比例（如 `enter_item` ~40%）。
4.  **异常注入验证**: 检查 Kafka 消息中是否存在 `duration_sec` 极小（秒退）或 `ts` 乱序的情况。
5.  **性能验证**: 观察控制台输出或 Kafka 消费速率，验证 EPS 是否在设定范围内。

## 4. 假设与决策
- **路径适配**: 严格遵循 `/home/hyr/data_files` 作为根目录。为了支持在 Windows 环境下的开发和预览，代码中将路径提取为常量/配置，但在提交版本中将保持为文档要求的 Linux 路径。
- **Kafka 连接**: 默认使用文档提供的 3 个 IP 地址。若在开发环境中无法连接，将自动降级为控制台日志模式，以便观察生成的 JSON 数据。
