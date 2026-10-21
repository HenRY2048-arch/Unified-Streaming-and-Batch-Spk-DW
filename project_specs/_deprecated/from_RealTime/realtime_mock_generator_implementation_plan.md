# RealTime Mock Generator 实施计划 (V5 - 最终自检版)

本项目严格遵循 [RealTime_Mock_Generator_Design_Doc.md](file:///c:/Users/23977/Desktop/RealTime/RealTime_Mock_Generator_Design_Doc.md) 和 [RealTime_Mock_Generator_Implementation_Doc.md](file:///c:/Users/23977/Desktop/RealTime/RealTime_Mock_Generator_Implementation_Doc.md) 的所有细节。脚本设计为在 Windows 构建、CentOS 7 (hadoop102-104) 生产集群运行。

## 1. 项目初始化与依赖管理 (Phase 1)
- **目标**: 构建兼容 JDK 8 的稳定 Maven 工程。
- **核心依赖 (严格筛选)**:
    - `kafka-clients`: 3.4.0 (官方推荐稳定版)。
    - `jackson-databind` & `jackson-datatype-jsr310`: 2.15.2 (处理 ISO 8601 的工业标准)。
    - `javafaker`: 1.0.2 (生成模拟文本)。
    - `slf4j-simple`: 2.0.7 (轻量级日志)。
- **构建目标**: 使用 `maven-shade-plugin` 产出 Fat Jar，确保在 `hadoop102` 上具备全量依赖。

## 2. 维表管理与种子分布 (Phase 2)
- **目标**: 在 Linux 环境自动维护高仿真维表。
- **路径要求**: **仅识别并处理 Linux 路径 `/home/hyr/data_files`**。启动时自动创建路径及缺失文件。
- **dim_user_profile.csv (250条)**:
    - **画像覆盖率**: 20% (50名) 随机分配 1-3 个静态标签；80% 留空。
    - **等级分布**: 严格按正态分布生成 Level (L1/L5 各 5%, L2/L4 各 20%, L3 50%)。
- **dim_item.csv (100条)**:
    - **类目均衡**: 5 大分类（时尚美妆、生活记录、知识成长、身心健康、家庭生活）各 20 篇。
    - **长度分布**: `content_len` 在 [50, 5000] 均匀分布。

## 3. 数据契约与时间语义 (Phase 3)
- **目标**: 生成符合下游 Spark 解析要求的 JSON 流。
- **时间规范**: 使用 `java.time.ZonedDateTime` (CST/GMT+8)，格式：`YYYY-MM-DDTHH:mm:ss.sssZ`。
- **Schema**: 包含 `event_id`, `user_id`, `event_type`, `ts`, `session_id`, `platform`, `item_id`, `target_user_id`, `duration_sec`, `comment_text`, `content_len`。
- **Kafka 策略**: Key 固定为 `user_id` (Long)，确保用户行为有序性。

## 4. 核心业务仿真逻辑 (Phase 4)
- **推荐算法 (70/30)**:
    - **70% 兴趣流**: 从用户静态标签中随机选一个，并在笔记池中匹配。
    - **30% 探索流**: 全量笔记池均匀随机抽样。
    - **冷启动**: 无画像用户 100% 进入探索流。
- **事件权重**: 曝光 (40%)、消费 (35%)、反馈 (17%)、意向 (4.5%)、辅助 (0.5%)、基础 (3%)。
- **语料库**: 严格使用文档定义的评论 ["写得真好", "学到了"...] 和咨询 ["怎么买？", "链接有吗？"...]。
- **状态机**: 记录 `enter_item` 时间戳，用于计算 `leave_item` 的停留时长。

## 5. 异常注入引擎 (Phase 5)
- **注入规则**:
    - **A1/A2 (刷屏/狂点)**: 基于全局计数器触发，产生连续事件流。
    - **D1 (秒退)**: 15% 概率注入 `duration_sec` ∈ [0.2, 1.2]。
    - **E1 (超速)**: 5% 概率注入 `duration_sec` < 0.8s 且 `content_len` > 2000。
    - **C (挂机)**: 0.5% 概率注入 `duration_sec` ∈ [1800, 3600]。
    - **B1 (乱序)**: 10% 概率将 `ts` 回拨 30-90s。

## 6. 流量形态控制 (Phase 6)
- **模式切换**:
    - **常规**: 30-50 EPS。
    - **洪峰**: 每 10 分钟触发 30s，流速提升至 150-200 EPS。
- **集群配置**: `bootstrap.servers` 指向 `192.168.10.102:9092, 192.168.10.103:9092, 192.168.10.104:9092`。
- **物理延迟**: 发送前随机 `Thread.sleep(5-20ms)`。

## 7. 部署验证 (Phase 7)
- **环境**: 确认所有节点 (hadoop102-104) 已装 JDK 8。
- **验证流**:
    1. Windows 侧执行 `mvn clean package`。
    2. 上传 Jar 至 `hadoop102`。
    3. 运行并验证 `/home/hyr/data_files` 是否自动补全。
    4. 消费 Kafka Topic 验证字段逻辑。

## 关键假设与决策 (Final)
- **纯 Linux 路径**: 代码内不包含 Windows 路径逻辑，仅适配 CentOS 文件系统。
- **Topic 自动创建**: 启动时检查 `content_events`，缺失则创建 (3分区, 2副本)。
- **依赖稳定性**: 所有依赖均为非废弃版，支持在 Linux 长期运行。
