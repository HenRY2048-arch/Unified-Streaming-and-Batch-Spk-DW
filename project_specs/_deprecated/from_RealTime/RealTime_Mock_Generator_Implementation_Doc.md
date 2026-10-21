# **RealTime Mock Generator (Java 实施文档)**

## **1. 项目基调与目标**
- **语言环境**：Java 8 (JDK 1.8.0_412)
- **构建工具**：Maven
- **核心任务**：模拟小红书社区行为流，向 Kafka 集群推送高质量、带异常注入的 JSON 数据。
- **下游对接**：Spark Structured Streaming 3.x (需确保数据 Key 分区合理且时间语义严谨)。

## **2. 环境与基础设施配置**
- **Kafka 集群**：
  - `bootstrap.servers`: `192.168.10.102:9092,192.168.10.103:9092,192.168.10.104:9092`
  - `topic`: `content_events` (若不存在，需自动创建：3 分区，2 副本)
- **文件系统**：
  - 根目录：`/home/hyr/data_files` (脚本启动时需检查并自动创建此目录)
  - 维表文件：`dim_user_profile.csv`, `dim_item.csv`

## **3. 核心依赖清单 (pom.xml)**
AI 需包含以下核心库：
- `kafka-clients`: 3.x (兼容 JDK 8)
- `jackson-databind` & `jackson-datatype-jsr310`: 用于 JSON 序列化及 ISO 8601 时间处理
- `javafaker`: 用于模拟评论语料和用户属性
- `maven-shade-plugin`: 用于打包 Fat Jar

## **4. 模块化实施路径**

### **第一阶段：维表种子生成 (Master Data Seeding)**
1. **自动路径管理**：在 `/home/hyr/data_files` 下检查维表是否存在。
2. **dim_user_profile.csv (250条)**：
   - 字段：`user_id` (100001-100250), `interest_tags` (String), `level` (Integer)
   - 算法：20% 用户拥有 1-3 个标签（标签取自设计文档分类），80% 留空；Level 呈正态分布。
3. **dim_item.csv (100条)**：
   - 字段：`item_id` (200001-200100), `category` (String), `content_len` (50-5000)
   - 算法：5 大分类各 20 篇，内容长度均匀分布。

### **第二阶段：数据模型与时间语义**
1. **Event POJO**：采用“超集 Schema”设计，包含设计文档中提及的 11 个核心字段。
2. **时间格式化**：
   - 使用 `java.time.ZonedDateTime` 配合 `CST (GMT+8)` 时区。
   - 格式：`YYYY-MM-DDTHH:mm:ss.sssZ` (符合 ISO 8601，Spark 直接解析)。
3. **Kafka Key 策略**：每条消息必须携带 `user_id` 或 `session_id` 作为 Key，以确保下游 Spark 分区处理的有序性。

### **第三阶段：行为模拟逻辑 (The Vibe Logic)**
1. **70/30 推荐算法**：
   - 70% 概率：从用户静态 `interest_tags` 中随机取一个，并在 `dim_item` 中匹配相同标签的笔记。
   - 30% 概率（或冷启动用户）：全量笔记池随机抽样。
2. **13 种事件权重状态机**：
   - 严格遵循设计文档中的 `event_type` 权重分布（如 `enter_item` 40%, `leave_item` 35%）。
   - 维护 `(user_id, item_id)` 的进入时间戳，用于在 `leave_item` 事件中计算 `duration_sec`。
3. **会话管理**：
   - `session_id` 格式：`sess_{ts}_{user_id}`。
   - 若用户 30 分钟无活动，或触发 `app_open`，则更新 `session_id`。

### **第四阶段：异常注入引擎**
必须实现的 6 种异常模式：
- **A1/A2 (刷屏/狂点)**：基于全局计数器触发，产生连续的 `comment` 或短时间内的 `like`。
- **D1/E1 (秒退/超速)**：在 `leave_item` 生成时按概率注入极短停留时间。
- **C (极长驻留)**：注入 30-60 分钟的停留时间。
- **B1 (乱序数据)**：10% 概率将 `ts` 设置为 `current_time - random(30, 90)s`。

### **第五阶段：流速控制 (Traffic Shaping)**
- **常规模式**：30-50 EPS。
- **洪峰模式**：每 10 分钟触发一次，持续 30s，提升至 150-200 EPS。
- **物理延迟**：发送前随机 Thread.sleep(5-20ms)。

## **5. 运行与交付规格**
- **打包指令**：`mvn clean package` 产出 `mock-generator-1.0-SNAPSHOT-jar-with-dependencies.jar`。
- **运行指令**：`java -cp mock-generator.jar com.yourpath.Main`。
- **验证指标**：
  - 启动后检查 `/home/hyr/data_files` 是否生成 CSV。
  - 使用 `kafka-console-consumer` 观察 JSON 是否包含 `ts` 且 Key 不为空。

---

