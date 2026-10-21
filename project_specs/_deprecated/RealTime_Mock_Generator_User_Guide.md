> ⛔【已被取代】本文件内容已收编进 `project_specs/mock_generator/Mock_Source_Spec.md`（生成器文档唯一现行正本）。
> 仅作历史参考，**不要作为现行依据**。

==========================================================================
                RealTime Mock Generator 产品使用手册 (V1.1)
==========================================================================

一、产品概述
--------------------------------------------------------------------------
RealTime Mock Generator 是一款专为"小红书式"内容社区实时数仓设计的无界流数据模拟工具。
该工具通过 Java 8 编写，能够高仿真模拟真实用户行为，产出包含严谨时间语义、具备数据质量
挑战（乱序、异常）的实时日志，是测试 Spark/Flink 实时处理链路的理想数据源。

二、核心功能特性
--------------------------------------------------------------------------
1. 业务逻辑高仿真：
   - 支持 13 种社区行为：app_open, enter_item, leave_item, like, fav, share, 
     dislike, dm, follow, unfollow, click_tag, enter_profile, comment。
   - 70/30 推荐算法：模拟 70% 兴趣匹配（基于用户画像标签）+ 30% 随机探索逻辑。
   - 会话状态追踪：通过 session_id 维护行为连贯性，支持 30 分钟超时自动切换。

2. 严谨的时间语义：
   - 格式：严格遵守 ISO 8601 (YYYY-MM-DDTHH:mm:ss.sssZ)。
   - 时区：锁定 CST (GMT+8)。
   - 乱序注入：内置 10% 的乱序延迟逻辑（30-90s），挑战下游系统的 Watermark 处理。

3. 工业级异常注入引擎：
   - A1 刷屏评论：全局每 1000 条触发，单用户对单笔记连续发送 5 条评论。
   - A2 疯狂点赞：全局每 1000 条触发，单用户 5s 内对 15 篇不同笔记点赞。
   - D1 秒退：leave_item 事件 15% 概率触发，停留时长限制在 [0.2, 1.2]s。
   - E1 超速滑动：leave_item 事件 5% 概率触发，时长 < 0.8s 且内容长度 > 2000。
   - C 极长驻留：leave_item 事件 0.5% 概率触发，停留时长在 [1800, 3600]s。
   - B1 乱序数据：全量事件 10% 概率触发，时间戳回拨 30-90s。

三、运行环境与规格
--------------------------------------------------------------------------
- 语言环境：Java 8 (JDK 1.8.0_412+)
- 消息队列：Kafka (默认集群 具体IP:9092)
- 默认 Topic：content_events (自动创建，3 分区, 2 副本)
- 维表存储：Linux 本地目录 /home/您的账号/data_files (自动管理)
- 物理延迟：消息发送前随机注入 5-20ms 真实物理传输延迟。

四、详细使用指南
--------------------------------------------------------------------------
1. 部署环境准备：
   - 节点要求：建议在 hadoop102 节点运行，确保能访问集群内所有 Kafka Broker。
   - 目录权限：执行命令 `mkdir -p /home/您的账号/data_files` 确保目录存在且当前用户有写权限。
   - JDK 检查：执行 `java -version` 确认版本为 1.8.x。

2. 程序启动 (Start)：
   - 将 Fat Jar (mock-generator-1.0-SNAPSHOT.jar) 上传至虚拟机。
   - 使用前台运行（查看实时日志）：
     java -jar mock-generator-1.0-SNAPSHOT.jar
   - 使用后台运行（生产测试建议）：
     nohup java -jar mock-generator-1.0-SNAPSHOT.jar > generator.log 2>&1 &

3. 程序停止 (Stop)：
   - 若是前台运行：直接按下键盘 `Ctrl + C`。
   - 若是后台运行：
     step 1: 执行 `ps -ef | grep mock-generator` 查找进程 ID (PID)。
     step 2: 执行 `kill -15 <PID>` 优雅停止程序（程序会完成最后的 Kafka 发送并关闭连接）。

4. 流量整形 (Traffic Shaping) 使用说明：
   本工具内置了自动化的流量整形机制，无需人工手动干预，旨在还原真实的社区流量波动。
   - 触发逻辑：程序内部维护一个全局计时器。
   - 常规模式 (Normal)：
     - 表现：每秒生成 30-50 条数据。
     - 状态：这是程序的默认运行状态，模拟社区平稳时段。
   - 洪峰模式 (Peak)：
     - 触发频率：每运行 10 分钟自动触发一次。
     - 持续时间：每次洪峰持续 30 秒。
     - 表现：流速瞬间提升至 150-200 EPS。
     - 目的：用于观察下游 Spark/Flink 的 Backpressure (背压) 情况及处理积压数据的速度。

五、数据验证步骤
--------------------------------------------------------------------------
1. 维表自动生成验证：
   检查 /home/您的账号/data_files 下是否自动生成了：
   - dim_user_profile.csv (250条，20%带画像标签)
   - dim_item.csv (100条，5大分类均衡分布)

2. Kafka 实时流验证：
   在虚拟机终端执行：
   kafka-console-consumer.sh --bootstrap-server 具体IP:9092 --topic content_events --property print.key=true

六、详细 FAQ (常见问题与故障排查)
--------------------------------------------------------------------------
Q1: 启动时报错 "Failed to load class 'org.slf4j.impl.StaticLoggerBinder'" 怎么办？
A1: 该错误已在 V1.0 正式版中通过降级 slf4j-simple 并在打包时合并 Service 接口解决。
    请确保您使用的是最新构建的 Fat Jar。如果仍出现警告但不影响程序输出 "Generator is running"，则可以忽略。

Q2: 程序提示 "Failed to load dimension data" 并退出？
A2: 请检查：
    1. 运行环境是否为 Linux？(该脚本不适配 Windows 路径)。
    2. /home/hyr/data_files 目录是否存在？如果不存在，程序会尝试创建，请确保权限充足。
    3. 检查 CSV 文件是否损坏，如有问题可删除该文件夹，程序会在下次启动时重新生成。

Q3: Kafka 无法发送消息，提示 "TimeoutException" 或 "Connection Refused"？
A3: 请检查：
    1. 虚拟机防火墙是否关闭 (systemctl stop firewalld)。
    2. Kafka 的 server.properties 中 listeners 是否正确配置为集群 IP。
    3. Windows 物理机是否能 ping 通 具体IP。

Q4: 为什么我的 IDEA 控制台看不到数据？
A4: 请检查：
    1. 虚拟机端的 Generator 是否正在运行并提示发送成功？
    2. IDEA 端的消费者代码中 bootstrap.servers 是否配置正确？
    3. 检查 Topic 名称是否统一为 "content_events"。

Q5: 运行一段时间后 HDFS 空间会不足吗？
A5: 不会。本工具生成的 Mock 数据仅发送至 Kafka 内存/磁盘缓冲区。
    除非您运行了额外的 Flink/Flume Sink 将数据写入 HDFS，否则不会对 HDFS 产生任何污染。

Q6: 如何调整生成数据的流速？
A6: 目前流速硬编码在 Main.java 中以符合设计要求。如需修改，请调整 Main 类中的 eps 变量计算逻辑并重新打包。

Q7: 为什么 session_id 格式是 sess_{ts}_{user_id}？
A7: 这种格式保证了 session_id 的全局唯一性，且包含了时间语义，方便下游进行基于 Session 的窗口计算。

==========================================================================
                        (完) 2026-05-26 发行
==========================================================================
