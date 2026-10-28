# infra/ —— 本地环境（WSL2 / Docker Desktop）

> 目的：把"跑在哪、怎么起、怎么验"固定下来，供后续窗口直接复用。
> 更新：见文末。
> 完整组件落点、端到端 runbook 与调试地图见 `project_specs/overall_spec/Deployment_And_Operations.md`。

---

## 0. 宿主与约束（事实）

- 宿主：**WSL2 Ubuntu 26.04**，12 核，**内存硬顶 7.58GB**（Windows 宿主 16GB），磁盘 932GB 可用。
- Docker Desktop **29.7.2**（overlayfs）+ compose **v5.4.0**。
- **沙箱内无 docker 权限、`sudo` 被 `no_new_privs` 禁用、网络受限** → **所有 docker 命令必须在沙箱外执行（向用户申请）**。

---

## 1. 服务清单（当前）

| 服务 | 容器 | 镜像 | 端口 | 状态 |
|---|---|---|---|---|
| Kafka | `crt-kafka` | `apache/kafka:4.1.0` | `9092`(EXT) / `19092`(INT) / `9093`(CTRL) | ✅ 运行（KRaft 单节点） |
| Redis Stack | `crt-redis` | `redis/redis-stack-server:latest` | `6379` | ✅ 运行（**仅 Tier0 缓存，非真值**；maxmemory 512mb） |
| Spark | `crt-spark` | `apache/spark:3.5.3`（内置 Java 11） | `4040` | ⏸ 按需启动（`--profile tools`） |
| SeaweedFS | `crt-s3` | `chrislusf/seaweedfs:latest` | `8333`(S3) / `9333`(master) / `8888`(filer) | ✅ 运行（**S3 网关**，替代 MinIO） |

- Topic：`content_events`、`content_groundtruth`（各 3 分区，RF=1）。
- 实测内存：Kafka ~683MiB、Redis ~7MiB（限制：Kafka 1G / Redis 768M / Spark 3G）。
- 数据卷：`crt_kafka-data`、`crt_redis-data`（WSL2 ext4）。

---

## 2. 监听器约定（**重要**）

| Listener | 地址 | 谁用 |
|---|---|---|
| `EXTERNAL` | `localhost:9092` | WSL2 宿主工具；生成器容器使用 INTERNAL |
| `INTERNAL` | `kafka:19092` | **将来的容器**（如容器化的 Spark） |

> 生成器配置 **`bootstrap.servers = localhost:9092`**。
> 双 listener 一开始就配好，**避免 Spark 容器化时返工**。

---

## 3. 启停与验证

```bash
# 首次：生成本地 S3 凭证文件（git 不跟踪）
cp infra/seaweedfs/s3.example.json infra/seaweedfs/s3.json

# 启动
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps

# 停止（保留数据卷）
docker compose -f infra/docker-compose.yml down
# 停止并删卷（数据清空）
docker compose -f infra/docker-compose.yml down -v
```

**已验证（2026-10-16）**：topic 建/查通过；produce/consume 往返通过；宿主 `127.0.0.1:9092` 可达。

---

## 4. 待办与注意

- **内存是硬顶**：7.58GB ＝ 宿主 16GB 的一半。`.wslconfig` **已写入**（`memory=11GB, processors=10`），
  **待你在方便时 `wsl --shutdown` 生效**（会中断当前会话）。
- **对象存储已改用 SeaweedFS**：`minio/minio` 仓库在 Docker Hub **已整体消失**（API 返回 404）、`quay.io` 401。
  → 改用 **SeaweedFS**（Apache-2.0，单容器含 S3 网关）。**存储落点＝S3（path-style）**，lake root ＝ `s3a://crt-lake/`。
  · S3 端点：容器内 `http://seaweedfs:8333`，宿主 `http://localhost:8333`
  · 凭证：复制 `infra/seaweedfs/s3.example.json` 为 `infra/seaweedfs/s3.json`（默认 accessKey `crtadmin` / secretKey `crtadmin123`；实际 `s3.json` 已被 `.gitignore` 忽略）
  · 已用 aws-cli 验证：建桶 ✔ / 上传 ✔ / 未签名请求 403（鉴权生效）✔
- **Spark 镜像内置 Java 11**（官方 `apache/spark:3.5.3`，实测 `11.0.24`），宿主 Java 17 仅用于构建
  → 编译目标取 **JVM 8/11**，保证 jar 能在容器里跑。
- **版本配对是硬约束**：`delta-spark_2.12 3.2.1` 是对 **Spark 3.5.3** 编译的；放在 Spark 3.5.1 上跑，MERGE 会抛
  `NoSuchMethodError: ExpressionSet.$plus$plus`。**改 Delta 版本就必须同步改镜像 tag**（配对表见 §5）。
- **拉取镜像需要网络授权**（已验证可拉：`redis/redis-stack-server`、`apache/spark:3.5.1`）。
- **勿动其他项目的容器**：`ecom-kafka`、`minikube`、`pyflink`、`postgres`（`hw-mongo` 已按要求停止）。

---

## 5. S3A / Delta 冒烟（2026-10-16 通过，14/14）

### 5.1 依赖获取（宿主下载 + 挂载，不依赖容器内联网）

```bash
# 一次性把额外依赖拉到 infra/jars/（pom 里列了 delta / hadoop-aws / aws-sdk / spark-sql-kafka）
mvn -s RealTime/settings.xml -f infra/jars/pom.xml -B dependency:copy-dependencies \
    -DoutputDirectory=$PWD/infra/jars
```

- jar 体积较大（`aws-java-sdk-bundle` 约 280MB），**不入库**（`.gitignore` 已忽略 `infra/jars/*.jar`）。
- 容器通过 compose 里已有的 `./:/opt/crt/infra` 挂载复用同一份 jar；运行期只挂"必要的那几个"，
  避免与镜像自带的 `hadoop-client-runtime` 重复。

### 5.2 版本配对（硬约束）

| Delta | 编译自的 Spark | 与我们的镜像是否配对 |
|---|---|---|
| delta-spark_2.12 3.0.0 / 3.1.0 / 3.2.0 | Spark 3.5.0 | ✗（未验证） |
| **delta-spark_2.12 3.2.1** | **Spark 3.5.3** | ✅ **当前采用** |
| delta-spark_2.12 3.2.1 | Spark 3.5.1 | ✗ MERGE 抛 `NoSuchMethodError: ExpressionSet.$plus$plus` |

> 教训：Delta 与 Spark 的补丁号必须**精确配对**，不能只看"3.5.x 兼容 3.5"。冒烟脚本的价值正在于此。

### 5.3 冒烟脚本

`infra/scripts/delta_s3a_smoke.scala` —— 14 项检查，覆盖：清理幂等 / 建表写入 / 读回（DF API + SQL）/
大文件 multipart / `replaceWhere` 分区重写 / `MERGE` / 时间旅行 + 历史 / list-after-write /
`VACUUM` 真删 / 流式写 Delta + checkpoint 落 S3A / SQL `DELETE` / 流式 checkpoint 重启续跑 /
VACUUM 与 Delta 引用对齐。

```bash
docker compose -f infra/docker-compose.yml --profile tools run --rm \
  --entrypoint /opt/spark/bin/spark-shell spark \
  --master 'local[4]' --driver-memory 2g \
  --jars /opt/crt/infra/jars/delta-spark_2.12-3.2.1.jar,/opt/crt/infra/jars/delta-storage-3.2.1.jar,/opt/crt/infra/jars/hadoop-aws-3.3.4.jar,/opt/crt/infra/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/crt/infra/jars/antlr4-runtime-4.9.3.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://seaweedfs:8333 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.access.key=crtadmin \
  --conf spark.hadoop.fs.s3a.secret.key=crtadmin123 \
  --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
  --conf spark.hadoop.fs.s3a.multipart.size=16777216 \
  --conf spark.sql.warehouse.dir=s3a://crt-lake/warehouse \
  -i /opt/crt/infra/scripts/delta_s3a_smoke.scala
```

### 5.4 冒烟结果（要点）

- **14/14 PASS**；committer 用**默认**（`fs.s3a.committer.name=file`），无需 magic committer。
- multipart 上传生效（`fs.s3a.multipart.size=16M`，单文件 69.8MB）；`list-after-write` 成立。
- **VACUUM 真删**：对象数下降、陈旧文件 0 残留；磁盘空间不会无限膨胀。
- **流式 checkpoint 落 S3A 后重启续跑，重复行 = 0**（`versions 5 → 10`，行数单调增长）。
- **语义提醒（P3 必守）**：Delta 的 `replaceWhere` / `DELETE` **只在日志里移除引用**，物理文件靠 `VACUUM` 回收
  （实测：引用 4 个文件、S3 上 16 个 → VACUUM 后 4 个，回收 12 个）。批侧按窗口重算后要配 VACUUM。

### 5.5 数仓（T 层）构建与运行

```bash
# 1) 构建（宿主；首次需联网，之后可加 -o）
mvn -s RealTime/settings.xml -B clean package      # → target/crt-warehouse-0.1.0.jar

# 2) 前提：三个中间件都在跑（曾因 crt-s3 静默停摆，表现为"写 S3 卡住"）
docker compose -f infra/docker-compose.yml ps

# 2.1) 从空环境恢复 topic 和 lake bucket
infra/scripts/bootstrap_runtime.sh

# 3) 三个入口（脚本已封装 jar 列表、驱动内存、topic 覆盖）
infra/scripts/run_warehouse.sh com.hub.app.StreamApp --reset     # 常驻：Kafka→ODS + ODS→Tier0
infra/scripts/run_warehouse.sh com.hub.app.BatchApp --step all --days 2
infra/scripts/run_warehouse.sh com.hub.app.VerifyApp --days 3
infra/scripts/run_warehouse.sh com.hub.app.GateApp --stage all --days 2
infra/scripts/run_warehouse.sh com.hub.app.VacuumApp --hours 168   # 周维护/人工触发

# 生成器容器（profile: observation-normal / observation-incidents / observation-peak）
docker compose -f infra/docker-compose.yml --profile generator build generator
CRT_GENERATOR_PROFILE=observation-normal \
  docker compose -f infra/docker-compose.yml --profile generator up generator

# 整体观测（容器/Kafka/Redis/生成器清单/Delta/Verify/Gate）
CRT_GENERATOR_RUN_NAME=observation-normal infra/scripts/observe_warehouse.sh

# 或：一次完成构建、生成、批处理、对账、Gate、观测
PROFILE=observation-normal RUN_NAME=obs-$(date +%Y%m%d-%H%M%S) \
  DAYS=3 TARGET_DT=auto infra/scripts/run_observation.sh

# 生成器无 Kafka 的状态恢复冒烟（P3.9）
cd RealTime
mvn -o -s settings.xml -B test-compile
state_dir="$(mktemp -d /tmp/crt-state-smoke.XXXXXX)"
java -Dcrt.test.data.dir="$state_dir" -Ddata.dir="$state_dir" \
  -Dcrt.seed.source=data_files \
  -cp target/classes:target/test-classes com.realtime.mock.generator.StatePersistenceSmoke

# 4) 验收脚本（在容器里跑 spark-shell）
infra/scripts/run_shell.sh /opt/crt/infra/scripts/verify_p3_batch3.scala
```

**注意**：

- `StreamApp --reset` 只清**本作业自己的** checkpoint；若上次是 `kill` 掉的，先确认没有残留的 `crt-spark-run-*` 容器。
- 批作业与对账都是一次性进程；批作业按 `--days N` 重算"最近 N 个事件天"，起点由 ODS 的最大日期推出（不是机器当天）。
- 运行期依赖全在 `infra/jars/`（宿主 `mvn` 预下载、容器只读挂载）；版本必须与镜像配对（见 §5.2）。
- Airflow DAG 在 `infra/airflow/dags/`；当前为 Docker LocalExecutor + Postgres，UI 在 `localhost:8080`，
  启动/停止见 `infra/airflow/README.md`。宿主 venv 保留为备用。

## 6. 变更记录

| 日期 | 变更 |
|---|---|
| 2026-10-16 | 首版：起 `crt-kafka`（KRaft 双 listener），建 `content_events`，冒烟验证通过；停 `hw-mongo` |
| 2026-10-16 | 补环境：起 `crt-redis`（Redis Stack，PONG 验证）；拉取 `apache/spark:3.5.1`（1.56GB，内置 Java 11）；
  **MinIO 镜像不可用** → 下沉 profile `s3`，存储改为「可配置 lake root」（当前本地 FS） |
| 2026-10-16 | **对象存储落定 SeaweedFS**（MinIO 仓库已消失）：起 `crt-s3`（S3 网关 8333），aws-cli 验证建桶/上传/鉴权通过；lake root ＝ `s3a://crt-lake/` |
| 2026-10-16 | **Delta over S3A 冒烟通过（14/14）**：新增 §5（依赖获取 / 版本配对 / 冒烟脚本与结果）；Spark 镜像 3.5.1 → **3.5.3** |
| 2026-10-20 | 新增 §5.5 **数仓构建与运行**（Maven 薄 jar、三个入口、验收脚本、`crt-s3` 必须在线）；与 `AGENTS.md` §5.0 对齐 |
| 2026-10-20 | **P3.8 运行验收**：补 `GateApp`/`VacuumApp` 命令、Airflow DAG 指针；完整搭建/调试见 `Deployment_And_Operations.md` |
| 2026-10-28 | **全容器恢复与观测**：Docker 镜像/数据卷从空环境恢复；新增 `bootstrap_runtime.sh`；生成器镜像和 profile；Airflow Docker LocalExecutor+Postgres 实跑并验证 scheduler→Spark；新增 `observe_warehouse.sh` |
