"""CRT 每日批链路（Airflow 2.x/3.x，LocalExecutor）。

这一层只负责“按顺序调用命令”，不做数据计算：
  - Airflow 决定任务先后、失败重试和手工补跑；
  - 真正的 Spark 作业由 infra/scripts/run_warehouse.sh 提交；
  - Gate 规则由 GateApp -> GateRunner 执行。
"""

# os 用于读取项目根目录环境变量。
import os
# datetime 给 DAG 起始日期；timedelta 给默认重试间隔。
from datetime import datetime, timedelta

# Airflow 的 DAG 容器对象。
from airflow import DAG
# BashOperator 用来执行 shell 命令；这里统一走 run_warehouse.sh。
from airflow.operators.bash import BashOperator

# Airflow worker 上项目根目录；不同机器可用 CRT_PROJECT_ROOT 覆盖。
ROOT = os.environ.get("CRT_PROJECT_ROOT", "/home/henry/projects/Community_Real_Time")

# Airflow Jinja 模板：只有 dag_run.conf 里传了 dt 才追加 --dt 参数。
# 示例：{"dt": "2026-10-18"} -> GateApp --dt 2026-10-18
DT_ARG = "{{ ('--dt ' + dag_run.conf[\"dt\"]) if dag_run.conf and dag_run.conf.get('dt') else '' }}"

# DAG 默认参数：owner 只用于 UI 展示。
default_args = {
    "owner": "crt",
    # 不交给 Airflow 重试，因为 Gate 的 G1 等待由 GateApp --wait 内部处理。
    "retries": 0,
    # 保留 Airflow 默认重试间隔；当前 retries=0，所以实际不触发。
    "retry_delay": timedelta(minutes=1),
}

# 创建每日批 DAG。
with DAG(
    # DAG 唯一 id；Airflow UI 中看到的名字。
    dag_id="crt_daily_batch",
    # 人类可读描述，概括整条链路。
    description="ODS → DIM → DWD → DWS → semantic → verify → Gate v1",
    # DAG 起始日期；Airflow 用它计算调度区间。
    start_date=datetime(2026, 10, 1),
    # 每天调度一次。
    schedule="@daily",
    # 不回补历史日期，避免本地小机器一次补很多天。
    catchup=False,
    # 同一时间最多一个 DAG run，避免两个 Spark 批任务同时写同一批分区。
    max_active_runs=1,
    # 默认参数。
    default_args=default_args,
    # 手工触发时可覆盖：{"days": 3}。
    params={"days": 2},
    # UI 分类标签。
    tags=["crt", "warehouse", "gate"],
) as dag:

    # 统一拼命令：cd 到项目根目录，再调用 run_warehouse.sh。
    def cmd(entry: str, extra: str = "") -> str:
        # entry 是 main class；extra 是传给这个 class 的参数。
        return f"cd {ROOT} && infra/scripts/run_warehouse.sh {entry} {extra}".strip()

    # 任务 1：DR 就绪检查。
    gate_dr = BashOperator(
        task_id="gate_dr",
        # --wait 让 GateApp 在 DR-3 水位未到时内部等待，而不是让 Airflow 直接失败。
        bash_command=cmd("com.hub.app.GateApp", f"--stage dr --days {{{{ params.days }}}} {DT_ARG} --wait"),
    )

    # 任务 2：重建维表（seed 快照 + 事件驱动维表）。
    dim = BashOperator(
        task_id="dim",
        bash_command=cmd("com.hub.app.BatchApp", "--step dim"),
    )

    # 任务 3：重建明细层（曝光展开、点击、消费配对、互动、关系、发布、会话）。
    dwd = BashOperator(
        task_id="dwd",
        bash_command=cmd("com.hub.app.BatchApp", f"--step dwd --days {{{{ params.days }}}}"),
    )

    # 任务 4：DWD 后检查 IV-1（曝光展开守恒）。
    gate_dwd = BashOperator(
        task_id="gate_dwd",
        bash_command=cmd("com.hub.app.GateApp", f"--stage dwd --days {{{{ params.days }}}} {DT_ARG}"),
    )

    # 任务 5：重建主题层（11 张 DWS）。
    dws = BashOperator(
        task_id="dws",
        bash_command=cmd("com.hub.app.BatchApp", f"--step dws --days {{{{ params.days }}}}"),
    )

    # 任务 6：重建语义层（40 个指标 + nature + limitations）。
    semantic = BashOperator(
        task_id="semantic",
        bash_command=cmd("com.hub.app.BatchApp", "--step semantic"),
    )

    # 任务 7：DWS/语义层后检查 IV-2、CV、QL。
    gate_dws = BashOperator(
        task_id="gate_dws",
        bash_command=cmd("com.hub.app.GateApp", f"--stage dws --days {{{{ params.days }}}} {DT_ARG}"),
    )

    # 任务 8：跑 V-01 归因对账 + V-02 热度对账，写 verify/*。
    verify = BashOperator(
        task_id="verify",
        bash_command=cmd("com.hub.app.VerifyApp", f"--days {{{{ params.days }}}}"),
    )

    # 任务 9：最终 Gate；stage=verify 会包含 DR/IV/CV/QL/RC。
    gate_verify = BashOperator(
        task_id="gate_verify",
        bash_command=cmd("com.hub.app.GateApp", f"--stage verify --days {{{{ params.days }}}} {DT_ARG}"),
    )

    # DAG 依赖链：Gate 检查紧跟在它要检查的层之后。
    # gate_dr -> dim -> dwd -> gate_dwd -> dws -> semantic -> gate_dws -> verify -> gate_verify
    gate_dr >> dim >> dwd >> gate_dwd >> dws >> semantic >> gate_dws >> verify >> gate_verify
