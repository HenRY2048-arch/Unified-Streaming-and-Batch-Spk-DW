"""CRT 每日批链路（Airflow 2.x/3.x，LocalExecutor）。

前提：Airflow worker 能执行项目根目录下的 `infra/scripts/run_warehouse.sh`
（宿主机 venv，或挂载 Docker socket 的 Airflow 容器）。项目根路径用
`CRT_PROJECT_ROOT` 覆盖，默认 `/home/henry/projects/Community_Real_Time`。
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ.get("CRT_PROJECT_ROOT", "/home/henry/projects/Community_Real_Time")
DT_ARG = "{{ ('--dt ' + dag_run.conf[\"dt\"]) if dag_run.conf and dag_run.conf.get('dt') else '' }}"

default_args = {
    "owner": "crt",
    "retries": 0,  # G1 由 GateApp --wait 内部处理，不交给 Airflow 重试
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="crt_daily_batch",
    description="ODS → DIM → DWD → DWS → semantic → verify → Gate v1",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"days": 2},
    tags=["crt", "warehouse", "gate"],
) as dag:
    def cmd(entry: str, extra: str = "") -> str:
        return f"cd {ROOT} && infra/scripts/run_warehouse.sh {entry} {extra}".strip()

    gate_dr = BashOperator(
        task_id="gate_dr",
        bash_command=cmd("com.hub.app.GateApp", f"--stage dr --days {{{{ params.days }}}} {DT_ARG} --wait"),
    )
    dim = BashOperator(task_id="dim", bash_command=cmd("com.hub.app.BatchApp", "--step dim"))
    dwd = BashOperator(
        task_id="dwd",
        bash_command=cmd("com.hub.app.BatchApp", f"--step dwd --days {{{{ params.days }}}}"),
    )
    gate_dwd = BashOperator(
        task_id="gate_dwd",
        bash_command=cmd("com.hub.app.GateApp", f"--stage dwd --days {{{{ params.days }}}} {DT_ARG}"),
    )
    dws = BashOperator(
        task_id="dws",
        bash_command=cmd("com.hub.app.BatchApp", f"--step dws --days {{{{ params.days }}}}"),
    )
    semantic = BashOperator(task_id="semantic", bash_command=cmd("com.hub.app.BatchApp", "--step semantic"))
    gate_dws = BashOperator(
        task_id="gate_dws",
        bash_command=cmd("com.hub.app.GateApp", f"--stage dws --days {{{{ params.days }}}} {DT_ARG}"),
    )
    verify = BashOperator(
        task_id="verify",
        bash_command=cmd("com.hub.app.VerifyApp", f"--days {{{{ params.days }}}}"),
    )
    gate_verify = BashOperator(
        task_id="gate_verify",
        bash_command=cmd("com.hub.app.GateApp", f"--stage verify --days {{{{ params.days }}}} {DT_ARG}"),
    )

    gate_dr >> dim >> dwd >> gate_dwd >> dws >> semantic >> gate_dws >> verify >> gate_verify
