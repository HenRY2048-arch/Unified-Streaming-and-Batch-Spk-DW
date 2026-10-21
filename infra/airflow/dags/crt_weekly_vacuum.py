"""CRT 周维护：人工批准的 Delta VACUUM（不在每日主链）。

默认 `schedule=None`；确认保留期与对账结果后手动触发。保留期默认 7 天（168h）。
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ.get("CRT_PROJECT_ROOT", "/home/henry/projects/Community_Real_Time")

with DAG(
    dag_id="crt_weekly_vacuum",
    description="人工批准的 Delta VACUUM（默认 7 天保留期）",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"hours": 168},
    tags=["crt", "maintenance", "vacuum"],
) as dag:
    BashOperator(
        task_id="vacuum",
        bash_command=(
            f"cd {ROOT} && infra/scripts/run_warehouse.sh "
            "com.hub.app.VacuumApp --hours {{ params.hours }}"
        ),
    )
