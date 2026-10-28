"""CRT 周维护：人工批准的 Delta VACUUM。

VACUUM 会删除 Delta 历史文件，属于有破坏性的维护动作，所以默认不自动调度。
"""

# os 用于读取项目根目录环境变量。
import os
# datetime 给 DAG 起始日期。
from datetime import datetime

# Airflow DAG 容器对象。
from airflow import DAG
# 通过 BashOperator 调用项目里的 VacuumApp。
from airflow.operators.bash import BashOperator

# Airflow worker 上的项目根目录；可用 CRT_PROJECT_ROOT 覆盖。
ROOT = os.environ.get("CRT_PROJECT_ROOT", "/home/henry/projects/Community_Real_Time")

# 创建维护 DAG。
with DAG(
    # DAG 唯一 id。
    dag_id="crt_weekly_vacuum",
    # UI 描述。
    description="人工批准的 Delta VACUUM（默认 7 天保留期）",
    # DAG 起始日期。
    start_date=datetime(2026, 10, 1),
    # None = 不自动调度，只能手工触发。
    schedule=None,
    # 不回补历史。
    catchup=False,
    # 同一时间最多一个 VACUUM。
    max_active_runs=1,
    # 手工触发时可覆盖：{"hours": 168}。
    params={"hours": 168},
    # UI 标签。
    tags=["crt", "maintenance", "vacuum"],
) as dag:
    # 只创建一个任务：调用 VacuumApp。
    BashOperator(
        task_id="vacuum",
        # --hours 168 表示保留最近 7 天；小于 7 天必须人工确认。
        bash_command=(
            f"cd {ROOT} && infra/scripts/run_warehouse.sh "
            "com.hub.app.VacuumApp --hours {{ params.hours }}"
        ),
    )
