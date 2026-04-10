from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

# ── DEFAULT ARGS ─────────────────────────────────────────────────
default_args = {
    "owner":            "divyanshi",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────
dag = DAG(
    dag_id          = "spotify_elt_pipeline",
    description     = "Weekly ELT pipeline — iTunes charts → Snowflake → dbt",
    default_args    = default_args,
    schedule_interval = "0 0 * * 0",   # every Sunday at midnight
    start_date      = datetime(2026, 4, 1),
    catchup         = False,            # don't backfill missed runs
    tags            = ["spotify", "elt", "snowflake", "dbt"],
)

# ── TASK 1: EXTRACT + LOAD ───────────────────────────────────────
def run_extract_load():
    """Run the extract and load script."""
    script_path = "/opt/airflow/extract/spotify_to_snowflake.py"

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output = True,
        text           = True,
        env            = {
            **os.environ,
            "SPOTIFY_CLIENT_ID":     os.environ.get("SPOTIFY_CLIENT_ID", ""),
            "SPOTIFY_CLIENT_SECRET": os.environ.get("SPOTIFY_CLIENT_SECRET", ""),
        }
    )

    # Print output to Airflow logs
    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Extract + Load failed:\n{result.stderr}")

    print("Extract + Load completed successfully!")


task_extract_load = PythonOperator(
    task_id         = "extract_and_load",
    python_callable = run_extract_load,
    dag             = dag,
)

# ── TASK 2: DBT RUN ──────────────────────────────────────────────
task_dbt_run = BashOperator(
    task_id  = "dbt_run",
    bash_command = """
        cd /opt/airflow/spotify_dbt && \
        /opt/airflow/dbt_venv/bin/dbt run \
            --profiles-dir /opt/airflow/spotify_dbt \
            --project-dir  /opt/airflow/spotify_dbt
    """,
    dag = dag,
)

# ── TASK 3: DBT TEST ─────────────────────────────────────────────
task_dbt_test = BashOperator(
    task_id  = "dbt_test",
    bash_command = """
        cd /opt/airflow/spotify_dbt && \
        /opt/airflow/dbt_venv/bin/dbt test \
            --profiles-dir /opt/airflow/spotify_dbt \
            --project-dir  /opt/airflow/spotify_dbt
    """,
    dag = dag,
)

# ── TASK DEPENDENCIES ────────────────────────────────────────────
# This defines the order: 1 → 2 → 3
task_extract_load >> task_dbt_run >> task_dbt_test