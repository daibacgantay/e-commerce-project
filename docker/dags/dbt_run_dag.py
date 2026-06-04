import os
import subprocess

from airflow.sdk import Asset, dag, task
from datetime import datetime


bronze_updated = Asset("snowflake://ecommerce/bronze")


@dag(
    dag_id="dbt_run",
    schedule=[bronze_updated],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "gold", "transform"],
)
def dbt_run():

    @task()
    def run_dbt() -> None:
        dbt_dir = "/opt/airflow/dbt/ecommerce_dbt"

        env = {
            **os.environ,
            "SNOWFLAKE_ACCOUNT":   os.environ["SNOWFLAKE_ACCOUNT"],
            "SNOWFLAKE_USER":      os.environ["SNOWFLAKE_USER"],
            "SNOWFLAKE_PASSWORD":  os.environ["SNOWFLAKE_PASSWORD"],
            "SNOWFLAKE_DATABASE":  os.environ["SNOWFLAKE_DATABASE"],
            "SNOWFLAKE_WAREHOUSE": os.environ["SNOWFLAKE_WAREHOUSE"],
            "SNOWFLAKE_ROLE":      os.environ.get("SNOWFLAKE_ROLE", ""),
        }

        dbt_bin = "/home/airflow/dbt-venv/bin/dbt"
        profiles_dir = "/opt/airflow/dbt/ecommerce_dbt"

        for cmd in [
            [dbt_bin, "deps", "--project-dir", dbt_dir, "--profiles-dir", profiles_dir],
            [dbt_bin, "run",  "--project-dir", dbt_dir, "--profiles-dir", profiles_dir],
            [dbt_bin, "test", "--project-dir", dbt_dir, "--profiles-dir", profiles_dir],
        ]:
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            print(result.stdout)
            if result.returncode != 0:
                print(result.stderr)
                raise RuntimeError(f"Command failed: {' '.join(cmd)}")

    run_dbt()


dbt_run()