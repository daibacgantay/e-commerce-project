FROM apache/airflow:3.0.0

USER airflow

# Install Airflow providers using the official constraint file.
# This prevents pip from picking provider versions that conflict with
# Airflow 3.0's pre-installed packages (pendulum, sqlalchemy, click, etc.).
RUN PYTHON_VER=$(python --version | cut -d' ' -f2 | cut -d'.' -f1,2) && \
    pip install --no-cache-dir \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-${PYTHON_VER}.txt" \
        apache-airflow-providers-fab \
        apache-airflow-providers-snowflake \
        apache-airflow-providers-amazon \
        boto3

# Install dbt-snowflake in an isolated virtual environment.
# dbt-core has its own dependency tree (agate, jinja2 pins, etc.) that
# conflicts with Airflow's packages when installed in the same environment.
RUN python -m venv /home/airflow/dbt-venv && \
    /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-snowflake

# DAGs call dbt via this path: /home/airflow/dbt-venv/bin/dbt
ENV DBT_VENV=/home/airflow/dbt-venv
