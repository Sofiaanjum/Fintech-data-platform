from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "sofia",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt_project"
VENV = "/home/airflow/.local/bin"

with DAG(
    dag_id="fintech_pipeline",
    description="Kafka ingest + Snowflake load + dbt transform",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fintech", "kafka", "dbt", "snowflake"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command=f"cd {PROJECT_DIR} && python3 ingestion/producers/transaction_producer.py",
    )

    run_consumer = BashOperator(
        task_id="run_kafka_consumer",
        bash_command=f"cd {PROJECT_DIR} && timeout 120 python3 ingestion/consumers/transaction_consumer.py || true",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {VENV}/dbt run --profiles-dir {DBT_DIR} --no-partial-parse",

    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {VENV}/dbt test --profiles-dir {DBT_DIR} --select source:fintech_raw --no-partial-parse",
    )

    end = EmptyOperator(task_id="end")

    start >> run_producer >> run_consumer >> dbt_run >> dbt_test >> end