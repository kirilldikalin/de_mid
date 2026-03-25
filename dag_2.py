from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


def parse_comments(**context):
    comments = context["ti"].xcom_pull(task_ids="fetch_comments")
    if len(comments) > 0:
        return "load_comments"
    return "skip_comments"


def load_comments():
    print("load comments to clickhouse")


def notify():
    print("send webhook")


with DAG(
    dag_id="jira_comments_sync",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    wait_file = S3KeySensor(
        task_id="wait_file",
        bucket_name="raw-jira",
        bucket_key="comments/{{ ds }}/comments.json",
        poke_interval=30,
        timeout=60 * 60 * 3,
        mode="poke",
    )

    fetch_comments = SimpleHttpOperator(
        task_id="fetch_comments",
        http_conn_id="jira_api",
        endpoint="/rest/api/2/search?jql=project=DEPCONUX",
        method="GET",
        response_filter=lambda response: response.json()["issues"],
        log_response=True,
    )

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=parse_comments,
        provide_context=True,
    )

    load_comments_task = PythonOperator(
        task_id="load_comments",
        python_callable=load_comments,
    )

    skip_comments = EmptyOperator(
        task_id="skip_comments",
    )

    notify_task = PythonOperator(
        task_id="notify_webhook",
        python_callable=notify,
    )

    wait_file >> fetch_comments >> branch >> [load_comments_task, skip_comments] >> notify_task