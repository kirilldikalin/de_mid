from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

JIRA_URL = Variable.get("jira_url")
JIRA_TOKEN = Variable.get("jira_token")

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="jira_export_hourly",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=datetime.now(),
    catchup=True,
    max_active_runs=4,
    tags=["jira", "export"],
) as dag:

    @task
    def extract_issues():
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {JIRA_TOKEN}"
        })

        start_at = 0
        all_issues = []

        while True:
            resp = session.get(
                f"{JIRA_URL}/rest/api/2/search",
                params={
                    "jql": "project = DEPCONUX",
                    "startAt": start_at,
                    "maxResults": 1000,
                }
            )
            data = resp.json()
            all_issues.extend(data["issues"])

            if start_at + 1000 >= data["total"]:
                break

            start_at += 1000

        return all_issues

    @task
    def build_csv(issues):
        rows = []
        for issue in issues:
            fields = issue["fields"]
            rows.append({
                "key": issue["key"],
                "reporter": fields["reporter"]["displayName"],
                "assignee": fields["assignee"]["displayName"],
                "status": fields["status"]["name"],
                "summary": fields["summary"],
                "description": fields["description"],
            })
        return rows

    @task
    def upload_to_s3(rows):
        import boto3
        import csv
        from io import StringIO

        s3 = boto3.client("s3")
        buffer = StringIO()
        writer = csv.DictWriter(
            buffer,
            fieldnames=["key", "reporter", "assignee", "status", "summary", "description"]
        )
        writer.writeheader()
        writer.writerows(rows)

        s3.put_object(
            Bucket="jira-exports",
            Key="latest/jira_export.csv",
            Body=buffer.getvalue().encode("utf-8")
        )

    upload_to_s3(build_csv(extract_issues()))