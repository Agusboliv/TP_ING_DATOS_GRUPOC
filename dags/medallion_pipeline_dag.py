from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import dag, task, Asset

HEADLINES_ASSET = Asset("file:///opt/airflow/data")

@dag(
    dag_id="medallion_pipeline_dag",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["TP_ING"],
)
def medallion_pipeline_dag():
    @task(retries=2, retry_delay=timedelta(seconds=15))
    def fetch_headlines() -> list[dict]:
        return list[dict]

medallion_pipeline_dag()
