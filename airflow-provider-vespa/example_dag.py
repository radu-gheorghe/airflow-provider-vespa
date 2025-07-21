from datetime import datetime
from airflow import DAG
from airflow_provider_vespa.operators.vespa_ingest import VespaIngestOperator

with DAG(
    dag_id="vespa_hello_world",
    start_date=datetime(2025, 7, 17),
    schedule="@once",
    catchup=False,
    tags=["vespa", "demo"],
) as dag:

    VespaIngestOperator(
        task_id="send_hello",
        # TODO: the "fields" part should be added automatically (pyvespa?)
        body={"fields": {"body": "hello world"}},  # will be JSON-encoded by the hook
    )
