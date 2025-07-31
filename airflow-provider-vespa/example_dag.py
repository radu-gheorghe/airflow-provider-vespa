from datetime import datetime

from airflow.decorators import dag, task
from airflow_provider_vespa.operators.vespa_ingest import VespaIngestOperator

@dag(
    dag_id="vespa_hello_world",
    start_date=datetime(2025, 7, 30),
    schedule="@once",
    catchup=False,
    tags=["vespa", "demo"],
)
def vespa_dynamic():

    # this could read from a DB, S3, etc. and build micro-batches
    @task
    def build_batches():
        return [
            [  # batch‑0 → Task 0
                {"id": "doc1", "fields": {"body": "first document"}},
                {"id": "doc2", "fields": {"body": "second document"}},
            ],
            [  # batch‑1 → Task 1
                {"id": "doc3", "fields": {"body": "third document"}},
                # TODO: this should fail and it doesn't seem to
                {"id": "doc4", "fields": {"doesntexist": "fourth document"}},
            ],
        ]

    batches = build_batches()

    # dynamically create one VespaIngestOperator per micro-batch
    VespaIngestOperator.partial(task_id="send_batch").expand(docs=batches)

dag = vespa_dynamic()
