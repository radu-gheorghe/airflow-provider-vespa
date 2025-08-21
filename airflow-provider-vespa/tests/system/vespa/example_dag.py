from datetime import datetime
import os
from airflow.decorators import dag, task
from airflow_provider_vespa.operators.vespa_ingest import VespaIngestOperator

# avoid segfaults on macos
os.environ['NO_PROXY'] = '*'

@dag(
    dag_id="vespa_hello_world",
    start_date=datetime(2025, 7, 30),
    schedule="@once",
    catchup=False,
    tags=["vespa", "demo"],
)
def vespa_dynamic():

    vespa_conn_id = "vespa_connection"

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
                {"id": "doc4", "fields": {"body": "fourth document"}},
            ],
        ]

    batches = build_batches()

    # dynamically create one VespaIngestOperator per micro-batch
    send_batches = VespaIngestOperator.partial(vespa_conn_id=vespa_conn_id, task_id="send_batch").expand(docs=batches)

    # update third doc and remove fourth after initial batches are ingested
    update_doc3 = VespaIngestOperator(
        vespa_conn_id=vespa_conn_id,
        task_id="update_doc3",
        docs=[{"id": "doc3", "fields": {"body": "third document – UPDATED"}}],
        operation_type="update",
    )

    delete_doc4 = VespaIngestOperator(
        vespa_conn_id=vespa_conn_id,
        task_id="delete_doc4",
        docs=[{"id": "doc4"}],
        operation_type="delete",
    )

    send_batches >> [update_doc3, delete_doc4]

dag = vespa_dynamic()
