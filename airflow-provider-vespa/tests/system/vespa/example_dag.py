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

    @task(task_id="verify_docs_with_hook")
    def verify_docs_with_hook():
        """
        Use VespaHook (not the operator) to query and validate that:
          - doc1 and doc2 exist
          - doc3 is updated
          - doc4 is deleted
        """
        # Import inside the task to avoid import-time side effects in the scheduler
        from airflow_provider_vespa.hooks.vespa import VespaHook

        # Build the hook from the Airflow connection ID
        hook = VespaHook(conn_id=vespa_conn_id)

        # Helper to run a query and return hit count and first hit fields
        def run_query(yql: str, params: dict | None = None):
            params = params or {}
            res = hook.vespa_app.query(yql=yql, **params)
            hits = res.hits if hasattr(res, "hits") else []
            return {
                "count": len(hits),
                "first": hits[0].get("fields") if hits else None,
            }

        # Check for doc1 and doc2 by their body content
        q1 = run_query('select * from sources * where body contains "first document"')
        q2 = run_query('select * from sources * where body contains "second document"')

        # Check that doc3 has the updated body
        q3_updated = run_query('select * from sources * where body contains "UPDATED"')

        # Ensure doc4 content is gone
        q4_deleted = run_query('select * from sources * where body contains "fourth document"')

        # Basic assertions (raise if expectations are not met)
        assert q1["count"] >= 1, "doc1 not found"
        assert q2["count"] >= 1, "doc2 not found"
        assert q3_updated["count"] >= 1, "updated doc3 not found"
        assert q4_deleted["count"] == 0, "doc4 appears to still be present"

        return {
            "doc1_found": q1["count"],
            "doc2_found": q2["count"],
            "doc3_updated_found": q3_updated["count"],
            "doc4_found": q4_deleted["count"],
        }

    verify_docs_task = verify_docs_with_hook()
    send_batches >> [update_doc3, delete_doc4] >> verify_docs_task

dag = vespa_dynamic()
