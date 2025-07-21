from datetime import datetime
import uuid
from airflow import DAG
from airflow_provider_vespa.operators.vespa_ingest import VespaIngestOperator

def generate_sample_documents():
    """Generator function that yields documents with explicit IDs"""
    for i in range(5):
        yield {
            "id": f"doc_{i}_{uuid.uuid4().hex[:8]}",
            "fields": {
                "body": f"This is the content of document {i}"
            }
        }

with DAG(
    dag_id="vespa_hello_world",
    start_date=datetime(2025, 7, 17),
    schedule="@once",
    catchup=False,
    tags=["vespa", "demo"],
) as dag:

    # Generator function
    VespaIngestOperator(
        task_id="send_generated_docs",
        document_generator=generate_sample_documents,
    )

    # List of documents
    VespaIngestOperator(
        task_id="send_single_direct",
        document_generator=[{"id": "single_doc", "fields": {"body": "single document"}}],
    )
