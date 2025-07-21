import json, uuid, requests, vespa
from airflow.hooks.base import BaseHook

class VespaHook(BaseHook):
    """
    Very small hook: posts a single JSON document to Vespa via HTTP POST.
    Airflow Connection (type=http) should hold:
        host      -> https://my-vespa.xyz:8080
        extra     -> {"namespace": "my_ns", "schema": "my_doc"}
    """

    default_conn_name = "vespa_default"

    def __init__(self, conn_id: str = default_conn_name):
        # TODO use pyvespa
        super().__init__()
        self.conn = self.get_connection(conn_id)
        self.base_url = self.conn.host.rstrip("/")
        extra = self.conn.extra_dejson or {}
        self.namespace = extra["namespace"]
        self.schema = extra["schema"]

    def put_document(self, body: dict, doc_id: str | None = None):
        doc_id = doc_id or str(uuid.uuid4())
        url = f"{self.base_url}/document/v1/{self.namespace}/{self.schema}/docid/{doc_id}"
        resp = requests.post(url, data=json.dumps(body), headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.json()
