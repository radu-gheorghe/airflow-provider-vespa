from typing import Callable, Iterable
from vespa.application import Vespa
from vespa.io import VespaResponse
from vespa.exceptions import VespaError
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
        super().__init__()
        self.conn = self.get_connection(conn_id)
        # TODO handle authentication
        self.vespa_app = Vespa(url=self.conn.host.rstrip("/"))
        extra = self.conn.extra_dejson or {}
        self.namespace = extra["namespace"]
        self.schema = extra["schema"]
        self.max_queue_size = extra.get("max_queue_size")
        self.max_workers = extra.get("max_workers")
        self.max_connections = extra.get("max_connections")
        self.feed_errors = [] # collect errors from callback
    
    def default_callback(self, response: VespaResponse, id: str):
        if not response.is_successful():
            error_msg = f"ID: {id} Status: {response.status_code} Reason: {response.get_json()}"
            self.feed_errors.append(error_msg)

    def feed_async_iterable(self, bodies: Iterable[dict], callback: Callable = None):
        if callback is None:
            callback = self.default_callback

        # Build kwargs with mandatory and optional arguments
        feed_kwargs = {
            # mandatory arguments
            "iter": bodies,
            # TODO support update and delete
            "schema": self.schema,
            "namespace": self.namespace,
            "callback": callback,

            # optional arguments
            **{k: v for k, v in {
                "max_queue_size": self.max_queue_size,
                "max_workers": self.max_workers,
                "max_connections": self.max_connections
            }.items() if v is not None}
        }
            
        # Clear any previous errors
        self.feed_errors = []
        
        # feed documents
        self.vespa_app.feed_async_iterable(**feed_kwargs)
        
        # Check for any errors that occurred during feeding
        if self.feed_errors:
            raise VespaError(f"At least one feed operation failed: {'; '.join(self.feed_errors)}")
