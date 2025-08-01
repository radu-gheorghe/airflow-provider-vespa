from typing import Callable, Dict, Iterable, List
from vespa.application import Vespa
from vespa.io import VespaResponse
from vespa.exceptions import VespaError
from airflow.hooks.base import BaseHook
import uuid
from queue import Queue

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
        # TODO should the connection be Vespa-specific or can we keep it generic?
        self.conn = self.get_connection(conn_id)
        # TODO handle authentication
        self.vespa_app = Vespa(url=self.conn.host.rstrip("/"))
        extra = self.conn.extra_dejson or {}
        self.namespace = extra["namespace"]
        self.schema = extra["schema"]
        self.max_queue_size = extra.get("max_queue_size")
        self.max_workers = extra.get("max_workers")
        self.max_connections = extra.get("max_connections")
        self.feed_errors_queue = Queue()

    @classmethod
    def from_resolved_connection(cls, *, host: str, extra: Dict):
        """Instantiate a ``VespaHook`` without querying Airflow's metadata database.

        This helper is intended for use inside Trigger processes, where
        synchronous DB access (e.g. ``BaseHook.get_connection``) is prohibited.
        The caller must supply the already-resolved host and the ``extra``
        mapping that would normally come from an Airflow ``Connection``.
        """
        import types

        self = cls.__new__(cls)
        BaseHook.__init__(self)

        # Fake a minimal ``Connection``-like object for backwards compatibility.
        self.conn = types.SimpleNamespace(host=host, extra=extra)
        self.vespa_app = Vespa(url=host.rstrip("/"))
        # Extract parameters exactly as in the normal constructor.
        self.namespace = extra["namespace"]
        self.schema = extra["schema"]
        self.max_queue_size = extra.get("max_queue_size")
        self.max_workers = extra.get("max_workers")
        self.max_connections = extra.get("max_connections")
        self.feed_errors_queue = Queue()
        return self

    def _normalise(self,
        bodies: Iterable[Dict], *, gen_missing_id: bool = True
    ) -> List[Dict]:
        norm: List[Dict] = []
        for b in bodies:
            if "fields" in b:                 # already in Vespa format
                norm.append(b)
                continue
            # treat as raw "fields" dict
            doc_id = b.pop("id", None) or (str(uuid.uuid4()) if gen_missing_id else None)
            norm.append({"id": doc_id, "fields": b})
        return norm
    
    def default_callback(self, response: VespaResponse, id: str):
        if not response.is_successful():
            self.feed_errors_queue.put({"id": id, "status": response.status_code, "reason": response.get_json()})

    def feed_async_iterable(self, bodies: Iterable[dict], callback: Callable = None):
        if callback is None:
            callback = self.default_callback
        
        docs = self._normalise(bodies)

        # Build kwargs with mandatory and optional arguments
        feed_kwargs = {
            # mandatory arguments
            "iter": docs,
            # TODO support update and delete
            "schema": self.schema,
            # TODO namespace and schema should be properties of the hook, not the connection?
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
        self.feed_errors_queue = Queue()
        
        # feed documents
        self.vespa_app.feed_async_iterable(**feed_kwargs)
        
        # Collect all errors from the queue
        feed_errors = []
        while not self.feed_errors_queue.empty():
            try:
                error = self.feed_errors_queue.get_nowait()
                feed_errors.append(error)
            except:
                break
        
        return {
            "sent": len(docs),
            "errors": len(feed_errors),
            "error_details": feed_errors,
        }
