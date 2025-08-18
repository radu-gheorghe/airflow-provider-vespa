from typing import Callable, Dict, Iterable, List
from vespa.application import Vespa
from vespa.io import VespaResponse
from airflow.hooks.base import BaseHook

class VespaHook(BaseHook):
    """
    Very small hook: posts a single JSON document to Vespa via HTTP POST.
    Airflow Connection (type=http) should hold:
        host      -> https://my-vespa.xyz:8080
        extra     -> {"namespace": "my_ns", "schema": "my_doc"}
    """

    # Hook / connection metadata for Airflow UI
    conn_name_attr = "vespa_conn_id"
    default_conn_name = "vespa_default"
    conn_type = "vespa"
    hook_name = "Vespa"

    def __init__(self, conn_id: str = default_conn_name, *, namespace: str | None = None, schema: str | None = None):
        """Create a VespaHook.

        Parameters ``namespace`` and ``schema`` override the values stored in the
        Airflow ``Connection``.  This is convenient when you want to share the
        same endpoint across multiple DAGs but write to different schemas or
        namespaces without creating separate connections.
        """
        super().__init__()
        self.conn = self.get_connection(conn_id)
        extra = self.conn.extra_dejson or {}
        
        self._configure_from_connection(
            host=self.conn.host,
            namespace=namespace or extra.get("namespace", "default"),
            schema=schema or self.conn.schema,
            extra=extra,
        )

    @classmethod
    # --- Airflow connection UI helpers -------------------------------------------------

    @classmethod
    def get_connection_form_widgets(cls):

        try:
            from wtforms import StringField  # type: ignore
        except ImportError:  # type: ignore
            # Airflow's UI imports wtforms; during pure Python environments it may be absent.
            StringField = lambda *args, **kwargs: None  # type: ignore
            
        return {
            "extra__vespa__namespace": StringField("Namespace"),
            "extra__vespa__max_queue_size": StringField("Max feed queue size"),
            "extra__vespa__max_workers": StringField("Max feed workers"),
            "extra__vespa__max_connections": StringField("Max feed connections"),
            "extra__vespa__vespa_cloud_secret_token": StringField("Vespa Cloud secret token"),
            "extra__vespa__client_cert_path": StringField("Client certificate file path"),
            "extra__vespa__client_key_path": StringField("Client key file path"),
        }

    @classmethod
    def get_ui_field_behaviour(cls):
        return {
            "hidden_fields": [],
            "relabeling": {
                "host": "Endpoint",
            },
            "placeholders": {
                "extra__vespa__vespa_cloud_secret_token": "",
                "extra__vespa__namespace": "my_namespace",
                "extra__vespa__client_cert_path": "/path/to/client.pem",
                "extra__vespa__client_key_path": "/path/to/client.key",
            },
        }

    # -------------------------------------------------------------------------------

    def _configure_from_connection(self, *, host: str, namespace: str, schema: str, extra: Dict):
        """Populate instance attributes shared by both constructors."""
        
        # Get certificate file paths directly from connection config
        # TODO: In the future, pyvespa should accept certificate content as strings
        # so we can store certificates as encrypted Airflow Variables instead of files
        cert_file = extra.get("extra__vespa__client_cert_path")
        key_file = extra.get("extra__vespa__client_key_path")

        # Get token as well, in case we need it
        token = extra.get("extra__vespa__vespa_cloud_secret_token")
        
        # If no certificate files are provided, we can use token authentication
        if not cert_file or not key_file:
            self.log.info("No client certificate or key found, trying token authentication")
            # let's set both to None (in case one isn't), so we can use token authentication
            cert_file = None
            key_file = None

            if not token:
                self.log.info("No token found, either. Not authenticating.")
            else:
                self.log.info("Token authentication available")
        else:
            self.log.info("Using mTLS authentication with certificate files")
        
        
        self.vespa_app = Vespa(
            # TODO: param validation
            url=host.rstrip("/"), 
            vespa_cloud_secret_token=token,
            cert=cert_file,
            key=key_file,
        )
        
        self.namespace = namespace
        self.schema = schema
        self.max_queue_size = extra.get("max_queue_size")
        self.max_workers = extra.get("max_workers")
        self.max_connections = extra.get("max_connections")
        self.feed_errors_queue = Queue()

    @classmethod
    def from_resolved_connection(cls, *, host: str, schema: str | None = None, namespace: str | None = None, extra: Dict):
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
        
        self._configure_from_connection(
            host=host,
            namespace=namespace or extra.get("namespace", "default"),
            schema=schema or extra.get("schema"),
            extra=extra,
        )
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
            # Add debugging for the response content
            try:
                reason = response.get_json()
            except Exception as e:
                # If JSON parsing fails, capture the raw response
                reason = {
                    "json_parse_error": str(e),
                    "raw_response": getattr(response, 'text', 'No text attribute'),
                    "response_type": type(response).__name__
                }
            
            self.feed_errors_queue.put({"id": id, "status": response.status_code, "reason": reason})

    def feed_iterable(self, bodies: Iterable[dict], callback: Callable = None, operation_type: str = "feed"):
        
        if callback is None:
            callback = self.default_callback
        
        docs = self._normalise(bodies)

        # Build kwargs with mandatory and optional arguments
        feed_kwargs = {
            # mandatory arguments
            "iter": docs,
            "operation_type": operation_type,
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
        self.feed_errors_queue = Queue()
        
        self.log.info(f"Starting {operation_type} operation for {len(docs)} documents")
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
