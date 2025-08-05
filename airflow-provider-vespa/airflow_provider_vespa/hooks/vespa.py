from typing import Callable, Dict, Iterable, List
from vespa.application import Vespa
from vespa.io import VespaResponse
from airflow.hooks.base import BaseHook
import uuid
from queue import Queue

try:
    from wtforms import StringField  # type: ignore
except ImportError:  # type: ignore
    # Airflow's UI imports wtforms; during pure Python environments it may be absent.
    StringField = lambda *args, **kwargs: None  # type: ignore

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
        self._configure(
            host=self.conn.host,
            namespace=namespace or extra.get("namespace", "default"),
            schema=schema or self.conn.schema,
            extra=extra,
        )

    @classmethod
    # --- Airflow connection UI helpers -------------------------------------------------

    @classmethod
    def get_connection_form_widgets(cls):
        return {
            "extra__vespa__namespace": StringField("Namespace"),
            "extra__vespa__max_queue_size": StringField("Max feed queue size"),
            "extra__vespa__max_workers": StringField("Max feed workers"),
            "extra__vespa__max_connections": StringField("Max feed connections"),
            "extra__vespa__vespa_cloud_secret_token": StringField("Vespa Cloud secret token"),
            "extra__vespa__client_cert_secret": StringField("Client certificate secret name"),
            "extra__vespa__client_key_secret": StringField("Client key secret name"),
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
                "extra__vespa__client_cert_secret": "vespa_client_cert",
                "extra__vespa__client_key_secret": "vespa_client_key",
            },
        }

    def _write_temp_cert(self, cert_content: str, cert_type: str) -> str:
        """Write certificate content to a temporary file and return the path."""
        import tempfile
        import os
        
        # Create a temporary file that will be cleaned up when the process exits
        fd, temp_path = tempfile.mkstemp(suffix=f".{cert_type}", prefix="vespa_")
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(cert_content)
            # Set restrictive permissions for security
            os.chmod(temp_path, 0o600)
            return temp_path
        except:
            # Clean up on error
            os.unlink(temp_path)
            raise

    # -------------------------------------------------------------------------------

    def _configure(self, *, host: str, namespace: str, schema: str, extra: Dict, cert_content: str = None, key_content: str = None):
        """Populate instance attributes shared by both constructors."""
        
        # Handle mTLS certificates
        cert_file = key_file = None
        
        if cert_content:
            cert_file = self._write_temp_cert(cert_content, "cert")
        
        if key_content:
            key_file = self._write_temp_cert(key_content, "key")
        
        if not cert_file or not key_file:
            self.log.warning("No client certificate or key found, trying token authentication")
        
        token = extra.get("extra__vespa__vespa_cloud_secret_token")
        if not token:
            self.log.warning("No token found, either. We're not authenticating.")
        
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
    def from_resolved_connection(cls, *, host: str, schema: str | None = None, namespace: str | None = None, extra: Dict, cert_content: str = None, key_content: str = None):
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
        self._configure(
            host=host,
            namespace=namespace or extra.get("namespace", "default"),
            schema=schema or extra.get("schema"),
            extra=extra,
            cert_content=cert_content,
            key_content=key_content,
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
