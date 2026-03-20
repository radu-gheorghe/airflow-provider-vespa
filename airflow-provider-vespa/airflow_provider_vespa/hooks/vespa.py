from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable
from queue import Empty, Queue
from typing import Any

from vespa.application import Vespa
from vespa.io import VespaResponse
from airflow.hooks.base import BaseHook

try:
    from airflow.sdk import Connection
except ImportError:
    # Fallback for older Airflow versions
    from airflow.models import Connection

VALID_OPERATION_TYPES = frozenset({"feed", "update", "delete"})


class VespaHook(BaseHook):
    """Hook for interacting with a Vespa cluster via pyvespa.

    Supports document feed, update, and delete operations through
    ``feed_async_iterable``.  Uses a custom ``vespa`` connection type.

    Connection extras (bare keys canonical, ``extra__vespa__`` prefix
    accepted for backward compatibility):

    - ``namespace`` -- Vespa namespace (default: ``"default"``)
    - ``max_queue_size`` / ``max_workers`` / ``max_connections`` -- feed tuning
    - ``vespa_cloud_secret_token`` -- token auth
    - ``client_cert_path`` / ``client_key_path`` -- mTLS auth
    - ``protocol`` -- ``http`` or ``https`` (default: ``http``)
    """

    conn_name_attr = "vespa_conn_id"
    default_conn_name = "vespa_default"
    conn_type = "vespa"
    hook_name = "Vespa"

    # --- field helpers --------------------------------------------------------

    @staticmethod
    def _get_field(extra: dict, field: str) -> Any:
        """Return an extra-field value, preferring bare key over legacy prefix."""
        if field in extra:
            return extra[field]
        return extra.get(f"extra__vespa__{field}")

    @staticmethod
    def _get_int_field(extra: dict, field: str) -> int | None:
        """Return an extra field cast to ``int``, or *None* if absent/empty."""
        val = VespaHook._get_field(extra, field)
        if val is None or val == "":
            return None
        try:
            return int(val)
        except (TypeError, ValueError):
            raise ValueError(
                f"Connection extra '{field}' must be an integer, got {val!r}"
            )

    # --- constructors ---------------------------------------------------------

    def __init__(
        self,
        conn_id: str = default_conn_name,
        *,
        namespace: str | None = None,
        schema: str | None = None,
    ):
        """Create a VespaHook.

        Parameters ``namespace`` and ``schema`` override values stored in the
        Airflow ``Connection``.  This is convenient when you want to share the
        same endpoint across multiple DAGs but write to different schemas or
        namespaces without creating separate connections.
        """
        super().__init__()
        self.conn = self.get_connection(conn_id)
        extra = self.conn.extra_dejson or {}

        resolved_schema = (
            schema
            or self.conn.schema
            or self._get_field(extra, "schema")
        )
        resolved_namespace = (
            namespace
            or self._get_field(extra, "namespace")
            or "default"
        )

        self._configure_from_connection(
            host=self.conn.host,
            port=self.conn.port,
            namespace=resolved_namespace,
            schema=resolved_schema,
            extra=extra,
        )

    @classmethod
    def from_resolved_connection(
        cls,
        *,
        host: str,
        port: int | None = None,
        schema: str | None = None,
        namespace: str | None = None,
        extra: dict,
    ) -> VespaHook:
        """Instantiate without querying Airflow's metadata database.

        Intended for Trigger processes where synchronous DB access is
        prohibited.  The caller must supply the already-resolved connection
        parameters.
        """
        import types

        self = cls.__new__(cls)
        BaseHook.__init__(self)

        self.conn = types.SimpleNamespace(host=host, extra=extra)

        resolved_schema = schema or cls._get_field(extra, "schema")
        resolved_namespace = (
            namespace or cls._get_field(extra, "namespace") or "default"
        )

        self._configure_from_connection(
            host=host,
            port=port,
            namespace=resolved_namespace,
            schema=resolved_schema,
            extra=extra,
        )
        return self

    # --- Airflow connection UI helpers ----------------------------------------

    @classmethod
    def get_connection_form_widgets(cls) -> dict:
        try:
            from wtforms import StringField
        except ImportError:
            StringField = lambda *args, **kwargs: None  # type: ignore

        return {
            "namespace": StringField("Namespace"),
            "max_queue_size": StringField("Max feed queue size"),
            "max_workers": StringField("Max feed workers"),
            "max_connections": StringField("Max feed connections"),
            "vespa_cloud_secret_token": StringField("Vespa Cloud secret token"),
            "client_cert_path": StringField("Client certificate file path"),
            "client_key_path": StringField("Client key file path"),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict:
        return {
            "hidden_fields": [],
            "relabeling": {
                "host": "Endpoint",
                "schema": "Document Type",
            },
            "placeholders": {
                "vespa_cloud_secret_token": "",
                "namespace": "my_namespace",
                "client_cert_path": "/path/to/client.pem",
                "client_key_path": "/path/to/client.key",
            },
        }

    # --- connection setup -----------------------------------------------------

    def _configure_from_connection(
        self,
        *,
        host: str,
        port: int | None = None,
        namespace: str,
        schema: str | None,
        extra: dict,
    ) -> None:
        """Populate instance attributes shared by both constructors."""
        # TODO: pyvespa should accept certificate content as strings so we
        # can store certificates as encrypted Airflow Variables instead of files
        cert_file = self._get_field(extra, "client_cert_path")
        key_file = self._get_field(extra, "client_key_path")
        token = self._get_field(extra, "vespa_cloud_secret_token")

        if not cert_file or not key_file:
            self.log.info("No client certificate or key found, trying token authentication")
            cert_file = None
            key_file = None

            if not token:
                self.log.info("No token found either. Not authenticating.")
            else:
                self.log.info("Token authentication available")
        else:
            self.log.info("Using mTLS authentication with certificate files")

        url = host.rstrip("/")
        protocol = self._get_field(extra, "protocol") or "http"

        if host.startswith("http://") or host.startswith("https://"):
            url = host
        else:
            url = f"{protocol}://{host}"

        # append the port if it's provided and not already present
        if port and not url.endswith(f":{port}"):
            url = f"{url}:{port}"

        self.log.info("Connecting to Vespa at %s", url)

        self.vespa_app = Vespa(
            # TODO: param validation
            url=url, 
            vespa_cloud_secret_token=token,
            cert=cert_file,
            key=key_file,
        )

        self.namespace = namespace
        self.schema = schema
        self.max_queue_size = self._get_int_field(extra, "max_queue_size")
        self.max_workers = self._get_int_field(extra, "max_workers")
        self.max_connections = self._get_int_field(extra, "max_connections")
        self.feed_errors_queue: Queue = Queue()

    # --- document handling ----------------------------------------------------

    def _normalise(
        self,
        bodies: Iterable[dict],
        *,
        operation_type: str = "feed",
    ) -> list[dict]:
        """Convert documents to Vespa ``{id, fields}`` format.

        For ``feed``, missing IDs are auto-generated as UUIDs.
        For ``update`` and ``delete``, explicit IDs are required.
        """
        norm: list[dict] = []
        for i, body in enumerate(bodies):
            if "fields" in body:
                if operation_type in ("update", "delete") and "id" not in body:
                    raise ValueError(
                        f"Document at index {i} is missing required 'id' "
                        f"for {operation_type} operation"
                    )
                norm.append(body)
                continue

            b = dict(body)
            doc_id = b.pop("id", None)

            if doc_id is None and operation_type in ("update", "delete"):
                raise ValueError(
                    f"Document at index {i} is missing required 'id' "
                    f"for {operation_type} operation"
                )
            if doc_id is None:
                doc_id = str(uuid.uuid4())

            norm.append({"id": doc_id, "fields": b})
        return norm

    def default_callback(self, response: VespaResponse, id: str) -> None:
        if not response.is_successful():
            # Add debugging for the response content
            try:
                reason = response.get_json()
            except Exception as e:
                # If JSON parsing fails, capture the raw response
                reason = {
                    "json_parse_error": str(e),
                    "status_code": response.status_code,
                    "response_type": type(response).__name__,
                }

            self.feed_errors_queue.put(
                {"id": id, "status": response.status_code, "reason": reason}
            )

    def feed_iterable(
        self,
        bodies: Iterable[dict],
        callback: Callable | None = None,
        operation_type: str = "feed",
        **kwargs: Any,
    ) -> dict:
        """Feed, update, or delete documents via pyvespa.

        Extra ``**kwargs`` are forwarded to ``feed_async_iterable`` and
        ultimately to the per-document operation (e.g. ``auto_assign``
        and ``create`` for updates).
        """
        if operation_type not in VALID_OPERATION_TYPES:
            raise ValueError(
                f"Invalid operation_type {operation_type!r}. "
                f"Must be one of {sorted(VALID_OPERATION_TYPES)}"
            )

        if callback is None:
            callback = self.default_callback

        docs = self._normalise(bodies, operation_type=operation_type)

        feed_kwargs: dict[str, Any] = {
            "iter": docs,
            "operation_type": operation_type,
            "schema": self.schema,
            "namespace": self.namespace,
            "callback": callback,
        }

        for key in ("max_queue_size", "max_workers", "max_connections"):
            val = getattr(self, key, None)
            if val is not None:
                feed_kwargs[key] = val

        feed_kwargs.update(kwargs)

        self.feed_errors_queue = Queue()
        
        self.log.info(f"Starting {operation_type} operation for {len(docs)} documents")
        self.vespa_app.feed_async_iterable(**feed_kwargs)

        feed_errors: list[dict] = []
        while not self.feed_errors_queue.empty():
            try:
                feed_errors.append(self.feed_errors_queue.get_nowait())
            except Empty:
                break

        return {
            "sent": len(docs),
            "errors": len(feed_errors),
            "error_details": feed_errors,
        }

    # --- connection test ------------------------------------------------------

    def test_connection(self) -> tuple[bool, str]:
        """Test connectivity to the Vespa endpoint using pyvespa's own
        auth-configured session (mTLS / token)."""
        try:
            resp = self.vespa_app.get_application_status()
            if resp is not None and resp.status_code == 200:
                return True, "Connection successful"
            status = getattr(resp, "status_code", "unknown") if resp else "no response"
            return False, f"Vespa returned HTTP {status}"
        except Exception as e:
            return False, str(e)
