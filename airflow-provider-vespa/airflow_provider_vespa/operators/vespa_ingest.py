from __future__ import annotations
from typing import Iterable
from airflow.models import BaseOperator
from airflow_provider_vespa.triggers.vespa_feed_trigger import VespaFeedTrigger
from airflow.exceptions import AirflowException

class VespaIngestOperator(BaseOperator):
    template_fields = ("docs",)

    def __init__(self, *, docs: Iterable[dict], vespa_conn_id: str = "vespa_default", operation_type: str = "feed", **kw):
        super().__init__(**kw)
        self.docs = docs
        self.vespa_conn_id = vespa_conn_id
        self.operation_type = operation_type

    def execute(self, context):
        """Resolve the Vespa Airflow connection in the worker and defer the actual
        ingestion work to a trigger. Only plain, JSON-serialisable data is sent
        to the trigger so it can run without database access.
        """
        from airflow.hooks.base import BaseHook
        from airflow.exceptions import TaskDeferred
        from airflow.models import Variable

        conn = BaseHook.get_connection(self.vespa_conn_id)
        extra = conn.extra_dejson or {}
        
        # Resolve certificates in the worker context where sync DB calls are allowed
        cert_content = key_content = None
        if cert_secret := extra.get("extra__vespa__client_cert_secret"):
            try:
                cert_content = Variable.get(cert_secret)
            except KeyError:
                self.log.warning(f"Client certificate secret '{cert_secret}' not found")
        
        if key_secret := extra.get("extra__vespa__client_key_secret"):
            try:
                key_content = Variable.get(key_secret)
            except KeyError:
                self.log.warning(f"Client key secret '{key_secret}' not found")

        conn_info = {
            "host": conn.host,
            "schema": conn.schema,
            "namespace": extra.get("extra__vespa__namespace") or "default",
            "extra": extra,
            # Pass resolved certificate content directly
            "cert_content": cert_content,
            "key_content": key_content,
        }

        raise TaskDeferred(
            trigger=VespaFeedTrigger(self.docs, conn_info, self.operation_type),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        if not event["success"]:
            raise AirflowException(
                f"{len(event['errors'])} document(s) failed out of {len(self.docs)}; "
                f"Error details: {event['errors']}"
            )
        return {"ingested": len(self.docs)}
