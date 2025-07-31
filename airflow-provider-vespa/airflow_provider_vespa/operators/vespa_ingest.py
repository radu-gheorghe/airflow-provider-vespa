from __future__ import annotations
from typing import Iterable
from airflow.models import BaseOperator
from airflow_provider_vespa.triggers.vespa_feed_trigger import VespaFeedTrigger
from airflow.exceptions import AirflowException

class VespaIngestOperator(BaseOperator):
    template_fields = ("docs",)

    def __init__(self, *, docs: Iterable[dict], vespa_conn_id: str = "vespa_default", **kw):
        super().__init__(**kw)
        self.docs, self.vespa_conn_id = docs, vespa_conn_id

    def execute(self, context):
        # non‑deferrable fallback. TODO: do we need this?
        from airflow_provider_vespa.hooks.vespa import VespaHook
        return VespaHook(self.vespa_conn_id).feed_async_iterable(self.docs)

    def execute_defer(self, context):
        self.defer(
            trigger=VespaFeedTrigger(self.docs, self.vespa_conn_id),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        if not event["success"]:
            raise AirflowException(f"Vespa feed failed: {event['error']}")
        return event
