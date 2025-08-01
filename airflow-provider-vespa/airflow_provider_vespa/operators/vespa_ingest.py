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
        """Resolve the Vespa Airflow connection in the worker and defer the actual
        ingestion work to a trigger. Only plain, JSON-serialisable data is sent
        to the trigger so it can run without database access.
        """
        from airflow.hooks.base import BaseHook
        from airflow.exceptions import TaskDeferred

        conn = BaseHook.get_connection(self.vespa_conn_id)
        conn_info = {
            "host": conn.host,
            "extra": conn.extra_dejson or {},
        }

        raise TaskDeferred(
            trigger=VespaFeedTrigger(self.docs, conn_info),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        if not event["success"]:
            raise AirflowException(
                f"{len(event['errors'])} document(s) failed out of {len(self.docs)}; "
                f"Error details: {event['errors']}"
            )
        return {"ingested": len(self.docs)}
