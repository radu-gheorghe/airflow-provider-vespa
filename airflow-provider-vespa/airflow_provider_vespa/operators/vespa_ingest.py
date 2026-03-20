from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow_provider_vespa.hooks.vespa import VALID_OPERATION_TYPES, VespaHook
from airflow_provider_vespa.triggers.vespa_feed_trigger import VespaFeedTrigger


class VespaIngestOperator(BaseOperator):
    template_fields = ("docs",)

    def __init__(
        self,
        *,
        docs: Iterable[dict],
        vespa_conn_id: str = "vespa_default",
        operation_type: str = "feed",
        feed_kwargs: dict[str, Any] | None = None,
        **kw,
    ):
        super().__init__(**kw)
        self.docs = docs
        self.vespa_conn_id = vespa_conn_id
        if operation_type not in VALID_OPERATION_TYPES:
            raise ValueError(
                f"Invalid operation_type {operation_type!r}. "
                f"Must be one of {sorted(VALID_OPERATION_TYPES)}"
            )
        self.operation_type = operation_type
        self.feed_kwargs = feed_kwargs or {}

    def execute(self, context):
        """Resolve the Vespa connection in the worker and defer ingestion to
        a trigger so it does not block a worker slot.
        """
        from airflow.exceptions import TaskDeferred
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(self.vespa_conn_id)
        extra = conn.extra_dejson or {}

        self.docs = list(self.docs) if not isinstance(self.docs, list) else self.docs

        for i, doc in enumerate(self.docs):
            if not isinstance(doc, dict):
                raise AirflowException(
                    f"docs[{i}] must be a dict, got {type(doc).__name__}"
                )

        if self.feed_kwargs:
            import json
            try:
                json.dumps(self.feed_kwargs)
            except (TypeError, ValueError) as e:
                raise AirflowException(
                    f"feed_kwargs must be JSON-serializable for trigger deferral: {e}"
                )

        conn_info = {
            "host": conn.host,
            "port": conn.port,
            "schema": conn.schema,
            "namespace": VespaHook._get_field(extra, "namespace") or "default",
            "extra": extra,
        }

        raise TaskDeferred(
            trigger=VespaFeedTrigger(
                docs=self.docs,
                conn_info=conn_info,
                operation_type=self.operation_type,
                feed_kwargs=self.feed_kwargs,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        if not event["success"]:
            raise AirflowException(
                f"{len(event['errors'])} document(s) failed; "
                f"Error details: {event['errors']}"
            )
        return {"ingested": event.get("sent", 0)}
