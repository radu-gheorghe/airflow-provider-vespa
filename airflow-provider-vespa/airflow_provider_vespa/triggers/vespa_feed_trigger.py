from __future__ import annotations
from typing import Any, Dict, Iterable
from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio

class VespaFeedTrigger(BaseTrigger):
    """
    Fires the Event when the given iterable is fully acknowledged.
    """

    def __init__(
        self,
        docs: Iterable[Dict],
        conn_info: Dict[str, Any],
    ):
        super().__init__()
        self.docs = list(docs)
        self.conn_info = conn_info

    def serialize(self) -> tuple[str, Dict[str, Any]]:
        return (
            "airflow_provider_vespa.triggers.vespa_feed_trigger.VespaFeedTrigger",
            {"docs": self.docs, "conn_info": self.conn_info},
        )

    async def run(self):
        # Run potentially blocking Vespa client calls in a separate thread so we don't block the
        # triggerer's event loop and also avoid calling sync code directly in the async context.
        loop = asyncio.get_event_loop()

        def _feed():
            from airflow_provider_vespa.hooks.vespa import VespaHook
            hook = VespaHook.from_resolved_connection(
                host=self.conn_info["host"],
                extra=self.conn_info["extra"],
            )
            return hook.feed_async_iterable(self.docs)

        summary = await loop.run_in_executor(None, _feed)

        if summary["errors"]:
            yield TriggerEvent({"success": False, "errors": summary["error_details"]})
        else:
            yield TriggerEvent({"success": True})
