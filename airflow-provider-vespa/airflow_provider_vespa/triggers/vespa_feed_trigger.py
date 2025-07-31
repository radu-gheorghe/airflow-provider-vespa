from __future__ import annotations
import asyncio
from typing import Any, Dict, Iterable
from airflow.triggers.base import BaseTrigger, TriggerEvent

class VespaFeedTrigger(BaseTrigger):
    """
    Fires the Event when the given iterable is fully acknowledged.
    """

    def __init__(
        self,
        docs: Iterable[Dict],
        conn_id: str = "vespa_default",
    ):
        super().__init__()
        self.docs, self.conn_id = list(docs), conn_id

    def serialize(self) -> tuple[str, Dict[str, Any]]:
        return (
            "airflow_provider_vespa.triggers.vespa_feed_trigger.VespaFeedTrigger",
            {"docs": self.docs, "conn_id": self.conn_id},
        )

    async def run(self):
        from airflow_provider_vespa.hooks.vespa import VespaHook

        hook = VespaHook(self.conn_id)
        loop = asyncio.get_event_loop()

        # run the sync pyvespa call in a thread so we don't block the Triggerer
        summary = await loop.run_in_executor(None, hook.feed_async_iterable, self.docs)

        if summary["errors"]:
            yield TriggerEvent(
                {"success": False, "error": summary["first_error"], **summary}
            )
        else:
            yield TriggerEvent({"success": True, **summary})
