from __future__ import annotations
from typing import Any, Dict, Iterable
from airflow.triggers.base import BaseTrigger, TriggerEvent

class VespaFeedTrigger(BaseTrigger):
    """
    Fires the Event when the given iterable is fully acknowledged.
    """

    def __init__(
        self,
        docs: Iterable[Dict],
        conn_info: Dict[str, Any],
        operation_type: str = "feed",
    ):
        super().__init__()
        self.docs = list(docs)
        self.conn_info = conn_info
        self.operation_type = operation_type

    def serialize(self) -> tuple[str, Dict[str, Any]]:
        return (
            "airflow_provider_vespa.triggers.vespa_feed_trigger.VespaFeedTrigger",
            {"docs": self.docs, "conn_info": self.conn_info, "operation_type": self.operation_type},
        )

    async def run(self):
        try:
            from airflow_provider_vespa.hooks.vespa import VespaHook
            
            hook = VespaHook.from_resolved_connection(
                host=self.conn_info["host"],
                schema=self.conn_info["schema"],
                namespace=self.conn_info["namespace"],
                extra=self.conn_info["extra"],
            )
            
            # Run the synchronous hook method in a thread pool
            import asyncio
            loop = asyncio.get_event_loop()
            
            def _feed():
                return hook.feed_iterable(self.docs, operation_type=self.operation_type)
            
            summary = await loop.run_in_executor(None, _feed)
            
            if summary["errors"]:
                yield TriggerEvent({"success": False, "errors": summary["error_details"]})
            else:
                yield TriggerEvent({"success": True})
        except Exception as e:
            # Log the full exception for debugging purposes
            import traceback
            error_msg = f"Trigger failed: {type(e).__name__}: {e}"
            print(f"ERROR: {error_msg}")
            traceback.print_exc()
            yield TriggerEvent({"success": False, "errors": [{"error": error_msg}]})
