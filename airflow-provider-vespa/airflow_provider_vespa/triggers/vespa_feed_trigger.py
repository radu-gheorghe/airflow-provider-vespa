from __future__ import annotations

import asyncio
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

class VespaFeedTrigger(BaseTrigger):
    """
    Fires the Event when the given iterable is fully acknowledged.
    """

    def __init__(
        self,
        docs: list[dict],
        conn_info: dict[str, Any],
        operation_type: str = "feed",
        feed_kwargs: dict[str, Any] | None = None,
    ):
        super().__init__()
        self.docs = list(docs)
        self.conn_info = conn_info
        self.operation_type = operation_type
        self.feed_kwargs = feed_kwargs or {}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_vespa.triggers.vespa_feed_trigger.VespaFeedTrigger",
            {
                "docs": self.docs,
                "conn_info": self.conn_info,
                "operation_type": self.operation_type,
                "feed_kwargs": self.feed_kwargs,
            },
        )

    async def run(self):
        try:
            from airflow_provider_vespa.hooks.vespa import VespaHook

            hook = VespaHook.from_resolved_connection(
                host=self.conn_info["host"],
                port=self.conn_info.get("port"),
                schema=self.conn_info.get("schema"),
                namespace=self.conn_info.get("namespace"),
                extra=self.conn_info.get("extra", {}),
            )

            loop = asyncio.get_running_loop()

            def _feed():
                return hook.feed_iterable(
                    self.docs,
                    operation_type=self.operation_type,
                    **self.feed_kwargs,
                )

            summary = await loop.run_in_executor(None, _feed)

            if summary["errors"]:
                self.log.error(
                    "Vespa feed operation failed: %d error(s). Details: %s",
                    summary["errors"],
                    summary["error_details"],
                )
                yield TriggerEvent({
                    "success": False,
                    "sent": summary["sent"],
                    "errors": summary["error_details"],
                })
            else:
                self.log.info(
                    "Vespa feed operation completed successfully for %d document(s)",
                    len(self.docs),
                )
                yield TriggerEvent({"success": True, "sent": summary["sent"]})
        except Exception as e:
            error_msg = f"Trigger failed: {type(e).__name__}: {e}"
            self.log.error(error_msg, exc_info=True)
            yield TriggerEvent({
                "success": False,
                "errors": [{"error": error_msg}],
            })
