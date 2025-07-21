from __future__ import annotations
from typing import Any
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow_provider_vespa.hooks.vespa import VespaHook

class VespaIngestOperator(BaseOperator):
    """
    Writes one document (`body`) to Vespa.
    :param body: dict, eg {"body": "hello world"}
    :param vespa_conn_id: Airflow connection id
    """

    # TODO: how to add flexibility here in terms of schema?
    
    template_fields = ("body",)          # lets you {{ render }} Jinja vars

    def __init__(self, *, body: dict[str, Any], vespa_conn_id: str = VespaHook.default_conn_name, **kwargs):
        super().__init__(**kwargs)
        self.body = body
        self.vespa_conn_id = vespa_conn_id

    def execute(self, context: Context):
        hook = VespaHook(self.vespa_conn_id)
        # TODO how do we achieve throughput?
        result = hook.put_document(self.body)
        self.log.info("Indexed document: %s", result)
        return result
