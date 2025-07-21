from __future__ import annotations
from typing import Any, Callable, Iterable, Union
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow_provider_vespa.hooks.vespa import VespaHook

class VespaIngestOperator(BaseOperator):
    """
    Writes documents to Vespa from various sources.
    
    :param document_generator: Callable that returns an iterable of documents, or direct iterable
    :param vespa_conn_id: Airflow connection id
    """

    # TODO: how to add flexibility here in terms of schema?

    def __init__(self, *, 
                 document_generator: Union[Callable[[], Iterable[dict]], Iterable[dict]],
                 vespa_conn_id: str = VespaHook.default_conn_name, 
                 **kwargs):
        super().__init__(**kwargs)
        self.document_generator = document_generator
        # TODO retry logic should be here?
        self.vespa_conn_id = vespa_conn_id

    def execute(self, context: Context):
        hook = VespaHook(self.vespa_conn_id)
        
        # Pass documents to the hook
        documents = self.document_generator() if callable(self.document_generator) else self.document_generator
        hook.feed_async_iterable(documents)
        self.log.info("Successfully indexed documents")
