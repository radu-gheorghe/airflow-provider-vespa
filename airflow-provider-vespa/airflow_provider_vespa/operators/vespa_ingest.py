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
    # Jarek: This could be passed here as optional parameter - and namespace could be retrieve from the connection
    # by default but could be overridden here.
    # Additionally - uf you set schema, namespace and any other parameters as "self." attributes here and
    # add it to "template_fields" then you can use Jinja templating to pass them in the Dag definition -
    # which means that they can be dynamically set at runtime while the Dag is running - for example you can
    # pass the schema or namespace via Xcoms from previous tasks.

    def __init__(self, *,
                 document_generator: Union[Callable[[], Iterable[dict]], Iterable[dict]],
                 vespa_conn_id: str = VespaHook.default_conn_name,
                 **kwargs):
        super().__init__(**kwargs)
        # I am not sure if you need document_generator here. I think it would be better if you just pass a
        # single list or array of document (document handles? Not sure if that is the whole body of the
        # document here or you can pass - say - s3 url to it or something like that. Then you can use
        # Dynamic Task Mapping to have parallel tasks doing ingestions of the documents fully using the
        # power of Airflow - then you can not only defer parallelisation of the ingestion, but also you
        # will see each document ingestion as a separate "mapped" task inn the UI, you will be able to
        # see a separate logs for each document ingestion, and you will be able to restart individual tasks
        # from the UI if they fail.
        self.document_generator = document_generator
        # TODO retry logic should be here?
        self.vespa_conn_id = vespa_conn_id

    def execute(self, context: Context):
        hook = VespaHook(self.vespa_conn_id)

        # Jarek: I think this will be way more efficient to use "Deffered Operators" & "Triggers" pattern here.
        # Details in the mail thread
        # Pass documents to the hook
        documents = self.document_generator() if callable(self.document_generator) else self.document_generator
        hook.feed_async_iterable(documents)
        self.log.info("Successfully indexed documents")
        # Jarek: you should return here meaningful "handles" to the ingested document - such returned values
        # are automatically stored in Xcom as "return_value" of the task and can be used by other tasks
        # This is the big power of Airflow - that you can pass data between tasks easily in standard way
