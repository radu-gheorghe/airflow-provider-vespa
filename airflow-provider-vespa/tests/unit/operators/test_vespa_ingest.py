import pytest
from unittest.mock import Mock, patch

from airflow.exceptions import AirflowException, TaskDeferred

from airflow_provider_vespa.operators.vespa_ingest import VespaIngestOperator


class TestVespaIngestOperator:

    def test_init(self):
        docs = [{"id": "1", "title": "Test"}]
        op = VespaIngestOperator(
            task_id="test_ingest",
            docs=docs,
            vespa_conn_id="test_conn",
            operation_type="feed",
        )

        assert op.docs == docs
        assert op.vespa_conn_id == "test_conn"
        assert op.operation_type == "feed"
        assert op.feed_kwargs == {}

    def test_init_with_feed_kwargs(self):
        op = VespaIngestOperator(
            task_id="test_ingest",
            docs=[{"id": "1", "fields": {"x": {"assign": 1}}}],
            operation_type="update",
            feed_kwargs={"auto_assign": False, "create": True},
        )

        assert op.feed_kwargs == {"auto_assign": False, "create": True}

    def test_init_invalid_operation_type(self):
        with pytest.raises(ValueError, match="Invalid operation_type"):
            VespaIngestOperator(
                task_id="test",
                docs=[{"id": "1"}],
                operation_type="upsert",
            )

    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_execute_defers_to_trigger(self, mock_get_connection):
        mock_conn = Mock()
        mock_conn.host = "https://vespa.test:8080"
        mock_conn.port = None
        mock_conn.schema = "doc"
        mock_conn.extra_dejson = {"namespace": "test"}
        mock_get_connection.return_value = mock_conn

        docs = [{"id": "1", "content": "test"}]
        op = VespaIngestOperator(
            task_id="test_task",
            docs=docs,
            vespa_conn_id="test_conn",
        )

        context = {"task_instance": Mock()}

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context)

        trigger = exc_info.value.trigger
        assert trigger.docs == docs
        assert trigger.operation_type == "feed"
        assert trigger.feed_kwargs == {}
        assert exc_info.value.method_name == "execute_complete"

    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_execute_forwards_feed_kwargs(self, mock_get_connection):
        mock_conn = Mock()
        mock_conn.host = "https://vespa.test:8080"
        mock_conn.port = None
        mock_conn.schema = "doc"
        mock_conn.extra_dejson = {"namespace": "test"}
        mock_get_connection.return_value = mock_conn

        op = VespaIngestOperator(
            task_id="test_task",
            docs=[{"id": "1", "fields": {"x": 1}}],
            vespa_conn_id="test_conn",
            operation_type="update",
            feed_kwargs={"auto_assign": False},
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({"task_instance": Mock()})

        assert exc_info.value.trigger.feed_kwargs == {"auto_assign": False}

    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_execute_materializes_iterator(self, mock_get_connection):
        mock_conn = Mock()
        mock_conn.host = "https://vespa.test:8080"
        mock_conn.port = None
        mock_conn.schema = "doc"
        mock_conn.extra_dejson = {"namespace": "test"}
        mock_get_connection.return_value = mock_conn

        docs_iter = iter([{"id": "1"}, {"id": "2"}])
        op = VespaIngestOperator(
            task_id="test_task",
            docs=docs_iter,
            vespa_conn_id="test_conn",
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({"task_instance": Mock()})

        assert exc_info.value.trigger.docs == [{"id": "1"}, {"id": "2"}]

    def test_execute_complete_success(self):
        docs = [{"id": "1"}, {"id": "2"}]
        op = VespaIngestOperator(
            task_id="test_ingest",
            docs=docs,
            vespa_conn_id="test_conn",
        )

        result = op.execute_complete({}, {"success": True, "sent": 2})
        assert result == {"ingested": 2}

    def test_execute_complete_failure_with_errors(self):
        docs = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        op = VespaIngestOperator(
            task_id="test_ingest",
            docs=docs,
            vespa_conn_id="test_conn",
        )

        failure_event = {
            "success": False,
            "sent": 3,
            "errors": [
                {"id": "2", "status": 400, "reason": {"error": "Invalid format"}},
                {"id": "3", "status": 500, "reason": {"error": "Server error"}},
            ],
        }

        with pytest.raises(AirflowException) as exc_info:
            op.execute_complete({}, failure_event)

        msg = str(exc_info.value)
        assert "2 document(s) failed" in msg

    def test_execute_complete_failure_with_exception_error(self):
        op = VespaIngestOperator(
            task_id="test_ingest",
            docs=[{"id": "1"}],
            vespa_conn_id="test_conn",
        )

        failure_event = {
            "success": False,
            "errors": [{"error": "Trigger failed: ConnectionError: Unable to connect"}],
        }

        with pytest.raises(AirflowException) as exc_info:
            op.execute_complete({}, failure_event)

        assert "ConnectionError" in str(exc_info.value)

    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_execute_materializes_to_self_docs(self, mock_get_connection):
        """After execute(), self.docs should be a list so execute_complete can use len()."""
        mock_conn = Mock()
        mock_conn.host = "https://vespa.test:8080"
        mock_conn.port = None
        mock_conn.schema = "doc"
        mock_conn.extra_dejson = {"namespace": "test"}
        mock_get_connection.return_value = mock_conn

        docs_iter = iter([{"id": "1"}, {"id": "2"}])
        op = VespaIngestOperator(
            task_id="test_task",
            docs=docs_iter,
            vespa_conn_id="test_conn",
        )

        with pytest.raises(TaskDeferred):
            op.execute({"task_instance": Mock()})

        assert isinstance(op.docs, list)
        assert len(op.docs) == 2

    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_execute_rejects_non_dict_docs(self, mock_get_connection):
        mock_conn = Mock()
        mock_conn.host = "https://vespa.test:8080"
        mock_conn.port = None
        mock_conn.schema = "doc"
        mock_conn.extra_dejson = {"namespace": "test"}
        mock_get_connection.return_value = mock_conn

        op = VespaIngestOperator(
            task_id="test_task",
            docs=[{"id": "1"}, "not-a-dict"],
            vespa_conn_id="test_conn",
        )

        with pytest.raises(AirflowException, match="docs\\[1\\] must be a dict"):
            op.execute({"task_instance": Mock()})

    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_execute_rejects_non_serializable_feed_kwargs(self, mock_get_connection):
        mock_conn = Mock()
        mock_conn.host = "https://vespa.test:8080"
        mock_conn.port = None
        mock_conn.schema = "doc"
        mock_conn.extra_dejson = {"namespace": "test"}
        mock_get_connection.return_value = mock_conn

        op = VespaIngestOperator(
            task_id="test_task",
            docs=[{"id": "1", "fields": {"x": 1}}],
            vespa_conn_id="test_conn",
            feed_kwargs={"callback": lambda r: r},
        )

        with pytest.raises(AirflowException, match="feed_kwargs must be JSON-serializable"):
            op.execute({"task_instance": Mock()})
