import pytest
from unittest.mock import Mock, patch
from airflow.exceptions import TaskDeferred
from airflow_provider_vespa.operators.vespa_ingest import VespaIngestOperator


class TestVespaIngestOperator:
    """Test cases for the VespaIngestOperator class.
    
    VespaIngestOperator is a deferrable operator that ingests documents into Vespa.
    It follows Airflow's deferrable pattern:
    1. Resolves the Airflow connection in the worker process
    2. Defers the actual work to a trigger that runs asynchronously
    3. Handles the trigger's completion in execute_complete()
    
    This pattern prevents blocking worker slots during long-running operations.
    """
    
    def test_init(self):
        """Test VespaIngestOperator initialization.
        
        This test verifies that:
        - All constructor parameters are properly stored
        - Parameters are accessible as instance attributes
        """
        docs = [{"id": "1", "title": "Test"}]
        op = VespaIngestOperator(
            task_id="test_ingest",
            docs=docs,
            vespa_conn_id="test_conn",
            operation_type="feed"
        )
        
        # Verify all parameters were stored correctly
        assert op.docs == docs
        assert op.vespa_conn_id == "test_conn"
        assert op.operation_type == "feed"
    
    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_execute_defers_to_trigger(self, mock_get_connection):
        """Test that execute() properly defers to a trigger.
        
        The deferrable pattern works as follows:
        1. Operator resolves connection in worker (where DB access is available)
        2. Operator raises TaskDeferred with a trigger and connection info
        3. Airflow moves task to triggerer process
        4. Trigger runs asynchronously without blocking worker slots
        5. When trigger completes, execute_complete() is called
        
        This test verifies:
        - Connection is properly resolved from Airflow
        - TaskDeferred exception is raised (this is expected behavior)
        - Trigger is created with correct parameters
        - Callback method name is set correctly
        """
        # Mock the Airflow connection that would be resolved from the database
        mock_conn = Mock()
        mock_conn.host = "https://vespa.test:8080"
        mock_conn.schema = "doc"
        mock_conn.extra_dejson = {"extra__vespa__namespace": "test"}
        mock_get_connection.return_value = mock_conn
        
        # Create operator with test documents
        docs = [{"id": "1", "content": "test"}]
        op = VespaIngestOperator(
            task_id="test_task",
            docs=docs,
            vespa_conn_id="test_conn"
        )
        
        # Mock task context (normally provided by Airflow)
        context = {"task_instance": Mock()}
        
        # Execute should raise TaskDeferred (this is the deferrable pattern)
        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context)
        
        # Verify the trigger was created with correct parameters
        trigger = exc_info.value.trigger
        assert trigger.docs == docs
        assert trigger.operation_type == "feed"
        
        # Verify the callback method is set correctly
        assert exc_info.value.method_name == "execute_complete"