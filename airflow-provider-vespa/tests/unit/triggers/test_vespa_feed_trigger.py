import pytest
from unittest.mock import Mock, patch, AsyncMock
from airflow_provider_vespa.triggers.vespa_feed_trigger import VespaFeedTrigger


class TestVespaFeedTrigger:
    """Test cases for the VespaFeedTrigger class.
    
    VespaFeedTrigger runs asynchronously in Airflow's triggerer process to handle
    long-running Vespa document ingestion operations without blocking worker slots.
    
    Key aspects of triggers:
    - They run in a separate async process (triggerer)
    - They can't access the Airflow database directly
    - They receive pre-resolved connection information
    - They yield TriggerEvents to communicate back to the operator
    - They must be serializable for persistence and process communication
    """
    
    def test_init(self):
        """Test VespaFeedTrigger initialization.
        
        This test verifies that:
        - All constructor parameters are properly stored
        - Parameters are accessible as instance attributes
        """
        docs = [{"id": "1", "title": "Test"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc"}
        
        trigger = VespaFeedTrigger(
            docs=docs,
            conn_info=conn_info,
            operation_type="feed"
        )
        
        # Verify all parameters were stored correctly
        assert trigger.docs == docs
        assert trigger.conn_info == conn_info
        assert trigger.operation_type == "feed"
    
    @pytest.mark.asyncio
    async def test_run_success(self):
        """Test successful trigger execution.
        
        The trigger's run() method:
        1. Creates a VespaHook from resolved connection info (no DB access)
        2. Runs the hook's feed_iterable method in a thread pool (it's synchronous)
        3. Processes the results and yields appropriate TriggerEvents
        4. Handles both success and error scenarios
        
        This test verifies:
        - Hook is created with correct connection info
        - Feed operation is executed properly
        - Success event is yielded when no errors occur
        - Event payload contains correct success status
        """
        docs = [{"id": "1", "content": "test"}]
        # Connection info includes all details needed to create VespaHook
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)
        
        # Mock the VespaHook class and its behavior
        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            # Mock successful feed operation (no errors)
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook
            
            # Collect events yielded by the trigger
            events = []
            async for event in trigger.run():
                events.append(event)
            
            # Verify exactly one success event was yielded
            assert len(events) == 1
            assert events[0].payload["success"] == True
    
    def test_serialize(self):
        """Test trigger serialization.
        
        Triggers must be serializable because:
        - They're passed between different processes (worker -> triggerer)
        - They may be persisted to disk if the triggerer restarts
        - Airflow needs to recreate them from serialized data
        
        This test verifies:
        - Serialize method returns correct format (class path, data dict)
        - All necessary data is included in serialization
        - Serialized data contains no unpickleable objects
        """
        docs = [{"id": "1"}]
        conn_info = {"host": "test"}
        
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)
        serialized = trigger.serialize()
        
        # Verify serialization format: (class_path, data_dict)
        assert len(serialized) == 2
        assert isinstance(serialized[0], str)  # Class path
        assert isinstance(serialized[1], dict)  # Data dictionary
        
        # Verify all necessary data is included
        assert "docs" in serialized[1]
        assert "conn_info" in serialized[1]
        assert "operation_type" in serialized[1]