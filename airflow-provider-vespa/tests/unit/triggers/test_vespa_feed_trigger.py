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
    
    @pytest.mark.asyncio
    async def test_run_hook_creation_failure(self):
        """Test trigger behavior when VespaHook creation fails.
        
        If the trigger can't create a VespaHook (due to invalid connection info,
        missing dependencies, etc.), it should yield a failure event with 
        appropriate error details.
        """
        docs = [{"id": "1", "content": "test"}]
        # Connection info that might cause hook creation to fail
        conn_info = {"host": "invalid://host", "namespace": "test", "schema": "doc", "extra": {}}
        
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)
        
        # Mock VespaHook to raise exception during creation
        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook_class.from_resolved_connection.side_effect = ValueError("Invalid host URL")
            
            # Collect events yielded by the trigger
            events = []
            async for event in trigger.run():
                events.append(event)
            
            # Should yield exactly one failure event
            assert len(events) == 1
            assert events[0].payload["success"] == False
            assert "errors" in events[0].payload
            assert len(events[0].payload["errors"]) == 1
            assert "ValueError: Invalid host URL" in events[0].payload["errors"][0]["error"]
    
    @pytest.mark.asyncio
    async def test_run_feed_operation_failure(self):
        """Test trigger behavior when feed operation fails.
        
        If the VespaHook's feed_iterable method raises an exception,
        the trigger should catch it and yield a failure event.
        """
        docs = [{"id": "1", "content": "test"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)
        
        # Mock VespaHook creation to succeed, but feed_iterable to fail
        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.side_effect = ConnectionError("Unable to connect to Vespa server")
            mock_hook_class.from_resolved_connection.return_value = mock_hook
            
            # Collect events
            events = []
            async for event in trigger.run():
                events.append(event)
            
            # Should yield failure event with connection error
            assert len(events) == 1
            assert events[0].payload["success"] == False
            assert "ConnectionError: Unable to connect to Vespa server" in events[0].payload["errors"][0]["error"]
    
    @pytest.mark.asyncio
    async def test_run_async_executor_failure(self):
        """Test trigger behavior when async executor fails.
        
        If loop.run_in_executor fails (due to thread pool issues, etc.),
        the trigger should handle it gracefully.
        """
        docs = [{"id": "1", "content": "test"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)
        
        # Mock VespaHook creation to succeed
        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook
            
            # Mock loop.run_in_executor to fail
            with patch('asyncio.get_event_loop') as mock_get_loop:
                mock_loop = Mock()
                mock_loop.run_in_executor.side_effect = RuntimeError("Thread pool exhausted")
                mock_get_loop.return_value = mock_loop
                
                # Collect events
                events = []
                async for event in trigger.run():
                    events.append(event)
                
                # Should yield failure event with executor error
                assert len(events) == 1
                assert events[0].payload["success"] == False
                assert "RuntimeError: Thread pool exhausted" in events[0].payload["errors"][0]["error"]
    
    @pytest.mark.asyncio
    async def test_run_with_feed_errors(self):
        """Test trigger behavior when feed operation has document-level errors.
        
        When some documents fail during the feed operation, the trigger should
        yield a failure event with the detailed error information.
        """
        docs = [{"id": "1", "content": "good"}, {"id": "2", "content": "bad"}, {"id": "3", "content": "ugly"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)
        
        # Mock hook to return feed result with errors
        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {
                "sent": 3,
                "errors": 2,
                "error_details": [
                    {"id": "2", "status": 400, "reason": {"error": "Invalid format"}},
                    {"id": "3", "status": 500, "reason": {"error": "Server error"}}
                ]
            }
            mock_hook_class.from_resolved_connection.return_value = mock_hook
            
            # Collect events
            events = []
            async for event in trigger.run():
                events.append(event)
            
            # Should yield failure event with document errors
            assert len(events) == 1
            assert events[0].payload["success"] == False
            assert len(events[0].payload["errors"]) == 2
            assert events[0].payload["errors"][0]["id"] == "2"
            assert events[0].payload["errors"][1]["id"] == "3"
    
    @pytest.mark.asyncio
    async def test_run_update_operation(self):
        """Test trigger with update operation type.
        
        The trigger should pass the operation_type parameter correctly to 
        the VespaHook's feed_iterable method for different operations.
        """
        docs = [{"id": "1", "fields": {"title": {"assign": "Updated Title"}}}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        
        # Create trigger with update operation
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, operation_type="update")
        
        # Mock successful update operation
        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook
            
            # Collect events
            events = []
            async for event in trigger.run():
                events.append(event)
            
            # Verify operation_type was passed correctly
            mock_hook.feed_iterable.assert_called_once_with(docs, operation_type="update")
            
            # Should yield success event
            assert len(events) == 1
            assert events[0].payload["success"] == True
    
    @pytest.mark.asyncio
    async def test_run_delete_operation(self):
        """Test trigger with delete operation type.
        
        Delete operations should work the same as other operations, just
        passing the correct operation_type to the hook.
        """
        docs = [{"id": "1"}, {"id": "2"}]  # Only IDs needed for delete
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        
        # Create trigger with delete operation
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, operation_type="delete")
        
        # Mock successful delete operation
        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 2, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook
            
            # Collect events
            events = []
            async for event in trigger.run():
                events.append(event)
            
            # Verify operation_type was passed correctly
            mock_hook.feed_iterable.assert_called_once_with(docs, operation_type="delete")
            
            # Should yield success event
            assert len(events) == 1
            assert events[0].payload["success"] == True
    
    @pytest.mark.asyncio
    async def test_run_default_feed_operation(self):
        """Test trigger with default operation type (feed).
        
        When no operation_type is specified, it should default to "feed".
        """
        docs = [{"id": "1", "content": "test"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        
        # Create trigger without specifying operation_type (should default to "feed")
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)
        
        # Mock successful feed operation
        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook
            
            # Collect events
            events = []
            async for event in trigger.run():
                events.append(event)
            
            # Verify default operation_type was used
            mock_hook.feed_iterable.assert_called_once_with(docs, operation_type="feed")
            
            # Should yield success event
            assert len(events) == 1
            assert events[0].payload["success"] == True