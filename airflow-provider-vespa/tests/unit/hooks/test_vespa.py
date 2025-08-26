import pytest
from unittest.mock import Mock, patch
from airflow_provider_vespa.hooks.vespa import VespaHook


class TestVespaHook:
    """Test cases for the VespaHook class.
    
    VespaHook is responsible for connecting to Vespa and handling document operations.
    It supports both regular Airflow connections and direct instantiation from resolved
    connection info (used by triggers that can't access the Airflow database).
    """
    
    @patch('airflow_provider_vespa.hooks.vespa.BaseHook.get_connection')
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_init_default(self, mock_vespa, mock_get_connection):
        """Test VespaHook initialization with default parameters.
        
        This test verifies that:
        - The hook can be created without explicit parameters
        - Connection details are properly extracted from Airflow connection
        - Hook attributes are set from connection configuration
        """
        # Mock the Airflow connection object
        mock_conn = Mock()
        mock_conn.host = "https://vespa.example.com:8080"
        mock_conn.schema = "test_schema"
        mock_conn.extra_dejson = {"namespace": "default"}
        mock_get_connection.return_value = mock_conn
        
        # Create hook with default connection
        hook = VespaHook()
        
        # Verify connection details were properly set
        assert hook.namespace == "default"
        assert hook.schema == "test_schema"
    
    @patch('airflow_provider_vespa.hooks.vespa.BaseHook.get_connection')
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_init_with_params(self, mock_vespa, mock_get_connection):
        """Test VespaHook initialization with custom parameters.
        
        This test verifies that:
        - Custom connection ID is used
        - Namespace and schema parameters override connection values
        - Hook prioritizes explicit parameters over connection config
        """
        # Mock the Airflow connection with default values
        mock_conn = Mock()
        mock_conn.host = "https://vespa.example.com:8080"
        mock_conn.schema = "conn_schema"  # This should be overridden
        mock_conn.extra_dejson = {"namespace": "conn_ns"}  # This should be overridden
        mock_get_connection.return_value = mock_conn
        
        # Create hook with custom parameters that should override connection values
        hook = VespaHook(
            conn_id="custom_conn", 
            namespace="test_ns",     # Overrides connection namespace
            schema="test_schema"     # Overrides connection schema
        )
        
        # Verify custom parameters took precedence
        assert hook.namespace == "test_ns"
        assert hook.schema == "test_schema"
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_from_resolved_connection(self, mock_vespa):
        """Test VespaHook creation from resolved connection info.
        
        This method is used by triggers that run in async contexts where
        they can't access the Airflow database to resolve connections.
        Instead, they receive pre-resolved connection information.
        
        This test verifies that:
        - Hook can be created without database access
        - Connection info is properly applied
        - Vespa client is initialized correctly
        """
        # Connection info that would normally come from resolving an Airflow connection
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "document",
            "namespace": "prod",
            "extra": {}
        }
        
        # Create hook from resolved connection (no database access needed)
        hook = VespaHook.from_resolved_connection(
            host=conn_info["host"],
            schema=conn_info["schema"],
            namespace=conn_info["namespace"],
            extra=conn_info["extra"]
        )
        
        # Verify connection details were properly applied
        assert hook.namespace == "prod"
        assert hook.schema == "document"
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_already_vespa_format(self, mock_vespa):
        """Test _normalise with documents already in Vespa format.
        
        Documents that already have a "fields" key should be passed through
        unchanged since they're already in the correct Vespa format.
        """
        # Create hook with minimal connection info
        conn_info = {
            "host": "https://vespa.example.com:8080", 
            "schema": "doc", 
            "namespace": "test", 
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Documents already in Vespa format
        docs = [
            {"id": "doc1", "fields": {"title": "Test 1"}},
            {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}}
        ]
        
        result = hook._normalise(docs)
        
        # Should pass through unchanged
        assert len(result) == 2
        assert result[0] == {"id": "doc1", "fields": {"title": "Test 1"}}
        assert result[1] == {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}}
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_raw_fields(self, mock_vespa):
        """Test _normalise with raw field dictionaries.
        
        Raw documents (without "fields" key) should be converted to Vespa format
        by wrapping their content in a "fields" key and extracting/generating IDs.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test", 
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Raw field documents with explicit IDs
        docs = [
            {"id": "doc1", "title": "Test 1"},
            {"id": "doc2", "title": "Test 2", "content": "Content"}
        ]
        
        result = hook._normalise(docs)
        
        # Should be converted to Vespa format
        assert len(result) == 2
        assert result[0] == {"id": "doc1", "fields": {"title": "Test 1"}}
        assert result[1] == {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}}
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_generate_missing_ids(self, mock_vespa):
        """Test _normalise generates UUIDs for documents without IDs.
        
        When gen_missing_id=True (default), documents without "id" fields
        should get auto-generated UUID strings as their document IDs.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc", 
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Documents without IDs
        docs = [
            {"title": "Test 1"},
            {"title": "Test 2", "content": "Content"}
        ]
        
        result = hook._normalise(docs, gen_missing_id=True)
        
        # Should generate IDs and convert to Vespa format
        assert len(result) == 2
        assert "id" in result[0]
        assert "id" in result[1]
        assert result[0]["fields"] == {"title": "Test 1"}
        assert result[1]["fields"] == {"title": "Test 2", "content": "Content"}
        # IDs should be valid UUID strings
        import uuid
        assert uuid.UUID(result[0]["id"])  # Should not raise exception
        assert uuid.UUID(result[1]["id"])  # Should not raise exception
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_no_id_generation(self, mock_vespa):
        """Test _normalise with ID generation disabled.
        
        When gen_missing_id=False, documents without IDs should get None as their ID.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Document without ID
        docs = [{"title": "Test without ID"}]
        
        result = hook._normalise(docs, gen_missing_id=False)
        
        # Should have None as ID
        assert len(result) == 1
        assert result[0]["id"] is None
        assert result[0]["fields"] == {"title": "Test without ID"}
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_default_callback_successful_response(self, mock_vespa):
        """Test default_callback with successful response.
        
        Successful responses should not add anything to the error queue.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Mock successful response
        mock_response = Mock()
        mock_response.is_successful.return_value = True
        
        # Call callback - should not add to error queue
        hook.default_callback(mock_response, "doc1")
        
        # Error queue should be empty
        assert hook.feed_errors_queue.empty()
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_default_callback_failed_response_with_json(self, mock_vespa):
        """Test default_callback with failed response containing JSON error details.
        
        Failed responses with parseable JSON should capture the error details
        in the appropriate format for later processing.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc", 
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Mock failed response with JSON error
        mock_response = Mock()
        mock_response.is_successful.return_value = False
        mock_response.status_code = 400
        mock_response.get_json.return_value = {"error": "Invalid document format", "code": "INVALID_DOC"}
        
        # Call callback
        hook.default_callback(mock_response, "doc1")
        
        # Should have captured the error
        assert not hook.feed_errors_queue.empty()
        error = hook.feed_errors_queue.get()
        assert error["id"] == "doc1"
        assert error["status"] == 400
        assert error["reason"] == {"error": "Invalid document format", "code": "INVALID_DOC"}
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_default_callback_failed_response_json_parse_error(self, mock_vespa):
        """Test default_callback with failed response that has unparseable JSON.
        
        When JSON parsing fails, the callback should capture debugging information
        including the parsing error and raw response data.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test", 
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Mock failed response with JSON parsing error
        mock_response = Mock()
        mock_response.is_successful.return_value = False
        mock_response.status_code = 500
        mock_response.get_json.side_effect = ValueError("Invalid JSON")
        mock_response.text = "Internal Server Error"
        
        # Call callback
        hook.default_callback(mock_response, "doc2")
        
        # Should have captured the error with debugging info
        assert not hook.feed_errors_queue.empty()
        error = hook.feed_errors_queue.get()
        assert error["id"] == "doc2"
        assert error["status"] == 500
        assert "json_parse_error" in error["reason"]
        assert "Invalid JSON" in error["reason"]["json_parse_error"]
        assert error["reason"]["raw_response"] == "Internal Server Error"
        assert "response_type" in error["reason"]
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_default_callback_failed_response_no_text_attribute(self, mock_vespa):
        """Test default_callback with response object missing text attribute.
        
        Some response objects might not have a text attribute, so the callback
        should handle this gracefully.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Mock failed response without text attribute
        mock_response = Mock()
        mock_response.is_successful.return_value = False
        mock_response.status_code = 503
        mock_response.get_json.side_effect = Exception("Connection error")
        # Don't set text attribute
        del mock_response.text
        
        # Call callback
        hook.default_callback(mock_response, "doc3")
        
        # Should have captured the error with fallback message
        assert not hook.feed_errors_queue.empty()
        error = hook.feed_errors_queue.get()
        assert error["id"] == "doc3"
        assert error["status"] == 503
        assert error["reason"]["raw_response"] == "No text attribute"
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_success_default_operation(self, mock_vespa):
        """Test feed_iterable with successful feed operation using default operation type.
        
        This test verifies the main functionality of feed_iterable:
        - Documents are normalized properly
        - Vespa feed_async_iterable is called with correct parameters
        - Success summary is returned with correct counts
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Mock the Vespa app
        mock_app = Mock()
        hook.vespa_app = mock_app
        
        # Test documents
        docs = [
            {"id": "doc1", "title": "Test 1"},
            {"id": "doc2", "title": "Test 2"}
        ]
        
        # Call feed_iterable
        result = hook.feed_iterable(docs)
        
        # Verify feed_async_iterable was called with correct parameters
        mock_app.feed_async_iterable.assert_called_once()
        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        
        assert call_kwargs["operation_type"] == "feed"  # default
        assert call_kwargs["schema"] == "doc"
        assert call_kwargs["namespace"] == "test"
        assert call_kwargs["callback"] == hook.default_callback
        assert len(call_kwargs["iter"]) == 2
        # Check documents were normalized
        assert call_kwargs["iter"][0] == {"id": "doc1", "fields": {"title": "Test 1"}}
        assert call_kwargs["iter"][1] == {"id": "doc2", "fields": {"title": "Test 2"}}
        
        # Verify result summary
        assert result["sent"] == 2
        assert result["errors"] == 0
        assert result["error_details"] == []
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_custom_operation_type(self, mock_vespa):
        """Test feed_iterable with custom operation type (update).
        
        This verifies that different operation types are passed through correctly.
        For update operations, documents should use the {"assign": value} format
        to properly update fields in Vespa.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        mock_app = Mock()
        hook.vespa_app = mock_app
        
        # Update documents should use "assign" format for field updates
        docs = [{"id": "doc1", "fields": {"title": {"assign": "Updated Title"}}}]
        
        # Call with update operation
        hook.feed_iterable(docs, operation_type="update")
        
        # Verify operation_type was passed correctly
        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["operation_type"] == "update"
        # Verify the document passed through with assign format (already normalized)
        assert call_kwargs["iter"][0] == {"id": "doc1", "fields": {"title": {"assign": "Updated Title"}}}
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_custom_callback(self, mock_vespa):
        """Test feed_iterable with custom callback function.
        
        This verifies that custom callbacks can be used instead of default_callback.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        mock_app = Mock()
        hook.vespa_app = mock_app
        
        # Custom callback function
        custom_callback = Mock()
        
        docs = [{"id": "doc1", "title": "Test"}]
        
        # Call with custom callback
        hook.feed_iterable(docs, callback=custom_callback)
        
        # Verify custom callback was used
        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["callback"] == custom_callback
        assert call_kwargs["callback"] != hook.default_callback
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_with_optional_params(self, mock_vespa):
        """Test feed_iterable with optional feed parameters set.
        
        When optional parameters like max_queue_size are set on the hook,
        they should be passed to the Vespa feed operation.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {
                "max_queue_size": 1000,
                "max_workers": 8,
                "max_connections": 20
            }
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Simulate the optional parameters being set in _configure_from_connection
        hook.max_queue_size = 1000
        hook.max_workers = 8
        hook.max_connections = 20
        
        mock_app = Mock()
        hook.vespa_app = mock_app
        
        docs = [{"id": "doc1", "title": "Test"}]
        
        hook.feed_iterable(docs)
        
        # Verify optional parameters were included
        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["max_queue_size"] == 1000
        assert call_kwargs["max_workers"] == 8
        assert call_kwargs["max_connections"] == 20
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_with_errors(self, mock_vespa):
        """Test feed_iterable when some documents fail.
        
        This test verifies error handling:
        - Errors are collected from the queue after feed_async_iterable runs
        - Error count and details are included in the result
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {}
        }
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        mock_app = Mock()
        hook.vespa_app = mock_app
        
        # Mock feed_async_iterable to simulate errors being added to queue during execution
        def mock_feed_async(**kwargs):
            # Simulate the callback being called with errors during feed operation
            hook.feed_errors_queue.put({"id": "doc1", "status": 400, "reason": {"error": "Bad request"}})
            hook.feed_errors_queue.put({"id": "doc3", "status": 500, "reason": {"error": "Server error"}})
        
        mock_app.feed_async_iterable.side_effect = mock_feed_async
        
        docs = [
            {"id": "doc1", "title": "Bad doc"},
            {"id": "doc2", "title": "Good doc"}, 
            {"id": "doc3", "title": "Another bad doc"}
        ]
        
        result = hook.feed_iterable(docs)
        
        # Verify error summary
        assert result["sent"] == 3
        assert result["errors"] == 2
        assert len(result["error_details"]) == 2
        assert result["error_details"][0]["id"] == "doc1"
        assert result["error_details"][1]["id"] == "doc3"
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_mtls_with_certificates(self, mock_vespa_class):
        """Test mTLS authentication with certificate files.
        
        When both client certificate and key file paths are provided,
        the hook should use mTLS authentication.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {
                "extra__vespa__client_cert_path": "/path/to/client.pem",
                "extra__vespa__client_key_path": "/path/to/client.key",
                "extra__vespa__vespa_cloud_secret_token": "unused_token"  # Should be ignored
            }
        }
        
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Verify Vespa was initialized with certificate authentication
        mock_vespa_class.assert_called_once()
        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["url"] == "https://vespa.example.com:8080"
        assert call_kwargs["cert"] == "/path/to/client.pem"
        assert call_kwargs["key"] == "/path/to/client.key"
        assert call_kwargs["vespa_cloud_secret_token"] == "unused_token"  # Still passed but cert takes precedence
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_token_fallback(self, mock_vespa_class):
        """Test token authentication when certificates are not available.
        
        When certificate files are not provided but a token is available,
        the hook should fall back to token authentication.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc", 
            "namespace": "test",
            "extra": {
                "extra__vespa__vespa_cloud_secret_token": "secret_token_123"
            }
        }
        
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Verify Vespa was initialized with token authentication
        mock_vespa_class.assert_called_once()
        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["url"] == "https://vespa.example.com:8080"
        assert call_kwargs["vespa_cloud_secret_token"] == "secret_token_123"
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_partial_certificates_fallback_to_token(self, mock_vespa_class):
        """Test fallback to token when only partial certificate info is provided.
        
        If only cert OR key is provided (but not both), the hook should
        fall back to token authentication if available.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {
                "extra__vespa__client_cert_path": "/path/to/client.pem",
                # Missing client_key_path
                "extra__vespa__vespa_cloud_secret_token": "fallback_token"
            }
        }
        
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Should fall back to token authentication
        mock_vespa_class.assert_called_once()
        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] is None  # Should be cleared
        assert call_kwargs["key"] is None   # Should be cleared
        assert call_kwargs["vespa_cloud_secret_token"] == "fallback_token"
    
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_no_credentials(self, mock_vespa_class):
        """Test no authentication when neither certificates nor token are provided.
        
        When no authentication credentials are available, the hook should
        still initialize Vespa but without authentication.
        """
        conn_info = {
            "host": "https://vespa.example.com:8080",
            "schema": "doc",
            "namespace": "test",
            "extra": {}  # No authentication credentials
        }
        
        hook = VespaHook.from_resolved_connection(**conn_info)
        
        # Verify Vespa was initialized without authentication
        mock_vespa_class.assert_called_once()
        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["url"] == "https://vespa.example.com:8080"
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None
        assert call_kwargs["vespa_cloud_secret_token"] is None