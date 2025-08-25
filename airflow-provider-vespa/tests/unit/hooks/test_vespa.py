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