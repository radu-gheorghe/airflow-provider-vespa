import uuid

import pytest
from unittest.mock import Mock, patch

from airflow_provider_vespa.hooks.vespa import VespaHook, VALID_OPERATION_TYPES


class TestGetField:
    """Tests for the _get_field / _get_int_field helpers."""

    def test_bare_key_preferred(self):
        extra = {"namespace": "bare", "extra__vespa__namespace": "prefixed"}
        assert VespaHook._get_field(extra, "namespace") == "bare"

    def test_prefixed_fallback(self):
        extra = {"extra__vespa__namespace": "prefixed"}
        assert VespaHook._get_field(extra, "namespace") == "prefixed"

    def test_missing_returns_none(self):
        assert VespaHook._get_field({}, "namespace") is None

    def test_int_field_from_string(self):
        assert VespaHook._get_int_field({"max_queue_size": "500"}, "max_queue_size") == 500

    def test_int_field_from_int(self):
        assert VespaHook._get_int_field({"max_queue_size": 500}, "max_queue_size") == 500

    def test_int_field_empty_string(self):
        assert VespaHook._get_int_field({"max_queue_size": ""}, "max_queue_size") is None

    def test_int_field_missing(self):
        assert VespaHook._get_int_field({}, "max_queue_size") is None

    def test_int_field_invalid_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            VespaHook._get_int_field({"max_queue_size": "abc"}, "max_queue_size")


class TestVespaHook:

    @patch('airflow_provider_vespa.hooks.vespa.BaseHook.get_connection')
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_init_default(self, mock_vespa, mock_get_connection):
        mock_conn = Mock()
        mock_conn.host = "https://vespa.example.com:8080"
        mock_conn.schema = "test_schema"
        mock_conn.port = None
        mock_conn.extra_dejson = {"namespace": "default"}
        mock_get_connection.return_value = mock_conn

        hook = VespaHook()

        assert hook.namespace == "default"
        assert hook.schema == "test_schema"

    @patch('airflow_provider_vespa.hooks.vespa.BaseHook.get_connection')
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_init_with_params(self, mock_vespa, mock_get_connection):
        mock_conn = Mock()
        mock_conn.host = "https://vespa.example.com:8080"
        mock_conn.schema = "conn_schema"
        mock_conn.port = None
        mock_conn.extra_dejson = {"namespace": "conn_ns"}
        mock_get_connection.return_value = mock_conn

        hook = VespaHook(
            conn_id="custom_conn",
            namespace="test_ns",
            schema="test_schema",
        )

        assert hook.namespace == "test_ns"
        assert hook.schema == "test_schema"

    @patch('airflow_provider_vespa.hooks.vespa.BaseHook.get_connection')
    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_init_schema_resolution_chain(self, mock_vespa, mock_get_connection):
        """Schema resolves: explicit arg > conn.schema > extra['schema']."""
        mock_conn = Mock()
        mock_conn.host = "https://vespa.example.com:8080"
        mock_conn.schema = ""
        mock_conn.port = None
        mock_conn.extra_dejson = {"schema": "from_extra"}
        mock_get_connection.return_value = mock_conn

        hook = VespaHook()
        assert hook.schema == "from_extra"

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_from_resolved_connection(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="document",
            namespace="prod",
            extra={},
        )

        assert hook.namespace == "prod"
        assert hook.schema == "document"

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_from_resolved_connection_schema_fallback_to_extra(self, mock_vespa):
        """When schema arg is empty, falls back to extra['schema']."""
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="",
            extra={"schema": "doc_from_extra"},
        )
        assert hook.schema == "doc_from_extra"

    # --- _normalise -----------------------------------------------------------

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_already_vespa_format(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        docs = [
            {"id": "doc1", "fields": {"title": "Test 1"}},
            {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}},
        ]
        result = hook._normalise(docs)

        assert len(result) == 2
        assert result[0] == {"id": "doc1", "fields": {"title": "Test 1"}}
        assert result[1] == {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}}

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_raw_fields(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        docs = [
            {"id": "doc1", "title": "Test 1"},
            {"id": "doc2", "title": "Test 2", "content": "Content"},
        ]
        result = hook._normalise(docs)

        assert len(result) == 2
        assert result[0] == {"id": "doc1", "fields": {"title": "Test 1"}}
        assert result[1] == {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}}

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_does_not_mutate_input(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        original = {"id": "doc1", "title": "Test 1"}
        docs = [original]
        hook._normalise(docs)

        assert "id" in original, "Original dict should not be mutated"
        assert original == {"id": "doc1", "title": "Test 1"}

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_generate_missing_ids_for_feed(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        docs = [{"title": "Test 1"}, {"title": "Test 2"}]
        result = hook._normalise(docs, operation_type="feed")

        assert len(result) == 2
        assert result[0]["fields"] == {"title": "Test 1"}
        assert result[1]["fields"] == {"title": "Test 2"}
        uuid.UUID(result[0]["id"])
        uuid.UUID(result[1]["id"])

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_update_requires_id(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        with pytest.raises(ValueError, match="missing required 'id'.*update"):
            hook._normalise([{"title": "no id"}], operation_type="update")

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_delete_requires_id(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        with pytest.raises(ValueError, match="missing required 'id'.*delete"):
            hook._normalise([{"fields": {}}], operation_type="delete")

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_normalise_update_with_vespa_format_requires_id(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        with pytest.raises(ValueError, match="missing required 'id'.*update"):
            hook._normalise(
                [{"fields": {"title": {"assign": "New"}}}],
                operation_type="update",
            )

    # --- default_callback -----------------------------------------------------

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_default_callback_successful_response(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        mock_response = Mock()
        mock_response.is_successful.return_value = True

        hook.default_callback(mock_response, "doc1")
        assert hook.feed_errors_queue.empty()

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_default_callback_failed_response_with_json(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        mock_response = Mock()
        mock_response.is_successful.return_value = False
        mock_response.status_code = 400
        mock_response.get_json.return_value = {"error": "Invalid document format"}

        hook.default_callback(mock_response, "doc1")

        assert not hook.feed_errors_queue.empty()
        error = hook.feed_errors_queue.get()
        assert error["id"] == "doc1"
        assert error["status"] == 400
        assert error["reason"] == {"error": "Invalid document format"}

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_default_callback_failed_response_json_parse_error(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        mock_response = Mock()
        mock_response.is_successful.return_value = False
        mock_response.status_code = 500
        mock_response.get_json.side_effect = ValueError("Invalid JSON")

        hook.default_callback(mock_response, "doc2")

        error = hook.feed_errors_queue.get()
        assert error["id"] == "doc2"
        assert error["status"] == 500
        assert "json_parse_error" in error["reason"]
        assert "Invalid JSON" in error["reason"]["json_parse_error"]
        assert error["reason"]["status_code"] == 500

    # --- feed_iterable --------------------------------------------------------

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_success_default_operation(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock()
        hook.vespa_app = mock_app

        docs = [
            {"id": "doc1", "title": "Test 1"},
            {"id": "doc2", "title": "Test 2"},
        ]
        result = hook.feed_iterable(docs)

        mock_app.feed_async_iterable.assert_called_once()
        call_kwargs = mock_app.feed_async_iterable.call_args[1]

        assert call_kwargs["operation_type"] == "feed"
        assert call_kwargs["schema"] == "doc"
        assert call_kwargs["namespace"] == "test"
        assert call_kwargs["callback"] == hook.default_callback
        assert len(call_kwargs["iter"]) == 2
        assert call_kwargs["iter"][0] == {"id": "doc1", "fields": {"title": "Test 1"}}

        assert result["sent"] == 2
        assert result["errors"] == 0

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_invalid_operation_type(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        with pytest.raises(ValueError, match="Invalid operation_type"):
            hook.feed_iterable([{"id": "1"}], operation_type="upsert")

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_custom_operation_type(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock()
        hook.vespa_app = mock_app

        docs = [{"id": "doc1", "fields": {"title": {"assign": "Updated Title"}}}]
        hook.feed_iterable(docs, operation_type="update")

        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["operation_type"] == "update"
        assert call_kwargs["iter"][0] == {"id": "doc1", "fields": {"title": {"assign": "Updated Title"}}}

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_forwards_kwargs(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock()
        hook.vespa_app = mock_app

        docs = [{"id": "doc1", "fields": {"title": {"assign": "New"}}}]
        hook.feed_iterable(
            docs,
            operation_type="update",
            auto_assign=False,
            create=True,
        )

        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["auto_assign"] is False
        assert call_kwargs["create"] is True

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_custom_callback(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock()
        hook.vespa_app = mock_app

        custom_callback = Mock()
        docs = [{"id": "doc1", "title": "Test"}]
        hook.feed_iterable(docs, callback=custom_callback)

        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["callback"] == custom_callback

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_with_optional_params(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={
                "max_queue_size": 1000,
                "max_workers": 8,
                "max_connections": 20,
            },
        )
        mock_app = Mock()
        hook.vespa_app = mock_app

        docs = [{"id": "doc1", "title": "Test"}]
        hook.feed_iterable(docs)

        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["max_queue_size"] == 1000
        assert call_kwargs["max_workers"] == 8
        assert call_kwargs["max_connections"] == 20

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_feed_iterable_with_errors(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock()
        hook.vespa_app = mock_app

        def mock_feed_async(**kwargs):
            hook.feed_errors_queue.put({"id": "doc1", "status": 400, "reason": {"error": "Bad request"}})
            hook.feed_errors_queue.put({"id": "doc3", "status": 500, "reason": {"error": "Server error"}})

        mock_app.feed_async_iterable.side_effect = mock_feed_async

        docs = [
            {"id": "doc1", "title": "Bad doc"},
            {"id": "doc2", "title": "Good doc"},
            {"id": "doc3", "title": "Another bad doc"},
        ]
        result = hook.feed_iterable(docs)

        assert result["sent"] == 3
        assert result["errors"] == 2
        assert result["error_details"][0]["id"] == "doc1"
        assert result["error_details"][1]["id"] == "doc3"

    # --- authentication -------------------------------------------------------

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_mtls_with_certificates(self, mock_vespa_class):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={
                "client_cert_path": "/path/to/client.pem",
                "client_key_path": "/path/to/client.key",
                "vespa_cloud_secret_token": "unused_token",
            },
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] == "/path/to/client.pem"
        assert call_kwargs["key"] == "/path/to/client.key"
        assert call_kwargs["vespa_cloud_secret_token"] == "unused_token"

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_mtls_with_prefixed_keys(self, mock_vespa_class):
        """Backward compatibility: prefixed keys still work."""
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={
                "extra__vespa__client_cert_path": "/path/to/client.pem",
                "extra__vespa__client_key_path": "/path/to/client.key",
            },
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] == "/path/to/client.pem"
        assert call_kwargs["key"] == "/path/to/client.key"

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_token_fallback(self, mock_vespa_class):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={"vespa_cloud_secret_token": "secret_token_123"},
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["vespa_cloud_secret_token"] == "secret_token_123"
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_partial_certificates_fallback_to_token(self, mock_vespa_class):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={
                "client_cert_path": "/path/to/client.pem",
                "vespa_cloud_secret_token": "fallback_token",
            },
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None
        assert call_kwargs["vespa_cloud_secret_token"] == "fallback_token"

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_authentication_no_credentials(self, mock_vespa_class):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None
        assert call_kwargs["vespa_cloud_secret_token"] is None


class TestVespaHookTestConnection:

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_test_connection_success(self, mock_vespa_class):
        mock_vespa_instance = mock_vespa_class.return_value
        mock_resp = Mock(status_code=200)
        mock_vespa_instance.get_application_status.return_value = mock_resp

        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        ok, msg = hook.test_connection()

        assert ok is True
        assert msg == "Connection successful"
        mock_vespa_instance.get_application_status.assert_called_once()

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_test_connection_failure(self, mock_vespa_class):
        mock_vespa_instance = mock_vespa_class.return_value
        mock_resp = Mock(status_code=503)
        mock_vespa_instance.get_application_status.return_value = mock_resp

        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        ok, msg = hook.test_connection()

        assert ok is False
        assert "503" in msg

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_test_connection_none_response(self, mock_vespa_class):
        mock_vespa_instance = mock_vespa_class.return_value
        mock_vespa_instance.get_application_status.return_value = None

        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        ok, msg = hook.test_connection()

        assert ok is False
        assert "no response" in msg

    @patch('airflow_provider_vespa.hooks.vespa.Vespa')
    def test_test_connection_exception(self, mock_vespa_class):
        mock_vespa_instance = mock_vespa_class.return_value
        mock_vespa_instance.get_application_status.side_effect = Exception(
            "Connection refused"
        )

        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        ok, msg = hook.test_connection()

        assert ok is False
        assert "Connection refused" in msg
