import pytest
from unittest.mock import Mock, patch, AsyncMock

from airflow_provider_vespa.triggers.vespa_feed_trigger import VespaFeedTrigger


class TestVespaFeedTrigger:

    def test_init(self):
        docs = [{"id": "1", "title": "Test"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc"}

        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, operation_type="feed")

        assert trigger.docs == docs
        assert trigger.conn_info == conn_info
        assert trigger.operation_type == "feed"
        assert trigger.feed_kwargs == {}

    def test_init_with_feed_kwargs(self):
        trigger = VespaFeedTrigger(
            docs=[{"id": "1"}],
            conn_info={"host": "h"},
            operation_type="update",
            feed_kwargs={"auto_assign": False, "create": True},
        )

        assert trigger.feed_kwargs == {"auto_assign": False, "create": True}

    def test_serialize(self):
        docs = [{"id": "1"}]
        conn_info = {"host": "test"}
        kw = {"auto_assign": False}

        trigger = VespaFeedTrigger(
            docs=docs, conn_info=conn_info, feed_kwargs=kw,
        )
        class_path, data = trigger.serialize()

        assert class_path == "airflow_provider_vespa.triggers.vespa_feed_trigger.VespaFeedTrigger"
        assert data["docs"] == docs
        assert data["conn_info"] == conn_info
        assert data["operation_type"] == "feed"
        assert data["feed_kwargs"] == kw

    @pytest.mark.asyncio
    async def test_run_success(self):
        docs = [{"id": "1", "content": "test"}]
        conn_info = {
            "host": "https://vespa.test:8080",
            "namespace": "test",
            "schema": "doc",
            "extra": {},
        }
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)

        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            assert len(events) == 1
            assert events[0].payload["success"] is True
            assert events[0].payload["sent"] == 1

    @pytest.mark.asyncio
    async def test_run_forwards_feed_kwargs(self):
        docs = [{"id": "1", "fields": {"x": {"assign": 1}}}]
        conn_info = {
            "host": "https://vespa.test:8080",
            "namespace": "test",
            "schema": "doc",
            "extra": {},
        }
        trigger = VespaFeedTrigger(
            docs=docs,
            conn_info=conn_info,
            operation_type="update",
            feed_kwargs={"auto_assign": False, "create": True},
        )

        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(
                docs, operation_type="update", auto_assign=False, create=True,
            )

    @pytest.mark.asyncio
    async def test_run_hook_creation_failure(self):
        trigger = VespaFeedTrigger(
            docs=[{"id": "1"}],
            conn_info={"host": "invalid", "namespace": "test", "schema": "doc", "extra": {}},
        )

        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook_class.from_resolved_connection.side_effect = ValueError("Invalid host URL")

            events = []
            async for event in trigger.run():
                events.append(event)

            assert len(events) == 1
            assert events[0].payload["success"] is False
            assert "ValueError: Invalid host URL" in events[0].payload["errors"][0]["error"]

    @pytest.mark.asyncio
    async def test_run_feed_operation_failure(self):
        trigger = VespaFeedTrigger(
            docs=[{"id": "1"}],
            conn_info={"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}},
        )

        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.side_effect = ConnectionError("Unable to connect")
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            assert events[0].payload["success"] is False
            assert "ConnectionError" in events[0].payload["errors"][0]["error"]

    @pytest.mark.asyncio
    async def test_run_with_feed_errors(self):
        docs = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)

        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {
                "sent": 3,
                "errors": 2,
                "error_details": [
                    {"id": "2", "status": 400, "reason": {"error": "Invalid"}},
                    {"id": "3", "status": 500, "reason": {"error": "Server error"}},
                ],
            }
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            assert events[0].payload["success"] is False
            assert events[0].payload["sent"] == 3
            assert len(events[0].payload["errors"]) == 2

    @pytest.mark.asyncio
    async def test_run_update_operation(self):
        docs = [{"id": "1", "fields": {"title": {"assign": "Updated"}}}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, operation_type="update")

        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(
                docs, operation_type="update",
            )
            assert events[0].payload["success"] is True

    @pytest.mark.asyncio
    async def test_run_delete_operation(self):
        docs = [{"id": "1"}, {"id": "2"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, operation_type="delete")

        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 2, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(
                docs, operation_type="delete",
            )
            assert events[0].payload["success"] is True

    @pytest.mark.asyncio
    async def test_run_default_feed_operation(self):
        docs = [{"id": "1", "content": "test"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)

        with patch('airflow_provider_vespa.hooks.vespa.VespaHook') as mock_hook_class:
            mock_hook = Mock()
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(
                docs, operation_type="feed",
            )
            assert events[0].payload["success"] is True
