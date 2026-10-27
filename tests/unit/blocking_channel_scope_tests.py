"""Tests for BlockingConnection channel-pinning scopes."""

import contextvars
import unittest
from collections import deque
from unittest import mock

from pika import exceptions
from pika._utils import override
from pika.adapters import blocking_connection
from pika.connection import Connection
from tests.unit.channel_scope_tests import broker_close, close_ok, open_ok


class FakeIOLoop:
    """Minimal SelectConnection IOLoop stand-in: queued actions on poll, instant timers."""

    def __init__(self):
        self.pending = deque()
        self.timers = {}
        self._next_timer = 0

    def poll(self):
        if self.pending:
            self.pending.popleft()()

    def process_timeouts(self):
        callbacks = list(self.timers.values())
        self.timers.clear()
        for callback in callbacks:
            callback()

    def add_timeout(self, delay, callback):
        self._next_timer += 1
        self.timers[self._next_timer] = callback
        return self._next_timer

    def remove_timeout(self, timeout_id):
        self.timers.pop(timeout_id, None)

    def close(self):
        pass


class FakeImplConnection(Connection):
    """Core Connection driven by FakeIOLoop, standing in for SelectConnection."""

    def __init__(self):
        with mock.patch.object(FakeImplConnection, '_adapter_connect_stream'):
            super().__init__()
            self._set_connection_state(self.CONNECTION_OPEN)
            self._opened = True
        self.server_capabilities = {
            'publisher_confirms': True,
            'basic.nack': True,
            'consumer_cancel_notify': True,
        }
        self.fake_ioloop = FakeIOLoop()
        self._transport = object()

    @property
    def ioloop(self):
        return self.fake_ioloop

    @override
    def _adapter_connect_stream(self):
        raise NotImplementedError

    @override
    def _adapter_disconnect_stream(self):
        raise NotImplementedError

    @override
    def _adapter_call_later(self, delay, callback):
        return self.fake_ioloop.add_timeout(delay, callback)

    @override
    def _adapter_remove_timeout(self, timeout_id):
        self.fake_ioloop.remove_timeout(timeout_id)

    @override
    def _adapter_add_callback_threadsafe(self, callback):
        raise NotImplementedError

    @override
    def _adapter_emit_data(self, data):
        pass

    def _get_write_buffer_size(self):
        return 0


def queue_open(impl):
    """Drive Channel.OpenOk for the channel currently opening on the impl."""

    def deliver():
        opening = [c for c in impl._channels.values() if c.is_opening]
        assert len(opening) == 1
        open_ok(opening[0])

    impl.fake_ioloop.pending.append(deliver)


def queue_close(impl, channel_number):
    impl.fake_ioloop.pending.append(
        lambda: close_ok(impl._channels[channel_number]))


def queue_broker_close(impl):

    def deliver():
        opening = [c for c in impl._channels.values() if c.is_opening]
        assert len(opening) == 1
        broker_close(opening[0])

    impl.fake_ioloop.pending.append(deliver)


class BlockingChannelScopeTests(unittest.TestCase):

    def setUp(self):
        self.impl = FakeImplConnection()
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=self.impl):
            self.connection = blocking_connection.BlockingConnection(None)

    def tearDown(self):
        del self.connection
        del self.impl

    def test_open_channel_pins_and_teardown_closes_it(self):
        scope = self.connection.channel_scope()
        queue_open(self.impl)
        with scope:
            channel = scope.open_channel()
            self.assertEqual(channel.channel_number, 1)
            self.assertIs(scope.channel, channel)
            queue_close(self.impl, 1)
        self.assertTrue(scope.is_closed)
        self.assertEqual(set(self.impl._channels), set())

    def test_second_allocation_returns_same_proxy(self):
        scope = self.connection.channel_scope()
        queue_open(self.impl)
        with scope:
            channel = scope.open_channel()
            self.assertIs(self.connection.channel(), channel)
            queue_close(self.impl, 1)

    def test_nested_scopes_open_and_close_separate_channels(self):
        outer = self.connection.channel_scope()
        queue_open(self.impl)
        with outer:
            outer_channel = outer.open_channel()
            self.assertEqual(outer_channel.channel_number, 1)
            queue_open(self.impl)
            inner = self.connection.channel_scope()
            with inner:
                inner_channel = inner.open_channel()
                self.assertEqual(inner_channel.channel_number, 2)
                self.assertIs(self.connection.channel(), inner_channel)
                queue_close(self.impl, 2)
            self.assertEqual(set(self.impl._channels), {1})
            self.assertIs(self.connection.channel(), outer_channel)
            queue_close(self.impl, 1)
        self.assertEqual(set(self.impl._channels), set())

    def test_open_timeout_discards_half_open_channel(self):
        scope = self.connection.channel_scope(timeout=0.001)
        with self.assertRaises(exceptions.ChannelScopeTimeoutError), scope:
            scope.open_channel()
        self.assertEqual(set(self.impl._channels), set())
        self.assertTrue(scope.is_closed)

    def test_broker_rejected_open_surfaces_and_releases_number(self):
        scope = self.connection.channel_scope()
        queue_broker_close(self.impl)
        with self.assertRaises(exceptions.ChannelClosedByBroker), scope:
            scope.open_channel()
        self.assertEqual(set(self.impl._channels), set())

    def test_dirty_exit_raises_and_closes_on_failure_path(self):
        scope = self.connection.channel_scope()
        queue_open(self.impl)
        with self.assertRaises(
                exceptions.ChannelScopeDirtyError) as caught, scope:
            channel = scope.open_channel()
            channel._impl._consumers['ctag1'] = mock.Mock()
            queue_close(self.impl, 1)
        self.assertIn('active consumers', caught.exception.details)
        self.assertEqual(set(self.impl._channels), set())

    def test_cancel_raises_scope_error_and_releases_channel(self):
        scope = self.connection.channel_scope()
        queue_open(self.impl)
        with self.assertRaises(exceptions.ChannelScopeError), scope:
            scope.open_channel()
            scope.cancel()
            queue_close(self.impl, 1)
        self.assertEqual(set(self.impl._channels), set())

    def test_teardown_timeout_force_discards_pin(self):
        scope = self.connection.channel_scope(timeout=0.001)
        queue_open(self.impl)
        with self.assertRaises(exceptions.ChannelScopeTimeoutError), scope:
            channel = scope.open_channel()
            self.assertEqual(channel.channel_number, 1)
            # No CloseOk queued: the teardown grace forces the discard.
        self.assertEqual(set(self.impl._channels), set())

    def test_rpc_on_outside_channel_inside_scope_is_rejected(self):
        outside = open_channel_via(self.connection, self.impl)
        scope = self.connection.channel_scope()
        queue_open(self.impl)
        with scope:
            pinned = scope.open_channel()
            self.assertEqual(pinned.channel_number, 2)
            with self.assertRaises(exceptions.ChannelPinViolationError):
                outside.basic_ack(1)
            queue_close(self.impl, 2)
        queue_close(self.impl, 1)
        outside.close()

    def test_unrelated_context_keeps_using_other_channels(self):
        # Snapshot the scope-free context before entering; work in that context must
        # allocate and use another channel while the scope keeps its pin.
        scope_free_context = contextvars.copy_context()
        scope = self.connection.channel_scope()
        queue_open(self.impl)
        with scope:
            pinned = scope.open_channel()
            self.assertEqual(pinned.channel_number, 1)

            def unrelated_work():
                queue_open(self.impl)
                other = self.connection.channel()
                self.assertEqual(other.channel_number, 2)
                other.basic_ack(1)
                queue_close(self.impl, 2)
                other.close()

            scope_free_context.run(unrelated_work)
            self.assertEqual(set(self.impl._channels), {1})
            queue_close(self.impl, 1)


def open_channel_via(blocking_connection_impl, impl):
    """Open a normal BlockingChannel outside any scope, driving its OpenOk."""
    queue_open(impl)
    return blocking_connection_impl.channel()
