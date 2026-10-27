"""Tests for pika.channel_scope and its hooks in pika.connection.Connection."""

import contextvars
import threading
import unittest
from unittest import mock

from pika import connection, exceptions, frame, spec
from pika._utils import override
from pika.channel_scope import ChannelScope


class ConstructibleConnection(connection.Connection):
    """Adds dummy overrides for Connection's abstract methods and an in-memory timer service."""

    def __init__(self):
        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            super().__init__()
            self._set_connection_state(self.CONNECTION_OPEN)
            self._opened = True
        self.server_capabilities = {
            'publisher_confirms': True,
            'basic.nack': True,
            'consumer_cancel_notify': True,
        }
        self._transport = object()
        self.timers = {}
        self._next_timer = 0

    @override
    def _adapter_connect_stream(self):
        raise NotImplementedError

    @override
    def _adapter_disconnect_stream(self):
        raise NotImplementedError

    @override
    def _adapter_call_later(self, delay, callback):
        self._next_timer += 1
        self.timers[self._next_timer] = callback
        return self._next_timer

    @override
    def _adapter_remove_timeout(self, timeout_id):
        self.timers.pop(timeout_id, None)

    @override
    def _adapter_add_callback_threadsafe(self, callback):
        raise NotImplementedError

    @override
    def _adapter_emit_data(self, data):
        pass

    def _get_write_buffer_size(self):
        return 0


def open_ok(channel_impl):
    """Simulate Channel.OpenOk arriving for an opening channel."""
    channel_impl._on_openok(
        frame.Method(channel_impl.channel_number, spec.Channel.OpenOk()))


def close_ok(channel_impl):
    """Simulate Channel.CloseOk arriving for a closing channel."""
    channel_impl._on_closeok(
        frame.Method(channel_impl.channel_number, spec.Channel.CloseOk()))


def broker_close(channel_impl, reply_code=403, reply_text='refused'):
    """Simulate the broker closing a channel."""
    channel_impl._on_close_from_broker(
        frame.Method(
            channel_impl.channel_number,
            spec.Channel.Close(reply_code=reply_code,
                               reply_text=reply_text,
                               class_id=0,
                               method_id=0)))


def open_channel(connection_impl):
    channel_impl = connection_impl.channel()
    open_ok(channel_impl)
    return channel_impl


def close_scope(scope, channel_impl):
    """Drive the callback-style teardown: initiate close then deliver CloseOk."""
    scope.exit()
    close_ok(channel_impl)


class ChannelScopeTests(unittest.TestCase):

    def setUp(self):
        self.connection = ConstructibleConnection()

    def tearDown(self):
        del self.connection

    def test_without_scope_allocations_and_rpcs_are_unchanged(self):
        first = open_channel(self.connection)
        second = open_channel(self.connection)
        self.assertEqual((first.channel_number, second.channel_number), (1, 2))
        self.connection._send_method(1, spec.Basic.Ack(1))
        self.connection._send_method(2, spec.Basic.Nack(1))

    def test_lazy_binding_pins_first_opened_channel(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            self.assertIs(scope.channel, pinned)
            self.assertEqual(pinned.channel_number, 1)
            # A second allocation returns the pinned channel instead of opening another.
            self.assertIs(self.connection.channel(), pinned)
            close_scope(scope, pinned)
        self.assertTrue(scope.is_closed)
        self.assertEqual(set(self.connection._channels), set())

    def test_rpcs_on_another_channel_inside_scope_are_rejected(self):
        other = open_channel(self.connection)
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            self.assertEqual(pinned.channel_number, 2)
            with self.assertRaises(exceptions.ChannelPinViolationError):
                self.connection._send_method(other.channel_number,
                                             spec.Basic.Ack(1))
            other.close()
            close_ok(other)
            close_scope(scope, pinned)

    def test_pinned_channel_carries_every_listed_rpc(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            number = pinned.channel_number
            pinned.basic_ack(1)
            pinned.basic_nack(1)
            pinned.basic_reject(1)
            pinned.basic_publish('', 'k', b'body')
            pinned.exchange_declare('ex')
            pinned.queue_declare('q')
            pinned.queue_bind('q', 'ex')
            pinned.tx_select()
            pinned.confirm_delivery(mock.Mock())
            close_scope(scope, pinned)
        self.assertNotIn(number, self.connection._channels)

    def test_explicit_other_channel_number_inside_scope_is_rejected(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            with self.assertRaises(exceptions.ChannelPinViolationError):
                self.connection.channel(channel_number=pinned.channel_number +
                                        1)
            close_scope(scope, pinned)

    def test_inner_unbound_scope_inherits_outer_pin(self):
        outer = self.connection.channel_scope()
        with outer:
            pinned = open_channel(self.connection)
            inner = self.connection.channel_scope()
            with inner:
                self.assertIs(inner.channel, pinned)
                self.assertIs(self.connection.channel(), pinned)
            self.assertIs(outer.channel, pinned)
            close_scope(outer, pinned)

    def test_inner_self_bound_channel_is_closed_without_touching_outer(self):
        outer = self.connection.channel_scope()
        with outer:
            outer_channel = open_channel(self.connection)
            inner = self.connection.channel_scope()
            with inner:
                inner_channel = inner.open_channel()
                open_ok(inner_channel)
                self.assertEqual(inner_channel.channel_number, 2)
                self.assertIs(self.connection.channel(), inner_channel)
                with self.assertRaises(exceptions.ChannelPinViolationError):
                    self.connection._send_method(outer_channel.channel_number,
                                                 spec.Basic.Ack(1))
                close_scope(inner, inner_channel)
            self.assertIs(self.connection.channel(), outer_channel)
            self.assertEqual(set(self.connection._channels), {1})
            close_scope(outer, outer_channel)

    def test_parallel_scopes_pin_distinct_channels_in_isolated_contexts(self):
        opened = {}

        def open_in_ctx(name):
            scope = self.connection.channel_scope()
            scope.enter()
            channel_impl = open_channel(self.connection)
            opened[name] = (scope, channel_impl)
            return channel_impl.channel_number

        context_a = contextvars.copy_context()
        context_b = contextvars.copy_context()
        first_number = context_a.run(open_in_ctx, 'a')
        second_number = context_b.run(open_in_ctx, 'b')
        self.assertEqual({first_number, second_number}, {1, 2})
        self.assertEqual(set(self.connection._channels), {1, 2})

        context_a.run(lambda: close_scope(*opened['a']))
        context_b.run(lambda: close_scope(*opened['b']))
        self.assertEqual(set(self.connection._channels), set())

    def test_parallel_scopes_do_not_block_each_other_across_threads(self):
        barrier = threading.Barrier(2)
        results = {}
        errors = []

        def work(name):
            try:
                scope = self.connection.channel_scope()
                with scope:
                    channel_impl = open_channel(self.connection)
                    results[name] = channel_impl.channel_number
                    barrier.wait(timeout=5)
                    self.assertIs(scope.channel.channel_number,
                                  channel_impl.channel_number)
                    close_scope(scope, channel_impl)
            except BaseException as exc:  # report any failure to the test
                errors.append(exc)

        first = threading.Thread(target=work, args=('a',))
        second = threading.Thread(target=work, args=('b',))
        first.start()
        second.start()
        first.join(10)
        second.join(10)
        self.assertEqual(errors, [])
        self.assertEqual(results, {'a': 1, 'b': 2})

    def test_scope_entered_twice_is_rejected(self):
        scope = self.connection.channel_scope()
        scope.enter()
        try:
            with self.assertRaises(exceptions.ChannelScopeBusyError):
                scope.enter()
        finally:
            scope.exit()

    def test_scope_entered_concurrently_is_rejected(self):
        scope = self.connection.channel_scope()
        scope.enter()
        try:
            error = []

            def enter_again():
                try:
                    scope.enter()
                except exceptions.ChannelScopeBusyError as exc:
                    error.append(exc)

            thread = threading.Thread(target=enter_again)
            thread.start()
            thread.join(5)
            self.assertEqual(len(error), 1)
        finally:
            scope.exit()

    def test_failed_open_does_not_bind_and_releases_channel(self):
        scope = self.connection.channel_scope()
        with scope:
            candidate = self.connection.channel()
            broker_close(candidate)
            self.assertIsNone(scope.channel)
            self.assertEqual(set(self.connection._channels), set())
            with self.assertRaises(exceptions.ChannelClosedByBroker):
                self.connection.channel()

    def test_pinned_channel_dying_fails_scope_without_silent_replace(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            broker_close(pinned)
            with self.assertRaises(exceptions.ChannelClosedByBroker):
                self.connection.channel()
            with self.assertRaises(exceptions.ChannelClosedByBroker):
                self.connection._send_method(pinned.channel_number,
                                             spec.Basic.Ack(1))

    def test_cancel_closes_pin_and_releases_number(self):
        scope = self.connection.channel_scope()
        scope.enter()
        try:
            pinned = open_channel(self.connection)
            scope.cancel()
            self.assertTrue(pinned.is_closing)
            close_ok(pinned)
            self.assertTrue(scope.is_closed)
            self.assertEqual(set(self.connection._channels), set())
        finally:
            if not scope.is_closed:
                scope.exit()

    def test_dirty_exit_with_consumer_uses_failure_path(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            pinned._consumers['ctag1'] = mock.Mock()
            scope.exit()
            close_ok(pinned)
        self.assertIsInstance(scope._failed_reason,
                              exceptions.ChannelScopeDirtyError)
        self.assertEqual(set(self.connection._channels), set())
        self.assertIn('active consumers', scope._failed_reason.details)

    def test_open_transaction_is_unfinished_work(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            pinned.callbacks.process(
                pinned.channel_number, spec.Tx.SelectOk, pinned,
                frame.Method(pinned.channel_number, spec.Tx.SelectOk()))
            scope.exit()
            close_ok(pinned)
        self.assertIsInstance(scope._failed_reason,
                              exceptions.ChannelScopeDirtyError)
        self.assertIn('open transaction', scope._failed_reason.details)

    def test_unconfirmed_publish_is_unfinished_work(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            number = pinned.channel_number
            self.connection._send_method(number, spec.Confirm.Select())
            self.connection._send_method(number, spec.Basic.Publish(0, '', 'k'),
                                         (spec.BasicProperties(), b'a'))
            self.connection._send_method(number, spec.Basic.Publish(0, '', 'k'),
                                         (spec.BasicProperties(), b'b'))
            # Acking delivery tag 1 (non-multiple) leaves tag 2 outstanding.
            pinned.callbacks.process(
                number, spec.Basic.Ack, pinned,
                frame.Method(number, spec.Basic.Ack(1, False)))
            scope.exit()
            close_ok(pinned)
        self.assertIsInstance(scope._failed_reason,
                              exceptions.ChannelScopeDirtyError)
        self.assertIn('unconfirmed publisher confirms',
                      scope._failed_reason.details)

    def test_confirmed_publishes_allow_clean_exit(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            number = pinned.channel_number
            self.connection._send_method(number, spec.Confirm.Select())
            self.connection._send_method(number, spec.Basic.Publish(0, '', 'k'),
                                         (spec.BasicProperties(), b'a'))
            pinned.callbacks.process(
                number, spec.Basic.Ack, pinned,
                frame.Method(number, spec.Basic.Ack(1, True)))
            scope.exit()
            close_ok(pinned)
        self.assertIsNone(scope._failed_reason)

    def test_clean_exit_closes_pin_normally(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            scope.exit()
            self.assertTrue(pinned.is_closing)
            close_ok(pinned)
        self.assertTrue(scope.is_closed)
        self.assertEqual(set(self.connection._channels), set())
        self.assertNotIn(scope, self.connection._channel_scopes)

    def test_teardown_grace_timeout_discards_pin_locally(self):
        scope = self.connection.channel_scope()
        with scope:
            open_channel(self.connection)
            scope.exit()
            scope.force_discard_pin()
        self.assertTrue(scope.is_closed)
        self.assertIsInstance(scope._failed_reason,
                              exceptions.ChannelScopeTimeoutError)
        self.assertEqual(set(self.connection._channels), set())

    def test_channel_zero_frames_are_always_allowed(self):
        scope = self.connection.channel_scope()
        with scope:
            pinned = open_channel(self.connection)
            self.connection._send_method(0,
                                         spec.Connection.Close(200, 'ok', 0, 0))
            close_scope(scope, pinned)

    def test_on_close_callback_fires_on_teardown(self):
        closed = []
        scope = self.connection.channel_scope()
        scope.add_on_close_callback(lambda s, reason: closed.append(reason))
        with scope:
            pinned = open_channel(self.connection)
            close_scope(scope, pinned)
        self.assertEqual(len(closed), 1)

    def test_bind_callback_restores_scope_for_continuation(self):
        scope = self.connection.channel_scope()
        seen = []
        with scope:
            pinned = open_channel(self.connection)
            continuation = scope.bind_callback(lambda: seen.append(
                ChannelScope._innermost_for(self.connection)))
            continuation()
            self.assertIs(seen[0], scope)
            close_scope(scope, pinned)
