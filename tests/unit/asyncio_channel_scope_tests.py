"""Tests for AsyncioConnection channel-pinning scopes."""

import asyncio
import unittest
from unittest import mock

from pika import exceptions
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.connection import Connection
from tests.base.asyncio_loop import new_pika_asyncio_loop
from tests.unit.channel_scope_tests import broker_close, close_ok, open_ok


def make_connection(loop):
    with mock.patch.object(AsyncioConnection, '_adapter_connect_stream'):
        connection = AsyncioConnection(custom_ioloop=loop)
        connection._set_connection_state(Connection.CONNECTION_OPEN)
        connection._opened = True
    connection.server_capabilities = {
        'publisher_confirms': True,
        'basic.nack': True,
        'consumer_cancel_notify': True,
    }
    connection._adapter_emit_data = mock.Mock()
    return connection


def schedule_open(loop, connection):
    # Open every OPENING channel: parallel tasks can both be opening before
    # either scheduled action runs.
    def deliver():
        for channel in [
                c for c in list(connection._channels.values()) if c.is_opening
        ]:
            open_ok(channel)

    loop.call_soon(deliver)


def schedule_close(loop, connection, channel_number):
    loop.call_soon(lambda: close_ok(connection._channels[channel_number]))


def schedule_broker_close(loop, connection):

    def deliver():
        opening = [c for c in connection._channels.values() if c.is_opening]
        assert len(opening) == 1
        broker_close(opening[0])

    loop.call_soon(deliver)


class AsyncioChannelScopeTests(unittest.TestCase):

    def setUp(self):
        self.loop = new_pika_asyncio_loop()
        self.connection = make_connection(self.loop)

    def tearDown(self):
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()
        del self.connection

    def run_coro(self, coroutine):
        return self.loop.run_until_complete(coroutine)

    def test_open_channel_pins_and_exit_closes_it(self):

        async def scenario():
            scope = self.connection.channel_scope()
            self.assertIsInstance(scope.timeout, type(None))
            schedule_open(self.loop, self.connection)
            async with scope:
                channel = await scope.open_channel()
                self.assertEqual(channel.channel_number, 1)
                self.assertIs(self.connection.channel(), channel)
                schedule_close(self.loop, self.connection, 1)
            self.assertTrue(scope.is_closed)
            self.assertEqual(set(self.connection._channels), set())

        self.run_coro(scenario())

    def test_nested_scopes_bind_separate_channels(self):

        async def scenario():
            outer = self.connection.channel_scope()
            schedule_open(self.loop, self.connection)
            async with outer:
                outer_channel = await outer.open_channel()
                schedule_open(self.loop, self.connection)
                inner = self.connection.channel_scope()
                async with inner:
                    inner_channel = await inner.open_channel()
                    self.assertEqual(inner_channel.channel_number, 2)
                    self.assertIs(self.connection.channel(), inner_channel)
                    schedule_close(self.loop, self.connection, 2)
                self.assertIs(self.connection.channel(), outer_channel)
                schedule_close(self.loop, self.connection, 1)

        self.run_coro(scenario())

    def test_broker_rejected_open_raises_and_releases_number(self):

        async def scenario():
            scope = self.connection.channel_scope()
            schedule_broker_close(self.loop, self.connection)
            with self.assertRaises(exceptions.ChannelClosedByBroker):
                async with scope:
                    await scope.open_channel()
            self.assertEqual(set(self.connection._channels), set())

        self.run_coro(scenario())

    def test_open_timeout_discards_half_open_channel(self):

        async def scenario():
            scope = self.connection.channel_scope(timeout=0.01)
            with self.assertRaises(exceptions.ChannelScopeTimeoutError):
                async with scope:
                    await scope.open_channel()
            self.assertEqual(set(self.connection._channels), set())

        self.run_coro(scenario())

    def test_teardown_timeout_force_discards_pin(self):

        async def scenario():
            scope = self.connection.channel_scope(timeout=0.1)
            schedule_open(self.loop, self.connection)
            with self.assertRaises(exceptions.ChannelScopeTimeoutError):
                async with scope:
                    channel = await scope.open_channel()
                    self.assertEqual(channel.channel_number, 1)
                    # No CloseOk scheduled: teardown must discard locally.
            self.assertEqual(set(self.connection._channels), set())

        self.run_coro(scenario())

    def test_dirty_exit_raises_with_failure_close(self):

        async def scenario():
            scope = self.connection.channel_scope()
            schedule_open(self.loop, self.connection)
            with self.assertRaises(exceptions.ChannelScopeDirtyError) as caught:
                async with scope:
                    channel = await scope.open_channel()
                    channel._consumers['ctag1'] = mock.Mock()
                    schedule_close(self.loop, self.connection, 1)
            self.assertIn('active consumers', caught.exception.details)
            self.assertEqual(set(self.connection._channels), set())

        self.run_coro(scenario())

    def test_rpc_on_outside_channel_inside_scope_is_rejected(self):

        async def scenario():
            schedule_open(self.loop, self.connection)
            outside = self.connection.channel()
            await asyncio.sleep(0)
            self.assertEqual(outside.channel_number, 1)

            scope = self.connection.channel_scope()
            schedule_open(self.loop, self.connection)
            async with scope:
                pinned = await scope.open_channel()
                self.assertEqual(pinned.channel_number, 2)
                with self.assertRaises(exceptions.ChannelPinViolationError):
                    outside.basic_ack(1)
                schedule_close(self.loop, self.connection, 2)
            schedule_close(self.loop, self.connection, 1)
            outside.close()
            await asyncio.sleep(0)

        self.run_coro(scenario())

    def test_parallel_tasks_pin_distinct_channels(self):
        results = {}

        async def worker(ready, peer_ready, name):
            scope = self.connection.channel_scope()
            async with scope:
                schedule_open(self.loop, self.connection)
                channel = await scope.open_channel()
                results[name] = channel.channel_number
                ready.set()
                await peer_ready.wait()
                schedule_close(self.loop, self.connection,
                               channel.channel_number)

        async def scenario():
            first_ready = asyncio.Event()
            second_ready = asyncio.Event()
            await asyncio.gather(worker(first_ready, second_ready, 'a'),
                                 worker(second_ready, first_ready, 'b'))
            self.assertEqual(results, {'a': 1, 'b': 2})
            self.assertEqual(set(self.connection._channels), set())

        self.run_coro(scenario())

    def test_concurrent_enter_from_another_task_is_rejected(self):

        async def scenario():
            scope = self.connection.channel_scope()
            scope.enter()
            try:
                schedule_open(self.loop, self.connection)
                pinned = await scope.open_channel()
                outcome = {}

                async def sibling():
                    try:
                        scope.enter()
                    except exceptions.ChannelScopeBusyError:
                        outcome['busy'] = True

                task = asyncio.ensure_future(sibling())
                await task
                self.assertTrue(outcome.get('busy'))
                schedule_close(self.loop, self.connection,
                               pinned.channel_number)
            finally:
                scope.exit()
                await asyncio.sleep(0)

        self.run_coro(scenario())
