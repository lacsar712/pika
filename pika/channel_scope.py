"""Channel pinning scopes.

A scope pins a stretch of work on a connection to the first channel successfully opened inside it.
Allocation (`Connection.channel`) and every channel RPC egress point (`Connection._send_method`)
consult the ambient scope stack, so a pinned channel can neither be replaced silently nor bypassed
by an RPC on another channel. The scope machinery lives entirely in the connection/channel common
path; adapters only add their native waiting and teardown semantics.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import threading
import time
from typing import TYPE_CHECKING, Any, Callable

from pika import exceptions, spec

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self

    from pika.channel import Channel
    from pika.connection import Connection

LOGGER = logging.getLogger(__name__)

# AMQP CHANNEL_ERROR; sent when a scope's failure path discards the pinned channel.
_FAILURE_REPLY_CODE = 504
_FAILURE_REPLY_TEXT = 'channel scope closed with unfinished work'
_TIMEOUT_REPLY_TEXT = 'channel scope timed out'

_scope_stack: contextvars.ContextVar[tuple[ChannelScope,
                                           ...]] = (contextvars.ContextVar(
                                               'pika_channel_scope_stack',
                                               default=()))


def _current_owner() -> tuple[int, Any]:
    """Identify the owning execution context: the thread and, when set, its asyncio task."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return (threading.get_ident(), task)


class ChannelScope:
    """Pins work inside the scope to one channel of one connection.

    Enter with a context manager (``with connection.channel_scope() as scope:`` for
    ``BlockingConnection`` and the callback adapters, ``async with`` for ``AsyncioConnection``).
    The scope is lazy: it does not open a channel until ``open_channel()`` or
    ``Connection.channel()`` is called inside it. The first channel that finishes opening becomes
    the pinned channel and is returned by every subsequent channel allocation in the scope; every
    channel RPC in the scope must use it.

    On exit the pinned channel is always closed and removed from the connection; it is never
    returned for reuse. If consumers, a transaction or unconfirmed publisher-confirm deliveries
    are still open at exit, the channel is closed on the failure path and
    [`ChannelScopeDirtyError`][pika.exceptions.ChannelScopeDirtyError] is raised (blocking
    adapters).

    An inner scope without its own pinned channel shares the enclosing scope's pin. Calling
    ``open_channel()`` on the inner scope binds it to its own channel; closing the inner scope
    leaves the outer pin in place.

    A scope object serves one active region at a time. Re-entering it before exit, or entering it
    concurrently from another task or thread, raises
    [`ChannelScopeBusyError`][pika.exceptions.ChannelScopeBusyError].
    """

    # Upper bound on waiting for the broker's Channel.CloseOk at teardown. After this the scope
    # meta-closes the channel locally so its number cannot stay occupied forever.
    TEARDOWN_GRACE: float = 10.0

    READY = 0
    ACTIVE = 1
    CLOSED = 2

    def __init__(self,
                 connection: Connection,
                 timeout: float | None = None) -> None:
        """
        :param connection: connection whose channel allocations and RPCs the scope governs
        :param timeout: total budget in seconds from enter to the end of teardown; None waits
            without a scope-level deadline
        """
        self._connection = connection
        self._timeout = timeout
        self._state: int = self.READY
        self._owner: tuple[int, Any] | None = None
        self._lifecycle_lock = threading.Lock()
        self._stack_token: contextvars.Token[Any] | None = None

        self._pin: Channel | None = None
        # Numbers of channels allocated in this scope whose Channel.OpenOk has not arrived.
        self._candidates: set[int] = set()
        # Set by open_channel() and consumed once by Connection.channel().
        self._force_bind = False
        self._exiting = False

        self._failed_reason: Exception | None = None
        self._deadline: float | None = None
        self._grace_timer: object | None = None
        self._close_listeners: list[Callable[[ChannelScope, Exception],
                                             None]] = []

        # Detectable unfinished work at exit.
        self._tx_open = False
        self._confirm_mode = False
        self._publish_seq = 0
        self._unconfirmed: set[int] = set()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def enter(self) -> ChannelScope:
        """Activate the scope and push it onto the ambient scope stack.

        :raises exceptions.ChannelScopeBusyError: if the scope was already entered or closed
        """
        with self._lifecycle_lock:
            if self._state != self.READY:
                raise exceptions.ChannelScopeBusyError(
                    'channel scope cannot be entered twice; open a new scope '
                    'instead')
            self._owner = _current_owner()
            if self._timeout is not None:
                self._deadline = time.monotonic() + self._timeout
            self._state = self.ACTIVE
            self._stack_token = _scope_stack.set((*_scope_stack.get(), self))
        return self

    def exit(self) -> None:
        """Tear the scope down.

        Initiates the pinned channel's close (failure path when the scope is dirty or failed) and
        bounds the wait for Channel.CloseOk. Callback adapters complete asynchronously; blocking
        and asyncio adapters drive I/O until completion in their context-manager exit.
        """
        if self._state == self.CLOSED:
            return
        self._check_owner()
        if self._exiting:
            return
        self._exiting = True

        ch = self._pin
        if ch is None or ch.is_closed:
            reason = (self._failed_reason or
                      exceptions.ChannelClosedByClient(200, 'Normal shutdown'))
            self._complete_teardown(reason)
            return

        if ch.is_closing:
            # The user closed the pinned channel themselves; just bound the wait.
            self._schedule_grace(ch)
            return

        failure = self._failed_reason is not None
        if not failure:
            details = self._dirty_details()
            if details:
                self._failed_reason = exceptions.ChannelScopeDirtyError(
                    *details)
                failure = True

        reply_code, reply_text = self._close_arguments(failure)
        LOGGER.debug('Closing pinned channel %s on scope exit (%s, %r)',
                     ch.channel_number, reply_code, reply_text)
        ch.close(reply_code=reply_code, reply_text=reply_text)
        self._schedule_grace(ch)

    def cancel(self) -> None:
        """Cancel the scope: fail it and tear the pinned channel down immediately.

        Subsequent RPCs and channel allocations in the scope raise; the channel is closed on the
        failure path and its number released, bounded by the teardown grace period.
        """
        if self._state == self.READY:
            raise exceptions.ChannelScopeBusyError(
                'cannot cancel a channel scope that was never entered')
        if self._state == self.CLOSED:
            return
        self._check_owner()
        if self._failed_reason is None:
            self._failed_reason = exceptions.ChannelScopeError(
                'channel scope was cancelled')
        self.exit()

    def __enter__(self) -> Self:
        self.enter()
        return self

    def __exit__(self, exc_type: type[BaseException] | None,
                 exc: BaseException | None,
                 _traceback: TracebackType | None) -> None:
        self.exit()

    # ------------------------------------------------------------------
    # Public state
    # ------------------------------------------------------------------

    @property
    def is_closed(self) -> bool:
        """True once teardown has completed and the ambient pin is gone."""
        return self._state == self.CLOSED

    @property
    def is_active(self) -> bool:
        """True between enter and completed teardown."""
        return self._state == self.ACTIVE

    @property
    def timeout(self) -> float | None:
        """Configured total scope timeout in seconds, or None."""
        return self._timeout

    @property
    def remaining(self) -> float | None:
        """Seconds of the total timeout left (0 once expired), or None without a timeout."""
        if self._deadline is None:
            return None
        return max(0.0, self._deadline - time.monotonic())

    @property
    def channel(self) -> Channel | None:
        """The channel the scope is effectively pinned to.

        Its own channel once bound, otherwise the nearest enclosing scope's pinned channel, or
        None before any channel has been bound.
        """
        if self._pin is not None:
            return self._pin
        stack = _scope_stack.get()
        for index in range(len(stack) - 1, -1, -1):
            candidate = stack[index]
            if candidate is self:
                continue
            if (candidate._connection is self._connection and
                    candidate._pin is not None and
                    candidate._state != self.CLOSED):
                return candidate._pin
        return None

    def add_on_close_callback(
            self,
            callback: Callable[[ChannelScope, Exception], None]) -> None:
        """Register a one-shot callback invoked with the scope and its closing reason.

        Fires immediately when the scope is already closed.

        :param callback: callback(scope, reason)
        """
        if self._state == self.CLOSED:
            callback(
                self, self._failed_reason or
                exceptions.ChannelClosedByClient(200, 'Normal shutdown'))
        else:
            self._close_listeners.append(callback)

    def bind_callback(self, callback: Callable[..., Any]) -> Callable[..., Any]:
        """Bind a callback-style continuation to this scope.

        The callback adapters finish an RPC on a later I/O-loop tick, where the ambient stack of
        the tick that scheduled it is gone. The wrapper puts the scope back on the stack for the
        duration of the callback without changing the scope's state or owner, so it is not a
        second enter.

        :param callback: continuation to run with the scope ambient
        :returns: wrapped callback
        """

        @functools.wraps(callback)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            token = _scope_stack.set((*_scope_stack.get(), self))
            try:
                return callback(*args, **kwargs)
            finally:
                _scope_stack.reset(token)

        return wrapper

    def open_channel(
            self,
            channel_number: int | None = None,
            on_open_callback: Callable[[Channel], Any] | None = None
    ) -> Channel:
        """Open a channel bound to this scope, even when an enclosing scope is already pinned.

        Without ``open_channel``, allocations in an unbound inner scope share the enclosing pin.
        A scope can bind at most one channel; calling this twice raises
        [`ChannelPinViolationError`][pika.exceptions.ChannelPinViolationError].

        :param channel_number: optional specific channel number, allocated by the connection when
            omitted
        :param on_open_callback: callback invoked with the open channel
        :returns: the opening channel; callers that need the usable channel wait on the callback
            or use their adapter's blocking/async wrapper
        """
        self._check_owner()
        self._raise_if_failed()
        if self._state != self.ACTIVE:
            raise exceptions.ChannelScopeError(
                'open_channel requires an active channel scope')
        if self._pin is not None:
            raise exceptions.ChannelPinViolationError(
                f'scope is already pinned to channel '
                f'{self._pin.channel_number}')
        self._force_bind = True
        try:
            return self._connection.channel(channel_number=channel_number,
                                            on_open_callback=on_open_callback)
        finally:
            self._force_bind = False

    # ------------------------------------------------------------------
    # Hooks used by Connection.channel / Connection._send_method
    # ------------------------------------------------------------------

    @classmethod
    def _innermost_for(cls, connection: Connection) -> ChannelScope | None:
        """Innermost ambient scope belonging to connection, if any."""
        for scope in reversed(_scope_stack.get()):
            if (scope._connection is connection and scope._state != cls.CLOSED):
                return scope
        return None

    def _consume_force_bind(self) -> bool:
        """Read and clear the one-shot force-bind mark left by open_channel."""
        forced = self._force_bind
        self._force_bind = False
        return forced

    def effective_channel(self) -> Channel | None:
        """The channel allocations in this scope must currently use, if any."""
        return self.channel

    def _wrap_open_callback(
        self, user_callback: Callable[[Channel], Any] | None
    ) -> Callable[[Channel], None]:
        """Bind the channel to the scope when its OpenOk arrives, then run the user callback."""

        def _on_open(channel: Channel) -> None:
            self._on_candidate_opened(channel)
            if user_callback is not None:
                user_callback(channel)

        return _on_open

    def _register_candidate(self, channel: Channel) -> None:
        """Remember an opening channel allocated under this scope."""
        self._candidates.add(channel.channel_number)
        channel.add_on_close_callback(self._on_channel_closed)

    def _raise_if_failed(self) -> None:
        if self._failed_reason is not None:
            raise self._failed_reason

    @classmethod
    def _enforce_egress(cls, connection: Connection, channel_number: int,
                        method: Any) -> None:
        """Reject frames that escape the effective pinned channel of the ambient scopes."""
        if channel_number == 0:
            return
        stack = [
            scope for scope in _scope_stack.get()
            if scope._connection is connection and scope._state != cls.CLOSED
        ]
        if not stack:
            return

        # Only the innermost scope's own pin is usable; an unbound inner scope
        # inherits the nearest enclosing pin. An outer pin is not reachable once
        # the inner scope binds its own channel.
        innermost = stack[-1]
        allowed = set(innermost._candidates)
        egress_scope = innermost
        if innermost._pin is not None:
            allowed.add(innermost._pin.channel_number)
        else:
            for outer in reversed(stack[:-1]):
                if outer._pin is not None:
                    allowed.add(outer._pin.channel_number)
                    egress_scope = outer
                    break

        if channel_number in allowed:
            egress_scope._note_egress(channel_number, method)
            return

        reason = next((scope._failed_reason
                       for scope in reversed(stack)
                       if scope._failed_reason is not None), None)
        if reason is not None:
            raise reason
        raise exceptions.ChannelPinViolationError(
            f'channel {channel_number} is outside the pinned channel; the '
            'active scope requires all RPCs to use its pinned channel')

    # ------------------------------------------------------------------
    # Teardown hooks used by the blocking and asyncio adapters
    # ------------------------------------------------------------------

    def deactivate(self) -> None:
        """Remove the scope from the caller's ambient stack. Safe to call repeatedly from any
        context; only the entering context actually resets its stack.
        """
        token = self._stack_token
        if token is None:
            return
        self._stack_token = None
        if self in _scope_stack.get():
            try:
                _scope_stack.reset(token)
            except ValueError:
                # The completion callback ran in a different context (asyncio
                # loop context vs. user task); the task's own exit pops there.
                pass

    def wait_budget(self) -> float | None:
        """Remaining total timeout for an adapter wait, or None to wait without a deadline."""
        return self.remaining

    def mark_timeout(self) -> None:
        """Record a scope timeout as the failure reason unless one is already recorded."""
        if self._failed_reason is None:
            self._failed_reason = exceptions.ChannelScopeTimeoutError(
                'channel scope timed out')

    def discard_channel(self, channel: Channel) -> None:
        """Release an opening or pinned channel locally without further broker exchange.

        Used when an open or teardown wait expires, so a half-pinned channel cannot keep its
        number occupied.
        """
        if not channel.is_closed:
            reason = (
                self._failed_reason or
                exceptions.ChannelScopeTimeoutError('channel scope timed out'))
            channel._on_close_meta(reason)

    def force_discard_pin(self) -> None:
        """Discard the still-open pinned channel after a teardown wait expired."""
        channel = self._pin
        if channel is None:
            return
        self.mark_timeout()
        LOGGER.warning(
            'Scope teardown grace expired for channel %s; discarding it '
            'locally', channel.channel_number)
        self.discard_channel(channel)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _check_owner(self) -> None:
        if _current_owner() != self._owner:
            raise exceptions.ChannelScopeBusyError(
                'channel scope is active in another task or thread')

    def _on_candidate_opened(self, channel: Channel) -> None:
        self._candidates.discard(channel.channel_number)
        if self._pin is None:
            self._pin = channel
            self._attach_state_trackers(channel)

    def _on_channel_closed(self, channel: Channel,
                           reason: Exception) -> None:
        """Channel lifecycle callback for candidates and the pinned channel."""
        self._candidates.discard(channel.channel_number)
        if self._pin is channel:
            self._pin = None
            if self._exiting:
                self._complete_teardown(reason)
            else:
                # Keep the failed scope ambient until exit so further RPCs and
                # allocations raise instead of silently getting a replacement.
                if self._failed_reason is None:
                    self._failed_reason = reason
                self._cancel_grace_timer()
        elif not self._exiting and self._failed_reason is None:
            # A candidate died before Channel.OpenOk could bind it.
            self._failed_reason = reason

    def _attach_state_trackers(self, channel: Channel) -> None:
        callbacks = channel.callbacks
        number = channel.channel_number
        callbacks.add(number, spec.Tx.SelectOk, self._on_tx_selected, False)
        callbacks.add(number, spec.Tx.CommitOk, self._on_tx_finished, False)
        callbacks.add(number, spec.Tx.RollbackOk, self._on_tx_finished, False)
        callbacks.add(number, spec.Basic.Ack, self._on_publish_ack_nack, False)
        callbacks.add(number, spec.Basic.Nack, self._on_publish_ack_nack, False)

    def _on_tx_selected(self, _method_frame: Any) -> None:
        self._tx_open = True

    def _on_tx_finished(self, _method_frame: Any) -> None:
        self._tx_open = False

    def _on_publish_ack_nack(self, method_frame: Any) -> None:
        delivery_tag = method_frame.method.delivery_tag
        if method_frame.method.multiple:
            self._unconfirmed = {
                tag for tag in self._unconfirmed if tag > delivery_tag
            }
        else:
            self._unconfirmed.discard(delivery_tag)

    def _note_egress(self, channel_number: int, method: Any) -> None:
        """Track publishes against publisher confirms on the pinned channel."""
        if self._pin is None or channel_number != self._pin.channel_number:
            return
        if isinstance(method, spec.Confirm.Select):
            self._confirm_mode = True
        elif self._confirm_mode and isinstance(method, spec.Basic.Publish):
            self._publish_seq += 1
            self._unconfirmed.add(self._publish_seq)

    def _dirty_details(self) -> list[str]:
        channel = self._pin
        details = []
        if channel is not None and channel.consumer_tags:
            details.append('active consumers')
        if self._tx_open:
            details.append('open transaction')
        if self._unconfirmed:
            details.append('unconfirmed publisher confirms')
        return details

    def _close_arguments(self, failure: bool) -> tuple[int, str]:
        if not failure:
            return 200, 'Normal shutdown'
        if isinstance(self._failed_reason, exceptions.ChannelScopeTimeoutError):
            return _FAILURE_REPLY_CODE, _TIMEOUT_REPLY_TEXT
        return _FAILURE_REPLY_CODE, _FAILURE_REPLY_TEXT

    def _schedule_grace(self, channel: Channel) -> None:
        try:
            self._grace_timer = self._connection._adapter_call_later(
                self.TEARDOWN_GRACE, lambda: self._grace_expired(channel))
        except NotImplementedError:
            # Unit-test doubles without timer support bound the wait themselves.
            self._grace_timer = None

    def _grace_expired(self, channel: Channel) -> None:
        if self._state == self.CLOSED:
            return
        self.mark_timeout()
        self.discard_channel(channel)

    def _cancel_grace_timer(self) -> None:
        if self._grace_timer is not None:
            try:
                self._connection._adapter_remove_timeout(self._grace_timer)
            except Exception:  # adapter double already gone; teardown wins
                LOGGER.debug('Could not cancel scope grace timer',
                             exc_info=True)
            self._grace_timer = None

    def _complete_teardown(self, reason: Exception) -> None:
        if self._state == self.CLOSED:
            return
        self._cancel_grace_timer()
        self._state = self.CLOSED
        self._pin = None
        self._candidates.clear()
        self.deactivate()
        self._connection._unregister_scope(self)

        listeners = self._close_listeners
        self._close_listeners = []
        for callback in listeners:
            callback(self, reason)
        self._on_teardown_complete(reason)

    def _on_teardown_complete(self, reason: Exception) -> None:
        """Override hook for adapters; reason describes why the channel closed."""
