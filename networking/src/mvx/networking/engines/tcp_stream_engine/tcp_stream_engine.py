# src/mvx/networking/engines/tcp_stream_engine/tcp_stream_engine.py

"""
TCP stream engine for single-connection clients.

This module provides :class:`TcpStreamEngine`, a small, self-contained async
engine that owns a single TCP stream (optionally wrapped with TLS) and exposes
a minimal, structured API for opening, closing and performing I/O against it.

The engine is designed as a reusable building block for higher level protocol
clients (for example LDAP, NMEA or other stream-based adapters). It provides:

  * lifecycle and concurrency control around a single TCP connection,
  * DNS / address resolution via :class:`RemoteEndpoint`,
  * connection establishment with detailed error mapping,
  * read/write/drain primitives with timeout control,
  * optional attachment of a stream-level crypto codec for transparent
    read/write transformation,
  * stable error surfaces and logging based on mvx.logger.

Scope and responsibilities
==========================

`TcpStreamEngine` is responsible for:

  * Translating a logical remote endpoint description
    (:class:`RemoteEndpointConnectionInfoProto` via :class:`RemoteEndpoint`)
    into concrete connection attempts (sockets, TLS, timeouts).

  * Owning the underlying `asyncio.StreamReader` / `StreamWriter` pair,
    including their lifecycle (open, close, error).

  * Optionally owning an attached stream-level crypto codec
    (:class:`CryptoCodec`) that transforms public `read()` / `write()`
    operations while leaving the underlying raw stream ownership unchanged.

  * Exposing a small public API:
      - :meth:`open` / :meth:`close` for lifecycle control,
      - :meth:`attach_crypto_codec` / :meth:`detach_crypto_codec` for
        attaching or detaching an optional stream-level crypto codec,
      - :meth:`read` / :meth:`write` / :meth:`drain` for stream I/O,
      - read-only properties :pyattr:`state` and :pyattr:`is_open`.

  * Enforcing consistent behavior and error mapping across all callers:
      - well-defined `EngineState` transitions,
      - stable network error types from :mod:`mvx.asyncio.networking.net_errors`,
      - a single unexpected error type,
        :class:`TcpStreamEngineUnexpectedError`, for public all API methods.

The engine is deliberately narrow in scope:

  * It does not implement protocol framing or message routing.
  * It does not manage reconnection or backoff policies.
  * It does not manage pools or multiple simultaneous connections.
  * It does not interpret protocol messages when a crypto codec is attached;
    the codec operates strictly at the byte-stream level.

These concerns are expected to be implemented in higher layers that depend on
`TcpStreamEngine` as their transport primitive.

Configuration surfaces
======================

`TcpStreamEngine` is configured with:

  * :class:`RemoteEndpoint`:
      Encapsulates the logical target (`host`, `port`, address family policy)
      and optional address info caching. It uses:

        - :class:`RemoteEndpointConnectionInfoProto`:
            The logical connection parameters (host, port, timeouts, TLS, local
            bind hints).

        - :data:`IpMode` and :data:`AddrInfo`:
            IP family selection and concrete address candidates.

  * `logger`:
      A :class:`logging.Logger` instance used by the `log_invocation` decorator
      and internal diagnostics. When omitted, the engine uses the
      `"mvx.tcp_stream_engine"` logger.

The `RemoteEndpointConnectionInfoProto` is expected to provide, at minimum:

  * ``host`` / ``port``:
      Target address as a logical token (DNS name or IP) and TCP port.

  * ``connect_timeout_ms``:
      Per-candidate connection timeout, applied when establishing a TCP
      connection.

  * ``socket_timeout_ms``:
      Base I/O timeout, used as a default for `read()` / `drain()` in
      `SocketTimeoutMode.LIMITED`. Must be strictly positive; otherwise
      the engine fails fast in the constructor.

  * ``source_address`` / ``source_port_list``:
      Optional local bind hints. If provided, `_open_socket` iterates
      `source_port_list` and attempts to bind `source_address` plus each
      port in turn.

  * ``tls``:
      TLS configuration (:class:`TlsInfoProto`). When ``tls.tls_mode ==
      TLS_MODE_TLS``, the engine wraps the established stream with TLS via
      :func:`wrap_stream_tls`.

Public API and error surface
============================

All public members intended for external consumption are wrapped with
:func:`api_error_processor`:

  * Properties:
      - :pyattr:`is_open`
      - :pyattr:`state`

  * Methods:
      - :meth:`open`
      - :meth:`close`
      - :meth:`attach_crypto_codec`
      - :meth:`detach_crypto_codec`
      - :meth:`read`
      - :meth:`write`
      - :meth:`drain`

The decorator is configured as:

  * `passthrough_error_types`:
      - :class:`NetError` (family of structured network errors),

    These errors pass through unchanged and form the declared public error
    surface that callers are expected to handle explicitly.

  * `raise_error_type`:
      - :class:`TcpStreamEngineUnexpectedError`

    Any other `Exception` (excluding `asyncio.CancelledError` and RuntimeExtendedError
    descendants) is wrapped into `TcpStreamEngineUnexpectedError(module=...,
    qualname=..., cause=...)`.
    This stabilizes the observable error surface while preserving the original
    exception as a cause for diagnostics and logging.

Cancellation is treated as control flow:

  * `asyncio.CancelledError` is never wrapped and is always re-raised
    unchanged at the public boundary.

Lifecycle and state model
=========================

The engine maintains a lifecycle state using :class:`EngineState`:

  * ``VIRGIN``:
      Newly constructed engine, never opened.

  * ``OPENING``:
      Transitional state during `open()`: address resolution, candidate
      iteration, socket/TLS setup. The engine ensures it never remains in
      OPENING on exit (including cancellation).

  * ``OPENED``:
      Active connection with a valid `(reader, writer)` pair. I/O operations
      are allowed in this state.

  * ``CLOSING``:
      Transitional state during `close()`: stream teardown and cleanup.
      Callers may wait for the engine to fully close.

  * ``CLOSED``:
      Gracefully closed; no active reader/writer is present.

  * ``ERROR``:
      Terminal failure state for a broken connection attempt or
      unexpected internal error during `open()`.

Concurrency and synchronization
===============================

The engine serializes state transitions and I/O gating using:

  * An internal :class:`asyncio.Lock` and :class:`asyncio.Condition`:

      - `open()` and `close()` coordinate via the condition to avoid
        overlapping transitions.

      - I/O helpers wait for transitional states (OPENING / CLOSING) to
        complete before performing operations.

  * The internal `_acquire_opened_streams()` helper:

      - Waits for OPENING/CLOSING to finish.
      - Validates that the engine is in OPENED state and both reader
        and writer are present.
      - Returns a snapshot `(reader, writer, default_timeout_s)` with the
        default timeout derived from `socket_timeout_ms`.

If the engine is not OPENED or is missing the required streams,
`_acquire_opened_streams()` raises:

  * :class:`TcpStreamEngineNotOpenError` when the engine is not usable
    for I/O.

  * :class:`TcpStreamEngineUnexpectedlyClosingError` if a writer exists
    but is already closing (guard against races with closure).

`is_open` property
------------------

The `is_open` property returns `True` only when:

  * state is OPENED, and
  * a writer exists, and
  * the writer is not in a closing state.

This property is intended as a fast, non-raising readiness hint. It does
not acquire locks and therefore does not provide strong concurrency
guarantees; for strict checks, use `_acquire_opened_streams()` via the
public I/O methods.

open()
------

`open()` establishes a connection to the first successfully opened
candidate address:

  * Waits for any in-progress OPENING or CLOSING to complete.

  * Returns immediately with ``OpenOutcome.ALREADY_OPENED`` if
  already OPENED (idempotent).

  * Transitions state to OPENING and obtains candidates via
    `RemoteEndpoint.get_candidate_addresses()`:

      - If candidate resolution fails and produces no addresses, raises
        :class:`OpenConnectionError` with reason
        ``HOST_CANNOT_BE_RESOLVED``.

  * Iterates candidates in order:

      - For each candidate, calls `_open_socket(info, candidate)`.

      - On :class:`OpenSocketError`, collects it and tries the next
        candidate.

      - On success, stores `(reader, writer)`, transitions to OPENED,
        notifies waiters and returns ``OpenOutcome.OPENED``.

  * If all candidates fail with :class:`OpenSocketError`, raises
    :class:`OpenConnectionError` with reason
    ``CONNECTION_TO_HOST_FAILURE`` and the list of per-candidate
    socket errors in attempt order.

The `finally` block ensures that OPENING is never left as the final
state:

  * On any exit (success, failure, cancellation), a shielded clean-up
    sets state to ERROR if it is still OPENING and notifies all waiters.

open()` returns :class:`OpenOutcome`:

  * ``OpenOutcome.OPENED``:
      A connection was successfully established by this call. The engine
      transitions to ``EngineState.OPENED`` and stores a valid
      ``(reader, writer)`` pair.

  * ``OpenOutcome.ALREADY_OPENED``:
      The engine was already in ``EngineState.OPENED`` when `open()` was
      called. The call is idempotent and leaves the existing connection
      unchanged.

close()
-------
This method is a thin public wrapper and delegates the entire shutdown
procedure to the internal `_close_core()` helper.

The shutdown behavior (implemented by `_close_core()`) is:

  * Waits for OPENING to finish (cannot close a half-open engine).

  * If another close is already in progress (CLOSING), waits for it to
    finish and returns ``CloseOutcome.NOT_OPENED``.

  * If not OPENED, returns immediately with ``CloseOutcome.NOT_OPENED``
    (idempotent).

  * Transitions to CLOSING, clears stored reader/writer and then closes
    the writer:

      - `writer.close()`
      - `await writer.wait_closed()`

  * In a `finally` block, ensures that:

      - CLOSING is always resolved into CLOSED,
      - waiters are notified regardless of cancellation or errors.

`close()` returns :class:`CloseOutcome` (is the outcome produced by `_close_core()`):

  * ``CloseOutcome.CLOSED``:
      The engine was in ``EngineState.OPENED`` and this call performed the
      shutdown sequence. The engine transitions through ``EngineState.CLOSING``
      and finishes in ``EngineState.CLOSED``.

  * ``CloseOutcome.NOT_OPENED``:
      There was no open connection to close (the engine was not in
      ``EngineState.OPENED``), or another close was already in progress and this
      call waited for it to finish. The call is idempotent and leaves the engine
      in a non-open state.

Read / write / drain semantics
==============================

Socket timeout mode
-------------------

`read()` and `drain()` accept:

  * `mode: SocketTimeoutMode`:

      - ``UNLIMITED``:
          No explicit timeout is applied. The operation may block
          indefinitely unless higher level cancellation is used.

      - ``LIMITED``:
          An explicit timeout is enforced using `asyncio.wait_for()`.

  * `socket_timeout_s: float | None`:

      - Optional per-call override for the timeout in seconds.

      - If `None` or `<= 0`, the engine uses the default timeout derived
        from `socket_timeout_ms` on the `RemoteEndpoint.info`.

`write()` is synchronous and does not apply timeouts directly; backpressure
is handled via `drain()`.

read()
------

`read(n, mode, socket_timeout_s)`:

  * Acquires an OPENED snapshot via `_acquire_opened_streams()` with
    `io_operation_type=TCP_READ`.

  * Determines the effective timeout (when `mode == LIMITED`):

      - `effective_timeout_s = socket_timeout_s > 0 ? socket_timeout_s : default_timeout_s`.

  * Performs the read:

      - `LIMITED`:
          `await asyncio.wait_for(reader.read(n), timeout=effective_timeout_s)`

      - `UNLIMITED`:
          `await reader.read(n)`

  * On timeout (`asyncio.TimeoutError` in LIMITED mode):

      - Raises :class:`SocketTimeoutError` with metadata:

          - `io_op_type=TCP_READ`
          - `engine_state=<state at time of error>`
          - `socket_timeout_mode=mode`
          - `socket_timeout_s=effective_timeout_s`
          - additional detail `"read_max_bytes" = n`.

      - The engine is not force-closed by this method.

  * On EOF (`data == b""`):

      - Captures the state before closing.
      - Calls `await _close_core()`.
      - Raises :class:`TcpStreamRemotelyDisconnectedError` with the
        previous engine state.

  * On other exceptions during read:

      - Maps the exception to a :class:`TcpStreamIoErrorReason` via
        `_map_io_exception_to_reason()`.

      - Captures the state before closing.

      - Calls `await _close_core()`.

      - Raises :class:`TcpStreamIoError` with:

          - `io_op_type=TCP_READ`
          - `engine_state=<previous state>`
          - `reason=<mapped reason>`
          - `cause=<original exception>`.

write()
-------

`write(data)`:

  * If `data` is empty, returns immediately.

  * Validates that the engine is OPENED and a writer exists; otherwise
    raises :class:`TcpStreamEngineNotOpenError` with:

      - `io_op_type=TCP_WRITE`
      - `engine_state=<current state>`
      - `is_reader` / `is_writer` flags describing the presence of
        underlying stream objects.

  * If the writer is closing, raises
    :class:`TcpStreamEngineUnexpectedlyClosingError`.

  * Attempts to write to the underlying `StreamWriter`:

      - On any exception, maps it to a :class:`TcpStreamIoErrorReason`
        via `_map_io_exception_to_reason()` and raises
        :class:`TcpStreamIoError` with:

          - `io_op_type=TCP_WRITE`
          - `engine_state=<current state>`
          - `reason=<mapped reason>`
          - `cause=<original exception>`.

  * Does not call `drain()`; callers must explicitly call `drain()` to
    apply backpressure.

Attached crypto codec
---------------------

When no crypto codec is attached, :meth:`read` and :meth:`write` operate
directly on the underlying TCP/TLS stream.

When a crypto codec is attached via :meth:`attach_crypto_codec`:

* :meth:`read` delegates stream consumption to the attached codec, which
obtains raw bytes through the engine's internal raw-read primitive and
returns transformed plaintext bytes to the caller.

* :meth:`write` delegates outgoing bytes to the attached codec, which may
transform and emit one or more raw chunks through the engine's internal
raw-write primitive.

* :meth:`drain` remains a transport-level operation over the underlying
writer and is not routed through the codec.

The crypto codec is therefore an optional stream-level transformation layer
applied to the public read/write path, while raw stream ownership and lifecycle
remain with :class:`TcpStreamEngine`.

drain()
-------

`drain(mode, socket_timeout_s)`:

  * Acquires an OPENED snapshot via `_acquire_opened_streams()` with
    `io_operation_type=TCP_DRAIN`.

  * Determines the effective timeout similarly to `read()`.

  * Performs the drain:

      - `LIMITED`:
          `await asyncio.wait_for(writer.drain(), timeout=effective_timeout_s)`

      - `UNLIMITED`:
          `await writer.drain()`

  * On timeout (`asyncio.TimeoutError` in LIMITED mode):

      - Raises :class:`SocketTimeoutError` with:

          - `io_op_type=TCP_DRAIN`
          - `engine_state=<current state>`
          - `socket_timeout_mode=mode`
          - `socket_timeout_s=effective_timeout_s`.

      - The engine is not force-closed by this method.

  * On other exceptions during drain:

      - Maps the exception to a :class:`TcpStreamIoErrorReason`.

      - Captures the state before closing.

      - Calls `await _close_core()`.

      - Raises :class:`TcpStreamIoError` with:

          - `io_op_type=TCP_DRAIN`
          - `engine_state=<previous state>`
          - `reason=<mapped reason>`
          - `cause=<original exception>`.

Per-candidate socket opening
============================

`_open_socket(info, address_info)` is a method that encapsulates
the logic of opening a single candidate address:

  * Uses :func:`_create_open_socket_error_raiser` to build an error
    raiser bound to the candidate and configuration. This helper:

      - Classifies low-level exceptions into :class:`OpenSocketErrorReason`
        based on the current :class:`OpenSocketStage` and (for connect
        failures) `errno` values.

      - Raises :class:`OpenSocketError` with:

          - `reason`
          - `details` (host, port, family, socktype, proto, source bind hints)
          - `candidate` (stringified via :func:`_candidate_to_str`)
          - `cause` (original exception).

  * Steps:

      A) SOCKET_CREATE:
          Create `socket.socket(family, socktype, proto)`.

      B) SOCKET_BIND (optional):
          If `source_address` and `source_port_list` are provided, iterate
          the ports and attempt to `bind()`. If all ports fail, propagate
          the last bind exception which is then mapped to
          `SOCKET_BIND_FAILED`.

      C) SOCKET_CONNECT:
          Set the socket to non-blocking, then:

            - `await asyncio.wait_for(loop.sock_connect(sock, sockaddr), timeout=connect_timeout_s)`
            - `reader, writer = await asyncio.open_connection(sock=sock)`

      D) SOCKET_WRAP_SSL:
          If `tls.tls_mode == TLS_MODE_TLS`, call `wrap_stream_tls(info, writer)`
          to wrap the stream into TLS.

  * Cleanup:

      - On `asyncio.CancelledError`:

          - Performs best-effort cleanup via `_clean_up(await_writer_close=False)`.
          - Re-raises the cancellation.

      - On other exceptions:

          - Attempts to map via `error_raiser(stage, exc)`; if it raises
            `OpenSocketError`, that becomes the mapped error.

          - Performs `_clean_up(await_writer_close=True)`.

          - If an `OpenSocketError` was produced, it is raised; otherwise
            the original exception is re-raised.

Integration with logging
========================

Events emitted by @log_invocation (public API)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These events are emitted by the `log_invocation` decorator on the public
API of the engine. Base context is produced via `context_fields` and the
(optional) `context_formatter` and is merged into the top-level payload.

Note on activation_profiles
---------------------------
In the current implementation of mvx.logger.log_invocation, normal events
(INVOKE and SUCCESS) may be conditionally suppressed based on the active log
profile. When `activation_profiles` is set on a decorated function, INVOKE and
SUCCESS are emitted only if the active profile matches one of the configured
profiles. Terminal events (FAILED and CANCELLED) are always emitted regardless
of the active profile. This suppression is independent of log levels; it is
controlled solely by the active profile match.

Arguments configured via `log_kwargs_on_invoke` are logged ONLY for `*.invoke`
under the `kwargs` key. Values configured via `log_result_on_success` are logged
ONLY for `*.success` under the `result` key.

  tcp_stream_engine.open.invoke
  tcp_stream_engine.open.success
  tcp_stream_engine.open.failed
  tcp_stream_engine.open.cancelled
      Emitted around TcpStreamEngine.open().

      Top-level context on all events:
        - engine_id: string
        - engine_state: EngineState.value (string)

      Additional context on invoke ONLY (via context_formatter):
        - connection_info: dict (either info.to_log_payload() or a default dict)
          with at least:
            * host: string
            * port: int
            * tls_mode: string

      Result on success:
        - result.value: OpenOutcome.value ("OPENED" or "ALREADY_OPENED")

  tcp_stream_engine.close.invoke
  tcp_stream_engine.close.success
  tcp_stream_engine.close.failed
  tcp_stream_engine.close.cancelled
      Emitted around TcpStreamEngine.close().

      Top-level context on all events:
        - engine_id: string
        - engine_state: EngineState.value (string)

      Result on success:
        - result.value: CloseOutcome.value ("CLOSED" or "NOT_OPENED")

  tcp_stream_engine.read.invoke
  tcp_stream_engine.read.success
  tcp_stream_engine.read.failed
  tcp_stream_engine.read.cancelled
      Emitted around TcpStreamEngine.read().

      Profile behavior (hot path)
      ---------------------------
      In this module, TcpStreamEngine.read() is decorated with:
        activation_profiles=("inspection",)

      Therefore:
        - "tcp_stream_engine.read.invoke" and "tcp_stream_engine.read.success" are
          emitted only when the active log profile is "inspection".
        - "tcp_stream_engine.read.failed" and "tcp_stream_engine.read.cancelled" are
          always emitted (even when INVOKE/SUCCESS are suppressed).

      Cancellation level
      ------------------
      In this module, the ".cancelled" event for this hot-path method is configured
      to be logged at DEBUG level (cancel_level=logging.DEBUG).

      Top-level context on all events:
        - engine_id: string
        - engine_state: EngineState.value (string)

      Invoke kwargs snapshot (invoke ONLY, under "kwargs"):
        - kwargs.read_max_bytes: int
        - kwargs.timeout_mode: string (SocketTimeoutMode.value)
        - kwargs.timeout_override_s: float | None

      Result on success (under "result"):
        - result.read_bytes: int

  tcp_stream_engine.write.invoke
  tcp_stream_engine.write.success
  tcp_stream_engine.write.failed
  tcp_stream_engine.write.cancelled
      Emitted around TcpStreamEngine.write().

      Profile behavior (hot path)
      ---------------------------
      In this module, TcpStreamEngine.write() is decorated with:
        activation_profiles=("inspection",)

      Therefore:
        - "tcp_stream_engine.write.invoke" and "tcp_stream_engine.write.success" are
          emitted only when the active log profile is "inspection".
        - "tcp_stream_engine.write.failed" and "tcp_stream_engine.write.cancelled" are
          always emitted (even when INVOKE/SUCCESS are suppressed).

      Cancellation level
      ------------------
      In this module, the ".cancelled" event for this hot-path method is configured
      to be logged at DEBUG level (cancel_level=logging.DEBUG).


      Top-level context on all events:
        - engine_id: string
        - engine_state: EngineState.value (string)


      Invoke kwargs snapshot (invoke ONLY, under "kwargs"):
        - kwargs.write_bytes: int (len(data))

  tcp_stream_engine.drain.invoke
  tcp_stream_engine.drain.success
  tcp_stream_engine.drain.failed
  tcp_stream_engine.drain.cancelled
      Emitted around TcpStreamEngine.drain().

      Profile behavior (hot path)
      ---------------------------
      In this module, TcpStreamEngine.drain() is decorated with:
        activation_profiles=("inspection",)

      Therefore:
        - "tcp_stream_engine.drain.invoke" and "tcp_stream_engine.drain.success" are
          emitted only when the active log profile is "inspection".
        - "tcp_stream_engine.drain.failed" and "tcp_stream_engine.drain.cancelled" are
          always emitted (even when INVOKE/SUCCESS are suppressed).

      Cancellation level
      ------------------
      In this module, the ".cancelled" event for this hot-path method is configured
      to be logged at DEBUG level (cancel_level=logging.DEBUG).

      Top-level context on all events:
        - engine_id: string
        - engine_state: EngineState.value (string)

      Invoke kwargs snapshot (invoke ONLY, under "kwargs"):
        - kwargs.timeout_mode: string (SocketTimeoutMode.value)
        - kwargs.timeout_override_s: float | None


Internal/background events
~~~~~~~~~~~~~~~~~~~~~~~~~~

TcpStreamEngine does not own dedicated background tasks (RX/TX loops).
Observable behavior is primarily captured via the public API events above.

In addition, the engine emits an internal debug event when it performs an
abortive close due to an error passed into _close_core(exc=...):

  tcp_stream_engine.abortive_close
      Emitted from _close_core(exc=...) in the finally block.
      data:
        - engine_id: string
        - due_to: exc.to_log_payload()

For hot-path I/O methods (read/write/drain), INVOKE/SUCCESS may be suppressed
depending on the active log profile; FAILED/CANCELLED remain observable.

Error payload rules
-------------------

Public API methods of TcpStreamEngine are instrumented with `log_invocation`,
which is responsible for logging terminal events (`*.failed` / `*.cancelled`)
and for building the structured error payload.

For `*.failed`, the detailed `error` payload is produced by
`build_error_payload(err)` and is emitted according to the decorator's
`log_error_policy` and the per-exception "already logged" flag
(`is_error_logged(err)` / `mark_error_logged(err)`), so that a detailed payload
is typically emitted at most once per exception instance across multiple layers.

For `*.cancelled`, `log_invocation` always logs `cancelled=True` together with
the `error` payload for the `asyncio.CancelledError`.


Contract for extensions
=======================

Components that build on top of `TcpStreamEngine` should treat it as the
single owner of the underlying TCP stream and of the optional attached crypto
codec reference, and should not:

  * mutate the internal reader/writer or state directly,
  * close the underlying writer outside of `close()` or `_open_socket()`.

They may safely:

  * rely on the public API and documented error types as a stable contract,
  * compose additional behavior (reconnection, higher-level protocols,
    message routing) around this engine,
  * use the provided logging hooks to enrich context at higher levels
    without changing the core engine behavior.
  * attach or detach a stream-level crypto codec at protocol-defined
    boundaries, while keeping protocol framing and message semantics outside
    of this transport layer.

Any new behavior added to this module should preserve the invariants and
error semantics described above.
"""

from __future__ import annotations

from typing import Callable, Any, cast
from enum import StrEnum

import asyncio
import socket
import errno
import ssl
from uuid import uuid4


from mvx.common.helpers import (
    api_error_processor,
    run_with_cancellation_policy,
    CancellationPolicy,
)

from mvx.common.logger import (
    LogLevel,
    LogContextProto,
    LogContext,
    log_invocation,
)

from ...helpers import RemoteEndpoint, wrap_stream_tls

from ...models import (
    AddrInfo,
    EngineState,
    SocketTimeoutMode,
    TcpIoOperation,
    TCP_READ,
    TCP_WRITE,
    TCP_DRAIN,
    RemoteEndpointConnectionInfoProto,
)

from ...net_errors import (
    NetError,
    OpenSocketError,
    OpenSocketErrorReason,
    TlsError,
    OpenConnectionError,
    OpenConnectionErrorReason,
    TcpStreamIoError,
    TcpStreamIoErrorReason,
    TcpStreamRemotelyDisconnectedError,
    SocketTimeoutError,
    TlsErrorReason,
)

from .errors import (
    TcpStreamEngineNotOpenError,
    TcpStreamEngineUnexpectedlyClosingError,
    TcpStreamEngineUnexpectedError,
)

from .crypto_codec import CryptoCodec

__all__ = (
    "TcpStreamEngine",
    "TcpStreamOpenOutcome",
    "TcpStreamCloseOutcome",
    "TcpStreamReconfigOutcome",
    "TcpStreamSecurityMode",
)


class _OpenSocketStage(StrEnum):
    SOCKET_CREATE = "SOCKET_CREATE"
    SOCKET_BIND = "SOCKET_BIND"
    SOCKET_CONNECT = "SOCKET_CONNECT"
    SOCKET_WRAP_SSL = "SOCKET_WRAP_SSL"


class TcpStreamOpenOutcome(StrEnum):
    OPENED = "OPENED"
    ALREADY_OPENED = "ALREADY_OPENED"


class TcpStreamCloseOutcome(StrEnum):
    CLOSED = "CLOSED"
    NOT_OPENED = "NOT_OPENED"


class TcpStreamReconfigOutcome(StrEnum):
    DONE = "DONE"
    REFUSED_CONNECTION_NOT_OPENED = "REFUSED_CONNECTION_NOT_OPENED"
    REFUSED_CONNECTION_ALREADY_UNDER_SSL = "REFUSED_CONNECTION_ALREADY_UNDER_SSL"
    REFUSED_START_TLS_ALREADY_ACTIVE = "REFUSED_START_TLS_ALREADY_ACTIVE"
    REFUSED_CRYPTO_CODEC_ATTACHED = "REFUSED_CRYPTO_CODEC_ATTACHED"
    REFUSED_CRYPTO_CODEC_NOT_ATTACHED = "REFUSED_CRYPTO_CODEC_NOT_ATTACHED"


class TcpStreamSecurityMode(StrEnum):
    NOT_AVAILABLE = "NOT_AVAILABLE"
    PLAIN = "PLAIN"
    SSL = "SSL"
    START_TLS = "START_TLS"
    CODEC = "CODEC"


_error_processor = api_error_processor(
    passthrough_error_types=(NetError, TypeError, ValueError),
    raise_error_type=TcpStreamEngineUnexpectedError,
)


def _create_open_socket_error_raiser(
    info: RemoteEndpointConnectionInfoProto, address_info: AddrInfo
) -> Callable[[_OpenSocketStage, Exception], None]:
    """
    Build a stage-aware mapper that converts low-level socket/TLS exceptions into
    a stable, domain-specific OpenSocketError.

    The returned callable:
      - accepts the current OpenSocketStage and the original exception,
      - classifies the failure into an OpenSocketErrorReason (bounded enum),
      - raises OpenSocketError(reason=..., candidate=..., details=..., cause=...) from base_error.

    Notes
    -----
    - This helper is intentionally "data-only": it derives all log context from
      `info` and `address_info` and does not depend on external state.
    - Reasons are classified primarily by stage; for connect failures an errno-based
      refinement is applied when the underlying error is an OSError.
    - If the error cannot be classified for the given stage, the raiser does nothing
      (caller decides how to proceed).
    """
    family, socktype, proto, _canonname, _ = address_info
    candidate = _candidate_to_str(address_info)
    details = {
        "target_host": info.host,
        "target_port": info.port,
        "family": int(family),
        "socktype": int(socktype),
        "proto": int(proto),
        "source_address": info.source_address,
        "source_port_list": info.source_port_list,
        "connect_timeout_s": info.connect_timeout_ms / 1000.0,
    }

    def inner(stage: _OpenSocketStage, base_error: Exception) -> None:

        reason: OpenSocketErrorReason | None = None
        if stage is _OpenSocketStage.SOCKET_CREATE:
            reason = OpenSocketErrorReason.SOCKET_CREATE_FAILED
        elif stage is _OpenSocketStage.SOCKET_BIND:
            reason = OpenSocketErrorReason.SOCKET_BIND_FAILED
        elif stage is _OpenSocketStage.SOCKET_CONNECT:
            if isinstance(base_error, asyncio.TimeoutError):
                reason = OpenSocketErrorReason.SOCKET_CONNECT_TIMEOUT
            elif isinstance(base_error, ConnectionRefusedError):
                reason = OpenSocketErrorReason.SOCKET_CONNECT_REFUSED
            elif isinstance(base_error, OSError):
                err_no = getattr(base_error, "errno", None)
                if err_no == errno.ENETUNREACH:
                    reason = OpenSocketErrorReason.SOCKET_CONNECT_NO_ROUTE_TO_HOST
                elif err_no in (errno.EHOSTUNREACH, errno.EHOSTDOWN):
                    reason = OpenSocketErrorReason.SOCKET_CONNECT_HOST_UNREACHABLE
                elif err_no == errno.ETIMEDOUT:
                    reason = OpenSocketErrorReason.SOCKET_CONNECT_TIMEOUT
                else:
                    reason = OpenSocketErrorReason.SOCKET_CONNECT_FAILED
            else:
                reason = OpenSocketErrorReason.SOCKET_OPEN_FAILED_UNKNOWN
        elif stage is _OpenSocketStage.SOCKET_WRAP_SSL:
            if isinstance(base_error, TlsError):
                reason = OpenSocketErrorReason.SOCKET_SSL_WRAP_FAILED

        if reason is not None:
            raise OpenSocketError(
                reason=reason,
                details=details,
                candidate=candidate,
                cause=base_error,
            ) from base_error

    return inner


def _map_io_exception_to_reason(exc: BaseException) -> TcpStreamIoErrorReason:
    """
    Map a low-level I/O exception into a stable TcpStreamIoErrorReason.

    Rules
    -----
    - ConnectionResetError / OSError(ECONNRESET) -> TCP_STREAM_CONNECTION_RESET
    - BrokenPipeError / OSError(EPIPE) -> TCP_STREAM_BROKEN_PIPE
    - Other OSError -> TCP_STREAM_IO_ERROR
    - Anything else -> TCP_STREAM_IO_ERROR_UNKNOWN

    Notes
    -----
    - This function is intentionally conservative: it only classifies what it can
      recognize reliably. Unknown cases fall back to TCP_STREAM_IO_ERROR_UNKNOWN.
    """
    if isinstance(exc, ConnectionResetError):
        return TcpStreamIoErrorReason.TCP_STREAM_CONNECTION_RESET
    if isinstance(exc, BrokenPipeError):
        return TcpStreamIoErrorReason.TCP_STREAM_BROKEN_PIPE
    if isinstance(exc, OSError):
        err_no = getattr(exc, "errno", None)
        if err_no == errno.ECONNRESET:
            return TcpStreamIoErrorReason.TCP_STREAM_CONNECTION_RESET
        if err_no == errno.EPIPE:
            return TcpStreamIoErrorReason.TCP_STREAM_BROKEN_PIPE
        return TcpStreamIoErrorReason.TCP_STREAM_IO_ERROR
    return TcpStreamIoErrorReason.TCP_STREAM_IO_ERROR_UNKNOWN


def _map_ssl_exception_to_reason(exc: BaseException) -> TlsErrorReason:
    """
    Map a SSL exception into a stable TlsErrorReason.

    Rules
    -----
    - SSLZeroReturnError -> TLS_SESSION_CLOSED_CLEANLY_BY_PEER
    - SSLEOFError -> TLS_SESSION_TERMINATED_ABRUPTLY
    - Anything else -> TLS_UNEXPECTED_ERROR

    Notes
    -----
    - This function is intentionally conservative: it only classifies what it can
      recognize reliably. Unknown cases fall back to TLS_UNEXPECTED_ERROR.
    """

    if isinstance(exc, ssl.SSLZeroReturnError):
        return TlsErrorReason.TLS_SESSION_CLOSED_CLEANLY_BY_PEER

    if isinstance(exc, ssl.SSLEOFError):
        return TlsErrorReason.TLS_SESSION_TERMINATED_ABRUPTLY

    return TlsErrorReason.TLS_UNEXPECTED_ERROR


def _candidate_to_str(candidate: AddrInfo) -> str:
    """
    Convert AddrInfo to a stable, human-friendly candidate identifier.

    Rules
    -----
    - For INET sockaddrs (tuple[str, int, ...]) returns "<ip>:<port>".
    - Otherwise returns repr(sockaddr) as a generic stable identifier.

    This string is used for:
      - per-candidate error reporting,
      - deterministic logs/tests,
      - preserving the attempt order at higher layers.
    """
    sockaddr = candidate[4]
    if (
        isinstance(sockaddr, tuple)
        and len(sockaddr) >= 2
        and isinstance(sockaddr[0], str)
        and isinstance(sockaddr[1], int)
    ):
        return f"{sockaddr[0]}:{sockaddr[1]}"
    return repr(sockaddr)


def _remote_endpoint_log_formatter(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    """
    Formatter used only for `TcpStreamEngine.open()` logging.

    It is passed as `context_formatter` to the `@log_invocation(...)` decorator
    on `TcpStreamEngine.open()` to achieve one specific behavior:
    include endpoint parameters (`host`, `port`, `tls_mode`) only on the
    `*.invoke` event, while always emitting `engine_id` and `engine_state`.

    Exactly what it returns
    -----------------------
    Always returns a dict with:
      - ``engine_id``: ``fields["engine_id"]``
      - ``engine_state``: ``fields["engine_state"]``

    Additionally, only when ``event_type == InvocationEventType.INVOKE``:
      - reads ``info = fields["connection_info"]``;
      - if ``info`` has ``to_log_payload`` attribute, tries to call it;
        otherwise builds a minimal fallback payload:
          ``{"host": info.host, "port": info.port, "tls_mode": info.tls.tls_mode}``
      - if calling ``to_log_payload()`` raises, falls back to the minimal payload;
      - if the resulting payload is truthy, adds it under the key
        ``"connection_info"``.

    Notes
    -----
    - `event_prefix` is accepted but not used.
    - This formatter assumes required keys exist in `fields`. Missing keys will
      raise `KeyError`.
    """

    _ = event, ctx

    def _build_deffault_payload() -> dict[str, Any]:
        return {
            "host": info.host,
            "port": info.port,
        }

    payload: dict[str, Any] = {
        "engine_state": fields["engine_state"],
    }

    # noinspection PyStringConversionWithoutDunderMethod
    if str(event_outcome) == "invoke":

        info: RemoteEndpointConnectionInfoProto = cast(
            RemoteEndpointConnectionInfoProto, fields["connection_info"]
        )

        # noinspection PyBroadException
        try:
            if hasattr(info, "to_log_payload"):
                connection_info = info.to_log_payload()
            else:
                connection_info = _build_deffault_payload()
        except Exception:
            connection_info = _build_deffault_payload()

        if connection_info:
            payload["connection_info"] = connection_info

    return payload


class TcpStreamEngine:
    def __init__(
        self,
        *,
        remote_endpoint: RemoteEndpoint,
        log_context: LogContext | None = None,
        entity_id: str | None = None,
    ) -> None:

        # Argument validation
        if remote_endpoint is None:
            raise ValueError("argument 'remote_endpoint' must not be None")

        if not isinstance(remote_endpoint, RemoteEndpoint):
            raise TypeError("argument 'remote_endpoint' must be an instance of 'RemoteEndpoint'")

        if log_context is not None:
            if not isinstance(log_context, LogContext):
                raise TypeError(
                    "argument 'log_context' must be an instance of 'LogContext' when provided"
                )

        if entity_id is not None:
            if not isinstance(entity_id, str):
                raise TypeError("argument 'entity_id' must be string when provided")

        # Logging infrastructure
        _id = (entity_id or "").strip()
        self._id = _id or uuid4().hex[:8]

        self._log_context = log_context

        # Connection info
        self._remote_endpoint = remote_endpoint

        # Validate socket_timeout_ms once, fail fast with a clear unexpected error.
        if self._remote_endpoint.info.socket_timeout_ms <= 0:
            raise ValueError(
                f"argument 'remote_endpoint.info.socket_timeout_ms' must be > 0, "
                f"got {self._remote_endpoint.info.socket_timeout_ms!r}"
            )

        # Rx stream reader/ Tx stream writer
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

        # Locks
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(self._lock)

        # State
        self._state: EngineState = EngineState.VIRGIN

        # Security
        self._security_mode: TcpStreamSecurityMode = TcpStreamSecurityMode.NOT_AVAILABLE
        self._crypto_codec: CryptoCodec | None = None

    # ---- Logging infrastructure ----------------------------------------------------------

    def get_log_context(self) -> LogContextProto | None:
        """
        Return the LoggerContextProto associated with this TcpStreamEngine instance.
        Used by log_invocation decorators and internal helpers.
        """
        return self._log_context

    @property
    def identity(self) -> str:
        return self._id

    # ---- Public API ----------------------------------------------------------------------

    @property
    @_error_processor
    def is_open(self) -> bool:
        """
        Whether the engine is currently usable for I/O.

        Returns True only when:
        - state is OPENED, and
        - a StreamWriter exists, and
        - the writer is not in a closing state.
        """
        return (
            self._state is EngineState.OPENED
            and self._writer is not None
            and not self._writer.is_closing()
        )

    @property
    @_error_processor
    def state(self) -> EngineState:
        """
        Current lifecycle state of the engine.

        The state is updated by open()/close() transitions and is used to gate I/O.
        """
        return self._state

    @property
    async def stream_security_mode(self) -> TcpStreamSecurityMode:
        async with self._cond:
            while self._state in (
                EngineState.OPENING,
                EngineState.CLOSING,
                EngineState.RECONFIGURING,
            ):
                await self._cond.wait()

            return self._security_mode

    @log_invocation(
        event="tcp_stream_engine.open",
        context_fields=(
            "engine_state=self._state.value",
            "connection_info=self._remote_endpoint.info",
        ),
        log_kwargs_on_invoke=("use_ssl=use_ssl",),
        context_formatter=_remote_endpoint_log_formatter,
        log_result_on_success=("value",),
    )
    @_error_processor
    async def open(self, *, use_ssl: bool = False) -> TcpStreamOpenOutcome:
        """
        Establish a TCP (optionally TLS) connection to the remote endpoint.

        Behavior
        --------
        - Waits until any in-progress OPENING or CLOSING transition finishes.
        - If the engine is already in OPENED state, returns immediately with
          OpenOutcome.ALREADY_OPENED without changing the existing connection.
        - Otherwise:
            1) Sets the state to OPENING.
            2) Requests candidate addresses from the associated RemoteEndpoint.
               If the candidate list is empty, raises OpenConnectionError with
               reason=OpenConnectionErrorReason.HOST_CANNOT_BE_RESOLVED.
            3) Iterates candidates in order and for each candidate:
                 - calls _open_socket(info, candidate),
                 - on success, stores (reader, writer), sets state to OPENED,
                   notifies waiters via the condition and returns OpenOutcome.OPENED,
                 - on OpenSocketError, appends the error to a local list and
                   proceeds to the next candidate.
            4) If all candidates fail with OpenSocketError, raises
               OpenConnectionError with
               reason=OpenConnectionErrorReason.CONNECTION_TO_HOST_FAILURE
               and socket_error_list set to the collected errors.

        State guarantees
        ----------------
        - OPENING is always a transient state. In the finalizer, if the engine
          is still in OPENING when open() is exiting (for any reason), the
          state is switched to ERROR and all waiters on the condition are
          notified.
        - On successful completion the state is OPENED.

        Return value
        ------------
        OpenOutcome
            One of:

            - ``OpenOutcome.OPENED``:
                A connection was successfully established by this call. The engine
                stores a valid ``(reader, writer)`` pair and transitions to
                ``EngineState.OPENED``.

            - ``OpenOutcome.ALREADY_OPENED``:
                The engine was already in ``EngineState.OPENED`` when `open()` was
                called. The call is idempotent and leaves the existing connection
                unchanged.

        Cancellation
        ------------
        - If cancellation happens after a writer has been created, the writer
          is closed with best-effort wait for close, then asyncio.CancelledError
          is re-raised.
        - The finalizer still runs under asyncio.shield() and may switch the
          state from OPENING to ERROR.

        Errors
        ------
        OpenConnectionError
            Raised when no candidates are available or all candidates fail.
        TcpStreamEngineUnexpectedError
            Any other unexpected exception escaping this method is wrapped
            into TcpStreamEngineUnexpectedError by @error_processor.
        """

        if not isinstance(use_ssl, bool):
            raise TypeError("argument 'use_ssl' must be bool when provided")

        async with self._cond:
            # Wait for transitional states to finish
            while self._state in (
                EngineState.OPENING,
                EngineState.CLOSING,
                EngineState.RECONFIGURING,
            ):
                await self._cond.wait()

            if self._state is EngineState.OPENED:
                return TcpStreamOpenOutcome.ALREADY_OPENED

            # Allow (re)open from VIRGIN/CLOSED/ERROR
            self._state = EngineState.OPENING

        try:

            candidates: list[AddrInfo] = await self._remote_endpoint.get_candidate_addresses()

            if not candidates:
                raise OpenConnectionError(
                    reason=OpenConnectionErrorReason.HOST_CANNOT_BE_RESOLVED,
                )

            causes: list[OpenSocketError] = []
            for candidate in candidates:
                try:
                    reader, writer = await self._open_socket(
                        self._remote_endpoint.info,
                        candidate,
                        use_ssl=use_ssl,
                    )

                    try:
                        async with self._cond:
                            self._reader = reader
                            self._writer = writer
                            self._security_mode = (
                                TcpStreamSecurityMode.SSL
                                if use_ssl
                                else TcpStreamSecurityMode.PLAIN
                            )
                            self._state = EngineState.OPENED
                            self._cond.notify_all()
                        return TcpStreamOpenOutcome.OPENED
                    except asyncio.CancelledError:
                        writer.close()
                        # noinspection PyBroadException
                        try:
                            await writer.wait_closed()
                        except Exception:
                            pass
                        raise
                except OpenSocketError as e:
                    causes.append(e)

            raise OpenConnectionError(
                reason=OpenConnectionErrorReason.CONNECTION_TO_HOST_FAILURE,
                socket_error_list=causes,
            )

        finally:
            # Ensure we never get stuck in OPENING (including on cancellation).
            async def _finalize_open() -> None:
                async with self._cond:
                    if self._state is EngineState.OPENING:
                        self._state = EngineState.ERROR
                    self._cond.notify_all()

            await asyncio.shield(_finalize_open())

    @log_invocation(
        event="tcp_stream_engine.close",
        context_fields=("engine_state=self._state.value",),
        log_result_on_success=("value",),
    )
    @_error_processor
    async def close(self) -> TcpStreamCloseOutcome:
        """
        Close the current connection, if the engine is OPENED.

        Behavior
        --------
        - Acquires the internal condition and:
            * waits while the state is OPENING (cannot close a half-open engine),
            * if the state is CLOSING, waits until it changes and then returns,
            * if the state is not OPENED, returns immediately (idempotent),
            * if the state is OPENED:
                - sets the state to CLOSING,
                - stores the current writer in a local variable,
                - clears the stored reader, writer and crypto codec references.
        - After releasing the condition:
            * if a writer was present, calls writer.close() and awaits
              writer.wait_closed() inside a try block.
        - In the finally block:
            * re-acquires the condition,
            * if the state is still CLOSING, sets it to CLOSED,
            * notifies all waiters on the condition.

        Return value
        ------------
        CloseOutcome
            One of:

            - ``CloseOutcome.CLOSED``:
                The engine was in ``EngineState.OPENED`` and this call performed the
                shutdown sequence. The engine transitions through ``EngineState.CLOSING``
                and finishes in ``EngineState.CLOSED``.

            - ``CloseOutcome.NOT_OPENED``:
                There was no open connection to close (the engine was not in
                ``EngineState.OPENED``), or another close was already in progress and this
                call waited for it to finish. The call is idempotent and leaves the engine
                in a non-open state.

        State guarantees
        ----------------
        - CLOSE is idempotent: calling close() when the engine is not OPENED
          has no effect and does not raise.
        - CLOSING is always a transient state: the method ensures that on exit
          (including cancellation) the state is no longer CLOSING and is set
          to CLOSED when appropriate.

        Cancellation
        ------------
        - asyncio.CancelledError may be raised while waiting for
          writer.wait_closed(); the finally block still runs and ensures that
          the state is not left in CLOSING and that waiters are notified.

        Errors
        ------
        TcpStreamEngineUnexpectedError
            Any unexpected exception escaping this method is wrapped into
            TcpStreamEngineUnexpectedError by @error_processor.
        """
        return await self._close_core()

    @log_invocation(
        event="tcp_stream_engine.start_tls",
        context_fields=(
            "engine_state=self._state.value",
            "security_mode=self._security_mode.value",
        ),
        log_result_on_success=("value",),
    )
    @_error_processor
    async def start_tls(
        self,
        *,
        handshake_timeout_s: float | None = None,
    ) -> TcpStreamReconfigOutcome:

        if handshake_timeout_s is not None:
            if isinstance(handshake_timeout_s, bool) or not isinstance(
                handshake_timeout_s,
                (int, float),
            ):
                raise TypeError("argument 'handshake_timeout_s' must be float when provided")

            if handshake_timeout_s <= 0:
                raise ValueError("argument 'handshake_timeout_s' must be positive when provided")

        async with self._cond:
            while self._state in (
                EngineState.OPENING,
                EngineState.CLOSING,
                EngineState.RECONFIGURING,
            ):
                await self._cond.wait()

            if self._state is not EngineState.OPENED:
                return TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED

            security_mode = self._security_mode

            if security_mode is TcpStreamSecurityMode.SSL:
                return TcpStreamReconfigOutcome.REFUSED_CONNECTION_ALREADY_UNDER_SSL

            if security_mode is TcpStreamSecurityMode.START_TLS:
                return TcpStreamReconfigOutcome.REFUSED_START_TLS_ALREADY_ACTIVE

            if security_mode is TcpStreamSecurityMode.CODEC:
                return TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_ATTACHED

            writer = self._writer
            self._state = EngineState.RECONFIGURING
            self._cond.notify_all()

        tls_started = False
        assert writer is not None, "TcpStreamEngine.start_tls: writer unexpectedly None"
        try:
            # noinspection PyTypeChecker
            # noinspection PyTupleAssignmentBalance
            cancelled, _ = await run_with_cancellation_policy(
                lambda: wrap_stream_tls(
                    self._remote_endpoint.info,
                    writer,
                    handshake_timeout_s=handshake_timeout_s,
                ),
                policy=CancellationPolicy.DEFER_FLAG,
            )
            tls_started = True

            if cancelled:
                raise asyncio.CancelledError

        except NetError as exc:
            await self._close_core(exc=exc, allow_when_reconfiguring=True)
            raise

        finally:
            async with self._cond:
                if self._state is EngineState.RECONFIGURING:
                    if tls_started:
                        self._security_mode = TcpStreamSecurityMode.START_TLS
                        self._state = EngineState.OPENED
                    self._cond.notify_all()

        return TcpStreamReconfigOutcome.DONE

    @log_invocation(
        event="tcp_stream_engine.attach_crypto_codec",
        context_fields=("engine_state=self._state.value",),
        log_kwargs_on_invoke=("crypto_codec=codec",),
        log_result_on_success=("value",),
    )
    @_error_processor
    async def attach_crypto_codec(self, codec: CryptoCodec) -> TcpStreamReconfigOutcome:
        """
        Attach a crypto codec to the public read/write path of the engine.

        This method atomically stores the provided codec reference under the
        engine lifecycle condition. It does not perform any stream I/O and does
        not coordinate protocol-level boundaries.

        In particular, this method does not:

          * wait for in-flight read/write activity to complete,
          * drain the underlying socket,
          * validate that the attach point is protocol-safe.

        These concerns are the responsibility of the caller.

        Behavior
        --------
        - Waits while the engine is in OPENING or CLOSING state.
        - If the engine is not OPENED, refuses the operation and returns
          AttachOutcome.REFUSED_CONNECTION_NOT_OPENED.
        - If a crypto codec is already attached, refuses the operation and
          returns AttachOutcome.REFUSED_ALREADY_ATTACHED.
        - Otherwise stores the codec in `_crypto_codec` and returns
          AttachOutcome.DONE.

        Return value
        ------------
        AttachOutcome
            One of:

            - ``AttachOutcome.DONE``:
                The codec was successfully attached.

            - ``AttachOutcome.REFUSED_CONNECTION_NOT_OPENED``:
                The engine was not in OPENED state, so the codec was not attached.

            - ``AttachOutcome.REFUSED_ALREADY_ATTACHED``:
                A codec was already attached, so this call made no changes.

        Notes
        -----
        - Attaching a codec changes only how subsequent public `read()` and
          `write()` calls are routed.
        - Ownership of the underlying TCP/TLS stream remains with
          `TcpStreamEngine`.
        """

        if codec is None:
            raise ValueError("argument 'codec' must not be None")

        if not isinstance(codec, CryptoCodec):
            raise TypeError("argument 'codec' must be an instance of 'CryptoCodec'")

        async with self._cond:
            while self._state in (
                EngineState.OPENING,
                EngineState.CLOSING,
                EngineState.RECONFIGURING,
            ):
                await self._cond.wait()

            if self._state is not EngineState.OPENED:
                return TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED

            if self._security_mode is TcpStreamSecurityMode.SSL:
                return TcpStreamReconfigOutcome.REFUSED_CONNECTION_ALREADY_UNDER_SSL

            if self._security_mode is TcpStreamSecurityMode.START_TLS:
                return TcpStreamReconfigOutcome.REFUSED_START_TLS_ALREADY_ACTIVE

            if self._crypto_codec is not None:
                return TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_ATTACHED

            self._crypto_codec = codec

            self._security_mode = TcpStreamSecurityMode.CODEC

            return TcpStreamReconfigOutcome.DONE

    @log_invocation(
        event="tcp_stream_engine.detach_crypto_codec",
        context_fields=("engine_state=self._state.value",),
        log_result_on_success=("value",),
    )
    @_error_processor
    async def detach_crypto_codec(self) -> TcpStreamReconfigOutcome:
        """
        Detach the currently attached crypto codec from the public read/write path.

        This method atomically clears the attached codec reference under the
        engine lifecycle condition. It does not perform any stream I/O and does
        not coordinate protocol-level boundaries.

        In particular, this method does not:

          * wait for in-flight read/write activity to complete,
          * drain the underlying socket,
          * validate that the detach point is protocol-safe.

        These concerns are the responsibility of the caller.

        Behavior
        --------
        - Waits while the engine is in OPENING state.
        - If the engine is not OPENED, refuses the operation and returns
          DetachOutcome.REFUSED_CONNECTION_NOT_OPENED.
        - If no crypto codec is currently attached, refuses the operation and
          returns DetachOutcome.REFUSED_NOT_ATTACHED.
        - Otherwise clears `_crypto_codec` and returns DetachOutcome.DONE.

        Return value
        ------------
        DetachOutcome
            One of:

            - ``DetachOutcome.DONE``:
                The codec was successfully detached.

            - ``DetachOutcome.REFUSED_CONNECTION_NOT_OPENED``:
                The engine was not in OPENED state, so no codec was detached.

            - ``DetachOutcome.REFUSED_NOT_ATTACHED``:
                No codec was attached, so this call made no changes.

        Notes
        -----
        - Detaching a codec changes only how subsequent public `read()` and
          `write()` calls are routed.
        - Any protocol-specific coordination required before detaching the codec
          must be handled by the caller.
        """
        async with self._cond:
            while self._state in (EngineState.OPENING, EngineState.RECONFIGURING):
                await self._cond.wait()

            if self._state is not EngineState.OPENED:
                return TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED

            if self._crypto_codec is None:
                return TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_NOT_ATTACHED

            self._crypto_codec = None
            self._security_mode = TcpStreamSecurityMode.PLAIN

            return TcpStreamReconfigOutcome.DONE

    @log_invocation(
        event="tcp_stream_engine.read",
        cancel_level=LogLevel.DEBUG,
        context_fields=("engine_state=self._state.value",),
        log_kwargs_on_invoke=(
            "read_max_bytes=n",
            "timeout_mode=mode.value",
            "timeout_override_s=socket_timeout_s",
        ),
        log_result_on_success=("read_bytes=len()",),
        log_error_policy=((SocketTimeoutError, False),),
    )
    @_error_processor
    async def read(
        self,
        n: int = 4096,
        *,
        mode: SocketTimeoutMode = SocketTimeoutMode.UNLIMITED,
        socket_timeout_s: float | None = None,
    ) -> bytes:
        """
        Read up to `n` plaintext bytes from the connection stream.

        Parameters
        ----------
        n
            Maximum number of plaintext bytes to return to the caller.
        mode
            Socket timeout mode for this operation:
              - SocketTimeoutMode.UNLIMITED:
                  Perform raw stream reads without an explicit timeout.
              - SocketTimeoutMode.LIMITED:
                  Wrap each raw read in asyncio.wait_for(...) with an effective
                  timeout.
        socket_timeout_s
            Per-call timeout override (seconds) used only when
            mode == SocketTimeoutMode.LIMITED.
            If None or <= 0, the engine default timeout (derived from
            remote_endpoint.info.socket_timeout_ms) is used.

        Returns
        -------
        bytes
            Up to `n` plaintext bytes from the public stream view exposed by the
            engine. Never returns b""; EOF is reported as an error.

        Behavior
        --------
        The method operates in one of two modes:

        - If no crypto codec is attached:
            delegates directly to `_read_raw(...)`.

        - If a crypto codec is attached:
            delegates to the codec, which obtains raw bytes through the provided
            callback and returns plaintext bytes to the caller.

        Raw socket I/O is always performed by `_read_raw(...)`, which preserves
        the transport-level timeout, EOF and I/O error semantics of the engine.

        Timeout handling
        ----------------
        - If a raw read times out in LIMITED mode:
            raises SocketTimeoutError with:
                - io_op_type=TCP_READ
                - engine_state=self._state
                - socket_timeout_mode=mode
                - socket_timeout_s=<effective timeout>
              and an additional detail "read_max_bytes" set to `n`.

        EOF / I/O errors
        ----------------
        - If the remote peer closes the connection during a raw read:
            the engine closes itself and raises TcpStreamRemotelyDisconnectedError.

        - If a raw I/O exception occurs:
            the engine closes itself and raises TcpStreamIoError with the mapped
            TcpStreamIoErrorReason.

        Errors
        ------
        TcpStreamEngineNotOpenError
            Raised when the engine is not OPENED or reader/writer are missing.
        TcpStreamEngineUnexpectedlyClosingError
            Raised when the writer exists but is already closing.
        SocketTimeoutError
            Raised on timeout in LIMITED mode. The engine remains open.
        TcpStreamRemotelyDisconnectedError
            Raised when the remote peer closes the connection (EOF); the
            engine is closed before raising.
        TcpStreamIoError
            Raised on non-timeout raw I/O exceptions; the engine is closed
            before raising.
        TcpStreamEngineUnexpectedError
            Any other unexpected exception escaping this method is wrapped
            into TcpStreamEngineUnexpectedError by @error_processor.

        Logging note
        ------------
        INVOKE/SUCCESS events for this method are emitted only under the
        "inspection" log profile. FAILED/CANCELLED events are always emitted.
        """

        if isinstance(n, bool) or not isinstance(n, int):
            raise TypeError("argument 'n' must be int when provided")

        if n <= 0:
            raise ValueError("argument 'n' must be positive when provided")

        if not isinstance(mode, SocketTimeoutMode):
            raise TypeError(
                "argument 'mode' must be an instance of 'SocketTimeoutMode' when provided"
            )

        if socket_timeout_s is not None:
            if isinstance(socket_timeout_s, bool) or not isinstance(
                socket_timeout_s,
                (int, float),
            ):
                raise TypeError("argument 'socket_timeout_s' must be float when provided")

            if socket_timeout_s <= 0:
                raise ValueError("argument 'socket_timeout_s' must be positive when provided")

        crypto_codec = self._crypto_codec
        if crypto_codec is not None:
            return await crypto_codec.read(
                lambda: self._read_raw(n, mode=mode, socket_timeout_s=socket_timeout_s)
            )

        return await self._read_raw(n, mode=mode, socket_timeout_s=socket_timeout_s)

    @log_invocation(
        event="tcp_stream_engine.write",
        cancel_level=LogLevel.DEBUG,
        context_fields=("engine_state=self._state.value",),
        log_kwargs_on_invoke=("write_bytes=data.len()",),
    )
    @_error_processor
    def write(self, data: bytes) -> None:
        """
        Buffer plaintext bytes for sending through the connection stream.

        Parameters
        ----------
        data
            Plaintext bytes to send.

        Behavior
        --------
        The method operates in one of two modes:

        - If no crypto codec is attached:
            delegates directly to `_write_raw(data)`.

        - If a crypto codec is attached:
            delegates to the codec, which may transform the plaintext payload
            and emit one or more raw chunks through `_write_raw(...)`.

        The method itself does not call `drain()`. Backpressure remains the
        responsibility of the caller via `drain()`.

        Raw write semantics
        -------------------
        `_write_raw(...)` writes directly to the underlying StreamWriter and
        preserves the transport-level validation and error mapping of the
        engine.

        - If `data` is empty, the operation is a no-op.

        - If the engine is not OPENED or the writer is missing:
            raises TcpStreamEngineNotOpenError.

        - If the writer exists but is already closing:
            raises TcpStreamEngineUnexpectedlyClosingError.

        - If a raw write fails:
            raises TcpStreamIoError with:
                - io_op_type=TCP_WRITE
                - engine_state=self._state
                - reason=<mapped reason>
                - cause=<original exception>.

        Errors
        ------
        TcpStreamEngineNotOpenError
            Raised when the engine is not in OPENED state or the writer is missing.
        TcpStreamEngineUnexpectedlyClosingError
            Raised when the writer exists but is already closing.
        TcpStreamIoError
            Raised when a raw writer.write() fails with an I/O-related exception.
        TcpStreamEngineUnexpectedError
            Any other unexpected exception escaping this method is wrapped
            into TcpStreamEngineUnexpectedError by @error_processor.

        Logging note
        ------------
        INVOKE/SUCCESS events for this method are emitted only under the
        "inspection" log profile. FAILED/CANCELLED events are always emitted.
        """
        if not isinstance(data, bytes):
            raise TypeError("argument 'data' must be bytes")

        if not data:
            return

        crypto_codec = self._crypto_codec
        if crypto_codec is not None:
            crypto_codec.write(self._write_raw, data)
            return

        self._write_raw(data)

    @log_invocation(
        event="tcp_stream_engine.drain",
        cancel_level=LogLevel.DEBUG,
        context_fields=("engine_state=self._state.value",),
        log_kwargs_on_invoke=(
            "timeout_mode=mode.value",
            "timeout_override_s=socket_timeout_s",
        ),
    )
    @_error_processor
    async def drain(
        self,
        *,
        mode: SocketTimeoutMode = SocketTimeoutMode.UNLIMITED,
        socket_timeout_s: float | None = None,
    ) -> None:
        """
        Flush the write buffer of the underlying TCP stream.

        Parameters
        ----------
        mode
            Socket timeout mode for this operation:
              - SocketTimeoutMode.UNLIMITED:
                  Perform writer.drain() without an explicit timeout.
              - SocketTimeoutMode.LIMITED:
                  Wrap writer.drain() in asyncio.wait_for(...) with an
                  effective timeout.
        socket_timeout_s
            Per-call timeout override (seconds) used only when
            mode == SocketTimeoutMode.LIMITED.
            If None or <= 0, the engine default timeout (derived from
            remote_endpoint.info.socket_timeout_ms) is used.

        Behavior
        --------
        - Calls _acquire_opened_streams(io_operation_type=TCP_DRAIN) to:
            * wait for any OPENING/CLOSING transition to finish,
            * verify that the engine is OPENED and has reader/writer,
            * obtain (reader, writer, default_timeout_s).
        - Computes the effective timeout for LIMITED mode:
            * if socket_timeout_s is not None and > 0, uses it,
            * otherwise uses default_timeout_s.
        - Performs the drain:
            * if mode == LIMITED:
                await asyncio.wait_for(writer.drain(), timeout=effective_timeout_s)
            * if mode == UNLIMITED:
                await writer.drain()

        Timeout handling
        ----------------
        - If asyncio.TimeoutError is raised in LIMITED mode:
            * does not close the engine,
            * raises SocketTimeoutError with:
                - io_op_type=TCP_DRAIN
                - engine_state=self._state
                - socket_timeout_mode=mode
                - socket_timeout_s=effective_timeout_s.

        I/O errors
        ----------
        - For any other Exception during drain():
            * maps the exception to a TcpStreamIoErrorReason via
              _map_io_exception_to_reason(),
            * captures the current engine state,
            * calls await close(),
            * raises TcpStreamIoError with:
                - io_op_type=TCP_DRAIN
                - engine_state=<captured state>
                - reason=<mapped reason>
                - cause=<original exception>.

        Errors
        ------
        TcpStreamEngineNotOpenError
            Raised by _acquire_opened_streams() if the engine is not OPENED
            or reader/writer are missing.
        TcpStreamEngineUnexpectedlyClosingError
            Raised by _acquire_opened_streams() if the writer exists but is
            already closing.
        SocketTimeoutError
            Raised on timeout in LIMITED mode. The engine remains open.
        TcpStreamIoError
            Raised on non-timeout I/O exceptions; the engine is closed
            before raising.
        TcpStreamEngineUnexpectedError
            Any other unexpected exception escaping this method is wrapped
            into TcpStreamEngineUnexpectedError by @error_processor.
        TlsError
            Raised on TLS-related errors; the engine is closed before raising.


        Logging note
        ------------
        INVOKE/SUCCESS events for this method are emitted only under the "inspection"
        log profile. FAILED/CANCELLED events are always emitted.
        """

        if not isinstance(mode, SocketTimeoutMode):
            raise TypeError(
                "argument 'mode' must be an instance of 'SocketTimeoutMode' when provided"
            )

        if socket_timeout_s is not None:
            if isinstance(socket_timeout_s, bool) or not isinstance(
                socket_timeout_s,
                (int, float),
            ):
                raise TypeError("argument 'socket_timeout_s' must be float when provided")

            if socket_timeout_s <= 0:
                raise ValueError("argument 'socket_timeout_s' must be positive when provided")

        _reader, writer, default_timeout_s = await self._acquire_opened_streams(
            io_operation_type=TCP_DRAIN
        )

        if socket_timeout_s is None:
            socket_timeout_s = default_timeout_s

        try:
            if mode == SocketTimeoutMode.LIMITED:
                await asyncio.wait_for(writer.drain(), timeout=socket_timeout_s)
            else:  # mode == SocketTimeoutMode.UNLIMITED:
                await writer.drain()

        except asyncio.TimeoutError:
            raise SocketTimeoutError(
                io_op_type=TCP_DRAIN,
                engine_state=self._state,
                socket_timeout_mode=mode,
                socket_timeout_s=socket_timeout_s,
            )

        except ssl.SSLError as e:
            reason_ssl = _map_ssl_exception_to_reason(e)
            state_before = self._state
            exc_ssl = TlsError(
                reason=reason_ssl,
                details={
                    "io_operation_type": TCP_DRAIN,
                    "engine_state_at_error": state_before.value,
                },
                cause=e,
            )
            await self._close_core(exc=exc_ssl)
            raise exc_ssl from e

        except Exception as e:
            reason_io = _map_io_exception_to_reason(e)
            state_before = self._state
            exc_io = TcpStreamIoError(
                io_op_type=TCP_DRAIN,
                engine_state=state_before,
                reason=reason_io,
                cause=e,
            )
            await self._close_core(exc=exc_io)
            raise exc_io from e

    # ---- Internals -----------------------------------------------------------------------

    async def _read_raw(
        self,
        n: int = 4096,
        *,
        mode: SocketTimeoutMode = SocketTimeoutMode.UNLIMITED,
        socket_timeout_s: float | None = None,
    ) -> bytes:
        """
        Read up to `n` bytes directly from the underlying raw stream.

        This helper bypasses the optional attached crypto codec and performs a
        transport-level read against the owned StreamReader.

        Parameters
        ----------
        n
            Maximum number of raw bytes to read from the underlying stream.
        mode
            Socket timeout mode for this raw read:
              - SocketTimeoutMode.UNLIMITED:
                  Perform the read without an explicit timeout.
              - SocketTimeoutMode.LIMITED:
                  Wrap the read in asyncio.wait_for(...) with an effective
                  timeout.
        socket_timeout_s
            Per-call timeout override (seconds) used only when
            mode == SocketTimeoutMode.LIMITED.
            If None or <= 0, the engine default timeout (derived from
            remote_endpoint.info.socket_timeout_ms) is used.

        Returns
        -------
        bytes
            Raw bytes read from the underlying stream. Never returns b"";
            EOF is reported as an error.

        Behavior
        --------
        - Acquires an OPENED raw stream snapshot via
          `_acquire_opened_streams(io_operation_type=TCP_READ)`.
        - Computes the effective timeout for LIMITED mode.
        - Performs `reader.read(n)` directly on the underlying StreamReader.

        Timeout handling
        ----------------
        - If asyncio.TimeoutError is raised in LIMITED mode:
            raises SocketTimeoutError with:
                - io_op_type=TCP_READ
                - engine_state=self._state
                - socket_timeout_mode=mode
                - socket_timeout_s=<effective timeout>
              and an additional detail "read_max_bytes" set to `n`.

        EOF / I/O errors
        ----------------
        - If the read result is b"" (EOF):
            captures the current state, closes the engine via `_close_core()`,
            and raises TcpStreamRemotelyDisconnectedError.

        - If any other exception occurs during the raw read:
            maps it to TcpStreamIoErrorReason, closes the engine via
            `_close_core()`, and raises TcpStreamIoError.

        Errors
        ------
        TcpStreamEngineNotOpenError
            Raised when the engine is not OPENED or reader/writer are missing.
        TcpStreamEngineUnexpectedlyClosingError
            Raised when the writer exists but is already closing.
        SocketTimeoutError
            Raised on timeout in LIMITED mode. The engine remains open.
        TcpStreamRemotelyDisconnectedError
            Raised when the remote peer closes the connection (EOF); the
            engine is closed before raising.
        TcpStreamIoError
            Raised on non-timeout raw I/O exceptions; the engine is closed
            before raising.
        TlsError
            Raised on TLS-related errors; the engine is closed before raising.
        """
        exc: NetError
        reader, _writer, default_timeout_s = await self._acquire_opened_streams(
            io_operation_type=TCP_READ
        )
        if socket_timeout_s is None or socket_timeout_s <= 0:
            socket_timeout_s = default_timeout_s

        try:
            if mode == SocketTimeoutMode.LIMITED:
                data = await asyncio.wait_for(reader.read(n), timeout=socket_timeout_s)
            else:  # mode == SocketTimeoutMode.UNLIMITED:
                data = await reader.read(n)
        except asyncio.TimeoutError:

            raise SocketTimeoutError(
                io_op_type=TCP_READ,
                engine_state=self._state,
                socket_timeout_mode=mode,
                socket_timeout_s=socket_timeout_s,
            ).with_detail("read_max_bytes", n)

        except asyncio.CancelledError as cancel_exc:
            log_context = self.get_log_context()
            if log_context is not None:
                # prevent logging cancellation error as it is a normal pathway
                log_context.mark_error_logged(cancel_exc)
            raise

        except ssl.SSLError as e:
            reason_ssl = _map_ssl_exception_to_reason(e)
            state_before = self._state
            exc = TlsError(
                reason=reason_ssl,
                details={
                    "io_operation_type": TCP_READ,
                    "engine_state_at_error": state_before.value,
                },
                cause=e,
            )
            await self._close_core(exc=exc)
            raise exc from e

        except Exception as e:
            reason_io = _map_io_exception_to_reason(e)
            state_before = self._state
            exc = TcpStreamIoError(
                io_op_type=TCP_READ,
                engine_state=state_before,
                reason=reason_io,
                cause=e,
            )
            await self._close_core(exc=exc)
            raise exc from e

        if data == b"":
            state_before = self._state
            exc = TcpStreamRemotelyDisconnectedError(
                engine_state=state_before,
            )
            await self._close_core(exc=exc)
            raise exc

        return data

    def _write_raw(self, data: bytes) -> None:
        """
        Write bytes directly into the underlying StreamWriter buffer.

        This helper bypasses the optional attached crypto codec and performs a
        transport-level raw write against the owned StreamWriter.

        Parameters
        ----------
        data
            Raw bytes to write into the underlying StreamWriter buffer.

        Behavior
        --------
        - If `data` is empty, returns immediately and does nothing.
        - Validates that the engine is OPENED and that a writer is present.
        - Validates that the writer is not already closing.
        - Calls `writer.write(data)` directly on the underlying StreamWriter.

        Notes
        -----
        - This helper does not call `drain()`.
        - It is used both by the public `write()` path when no crypto codec is
          attached and by attached crypto codecs when emitting transformed raw
          chunks.

        I/O errors
        ----------
        - If the engine is not OPENED or the writer is missing:
            raises TcpStreamEngineNotOpenError.

        - If the writer exists but is already closing:
            raises TcpStreamEngineUnexpectedlyClosingError.

        - If `writer.write(data)` raises an exception:
            maps it to TcpStreamIoErrorReason and raises TcpStreamIoError with:
                - io_op_type=TCP_WRITE
                - engine_state=self._state
                - reason=<mapped reason>
                - cause=<original exception>.

        Errors
        ------
        TcpStreamEngineNotOpenError
            Raised when the engine is not in OPENED state or the writer is missing.
        TcpStreamEngineUnexpectedlyClosingError
            Raised when the writer exists but is already closing.
        TcpStreamIoError
            Raised when the raw write fails with an I/O-related exception.
        TlsError
            Raised on TLS-related errors.
        """
        if not data:
            return

        writer = self._writer

        if self._state is not EngineState.OPENED or writer is None:
            raise TcpStreamEngineNotOpenError(
                io_op_type=TCP_WRITE,
                engine_state=self._state,
                is_reader=False if self._reader is None else True,
                is_writer=False if self._writer is None else True,
            )

        if writer.is_closing():
            raise TcpStreamEngineUnexpectedlyClosingError(
                io_op_type=TCP_WRITE,
                engine_state=self._state,
            )

        try:
            writer.write(data)

        except ssl.SSLError as e:
            reason_ssl = _map_ssl_exception_to_reason(e)
            raise TlsError(
                reason=reason_ssl,
                details={
                    "io_operation_type": TCP_WRITE,
                    "engine_state_at_error": self._state.value,
                },
                cause=e,
            ) from e

        except Exception as e:
            reason = _map_io_exception_to_reason(e)
            raise TcpStreamIoError(
                io_op_type=TCP_WRITE,
                engine_state=self._state,
                reason=reason,
                cause=e,
            ) from e

    @staticmethod
    async def _open_socket(
        info: RemoteEndpointConnectionInfoProto,
        address_info: AddrInfo,
        *,
        use_ssl: bool,
    ) -> tuple[
        asyncio.StreamReader,
        asyncio.StreamWriter,
    ]:
        """
        Open a single concrete address candidate and return an asyncio stream pair.

        This is an internal helper used by `open()` to try one `AddrInfo` candidate.
        It encapsulates the low-level socket lifecycle, optional local bind, async
        connect with a timeout, and optional TLS wrapping. Errors are mapped into a
        stable `OpenSocketError` where possible.

        Inputs
        ------
        info
            Logical connection parameters (timeouts, optional bind hints, TLS mode).
        address_info
            Concrete candidate tuple in `AddrInfo` form (family, socktype, proto, sockaddr, ...).

        Algorithm (stages)
        ------------------
        The function tracks the current stage using `OpenSocketStage` to classify
        failures:

        A) SOCKET_CREATE
            Create a socket using the candidate's `family/socktype/proto`.

        B) SOCKET_BIND (optional)
            If both `info.source_address` and `info.source_port_list` are provided,
            iterate ports in `source_port_list` and attempt:
              `sock.bind((info.source_address, port))`
            The first successful bind wins. If all bind attempts fail, the last bind
            exception is raised.

        C) SOCKET_CONNECT
            Switch the socket to non-blocking mode and connect asynchronously:
              `await asyncio.wait_for(loop.sock_connect(sock, sockaddr), timeout=connect_timeout_s)`
            On success, wrap the connected socket into asyncio streams:
              `reader, writer = await asyncio.open_connection(sock=sock)`

        D) SOCKET_WRAP_SSL (optional)
            If `info.tls.tls_mode == TLS_MODE_TLS`, upgrade the established stream by
            calling:
              `await wrap_stream_tls(info, writer)`

        Returns
        -------
        (reader, writer)
            A valid `(asyncio.StreamReader, asyncio.StreamWriter)` pair for the
            established connection (optionally TLS-wrapped).

        Cancellation
        ------------
        On `asyncio.CancelledError`, performs best-effort cleanup without awaiting
        `writer.wait_closed()` (if a writer was already created) and then re-raises
        the cancellation.

        Error mapping and cleanup
        -------------------------
        Any non-cancellation exception triggers stage-aware mapping via
        `_create_open_socket_error_raiser(info, address_info)`:

        - The mapper may raise `OpenSocketError(reason=..., candidate=..., details=..., cause=...)`
          based on the current stage and the original exception.
        - Regardless of whether mapping succeeds, cleanup is performed with
          `await_writer_close=True` (best-effort `writer.close()` + `await writer.wait_closed()`,
          or `sock.close()` if streams were not created).
        - If an `OpenSocketError` was produced by the mapper, it is raised;
          otherwise the original exception is re-raised.

        Notes
        -----
        - This helper does not log by itself.
        - No connection attempts beyond this single candidate are performed here;
          higher-level logic is responsible for iterating candidates and collecting
          per-candidate failures.
        """
        error_raiser = _create_open_socket_error_raiser(info, address_info)

        connect_timeout_s = info.connect_timeout_ms / 1000.0
        loop = asyncio.get_running_loop()

        sock: socket.socket | None = None
        writer: asyncio.StreamWriter | None = None

        family, socktype, proto, _canonname, sockaddr = address_info

        async def _clean_up(*, await_writer_close: bool) -> None:
            # Cleanup must depend on how far we got.
            if writer is not None:
                # noinspection PyBroadException
                try:
                    writer.close()
                    if await_writer_close:
                        await writer.wait_closed()
                except Exception:
                    pass
            elif sock is not None:
                # noinspection PyBroadException
                try:
                    sock.close()
                except Exception:
                    pass

        stage = _OpenSocketStage.SOCKET_CREATE
        try:
            # Phase A: socket creation

            sock = socket.socket(family, socktype, proto)

            assert sock is not None, "TcpStreamEngine._open_socket: socket creation should not fail"

            # Phase B: local bind
            info_source_address = info.source_address
            info_source_port_list = info.source_port_list

            if info_source_address and info_source_port_list:
                stage = _OpenSocketStage.SOCKET_BIND

                bound = False
                last_bind_exc: Exception | None = None
                for port in info_source_port_list:
                    try:
                        sock.bind((info.source_address, port))
                        bound = True
                        break
                    except Exception as be:
                        last_bind_exc = be

                if not bound:
                    assert last_bind_exc is not None
                    raise last_bind_exc

            # Phase C: connect (async)
            stage = _OpenSocketStage.SOCKET_CONNECT
            sock.setblocking(False)
            await asyncio.wait_for(
                loop.sock_connect(sock, sockaddr),
                timeout=connect_timeout_s,
            )
            reader, writer = await asyncio.open_connection(sock=sock)

            assert reader is not None, "TcpStreamEngine._open_socket: reader should not be None"
            assert writer is not None, "TcpStreamEngine._open_socket: writer should not be None"

            # Phase D: SSL wrapping (TLS)
            if use_ssl:
                stage = _OpenSocketStage.SOCKET_WRAP_SSL
                await wrap_stream_tls(info, writer)
            return reader, writer

        except asyncio.CancelledError:
            await _clean_up(await_writer_close=False)
            raise

        except Exception as exc:
            mapped: OpenSocketError | None = None
            try:
                error_raiser(stage, exc)
            except OpenSocketError as oe:
                mapped = oe
            await _clean_up(await_writer_close=True)
            if mapped is not None:
                raise mapped
            raise

    async def _close_core(
        self,
        *,
        exc: NetError | None = None,
        allow_when_reconfiguring: bool = False,
    ) -> TcpStreamCloseOutcome:
        """
        Internal close implementation used by both `close()` and abortive shutdown paths.

        This helper owns the entire shutdown state machine for the underlying stream.
        The public `close()` method is a thin wrapper that delegates to this function.
        In addition, I/O methods may call `_close_core(exc=...)` to close the engine
        as part of error handling (EOF / I/O failure).

        Parameters
        ----------
        exc
            Optional network error that caused the shutdown. When provided, the engine
            emits a debug event `tcp_stream_engine.abortive_close` with
            `due_to=exc.to_log_payload()` after the writer teardown logic has run.

        Behavior
        --------
        1) Acquire the internal condition and wait while the engine is OPENING.
           (`close` is not allowed to race with a half-open transition.)

        2) Handle concurrent closes:
           - If the engine is already CLOSING, wait until it leaves CLOSING and return
             ``CloseOutcome.NOT_OPENED`` (this call did not perform the shutdown).

        3) Idempotency:
           - If the engine is not OPENED, return ``CloseOutcome.NOT_OPENED``.


        4) Start closing from OPENED:
           - Set state to ``EngineState.CLOSING``.
           - Snapshot the current writer into a local variable.
           - Clear stored `_reader` / `_writer` references and detach any
             currently attached crypto codec immediately while still holding
             the condition (so new I/O cannot acquire the stream and no codec
             remains attached to a closing engine).

        5) Teardown outside the condition:
           - If a writer was present, call `writer.close()` and `await writer.wait_closed()`.

        6) Finalization (always runs, even on cancellation during wait_closed):
           - Re-acquire the condition.
           - If `exc` is provided, emit `tcp_stream_engine.abortive_close` (debug).
           - If state is still CLOSING, switch it to ``EngineState.CLOSED``.
           - Notify all condition waiters.

        Return value
        ------------
        CloseOutcome
            - ``CloseOutcome.CLOSED``:
                This call initiated the shutdown from OPENED and completed the close
                sequence (state ends in CLOSED).
            - ``CloseOutcome.NOT_OPENED``:
                The engine was not OPENED, or another close was already in progress and
                this call only waited for it to finish.

        Notes
        -----
        - This function does not suppress exceptions from `writer.wait_closed()`;
          such exceptions propagate to the caller, but the finalizer still runs and
          ensures the engine does not remain in CLOSING and that waiters are notified.
        - If a crypto codec is attached at the moment closing begins, it is
          detached as part of the state transition into CLOSING.
        """
        async with self._cond:
            while self._state is EngineState.OPENING or (
                self._state is EngineState.RECONFIGURING and not allow_when_reconfiguring
            ):
                await self._cond.wait()

            if self._state is EngineState.CLOSING:
                while self._state is EngineState.CLOSING:
                    await self._cond.wait()
                return TcpStreamCloseOutcome.NOT_OPENED

            if self._state not in (EngineState.OPENED, EngineState.RECONFIGURING):
                return TcpStreamCloseOutcome.NOT_OPENED

            self._state = EngineState.CLOSING
            writer = self._writer
            self._reader = None
            self._writer = None
            self._crypto_codec = None
            self._security_mode = TcpStreamSecurityMode.NOT_AVAILABLE

        try:
            if writer is not None:
                writer.close()
                await writer.wait_closed()
        finally:
            async with self._cond:
                if exc is not None:
                    log_context = self._log_context
                    if log_context is not None:
                        payload = {
                            "due_to": exc.to_log_payload(),
                        }
                        log_context.log_debug_event(
                            event="tcp_stream_engine.abortive_close",
                            payload=payload,
                            entity_id=self.identity,
                            skip_payload_normalization=True,
                        )

                # ensure we don't get stuck in CLOSING on cancellation
                if self._state is EngineState.CLOSING:
                    self._state = EngineState.CLOSED
                self._cond.notify_all()

        return TcpStreamCloseOutcome.CLOSED

    async def _acquire_opened_streams(
        self, *, io_operation_type: TcpIoOperation
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, float]:
        """
        Acquire a consistent snapshot of the opened stream objects for an I/O operation.

        This internal helper gates all I/O methods. It serializes against lifecycle
        transitions using the engine's condition, waits for transitional states to
        complete, validates invariants required for performing I/O, and returns a
        stable `(reader, writer, default_timeout_s)` snapshot to the caller.

        Parameters
        ----------
        io_operation_type
            The I/O operation identifier (e.g. TCP_READ / TCP_WRITE / TCP_DRAIN).
            It is embedded into raised errors so higher layers can attribute failures
            to the specific I/O type.

        Behavior
        --------
        1) Acquire the internal condition and wait while the engine is in a
           transitional state:
             - waits for OPENING / CLOSING to finish.

        2) Validate that the engine is usable for I/O:
             - state must be ``EngineState.OPENED``,
             - both `_reader` and `_writer` must be present.

           If any of these checks fails, raises `TcpStreamEngineNotOpenError` with:
             - `io_op_type=io_operation_type`
             - `engine_state=self._state`
             - `is_reader` / `is_writer` presence flags

        3) Snapshot the stream objects and compute the default timeout:
             - `reader = self._reader`
             - `writer = self._writer`
             - `default_timeout_s = self._remote_endpoint.info.socket_timeout_ms / 1000`

           The snapshot is taken while holding the condition; the lock is then released.

        4) After releasing the condition, guard against a closing writer:
             - if `writer.is_closing()` is True, raises
               `TcpStreamEngineUnexpectedlyClosingError` with:
                 - `io_op_type=io_operation_type`
                 - `engine_state=self._state`

        Returns
        -------
        (reader, writer, default_timeout_s)
            - `reader`: asyncio.StreamReader (non-None)
            - `writer`: asyncio.StreamWriter (non-None and not closing)
            - `default_timeout_s`: float, derived from `socket_timeout_ms`

        Notes
        -----
        - This helper does not perform any I/O itself; it only gates and validates.
        - The returned `default_timeout_s` is a derived value (ms -> seconds) and is
          used by read/drain timeout logic when per-call overrides are not provided
          or are non-positive.
        """
        async with self._cond:
            while self._state in (
                EngineState.OPENING,
                EngineState.CLOSING,
                EngineState.RECONFIGURING,
            ):
                await self._cond.wait()

            if (
                self._state is not EngineState.OPENED
                or self._reader is None
                or self._writer is None
            ):
                raise TcpStreamEngineNotOpenError(
                    io_op_type=io_operation_type,
                    engine_state=self._state,
                    is_reader=False if self._reader is None else True,
                    is_writer=False if self._writer is None else True,
                )

            reader = self._reader
            writer = self._writer
            default_timeout_s = self._remote_endpoint.info.socket_timeout_ms / 1000
            assert default_timeout_s is not None

        if writer.is_closing():
            raise TcpStreamEngineUnexpectedlyClosingError(
                io_op_type=io_operation_type,
                engine_state=self._state,
            )

        return reader, writer, default_timeout_s
