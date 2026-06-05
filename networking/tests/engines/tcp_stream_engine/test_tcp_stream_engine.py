# tests/engines/tcp_stream_engine/test_tcp_stream_engine.py
"""
Tests for mvx.networking.engines.tcp_stream_engine.tcp_stream_engine.TcpStreamEngine.

Grouping rule:
  - Group a: Constructor, properties, basic invariants
  - Group b: open() happy path, idempotency, argument validation
  - Group c: open() finalizer, cancellation, unexpected errors
  - Group d: close() behavior and idempotency
  - Group e: I/O gating via _acquire_opened_streams and is_open
  - Group f: read() semantics, timeouts, EOF, errors, cancellation, argument validation
  - Group g: write() semantics, no-op, errors, closing, argument validation
  - Group h: drain() semantics, timeouts, errors, cancellation, argument validation
  - Group i: _open_socket() stages, mapping, cleanup, cancellation
  - Group j: Races, stress, no leaks
  - Group k: crypto codec attach/detach and read/write routing
  - Group l: helper mappings
  - Group m: start_tls()
  - Group n: SSL/TLS I/O error handling
  - Group o: logging integration
  - Group p: metrics integration

Naming rule:
  Each test name starts with test_<group><num>_, e.g. test_a1_...
"""

from __future__ import annotations

from typing import Any, Optional, cast, Literal, Iterable
from collections.abc import Mapping
from dataclasses import dataclass

import asyncio
import errno
import socket
import ssl
from threading import RLock

import pytest

from mvx.common.helpers import CancellationPolicy

from mvx.common.logger import (
    LogContext,
    LogEvent,
    LogPayloadProcessor,
)

from mvx.networking.metrics.metrics_runtime.metrics_runtime import MetricsRuntime

from mvx.networking.helpers import RemoteEndpoint
from mvx.networking.metrics import Metric, MetricEvent
from mvx.networking.metrics.asyncio_metrics_recorder.metrics_recorder import (
    AsyncioMetricsRecorder,
)

from mvx.networking.engines.tcp_stream_engine.crypto_codec import CryptoCodec

# noinspection PyProtectedMember
from mvx.networking.engines.tcp_stream_engine.tcp_stream_engine import (
    TcpStreamEngine,
    TcpStreamOpenOutcome,
    TcpStreamCloseOutcome,
    TcpStreamReconfigOutcome,
    TcpStreamSecurityMode,
    _candidate_to_str,
    _map_io_exception_to_reason,
    _map_ssl_exception_to_reason,
)

from mvx.networking.models import (
    AddrInfo,
    EngineState,
    RemoteEndpointConnectionInfoProto,
    SocketTimeoutMode,
    TCP_DRAIN,
    TCP_READ,
    TCP_WRITE,
)

from mvx.networking.net_errors import (
    OpenConnectionError,
    OpenConnectionErrorReason,
    OpenSocketError,
    OpenSocketErrorReason,
    SocketTimeoutError,
    TcpStreamIoError,
    TcpStreamIoErrorReason,
    TcpStreamRemotelyDisconnectedError,
    TlsError,
    TlsErrorReason,
)

from mvx.networking.engines.tcp_stream_engine.errors import (
    TcpStreamEngineNotOpenError,
    TcpStreamEngineUnexpectedError,
    TcpStreamEngineUnexpectedlyClosingError,
)


from mvx.networking.engines.tcp_stream_engine.metrics import (
    TcpStreamOpenAttemptsMetric,
    TcpStreamOpenAttemptMetricEvent,
    TcpStreamOpenAttemptOutcome,
    TcpStreamCloseAttemptsMetric,
    TcpStreamCloseAttemptMetricEvent,
    TcpStreamCloseAttemptOutcome,
    TcpStreamStartTlsAttemptsMetric,
    TcpStreamStartTlsAttemptMetricEvent,
    TcpStreamStartTlsAttemptOutcome,
    TcpStreamCryptoCodecAttachAttemptsMetric,
    TcpStreamCryptoCodecAttachAttemptMetricEvent,
    TcpStreamCryptoCodecAttachAttemptOutcome,
    TcpStreamCryptoCodecDetachAttemptsMetric,
    TcpStreamCryptoCodecDetachAttemptMetricEvent,
    TcpStreamCryptoCodecDetachAttemptOutcome,
    TcpStreamStreamReadAttemptsMetric,
    TcpStreamStreamReadAttemptMetricEvent,
    TcpStreamStreamReadAttemptOutcome,
    TcpStreamStreamWriteAttemptsMetric,
    TcpStreamStreamWriteAttemptMetricEvent,
    TcpStreamStreamWriteAttemptOutcome,
    TcpStreamDrainAttemptsMetric,
    TcpStreamDrainAttemptMetricEvent,
    TcpStreamDrainAttemptOutcome,
    TcpStreamBytesReceivedMetric,
    TcpStreamBytesReceivedMetricEvent,
    TcpStreamBytesSentMetric,
    TcpStreamBytesSentMetricEvent,
    TcpStreamRemoteDisconnectMetric,
    TcpStreamRemoteDisconnectMetricEvent,
    TcpStreamAbortiveCloseMetric,
    TcpStreamAbortiveCloseMetricEvent,
)

# -------------------------
# Test fakes
# -------------------------


@dataclass(frozen=True, slots=True)
class _TlsInfo:
    tls_mode: Literal["OFF", "TLS", "STARTTLS"] = "OFF"
    ca_certs_file: Optional[str] = None
    ca_certs_path: Optional[str] = None
    ca_certs_data: Optional[str | bytes] = None
    client_cert_file: Optional[str] = None
    client_key_file: Optional[str] = None
    client_key_password: Optional[str] = None
    sni: Optional[str] = None
    valid_names: Optional[list[str]] = None


@dataclass(frozen=True, slots=True)
class _Info:
    host: str = "example.com"
    port: int = 389
    connect_timeout_ms: int = 1000
    socket_timeout_ms: int = 1000
    source_address: Optional[str] = None
    source_port_list: Optional[list[int]] = None
    tls: _TlsInfo = _TlsInfo()


class _FakeRemoteEndpoint(RemoteEndpoint):
    # noinspection PyMissingConstructor
    def __init__(self, info: RemoteEndpointConnectionInfoProto) -> None:
        self._info = info
        self.calls = 0
        self._candidates: list[AddrInfo] = []
        self._exc: BaseException | None = None
        self._wait: asyncio.Event | None = None

    @property
    def info(self) -> RemoteEndpointConnectionInfoProto:
        return self._info

    def set_candidates(self, candidates: list[AddrInfo]) -> None:
        self._candidates = list(candidates)

    def set_exception(self, exc: BaseException) -> None:
        self._exc = exc

    def set_wait_gate(self, ev: asyncio.Event) -> None:
        self._wait = ev

    async def get_candidate_addresses(self) -> list[AddrInfo]:
        self.calls += 1
        if self._wait is not None:
            await self._wait.wait()
        if self._exc is not None:
            raise self._exc
        return list(self._candidates)


class _FakeStreamReader:
    def __init__(self) -> None:
        self.read_calls: list[int] = []
        self._next_result: bytes | None = b"data"
        self._exc: BaseException | None = None
        self._gate: asyncio.Event | None = None

    def set_next(self, data: bytes) -> None:
        self._next_result = data
        self._exc = None

    def set_exc(self, exc: BaseException) -> None:
        self._exc = exc

    def set_gate(self, gate: asyncio.Event) -> None:
        self._gate = gate

    async def read(self, n: int) -> bytes:
        self.read_calls.append(n)
        if self._gate is not None:
            await self._gate.wait()
        if self._exc is not None:
            raise self._exc
        assert self._next_result is not None
        return self._next_result


class _FakeStreamWriter:
    def __init__(self) -> None:
        self.write_calls: list[bytes] = []
        self.drain_calls = 0
        self.close_calls = 0
        self.wait_closed_calls = 0
        self._closing = False
        self._write_exc: BaseException | None = None
        self._drain_exc: BaseException | None = None
        self._drain_gate: asyncio.Event | None = None
        self._wait_closed_gate: asyncio.Event | None = None

    def is_closing(self) -> bool:
        return self._closing

    def set_closing(self, value: bool) -> None:
        self._closing = value

    def set_write_exc(self, exc: BaseException) -> None:
        self._write_exc = exc

    def set_drain_exc(self, exc: BaseException) -> None:
        self._drain_exc = exc

    def set_wait_closed_gate(self, ev: asyncio.Event) -> None:
        self._wait_closed_gate = ev

    def write(self, data: bytes) -> None:
        if self._write_exc is not None:
            raise self._write_exc
        self.write_calls.append(data)

    def set_drain_gate(self, ev: asyncio.Event) -> None:
        self._drain_gate = ev

    async def drain(self) -> None:
        self.drain_calls += 1
        if self._drain_gate is not None:
            await self._drain_gate.wait()
        if self._drain_exc is not None:
            raise self._drain_exc

    def close(self) -> None:
        self.close_calls += 1
        self._closing = True

    async def wait_closed(self) -> None:
        self.wait_closed_calls += 1
        if self._wait_closed_gate is not None:
            await self._wait_closed_gate.wait()


class _FakeCryptoCodec(CryptoCodec):
    # noinspection PyMissingConstructor
    def __init__(self) -> None:
        self.read_calls = 0
        self.write_calls: list[bytes] = []
        self.read_payload: bytes = b"crypto-read-result"

        self.read_raw_results: list[bytes] = []
        self.read_exc: BaseException | None = None
        self.write_exc: BaseException | None = None

    async def read(self, reader: Any) -> bytes:
        self.read_calls += 1
        raw = await reader()
        self.read_raw_results.append(raw)
        if self.read_exc is not None:
            raise self.read_exc
        return self.read_payload

    def write(self, writer: Any, data: bytes) -> None:
        self.write_calls.append(data)
        if self.write_exc is not None:
            raise self.write_exc
        writer(b"crypto:" + data)


def _ai_inet(ip: str, port: int) -> AddrInfo:
    return socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port)


def _ai_inet6(ip: str, port: int) -> AddrInfo:
    return socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port, 0, 0)


@pytest.fixture()
def module_under_test():
    import mvx.networking.engines.tcp_stream_engine.tcp_stream_engine as m

    return m


@pytest.fixture()
def info_plain() -> RemoteEndpointConnectionInfoProto:
    return cast(RemoteEndpointConnectionInfoProto, _Info())


@pytest.fixture()
def info_tls() -> RemoteEndpointConnectionInfoProto:
    return cast(RemoteEndpointConnectionInfoProto, _Info())


@pytest.fixture()
def remote_endpoint(info_plain: RemoteEndpointConnectionInfoProto) -> _FakeRemoteEndpoint:
    return _FakeRemoteEndpoint(info_plain)


def _make_engine(remote: _FakeRemoteEndpoint) -> TcpStreamEngine:
    return TcpStreamEngine(remote_endpoint=cast(Any, remote))


# -------------------------
# Group a: Constructor, properties, basic invariants
# -------------------------


def test_a1_ctor_rejects_non_positive_socket_timeout_ms(module_under_test):
    """Constructor rejects non-positive socket_timeout_ms."""
    bad_info = cast(RemoteEndpointConnectionInfoProto, _Info(socket_timeout_ms=0))
    remote = _FakeRemoteEndpoint(bad_info)

    with pytest.raises(ValueError, match="socket_timeout_ms"):
        _ = TcpStreamEngine(remote_endpoint=remote)


@pytest.mark.asyncio
async def test_a2_state_initially_virgin(remote_endpoint: _FakeRemoteEndpoint):
    """New engine starts in VIRGIN."""
    eng = _make_engine(remote_endpoint)
    assert eng.state is EngineState.VIRGIN


@pytest.mark.asyncio
async def test_a3_is_open_false_initially(remote_endpoint: _FakeRemoteEndpoint):
    """New engine is not open."""
    eng = _make_engine(remote_endpoint)
    assert eng.is_open is False


def test_a4_identity_returns_explicit_entity_id(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """identity returns explicit entity_id."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        entity_id="engine-1",
    )

    assert eng.identity == "engine-1"


def test_a5_identity_strips_explicit_entity_id(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """identity strips explicit entity_id."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        entity_id="  engine-1  ",
    )

    assert eng.identity == "engine-1"


def test_a6_identity_is_generated_when_entity_id_is_missing(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """identity is generated when entity_id is missing."""

    class _FakeUuid:
        hex = "abcdef1234567890"

    monkeypatch.setattr(module_under_test, "uuid4", lambda: _FakeUuid())

    eng = TcpStreamEngine(remote_endpoint=cast(Any, remote_endpoint))

    assert eng.identity == "abcdef12"


def test_a7_identity_is_generated_when_entity_id_is_blank(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """identity is generated when entity_id is blank."""

    class _FakeUuid:
        hex = "1234567890abcdef"

    monkeypatch.setattr(module_under_test, "uuid4", lambda: _FakeUuid())

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        entity_id="   ",
    )

    assert eng.identity == "12345678"


@pytest.mark.asyncio
async def test_a8_stream_security_mode_returns_current_mode_immediately(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """stream_security_mode returns current security mode."""
    eng = _make_engine(remote_endpoint)
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    result = await eng.stream_security_mode

    assert result is TcpStreamSecurityMode.PLAIN


@pytest.mark.asyncio
async def test_a9_stream_security_mode_waits_reconfiguring_then_returns_mode(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """stream_security_mode waits while engine is RECONFIGURING."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING
        eng._security_mode = TcpStreamSecurityMode.PLAIN

    completed = asyncio.Event()

    async def get_mode() -> TcpStreamSecurityMode:
        _result = await eng.stream_security_mode
        completed.set()
        return _result

    task = asyncio.create_task(get_mode())
    await asyncio.sleep(0)

    assert completed.is_set() is False

    async with eng._cond:
        eng._state = EngineState.OPENED
        eng._security_mode = TcpStreamSecurityMode.START_TLS
        eng._cond.notify_all()

    result = await asyncio.wait_for(task, timeout=1.0)

    assert result is TcpStreamSecurityMode.START_TLS
    assert completed.is_set() is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transitional_state",
    [
        EngineState.OPENING,
        EngineState.CLOSING,
    ],
)
async def test_a10_stream_security_mode_waits_opening_or_closing_then_returns_mode(
    remote_endpoint: _FakeRemoteEndpoint,
    transitional_state: EngineState,
):
    """stream_security_mode waits while engine is OPENING or CLOSING."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = transitional_state
        eng._security_mode = TcpStreamSecurityMode.PLAIN

    completed = asyncio.Event()

    async def get_mode() -> TcpStreamSecurityMode:
        _result = await eng.stream_security_mode
        completed.set()
        return _result

    task = asyncio.create_task(get_mode())
    await asyncio.sleep(0)

    assert completed.is_set() is False

    async with eng._cond:
        eng._state = EngineState.OPENED
        eng._security_mode = TcpStreamSecurityMode.SSL
        eng._cond.notify_all()

    result = await asyncio.wait_for(task, timeout=1.0)

    assert result is TcpStreamSecurityMode.SSL
    assert completed.is_set() is True


def test_a11_ctor_rejects_none_remote_endpoint():
    """Constructor rejects None remote_endpoint."""
    with pytest.raises(ValueError, match="remote_endpoint"):
        _ = TcpStreamEngine(remote_endpoint=cast(Any, None))


def test_a12_ctor_rejects_non_remote_endpoint_instance():
    """Constructor rejects non-RemoteEndpoint remote_endpoint."""
    with pytest.raises(TypeError, match="remote_endpoint"):
        _ = TcpStreamEngine(remote_endpoint=cast(Any, object()))


def test_a13_ctor_rejects_non_log_context_instance(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Constructor rejects non-LogContext log_context."""
    with pytest.raises(TypeError, match="log_context"):
        _ = TcpStreamEngine(
            remote_endpoint=remote_endpoint,
            log_context=cast(Any, object()),
        )


@pytest.mark.parametrize(
    "bad_entity_id",
    [
        1,
        True,
        b"engine",
        object(),
    ],
)
def test_a14_ctor_rejects_non_string_entity_id(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_entity_id: object,
):
    """Constructor rejects non-string entity_id."""
    with pytest.raises(TypeError, match="entity_id"):
        _ = TcpStreamEngine(
            remote_endpoint=remote_endpoint,
            entity_id=cast(Any, bad_entity_id),
        )


# -------------------------
# Group b: open() happy path and idempotency
# -------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_use_ssl",
    [
        None,
        0,
        1,
        "true",
        object(),
    ],
)
async def test_b8_open_rejects_non_bool_use_ssl(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_use_ssl: object,
):
    """open() rejects non-bool use_ssl."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="use_ssl"):
        await eng.open(use_ssl=cast(Any, bad_use_ssl))

    assert eng.state is EngineState.VIRGIN
    assert remote_endpoint.calls == 0


@pytest.mark.asyncio
async def test_b2_open_idempotent_when_already_opened(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """open() is idempotent when already OPENED."""
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])
    eng = _make_engine(remote_endpoint)

    calls = 0

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        nonlocal calls
        calls += 1
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    first = await eng.open()
    second = await eng.open()

    assert calls == 1
    assert remote_endpoint.calls == 1
    assert eng.state is EngineState.OPENED

    assert first is TcpStreamOpenOutcome.OPENED
    assert second is TcpStreamOpenOutcome.ALREADY_OPENED


@pytest.mark.asyncio
async def test_b3_open_all_candidates_fail_raises_open_connection_error_with_list(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """open() raises OpenConnectionError with ordered per-candidate payloads."""
    c1 = _ai_inet("192.0.2.10", 389)
    c2 = _ai_inet("192.0.2.11", 389)
    remote_endpoint.set_candidates([c1, c2])
    eng = _make_engine(remote_endpoint)

    e1 = OpenSocketError(
        reason=OpenSocketErrorReason.SOCKET_CONNECT_FAILED,
        details={},
        candidate="c1",
        cause=OSError("x"),
    )
    e2 = OpenSocketError(
        reason=OpenSocketErrorReason.SOCKET_CONNECT_TIMEOUT,
        details={},
        candidate="c2",
        cause=asyncio.TimeoutError(),
    )

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, use_ssl
        if cand == c1:
            raise e1
        raise e2

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    with pytest.raises(OpenConnectionError) as ei:
        await eng.open()

    err = ei.value
    assert err.reason_code == OpenConnectionErrorReason.CONNECTION_TO_HOST_FAILURE.value

    cands = err.details.get("candidates")
    assert isinstance(cands, list)
    assert len(cands) == 2

    assert cands[0]["candidate"] == "c1"
    assert cands[1]["candidate"] == "c2"

    assert cands[0]["error"]["reason"] == OpenSocketErrorReason.SOCKET_CONNECT_FAILED.value
    assert cands[1]["error"]["reason"] == OpenSocketErrorReason.SOCKET_CONNECT_TIMEOUT.value

    assert eng.state is EngineState.ERROR


@pytest.mark.asyncio
async def test_b4_open_no_candidates_raises_host_cannot_be_resolved(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """open() raises HOST_CANNOT_BE_RESOLVED when candidates are empty."""
    remote_endpoint.set_candidates([])
    eng = _make_engine(remote_endpoint)

    with pytest.raises(OpenConnectionError) as ei:
        await eng.open()

    assert ei.value.reason_code == OpenConnectionErrorReason.HOST_CANNOT_BE_RESOLVED.value
    assert eng.state is EngineState.ERROR


@pytest.mark.asyncio
async def test_b5_open_never_leaves_opening_on_success(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """open() does not leave engine in OPENING after success."""
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])
    eng = _make_engine(remote_endpoint)

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    await eng.open()
    assert eng.state is EngineState.OPENED


@pytest.mark.asyncio
async def test_b6_open_waits_for_reconfiguring_to_finish_then_opens(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """open() waits while engine is RECONFIGURING."""
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING

    completed = asyncio.Event()

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    async def do_open() -> TcpStreamOpenOutcome:
        _result = await eng.open()
        completed.set()
        return _result

    task = asyncio.create_task(do_open())
    await asyncio.sleep(0)

    assert completed.is_set() is False

    async with eng._cond:
        eng._state = EngineState.CLOSED
        eng._cond.notify_all()

    result = await asyncio.wait_for(task, timeout=1.0)

    assert result is TcpStreamOpenOutcome.OPENED
    assert completed.is_set() is True
    assert eng.state is EngineState.OPENED


@pytest.mark.asyncio
async def test_b7_open_cancelled_while_waiting_reconfiguring_leaves_state_unchanged(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """open() cancellation while waiting for RECONFIGURING leaves state unchanged."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING
        eng._security_mode = TcpStreamSecurityMode.PLAIN

    task = asyncio.create_task(eng.open())
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert eng.state is EngineState.RECONFIGURING
    assert eng._security_mode is TcpStreamSecurityMode.PLAIN
    assert remote_endpoint.calls == 0


# -------------------------
# Group c: open() finalizer, cancellation, unexpected errors
# -------------------------


@pytest.mark.asyncio
async def test_c1_open_cancel_during_candidate_resolution_leaves_error_and_notifies(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Cancellation during resolution re-raises and finalizes state to ERROR."""
    gate = asyncio.Event()
    remote_endpoint.set_wait_gate(gate)
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])
    eng = _make_engine(remote_endpoint)

    waiter_started = asyncio.Event()
    waiter_released = asyncio.Event()

    async def waiter() -> None:
        async with eng._cond:
            waiter_started.set()
            await eng._cond.wait()
        waiter_released.set()

    wtask = asyncio.create_task(waiter())

    task = asyncio.create_task(eng.open())
    await asyncio.sleep(0)

    await waiter_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    gate.set()

    await asyncio.wait_for(waiter_released.wait(), timeout=1.0)
    assert eng.state is EngineState.ERROR

    # Waiter is expected to complete after notify_all().
    await wtask


@pytest.mark.asyncio
async def test_c2_open_cancel_while_waiting_to_commit_streams_closes_writer(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """Cancellation before commit closes writer and does not leave OPENING."""
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])
    eng = _make_engine(remote_endpoint)

    reader = _FakeStreamReader()
    writer = _FakeStreamWriter()

    opened = asyncio.Event()
    allow_return = asyncio.Event()

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        opened.set()
        await allow_return.wait()
        return reader, writer

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    task = asyncio.create_task(eng.open())

    await asyncio.wait_for(opened.wait(), timeout=1.0)

    await eng._lock.acquire()
    try:
        allow_return.set()
        await asyncio.sleep(0)
        task.cancel()
    finally:
        eng._lock.release()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1
    assert eng._reader is None
    assert eng._writer is None
    assert eng.state is EngineState.ERROR


@pytest.mark.asyncio
async def test_c3_open_unexpected_exception_is_wrapped(remote_endpoint: _FakeRemoteEndpoint):
    """Unexpected exception in open() is wrapped into TcpStreamEngineUnexpectedError."""
    remote_endpoint.set_exception(RuntimeError("boom"))
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TcpStreamEngineUnexpectedError) as ei:
        await eng.open()

    assert isinstance(ei.value.__cause__, RuntimeError)
    assert eng.state is EngineState.ERROR


@pytest.mark.asyncio
async def test_c4_open_finalizer_does_not_override_opened(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """Finalizer does not change OPENED back to ERROR."""
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])
    eng = _make_engine(remote_endpoint)

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    await eng.open()
    assert eng.state is EngineState.OPENED


# -------------------------
# Group d: close() behavior and idempotency
# -------------------------


@pytest.mark.asyncio
async def test_d1_close_when_virgin_is_noop(remote_endpoint: _FakeRemoteEndpoint):
    """close() is a no-op in VIRGIN."""
    eng = _make_engine(remote_endpoint)
    result = await eng.close()

    assert result is TcpStreamCloseOutcome.NOT_OPENED
    assert eng.state is EngineState.VIRGIN


@pytest.mark.asyncio
async def test_d2_close_when_closed_is_noop(remote_endpoint: _FakeRemoteEndpoint):
    """close() is a no-op in CLOSED."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.CLOSED
    result = await eng.close()

    assert result is TcpStreamCloseOutcome.NOT_OPENED
    assert eng.state is EngineState.CLOSED


@pytest.mark.asyncio
async def test_d3_close_when_opened_transitions_to_closed_and_clears_streams(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """close() on OPENED closes writer, clears streams, sets CLOSED."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    reader = _FakeStreamReader()
    writer = _FakeStreamWriter()
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, writer)

    result = await eng.close()

    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1
    assert eng._reader is None
    assert eng._writer is None
    assert eng.state is EngineState.CLOSED
    assert result is TcpStreamCloseOutcome.CLOSED
    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE


@pytest.mark.asyncio
async def test_d4_close_waits_for_opening_to_finish(remote_endpoint: _FakeRemoteEndpoint):
    """close() waits while state is OPENING."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.OPENING

    started = asyncio.Event()
    unblock = asyncio.Event()

    close_result: TcpStreamCloseOutcome | None = None

    async def do_close() -> None:
        nonlocal close_result
        started.set()
        close_result = await eng.close()
        unblock.set()

    t = asyncio.create_task(do_close())
    await started.wait()
    await asyncio.sleep(0)

    assert unblock.is_set() is False

    async with eng._cond:
        eng._state = EngineState.ERROR
        eng._cond.notify_all()

    await asyncio.wait_for(unblock.wait(), timeout=1.0)
    assert close_result is TcpStreamCloseOutcome.NOT_OPENED
    assert eng.state is EngineState.ERROR

    await t


@pytest.mark.asyncio
async def test_d5_two_concurrent_close_calls_only_one_waits_for_writer(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Concurrent close() calls serialize and only one closes the writer."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    gate = asyncio.Event()
    writer.set_wait_closed_gate(gate)
    eng._writer = cast(Any, writer)

    t1 = asyncio.create_task(eng.close())
    t2 = asyncio.create_task(eng.close())

    await asyncio.sleep(0)
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1

    gate.set()
    results = await asyncio.gather(t1, t2)

    assert results.count(TcpStreamCloseOutcome.CLOSED) == 1
    assert results.count(TcpStreamCloseOutcome.NOT_OPENED) == 1

    assert eng.state is EngineState.CLOSED


@pytest.mark.asyncio
async def test_d6_close_cancellation_during_wait_closed_still_sets_closed_and_notifies(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Cancellation during wait_closed still finalizes to CLOSED."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    gate = asyncio.Event()
    writer.set_wait_closed_gate(gate)
    eng._writer = cast(Any, writer)

    task = asyncio.create_task(eng.close())
    await asyncio.sleep(0)

    task.cancel()
    gate.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert eng.state is EngineState.CLOSED
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1
    assert eng._reader is None
    assert eng._writer is None


@pytest.mark.asyncio
async def test_d7_close_waits_for_reconfiguring_to_finish(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """close() waits while engine is RECONFIGURING."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING

    completed = asyncio.Event()
    close_result: TcpStreamCloseOutcome | None = None

    async def do_close() -> None:
        nonlocal close_result
        close_result = await eng.close()
        completed.set()

    task = asyncio.create_task(do_close())
    await asyncio.sleep(0)

    assert completed.is_set() is False

    async with eng._cond:
        eng._state = EngineState.ERROR
        eng._cond.notify_all()

    await asyncio.wait_for(completed.wait(), timeout=1.0)
    await task

    assert close_result is TcpStreamCloseOutcome.NOT_OPENED
    assert eng.state is EngineState.ERROR


# -------------------------
# Group e: I/O gating via _acquire_opened_streams and is_open
# -------------------------


@pytest.mark.asyncio
async def test_e1_acquire_streams_raises_not_open_when_not_opened(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """_acquire_opened_streams raises when engine is not OPENED."""
    eng = _make_engine(remote_endpoint)
    with pytest.raises(TcpStreamEngineNotOpenError):
        await eng._acquire_opened_streams(io_operation_type=TCP_READ)


@pytest.mark.asyncio
async def test_e2_acquire_streams_waits_transitional_states_then_proceeds(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """_acquire_opened_streams waits for OPENING/CLOSING to finish."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.OPENING

    done = asyncio.Event()

    async def acq() -> None:
        with pytest.raises(TcpStreamEngineNotOpenError):
            await eng._acquire_opened_streams(io_operation_type=TCP_READ)
        done.set()

    t = asyncio.create_task(acq())
    await asyncio.sleep(0)

    async with eng._cond:
        eng._state = EngineState.CLOSED
        eng._cond.notify_all()

    await asyncio.wait_for(done.wait(), timeout=1.0)
    await t


@pytest.mark.asyncio
async def test_e3_acquire_streams_raises_unexpectedly_closing_when_writer_is_closing(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Writer closing in OPENED triggers TcpStreamEngineUnexpectedlyClosingError."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    writer.set_closing(True)
    eng._writer = cast(Any, writer)

    with pytest.raises(TcpStreamEngineUnexpectedlyClosingError):
        await eng._acquire_opened_streams(io_operation_type=TCP_READ)


@pytest.mark.asyncio
async def test_e4_is_open_true_only_when_opened_and_writer_not_closing(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """is_open is true only for OPENED with non-closing writer."""
    eng = _make_engine(remote_endpoint)
    assert eng.is_open is False

    writer = _FakeStreamWriter()
    eng._state = EngineState.OPENED
    eng._writer = cast(Any, writer)
    assert eng.is_open is True

    writer.set_closing(True)
    assert eng.is_open is False

    eng._state = EngineState.CLOSED
    assert eng.is_open is False


@pytest.mark.asyncio
async def test_e5_acquire_streams_default_timeout_is_derived_from_info(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Default timeout is socket_timeout_ms / 1000."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    _r, _w, timeout_s = await eng._acquire_opened_streams(io_operation_type=TCP_READ)
    assert timeout_s == remote_endpoint.info.socket_timeout_ms / 1000


@pytest.mark.asyncio
async def test_e6_acquire_streams_waits_reconfiguring_then_raises_not_open(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """_acquire_opened_streams waits for RECONFIGURING to finish."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING

    done = asyncio.Event()

    async def acq() -> None:
        with pytest.raises(TcpStreamEngineNotOpenError):
            await eng._acquire_opened_streams(io_operation_type=TCP_READ)
        done.set()

    task = asyncio.create_task(acq())
    await asyncio.sleep(0)

    assert done.is_set() is False

    async with eng._cond:
        eng._state = EngineState.CLOSED
        eng._cond.notify_all()

    await asyncio.wait_for(done.wait(), timeout=1.0)
    await task


def test_e7_is_open_false_while_reconfiguring(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """is_open is false while engine is RECONFIGURING."""
    eng = _make_engine(remote_endpoint)
    writer = _FakeStreamWriter()

    eng._state = EngineState.RECONFIGURING
    eng._writer = cast(Any, writer)

    assert eng.is_open is False


# -------------------------
# Group f: read() semantics
# -------------------------


@pytest.mark.asyncio
async def test_f1_read_unlimited_success_returns_bytes(remote_endpoint: _FakeRemoteEndpoint):
    """read(UNLIMITED) returns bytes."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    reader = _FakeStreamReader()
    reader.set_next(b"hello")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    # noinspection PyArgumentEqualDefault
    data = await eng.read(5, mode=SocketTimeoutMode.UNLIMITED)
    assert data == b"hello"
    assert reader.read_calls == [5]


@pytest.mark.asyncio
async def test_f2_read_limited_uses_default_timeout_when_override_none(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """read(LIMITED) uses default timeout when override is None."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    reader = _FakeStreamReader()
    reader.set_next(b"x")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    observed: dict[str, Any] = {}

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        observed["timeout"] = timeout
        return await awaitable

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    # noinspection PyArgumentEqualDefault
    _ = await eng.read(1, mode=SocketTimeoutMode.LIMITED, socket_timeout_s=None)

    assert observed["timeout"] == remote_endpoint.info.socket_timeout_ms / 1000


@pytest.mark.asyncio
async def test_f3_read_limited_uses_override_timeout_when_positive(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """read(LIMITED) uses positive per-call override timeout."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    reader = _FakeStreamReader()
    reader.set_next(b"x")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    observed: dict[str, Any] = {}

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        observed["timeout"] = timeout
        return await awaitable

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    _ = await eng.read(1, mode=SocketTimeoutMode.LIMITED, socket_timeout_s=12.5)
    assert observed["timeout"] == 12.5


@pytest.mark.asyncio
async def test_f4_read_timeout_raises_socket_timeout_error_and_does_not_close_engine(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """Read timeout raises SocketTimeoutError and keeps engine open."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    reader = _FakeStreamReader()
    reader.set_next(b"x")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    with pytest.raises(SocketTimeoutError) as ei:
        await eng.read(10, mode=SocketTimeoutMode.LIMITED, socket_timeout_s=1.0)

    err = ei.value
    assert err.reason_code == "SOCKET_TIMEOUT"
    assert err.details["io_operation_type"] == TCP_READ
    assert err.details["socket_timeout_mode"] == SocketTimeoutMode.LIMITED
    assert err.details["socket_timeout_s"] == 1.0
    assert err.details["read_max_bytes"] == 10

    assert eng.state is EngineState.OPENED
    assert eng.is_open is True


@pytest.mark.asyncio
async def test_f5_read_eof_closes_engine_and_raises_remotely_disconnected(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """EOF triggers close() and raises TcpStreamRemotelyDisconnectedError."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    reader = _FakeStreamReader()
    reader.set_next(b"")
    writer = _FakeStreamWriter()
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, writer)

    with pytest.raises(TcpStreamRemotelyDisconnectedError) as ei:
        # noinspection PyArgumentEqualDefault
        await eng.read(1024, mode=SocketTimeoutMode.UNLIMITED)

    err = ei.value
    assert err.reason_code == "TCP_STREAM_REMOTELY_DISCONNECTED"
    assert err.details["io_operation_type"] == TCP_READ
    assert err.details["engine_state_at_error"] == EngineState.OPENED.value

    assert eng.state is EngineState.CLOSED
    assert eng._reader is None
    assert eng._writer is None
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1


@pytest.mark.asyncio
async def test_f6_read_io_exception_maps_reason_closes_engine_and_raises_tcp_stream_io_error(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Read exception maps reason, closes engine, raises TcpStreamIoError."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    reader = _FakeStreamReader()
    reader.set_exc(ConnectionResetError("reset"))
    eng._reader = cast(Any, reader)

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    with pytest.raises(TcpStreamIoError) as ei:
        # noinspection PyArgumentEqualDefault
        await eng.read(1, mode=SocketTimeoutMode.UNLIMITED)

    err = ei.value
    assert err.reason_code == TcpStreamIoErrorReason.TCP_STREAM_CONNECTION_RESET.value
    assert err.details["io_operation_type"] == TCP_READ
    assert err.details["engine_state_at_error"] == EngineState.OPENED.value

    assert eng.state is EngineState.CLOSED

    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1


@pytest.mark.asyncio
async def test_f7_read_cancelled_error_propagates_and_is_not_wrapped(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """CancelledError during read propagates unchanged."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    gate = asyncio.Event()
    reader = _FakeStreamReader()
    reader.set_gate(gate)
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    # noinspection PyArgumentEqualDefault
    task = asyncio.create_task(eng.read(1, mode=SocketTimeoutMode.UNLIMITED))
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    gate.set()

    assert eng.state is EngineState.OPENED
    assert eng.is_open is True


@pytest.mark.asyncio
async def test_f8_read_limited_cancelled_during_wait_for_propagates_and_keeps_open(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """read(LIMITED) cancellation inside wait_for propagates and keeps engine open."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED

    reader = _FakeStreamReader()
    reader.set_next(b"x")
    eng._reader = cast(Any, reader)

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.CancelledError

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    with pytest.raises(asyncio.CancelledError):
        await eng.read(1, mode=SocketTimeoutMode.LIMITED, socket_timeout_s=1.0)

    assert eng.state is EngineState.OPENED
    assert eng.is_open is True
    assert eng._reader is reader
    assert eng._writer is writer
    assert writer.close_calls == 0
    assert writer.wait_closed_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_n",
    [
        None,
        True,
        False,
        1.5,
        "1",
        object(),
    ],
)
async def test_f9_read_rejects_invalid_n_type(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_n: object,
):
    """read() rejects non-int n and bool n."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="n"):
        await eng.read(cast(Any, bad_n))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_n",
    [
        0,
        -1,
    ],
)
async def test_f10_read_rejects_non_positive_n(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_n: int,
):
    """read() rejects non-positive n."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(ValueError, match="n"):
        await eng.read(bad_n)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_mode",
    [
        None,
        "LIMITED",
        object(),
    ],
)
async def test_f11_read_rejects_invalid_timeout_mode(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_mode: object,
):
    """read() rejects non-SocketTimeoutMode mode."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="mode"):
        await eng.read(1, mode=cast(Any, bad_mode))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_timeout",
    [
        True,
        False,
        "1.0",
        object(),
    ],
)
async def test_f12_read_rejects_invalid_socket_timeout_type(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_timeout: object,
):
    """read() rejects non-numeric socket_timeout_s and bool socket_timeout_s."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="socket_timeout_s"):
        await eng.read(1, socket_timeout_s=cast(Any, bad_timeout))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_timeout",
    [
        0,
        -1,
        -0.1,
    ],
)
async def test_f13_read_rejects_non_positive_socket_timeout(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_timeout: float,
):
    """read() rejects non-positive socket_timeout_s."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(ValueError, match="socket_timeout_s"):
        await eng.read(1, socket_timeout_s=bad_timeout)


# -------------------------
# Group g: write() semantics
# -------------------------


def test_g1_write_empty_payload_is_noop(remote_endpoint: _FakeRemoteEndpoint):
    """write(b'') is a no-op."""
    eng = _make_engine(remote_endpoint)

    eng.write(b"")

    assert eng.state is EngineState.VIRGIN


def test_g2_write_not_open_raises_not_open_error_with_flags(remote_endpoint: _FakeRemoteEndpoint):
    """write() when not OPENED raises TcpStreamEngineNotOpenError."""
    eng = _make_engine(remote_endpoint)
    with pytest.raises(TcpStreamEngineNotOpenError) as ei:
        eng.write(b"x")

    err = ei.value
    assert err.details["io_operation_type"] == TCP_WRITE
    assert err.details["engine_state_at_error"] == EngineState.VIRGIN.value
    assert err.details["is_reader"] is False
    assert err.details["is_writer"] is False


def test_g3_write_writer_is_closing_raises_unexpectedly_closing(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """write() raises when writer is closing."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    writer.set_closing(True)
    eng._writer = cast(Any, writer)

    with pytest.raises(TcpStreamEngineUnexpectedlyClosingError) as ei:
        eng.write(b"x")

    err = ei.value
    assert err.details["io_operation_type"] == TCP_WRITE
    assert err.details["engine_state_at_error"] == EngineState.OPENED.value


def test_g4_write_success_calls_writer_write_once(remote_endpoint: _FakeRemoteEndpoint):
    """write() buffers data into writer."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    eng.write(b"abc")
    assert writer.write_calls == [b"abc"]
    assert writer.drain_calls == 0


def test_g5_write_exception_maps_reason_and_raises_tcp_stream_io_error(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """write() exception maps reason and raises TcpStreamIoError."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    writer.set_write_exc(BrokenPipeError("pipe"))
    eng._writer = cast(Any, writer)

    with pytest.raises(TcpStreamIoError) as ei:
        eng.write(b"abc")

    err = ei.value
    assert err.reason_code == TcpStreamIoErrorReason.TCP_STREAM_BROKEN_PIPE.value
    assert err.details["io_operation_type"] == TCP_WRITE
    assert err.details["engine_state_at_error"] == EngineState.OPENED.value

    # write() does not close the engine on error in this implementation.
    assert eng.state is EngineState.OPENED


def test_g6_write_while_reconfiguring_raises_not_open(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """write() is refused while engine is RECONFIGURING."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.RECONFIGURING
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    with pytest.raises(TcpStreamEngineNotOpenError) as ei:
        eng.write(b"x")

    err = ei.value
    assert err.details["io_operation_type"] == TCP_WRITE
    assert err.details["engine_state_at_error"] == EngineState.RECONFIGURING.value
    assert err.details["is_reader"] is True
    assert err.details["is_writer"] is True


@pytest.mark.parametrize(
    "bad_data",
    [
        None,
        "abc",
        bytearray(b"abc"),
        memoryview(b"abc"),
        object(),
    ],
)
def test_g7_write_rejects_non_bytes_data(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_data: object,
):
    """write() rejects non-bytes data."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="data"):
        eng.write(cast(Any, bad_data))

    assert eng.state is EngineState.VIRGIN


# -------------------------
# Group h: drain() semantics
# -------------------------


@pytest.mark.asyncio
async def test_h1_drain_unlimited_success(remote_endpoint: _FakeRemoteEndpoint):
    """drain(UNLIMITED) calls writer.drain()."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    # noinspection PyArgumentEqualDefault
    await eng.drain(mode=SocketTimeoutMode.UNLIMITED)
    assert writer.drain_calls == 1
    assert eng.state is EngineState.OPENED
    assert eng.is_open is True


@pytest.mark.asyncio
async def test_h2_drain_limited_uses_default_timeout_when_override_none(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """drain(LIMITED) uses default timeout when override is None."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    observed: dict[str, Any] = {}

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        observed["timeout"] = timeout
        return await awaitable

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    # noinspection PyArgumentEqualDefault
    await eng.drain(mode=SocketTimeoutMode.LIMITED, socket_timeout_s=None)

    assert observed["timeout"] == remote_endpoint.info.socket_timeout_ms / 1000


@pytest.mark.asyncio
async def test_h3_drain_limited_timeout_raises_socket_timeout_error_and_does_not_close_engine(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """Drain timeout raises SocketTimeoutError and keeps engine open."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)
    monkeypatch.setattr(module_under_test, "wait_for", fake_wait_for, raising=False)

    with pytest.raises(SocketTimeoutError) as ei:
        await eng.drain(mode=SocketTimeoutMode.LIMITED, socket_timeout_s=1.0)

    err = ei.value
    assert err.reason_code == "SOCKET_TIMEOUT"
    assert err.details["io_operation_type"] == TCP_DRAIN
    assert err.details["socket_timeout_mode"] == SocketTimeoutMode.LIMITED
    assert err.details["socket_timeout_s"] == 1.0

    assert eng.state is EngineState.OPENED
    assert eng.is_open is True


@pytest.mark.asyncio
async def test_h4_drain_exception_maps_reason_closes_engine_and_raises_tcp_stream_io_error(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Drain exception maps reason, closes engine, raises TcpStreamIoError."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    writer.set_drain_exc(OSError(errno.EPIPE, "pipe"))
    eng._writer = cast(Any, writer)

    with pytest.raises(TcpStreamIoError) as ei:
        # noinspection PyArgumentEqualDefault
        await eng.drain(mode=SocketTimeoutMode.UNLIMITED)

    err = ei.value
    assert err.details["io_operation_type"] == TCP_DRAIN
    assert err.details["engine_state_at_error"] == EngineState.OPENED.value
    assert err.reason_code == TcpStreamIoErrorReason.TCP_STREAM_BROKEN_PIPE.value

    assert eng.state is EngineState.CLOSED
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1
    assert eng._reader is None
    assert eng._writer is None


@pytest.mark.asyncio
async def test_h5_drain_cancelled_error_propagates_and_keeps_engine_open(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """CancelledError during drain propagates unchanged and does not close the engine."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    gate = asyncio.Event()
    writer = _FakeStreamWriter()
    writer.set_drain_gate(gate)
    eng._writer = cast(Any, writer)

    # noinspection PyArgumentEqualDefault
    task = asyncio.create_task(eng.drain(mode=SocketTimeoutMode.UNLIMITED))
    await asyncio.sleep(0)

    assert writer.drain_calls == 1

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert eng.state is EngineState.OPENED
    assert eng.is_open is True
    assert eng._writer is writer
    assert writer.close_calls == 0
    assert writer.wait_closed_calls == 0


@pytest.mark.asyncio
async def test_h6_drain_waits_reconfiguring_then_raises_not_open(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """drain() waits while engine is RECONFIGURING."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING

    completed = asyncio.Event()

    async def do_drain() -> None:
        with pytest.raises(TcpStreamEngineNotOpenError):
            await eng.drain()
        completed.set()

    task = asyncio.create_task(do_drain())
    await asyncio.sleep(0)

    assert completed.is_set() is False

    async with eng._cond:
        eng._state = EngineState.CLOSED
        eng._cond.notify_all()

    await asyncio.wait_for(completed.wait(), timeout=1.0)
    await task


@pytest.mark.asyncio
async def test_h7_drain_limited_cancelled_during_wait_for_propagates_and_keeps_open(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """drain(LIMITED) cancellation inside wait_for propagates and keeps engine open."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.CancelledError

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    with pytest.raises(asyncio.CancelledError):
        await eng.drain(mode=SocketTimeoutMode.LIMITED, socket_timeout_s=1.0)

    assert eng.state is EngineState.OPENED
    assert eng.is_open is True
    assert eng._writer is writer
    assert writer.close_calls == 0
    assert writer.wait_closed_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_mode",
    [
        None,
        "LIMITED",
        object(),
    ],
)
async def test_h8_drain_rejects_invalid_timeout_mode(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_mode: object,
):
    """drain() rejects non-SocketTimeoutMode mode."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="mode"):
        await eng.drain(mode=cast(Any, bad_mode))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_timeout",
    [
        True,
        False,
        "1.0",
        object(),
    ],
)
async def test_h9_drain_rejects_invalid_socket_timeout_type(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_timeout: object,
):
    """drain() rejects non-numeric socket_timeout_s and bool socket_timeout_s."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="socket_timeout_s"):
        await eng.drain(socket_timeout_s=cast(Any, bad_timeout))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_timeout",
    [
        0,
        -1,
        -0.1,
    ],
)
async def test_h10_drain_rejects_non_positive_socket_timeout(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_timeout: float,
):
    """drain() rejects non-positive socket_timeout_s."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(ValueError, match="socket_timeout_s"):
        await eng.drain(socket_timeout_s=bad_timeout)


# -------------------------
# Group i: _open_socket() stages, mapping, cleanup, cancellation
# -------------------------


class _FakeSocket:
    def __init__(self) -> None:
        self.bind_calls: list[tuple[Any, ...]] = []
        self.closed = False
        self.blocking: Optional[bool] = None
        self._bind_exc_by_port: dict[int, BaseException] = {}

    def set_bind_exc(self, port: int, exc: BaseException) -> None:
        self._bind_exc_by_port[port] = exc

    def setblocking(self, flag: bool) -> None:
        self.blocking = flag

    def bind(self, addr: tuple[str, int]) -> None:
        self.bind_calls.append(addr)
        port = addr[1]
        if port in self._bind_exc_by_port:
            raise self._bind_exc_by_port[port]

    def close(self) -> None:
        self.closed = True


class _FakeLoop:
    def __init__(self) -> None:
        self.sock_connect_calls: list[tuple[Any, Any]] = []
        self._sock_connect_exc: BaseException | None = None
        self._sock_connect_gate: asyncio.Event | None = None

    def set_sock_connect_exc(self, exc: BaseException) -> None:
        self._sock_connect_exc = exc

    def set_sock_connect_gate(self, gate: asyncio.Event) -> None:
        self._sock_connect_gate = gate

    async def sock_connect(self, sock: Any, sockaddr: Any) -> None:
        self.sock_connect_calls.append((sock, sockaddr))
        if self._sock_connect_gate is not None:
            await self._sock_connect_gate.wait()
        if self._sock_connect_exc is not None:
            raise self._sock_connect_exc


@pytest.mark.asyncio
async def test_i1_open_socket_success_no_tls_returns_reader_writer(
    info_plain, module_under_test, monkeypatch
):
    """_open_socket succeeds without TLS and returns streams."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    r = _FakeStreamReader()
    w = _FakeStreamWriter()

    async def fake_open_connection(*, sock: Any) -> Any:
        assert sock is fake_sock
        return r, w

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    cand = _ai_inet("192.0.2.10", 389)
    reader, writer = await TcpStreamEngine._open_socket(
        info_plain,
        cand,
        use_ssl=False,
    )

    assert reader is r
    assert writer is w
    assert fake_sock.blocking is False
    assert fake_loop.sock_connect_calls == [(fake_sock, ("192.0.2.10", 389))]


@pytest.mark.asyncio
async def test_i2_open_socket_use_ssl_true_calls_wrap_stream_tls(
    info_tls, module_under_test, monkeypatch
):
    """_open_socket calls wrap_stream_tls when use_ssl=True."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    r = _FakeStreamReader()
    w = _FakeStreamWriter()

    async def fake_open_connection(*, sock: Any) -> Any:
        _ = sock
        return r, w

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    called = {"n": 0}

    async def fake_wrap(
        info: Any, _writer: Any, *, handshake_timeout_s: float | None = None
    ) -> None:
        _ = info, handshake_timeout_s
        called["n"] += 1
        assert _writer is w

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap)

    cand = _ai_inet("192.0.2.10", 389)

    reader, writer = await TcpStreamEngine._open_socket(
        info_tls,
        cand,
        use_ssl=True,
    )

    assert reader is r
    assert writer is w
    assert called["n"] == 1

    assert fake_loop.sock_connect_calls == [(fake_sock, ("192.0.2.10", 389))]
    assert fake_sock.blocking is False


@pytest.mark.asyncio
async def test_i3_open_socket_cancelled_during_connect_closes_socket_no_wait_closed(
    info_plain, module_under_test, monkeypatch
):
    """Cancellation during connect closes socket and re-raises CancelledError."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()
    gate = asyncio.Event()
    fake_loop.set_sock_connect_gate(gate)

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    async def fake_open_connection(*, sock: Any) -> Any:
        _ = sock
        raise AssertionError("should not reach open_connection")

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    cand = _ai_inet("192.0.2.10", 389)
    task = asyncio.create_task(
        TcpStreamEngine._open_socket(
            info_plain,
            cand,
            use_ssl=False,
        )
    )
    await asyncio.sleep(0)

    task.cancel()
    gate.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert fake_sock.closed is True


@pytest.mark.asyncio
async def test_i4_open_socket_socket_create_failure_maps_to_socket_create_failed(
    info_plain, module_under_test, monkeypatch
):
    """Socket creation failure maps to OpenSocketError(SOCKET_CREATE_FAILED)."""

    def fail_socket(*a: Any, **k: Any) -> Any:
        _ = a, k
        raise OSError("no socket")

    monkeypatch.setattr(module_under_test.socket, "socket", fail_socket)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: _FakeLoop())

    cand = _ai_inet("192.0.2.10", 389)
    with pytest.raises(OpenSocketError) as ei:
        await TcpStreamEngine._open_socket(
            info_plain,
            cand,
            use_ssl=False,
        )

    assert ei.value.reason_code == OpenSocketErrorReason.SOCKET_CREATE_FAILED.value


@pytest.mark.asyncio
async def test_i5_open_socket_bind_all_ports_fail_maps_to_bind_failed(
    info_plain, module_under_test, monkeypatch
):
    """Bind failures across all ports map to SOCKET_BIND_FAILED."""
    info = cast(
        RemoteEndpointConnectionInfoProto,
        _Info(source_address="127.0.0.1", source_port_list=[10000, 10001]),
    )
    fake_sock = _FakeSocket()
    fake_sock.set_bind_exc(10000, OSError("bind1"))
    fake_sock.set_bind_exc(10001, OSError("bind2"))
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    async def fake_open_connection(*, sock: Any) -> Any:
        _ = sock
        raise AssertionError("should not reach open_connection")

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    cand = _ai_inet("192.0.2.10", 389)
    with pytest.raises(OpenSocketError) as ei:
        await TcpStreamEngine._open_socket(
            info,
            cand,
            use_ssl=False,
        )

    assert fake_sock.bind_calls == [
        ("127.0.0.1", 10000),
        ("127.0.0.1", 10001),
    ]

    assert ei.value.reason_code == OpenSocketErrorReason.SOCKET_BIND_FAILED.value
    assert fake_sock.closed is True


@pytest.mark.asyncio
async def test_i6_open_socket_connect_timeout_maps_to_connect_timeout(
    info_plain, module_under_test, monkeypatch
):
    """Connect timeout maps to SOCKET_CONNECT_TIMEOUT."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    cand = _ai_inet("192.0.2.10", 389)
    with pytest.raises(OpenSocketError) as ei:
        await TcpStreamEngine._open_socket(
            info_plain,
            cand,
            use_ssl=False,
        )

    assert ei.value.reason_code == OpenSocketErrorReason.SOCKET_CONNECT_TIMEOUT.value
    assert fake_sock.closed is True


@pytest.mark.asyncio
async def test_i7_open_socket_connect_refused_maps_to_connect_refused(
    info_plain, module_under_test, monkeypatch
):
    """ConnectionRefusedError maps to SOCKET_CONNECT_REFUSED."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()
    fake_loop.set_sock_connect_exc(ConnectionRefusedError("refused"))

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    cand = _ai_inet("192.0.2.10", 389)
    with pytest.raises(OpenSocketError) as ei:
        await TcpStreamEngine._open_socket(
            info_plain,
            cand,
            use_ssl=False,
        )

    assert ei.value.reason_code == OpenSocketErrorReason.SOCKET_CONNECT_REFUSED.value
    assert fake_sock.closed is True


@pytest.mark.asyncio
async def test_i8_open_socket_connect_oserror_errno_maps_to_specific_reason(
    info_plain, module_under_test, monkeypatch
):
    """OSError errno maps to specific connect reasons."""
    mapping = [
        (errno.ENETUNREACH, OpenSocketErrorReason.SOCKET_CONNECT_NO_ROUTE_TO_HOST),
        (errno.EHOSTUNREACH, OpenSocketErrorReason.SOCKET_CONNECT_HOST_UNREACHABLE),
        (errno.EHOSTDOWN, OpenSocketErrorReason.SOCKET_CONNECT_HOST_UNREACHABLE),
        (errno.ETIMEDOUT, OpenSocketErrorReason.SOCKET_CONNECT_TIMEOUT),
        (errno.ECONNRESET, OpenSocketErrorReason.SOCKET_CONNECT_FAILED),
    ]

    for err_no, expected in mapping:
        fake_sock = _FakeSocket()
        fake_loop = _FakeLoop()
        fake_loop.set_sock_connect_exc(OSError(err_no, "x"))

        monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
        monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

        cand = _ai_inet("192.0.2.10", 389)
        with pytest.raises(OpenSocketError) as ei:
            await TcpStreamEngine._open_socket(
                info_plain,
                cand,
                use_ssl=False,
            )

        assert ei.value.reason_code == expected.value
        assert fake_sock.closed is True


@pytest.mark.asyncio
async def test_i9_open_socket_wrap_tls_tls_error_maps_to_ssl_wrap_failed(
    info_tls, module_under_test, monkeypatch
):
    """TlsError during wrap maps to SOCKET_SSL_WRAP_FAILED."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    r = _FakeStreamReader()
    w = _FakeStreamWriter()

    async def fake_open_connection(*, sock: Any) -> Any:
        _ = sock
        return r, w

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    async def fail_wrap(info: Any, writer: Any) -> None:
        _ = info, writer
        raise TlsError(
            reason=TlsErrorReason.TLS_HANDSHAKE_FAILED,
            details={"x": 1},
            cause=RuntimeError("tls"),
        )

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fail_wrap)

    cand = _ai_inet("192.0.2.10", 389)
    with pytest.raises(OpenSocketError) as ei:
        await TcpStreamEngine._open_socket(
            info_tls,
            cand,
            use_ssl=True,
        )

    assert ei.value.reason_code == OpenSocketErrorReason.SOCKET_SSL_WRAP_FAILED.value
    assert w.close_calls == 1
    assert w.wait_closed_calls == 1


@pytest.mark.asyncio
async def test_i10_open_socket_unknown_exception_in_connect_maps_to_unknown_reason(
    info_plain, module_under_test, monkeypatch
):
    """Unknown exception during connect maps to OpenSocketError(SOCKET_OPEN_FAILED_UNKNOWN)."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()
    fake_loop.set_sock_connect_exc(ValueError("weird"))

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    cand = _ai_inet("192.0.2.10", 389)
    with pytest.raises(OpenSocketError) as ei:
        await TcpStreamEngine._open_socket(
            info_plain,
            cand,
            use_ssl=False,
        )

    err = ei.value
    assert err.reason_code == OpenSocketErrorReason.SOCKET_OPEN_FAILED_UNKNOWN.value
    assert isinstance(err.__cause__, ValueError)
    assert str(err.__cause__) == "weird"
    assert fake_sock.closed is True


@pytest.mark.asyncio
async def test_i11_open_socket_cleanup_on_exception_closes_writer_and_waits_closed(
    info_plain, module_under_test, monkeypatch
):
    """If writer exists on error, cleanup closes and waits for writer."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    r = _FakeStreamReader()
    w = _FakeStreamWriter()

    async def fake_open_connection(*, sock: Any) -> Any:
        _ = sock
        return r, w

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    async def fail_wrap(info: Any, writer: Any) -> None:
        _ = info, writer
        raise TlsError(
            reason=TlsErrorReason.TLS_HANDSHAKE_FAILED,
            details={"x": 1},
            cause=RuntimeError("tls"),
        )

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fail_wrap)

    cand = _ai_inet("192.0.2.10", 389)

    with pytest.raises(OpenSocketError):
        await TcpStreamEngine._open_socket(
            info_plain,
            cand,
            use_ssl=True,
        )

    assert w.close_calls == 1
    assert w.wait_closed_calls == 1


@pytest.mark.asyncio
async def test_i12_open_socket_cleanup_on_exception_closes_socket_when_writer_not_created(
    info_plain, module_under_test, monkeypatch
):
    """If writer is not created, cleanup closes socket."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()
    fake_loop.set_sock_connect_exc(ConnectionRefusedError("refused"))

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    async def fake_open_connection(*, sock: Any) -> Any:
        _ = sock
        raise AssertionError("should not reach open_connection")

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    cand = _ai_inet("192.0.2.10", 389)

    with pytest.raises(OpenSocketError):
        await TcpStreamEngine._open_socket(
            info_plain,
            cand,
            use_ssl=False,
        )

    assert fake_sock.closed is True


@pytest.mark.asyncio
async def test_i13_open_socket_bind_uses_first_successful_source_port_and_skips_rest(
    module_under_test,
    monkeypatch,
):
    """_open_socket binds source ports until first success and skips remaining ports."""
    info = cast(
        RemoteEndpointConnectionInfoProto,
        _Info(
            source_address="127.0.0.1",
            source_port_list=[10000, 10001, 10002],
        ),
    )

    fake_sock = _FakeSocket()
    fake_sock.set_bind_exc(10000, OSError("bind1"))
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    r = _FakeStreamReader()
    w = _FakeStreamWriter()

    async def fake_open_connection(*, sock: Any) -> Any:
        assert sock is fake_sock
        return r, w

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    cand = _ai_inet("192.0.2.10", 389)

    reader, writer = await TcpStreamEngine._open_socket(
        info,
        cand,
        use_ssl=False,
    )

    assert reader is r
    assert writer is w
    assert fake_sock.bind_calls == [
        ("127.0.0.1", 10000),
        ("127.0.0.1", 10001),
    ]
    assert fake_loop.sock_connect_calls == [(fake_sock, ("192.0.2.10", 389))]
    assert fake_sock.closed is False


@pytest.mark.asyncio
async def test_i14_open_socket_unmapped_wrap_exception_is_reraised_after_writer_cleanup(
    info_tls,
    module_under_test,
    monkeypatch,
):
    """Non-TlsError from TLS wrap is re-raised after writer cleanup."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    r = _FakeStreamReader()
    w = _FakeStreamWriter()

    async def fake_open_connection(*, sock: Any) -> Any:
        assert sock is fake_sock
        return r, w

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    wrap_error = RuntimeError("unexpected-wrap-error")

    async def fail_wrap(info: Any, writer: Any) -> None:
        _ = info, writer
        raise wrap_error

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fail_wrap)

    cand = _ai_inet("192.0.2.10", 389)

    with pytest.raises(RuntimeError) as ei:
        await TcpStreamEngine._open_socket(
            info_tls,
            cand,
            use_ssl=True,
        )

    assert ei.value is wrap_error
    assert w.close_calls == 1
    assert w.wait_closed_calls == 1
    assert fake_sock.closed is False


@pytest.mark.asyncio
async def test_i15_open_socket_cancelled_during_open_connection_closes_socket(
    info_plain,
    module_under_test,
    monkeypatch,
):
    """Cancellation during asyncio.open_connection closes raw socket."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    gate = asyncio.Event()

    async def fake_open_connection(*, sock: Any) -> Any:
        assert sock is fake_sock
        await gate.wait()
        raise AssertionError("open_connection should be cancelled before returning")

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    cand = _ai_inet("192.0.2.10", 389)

    task = asyncio.create_task(
        TcpStreamEngine._open_socket(
            info_plain,
            cand,
            use_ssl=False,
        )
    )
    await asyncio.sleep(0)

    task.cancel()
    gate.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert fake_sock.closed is True
    assert fake_loop.sock_connect_calls == [(fake_sock, ("192.0.2.10", 389))]


# -------------------------
# Group j: Races, stress, no leaks
# -------------------------


@pytest.mark.asyncio
async def test_j1_race_open_and_close_no_deadlock(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """Concurrent open() and close() do not deadlock."""
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])
    eng = _make_engine(remote_endpoint)

    gate = asyncio.Event()

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        await gate.wait()
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    t_open = asyncio.create_task(eng.open())
    await asyncio.sleep(0)
    t_close = asyncio.create_task(eng.close())

    gate.set()
    open_result, close_result = await asyncio.gather(t_open, t_close)

    assert open_result is TcpStreamOpenOutcome.OPENED
    assert close_result is TcpStreamCloseOutcome.CLOSED
    assert eng.state is EngineState.CLOSED


@pytest.mark.asyncio
async def test_j2_concurrent_open_calls_single_socket_attempt_and_all_return(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """Concurrent open() calls share one actual open attempt."""
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])
    eng = _make_engine(remote_endpoint)

    calls = 0
    gate = asyncio.Event()

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        nonlocal calls
        calls += 1
        await gate.wait()
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    t1 = asyncio.create_task(eng.open())
    t2 = asyncio.create_task(eng.open())
    t3 = asyncio.create_task(eng.open())

    await asyncio.sleep(0)
    gate.set()
    r1, r2, r3 = await asyncio.gather(t1, t2, t3)

    assert calls == 1
    assert [r1, r2, r3].count(TcpStreamOpenOutcome.OPENED) == 1
    assert [r1, r2, r3].count(TcpStreamOpenOutcome.ALREADY_OPENED) == 2
    assert eng.state is EngineState.OPENED


@pytest.mark.asyncio
async def test_j3_read_while_closing_waits_then_raises_not_open(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """read() during CLOSING waits then raises NotOpen."""
    eng = _make_engine(remote_endpoint)

    async with eng._cond:
        eng._state = EngineState.CLOSING
        eng._cond.notify_all()

    done = asyncio.Event()

    async def do_read() -> None:
        with pytest.raises(TcpStreamEngineNotOpenError):
            await eng.read(1)
        done.set()

    t = asyncio.create_task(do_read())
    await asyncio.sleep(0)

    async with eng._cond:
        eng._state = EngineState.CLOSED
        eng._cond.notify_all()

    await asyncio.wait_for(done.wait(), timeout=1.0)
    await t


@pytest.mark.asyncio
async def test_j4_close_notifies_waiters_when_leaving_opened(remote_endpoint: _FakeRemoteEndpoint):
    """close() notifies waiters on condition."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    gate = asyncio.Event()
    writer.set_wait_closed_gate(gate)
    eng._writer = cast(Any, writer)

    notified = asyncio.Event()

    async def waiter() -> None:
        async with eng._cond:
            while eng._state is EngineState.OPENED:
                await eng._cond.wait()
        notified.set()

    w = asyncio.create_task(waiter())
    c = asyncio.create_task(eng.close())

    await asyncio.sleep(0)
    gate.set()

    await asyncio.gather(c, w)
    assert notified.is_set() is True


@pytest.mark.asyncio
async def test_j5_no_resource_leak_after_failed_open_all_candidates(
    remote_endpoint: _FakeRemoteEndpoint, module_under_test, monkeypatch
):
    """After failed open(), engine holds no streams."""
    c1 = _ai_inet("192.0.2.10", 389)
    c2 = _ai_inet("192.0.2.11", 389)
    remote_endpoint.set_candidates([c1, c2])
    eng = _make_engine(remote_endpoint)

    seen: list[AddrInfo] = []

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        seen.append(cand)
        raise OpenSocketError(
            reason=OpenSocketErrorReason.SOCKET_CONNECT_FAILED,
            candidate=str(cand),
            details={},
            cause=OSError("fail"),
        )

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    with pytest.raises(OpenConnectionError):
        await eng.open()

    assert eng._reader is None
    assert eng._writer is None
    assert eng.state is EngineState.ERROR
    assert seen == [c1, c2]


@pytest.mark.asyncio
async def test_j6_no_resource_leak_after_cancelled_open_during_tls_wrap(
    info_tls, module_under_test, monkeypatch
):
    """Cancellation during TLS wrap closes writer without waiting for wait_closed()."""
    fake_sock = _FakeSocket()
    fake_loop = _FakeLoop()

    monkeypatch.setattr(module_under_test.socket, "socket", lambda *a, **k: fake_sock)
    monkeypatch.setattr(module_under_test.asyncio, "get_running_loop", lambda: fake_loop)

    r = _FakeStreamReader()
    w = _FakeStreamWriter()

    async def fake_open_connection(*, sock: Any) -> Any:
        _ = sock
        return r, w

    monkeypatch.setattr(module_under_test.asyncio, "open_connection", fake_open_connection)

    gate = asyncio.Event()

    async def block_wrap(info: Any, writer: Any) -> None:
        _ = info, writer
        await gate.wait()

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", block_wrap)

    cand = _ai_inet("192.0.2.10", 389)
    task = asyncio.create_task(
        TcpStreamEngine._open_socket(
            info_tls,
            cand,
            use_ssl=True,
        )
    )
    await asyncio.sleep(0)

    task.cancel()
    gate.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert w.close_calls == 1
    assert w.wait_closed_calls == 0
    assert fake_sock.closed is False


# -------------------------
# Group k: crypto codec attach/detach and read/write routing
# -------------------------


@pytest.mark.asyncio
async def test_k1_attach_crypto_codec_success_on_opened_engine(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """attach_crypto_codec() attaches codec on OPENED engine."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    codec = _FakeCryptoCodec()

    result = await eng.attach_crypto_codec(cast(Any, codec))

    assert result is TcpStreamReconfigOutcome.DONE
    assert eng._security_mode is TcpStreamSecurityMode.CODEC

    assert eng._crypto_codec is codec


@pytest.mark.asyncio
async def test_k2_attach_crypto_codec_refused_when_engine_not_opened(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """attach_crypto_codec() is refused when engine is not OPENED."""
    eng = _make_engine(remote_endpoint)
    codec = _FakeCryptoCodec()

    result = await eng.attach_crypto_codec(cast(Any, codec))

    assert result is TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED
    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE
    assert eng._crypto_codec is None


@pytest.mark.asyncio
async def test_k3_attach_crypto_codec_refused_when_already_attached(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """attach_crypto_codec() refuses second attach."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    codec1 = _FakeCryptoCodec()
    codec2 = _FakeCryptoCodec()

    result1 = await eng.attach_crypto_codec(cast(Any, codec1))
    result2 = await eng.attach_crypto_codec(cast(Any, codec2))

    assert result1 is TcpStreamReconfigOutcome.DONE
    assert result2 is TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_ATTACHED
    assert eng._crypto_codec is codec1
    assert eng._security_mode is TcpStreamSecurityMode.CODEC
    assert eng._crypto_codec is codec1


@pytest.mark.asyncio
async def test_k4_detach_crypto_codec_success_on_opened_engine(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """detach_crypto_codec() detaches attached codec on OPENED engine."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    codec = _FakeCryptoCodec()
    eng._crypto_codec = cast(Any, codec)
    eng._security_mode = TcpStreamSecurityMode.CODEC

    result = await eng.detach_crypto_codec()

    assert result is TcpStreamReconfigOutcome.DONE
    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.PLAIN
    assert eng._crypto_codec is None


@pytest.mark.asyncio
async def test_k5_detach_crypto_codec_refused_when_engine_not_opened(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """detach_crypto_codec() is refused when engine is not OPENED."""
    eng = _make_engine(remote_endpoint)
    eng._crypto_codec = cast(Any, _FakeCryptoCodec())

    result = await eng.detach_crypto_codec()

    assert result is TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED
    assert eng._crypto_codec is not None


@pytest.mark.asyncio
async def test_k6_detach_crypto_codec_refused_when_not_attached(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """detach_crypto_codec() is refused when no codec is attached."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    result = await eng.detach_crypto_codec()

    assert result is TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_NOT_ATTACHED
    assert eng._crypto_codec is None


@pytest.mark.asyncio
async def test_k7_read_without_codec_uses_raw_read(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """read() without codec delegates directly to raw stream."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._security_mode = TcpStreamSecurityMode.CODEC

    reader = _FakeStreamReader()
    reader.set_next(b"plain")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    # noinspection PyArgumentEqualDefault
    result = await eng.read(5, mode=SocketTimeoutMode.UNLIMITED)

    assert result == b"plain"
    assert reader.read_calls == [5]


@pytest.mark.asyncio
async def test_k8_read_with_codec_routes_through_codec_and_returns_codec_result(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """read() with attached codec routes through codec."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._security_mode = TcpStreamSecurityMode.CODEC

    reader = _FakeStreamReader()
    reader.set_next(b"raw-from-stream")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    codec = _FakeCryptoCodec()
    codec.read_payload = b"decoded-by-codec"
    eng._crypto_codec = cast(Any, codec)

    # noinspection PyArgumentEqualDefault
    result = await eng.read(123, mode=SocketTimeoutMode.UNLIMITED)

    assert result == b"decoded-by-codec"
    assert codec.read_calls == 1
    assert codec.read_raw_results == [b"raw-from-stream"]
    assert reader.read_calls == [123]


def test_k9_write_without_codec_uses_raw_write(remote_endpoint: _FakeRemoteEndpoint):
    """write() without codec delegates directly to raw writer."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._security_mode = TcpStreamSecurityMode.CODEC
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    eng.write(b"plain")

    assert writer.write_calls == [b"plain"]


def test_k10_write_with_codec_routes_through_codec_and_emits_transformed_raw_chunks(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """write() with attached codec routes through codec."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._security_mode = TcpStreamSecurityMode.CODEC
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    codec = _FakeCryptoCodec()
    eng._crypto_codec = cast(Any, codec)

    eng.write(b"plain")

    assert codec.write_calls == [b"plain"]
    assert writer.write_calls == [b"crypto:plain"]


@pytest.mark.asyncio
async def test_k11_close_detaches_attached_crypto_codec(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """close() clears attached codec reference."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._crypto_codec = cast(Any, _FakeCryptoCodec())

    await eng.close()

    assert eng.state is EngineState.CLOSED
    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE


@pytest.mark.asyncio
async def test_k12_read_codec_exception_propagates_and_does_not_force_close_engine(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Exception from codec.read() propagates from read()."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED

    reader = _FakeStreamReader()
    reader.set_next(b"raw")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    codec = _FakeCryptoCodec()
    codec.read_exc = RuntimeError("codec-read-failed")
    eng._crypto_codec = cast(Any, codec)

    with pytest.raises(TcpStreamEngineUnexpectedError) as ei:
        await eng.read(10)

    assert isinstance(ei.value.__cause__, RuntimeError)
    assert str(ei.value.__cause__) == "codec-read-failed"
    assert eng.state is EngineState.OPENED


def test_k13_write_codec_exception_propagates_from_write(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Exception from codec.write() propagates from write()."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    codec = _FakeCryptoCodec()
    codec.write_exc = RuntimeError("codec-write-failed")
    eng._crypto_codec = cast(Any, codec)

    with pytest.raises(TcpStreamEngineUnexpectedError) as ei:
        eng.write(b"x")

    assert isinstance(ei.value.__cause__, RuntimeError)
    assert str(ei.value.__cause__) == "codec-write-failed"
    assert eng.state is EngineState.OPENED


@pytest.mark.asyncio
async def test_k14_attach_crypto_codec_waits_reconfiguring_then_attaches(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """attach_crypto_codec() waits while engine is RECONFIGURING."""
    eng = _make_engine(remote_endpoint)
    codec = _FakeCryptoCodec()

    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING
        eng._security_mode = TcpStreamSecurityMode.PLAIN

    completed = asyncio.Event()

    async def do_attach() -> TcpStreamReconfigOutcome:
        _result = await eng.attach_crypto_codec(cast(Any, codec))
        completed.set()
        return _result

    task = asyncio.create_task(do_attach())
    await asyncio.sleep(0)

    assert completed.is_set() is False

    async with eng._cond:
        eng._state = EngineState.OPENED
        eng._security_mode = TcpStreamSecurityMode.PLAIN
        eng._cond.notify_all()

    result = await asyncio.wait_for(task, timeout=1.0)

    assert result is TcpStreamReconfigOutcome.DONE
    assert completed.is_set() is True
    assert eng._crypto_codec is codec
    assert eng._security_mode is TcpStreamSecurityMode.CODEC


@pytest.mark.asyncio
async def test_k15_attach_crypto_codec_refused_when_under_ssl(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """attach_crypto_codec() is refused when stream is already under SSL."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.SSL

    codec = _FakeCryptoCodec()

    result = await eng.attach_crypto_codec(cast(Any, codec))

    assert result is TcpStreamReconfigOutcome.REFUSED_CONNECTION_ALREADY_UNDER_SSL
    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.SSL


@pytest.mark.asyncio
async def test_k16_attach_crypto_codec_refused_when_start_tls_active(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """attach_crypto_codec() is refused when START_TLS is active."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.START_TLS

    codec = _FakeCryptoCodec()

    result = await eng.attach_crypto_codec(cast(Any, codec))

    assert result is TcpStreamReconfigOutcome.REFUSED_START_TLS_ALREADY_ACTIVE
    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS


@pytest.mark.asyncio
async def test_k17_detach_crypto_codec_waits_reconfiguring_then_detaches(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """detach_crypto_codec() waits while engine is RECONFIGURING."""
    eng = _make_engine(remote_endpoint)
    codec = _FakeCryptoCodec()

    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._crypto_codec = cast(Any, codec)

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING
        eng._security_mode = TcpStreamSecurityMode.CODEC

    completed = asyncio.Event()

    async def do_detach() -> TcpStreamReconfigOutcome:
        _result = await eng.detach_crypto_codec()
        completed.set()
        return _result

    task = asyncio.create_task(do_detach())
    await asyncio.sleep(0)

    assert completed.is_set() is False

    async with eng._cond:
        eng._state = EngineState.OPENED
        eng._security_mode = TcpStreamSecurityMode.CODEC
        eng._cond.notify_all()

    result = await asyncio.wait_for(task, timeout=1.0)

    assert result is TcpStreamReconfigOutcome.DONE
    assert completed.is_set() is True
    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.PLAIN


@pytest.mark.asyncio
async def test_k18_attach_waits_for_start_tls_then_refused_start_tls_active(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """attach_crypto_codec() waits for START_TLS reconfiguration then refuses."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    tls_started = asyncio.Event()
    allow_tls_finish = asyncio.Event()

    async def fake_wrap_stream_tls(
        info: Any,
        stream_writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        _ = info, stream_writer, handshake_timeout_s
        tls_started.set()
        await allow_tls_finish.wait()

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        _ = policy
        result = await factory()
        return False, result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    start_task = asyncio.create_task(eng.start_tls())
    await asyncio.wait_for(tls_started.wait(), timeout=1.0)

    codec = _FakeCryptoCodec()
    attach_task = asyncio.create_task(eng.attach_crypto_codec(cast(Any, codec)))
    await asyncio.sleep(0)

    assert attach_task.done() is False
    assert eng.state is EngineState.RECONFIGURING

    allow_tls_finish.set()

    start_result, attach_result = await asyncio.gather(start_task, attach_task)

    assert start_result is TcpStreamReconfigOutcome.DONE
    assert attach_result is TcpStreamReconfigOutcome.REFUSED_START_TLS_ALREADY_ACTIVE
    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS
    assert eng._crypto_codec is None
    assert writer.close_calls == 0
    assert writer.wait_closed_calls == 0


@pytest.mark.asyncio
async def test_k19_attach_crypto_codec_rejects_none(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """attach_crypto_codec() rejects None codec."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(ValueError, match="codec"):
        await eng.attach_crypto_codec(cast(Any, None))

    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_codec",
    [
        object(),
        "codec",
        b"codec",
    ],
)
async def test_k20_attach_crypto_codec_rejects_non_crypto_codec(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_codec: object,
):
    """attach_crypto_codec() rejects non-CryptoCodec codec."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="codec"):
        await eng.attach_crypto_codec(cast(Any, bad_codec))

    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE


# -------------------------
# Group l: helper mappings
# -------------------------


def test_l1_map_io_exception_connection_reset_error() -> None:
    reason = _map_io_exception_to_reason(ConnectionResetError("reset"))

    assert reason is TcpStreamIoErrorReason.TCP_STREAM_CONNECTION_RESET


def test_l2_map_io_exception_broken_pipe_error() -> None:
    reason = _map_io_exception_to_reason(BrokenPipeError("pipe"))

    assert reason is TcpStreamIoErrorReason.TCP_STREAM_BROKEN_PIPE


def test_l3_map_io_exception_oserror_econnreset() -> None:
    reason = _map_io_exception_to_reason(OSError(errno.ECONNRESET, "reset"))

    assert reason is TcpStreamIoErrorReason.TCP_STREAM_CONNECTION_RESET


def test_l4_map_io_exception_oserror_epipe() -> None:
    reason = _map_io_exception_to_reason(OSError(errno.EPIPE, "pipe"))

    assert reason is TcpStreamIoErrorReason.TCP_STREAM_BROKEN_PIPE


def test_l5_map_io_exception_generic_oserror() -> None:
    reason = _map_io_exception_to_reason(OSError(errno.EINVAL, "generic"))

    assert reason is TcpStreamIoErrorReason.TCP_STREAM_IO_ERROR


def test_l6_map_io_exception_unknown_non_oserror() -> None:
    reason = _map_io_exception_to_reason(RuntimeError("unknown"))

    assert reason is TcpStreamIoErrorReason.TCP_STREAM_IO_ERROR_UNKNOWN


def test_l7_map_ssl_exception_zero_return() -> None:
    reason = _map_ssl_exception_to_reason(ssl.SSLZeroReturnError("clean close"))

    assert reason is TlsErrorReason.TLS_SESSION_CLOSED_CLEANLY_BY_PEER


def test_l8_map_ssl_exception_eof() -> None:
    reason = _map_ssl_exception_to_reason(ssl.SSLEOFError("abrupt eof"))

    assert reason is TlsErrorReason.TLS_SESSION_TERMINATED_ABRUPTLY


def test_l9_map_ssl_exception_generic_ssl_error() -> None:
    reason = _map_ssl_exception_to_reason(ssl.SSLError("ssl error"))

    assert reason is TlsErrorReason.TLS_UNEXPECTED_ERROR


def test_l10_candidate_to_str_for_inet_sockaddr() -> None:
    candidate = _ai_inet("192.0.2.10", 389)

    result = _candidate_to_str(candidate)

    assert result == "192.0.2.10:389"


def test_l11_candidate_to_str_for_inet6_sockaddr() -> None:
    candidate = _ai_inet6("2001:db8::1", 389)

    result = _candidate_to_str(candidate)

    assert result == "2001:db8::1:389"


def _as_addr_info(value: object) -> AddrInfo:
    return cast(AddrInfo, value)


def test_l12_candidate_to_str_falls_back_to_repr_for_non_inet_sockaddr() -> None:
    sockaddr = ("not-ip", "not-port")
    candidate = (
        socket.AF_INET,
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP,
        "",
        sockaddr,
    )

    result = _candidate_to_str(_as_addr_info(candidate))

    assert result == repr(sockaddr)


# -------------------------
# Group m: start_tls()
# -------------------------


@pytest.mark.asyncio
async def test_m1_start_tls_refused_when_connection_not_opened(
    remote_endpoint: _FakeRemoteEndpoint,
):
    eng = _make_engine(remote_endpoint)

    result = await eng.start_tls()

    assert result is TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED
    assert eng.state is EngineState.VIRGIN
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE


@pytest.mark.asyncio
async def test_m2_start_tls_refused_when_already_under_ssl(
    remote_endpoint: _FakeRemoteEndpoint,
):
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.SSL

    result = await eng.start_tls()

    assert result is TcpStreamReconfigOutcome.REFUSED_CONNECTION_ALREADY_UNDER_SSL
    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.SSL


@pytest.mark.asyncio
async def test_m3_start_tls_refused_when_start_tls_already_active(
    remote_endpoint: _FakeRemoteEndpoint,
):
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.START_TLS

    result = await eng.start_tls()

    assert result is TcpStreamReconfigOutcome.REFUSED_START_TLS_ALREADY_ACTIVE
    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS


@pytest.mark.asyncio
async def test_m4_start_tls_refused_when_crypto_codec_attached(
    remote_endpoint: _FakeRemoteEndpoint,
):
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.CODEC
    eng._crypto_codec = cast(Any, _FakeCryptoCodec())

    result = await eng.start_tls()

    assert result is TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_ATTACHED
    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.CODEC
    assert eng._crypto_codec is not None


@pytest.mark.asyncio
async def test_m5_start_tls_success_sets_start_tls_mode_and_returns_done(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    wrap_calls: list[Any] = []
    run_calls: list[Any] = []

    async def fake_wrap_stream_tls(
        info: Any,
        stream_writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        wrap_calls.append((info, stream_writer, handshake_timeout_s))

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        run_calls.append(policy)
        _result = await factory()
        return False, _result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    result = await eng.start_tls()

    assert result is TcpStreamReconfigOutcome.DONE
    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS
    assert wrap_calls == [(remote_endpoint.info, writer, None)]
    assert run_calls == [CancellationPolicy.DEFER_FLAG]


@pytest.mark.asyncio
async def test_m6_start_tls_passes_handshake_timeout_to_wrap_stream_tls(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    observed: dict[str, Any] = {}

    async def fake_wrap_stream_tls(
        info: Any,
        stream_writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        observed["info"] = info
        observed["writer"] = stream_writer
        observed["handshake_timeout_s"] = handshake_timeout_s

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        observed["policy"] = policy
        _result = await factory()
        return False, _result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    result = await eng.start_tls(handshake_timeout_s=12.5)

    assert result is TcpStreamReconfigOutcome.DONE
    assert observed["info"] is remote_endpoint.info
    assert observed["writer"] is writer
    assert observed["handshake_timeout_s"] == 12.5
    assert observed["policy"] is CancellationPolicy.DEFER_FLAG
    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS


@pytest.mark.asyncio
async def test_m7_start_tls_net_error_closes_engine_and_reraises(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    tls_error = TlsError(
        reason=TlsErrorReason.TLS_HANDSHAKE_FAILED,
        details={"x": 1},
        cause=RuntimeError("tls"),
    )

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        _ = factory, policy
        raise tls_error

    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    with pytest.raises(TlsError) as ei:
        await eng.start_tls()

    assert ei.value is tls_error
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1
    assert eng.state is EngineState.CLOSED
    assert eng._reader is None
    assert eng._writer is None
    assert eng._crypto_codec is None
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE


@pytest.mark.asyncio
async def test_m8_start_tls_deferred_cancellation_raises_cancelled_error_after_tls_started(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    async def fake_wrap_stream_tls(
        info: Any,
        stream_writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        _ = info, stream_writer, handshake_timeout_s

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        _ = policy
        result = await factory()
        return True, result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    with pytest.raises(asyncio.CancelledError):
        await eng.start_tls()

    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS
    assert eng._writer is writer
    assert writer.close_calls == 0
    assert writer.wait_closed_calls == 0


@pytest.mark.asyncio
async def test_m9_start_tls_waits_while_reconfiguring(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    eng = _make_engine(remote_endpoint)
    eng._reader = cast(Any, _FakeStreamReader())
    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    async with eng._cond:
        eng._state = EngineState.RECONFIGURING
        eng._security_mode = TcpStreamSecurityMode.PLAIN

    started = asyncio.Event()
    completed = asyncio.Event()

    async def fake_wrap_stream_tls(
        info: Any,
        stream_writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        _ = info, stream_writer, handshake_timeout_s

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        _ = policy
        _result = await factory()
        return False, _result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    async def do_start_tls() -> TcpStreamReconfigOutcome:
        started.set()
        _result = await eng.start_tls()
        completed.set()
        return _result

    task = asyncio.create_task(do_start_tls())

    await started.wait()
    await asyncio.sleep(0)

    assert completed.is_set() is False

    async with eng._cond:
        eng._state = EngineState.OPENED
        eng._security_mode = TcpStreamSecurityMode.PLAIN
        eng._cond.notify_all()

    result = await asyncio.wait_for(task, timeout=1.0)

    assert result is TcpStreamReconfigOutcome.DONE
    assert completed.is_set() is True
    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS


@pytest.mark.asyncio
async def test_m10_race_start_tls_and_close_success_no_deadlock_finally_closed(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """Concurrent start_tls() and close() do not deadlock and finally close."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    tls_started = asyncio.Event()
    allow_tls_finish = asyncio.Event()

    async def fake_wrap_stream_tls(
        info: Any,
        stream_writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        _ = info, stream_writer, handshake_timeout_s
        tls_started.set()
        await allow_tls_finish.wait()

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        _ = policy
        result = await factory()
        return False, result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    start_task = asyncio.create_task(eng.start_tls())
    await asyncio.wait_for(tls_started.wait(), timeout=1.0)

    close_task = asyncio.create_task(eng.close())
    await asyncio.sleep(0)

    assert eng.state is EngineState.RECONFIGURING
    assert close_task.done() is False

    allow_tls_finish.set()

    start_result, close_result = await asyncio.gather(start_task, close_task)

    assert start_result is TcpStreamReconfigOutcome.DONE
    assert close_result is TcpStreamCloseOutcome.CLOSED
    assert eng.state is EngineState.CLOSED
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE
    assert eng._reader is None
    assert eng._writer is None
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1


@pytest.mark.asyncio
async def test_m11_concurrent_start_tls_calls_single_reconfiguration_second_refused(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """Concurrent start_tls() calls serialize; second call is refused after first succeeds."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    calls = 0
    tls_started = asyncio.Event()
    allow_tls_finish = asyncio.Event()

    async def fake_wrap_stream_tls(
        info: Any,
        stream_writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        _ = info, stream_writer, handshake_timeout_s
        nonlocal calls
        calls += 1
        tls_started.set()
        await allow_tls_finish.wait()

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        _ = policy
        result = await factory()
        return False, result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    first_task = asyncio.create_task(eng.start_tls())
    await asyncio.wait_for(tls_started.wait(), timeout=1.0)

    second_task = asyncio.create_task(eng.start_tls())
    await asyncio.sleep(0)

    assert second_task.done() is False
    assert eng.state is EngineState.RECONFIGURING

    allow_tls_finish.set()

    first_result, second_result = await asyncio.gather(first_task, second_task)

    assert calls == 1
    assert first_result is TcpStreamReconfigOutcome.DONE
    assert second_result is TcpStreamReconfigOutcome.REFUSED_START_TLS_ALREADY_ACTIVE
    assert eng.state is EngineState.OPENED
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS
    assert writer.close_calls == 0
    assert writer.wait_closed_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_timeout",
    [
        True,
        False,
        "1.0",
        object(),
    ],
)
async def test_m12_start_tls_rejects_invalid_handshake_timeout_type(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_timeout: object,
):
    """start_tls() rejects non-numeric handshake_timeout_s and bool handshake_timeout_s."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(TypeError, match="handshake_timeout_s"):
        await eng.start_tls(handshake_timeout_s=cast(Any, bad_timeout))

    assert eng.state is EngineState.VIRGIN
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_timeout",
    [
        0,
        -1,
        -0.1,
    ],
)
async def test_m13_start_tls_rejects_non_positive_handshake_timeout(
    remote_endpoint: _FakeRemoteEndpoint,
    bad_timeout: float,
):
    """start_tls() rejects non-positive handshake_timeout_s."""
    eng = _make_engine(remote_endpoint)

    with pytest.raises(ValueError, match="handshake_timeout_s"):
        await eng.start_tls(handshake_timeout_s=bad_timeout)

    assert eng.state is EngineState.VIRGIN
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE


# -------------------------
# Group n: SSL/TLS I/O error handling
# -------------------------


@pytest.mark.asyncio
async def test_n1_read_ssl_error_maps_to_tls_error_and_closes_engine(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """ssl.SSLError during read maps to TlsError and closes the engine."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._security_mode = TcpStreamSecurityMode.START_TLS

    reader = _FakeStreamReader()
    reader.set_exc(ssl.SSLError("ssl-read-failed"))
    eng._reader = cast(Any, reader)

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    with pytest.raises(TlsError) as ei:
        # noinspection PyArgumentEqualDefault
        await eng.read(1, mode=SocketTimeoutMode.UNLIMITED)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_UNEXPECTED_ERROR.value
    assert err.details["io_operation_type"] == TCP_READ
    assert err.details["engine_state_at_error"] == EngineState.OPENED.value

    assert eng.state is EngineState.CLOSED
    assert eng._reader is None
    assert eng._writer is None
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1


@pytest.mark.asyncio
async def test_n2_drain_ssl_error_maps_to_tls_error_and_closes_engine(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """ssl.SSLError during drain maps to TlsError and closes the engine."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._security_mode = TcpStreamSecurityMode.START_TLS
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    writer.set_drain_exc(ssl.SSLError("ssl-drain-failed"))
    eng._writer = cast(Any, writer)

    with pytest.raises(TlsError) as ei:
        # noinspection PyArgumentEqualDefault
        await eng.drain(mode=SocketTimeoutMode.UNLIMITED)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_UNEXPECTED_ERROR.value
    assert err.details["io_operation_type"] == TCP_DRAIN
    assert err.details["engine_state_at_error"] == EngineState.OPENED.value

    assert eng.state is EngineState.CLOSED
    assert eng._reader is None
    assert eng._writer is None
    assert eng._security_mode is TcpStreamSecurityMode.NOT_AVAILABLE
    assert writer.close_calls == 1
    assert writer.wait_closed_calls == 1


def test_n3_write_ssl_error_maps_to_tls_error_and_keeps_engine_open(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """ssl.SSLError during write maps to TlsError and does not close the engine."""
    eng = _make_engine(remote_endpoint)
    eng._state = EngineState.OPENED
    eng._security_mode = TcpStreamSecurityMode.START_TLS
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    writer.set_write_exc(ssl.SSLError("ssl-write-failed"))
    eng._writer = cast(Any, writer)

    with pytest.raises(TlsError) as ei:
        eng.write(b"x")

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_UNEXPECTED_ERROR.value
    assert err.details["io_operation_type"] == TCP_WRITE
    assert err.details["engine_state_at_error"] == EngineState.OPENED.value

    assert eng.state is EngineState.OPENED
    assert eng._reader is not None
    assert eng._writer is writer
    assert eng._security_mode is TcpStreamSecurityMode.START_TLS
    assert writer.close_calls == 0
    assert writer.wait_closed_calls == 0


# -------------------------
# Group o: logging integration
# -------------------------


class _MemoryLogSink:
    def __init__(self) -> None:
        self._lock = RLock()
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        with self._lock:
            self.events.append(event)


@pytest.fixture()
def memory_log_sink() -> _MemoryLogSink:
    return _MemoryLogSink()


@pytest.fixture()
def memory_log_context(memory_log_sink: _MemoryLogSink) -> LogContext:
    return LogContext(
        namespace="tcp-stream-engine-tests",
        log_sink=memory_log_sink,
        payload_processor=LogPayloadProcessor(),
    )


def _log_pairs(sink: _MemoryLogSink) -> list[tuple[str, str | None]]:
    return [(event.meta.event_name, event.event_outcome) for event in sink.events]


def _single_log_event(
    sink: _MemoryLogSink,
    *,
    event_name: str,
    outcome: str | None,
) -> LogEvent:
    matches = [
        event
        for event in sink.events
        if event.meta.event_name == event_name and event.event_outcome == outcome
    ]

    assert len(matches) == 1
    return matches[0]


@pytest.mark.asyncio
async def test_o1_open_success_logs_invoke_and_success_with_formatter_payload(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """open() emits invoke/success events with formatter payload."""
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-1",
    )

    reader = _FakeStreamReader()
    writer = _FakeStreamWriter()

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        assert info is remote_endpoint.info
        assert cand == _ai_inet("192.0.2.10", 389)
        assert use_ssl is True
        return reader, writer

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    result = await eng.open(use_ssl=True)

    assert result is TcpStreamOpenOutcome.OPENED

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.open", "invoke"),
        ("tcp_stream_engine.open", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.open",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.open",
        outcome="success",
    )

    assert invoke.meta.entity_id == "engine-log-1"
    assert success.meta.entity_id == "engine-log-1"

    assert "engine_id" not in invoke.payload
    assert "engine_id" not in success.payload

    assert invoke.payload["engine_state"] == EngineState.VIRGIN.value
    assert invoke.payload["connection_info"]["host"] == remote_endpoint.info.host
    assert invoke.payload["connection_info"]["port"] == remote_endpoint.info.port

    assert success.payload["engine_state"] == EngineState.OPENED.value
    assert "connection_info" not in success.payload
    assert success.payload["result"] == TcpStreamOpenOutcome.OPENED.value


@pytest.mark.asyncio
async def test_o2_open_failed_logs_failed_event_with_public_error_boundary(
    remote_endpoint: _FakeRemoteEndpoint,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """open() failed path emits failed event after public error normalization."""
    remote_endpoint.set_exception(RuntimeError("boom"))

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-2",
    )

    with pytest.raises(TcpStreamEngineUnexpectedError) as ei:
        await eng.open()

    assert isinstance(ei.value.__cause__, RuntimeError)

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.open", "invoke"),
        ("tcp_stream_engine.open", "failed"),
    ]

    failed = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.open",
        outcome="failed",
    )

    assert failed.meta.entity_id == "engine-log-2"
    assert failed.payload["engine_state"] == EngineState.ERROR.value
    assert "error" in failed.payload


@pytest.mark.asyncio
async def test_o3_open_cancelled_logs_cancelled_event(
    remote_endpoint: _FakeRemoteEndpoint,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """open() cancellation emits cancelled event and re-raises CancelledError."""
    gate = asyncio.Event()
    remote_endpoint.set_wait_gate(gate)
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-3",
    )

    task = asyncio.create_task(eng.open())
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    gate.set()

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.open", "invoke"),
        ("tcp_stream_engine.open", "cancelled"),
    ]

    cancelled = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.open",
        outcome="cancelled",
    )

    assert cancelled.meta.entity_id == "engine-log-3"
    assert cancelled.payload["engine_state"] == EngineState.ERROR.value
    assert cancelled.payload["cancelled"] is True


@pytest.mark.asyncio
async def test_o4_close_success_logs_result_value(
    remote_endpoint: _FakeRemoteEndpoint,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """close() emits invoke/success events with result value."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-4",
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    result = await eng.close()

    assert result is TcpStreamCloseOutcome.CLOSED

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.close", "invoke"),
        ("tcp_stream_engine.close", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.close",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.close",
        outcome="success",
    )

    assert invoke.meta.entity_id == "engine-log-4"
    assert success.meta.entity_id == "engine-log-4"

    assert invoke.payload["engine_state"] == EngineState.OPENED.value
    assert success.payload["engine_state"] == EngineState.CLOSED.value
    assert success.payload["result"] == TcpStreamCloseOutcome.CLOSED.value


@pytest.mark.asyncio
async def test_o5_start_tls_success_logs_security_mode_and_result(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """start_tls() logs security_mode context and result value."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-5",
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    async def fake_wrap_stream_tls(
        info: Any,
        writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        _ = info, writer, handshake_timeout_s

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        _ = policy
        _result = await factory()
        return False, _result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    result = await eng.start_tls()

    assert result is TcpStreamReconfigOutcome.DONE

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.start_tls", "invoke"),
        ("tcp_stream_engine.start_tls", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.start_tls",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.start_tls",
        outcome="success",
    )

    assert invoke.meta.entity_id == "engine-log-5"
    assert success.meta.entity_id == "engine-log-5"

    assert invoke.payload["engine_state"] == EngineState.OPENED.value
    assert invoke.payload["security_mode"] == TcpStreamSecurityMode.PLAIN.value

    assert success.payload["engine_state"] == EngineState.OPENED.value
    assert success.payload["security_mode"] == TcpStreamSecurityMode.START_TLS.value
    assert success.payload["result"] == TcpStreamReconfigOutcome.DONE.value


@pytest.mark.asyncio
async def test_o6_read_success_logs_kwargs_and_result(
    remote_endpoint: _FakeRemoteEndpoint,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """read() emits kwargs on invoke and read_bytes on success."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-6",
    )
    eng._state = EngineState.OPENED

    reader = _FakeStreamReader()
    reader.set_next(b"hello")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    # noinspection PyArgumentEqualDefault
    result = await eng.read(
        5,
        mode=SocketTimeoutMode.UNLIMITED,
        socket_timeout_s=12.5,
    )

    assert result == b"hello"

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.read", "invoke"),
        ("tcp_stream_engine.read", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.read",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.read",
        outcome="success",
    )

    assert invoke.meta.entity_id == "engine-log-6"
    assert success.meta.entity_id == "engine-log-6"

    assert invoke.payload["engine_state"] == EngineState.OPENED.value
    assert invoke.payload["kwargs"]["read_max_bytes"] == 5
    assert invoke.payload["kwargs"]["timeout_mode"] == SocketTimeoutMode.UNLIMITED.value
    assert invoke.payload["kwargs"]["timeout_override_s"] == 12.5

    assert success.payload["engine_state"] == EngineState.OPENED.value
    assert success.payload["result"]["read_bytes"] == 5


def test_o7_write_success_logs_write_bytes(
    remote_endpoint: _FakeRemoteEndpoint,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """write() emits write_bytes on invoke and success event."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-7",
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    eng._writer = cast(Any, writer)

    eng.write(b"abc")

    assert writer.write_calls == [b"abc"]

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.write", "invoke"),
        ("tcp_stream_engine.write", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.write",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.write",
        outcome="success",
    )

    assert invoke.meta.entity_id == "engine-log-7"
    assert success.meta.entity_id == "engine-log-7"

    assert invoke.payload["engine_state"] == EngineState.OPENED.value
    assert invoke.payload["kwargs"]["write_bytes"] == 3


@pytest.mark.asyncio
async def test_o8_drain_failed_logs_failed_event(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """drain() emits failed event when public API raises SocketTimeoutError."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-8",
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    with pytest.raises(SocketTimeoutError):
        await eng.drain(
            mode=SocketTimeoutMode.LIMITED,
            socket_timeout_s=1.0,
        )

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.drain", "invoke"),
        ("tcp_stream_engine.drain", "failed"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.drain",
        outcome="invoke",
    )
    failed = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.drain",
        outcome="failed",
    )

    assert invoke.meta.entity_id == "engine-log-8"
    assert failed.meta.entity_id == "engine-log-8"

    assert invoke.payload["engine_state"] == EngineState.OPENED.value
    assert invoke.payload["kwargs"]["timeout_mode"] == SocketTimeoutMode.LIMITED.value
    assert invoke.payload["kwargs"]["timeout_override_s"] == 1.0

    assert failed.payload["engine_state"] == EngineState.OPENED.value
    assert "error" in failed.payload


@pytest.mark.asyncio
async def test_o9_attach_and_detach_crypto_codec_log_result_values(
    remote_endpoint: _FakeRemoteEndpoint,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """attach/detach crypto codec emit public reconfiguration log events."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-9",
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    codec = _FakeCryptoCodec()

    attach_result = await eng.attach_crypto_codec(cast(Any, codec))
    detach_result = await eng.detach_crypto_codec()

    assert attach_result is TcpStreamReconfigOutcome.DONE
    assert detach_result is TcpStreamReconfigOutcome.DONE

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.attach_crypto_codec", "invoke"),
        ("tcp_stream_engine.attach_crypto_codec", "success"),
        ("tcp_stream_engine.detach_crypto_codec", "invoke"),
        ("tcp_stream_engine.detach_crypto_codec", "success"),
    ]

    attach_invoke = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.attach_crypto_codec",
        outcome="invoke",
    )
    attach_success = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.attach_crypto_codec",
        outcome="success",
    )
    detach_success = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.detach_crypto_codec",
        outcome="success",
    )

    assert attach_invoke.meta.entity_id == "engine-log-9"
    assert attach_success.meta.entity_id == "engine-log-9"
    assert detach_success.meta.entity_id == "engine-log-9"

    assert attach_invoke.payload["engine_state"] == EngineState.OPENED.value
    assert "crypto_codec" in attach_invoke.payload["kwargs"]

    assert attach_success.payload["result"] == TcpStreamReconfigOutcome.DONE.value
    assert detach_success.payload["result"] == TcpStreamReconfigOutcome.DONE.value


@pytest.mark.asyncio
async def test_o10_read_error_emits_abortive_close_internal_event(
    remote_endpoint: _FakeRemoteEndpoint,
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
):
    """read() I/O error emits failed public event and engine abortive_close event."""
    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        log_context=memory_log_context,
        entity_id="engine-log-10",
    )
    eng._state = EngineState.OPENED

    reader = _FakeStreamReader()
    reader.set_exc(ConnectionResetError("reset"))
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    with pytest.raises(TcpStreamIoError):
        await eng.read(1)

    assert _log_pairs(memory_log_sink) == [
        ("tcp_stream_engine.read", "invoke"),
        ("tcp_stream_engine.abortive_close", None),
        ("tcp_stream_engine.read", "failed"),
    ]

    abortive = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.abortive_close",
        outcome=None,
    )
    failed = _single_log_event(
        memory_log_sink,
        event_name="tcp_stream_engine.read",
        outcome="failed",
    )

    assert abortive.meta.entity_id == "engine-log-10"
    assert "due_to" in abortive.payload

    assert failed.meta.entity_id == "engine-log-10"
    assert failed.payload["engine_state"] == EngineState.CLOSED.value
    assert "error" in failed.payload


# -------------------------
# Group p: metrics integration
# -------------------------


class _MemoryMetricsRecorder:
    def __init__(self) -> None:
        self.metrics: list[Metric] = []
        self.events: list[MetricEvent] = []

    def register_metric(self, metric: Metric) -> None:
        self.metrics.append(metric)

    def register_event(self, event: MetricEvent) -> None:
        self.events.append(event)

    def get_metric_snapshots(self) -> Mapping[str, Mapping[str, Any]]:
        return {metric.metric_name: metric.snapshot() for metric in self.metrics}

    def iter_metrics(self) -> Iterable[Metric]:
        return iter(self.metrics)


class _FailingMetricsRecorder:
    def register_metric(self, metric: Metric) -> None:
        _ = self, metric
        raise RuntimeError("metric registration failed")

    def register_event(self, event: MetricEvent) -> None:
        _ = self, event

    def get_metric_snapshots(self) -> Mapping[str, Mapping[str, Any]]:
        _ = self
        return {}

    def iter_metrics(self) -> Iterable[Metric]:
        _ = self
        return iter(())


def _last_metric_event(recorder: _MemoryMetricsRecorder) -> MetricEvent:
    assert recorder.events
    return recorder.events[-1]


def _metric_events_of_type(
    recorder: _MemoryMetricsRecorder,
    event_type: type[MetricEvent],
) -> list[MetricEvent]:
    return [event for event in recorder.events if isinstance(event, event_type)]


def _single_metric_event_of_type(
    recorder: _MemoryMetricsRecorder,
    event_type: type[MetricEvent],
) -> MetricEvent:
    events = _metric_events_of_type(recorder, event_type)

    assert len(events) == 1

    return events[0]


def test_p1_ctor_registers_standard_tcp_stream_metrics(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Constructor registers standard TcpStreamEngine metrics."""
    recorder = _MemoryMetricsRecorder()

    _ = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    assert [metric.metric_name for metric in recorder.metrics] == [
        "tcp_stream.open.attempts",
        "tcp_stream.close.attempts",
        "tcp_stream.start_tls.attempts",
        "tcp_stream.crypto_codec.attach.attempts",
        "tcp_stream.crypto_codec.detach.attempts",
        "tcp_stream.stream_read.attempts",
        "tcp_stream.stream_write.attempts",
        "tcp_stream.drain.attempts",
        "tcp_stream.bytes.received",
        "tcp_stream.bytes.sent",
        "tcp_stream.remote_disconnect",
        "tcp_stream.abortive_close",
    ]

    assert recorder.events == []


def test_p2_ctor_registers_expected_metric_instances(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Constructor registers concrete standard metric objects."""
    recorder = _MemoryMetricsRecorder()

    _ = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    assert len(recorder.metrics) == 12

    assert isinstance(recorder.metrics[0], TcpStreamOpenAttemptsMetric)
    assert isinstance(recorder.metrics[1], TcpStreamCloseAttemptsMetric)
    assert isinstance(recorder.metrics[2], TcpStreamStartTlsAttemptsMetric)
    assert isinstance(recorder.metrics[3], TcpStreamCryptoCodecAttachAttemptsMetric)
    assert isinstance(recorder.metrics[4], TcpStreamCryptoCodecDetachAttemptsMetric)
    assert isinstance(recorder.metrics[5], TcpStreamStreamReadAttemptsMetric)
    assert isinstance(recorder.metrics[6], TcpStreamStreamWriteAttemptsMetric)
    assert isinstance(recorder.metrics[7], TcpStreamDrainAttemptsMetric)
    assert isinstance(recorder.metrics[8], TcpStreamBytesReceivedMetric)
    assert isinstance(recorder.metrics[9], TcpStreamBytesSentMetric)
    assert isinstance(recorder.metrics[10], TcpStreamRemoteDisconnectMetric)
    assert isinstance(recorder.metrics[11], TcpStreamAbortiveCloseMetric)


def test_p3_ctor_without_metrics_recorder_does_not_register_metrics(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Constructor works without metrics recorder."""
    eng = TcpStreamEngine(remote_endpoint=cast(Any, remote_endpoint))

    assert eng.state is EngineState.VIRGIN


def test_p4_ctor_rejects_invalid_metrics_recorder(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Constructor rejects object that does not implement MetricsRecorderProto."""
    with pytest.raises(TypeError, match="metrics_recorder"):
        _ = TcpStreamEngine(
            remote_endpoint=cast(Any, remote_endpoint),
            metrics_recorder=cast(Any, object()),
        )


def test_p5_metric_registration_failure_does_not_break_constructor(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """Custom recorder registration failure does not break engine construction."""
    recorder = _FailingMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    assert eng.state is EngineState.VIRGIN


@pytest.mark.asyncio
async def test_p6_open_success_emits_open_success_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    recorder = _MemoryMetricsRecorder()
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    result = await eng.open(use_ssl=True)

    event = _last_metric_event(recorder)

    assert result is TcpStreamOpenOutcome.OPENED
    assert isinstance(event, TcpStreamOpenAttemptMetricEvent)
    assert event.event_type == "tcp_stream.open.attempt"
    assert event.use_ssl is True
    assert event.outcome is TcpStreamOpenAttemptOutcome.SUCCESS


@pytest.mark.asyncio
async def test_p7_open_already_opened_emits_open_already_opened_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    recorder = _MemoryMetricsRecorder()
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    first = await eng.open()
    second = await eng.open()

    assert first is TcpStreamOpenOutcome.OPENED
    assert second is TcpStreamOpenOutcome.ALREADY_OPENED

    assert len(recorder.events) == 2

    event = recorder.events[-1]
    assert isinstance(event, TcpStreamOpenAttemptMetricEvent)
    assert event.outcome is TcpStreamOpenAttemptOutcome.ALREADY_OPENED


@pytest.mark.asyncio
async def test_p8_open_failure_emits_open_failure_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()
    remote_endpoint.set_exception(RuntimeError("boom"))

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    with pytest.raises(TcpStreamEngineUnexpectedError):
        await eng.open()

    event = _last_metric_event(recorder)

    assert isinstance(event, TcpStreamOpenAttemptMetricEvent)
    assert event.outcome is TcpStreamOpenAttemptOutcome.FAILURE


@pytest.mark.asyncio
async def test_p9_close_success_emits_close_success_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    result = await eng.close()

    event = _last_metric_event(recorder)

    assert result is TcpStreamCloseOutcome.CLOSED
    assert isinstance(event, TcpStreamCloseAttemptMetricEvent)
    assert event.event_type == "tcp_stream.close.attempt"
    assert event.outcome is TcpStreamCloseAttemptOutcome.SUCCESS


@pytest.mark.asyncio
async def test_p10_close_not_opened_emits_close_not_opened_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    result = await eng.close()

    event = _last_metric_event(recorder)

    assert result is TcpStreamCloseOutcome.NOT_OPENED
    assert isinstance(event, TcpStreamCloseAttemptMetricEvent)
    assert event.outcome is TcpStreamCloseAttemptOutcome.NOT_OPENED


@pytest.mark.asyncio
async def test_p11_start_tls_success_emits_start_tls_success_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    async def fake_wrap_stream_tls(
        info: Any,
        writer: Any,
        *,
        handshake_timeout_s: float | None = None,
    ) -> None:
        _ = info, writer, handshake_timeout_s

    async def fake_run_with_cancellation_policy(
        factory: Any,
        *,
        policy: Any,
    ) -> tuple[bool, Any]:
        _ = policy
        _result = await factory()
        return False, _result

    monkeypatch.setattr(module_under_test, "wrap_stream_tls", fake_wrap_stream_tls)
    monkeypatch.setattr(
        module_under_test,
        "run_with_cancellation_policy",
        fake_run_with_cancellation_policy,
    )

    result = await eng.start_tls()

    event = _last_metric_event(recorder)

    assert result is TcpStreamReconfigOutcome.DONE
    assert isinstance(event, TcpStreamStartTlsAttemptMetricEvent)
    assert event.event_type == "tcp_stream.start_tls.attempt"
    assert event.outcome is TcpStreamStartTlsAttemptOutcome.SUCCESS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("security_mode", "result", "outcome"),
    [
        (
            TcpStreamSecurityMode.SSL,
            TcpStreamReconfigOutcome.REFUSED_CONNECTION_ALREADY_UNDER_SSL,
            TcpStreamStartTlsAttemptOutcome.REFUSED_ALREADY_UNDER_SSL,
        ),
        (
            TcpStreamSecurityMode.START_TLS,
            TcpStreamReconfigOutcome.REFUSED_START_TLS_ALREADY_ACTIVE,
            TcpStreamStartTlsAttemptOutcome.REFUSED_START_TLS_ALREADY_ACTIVE,
        ),
        (
            TcpStreamSecurityMode.CODEC,
            TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_ATTACHED,
            TcpStreamStartTlsAttemptOutcome.REFUSED_CRYPTO_CODEC_ATTACHED,
        ),
    ],
)
async def test_p12_start_tls_refused_by_security_mode_emits_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
    security_mode: TcpStreamSecurityMode,
    result: TcpStreamReconfigOutcome,
    outcome: TcpStreamStartTlsAttemptOutcome,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = security_mode

    actual_result = await eng.start_tls()

    event = _last_metric_event(recorder)

    assert actual_result is result
    assert isinstance(event, TcpStreamStartTlsAttemptMetricEvent)
    assert event.outcome is outcome


@pytest.mark.asyncio
async def test_p13_start_tls_refused_not_opened_emits_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    result = await eng.start_tls()

    event = _last_metric_event(recorder)

    assert result is TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED
    assert isinstance(event, TcpStreamStartTlsAttemptMetricEvent)
    assert event.outcome is TcpStreamStartTlsAttemptOutcome.REFUSED_NOT_OPENED


@pytest.mark.asyncio
async def test_p14_attach_success_emits_attach_success_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._security_mode = TcpStreamSecurityMode.PLAIN

    result = await eng.attach_crypto_codec(cast(Any, _FakeCryptoCodec()))

    event = _last_metric_event(recorder)

    assert result is TcpStreamReconfigOutcome.DONE
    assert isinstance(event, TcpStreamCryptoCodecAttachAttemptMetricEvent)
    assert event.event_type == "tcp_stream.crypto_codec.attach.attempt"
    assert event.outcome is TcpStreamCryptoCodecAttachAttemptOutcome.SUCCESS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "security_mode", "has_codec", "result", "outcome"),
    [
        (
            EngineState.VIRGIN,
            TcpStreamSecurityMode.NOT_AVAILABLE,
            False,
            TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED,
            TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_NOT_OPENED,
        ),
        (
            EngineState.OPENED,
            TcpStreamSecurityMode.SSL,
            False,
            TcpStreamReconfigOutcome.REFUSED_CONNECTION_ALREADY_UNDER_SSL,
            TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_UNDER_SSL,
        ),
        (
            EngineState.OPENED,
            TcpStreamSecurityMode.START_TLS,
            False,
            TcpStreamReconfigOutcome.REFUSED_START_TLS_ALREADY_ACTIVE,
            TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_START_TLS_ACTIVE,
        ),
        (
            EngineState.OPENED,
            TcpStreamSecurityMode.PLAIN,
            True,
            TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_ATTACHED,
            TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_ATTACHED,
        ),
    ],
)
async def test_p15_attach_refused_emits_attach_refused_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
    state: EngineState,
    security_mode: TcpStreamSecurityMode,
    has_codec: bool,
    result: TcpStreamReconfigOutcome,
    outcome: TcpStreamCryptoCodecAttachAttemptOutcome,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = state
    eng._security_mode = security_mode

    if state is EngineState.OPENED:
        eng._reader = cast(Any, _FakeStreamReader())
        eng._writer = cast(Any, _FakeStreamWriter())

    if has_codec:
        eng._crypto_codec = cast(Any, _FakeCryptoCodec())

    actual_result = await eng.attach_crypto_codec(cast(Any, _FakeCryptoCodec()))

    event = _last_metric_event(recorder)

    assert actual_result is result
    assert isinstance(event, TcpStreamCryptoCodecAttachAttemptMetricEvent)
    assert event.outcome is outcome


@pytest.mark.asyncio
async def test_p16_detach_success_emits_detach_success_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())
    eng._crypto_codec = cast(Any, _FakeCryptoCodec())
    eng._security_mode = TcpStreamSecurityMode.CODEC

    result = await eng.detach_crypto_codec()

    event = _last_metric_event(recorder)

    assert result is TcpStreamReconfigOutcome.DONE
    assert isinstance(event, TcpStreamCryptoCodecDetachAttemptMetricEvent)
    assert event.event_type == "tcp_stream.crypto_codec.detach.attempt"
    assert event.outcome is TcpStreamCryptoCodecDetachAttemptOutcome.SUCCESS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "has_codec", "result", "outcome"),
    [
        (
            EngineState.VIRGIN,
            True,
            TcpStreamReconfigOutcome.REFUSED_CONNECTION_NOT_OPENED,
            TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_OPENED,
        ),
        (
            EngineState.OPENED,
            False,
            TcpStreamReconfigOutcome.REFUSED_CRYPTO_CODEC_NOT_ATTACHED,
            TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_ATTACHED,
        ),
    ],
)
async def test_p17_detach_refused_emits_detach_refused_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
    state: EngineState,
    has_codec: bool,
    result: TcpStreamReconfigOutcome,
    outcome: TcpStreamCryptoCodecDetachAttemptOutcome,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = state

    if state is EngineState.OPENED:
        eng._reader = cast(Any, _FakeStreamReader())
        eng._writer = cast(Any, _FakeStreamWriter())

    if has_codec:
        eng._crypto_codec = cast(Any, _FakeCryptoCodec())

    actual_result = await eng.detach_crypto_codec()

    event = _last_metric_event(recorder)

    assert actual_result is result
    assert isinstance(event, TcpStreamCryptoCodecDetachAttemptMetricEvent)
    assert event.outcome is outcome


@pytest.mark.asyncio
async def test_p18_read_success_emits_stream_read_success_and_bytes_received_events(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED

    reader = _FakeStreamReader()
    reader.set_next(b"hello")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    # noinspection PyArgumentEqualDefault
    result = await eng.read(5, mode=SocketTimeoutMode.UNLIMITED)

    stream_read_event = _single_metric_event_of_type(
        recorder,
        TcpStreamStreamReadAttemptMetricEvent,
    )
    bytes_received_event = _single_metric_event_of_type(
        recorder,
        TcpStreamBytesReceivedMetricEvent,
    )

    assert result == b"hello"

    assert isinstance(stream_read_event, TcpStreamStreamReadAttemptMetricEvent)
    assert stream_read_event.event_type == "tcp_stream.stream_read.attempt"
    assert stream_read_event.outcome is TcpStreamStreamReadAttemptOutcome.SUCCESS

    assert isinstance(bytes_received_event, TcpStreamBytesReceivedMetricEvent)
    assert bytes_received_event.event_type == "tcp_stream.bytes.received"
    assert bytes_received_event.size == 5


@pytest.mark.asyncio
async def test_p19_read_timeout_emits_stream_read_timeout_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    with pytest.raises(SocketTimeoutError):
        await eng.read(
            10,
            mode=SocketTimeoutMode.LIMITED,
            socket_timeout_s=1.0,
        )

    event = _single_metric_event_of_type(
        recorder,
        TcpStreamStreamReadAttemptMetricEvent,
    )

    assert isinstance(event, TcpStreamStreamReadAttemptMetricEvent)
    assert event.outcome is TcpStreamStreamReadAttemptOutcome.TIMEOUT
    assert _metric_events_of_type(recorder, TcpStreamBytesReceivedMetricEvent) == []


@pytest.mark.asyncio
async def test_p20_read_cancelled_emits_stream_read_cancelled_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED

    gate = asyncio.Event()
    reader = _FakeStreamReader()
    reader.set_gate(gate)
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    # noinspection PyArgumentEqualDefault
    task = asyncio.create_task(eng.read(1, mode=SocketTimeoutMode.UNLIMITED))
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    event = _single_metric_event_of_type(
        recorder,
        TcpStreamStreamReadAttemptMetricEvent,
    )

    assert isinstance(event, TcpStreamStreamReadAttemptMetricEvent)
    assert event.outcome is TcpStreamStreamReadAttemptOutcome.CANCELLED


@pytest.mark.asyncio
async def test_p21_read_io_error_emits_stream_read_error_and_abortive_close_events(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED

    reader = _FakeStreamReader()
    reader.set_exc(ConnectionResetError("reset"))
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    with pytest.raises(TcpStreamIoError):
        # noinspection PyArgumentEqualDefault
        await eng.read(1, mode=SocketTimeoutMode.UNLIMITED)

    stream_read_event = _single_metric_event_of_type(
        recorder,
        TcpStreamStreamReadAttemptMetricEvent,
    )
    abortive_close_event = _single_metric_event_of_type(
        recorder,
        TcpStreamAbortiveCloseMetricEvent,
    )

    assert isinstance(stream_read_event, TcpStreamStreamReadAttemptMetricEvent)
    assert stream_read_event.outcome is TcpStreamStreamReadAttemptOutcome.ERROR

    assert isinstance(abortive_close_event, TcpStreamAbortiveCloseMetricEvent)
    assert abortive_close_event.event_type == "tcp_stream.abortive_close"


@pytest.mark.asyncio
async def test_p22_read_tls_error_emits_stream_read_tls_error_and_abortive_close_events(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._security_mode = TcpStreamSecurityMode.START_TLS

    reader = _FakeStreamReader()
    reader.set_exc(ssl.SSLError("ssl-read-failed"))
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    with pytest.raises(TlsError):
        # noinspection PyArgumentEqualDefault
        await eng.read(1, mode=SocketTimeoutMode.UNLIMITED)

    stream_read_event = _single_metric_event_of_type(
        recorder,
        TcpStreamStreamReadAttemptMetricEvent,
    )
    abortive_close_event = _single_metric_event_of_type(
        recorder,
        TcpStreamAbortiveCloseMetricEvent,
    )

    assert isinstance(stream_read_event, TcpStreamStreamReadAttemptMetricEvent)
    assert stream_read_event.outcome is TcpStreamStreamReadAttemptOutcome.TLS_ERROR

    assert isinstance(abortive_close_event, TcpStreamAbortiveCloseMetricEvent)
    assert abortive_close_event.event_type == "tcp_stream.abortive_close"


@pytest.mark.asyncio
async def test_p23_read_eof_emits_remote_disconnect_and_abortive_close_events(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED

    reader = _FakeStreamReader()
    reader.set_next(b"")
    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, _FakeStreamWriter())

    with pytest.raises(TcpStreamRemotelyDisconnectedError):
        # noinspection PyArgumentEqualDefault
        await eng.read(1024, mode=SocketTimeoutMode.UNLIMITED)

    remote_disconnect_event = _single_metric_event_of_type(
        recorder,
        TcpStreamRemoteDisconnectMetricEvent,
    )
    abortive_close_event = _single_metric_event_of_type(
        recorder,
        TcpStreamAbortiveCloseMetricEvent,
    )

    assert isinstance(remote_disconnect_event, TcpStreamRemoteDisconnectMetricEvent)
    assert remote_disconnect_event.event_type == "tcp_stream.remote_disconnect"

    assert isinstance(abortive_close_event, TcpStreamAbortiveCloseMetricEvent)
    assert abortive_close_event.event_type == "tcp_stream.abortive_close"

    assert _metric_events_of_type(recorder, TcpStreamStreamReadAttemptMetricEvent) == []
    assert _metric_events_of_type(recorder, TcpStreamBytesReceivedMetricEvent) == []


def test_p24_write_success_emits_stream_write_success_and_bytes_sent_events(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    eng.write(b"abc")

    stream_write_event = _single_metric_event_of_type(
        recorder,
        TcpStreamStreamWriteAttemptMetricEvent,
    )
    bytes_sent_event = _single_metric_event_of_type(
        recorder,
        TcpStreamBytesSentMetricEvent,
    )

    assert isinstance(stream_write_event, TcpStreamStreamWriteAttemptMetricEvent)
    assert stream_write_event.event_type == "tcp_stream.stream_write.attempt"
    assert stream_write_event.outcome is TcpStreamStreamWriteAttemptOutcome.SUCCESS

    assert isinstance(bytes_sent_event, TcpStreamBytesSentMetricEvent)
    assert bytes_sent_event.event_type == "tcp_stream.bytes.sent"
    assert bytes_sent_event.size == 3


def test_p25_write_io_error_emits_stream_write_error_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    writer.set_write_exc(BrokenPipeError("pipe"))
    eng._writer = cast(Any, writer)

    with pytest.raises(TcpStreamIoError):
        eng.write(b"abc")

    event = _single_metric_event_of_type(
        recorder,
        TcpStreamStreamWriteAttemptMetricEvent,
    )

    assert isinstance(event, TcpStreamStreamWriteAttemptMetricEvent)
    assert event.outcome is TcpStreamStreamWriteAttemptOutcome.ERROR
    assert _metric_events_of_type(recorder, TcpStreamBytesSentMetricEvent) == []


def test_p26_write_tls_error_emits_stream_write_tls_error_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    writer.set_write_exc(ssl.SSLError("ssl-write-failed"))
    eng._writer = cast(Any, writer)

    with pytest.raises(TlsError):
        eng.write(b"abc")

    event = _single_metric_event_of_type(
        recorder,
        TcpStreamStreamWriteAttemptMetricEvent,
    )

    assert isinstance(event, TcpStreamStreamWriteAttemptMetricEvent)
    assert event.outcome is TcpStreamStreamWriteAttemptOutcome.TLS_ERROR
    assert _metric_events_of_type(recorder, TcpStreamBytesSentMetricEvent) == []


@pytest.mark.asyncio
async def test_p27_drain_success_emits_drain_success_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    # noinspection PyArgumentEqualDefault
    await eng.drain(mode=SocketTimeoutMode.UNLIMITED)

    event = _single_metric_event_of_type(
        recorder,
        TcpStreamDrainAttemptMetricEvent,
    )

    assert isinstance(event, TcpStreamDrainAttemptMetricEvent)
    assert event.event_type == "tcp_stream.drain.attempt"
    assert event.outcome is TcpStreamDrainAttemptOutcome.SUCCESS


@pytest.mark.asyncio
async def test_p28_drain_timeout_emits_drain_timeout_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())
    eng._writer = cast(Any, _FakeStreamWriter())

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(module_under_test.asyncio, "wait_for", fake_wait_for)

    with pytest.raises(SocketTimeoutError):
        await eng.drain(
            mode=SocketTimeoutMode.LIMITED,
            socket_timeout_s=1.0,
        )

    event = _single_metric_event_of_type(
        recorder,
        TcpStreamDrainAttemptMetricEvent,
    )

    assert isinstance(event, TcpStreamDrainAttemptMetricEvent)
    assert event.outcome is TcpStreamDrainAttemptOutcome.TIMEOUT


@pytest.mark.asyncio
async def test_p29_drain_cancelled_emits_drain_cancelled_metric_event(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    gate = asyncio.Event()
    writer.set_drain_gate(gate)
    eng._writer = cast(Any, writer)

    # noinspection PyArgumentEqualDefault
    task = asyncio.create_task(eng.drain(mode=SocketTimeoutMode.UNLIMITED))
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    event = _single_metric_event_of_type(
        recorder,
        TcpStreamDrainAttemptMetricEvent,
    )

    assert isinstance(event, TcpStreamDrainAttemptMetricEvent)
    assert event.outcome is TcpStreamDrainAttemptOutcome.CANCELLED


@pytest.mark.asyncio
async def test_p30_drain_io_error_emits_drain_error_and_abortive_close_events(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    writer.set_drain_exc(OSError(errno.EPIPE, "pipe"))
    eng._writer = cast(Any, writer)

    with pytest.raises(TcpStreamIoError):
        # noinspection PyArgumentEqualDefault
        await eng.drain(mode=SocketTimeoutMode.UNLIMITED)

    drain_event = _single_metric_event_of_type(
        recorder,
        TcpStreamDrainAttemptMetricEvent,
    )
    abortive_close_event = _single_metric_event_of_type(
        recorder,
        TcpStreamAbortiveCloseMetricEvent,
    )

    assert isinstance(drain_event, TcpStreamDrainAttemptMetricEvent)
    assert drain_event.outcome is TcpStreamDrainAttemptOutcome.ERROR

    assert isinstance(abortive_close_event, TcpStreamAbortiveCloseMetricEvent)
    assert abortive_close_event.event_type == "tcp_stream.abortive_close"


@pytest.mark.asyncio
async def test_p31_drain_tls_error_emits_drain_tls_error_and_abortive_close_events(
    remote_endpoint: _FakeRemoteEndpoint,
):
    recorder = _MemoryMetricsRecorder()

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )
    eng._state = EngineState.OPENED
    eng._reader = cast(Any, _FakeStreamReader())

    writer = _FakeStreamWriter()
    writer.set_drain_exc(ssl.SSLError("ssl-drain-failed"))
    eng._writer = cast(Any, writer)

    with pytest.raises(TlsError):
        # noinspection PyArgumentEqualDefault
        await eng.drain(mode=SocketTimeoutMode.UNLIMITED)

    drain_event = _single_metric_event_of_type(
        recorder,
        TcpStreamDrainAttemptMetricEvent,
    )
    abortive_close_event = _single_metric_event_of_type(
        recorder,
        TcpStreamAbortiveCloseMetricEvent,
    )

    assert isinstance(drain_event, TcpStreamDrainAttemptMetricEvent)
    assert drain_event.outcome is TcpStreamDrainAttemptOutcome.TLS_ERROR

    assert isinstance(abortive_close_event, TcpStreamAbortiveCloseMetricEvent)
    assert abortive_close_event.event_type == "tcp_stream.abortive_close"


class _FailingEventMetricsRecorder:
    def __init__(self) -> None:
        self.metrics: list[Metric] = []

    def register_metric(self, metric: Metric) -> None:
        self.metrics.append(metric)

    def register_event(self, event: MetricEvent) -> None:
        _ = self, event
        raise RuntimeError("metric event failed")

    def get_metric_snapshots(self) -> Mapping[str, Mapping[str, Any]]:
        return {metric.metric_name: metric.snapshot() for metric in self.metrics}

    def iter_metrics(self) -> Iterable[Metric]:
        return iter(self.metrics)


@pytest.mark.asyncio
async def test_p100_metric_event_failure_does_not_break_open_success(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    recorder = _FailingEventMetricsRecorder()
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=cast(Any, recorder),
    )

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        return _FakeStreamReader(), _FakeStreamWriter()

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    result = await eng.open()

    assert result is TcpStreamOpenOutcome.OPENED
    assert eng.state is EngineState.OPENED


async def _wait_metric_dimension(
    recorder: AsyncioMetricsRecorder,
    *,
    metric_name: str,
    dimension: str,
    expected: int,
) -> None:
    for _ in range(200):
        snapshots = recorder.get_metric_snapshots()
        metric_snapshot = snapshots.get(metric_name)

        if metric_snapshot is not None:
            dimensions = metric_snapshot["dimensions"]
            assert isinstance(dimensions, dict)

            if dimensions.get(dimension) == expected:
                return

        await asyncio.sleep(0.005)

    snapshots = recorder.get_metric_snapshots()
    raise AssertionError(
        f"metric {metric_name!r} dimension {dimension!r} "
        f"did not reach {expected!r}; snapshots={snapshots!r}"
    )


@pytest.mark.asyncio
async def test_p150_asyncio_metrics_recorder_smoke_records_engine_success_path(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """AsyncioMetricsRecorder receives TcpStreamEngine events and updates metrics."""
    recorder = AsyncioMetricsRecorder(entity_id="tcp-engine-metrics-smoke-1")
    remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=recorder,
    )

    reader = _FakeStreamReader()
    reader.set_next(b"hello")

    writer = _FakeStreamWriter()

    async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
        _ = info, cand, use_ssl
        return reader, writer

    monkeypatch.setattr(
        module_under_test.TcpStreamEngine,
        "_open_socket",
        staticmethod(stub_open_socket),
    )

    start_result = await recorder.start()
    assert start_result.success is True

    try:
        open_result = await eng.open()
        read_result = await eng.read(5, mode=SocketTimeoutMode.UNLIMITED)
        eng.write(b"abc")
        await eng.drain(mode=SocketTimeoutMode.UNLIMITED)
        close_result = await eng.close()

        assert open_result is TcpStreamOpenOutcome.OPENED
        assert read_result == b"hello"
        assert writer.write_calls == [b"abc"]
        assert writer.drain_calls == 1
        assert close_result is TcpStreamCloseOutcome.CLOSED

        stop_result = await recorder.stop()
        assert stop_result.success is True

        snapshots = recorder.get_metric_snapshots()

        assert snapshots["tcp_stream.open.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "already_opened_total": 0,
            "failure_total": 0,
            "cancelled_total": 0,
        }

        assert snapshots["tcp_stream.stream_read.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "timeout_total": 0,
            "error_total": 0,
            "cancelled_total": 0,
            "tls_error_total": 0,
        }

        assert snapshots["tcp_stream.bytes.received"]["dimensions"] == {
            "total": 5,
        }

        assert snapshots["tcp_stream.stream_write.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "error_total": 0,
            "tls_error_total": 0,
        }

        assert snapshots["tcp_stream.bytes.sent"]["dimensions"] == {
            "total": 3,
        }

        assert snapshots["tcp_stream.drain.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "timeout_total": 0,
            "error_total": 0,
            "cancelled_total": 0,
            "tls_error_total": 0,
        }

        assert snapshots["tcp_stream.close.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "not_opened_total": 0,
            "failure_total": 0,
            "cancelled_total": 0,
        }

    finally:
        if recorder.get_status().value == "RUNNING":
            await recorder.stop()


@pytest.mark.asyncio
async def test_p151_asyncio_metrics_recorder_smoke_records_remote_disconnect_path(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """AsyncioMetricsRecorder records remote disconnect and abortive close metrics."""
    recorder = AsyncioMetricsRecorder(entity_id="tcp-engine-metrics-smoke-2")

    eng = TcpStreamEngine(
        remote_endpoint=cast(Any, remote_endpoint),
        metrics_recorder=recorder,
    )

    eng._state = EngineState.OPENED

    reader = _FakeStreamReader()
    reader.set_next(b"")

    writer = _FakeStreamWriter()

    eng._reader = cast(Any, reader)
    eng._writer = cast(Any, writer)

    start_result = await recorder.start()
    assert start_result.success is True

    try:
        with pytest.raises(TcpStreamRemotelyDisconnectedError):
            await eng.read(1024, mode=SocketTimeoutMode.UNLIMITED)

        stop_result = await recorder.stop()
        assert stop_result.success is True

        snapshots = recorder.get_metric_snapshots()

        assert snapshots["tcp_stream.remote_disconnect"]["dimensions"] == {
            "total": 1,
        }

        assert snapshots["tcp_stream.abortive_close"]["dimensions"] == {
            "total": 1,
        }

        assert snapshots["tcp_stream.bytes.received"]["dimensions"] == {
            "total": 0,
        }

        assert snapshots["tcp_stream.stream_read.attempts"]["dimensions"] == {
            "total": 0,
            "success_total": 0,
            "timeout_total": 0,
            "error_total": 0,
            "cancelled_total": 0,
            "tls_error_total": 0,
        }

        assert eng.state is EngineState.CLOSED
        assert writer.close_calls == 1
        assert writer.wait_closed_calls == 1

    finally:
        if recorder.get_status().value == "RUNNING":
            await recorder.stop()


def _shutdown_metrics_runtime_safely(runtime: MetricsRuntime) -> None:
    # noinspection PyBroadException
    try:
        runtime.shutdown()
    except Exception:
        pass


@pytest.mark.asyncio
async def test_p101_metrics_runtime_smoke_records_tcp_stream_engine_success_path(
    remote_endpoint: _FakeRemoteEndpoint,
    module_under_test,
    monkeypatch,
):
    """MetricsRuntime-created recorder receives TcpStreamEngine metrics end-to-end."""
    runtime = MetricsRuntime(namespace="tcp-stream-engine-runtime-smoke")

    try:
        runtime.start()
        recorder = runtime.create_recorder("engine-recorder")

        remote_endpoint.set_candidates([_ai_inet("192.0.2.10", 389)])

        eng = TcpStreamEngine(
            remote_endpoint=cast(Any, remote_endpoint),
            metrics_recorder=recorder,
        )

        reader = _FakeStreamReader()
        reader.set_next(b"hello")

        writer = _FakeStreamWriter()

        async def stub_open_socket(info: Any, cand: Any, *, use_ssl: bool) -> Any:
            _ = info, cand, use_ssl
            return reader, writer

        monkeypatch.setattr(
            module_under_test.TcpStreamEngine,
            "_open_socket",
            staticmethod(stub_open_socket),
        )

        open_result = await eng.open()
        read_result = await eng.read(5, mode=SocketTimeoutMode.UNLIMITED)
        eng.write(b"abc")
        await eng.drain(mode=SocketTimeoutMode.UNLIMITED)
        close_result = await eng.close()

        assert open_result is TcpStreamOpenOutcome.OPENED
        assert read_result == b"hello"
        assert writer.write_calls == [b"abc"]
        assert writer.drain_calls == 1
        assert close_result is TcpStreamCloseOutcome.CLOSED

        runtime.stop_recorder("engine-recorder")

        snapshots = recorder.get_metric_snapshots()

        assert snapshots["tcp_stream.open.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "already_opened_total": 0,
            "failure_total": 0,
            "cancelled_total": 0,
        }

        assert snapshots["tcp_stream.stream_read.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "timeout_total": 0,
            "error_total": 0,
            "cancelled_total": 0,
            "tls_error_total": 0,
        }

        assert snapshots["tcp_stream.bytes.received"]["dimensions"] == {
            "total": 5,
        }

        assert snapshots["tcp_stream.stream_write.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "error_total": 0,
            "tls_error_total": 0,
        }

        assert snapshots["tcp_stream.bytes.sent"]["dimensions"] == {
            "total": 3,
        }

        assert snapshots["tcp_stream.drain.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "timeout_total": 0,
            "error_total": 0,
            "cancelled_total": 0,
            "tls_error_total": 0,
        }

        assert snapshots["tcp_stream.close.attempts"]["dimensions"] == {
            "total": 1,
            "success_total": 1,
            "not_opened_total": 0,
            "failure_total": 0,
            "cancelled_total": 0,
        }

    finally:
        _shutdown_metrics_runtime_safely(runtime)


@pytest.mark.asyncio
async def test_p102_metrics_runtime_smoke_records_tcp_stream_engine_abortive_path(
    remote_endpoint: _FakeRemoteEndpoint,
):
    """MetricsRuntime-created recorder records remote disconnect and abortive close."""
    runtime = MetricsRuntime(namespace="tcp-stream-engine-runtime-smoke")

    try:
        runtime.start()
        recorder = runtime.create_recorder("engine-recorder")

        eng = TcpStreamEngine(
            remote_endpoint=cast(Any, remote_endpoint),
            metrics_recorder=recorder,
        )

        eng._state = EngineState.OPENED

        reader = _FakeStreamReader()
        reader.set_next(b"")

        writer = _FakeStreamWriter()

        eng._reader = cast(Any, reader)
        eng._writer = cast(Any, writer)

        with pytest.raises(TcpStreamRemotelyDisconnectedError):
            await eng.read(1024, mode=SocketTimeoutMode.UNLIMITED)

        runtime.stop_recorder("engine-recorder")

        snapshots = recorder.get_metric_snapshots()

        assert snapshots["tcp_stream.remote_disconnect"]["dimensions"] == {
            "total": 1,
        }

        assert snapshots["tcp_stream.abortive_close"]["dimensions"] == {
            "total": 1,
        }

        assert snapshots["tcp_stream.bytes.received"]["dimensions"] == {
            "total": 0,
        }

        assert snapshots["tcp_stream.stream_read.attempts"]["dimensions"] == {
            "total": 0,
            "success_total": 0,
            "timeout_total": 0,
            "error_total": 0,
            "cancelled_total": 0,
            "tls_error_total": 0,
        }

        assert eng.state is EngineState.CLOSED
        assert writer.close_calls == 1
        assert writer.wait_closed_calls == 1

    finally:
        _shutdown_metrics_runtime_safely(runtime)
