# src/mvx/networking/net_errors.py

"""
ReasonedError
└─ NetError
   ├─ OpenSocketError
   ├─ TlsError
   │  └─ TlsHostnameMismatchError     (also SSLCertVerificationError)
   ├─ OpenConnectionError
   ├─ NetIoBaseError
   │  ├─ TcpStreamRemotelyDisconnectedError
   │  ├─ TcpStreamIoError
   │  └─ SocketTimeoutError
   ├─ ServerInvalidResponseError
   └─ CryptoCodecError
      ├─ CryptoCodecReadError
      └─ CryptoCodecWriteError
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Mapping, Optional

from ssl import SSLCertVerificationError

from mvx.common.errors import ReasonedError

from .models import (
    TcpIoOperation,
    UdpIoOperation,
    TCP_READ,
    EngineState,
    SocketTimeoutMode,
)

# ---- NetError ----------------------------------------------------------------------------


class NetError(ReasonedError):
    """
    Base error type for network transport-layer failures.
    """

    pass


# ---- OpenSocketError ---------------------------------------------------------------------


class OpenSocketErrorReason(StrEnum):
    """
    Stable classifiers for failures while establishing a single socket/TLS connection.

    These values are designed to be:
      - bounded (finite set),
      - log-friendly,
      - suitable for metrics/alerts and deterministic tests.

    Classification guidelines (what each reason means)
    --------------------------------------------------
    SOCKET_CREATE_FAILED
        Failed to create a socket object for the given candidate address
        (e.g., resource exhaustion, unsupported family/type/proto).

    SOCKET_BIND_FAILED
        Failed to bind the socket locally to (source_address, source_port)
        before connecting (e.g., address not available, permission denied,
        port already in use, invalid bind tuple).

    SOCKET_CONNECT_TIMEOUT
        Connecting to the remote endpoint did not complete within the configured
        connect timeout. This is a connect-stage timeout.

    SOCKET_CONNECT_REFUSED
        Remote endpoint actively refused the connection (typically RST / closed port).

    SOCKET_CONNECT_NO_ROUTE_TO_HOST
        No network route is available to reach the destination network/host
        (e.g., ENETUNREACH). This indicates routing/network reachability issues.

    SOCKET_CONNECT_HOST_UNREACHABLE
        Destination host is unreachable even though a route may exist
        (e.g., EHOSTUNREACH/EHOSTDOWN). This indicates host reachability issues.

    SOCKET_CONNECT_FAILED
        Connect failed for other OS-level reasons (excluding timeout/refused/no-route/host-unreachable).

    SOCKET_SSL_WRAP_FAILED
        The TCP connection was established, but TLS/SSL wrapping or handshake failed.

    SOCKET_OPEN_FAILED_UNKNOWN
        Fallback when the failure could not be confidently classified into the above reasons.
    """

    SOCKET_CREATE_FAILED = "SOCKET_CREATE_FAILED"
    SOCKET_BIND_FAILED = "SOCKET_BIND_FAILED"
    SOCKET_CONNECT_TIMEOUT = "SOCKET_CONNECT_TIMEOUT"
    SOCKET_CONNECT_REFUSED = "SOCKET_CONNECT_REFUSED"
    SOCKET_CONNECT_NO_ROUTE_TO_HOST = "SOCKET_CONNECT_NO_ROUTE_TO_HOST"
    SOCKET_CONNECT_HOST_UNREACHABLE = "SOCKET_CONNECT_HOST_UNREACHABLE"
    SOCKET_CONNECT_FAILED = "SOCKET_CONNECT_FAILED"
    SOCKET_SSL_WRAP_FAILED = "SOCKET_SSL_WRAP_FAILED"
    SOCKET_OPEN_FAILED_UNKNOWN = "SOCKET_OPEN_FAILED_UNKNOWN"


class OpenSocketError(NetError):
    """
    Failure to establish a connection for a single address candidate.

    Attributes
    ----------
    candidate : str
        Stable identifier of the candidate that was attempted (e.g. "<ip>:<port>").

    reason: OpenSocketErrorReason
        Stable classifier describing *where and why* the attempt failed.

    details
        Structured context derived from the candidate and server configuration,
        suitable for logs/telemetry (e.g., family/socktype/proto, timeouts,
        source bind settings).

    cause
        The underlying exception that triggered the failure (e.g., TimeoutError,
        ConnectionRefusedError, OSError, TLS exceptions). This is optional and
        included for debugging; higher layers should rely on `reason_code`.
    """

    def __init__(
        self,
        *,
        reason: OpenSocketErrorReason,
        candidate: str,
        details: Optional[Mapping[str, Any]] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        self.candidate = candidate
        super().__init__(
            message=f"open socket error: {reason}",
            details=details,
            cause=cause,
            reason=reason.value,
        )


# ---- TlsError ----------------------------------------------------------------------------


class TlsErrorReason(StrEnum):
    """
    Stable classifiers for TLS-related failures during transport wrapping/handshake.

    Classification guidelines
    -------------------------
    TLS_CONTEXT_BUILD_FAILED
        Failed to create/configure SSLContext (invalid params, missing CA/cert/key,
        unsupported ciphers/protocols, platform/OpenSSL restrictions).

    TLS_HANDSHAKE_TIMEOUT
        TLS handshake did not complete within the configured handshake timeout.

    TLS_HANDSHAKE_VERIFY_FAILED
        TLS handshake completed enough to verify the peer, but verification failed
        (CA trust failure, chain issues, certificate expired/not-yet-valid, etc.).

    TLS_HANDSHAKE_FAILED
        TLS handshake failed for other protocol/IO reasons (alerts, unsupported versions,
        record/EOF errors, etc.).

    TLS_HANDSHAKE_UNKNOWN
        Fallback when handshake failure cannot be classified further.

    TLS_SSL_OBJECT_MISSING
        TLS wrapping reported success but the SSL object was not available from the stream.

    TLS_PEER_CERT_MISSING
        Peer certificate could not be obtained when required.

    TLS_PEER_CERT_MALFORMED
        Peer certificate data exists but cannot be parsed/decoded.

    TLS_HOSTNAME_MISMATCH
        Peer certificate is valid but does not match any allowed hostname/SAN.

    TLS_SESSION_CLOSED_CLEANLY_BY_PEER
        The peer cleanly closed the established TLS session
        (for example, close_notify / SSLZeroReturnError).
        This indicates termination of the TLS layer itself and does not, by
        itself, guarantee that the underlying TCP transport has already been closed.

    TLS_SESSION_TERMINATED_ABRUPTLY
        The established TLS session terminated abruptly
        (for example, SSLEOFError or other unexpected TLS-level stream breakage).
        This indicates abnormal TLS-layer termination; the underlying transport
        should generally be treated as no longer safe for continued use.
    """

    TLS_CONTEXT_BUILD_FAILED = "TLS_CONTEXT_BUILD_FAILED"
    TLS_HANDSHAKE_TIMEOUT = "TLS_HANDSHAKE_TIMEOUT"
    TLS_HANDSHAKE_VERIFY_FAILED = "TLS_HANDSHAKE_VERIFY_FAILED"
    TLS_HANDSHAKE_FAILED = "TLS_HANDSHAKE_FAILED"
    TLS_HANDSHAKE_UNKNOWN = "TLS_HANDSHAKE_UNKNOWN"
    TLS_SSL_OBJECT_MISSING = "TLS_SSL_OBJECT_MISSING"
    TLS_PEER_CERT_MISSING = "TLS_PEER_CERT_MISSING"
    TLS_PEER_CERT_MALFORMED = "TLS_PEER_CERT_MALFORMED"
    TLS_HOSTNAME_MISMATCH = "TLS_HOSTNAME_MISMATCH"
    TLS_SESSION_CLOSED_CLEANLY_BY_PEER = "TLS_SESSION_CLOSED_CLEANLY_BY_PEER"
    TLS_SESSION_TERMINATED_ABRUPTLY = "TLS_SESSION_TERMINATED_ABRUPTLY"
    TLS_UNEXPECTED_ERROR = "TLS_UNEXPECTED_ERROR"


class TlsError(NetError):
    """
    TLS failure raised by the transport during TLS context creation or handshake.

    `reason` is a TlsErrorReason providing a stable, bounded classification.
    The original TLS/IO exception may be attached as `cause` for debugging, but
    operational logic should primarily rely on `reason_code` and `details`.
    """

    def __init__(
        self,
        *,
        reason: TlsErrorReason,
        details: Optional[Mapping[str, Any]] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        super().__init__(
            message=f"tls error: {reason}",
            details=details,
            cause=cause,
            reason=reason.value,
        )


class TlsHostnameMismatchError(TlsError, SSLCertVerificationError):
    """
    TLS hostname verification failed.

    This error is both:
      - a transport-level TlsError (with reason_code=TLS_HOSTNAME_MISMATCH), and
      - an SSLCertVerificationError for compatibility with code that expects stdlib ssl types.

    Typical cause
    -------------
    The peer presented a certificate that does not match the expected hostname(s),
    e.g. missing/incorrect SAN entries.
    """

    def __init__(
        self,
        *,
        details: Mapping[str, Any],
        cause: Optional[Exception] = None,
    ) -> None:
        TlsError.__init__(
            self,
            reason=TlsErrorReason.TLS_HOSTNAME_MISMATCH,
            details=details,
            cause=cause,
        )
        # Also initialize SSLCertVerificationError base with a message.
        SSLCertVerificationError.__init__(self, self.message)


# ---- OpenConnectionError -----------------------------------------------------------------


class OpenConnectionErrorReason(StrEnum):
    """
    Stable classifiers for failures while opening the transport (multi-candidate).

    Classification guidelines
    -------------------------
    HOST_CANNOT_BE_RESOLVED
        This indicates that the target host/port could not be resolved into connectable
        addresses under the current configuration.

    CONNECTION_TO_HOST_FAILURE
        Candidate list was non-empty, but all candidates failed to establish a connection.
        Per-candidate failures are provided in `details["candidates"]` in the attempt order.
    """

    HOST_CANNOT_BE_RESOLVED = "HOST_CANNOT_BE_RESOLVED"
    CONNECTION_TO_HOST_FAILURE = "CONNECTION_TO_HOST_FAILURE"


class OpenConnectionError(NetError):
    """
    Failure to open the network connection.

    This is a higher-level error than OpenSocketError:
      - it represents the overall outcome of trying one or more candidates,
      - it carries a stable reason_code (OpenConnectionErrorReason),
      - it optionally includes ordered per-candidate failure payloads.

    details shape
    -------------
    If `socket_error_list` is provided, `details["candidates"]` is populated as a list
    preserving the attempt order:

        details["candidates"] = [
          {"candidate": "<id>", "error": <OpenSocketError.to_log_payload()>},
          ...
        ]

    Notes
    -----
    - This error does not attempt to normalize or hide unexpected exceptions coming
      from lower layers unless they are already expressed as OpenSocketError.
    - Operational logic should use `reason_code` first, and consult `details` for
      diagnostics and troubleshooting.
    """

    def __init__(
        self,
        *,
        reason: OpenConnectionErrorReason,
        socket_error_list: list[OpenSocketError] | None = None,
        cause: Optional[Exception] = None,
    ) -> None:
        details: dict[str, Any] = {}
        if socket_error_list is not None:
            details["candidates"] = [
                {"candidate": e.candidate, "error": e.to_log_payload()} for e in socket_error_list
            ]

        super().__init__(
            message=f"Unable to establish connection: {reason}",
            details=details,
            reason=reason.value,
            cause=cause,
        )


# ---- NetIoBaseError family -------------------------------------------------------------


class NetIoBaseError(NetError):
    """
    Base class for transport I/O failures.

    This error family is raised by TCP or UDP operations on the open socket when the
    engine cannot perform I/O as requested.

    Contract
    --------
    - `reason_code` is always a stable string (no enums).
    - `details` always includes:
        * io_operation_type: "TCP_READ" | "TCP_WRITE" | "TCP_DRAIN" | "UDP_SEND" | "UDP_RECEIVE"
        * engine_state: transport engine lifecycle state at the time of the failure
    - Subclasses may add additional structured diagnostics.
    """

    def __init__(
        self,
        *,
        io_op_type: str,
        engine_state: EngineState | None = None,
        reason: str,
        details: dict[str, object] | None = None,
        cause: Exception | None = None,
    ) -> None:

        _details: dict[str, object] = {
            "io_operation_type": io_op_type,
        }

        if engine_state is not None:
            _details["engine_state_at_error"] = engine_state.value

        if details is not None:
            _details.update(details)

        super().__init__(
            message=f"networking I/O error: {reason}",
            details=_details,
            cause=cause,
            reason=reason,
        )


class TcpStreamRemotelyDisconnectedError(NetIoBaseError):
    """
    Remote peer closed the connection (EOF) while performing a read operation.

    This corresponds to receiving an empty bytes object (b"") from the underlying
    stream read, meaning the peer has initiated a graceful shutdown.
    """

    def __init__(
        self,
        *,
        engine_state: EngineState | None = None,
    ) -> None:

        super().__init__(
            io_op_type=TCP_READ,
            engine_state=engine_state,
            reason="TCP_STREAM_REMOTELY_DISCONNECTED",
        )


class TcpStreamIoErrorReason(StrEnum):
    """
    Stable classifiers for generic I/O failures mapped from low-level exceptions.

    These reasons are used by TcpStreamIoError when the failure is not covered by a
    dedicated I/O error class (e.g. remote EOF).

    Classification
    --------------
    TCP_STREAM_CONNECTION_RESET
        The connection was reset by the peer (RST).
        Typical sources: ConnectionResetError, OSError(errno=ECONNRESET).

    TCP_STREAM_BROKEN_PIPE
        A write was attempted on a connection that is no longer writable.
        Typical sources: BrokenPipeError, OSError(errno=EPIPE).

    TCP_STREAM_IO_ERROR
        An OS-level I/O failure that does not match TIMEOUT/CONNECTION_RESET/BROKEN_PIPE.
        Typical source: OSError with other errno codes.

    TCP_STREAM_IO_ERROR_UNKNOWN
        A failure that could not be reliably classified (non-OSError unexpected exception).
    """

    TCP_STREAM_CONNECTION_RESET = "TCP_STREAM_CONNECTION_RESET"
    TCP_STREAM_BROKEN_PIPE = "TCP_STREAM_BROKEN_PIPE"
    TCP_STREAM_IO_ERROR = "TCP_STREAM_IO_ERROR"
    TCP_STREAM_IO_ERROR_UNKNOWN = "TCP_STREAM_IO_ERROR_UNKNOWN"


class TcpStreamIoError(NetIoBaseError):
    """
    Generic transport I/O error mapped from an underlying exception.

    Details
    -------
    - cause: original exception (for debugging)
    """

    def __init__(
        self,
        *,
        reason: TcpStreamIoErrorReason,
        io_op_type: TcpIoOperation,
        engine_state: EngineState,
        cause: Exception | None = None,
    ) -> None:

        super().__init__(
            io_op_type=io_op_type,
            engine_state=engine_state,
            reason=reason.value,
            cause=cause,
        )


class SocketTimeoutError(NetIoBaseError):
    """
    Timeout while performing a transport I/O operation.

    This error is raised when an I/O operation is executed in LIMITED timeout mode and
    asyncio.wait_for(...) expires before the operation completes.

    Attributes
    ----------
    reason_code
        Always "SOCKET_TIMEOUT".

    details
        Includes the standard I/O context from NetIoBaseError plus:
          - socket_timeout_mode: "LIMITED" | "UNLIMITED"
          - socket_timeout_s: effective timeout used for the operation (seconds)

        Call sites may add operation-specific details via with_detail(...), for example:
          - read_max_bytes for read().

    Notes
    -----
    - This error reports a timeout event; it does not imply the connection is closed.
      The transport implementation decides whether to keep the stream open after a timeout.
    """

    def __init__(
        self,
        *,
        io_op_type: TcpIoOperation | UdpIoOperation,
        engine_state: EngineState,
        socket_timeout_mode: SocketTimeoutMode,
        socket_timeout_s: float | None,
    ) -> None:
        details: dict[str, object] = {
            "socket_timeout_mode": socket_timeout_mode.value,
            "socket_timeout_s": socket_timeout_s,
        }
        super().__init__(
            reason="SOCKET_TIMEOUT",
            io_op_type=io_op_type,
            engine_state=engine_state,
            details=details,
        )


class ServerInvalidResponseError(NetError):
    def __init__(
        self,
        *,
        details: dict[str, object] | None = None,
        cause: Exception | None = None,
    ) -> None:

        super().__init__(
            message="server invalid response",
            details=details,
            cause=cause,
            reason="SERVER_INVALID_RESPONSE",
        )


class CryptoCodecError(NetError):
    def __init__(
        self,
        *,
        message: str,
        reason: str,
        cause: Exception | None = None,
    ) -> None:

        super().__init__(
            message=message,
            cause=cause,
            reason=reason,
        )


class CryptoCodecReadError(CryptoCodecError):
    def __init__(
        self,
        *,
        cause: Exception | None = None,
    ) -> None:

        super().__init__(
            message="attached crypto codec failed to read from the stream",
            cause=cause,
            reason="CRYPTO_CODEC_READING_FAILURE",
        )


class CryptoCodecWriteError(CryptoCodecError):
    def __init__(
        self,
        *,
        cause: Exception | None = None,
    ) -> None:

        super().__init__(
            message="attached crypto codec failed to write to the stream",
            cause=cause,
            reason="CRYPTO_CODEC_WRITING_FAILURE",
        )
