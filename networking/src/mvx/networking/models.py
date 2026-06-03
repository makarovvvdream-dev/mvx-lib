# src/mvx/networking/models.py
"""
Core networking models and protocol surfaces.

This module defines lightweight enums, literals and protocols that describe
the minimal networking surface used by async transports and remote endpoint
helpers. The goal is to have a shared vocabulary for:

  * transport engine lifecycle,
  * TCP and UDP I/O operation kinds,
  * socket address and addrinfo shapes,
  * IP and TLS modes,
  * configuration protocols for TLS and remote endpoints.

Higher level components such as TCP stream transports, UDP sockets,
LDAP servers, NMEA transponders, and other protocol specific clients
are expected to implement or reference these models instead of hardcoding
ad hoc shapes.

EngineState
===========

:class:`EngineState` is a generic lifecycle state machine for I/O engines
that own an underlying socket or stream transport.

States:

  * ``VIRGIN``:
      Engine instance has been created but not yet opened.
      No socket or underlying transport exists.

  * ``OPENING``:
      Transitional state while the engine is establishing connectivity and
      preparing its internals.

  * ``OPENED``:
      Engine is fully operational. Background I/O loops may be running and
      the underlying transport is ready for reads and writes.

  * ``CLOSING``:
      Graceful shutdown in progress. No new user operations should be
      accepted, but cleanup and draining may still be ongoing.

  * ``CLOSED``:
      Engine has completed shutdown and released its resources.

  * ``ERROR``:
      Terminal error state indicating that the engine failed to open or
      experienced an unrecoverable I/O error.

The exact transition graph is defined by concrete engine implementations,
but the semantic meaning of each state should be preserved.

I/O operation kinds
===================

Literal aliases define the kinds of low level I/O operations that may be
logged, metered or tracked by higher level components:

TCP:

  * ``TCP_READ``   : read from a TCP stream.
  * ``TCP_WRITE``  : write to a TCP stream.
  * ``TCP_DRAIN``  : drain pending TCP writes (e.g., `StreamWriter.drain()`).

UDP:

  * ``UDP_SEND``     : send a UDP datagram.
  * ``UDP_RECEIVE``  : receive a UDP datagram.

These literals are intended for structured logging, metrics and generic
retry or backoff policies.

SocketTimeoutMode
=================

:class:`SocketTimeoutMode` describes how low level I/O timeouts are applied:

  * ``UNLIMITED``:
      No socket level timeout is enforced. Blocking I/O calls may wait
      indefinitely unless guarded by higher level cancellation primitives.

  * ``LIMITED``:
      A finite timeout is applied to I/O operations, usually derived from
      configuration such as ``socket_timeout_ms`` on a connection info
      object.

The exact mapping from this enum to concrete socket or stream behavior
is the responsibility of the transport implementation.

Socket address and addrinfo shapes
==================================

:data:`SockAddr` is a type alias for socket address tuples as produced
and consumed by stdlib socket APIs:

  * IPv4: ``(host: str, port: int)``
  * IPv6: ``(host: str, port: int, flowinfo: int, scopeid: int)``

:data:`AddrInfo` is the canonical subset of records returned from
``socket.getaddrinfo()`` that transports and helpers rely on:

  * index 0: ``family``    - :class:`socket.AddressFamily`
  * index 1: ``socktype``  - :class:`socket.SocketKind`
  * index 2: ``proto``     - int (for example ``socket.IPPROTO_TCP``)
  * index 3: ``canonname`` - str (often empty)
  * index 4: ``sockaddr``  - :data:`SockAddr`

These shapes are designed to be directly usable with
``loop.create_connection()`` or ``socket.socket.connect()``.

IpMode
======

:data:`IpMode` describes how IP families are handled when selecting or
ordering candidate addresses:

  * ``SYSTEM_DEFAULT``:
      Do not filter or reorder. Use the address list as returned by
      ``getaddrinfo()`` (the first address is usually the primary one).

  * ``V4_ONLY``:
      Restrict to IPv4 addresses (AF_INET).

  * ``V6_ONLY``:
      Restrict to IPv6 addresses (AF_INET6).

  * ``V4_PREFERRED``:
      Prefer IPv4. All IPv4 addresses first, followed by all IPv6
      addresses.

  * ``V6_PREFERRED``:
      Prefer IPv6. All IPv6 addresses first, followed by all IPv4
      addresses.

The concrete selection and ordering logic is implemented by helpers such
as ``RemoteEndpoint``, while this module only defines the mode vocabulary.

TlsMode
=======

:data:`TlsMode` captures the transport level TLS strategy:

  * ``OFF``:
      Plain TCP; no TLS is negotiated or applied.

  * ``TLS``:
      Implicit TLS. The TCP connection is wrapped into a TLS session
      immediately after connect.

  * ``STARTTLS``:
      Explicit TLS upgrade. The connection starts as plain TCP and is
      upgraded to TLS after a protocol level STARTTLS handshake.

The interpretation of these modes is the responsibility of the concrete
protocol client or transport.

TlsInfoProto
============

:class:`TlsInfoProto` is a runtime checkable protocol that describes
the TLS configuration surface required by transports.

Expected properties:

  * ``tls_mode``:
      One of ``"OFF"``, ``"TLS"``, ``"STARTTLS"`` as defined by
      :data:`TlsMode`.

  * ``ca_certs_file``:
      Path to a CA bundle file used to verify peer certificates.

  * ``ca_certs_path``:
      Path to a directory containing CA certificates in a platform
      specific layout.

  * ``ca_certs_data``:
      In memory CA bundle (PEM) as ``str`` or ``bytes``. May be used
      instead of files or directories.

  * ``client_cert_file``:
      Path to a client certificate for mutual TLS, if required.

  * ``client_key_file``:
      Path to the private key corresponding to ``client_cert_file``.

  * ``client_key_password``:
      Password for encrypted private keys, if applicable.

  * ``sni``:
      Server Name Indication value to send during TLS handshake
      (commonly mapped to ``server_hostname``).

  * ``valid_names``:
      Optional list of additional allowed peer names for certificate
      validation (for example alternative hostnames in SAN/CN). This can
      complement or override the runtime host name used for the TCP
      connection.

Implementations may provide extra fields, but transports only rely on
the attributes documented above.

RemoteEndpointConnectionInfoProto
=================================

:class:`RemoteEndpointConnectionInfoProto` is a runtime checkable
protocol that captures the logical connection parameters needed to open
a network stream.

Expected properties:

  * ``host``:
      Target host token (DNS name or IP literal). Name resolution is
      delegated to an address info provider such as ``RemoteEndpoint``.

  * ``port``:
      Target TCP port.

  * ``connect_timeout_ms``:
      Per candidate connection timeout in milliseconds. This defines the
      deadline for establishing a single TCP connection attempt.

  * ``socket_timeout_ms``:
      I/O timeout in milliseconds applied after the connection is
      established. Semantics are defined by the transport implementation
      and usually depend on :class:`SocketTimeoutMode`.

  * ``source_address``:
      Optional local IP address to bind before connecting. When set, the
      transport should bind the socket to this address.

  * ``source_port_list``:
      Optional list of local source ports to try when binding before
      connect. A transport may iterate this list and bind to the first
      available port.

  * ``tls``:
      TLS configuration as a :class:`TlsInfoProto` instance, used when
      ``tls_mode`` is ``"TLS"`` or ``"STARTTLS"``.

Concrete configuration classes (for example LDAP specific server config,
NMEA transponder config, or generic TCP client settings) are expected to
implement this protocol so that transports and endpoint helpers can work
against a stable, documented surface.
"""

from __future__ import annotations
from typing import Literal, Protocol, Optional, runtime_checkable, TypeAlias
from enum import Enum
import socket


class EngineState(str, Enum):
    VIRGIN = "VIRGIN"
    OPENING = "OPENING"
    OPENED = "OPENED"
    RECONFIGURING = "RECONFIGURING"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    ERROR = "ERROR"


# Network IO Operation types
TcpIoOperation = Literal["TCP_READ", "TCP_WRITE", "TCP_DRAIN"]
TCP_READ: TcpIoOperation = "TCP_READ"
TCP_WRITE: TcpIoOperation = "TCP_WRITE"
TCP_DRAIN: TcpIoOperation = "TCP_DRAIN"

UdpIoOperation = Literal["UDP_SEND", "UDP_RECEIVE"]
UDP_SEND: UdpIoOperation = "UDP_SEND"
UDP_RECEIVE: UdpIoOperation = "UDP_RECEIVE"


class SocketTimeoutMode(str, Enum):
    UNLIMITED = "UNLIMITED"
    LIMITED = "LIMITED"


#: Socket address tuple as produced/consumed by stdlib socket APIs.
#:
#: - IPv4: ("1.2.3.4", 389)
#: - IPv6: ("2001:db8::1", 389, flowinfo, scopeid)
SockAddr: TypeAlias = tuple[str, int] | tuple[str, int, int, int]

#: Canonical AddrInfo record returned by socket.getaddrinfo() (subset we rely on).
#:
#: Fields:
#:   0) family   : socket.AddressFamily (AF_INET / AF_INET6)
#:   1) socktype : socket.SocketKind (usually SOCK_STREAM)
#:   2) proto    : int (usually IPPROTO_TCP)
#:   3) canonname: str (often empty; included for compatibility)
#:   4) sockaddr : SockAddr (tuple passed to connect())
AddrInfo: TypeAlias = tuple[socket.AddressFamily, socket.SocketKind, int, str, SockAddr]

IpMode = Literal["SYSTEM_DEFAULT", "V4_ONLY", "V6_ONLY", "V4_PREFERRED", "V6_PREFERRED"]
IP_MODE_SYSTEM_DEFAULT: IpMode = "SYSTEM_DEFAULT"
IP_MODE_V4_ONLY: IpMode = "V4_ONLY"
IP_MODE_V6_ONLY: IpMode = "V6_ONLY"
IP_MODE_V4_PREFERRED: IpMode = "V4_PREFERRED"
IP_MODE_V6_PREFERRED: IpMode = "V6_PREFERRED"


# noinspection PyUnresolvedReferences
@runtime_checkable
class TlsInfoProto(Protocol):
    """
    TLS configuration surface required by the transport.

    Attributes
    ----------
    ca_certs_file
        Path to a CA bundle file used to verify the peer certificate.
    ca_certs_path
        Path to a directory containing CA certs (platform-dependent layout).
    ca_certs_data
        In-memory CA bundle content (PEM) as str/bytes.
    client_cert_file
        Path to a client certificate (mTLS), if required.
    client_key_file
        Path to a private key corresponding to client_cert_file.
    client_key_password
        Password for encrypted private keys (if applicable).
    sni
        Server Name Indication for TLS handshake (server_hostname).
        When set, it is sent during handshake and may influence certificate selection.
    valid_names
        Optional list of allowed peer names for certificate validation.
        Used to validate the peer certificate (e.g., SAN/CN match) in addition to
        the runtime `host` value.
    """

    @property
    def ca_certs_file(self) -> Optional[str]: ...

    @property
    def ca_certs_path(self) -> Optional[str]: ...

    @property
    def ca_certs_data(self) -> Optional[str | bytes]: ...

    @property
    def client_cert_file(self) -> Optional[str]: ...

    @property
    def client_key_file(self) -> Optional[str]: ...

    @property
    def client_key_password(self) -> Optional[str]: ...

    @property
    def sni(self) -> Optional[str]: ...

    @property
    def valid_names(self) -> Optional[list[str]]: ...


# noinspection PyUnresolvedReferences
@runtime_checkable
class RemoteEndpointConnectionInfoProto(Protocol):
    """
    Connection parameters required by the transport to establish a network stream.

    Attributes
    ----------
    host
        Target host token (IP or DNS name). Resolution is delegated to the address
        info provider.
    port
        Target port.
    connect_timeout_ms
        Deadline (milliseconds) for establishing a connection to a single candidate.
    socket_timeout_ms
        I/O timeout (milliseconds) applied after the connection is established
        (e.g., recv/read timeout). Semantics depend on the transport implementation.
    source_address
        Optional local IP address to bind before connecting.
    source_port_list
        Optional list of local source ports to try when binding before connect.
        The transport may iterate this list and bind the first available port.
    tls
        TLS configuration needed for TLS/STARTTLS modes.
    """

    @property
    def host(self) -> str: ...
    @property
    def port(self) -> int: ...
    @property
    def connect_timeout_ms(self) -> int: ...
    @property
    def socket_timeout_ms(self) -> int: ...
    @property
    def source_address(self) -> Optional[str]: ...
    @property
    def source_port_list(self) -> Optional[list[int]]: ...
    @property
    def tls(self) -> TlsInfoProto: ...


class ConnectOutcome(str, Enum):
    CONNECTED = "CONNECTED"
    ALREADY_CONNECTED = "ALREADY_CONNECTED"


class DisconnectOutcome(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    NOT_CONNECTED = "NOT_CONNECTED"
