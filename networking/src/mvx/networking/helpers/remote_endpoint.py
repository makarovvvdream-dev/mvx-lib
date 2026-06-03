# src/mvx/networking/helpers/remote_endpoint.py
"""
Remote endpoint abstraction and address resolution helper.

This module defines :class:`RemoteEndpoint`, a small async helper that
encapsulates DNS / address resolution, IP family selection and TTL based
caching for a single logical remote TCP endpoint.

The intent is to reuse this building block across protocol specific
clients (for example LDAP servers, NMEA transponders, or other stream
based connectors) so that all of them share consistent resolution and
candidate selection rules.

Scope and responsibilities
==========================

:class:`RemoteEndpoint` works against protocol-neutral configuration
surfaces defined in :mod:`mvx.asyncio.networking.models`:

  * :class:`RemoteEndpointConnectionInfoProto`:
      Supplies the logical endpoint description:

        - ``host``: DNS name or IP literal of the peer.
        - ``port``: target TCP port.
        - other fields (timeouts, TLS, source address/ports) are not
          interpreted here but are available to higher level components.

  * :data:`IpMode`:
      Controls how IP families are filtered and ordered in the candidate
      list (for example ``V4_ONLY``, ``V6_PREFERRED``).

  * :data:`AddrInfo`:
      Canonical address records as returned by ``socket.getaddrinfo()``,
      suitable for use with ``loop.create_connection()`` or
      ``socket.socket.connect()``.

This module focuses purely on:

  * resolving ``info.host`` + ``info.port`` into a list of :data:`AddrInfo`,
  * caching the resolution for a configurable TTL,
  * applying an :data:`IpMode` policy to produce a candidate list for
    connection attempts.

It does **not** open sockets, manage transports or enforce retries. Those
concerns belong to higher level transport engines.

Public API
==========

RemoteEndpoint(info, *, addrinfo_ttl_s=300, ip_mode=IP_MODE_SYSTEM_DEFAULT)
    Construct a helper bound to a single logical endpoint.

    Parameters
    ----------
    info
        Object implementing :class:`RemoteEndpointConnectionInfoProto`.
        The ``host`` and ``port`` attributes are used during resolution.
    addrinfo_ttl_s
        Time-to-live (seconds) for cached address info:

          * ``> 0``: enable caching.
          * ``0``: disable caching (resolve on every call).

    ip_mode
        IP family selection and ordering policy as an :data:`IpMode`
        value. See *IP mode semantics* below.

info (property)
    Returns the underlying :class:`RemoteEndpointConnectionInfoProto`
    instance.

get_candidate_addresses()
    Asynchronously resolve the endpoint and return a list of candidate
    :data:`AddrInfo` records according to the current TTL and IP mode.

    The returned list is a shallow copy; callers are free to mutate it
    without affecting internal state.

Resolution and caching semantics
================================

Resolution is performed via the current event loop::

    loop = asyncio.get_running_loop()
    addrinfos = await loop.getaddrinfo(
        info.host,
        info.port,
        family=...,
        type=socket.SOCK_STREAM,
        proto=socket.IPPROTO_TCP,
    )

The resulting list is handled as follows:

  * When ``addrinfo_ttl_s > 0``:

      - On the first call, the result is stored in an internal cache and
        the current ``monotonic()`` timestamp is recorded.

      - Subsequent calls reuse the cached list while the TTL has not expired
        only if the cached list is non-empty.

      - Once the TTL expires, a new resolution is performed, the cache
        is replaced and the timestamp is updated.

      - An empty result (including failures mapped to an empty list) is s
        tored in the cache, but it is not reused by the cache guard. While
        the cache is empty, each call triggers a fresh resolution attempt,
        even within the TTL window.

  * When ``addrinfo_ttl_s == 0``:

      - Caching is disabled. Each call to
        :meth:`get_candidate_addresses` triggers a fresh
        ``getaddrinfo()`` call.

      - Internal cache state is not updated.

If ``socket.gaierror`` is raised by ``getaddrinfo()``, it is treated as
a resolution failure and converted into an empty ``AddrInfo`` list. No
exception is propagated to callers.

Concurrency model
=================

Concurrent calls to :meth:`get_candidate_addresses` are serialized with
an internal :class:`asyncio.Lock`:

  * Only one coroutine at a time performs a cache refresh.
  * Other coroutines either reuse the current cached value or wait for
    the in-flight refresh to complete.
  * All callers receive independent list instances (copies), so no
    shared mutable structures are exposed.

IP mode semantics
=================

The :data:`IpMode` value configured at construction time determines how
raw ``AddrInfo`` records are converted into the candidate list:

  * ``IP_MODE_SYSTEM_DEFAULT``:
      No filtering or reordering beyond what ``getaddrinfo()`` returns.
      The candidate list contains only the first entry from the resolved
      list. This mirrors the classic "use the primary address" behavior.

  * ``IP_MODE_V4_ONLY``:
      Keep only IPv4 addresses (AF_INET).

  * ``IP_MODE_V6_ONLY``:
      Keep only IPv6 addresses (AF_INET6).

  * ``IP_MODE_V4_PREFERRED``:
      Partition the resolved list into IPv4 and IPv6 subsets and return
      ``[all IPv4] + [all IPv6]``.

  * ``IP_MODE_V6_PREFERRED``:
      Partition the resolved list into IPv6 and IPv4 subsets and return
      ``[all IPv6] + [all IPv4]``.

If the configured `IpMode` is not recognized, an :class:`AssertionError`
is raised. This is treated as a programming error, not a runtime
condition.

Extension and reuse
===================

Typical integration patterns include:

  * Subclass :class:`RemoteEndpoint` in protocol specific client
    classes, adding higher level methods such as ``open()``,
    ``connect()`` or reconnection policies while reusing the resolution
    and IP selection logic unchanged.

  * Compose :class:`RemoteEndpoint` as a field of a richer connection or
    engine object (for example a TCP transport engine), delegating all
    host/port resolution concerns to it.

Implementations that build on top of this helper should preserve the
observable behavior described above, particularly:

  * interpretation of :class:`RemoteEndpointConnectionInfoProto`,
  * TTL based caching rules, including handling of empty results,
  * :data:`IpMode` driven filtering and ordering of candidates,
  * graceful handling of ``socket.gaierror`` by returning an empty list.
"""

from __future__ import annotations

import asyncio
import socket
from time import monotonic

from ..models import (
    RemoteEndpointConnectionInfoProto,
    AddrInfo,
    IpMode,
    IP_MODE_SYSTEM_DEFAULT,
    IP_MODE_V4_ONLY,
    IP_MODE_V6_ONLY,
    IP_MODE_V4_PREFERRED,
    IP_MODE_V6_PREFERRED,
)


class RemoteEndpoint:
    def __init__(
        self,
        info: RemoteEndpointConnectionInfoProto,
        *,
        addrinfo_ttl_s: int = 300,
        ip_mode: IpMode = IP_MODE_SYSTEM_DEFAULT,
    ) -> None:

        self._info: RemoteEndpointConnectionInfoProto = info

        self._addrinfo_ttl_s = addrinfo_ttl_s
        self._ip_mode: IpMode = ip_mode

        self._addrinfos_cache: list[AddrInfo] = []
        self._resolved_at: float = 0.0

        self._lock = asyncio.Lock()

    @property
    def info(self) -> RemoteEndpointConnectionInfoProto:
        return self._info

    async def get_candidate_addresses(self) -> list[AddrInfo]:
        ttl = self._addrinfo_ttl_s

        # TTL=0 disables caching: resolve every time and do not touch cache state.
        if ttl == 0:
            addrinfos = await self._resolve_address_info()
            return self._select_candidates(addrinfos)

        now = monotonic()

        async with self._lock:
            if self._addrinfos_cache and (now - self._resolved_at) <= ttl:
                addrinfos = list(self._addrinfos_cache)
            else:
                addrinfos = await self._refresh_address_info_locked(now)

        return self._select_candidates(addrinfos)

    async def _resolve_address_info(self) -> list[AddrInfo]:
        loop = asyncio.get_running_loop()

        family: socket.AddressFamily = socket.AF_UNSPEC
        if self._ip_mode == IP_MODE_V4_ONLY:
            family = socket.AF_INET
        elif self._ip_mode == IP_MODE_V6_ONLY:
            family = socket.AF_INET6

        try:
            addrinfos: list[AddrInfo] = await loop.getaddrinfo(
                self._info.host,
                self._info.port,
                family=family,
                type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
            )
        except socket.gaierror:
            addrinfos = []

        return list(addrinfos)

    async def _refresh_address_info_locked(self, now: float) -> list[AddrInfo]:
        addrinfos = await self._resolve_address_info()

        self._addrinfos_cache = list(addrinfos)
        self._resolved_at = now

        return list(self._addrinfos_cache)

    def _select_candidates(self, addrinfos: list[AddrInfo]) -> list[AddrInfo]:
        if not addrinfos:
            return []

        mode = self._ip_mode

        if mode == IP_MODE_SYSTEM_DEFAULT:
            return [addrinfos[0]]

        if mode == IP_MODE_V4_ONLY:
            return [ai for ai in addrinfos if ai[0] == socket.AF_INET]

        if mode == IP_MODE_V6_ONLY:
            return [ai for ai in addrinfos if ai[0] == socket.AF_INET6]

        if mode == IP_MODE_V4_PREFERRED:
            v4 = [ai for ai in addrinfos if ai[0] == socket.AF_INET]
            v6 = [ai for ai in addrinfos if ai[0] == socket.AF_INET6]
            return v4 + v6

        if mode == IP_MODE_V6_PREFERRED:
            v6 = [ai for ai in addrinfos if ai[0] == socket.AF_INET6]
            v4 = [ai for ai in addrinfos if ai[0] == socket.AF_INET]
            return v6 + v4

        raise AssertionError(f"Unexpected ip_mode: {mode!r}")
