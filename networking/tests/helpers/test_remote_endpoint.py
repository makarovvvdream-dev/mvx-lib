# tests/helpers/test_remote_endpoint.py
"""
Tests for mvx.asyncio.networking.helpers.remote_endpoint.RemoteEndpoint.

Grouping rule:
  - Group a: Basic API contract and copy semantics
  - Group b: TTL=0 (no caching)
  - Group c: TTL>0 (caching and refresh)
  - Group d: IP_MODE_SYSTEM_DEFAULT semantics
  - Group e: V4_ONLY / V6_ONLY filtering + resolver family
  - Group f: V4_PREFERRED / V6_PREFERRED ordering
  - Group g: Concurrency (lock serialization)
  - Group h: Defensive behavior (unknown ip_mode)

Naming rule:
  Each test name starts with test_<group><num>_, e.g. test_a1_...
"""

from __future__ import annotations

import asyncio
import socket
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, cast

import pytest

from mvx.networking.helpers.remote_endpoint import (
    RemoteEndpoint,
    IP_MODE_SYSTEM_DEFAULT,
    IP_MODE_V4_ONLY,
    IP_MODE_V6_ONLY,
    IP_MODE_V4_PREFERRED,
    IP_MODE_V6_PREFERRED,
)
from mvx.networking.models import RemoteEndpointConnectionInfoProto


@dataclass(frozen=True)
class _Info:
    host: str
    port: int


def _ai(family: socket.AddressFamily, ip: str, port: int) -> tuple[Any, ...]:
    """Build a minimal getaddrinfo-like AddrInfo tuple."""
    if family == socket.AF_INET:
        sockaddr: Any = (ip, port)
    elif family == socket.AF_INET6:
        sockaddr = (ip, port, 0, 0)
    else:
        sockaddr = (ip, port)

    return (
        family,
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP,
        "",
        sockaddr,
    )


class _FakeLoop:
    def __init__(self, getaddrinfo_impl: Callable[..., Any]) -> None:
        self._impl = getaddrinfo_impl
        self.calls: list[dict[str, Any]] = []

    async def getaddrinfo(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append({"args": args, "kwargs": kwargs})
        return await self._impl(*args, **kwargs)


def _monotonic_seq(values: Iterable[float]) -> Callable[[], float]:
    it: Iterator[float] = iter(values)

    def _next() -> float:
        return next(it)

    return _next


@pytest.fixture()
def info() -> RemoteEndpointConnectionInfoProto:
    return cast(
        RemoteEndpointConnectionInfoProto, cast(object, _Info(host="example.com", port=389))
    )


@pytest.fixture()
def mixed_addrinfos() -> list[tuple[Any, ...]]:
    return [
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET, "192.0.2.10", 389),
        _ai(socket.AF_INET6, "2001:db8::2", 389),
        _ai(socket.AF_INET, "192.0.2.11", 389),
    ]


@pytest.fixture()
def v4_only_addrinfos() -> list[tuple[Any, ...]]:
    return [
        _ai(socket.AF_INET, "192.0.2.10", 389),
        _ai(socket.AF_INET, "192.0.2.11", 389),
    ]


@pytest.fixture()
def v6_only_addrinfos() -> list[tuple[Any, ...]]:
    return [
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET6, "2001:db8::2", 389),
    ]


@pytest.fixture()
def module_under_test():
    import mvx.networking.helpers.remote_endpoint as m

    return m


# -------------------------
# Group a: Basic API contract and copy semantics
# -------------------------


@pytest.mark.asyncio
async def test_a1_info_property_returns_same_object(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """info property returns the original info object."""
    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_SYSTEM_DEFAULT)
    assert ep.info is info


@pytest.mark.asyncio
async def test_a2_empty_resolution_returns_empty_candidates(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """Empty resolution yields empty candidates."""
    fake_loop = _FakeLoop(getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=[]))
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_PREFERRED)
    out = await ep.get_candidate_addresses()
    assert out == []


@pytest.mark.asyncio
async def test_a3_candidates_are_shallow_copy_not_cache_alias(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """Returned list is independent and can be mutated without affecting cache."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_PREFERRED)

    out1 = await ep.get_candidate_addresses()
    out1.append(_ai(socket.AF_INET, "192.0.2.99", 389))
    out1.pop(0)

    out2 = await ep.get_candidate_addresses()
    assert out2 == [
        _ai(socket.AF_INET, "192.0.2.10", 389),
        _ai(socket.AF_INET, "192.0.2.11", 389),
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET6, "2001:db8::2", 389),
    ]


# -------------------------
# Group b: TTL=0 (no caching)
# -------------------------


@pytest.mark.asyncio
async def test_b1_ttl0_resolves_every_call_and_does_not_update_cache(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """TTL=0 triggers getaddrinfo each time and keeps cache fields untouched."""

    async def impl(*args: Any, **kwargs: Any) -> Any:
        _ = args
        _ = kwargs
        return list(mixed_addrinfos)

    fake_loop = _FakeLoop(getaddrinfo_impl=impl)
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]

    ep = RemoteEndpoint(info, addrinfo_ttl_s=0, ip_mode=IP_MODE_V4_PREFERRED)

    out1 = await ep.get_candidate_addresses()
    out2 = await ep.get_candidate_addresses()

    assert len(fake_loop.calls) == 2
    assert out1 == out2
    assert ep._addrinfos_cache == []
    assert ep._resolved_at == 0.0


@pytest.mark.asyncio
async def test_b2_ttl0_different_results_reflected_immediately(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """TTL=0 returns fresh resolution results on each call."""
    results = [
        [_ai(socket.AF_INET, "192.0.2.10", 389)],
        [_ai(socket.AF_INET, "192.0.2.11", 389)],
    ]

    async def impl(*args: Any, **kwargs: Any) -> Any:
        _ = args
        _ = kwargs
        return results.pop(0)

    fake_loop = _FakeLoop(getaddrinfo_impl=impl)
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]

    ep = RemoteEndpoint(info, addrinfo_ttl_s=0, ip_mode=IP_MODE_V4_ONLY)

    out1 = await ep.get_candidate_addresses()
    out2 = await ep.get_candidate_addresses()

    assert out1 == [_ai(socket.AF_INET, "192.0.2.10", 389)]
    assert out2 == [_ai(socket.AF_INET, "192.0.2.11", 389)]
    assert len(fake_loop.calls) == 2


@pytest.mark.asyncio
async def test_b3_ttl0_gaierror_is_swallowed_each_time(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """TTL=0 still swallows socket.gaierror and returns empty list."""

    async def impl(*args: Any, **kwargs: Any) -> Any:
        _ = args
        _ = kwargs
        raise socket.gaierror("boom")

    fake_loop = _FakeLoop(getaddrinfo_impl=impl)
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=0, ip_mode=IP_MODE_SYSTEM_DEFAULT)

    out1 = await ep.get_candidate_addresses()
    out2 = await ep.get_candidate_addresses()

    assert out1 == []
    assert out2 == []
    assert len(fake_loop.calls) == 2


# -------------------------
# Group c: TTL>0 (caching and refresh)
# -------------------------


@pytest.mark.asyncio
async def test_c1_first_call_populates_cache_and_reuses_within_ttl(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """Within TTL, cached resolution is reused (no extra getaddrinfo calls)."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = _monotonic_seq([0.0, 10.0])  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_PREFERRED)

    _ = await ep.get_candidate_addresses()
    _ = await ep.get_candidate_addresses()

    assert len(fake_loop.calls) == 1


@pytest.mark.asyncio
async def test_c2_ttl_expiry_triggers_refresh(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """After TTL expiry, a refresh occurs and new results are returned."""
    results = [
        [_ai(socket.AF_INET, "192.0.2.10", 389)],
        [_ai(socket.AF_INET, "192.0.2.11", 389)],
    ]

    async def impl(*args: Any, **kwargs: Any) -> Any:
        _ = args
        _ = kwargs
        return list(results.pop(0))

    fake_loop = _FakeLoop(getaddrinfo_impl=impl)
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = _monotonic_seq([0.0, 301.0])  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_ONLY)

    out1 = await ep.get_candidate_addresses()
    out2 = await ep.get_candidate_addresses()

    assert out1 == [_ai(socket.AF_INET, "192.0.2.10", 389)]
    assert out2 == [_ai(socket.AF_INET, "192.0.2.11", 389)]
    assert len(fake_loop.calls) == 2


@pytest.mark.asyncio
async def test_c3_empty_result_is_not_reused_by_cache_guard(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """Empty cached list is not reused due to the non-empty cache guard."""

    # This test asserts current implementation behavior:
    # cache reuse requires self._addrinfos_cache to be truthy.
    async def impl(*args: Any, **kwargs: Any) -> Any:
        _ = args
        _ = kwargs
        return []

    fake_loop = _FakeLoop(getaddrinfo_impl=impl)
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = _monotonic_seq([0.0, 10.0])  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_SYSTEM_DEFAULT)

    out1 = await ep.get_candidate_addresses()
    out2 = await ep.get_candidate_addresses()

    assert out1 == []
    assert out2 == []
    assert len(fake_loop.calls) == 2


@pytest.mark.asyncio
async def test_c4_gaierror_is_cached_as_empty_but_not_reused_by_guard(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """gaierror becomes empty, but still triggers re-resolution due to the guard."""

    async def impl(*args: Any, **kwargs: Any) -> Any:
        _ = args
        _ = kwargs
        raise socket.gaierror("nope")

    fake_loop = _FakeLoop(getaddrinfo_impl=impl)
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = _monotonic_seq([0.0, 10.0])  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_ONLY)

    out1 = await ep.get_candidate_addresses()
    out2 = await ep.get_candidate_addresses()

    assert out1 == []
    assert out2 == []
    assert len(fake_loop.calls) == 2


# -------------------------
# Group d: IP_MODE_SYSTEM_DEFAULT semantics
# -------------------------


@pytest.mark.asyncio
async def test_d1_system_default_returns_only_first_addrinfo(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """System default returns only the first resolved entry."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_SYSTEM_DEFAULT)
    out = await ep.get_candidate_addresses()

    assert out == [mixed_addrinfos[0]]


@pytest.mark.asyncio
async def test_d2_system_default_does_not_filter_by_family(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """System default returns first element even if it is IPv6 or IPv4."""
    addrinfos = [
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET, "192.0.2.10", 389),
    ]

    fake_loop = _FakeLoop(getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(addrinfos)))
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_SYSTEM_DEFAULT)
    out = await ep.get_candidate_addresses()

    assert out == [addrinfos[0]]


# -------------------------
# Group e: V4_ONLY / V6_ONLY filtering + resolver family
# -------------------------


@pytest.mark.asyncio
async def test_e1_v4_only_filters_candidates_to_af_inet(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """V4_ONLY filters candidates to AF_INET only."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_ONLY)
    out = await ep.get_candidate_addresses()

    assert out == [
        _ai(socket.AF_INET, "192.0.2.10", 389),
        _ai(socket.AF_INET, "192.0.2.11", 389),
    ]


@pytest.mark.asyncio
async def test_e2_v6_only_filters_candidates_to_af_inet6(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """V6_ONLY filters candidates to AF_INET6 only."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V6_ONLY)
    out = await ep.get_candidate_addresses()

    assert out == [
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET6, "2001:db8::2", 389),
    ]


@pytest.mark.asyncio
async def test_e3_v4_only_passes_family_af_inet_to_getaddrinfo(
    info: RemoteEndpointConnectionInfoProto, v4_only_addrinfos, module_under_test
):
    """V4_ONLY passes family=AF_INET to getaddrinfo."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(v4_only_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_ONLY)
    _ = await ep.get_candidate_addresses()

    assert len(fake_loop.calls) == 1
    kwargs = fake_loop.calls[0]["kwargs"]
    assert kwargs["family"] == socket.AF_INET
    assert kwargs["type"] == socket.SOCK_STREAM
    assert kwargs["proto"] == socket.IPPROTO_TCP


@pytest.mark.asyncio
async def test_e4_v6_only_passes_family_af_inet6_to_getaddrinfo(
    info: RemoteEndpointConnectionInfoProto, v6_only_addrinfos, module_under_test
):
    """V6_ONLY passes family=AF_INET6 to getaddrinfo."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(v6_only_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V6_ONLY)
    _ = await ep.get_candidate_addresses()

    assert len(fake_loop.calls) == 1
    kwargs = fake_loop.calls[0]["kwargs"]
    assert kwargs["family"] == socket.AF_INET6
    assert kwargs["type"] == socket.SOCK_STREAM
    assert kwargs["proto"] == socket.IPPROTO_TCP


@pytest.mark.asyncio
async def test_e5_only_modes_return_empty_if_no_matching_family(
    info: RemoteEndpointConnectionInfoProto, v6_only_addrinfos, module_under_test
):
    """ONLY modes return empty list when no entries match the requested family."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(v6_only_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_ONLY)
    out = await ep.get_candidate_addresses()
    assert out == []


# -------------------------
# Group f: V4_PREFERRED / V6_PREFERRED ordering
# -------------------------


@pytest.mark.asyncio
async def test_f1_v4_preferred_orders_v4_then_v6_preserving_relative_order(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """V4_PREFERRED returns [all v4] + [all v6], preserving order within each."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_PREFERRED)
    out = await ep.get_candidate_addresses()

    assert out == [
        _ai(socket.AF_INET, "192.0.2.10", 389),
        _ai(socket.AF_INET, "192.0.2.11", 389),
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET6, "2001:db8::2", 389),
    ]


@pytest.mark.asyncio
async def test_f2_v6_preferred_orders_v6_then_v4_preserving_relative_order(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """V6_PREFERRED returns [all v6] + [all v4], preserving order within each."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V6_PREFERRED)
    out = await ep.get_candidate_addresses()

    assert out == [
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET6, "2001:db8::2", 389),
        _ai(socket.AF_INET, "192.0.2.10", 389),
        _ai(socket.AF_INET, "192.0.2.11", 389),
    ]


@pytest.mark.asyncio
async def test_f3_preferred_modes_ignore_non_inet_families(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """Preferred modes ignore entries not in AF_INET/AF_INET6."""
    addrinfos = [
        (12345, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("x", 1)),  # unknown family
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET, "192.0.2.10", 389),
    ]

    fake_loop = _FakeLoop(getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(addrinfos)))
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V6_PREFERRED)
    out = await ep.get_candidate_addresses()

    assert out == [
        _ai(socket.AF_INET6, "2001:db8::1", 389),
        _ai(socket.AF_INET, "192.0.2.10", 389),
    ]


# -------------------------
# Group g: Concurrency (lock serialization)
# -------------------------


@pytest.mark.asyncio
async def test_g1_concurrent_calls_share_single_refresh(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """Concurrent calls with empty cache perform a single refresh."""
    gate = asyncio.Event()

    async def impl(*args: Any, **kwargs: Any) -> Any:
        _ = args
        _ = kwargs
        await gate.wait()
        return list(mixed_addrinfos)

    fake_loop = _FakeLoop(getaddrinfo_impl=impl)
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_PREFERRED)

    t1 = asyncio.create_task(ep.get_candidate_addresses())
    t2 = asyncio.create_task(ep.get_candidate_addresses())
    t3 = asyncio.create_task(ep.get_candidate_addresses())

    await asyncio.sleep(0)  # let tasks reach the await in getaddrinfo
    gate.set()

    r1, r2, r3 = await asyncio.gather(t1, t2, t3)

    assert len(fake_loop.calls) == 1
    assert r1 == r2 == r3


@pytest.mark.asyncio
async def test_g2_concurrent_calls_after_cache_populated_do_not_call_getaddrinfo(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """Concurrent calls within TTL reuse cache and do not call getaddrinfo."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = _monotonic_seq([0.0, 1.0, 1.0, 1.0])  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_PREFERRED)

    _ = await ep.get_candidate_addresses()  # warm cache
    assert len(fake_loop.calls) == 1

    t1 = asyncio.create_task(ep.get_candidate_addresses())
    t2 = asyncio.create_task(ep.get_candidate_addresses())
    t3 = asyncio.create_task(ep.get_candidate_addresses())
    _ = await asyncio.gather(t1, t2, t3)

    assert len(fake_loop.calls) == 1


@pytest.mark.asyncio
async def test_g3_concurrent_calls_during_expired_ttl_refresh_wait_for_single_refresh(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """After TTL expiry, concurrent callers synchronize on a single refresh."""
    gate = asyncio.Event()
    results = [
        [_ai(socket.AF_INET, "192.0.2.10", 389)],
        [_ai(socket.AF_INET, "192.0.2.11", 389)],
    ]

    async def impl(*args: Any, **kwargs: Any) -> Any:
        _ = args
        _ = kwargs
        if results and results[0] == [_ai(socket.AF_INET, "192.0.2.11", 389)]:
            await gate.wait()
        return list(results.pop(0))

    fake_loop = _FakeLoop(getaddrinfo_impl=impl)
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = _monotonic_seq([0.0, 301.0, 301.0, 301.0])  # type: ignore[assignment]

    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V4_ONLY)

    out1 = await ep.get_candidate_addresses()
    assert out1 == [_ai(socket.AF_INET, "192.0.2.10", 389)]
    assert len(fake_loop.calls) == 1

    t1 = asyncio.create_task(ep.get_candidate_addresses())
    t2 = asyncio.create_task(ep.get_candidate_addresses())
    t3 = asyncio.create_task(ep.get_candidate_addresses())

    await asyncio.sleep(0)
    gate.set()

    r1, r2, r3 = await asyncio.gather(t1, t2, t3)
    assert len(fake_loop.calls) == 2
    assert r1 == r2 == r3 == [_ai(socket.AF_INET, "192.0.2.11", 389)]


# -------------------------
# Group h: Defensive behavior (unknown ip_mode)
# -------------------------


@pytest.mark.asyncio
async def test_h1_unknown_ip_mode_raises_assertionerror_in_select_candidates(
    info: RemoteEndpointConnectionInfoProto, mixed_addrinfos, module_under_test
):
    """Unknown ip_mode triggers AssertionError."""
    fake_loop = _FakeLoop(
        getaddrinfo_impl=lambda *a, **k: asyncio.sleep(0, result=list(mixed_addrinfos))
    )
    module_under_test.asyncio.get_running_loop = lambda: fake_loop  # type: ignore[assignment]
    module_under_test.monotonic = lambda: 0.0  # type: ignore[assignment]

    # Bypass typing: inject an invalid mode.
    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_SYSTEM_DEFAULT)
    ep._ip_mode = object()  # type: ignore[assignment]

    with pytest.raises(AssertionError, match="Unexpected ip_mode"):
        await ep.get_candidate_addresses()


@pytest.mark.asyncio
async def test_h2_select_candidates_returns_empty_on_empty_input(
    info: RemoteEndpointConnectionInfoProto, module_under_test
):
    """Selecting candidates from empty list yields empty list."""
    # noinspection PyArgumentEqualDefault
    ep = RemoteEndpoint(info, addrinfo_ttl_s=300, ip_mode=IP_MODE_V6_PREFERRED)
    assert ep._select_candidates([]) == []
