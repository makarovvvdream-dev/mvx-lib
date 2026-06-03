# tests/helpers/test_tls.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import asyncio
import ssl

import pytest

import mvx.networking.helpers.tls as tls_mod
from mvx.networking.net_errors import (
    TlsError,
    TlsErrorReason,
    TlsHostnameMismatchError,
)


@dataclass(slots=True)
class DummyTls:
    """Minimal TLS info object used in tests."""

    tls_mode: str = "TLS"
    ca_certs_file: Optional[str] = None
    ca_certs_path: Optional[str] = None
    ca_certs_data: Optional[str | bytes] = None
    client_cert_file: Optional[str] = None
    client_key_file: Optional[str] = None
    client_key_password: Optional[str] = None
    sni: Optional[str] = None
    valid_names: Optional[list[str]] = None


@dataclass(slots=True)
class DummyInfo:
    """Minimal server info object used in tests."""

    host: str
    port: int = 636
    connect_timeout_ms: int = 1000
    socket_timeout_ms: int = 1000
    source_address: Optional[str] = None
    source_port_list: Optional[list[int]] = None
    tls: DummyTls = field(default_factory=DummyTls)


class DummySslObject:
    """Dummy SSL object with getpeercert()."""

    def __init__(self, cert: Any) -> None:
        self._cert = cert

    def getpeercert(self) -> Any:
        return self._cert


class DummyTransport:
    def is_closing(self: "DummyTransport") -> bool:
        _ = self
        return True


class DummyWriter(asyncio.StreamWriter):
    """Dummy StreamWriter that supports start_tls() and get_extra_info()."""

    # noinspection PyMissingConstructor
    def __init__(self: "DummyWriter") -> None:
        self._transport = DummyTransport()
        self.start_tls_calls: list[dict[str, Any]] = []
        self.start_tls_exc: BaseException | None = None
        self._ssl_object: Any = None

    async def start_tls(self: "DummyWriter", **kwargs: Any) -> None:
        self.start_tls_calls.append(dict(kwargs))
        if self.start_tls_exc is not None:
            raise self.start_tls_exc

    def get_extra_info(
        self: "DummyWriter",
        name: str,
        default: Any = None,
    ) -> Any:
        if name == "ssl_object":
            return self._ssl_object
        return default

    def set_ssl_object(self: "DummyWriter", obj: Any) -> None:
        self._ssl_object = obj


# -------- Group A: _pick_server_hostname() --------


def test_a1_pick_hostname_prefers_explicit_sni():
    """Use explicit SNI when provided, regardless of host value."""
    info = DummyInfo(host="1.2.3.4", tls=DummyTls(sni="ldap.example.com"))
    assert tls_mod._pick_server_hostname(info) == "ldap.example.com"


def test_a2_pick_hostname_strips_host_before_ip_check():
    """Strip host before IP parsing and return None for IP literals."""
    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host=" 1.2.3.4 ", tls=DummyTls(sni=None))
    assert tls_mod._pick_server_hostname(info) is None


def test_a3_pick_hostname_returns_none_for_ipv6_literal():
    """Return None for IPv6 literals (no SNI)."""
    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="2001:db8::1", tls=DummyTls(sni=None))
    assert tls_mod._pick_server_hostname(info) is None


def test_a4_pick_hostname_returns_host_for_dns_name():
    """Return host for DNS/FQDN targets when SNI is not set."""
    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", tls=DummyTls(sni=None))
    assert tls_mod._pick_server_hostname(info) == "ldap.example.com"


def test_a5_pick_hostname_returns_stripped_host_for_dns_name():
    """Return stripped host for DNS/FQDN targets."""
    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host=" ldap.example.com ", tls=DummyTls(sni=None))
    assert tls_mod._pick_server_hostname(info) == "ldap.example.com"


# -------- Group B: _build_ssl_context() --------


def test_b1_build_ssl_context_loads_verify_locations_from_tls_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    """Load CA trust from cafile/capath/cadata as configured."""
    calls: dict[str, Any] = {}

    class DummyContext:
        def __init__(self) -> None:
            self.verify_mode = None
            self.check_hostname = None

        def load_verify_locations(self, *, cafile=None, capath=None, cadata=None) -> None:
            _ = self
            calls["verify"] = {"cafile": cafile, "capath": capath, "cadata": cadata}

        def load_cert_chain(self, *, certfile, keyfile=None, password=None) -> None:
            _ = self
            calls["chain"] = {"certfile": certfile, "keyfile": keyfile, "password": password}

    def fake_create_default_context(*, purpose: Any) -> DummyContext:
        calls["purpose"] = purpose
        return DummyContext()

    monkeypatch.setattr(ssl, "create_default_context", fake_create_default_context)

    info = DummyInfo(
        host="ldap.example.com",
        tls=DummyTls(
            ca_certs_file="/tmp/ca.pem",
            ca_certs_path="/tmp/ca_dir",
            ca_certs_data="PEM",
        ),
    )

    ctx = tls_mod._build_ssl_context(info)

    assert calls["purpose"] == ssl.Purpose.SERVER_AUTH
    assert calls["verify"] == {"cafile": "/tmp/ca.pem", "capath": "/tmp/ca_dir", "cadata": "PEM"}
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is False


def test_b2_build_ssl_context_loads_client_cert_chain_only_when_certfile_present(
    monkeypatch: pytest.MonkeyPatch,
):
    """Load client cert chain only when client_cert_file is provided."""
    chain_calls: list[dict[str, Any]] = []

    class DummyContext:
        def __init__(self) -> None:
            self.verify_mode = None
            self.check_hostname = None

        def load_verify_locations(self, *, cafile=None, capath=None, cadata=None) -> None:
            _ = self, cafile, capath, cadata
            return None

        def load_cert_chain(self, *, certfile, keyfile=None, password=None) -> None:
            _ = self
            chain_calls.append({"certfile": certfile, "keyfile": keyfile, "password": password})

    monkeypatch.setattr(ssl, "create_default_context", lambda *, purpose: DummyContext())

    # noinspection PyArgumentEqualDefault
    info_no_mtls = DummyInfo(host="ldap.example.com", tls=DummyTls(client_cert_file=None))
    tls_mod._build_ssl_context(info_no_mtls)
    assert chain_calls == []

    info_mtls = DummyInfo(
        host="ldap.example.com",
        tls=DummyTls(
            client_cert_file="/tmp/cert.pem",
            client_key_file="/tmp/key.pem",
            client_key_password="pw",
        ),
    )
    tls_mod._build_ssl_context(info_mtls)
    assert chain_calls == [
        {"certfile": "/tmp/cert.pem", "keyfile": "/tmp/key.pem", "password": "pw"}
    ]


def test_b3_build_ssl_context_enforces_verify_mode_and_disables_check_hostname(
    monkeypatch: pytest.MonkeyPatch,
):
    """Set CERT_REQUIRED and disable stdlib hostname checking."""

    class DummyContext:
        def __init__(self) -> None:
            self.verify_mode = None
            self.check_hostname = None

        def load_verify_locations(self, *, cafile=None, capath=None, cadata=None) -> None:
            _ = self, cafile, capath, cadata
            return None

        def load_cert_chain(self, *, certfile, keyfile=None, password=None) -> None:
            _ = self, certfile, keyfile, password
            return None

    monkeypatch.setattr(ssl, "create_default_context", lambda *, purpose: DummyContext())

    info = DummyInfo(host="ldap.example.com")
    ctx = tls_mod._build_ssl_context(info)

    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is False


def test_b4_build_ssl_context_propagates_exceptions_to_caller(monkeypatch: pytest.MonkeyPatch):
    """Raise underlying exception on context configuration failures."""

    class DummyContext:
        def load_verify_locations(self, *, cafile=None, capath=None, cadata=None) -> None:
            raise FileNotFoundError("missing ca")

    monkeypatch.setattr(ssl, "create_default_context", lambda *, purpose: DummyContext())

    info = DummyInfo(host="ldap.example.com", tls=DummyTls(ca_certs_file="/nope/ca.pem"))
    with pytest.raises(FileNotFoundError):
        tls_mod._build_ssl_context(info)


# -------- Group C: wrap_stream_tls() --------


@pytest.mark.asyncio
async def test_c1_wrap_tls_maps_context_build_failure_to_tls_context_build_failed(
    monkeypatch: pytest.MonkeyPatch,
):
    """Map SSLContext build errors to TLS_CONTEXT_BUILD_FAILED with safe details."""

    def boom(_: Any) -> Any:
        raise RuntimeError("ctx failed")

    monkeypatch.setattr(tls_mod, "_build_ssl_context", boom)
    # noinspection PyArgumentEqualDefault
    info = DummyInfo(
        host="ldap.example.com",
        port=636,
        tls=DummyTls(
            ca_certs_file="/tmp/ca.pem",
            ca_certs_path="/tmp/ca_dir",
            ca_certs_data="PEM",
            client_cert_file="/tmp/cert.pem",
            client_key_file="/tmp/key.pem",
            client_key_password="pw",
        ),
    )
    writer = DummyWriter()

    with pytest.raises(TlsError) as ei:
        await tls_mod.wrap_stream_tls(info, writer, handshake_timeout_s=2.5)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_CONTEXT_BUILD_FAILED
    assert err.details["host"] == "ldap.example.com"
    assert err.details["port"] == 636
    assert err.details["ca_certs_file"] == "/tmp/ca.pem"
    assert err.details["ca_certs_path"] == "/tmp/ca_dir"
    assert err.details["ca_certs_data_present"] is True
    assert err.details["client_cert_file"] == "/tmp/cert.pem"
    assert err.details["client_key_file"] == "/tmp/key.pem"
    assert err.details["client_key_password_present"] is True
    assert isinstance(err.cause, RuntimeError)


@pytest.mark.asyncio
async def test_c2_wrap_tls_passes_server_hostname_from_picker_into_start_tls(
    monkeypatch: pytest.MonkeyPatch,
):
    """Pass server_hostname from picker into start_tls call."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: "picked.example.com")

    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", port=636, tls=DummyTls(sni="sni.example.com"))
    writer = DummyWriter()
    writer.set_ssl_object(DummySslObject(cert={"subject": (), "subjectAltName": ()}))

    monkeypatch.setattr(tls_mod, "validate_peer_hostname", lambda *args, **kwargs: None)

    await tls_mod.wrap_stream_tls(info, writer, handshake_timeout_s=1.0)

    assert len(writer.start_tls_calls) == 1
    call = writer.start_tls_calls[0]
    assert call["sslcontext"] is dummy_ctx
    assert call["server_hostname"] == "picked.example.com"
    assert call["ssl_handshake_timeout"] == 1.0


@pytest.mark.asyncio
async def test_c3_wrap_tls_maps_cert_verification_error_to_tls_handshake_verify_failed(
    monkeypatch: pytest.MonkeyPatch,
):
    """Map SSLCertVerificationError to TLS_HANDSHAKE_VERIFY_FAILED."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: "picked.example.com")

    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", port=636, tls=DummyTls(sni=None))
    writer = DummyWriter()
    writer.start_tls_exc = ssl.SSLCertVerificationError("verify failed")

    with pytest.raises(TlsError) as ei:
        await tls_mod.wrap_stream_tls(info, writer, handshake_timeout_s=3.0)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_HANDSHAKE_VERIFY_FAILED
    assert err.details["host"] == "ldap.example.com"
    assert err.details["port"] == 636
    assert err.details["server_hostname"] == "picked.example.com"
    assert err.details["sni"] is None
    assert err.details["handshake_timeout_s"] == 3.0
    assert isinstance(err.cause, ssl.SSLCertVerificationError)


@pytest.mark.asyncio
async def test_c4_wrap_tls_maps_timeout_error_to_tls_handshake_timeout(
    monkeypatch: pytest.MonkeyPatch,
):
    """Map asyncio.TimeoutError to TLS_HANDSHAKE_TIMEOUT."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: None)

    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="1.2.3.4", port=636, tls=DummyTls(sni=None))
    writer = DummyWriter()
    writer.start_tls_exc = asyncio.TimeoutError()

    with pytest.raises(TlsError) as ei:
        # noinspection PyArgumentEqualDefault
        await tls_mod.wrap_stream_tls(info, writer, handshake_timeout_s=None)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_HANDSHAKE_TIMEOUT
    assert err.details["server_hostname"] is None
    assert isinstance(err.cause, asyncio.TimeoutError)


@pytest.mark.asyncio
async def test_c5_wrap_tls_maps_ssl_error_to_tls_handshake_failed(monkeypatch: pytest.MonkeyPatch):
    """Map SSLError to TLS_HANDSHAKE_FAILED."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: "x")

    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", port=636)
    writer = DummyWriter()
    writer.start_tls_exc = ssl.SSLError("protocol error")

    with pytest.raises(TlsError) as ei:
        await tls_mod.wrap_stream_tls(info, writer, handshake_timeout_s=0.5)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_HANDSHAKE_FAILED
    assert isinstance(err.cause, ssl.SSLError)


@pytest.mark.asyncio
async def test_c6_wrap_tls_maps_unknown_exception_to_tls_handshake_unknown(
    monkeypatch: pytest.MonkeyPatch,
):
    """Map unexpected exceptions to TLS_HANDSHAKE_UNKNOWN."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: "x")

    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", port=636)
    writer = DummyWriter()
    writer.start_tls_exc = RuntimeError("boom")

    with pytest.raises(TlsError) as ei:
        await tls_mod.wrap_stream_tls(info, writer, handshake_timeout_s=0.5)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_HANDSHAKE_UNKNOWN
    assert isinstance(err.cause, RuntimeError)


@pytest.mark.asyncio
async def test_c7_wrap_tls_raises_ssl_object_missing_when_writer_has_no_ssl_object(
    monkeypatch: pytest.MonkeyPatch,
):
    """Raise TLS_SSL_OBJECT_MISSING when ssl_object is absent after start_tls."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: "picked")

    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", port=636)
    writer = DummyWriter()
    writer.set_ssl_object(None)

    with pytest.raises(TlsError) as ei:
        await tls_mod.wrap_stream_tls(info, writer)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_SSL_OBJECT_MISSING
    assert err.details["host"] == "ldap.example.com"
    assert err.details["port"] == 636
    assert err.details["server_hostname"] == "picked"


@pytest.mark.asyncio
async def test_c8_wrap_tls_raises_peer_cert_missing_when_getpeercert_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
):
    """Raise TLS_PEER_CERT_MISSING when peer cert is empty/missing."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: "picked")

    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", port=636)
    writer = DummyWriter()
    writer.set_ssl_object(DummySslObject(cert={}))

    with pytest.raises(TlsError) as ei:
        await tls_mod.wrap_stream_tls(info, writer)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_PEER_CERT_MISSING
    assert err.details["server_hostname"] == "picked"


@pytest.mark.asyncio
async def test_c9_wrap_tls_calls_validate_peer_hostname_with_host_and_valid_names(
    monkeypatch: pytest.MonkeyPatch,
):
    """Call validate_peer_hostname with cert, host and valid_names."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: "picked")

    called: dict[str, Any] = {}

    def spy(_cert: Any, *, host: str, valid_names: Optional[list[str]]) -> None:
        called["cert"] = _cert
        called["host"] = host
        called["valid_names"] = valid_names

    monkeypatch.setattr(tls_mod, "validate_peer_hostname", spy)

    cert = {"subject": (), "subjectAltName": (("DNS", "ldap.example.com"),)}
    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", port=636, tls=DummyTls(valid_names=["a", "b"]))
    writer = DummyWriter()
    writer.set_ssl_object(DummySslObject(cert=cert))

    await tls_mod.wrap_stream_tls(info, writer)

    assert called["cert"] == cert
    assert called["host"] == "ldap.example.com"
    assert called["valid_names"] == ["a", "b"]


@pytest.mark.asyncio
async def test_c10_wrap_tls_propagates_tls_hostname_mismatch_error(monkeypatch: pytest.MonkeyPatch):
    """Propagate TlsHostnameMismatchError from validate_peer_hostname."""
    dummy_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(tls_mod, "_build_ssl_context", lambda _: dummy_ctx)
    monkeypatch.setattr(tls_mod, "_pick_server_hostname", lambda _: "picked")

    def boom(*args: Any, **kwargs: Any) -> None:
        _ = args, kwargs
        raise TlsHostnameMismatchError(details={"host": "ldap.example.com"})

    monkeypatch.setattr(tls_mod, "validate_peer_hostname", boom)

    cert = {"subject": (), "subjectAltName": (("DNS", "ldap.example.com"),)}
    # noinspection PyArgumentEqualDefault
    info = DummyInfo(host="ldap.example.com", port=636)
    writer = DummyWriter()
    writer.set_ssl_object(DummySslObject(cert=cert))

    with pytest.raises(TlsHostnameMismatchError):
        await tls_mod.wrap_stream_tls(info, writer)


# -------- Group D: validate_peer_hostname() --------


def test_d1_validate_builds_allowed_as_valid_names_plus_host_dedup_preserve_order():
    """Build allowed list as valid_names + host with de-duplication and stable order."""
    cert = {
        "subjectAltName": (("DNS", "a.example.com"),),
        "subject": ((("commonName", "cn.example.com"),),),
    }
    tls_mod.validate_peer_hostname(
        cert, host="b.example.com", valid_names=["a.example.com", "b.example.com", "a.example.com"]
    )


def test_d2_validate_maps_unexpected_cert_shape_to_tls_peer_cert_malformed():
    """Raise TLS_PEER_CERT_MALFORMED when cert structure cannot be parsed."""
    cert = {
        "subjectAltName": "not-a-seq",
        "subject": ((("commonName", "ldap.example.com"),),),
    }

    with pytest.raises(TlsError) as ei:
        tls_mod.validate_peer_hostname(cert, host="ldap.example.com", valid_names=None)

    err = ei.value
    assert err.reason_code == TlsErrorReason.TLS_PEER_CERT_MALFORMED
    assert err.cause is not None


def test_d3_validate_ip_allowed_name_matches_only_san_ip():
    """IP allowed names must match only SAN 'IP Address' entries, never CN."""
    cert_ok = {
        "subjectAltName": (("IP Address", "1.2.3.4"),),
        "subject": ((("commonName", "1.2.3.4"),),),
    }
    tls_mod.validate_peer_hostname(cert_ok, host="1.2.3.4", valid_names=None)

    cert_bad = {
        "subjectAltName": (("DNS", "ldap.example.com"),),
        "subject": ((("commonName", "1.2.3.4"),),),
    }
    with pytest.raises(TlsHostnameMismatchError):
        tls_mod.validate_peer_hostname(cert_bad, host="1.2.3.4", valid_names=None)


def test_d4_validate_ip_ignores_invalid_san_ip_entries():
    """Ignore invalid SAN IP entries when matching IP targets."""
    cert = {
        "subjectAltName": (("IP Address", "not-an-ip"), ("IP Address", "1.2.3.4")),
        "subject": (),
    }
    tls_mod.validate_peer_hostname(cert, host="1.2.3.4", valid_names=None)


def test_d5_validate_dns_with_san_dns_present_uses_san_dns_only():
    """When SAN DNS exists, match only against SAN DNS and ignore CN."""
    cert = {
        "subjectAltName": (("DNS", "ldap.example.com"),),
        "subject": ((("commonName", "other.example.com"),),),
    }

    with pytest.raises(TlsHostnameMismatchError):
        tls_mod.validate_peer_hostname(cert, host="other.example.com", valid_names=None)

    tls_mod.validate_peer_hostname(cert, host="ldap.example.com", valid_names=None)


def test_d6_validate_dns_without_san_dns_falls_back_to_cn():
    """When SAN DNS is absent, fall back to CN values for DNS matching."""
    cert = {
        "subjectAltName": (("IP Address", "1.2.3.4"),),
        "subject": ((("commonName", "ldap.example.com"),),),
    }
    tls_mod.validate_peer_hostname(cert, host="ldap.example.com", valid_names=None)


def test_d7_validate_mismatch_raises_tls_hostname_mismatch_with_expected_details():
    """On mismatch, raise TlsHostnameMismatchError with stable diagnostic details."""
    cert = {
        "subjectAltName": (("DNS", "ldap.example.com"), ("IP Address", "1.2.3.4")),
        "subject": ((("commonName", "cn.example.com"),),),
    }

    with pytest.raises(TlsHostnameMismatchError) as ei:
        tls_mod.validate_peer_hostname(
            cert, host="nope.example.com", valid_names=["alt.example.com"]
        )

    err = ei.value
    details = err.details
    assert details["host"] == "nope.example.com"
    assert details["valid_names"] == ["alt.example.com"]
    assert details["allowed"] == ["alt.example.com", "nope.example.com"]
    assert "presented" in details
    assert set(details["presented"].keys()) == {"san_dns", "san_ip", "cns"}


# -------- Group E: DNS helpers --------


def test_e1_canon_dns_trims_trailing_dot_and_lowercases():
    """Canonicalize DNS names by trimming, stripping trailing dot and lowercasing."""
    assert tls_mod._canon_dns(" Example.COM. ") == "example.com"


def test_e2_match_dns_applies_canonicalization_to_both_sides():
    """Match must be case-insensitive and tolerate trailing dots via canonicalization."""
    assert tls_mod._match_dns("LDAP.EXAMPLE.COM.", ["ldap.example.com"]) is True


def test_e3_dnsname_match_exact_match_without_wildcard():
    """Exact match must succeed when pattern has no wildcard."""
    assert tls_mod._dnsname_match("a.example.com", "a.example.com") is True
    assert tls_mod._dnsname_match("a.example.com", "b.example.com") is False


def test_e4_dnsname_match_single_label_wildcard_matches_one_label_only():
    """Wildcard must match only a single left-most label."""
    assert tls_mod._dnsname_match("*.example.com", "a.example.com") is True
    assert tls_mod._dnsname_match("*.example.com", "a.b.example.com") is False


def test_e5_dnsname_match_rejects_invalid_wildcard_forms():
    """Reject invalid wildcard patterns (empty, multiple, not left-most label)."""
    assert tls_mod._dnsname_match("", "a.example.com") is False
    assert tls_mod._dnsname_match("*.*.example.com", "a.example.com") is False
    assert tls_mod._dnsname_match("a*.example.com", "a.example.com") is False
    assert tls_mod._dnsname_match("a.*.example.com", "a.b.example.com") is False
    assert tls_mod._dnsname_match("example.*", "example.com") is False
    assert tls_mod._dnsname_match("*.example.com", "example.com") is False


# -------- Group F: misc helpers --------


def test_f1_build_allowed_names_dedup_preserve_order():
    """Build allowed names with de-duplication and stable order."""
    assert tls_mod._build_allowed_names("b", ["a", "b", "a"]) == ["a", "b"]
    assert tls_mod._build_allowed_names("b", None) == ["b"]


def test_f2_try_parse_ip_returns_object_or_none():
    """Return ipaddress object for valid IP literals, else None."""
    assert tls_mod._try_parse_ip("1.2.3.4") is not None
    assert tls_mod._try_parse_ip("2001:db8::1") is not None
    assert tls_mod._try_parse_ip("not-an-ip") is None


def test_f3_match_ip_matches_exact_and_ignores_invalid_entries():
    """Match IP by exact equality and ignore invalid SAN IP strings."""
    target = tls_mod._try_parse_ip("1.2.3.4")
    assert target is not None
    assert tls_mod._match_ip(target, ["not-an-ip", "1.2.3.4"]) is True
    assert tls_mod._match_ip(target, ["1.2.3.5"]) is False


def test_f4_extract_common_names_collects_all_non_empty_cn_in_order():
    """Extract all non-empty CN values from subject preserving order."""
    cert = {
        "subject": (
            (("commonName", "a.example.com"),),
            (("organizationName", "X"), ("commonName", "b.example.com")),
            (("commonName", ""),),
            (("commonName", 123),),
        )
    }
    assert tls_mod._extract_common_names(cert) == ["a.example.com", "b.example.com"]
