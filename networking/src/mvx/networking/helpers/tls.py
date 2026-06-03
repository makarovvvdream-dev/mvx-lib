# src/mvx/networking/helpers/tls.py
"""
TLS helpers for async networking transports.

This module provides a small set of helpers that encapsulate the common
TLS mechanics needed by higher level engines:

  * upgrading an already-connected TCP stream to TLS, and
  * validating the peer certificate against expected hostnames.

The intent is to keep transport code (such as TcpStreamEngine) focused
on connection and I/O logic while delegating TLS policy, error mapping
and hostname rules to a shared helper.

Public API
==========

wrap_stream_tls(info, writer, *, handshake_timeout_s=None)
    Upgrade an established TCP connection (represented by an
    asyncio.StreamWriter) to TLS using configuration from
    :class:`RemoteEndpointConnectionInfoProto`.

    Responsibilities:

      * build and configure an SSLContext from `info.tls`
        (CA trust, optional client cert/key, CERT_REQUIRED, no
        stdlib hostname checking);

      * pick an appropriate `server_hostname` value for SNI based on
        `info.tls.sni` and `info.host`;

      * perform TLS handshake via `writer.start_tls(...)` with an
        optional handshake timeout;

      * obtain the negotiated SSL object and peer certificate from
        the upgraded writer;

      * validate the peer certificate against the expected host name(s)
        by calling :func:`validate_peer_hostname`.

    Error mapping:

      * context build failures -> :class:`TlsError` with
        ``reason=TLS_CONTEXT_BUILD_FAILED``;
      * handshake timeout -> :class:`TlsError` with
        ``reason=TLS_HANDSHAKE_TIMEOUT``;
      * certificate verification failures raised by the underlying
        SSL stack -> :class:`TlsError` with
        ``reason=TLS_HANDSHAKE_VERIFY_FAILED``;
      * other SSL protocol errors -> :class:`TlsError` with
        ``reason=TLS_HANDSHAKE_FAILED``;
      * unexpected handshake failures -> :class:`TlsError` with
        ``reason=TLS_HANDSHAKE_UNKNOWN``;
      * missing SSL state on the writer -> :class:`TlsError` with
        ``reason=TLS_SSL_OBJECT_MISSING``;
      * missing peer certificate -> :class:`TlsError` with
        ``reason=TLS_PEER_CERT_MISSING``;
      * hostname mismatch -> :class:`TlsHostnameMismatchError`
        (subclass of :class:`TlsError` and
        :class:`ssl.SSLCertVerificationError`).

    On success, the writer is upgraded in-place and subsequent I/O
    uses the TLS session. The function does not return a value.

validate_peer_hostname(cert, *, host, valid_names=None)
    Validate that the presented peer certificate matches at least one
    allowed name under a conservative rule set.

    Inputs:

      * `cert`:
          Peer certificate mapping as returned by
          ``SSLSocket.getpeercert()`` (dict with subjectAltName /
          subject entries).

      * `host`:
          Primary target host name or IP used for the connection.

      * `valid_names`:
          Optional list of additional allowed names. If provided,
          the effective validation set is::

              allowed = dedup(valid_names + [host])

    Matching rules (high level):

      * IP literals:
          matched only against SAN "IP Address" entries; CN is never
          used for IPs.

      * DNS names:
          - if SAN "DNS" entries are present -> match only against
            SAN DNS values;
          - if SAN DNS is absent -> fall back to subject CN values.

      * Wildcards:
          supported only for DNS names under a restricted policy
          (single-label wildcard like ``"*.example.com"``).

    Error mapping:

      * malformed/unexpected certificate structure ->
        :class:`TlsError` with ``reason=TLS_PEER_CERT_MALFORMED``;
      * no allowed name matches ->
        :class:`TlsHostnameMismatchError` with details including the
        requested host, allowed names and presented SAN/CN sets.

Integration
===========

Transports are expected to:

  * open a plain TCP connection first (e.g. via asyncio streams),
  * call :func:`wrap_stream_tls` when TLS is required by configuration,
  * treat all TLS-related failures as :class:`TlsError` (or
    :class:`TlsHostnameMismatchError`) with stable `reason` codes and
    structured details suitable for logging and diagnostics.
"""

from __future__ import annotations

from typing import Optional, Any, TypeAlias

import asyncio
import ssl
import ipaddress

from mvx.networking.models import RemoteEndpointConnectionInfoProto

from mvx.networking.net_errors import (
    TlsError,
    TlsErrorReason,
    TlsHostnameMismatchError,
)

__all__ = [
    "wrap_stream_tls",
    "validate_peer_hostname",
]

PeerCert: TypeAlias = dict[str, Any]


def _pick_server_hostname(info: RemoteEndpointConnectionInfoProto) -> Optional[str]:
    """
    Choose the `server_hostname` value for asyncio StreamWriter.start_tls().

    Purpose
    -------
    `server_hostname` controls two TLS behaviors:
      - SNI (Server Name Indication) sent to the peer during handshake,
      - (optionally) hostname verification in some stacks (we do custom verification).

    Rules
    -----
    - If `info.tls.sni` is set: always return it.
      Rationale: explicit configuration should win and provides stable SNI even if
      `info.host` is an IP or an alias.
    - Else:
      - If `info.host` parses as an IPv4/IPv6 address: return None.
        Rationale: SNI is a hostname concept; for IP targets we skip SNI.
      - Otherwise return `info.host` (FQDN/hostname).
        Rationale: send SNI for name-based virtual hosting and correct certificate selection.

    Notes
    -----
    This function does not validate or normalize `info.host` beyond stripping.
    """
    if info.tls.sni:
        return info.tls.sni

    host = info.host.strip()

    try:
        ipaddress.ip_address(host)
        return None
    except ValueError:
        return host


def _build_ssl_context(info: RemoteEndpointConnectionInfoProto) -> ssl.SSLContext:
    """
    Build and configure an SSLContext for client-side TLS.

    Responsibilities
    ----------------
    - Create a default client context suitable for server authentication.
    - Load CA trust configuration from one of:
        * `ca_certs_file`, `ca_certs_path`, `ca_certs_data`
    - Optionally load a client certificate/key for mutual TLS (mTLS).
    - Enforce certificate verification (`CERT_REQUIRED`).
    - Disable stdlib hostname checking (`check_hostname=False`) because hostname
      validation is performed explicitly by `validate_peer_hostname()`.

    Failure modes (mapped by callers)
    ---------------------------------
    Any exception raised during context creation/configuration is treated as a
    context build failure (e.g., missing CA files, unreadable key, invalid PEM,
    unsupported algorithms/ciphers, OpenSSL policy restrictions).
    """
    # noinspection PyArgumentEqualDefault
    ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

    # CA trust configuration
    ctx.load_verify_locations(
        cafile=info.tls.ca_certs_file,
        capath=info.tls.ca_certs_path,
        cadata=info.tls.ca_certs_data,
    )

    # Client certificate (mTLS) if provided
    client_cert_file: str | bytes | None = info.tls.client_cert_file
    if client_cert_file is not None:
        ctx.load_cert_chain(
            certfile=client_cert_file,
            keyfile=info.tls.client_key_file,
            password=info.tls.client_key_password,
        )

    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = False

    return ctx


async def wrap_stream_tls(
    info: RemoteEndpointConnectionInfoProto,
    writer: asyncio.StreamWriter,
    *,
    handshake_timeout_s: float | None = None,
) -> None:
    """
    Upgrade an established TCP stream to TLS and validate the peer certificate.

    Steps
    -----
    1) Build SSLContext based on `info.tls`.
       - On failure: raise TlsError(TLS_CONTEXT_BUILD_FAILED) with safe-to-log details.
    2) Select `server_hostname` for start_tls() using `_pick_server_hostname(info)`.
    3) Perform TLS handshake by calling `writer.start_tls(...)` with optional timeout.
       - On certificate verification error (chain/trust/expiry/etc.): raise
         TlsError(TLS_HANDSHAKE_VERIFY_FAILED).
       - On handshake timeout: raise TlsError(TLS_HANDSHAKE_TIMEOUT).
       - On SSL protocol error: raise TlsError(TLS_HANDSHAKE_FAILED).
       - On any other exception: raise TlsError(TLS_HANDSHAKE_UNKNOWN).
    4) Ensure TLS state is available on the writer (`ssl_object` exists).
       - If missing: raise TlsError(TLS_SSL_OBJECT_MISSING).
    5) Ensure a peer certificate is present.
       - If missing/empty: raise TlsError(TLS_PEER_CERT_MISSING).
    6) Perform custom hostname validation via `validate_peer_hostname(...)`.
       - On mismatch: raise TlsHostnameMismatchError (also an SSLCertVerificationError).

    Notes
    -----
    - This function is responsible for both transport-level TLS establishment and
      application-level peer identity validation (hostname/SAN rules).
    - All raised errors are domain-specific TLS errors with stable reason codes.
    """
    try:
        ctx = _build_ssl_context(info)
    except Exception as e:
        raise TlsError(
            reason=TlsErrorReason.TLS_CONTEXT_BUILD_FAILED,
            details={
                "host": info.host,
                "port": info.port,
                "ca_certs_file": info.tls.ca_certs_file,
                "ca_certs_path": info.tls.ca_certs_path,
                "ca_certs_data_present": bool(info.tls.ca_certs_data),
                "client_cert_file": info.tls.client_cert_file,
                "client_key_file": info.tls.client_key_file,
                "client_key_password_present": bool(info.tls.client_key_password),
            },
            cause=e,
        ) from e

    server_hostname = _pick_server_hostname(info)

    details = {
        "host": info.host,
        "port": info.port,
        "server_hostname": server_hostname,
        "sni": info.tls.sni,
        "handshake_timeout_s": handshake_timeout_s,
    }
    try:
        await writer.start_tls(
            sslcontext=ctx,
            server_hostname=server_hostname,
            ssl_handshake_timeout=handshake_timeout_s,
        )

    except ssl.SSLCertVerificationError as e:
        raise TlsError(
            reason=TlsErrorReason.TLS_HANDSHAKE_VERIFY_FAILED,
            details=details,
            cause=e,
        ) from e

    except asyncio.TimeoutError as e:
        raise TlsError(
            reason=TlsErrorReason.TLS_HANDSHAKE_TIMEOUT,
            details=details,
            cause=e,
        ) from e

    except ssl.SSLError as e:
        raise TlsError(
            reason=TlsErrorReason.TLS_HANDSHAKE_FAILED,
            details=details,
            cause=e,
        ) from e

    except Exception as e:
        raise TlsError(
            reason=TlsErrorReason.TLS_HANDSHAKE_UNKNOWN,
            details=details,
            cause=e,
        ) from e

    ssl_obj = writer.get_extra_info("ssl_object")
    if ssl_obj is None:
        raise TlsError(
            reason=TlsErrorReason.TLS_SSL_OBJECT_MISSING,
            details={
                "host": info.host,
                "port": info.port,
                "server_hostname": server_hostname,
            },
        )

    cert = ssl_obj.getpeercert()
    if not cert:
        raise TlsError(
            reason=TlsErrorReason.TLS_PEER_CERT_MISSING,
            details={
                "host": info.host,
                "port": info.port,
                "server_hostname": server_hostname,
            },
        )

    validate_peer_hostname(
        cert,
        host=info.host,
        valid_names=info.tls.valid_names,
    )


def validate_peer_hostname(
    cert: PeerCert,
    *,
    host: str,
    valid_names: Optional[list[str]],
) -> None:
    """
    Validate that the peer certificate matches at least one allowed name.

    Inputs
    ------
    cert
        Parsed peer certificate mapping as returned by `ssl.SSLSocket.getpeercert()`.
    host
        The primary target host requested by the caller.
    valid_names
        Optional additional allowed names. If provided, the validation set is the
        de-duplicated sequence: valid_names + [host].

    Matching rules (custom and intentionally strict)
    -----------------------------------------------
    Allowed names are checked one-by-one; validation succeeds on the first match.

    IP targets
      - If an allowed name is an IP literal, it is matched ONLY against SAN "IP Address".
      - CN is NEVER used for IP validation.

    DNS targets
      - If the certificate contains any SAN "DNS" entries:
          * match ONLY against SAN DNS names.
      - Else (no SAN DNS entries present):
          * fall back to matching against CN values.
          * CN fallback is allowed even if SAN contains IP entries.

    Error mapping
    -------------
    - If certificate structure cannot be parsed (unexpected types/shape):
        raise TlsError(TLS_PEER_CERT_MALFORMED) from the parsing exception.
    - If no allowed name matches:
        raise TlsHostnameMismatchError with details containing:
          host, valid_names, allowed list, and the presented SAN/CN sets.

    Notes
    -----
    - Wildcards are supported for DNS names under a restricted policy implemented
      by `_dnsname_match()`.
    - Name canonicalization is performed by `_canon_dns()` (trim, IDNA, lowercase).
    """

    allowed = _build_allowed_names(host, valid_names)

    try:
        san = cert.get("subjectAltName") or ()
        san_dns: list[str] = []
        san_ip: list[str] = []
        for key, value in san:
            if key == "DNS" and isinstance(value, str):
                san_dns.append(value)
            elif key == "IP Address" and isinstance(value, str):
                san_ip.append(value)

        # CN extracted once; used only for DNS fallback when SAN DNS is absent.
        cns: list[str] = _extract_common_names(cert)
    except Exception as e:
        raise TlsError(
            reason=TlsErrorReason.TLS_PEER_CERT_MALFORMED,
            cause=e,
        ) from e

    for name in allowed:
        ip_obj = _try_parse_ip(name)
        if ip_obj is not None:
            # IP targets: SAN IP only
            if _match_ip(ip_obj, san_ip):
                return
            continue

        # DNS targets:
        if san_dns:
            # SAN DNS present -> SAN DNS only
            if _match_dns(name, san_dns):
                return
        else:
            # SAN DNS absent -> CN fallback (even if SAN IP exists)
            if _match_dns(name, cns):
                return

    details = {
        "host": host,
        "valid_names": valid_names,
        "allowed": allowed,
        "presented": {
            "san_dns": san_dns,
            "san_ip": san_ip,
            "cns": cns,
        },
    }
    raise TlsHostnameMismatchError(details=details)


def _build_allowed_names(host: str, valid_names: Optional[list[str]]) -> list[str]:
    """
    Build a stable ordered list of allowed names with duplicates removed.

    Order is preserved:
      - names from valid_names (if any) first,
      - then the `host` value last.

    This ordering is useful for diagnostics because it reflects caller intent.
    """
    seq = (valid_names or []) + [host]
    out: list[str] = []
    seen: set[str] = set()
    for s in seq:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _try_parse_ip(value: str) -> ipaddress._BaseAddress | None:
    """
    Try to parse a string as an IP address.

    Returns
    -------
    An ipaddress object if `value` is a valid IPv4/IPv6 literal, else None.
    """
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        return None


def _match_ip(target: ipaddress._BaseAddress, san_ip: list[str]) -> bool:
    """
    Return True if `target` matches any SAN IP entry exactly.

    SAN IP entries are parsed as IP literals; invalid entries are ignored.
    """
    for ip_s in san_ip:
        ip_obj = _try_parse_ip(ip_s)
        if ip_obj is not None and ip_obj == target:
            return True
    return False


def _match_dns(hostname: str, patterns: list[str]) -> bool:
    """
    Return True if `hostname` matches at least one pattern in `patterns`.

    Both hostname and patterns are canonicalized using `_canon_dns()` and then
    compared using `_dnsname_match()` which implements a restricted wildcard policy.
    """
    hn = _canon_dns(hostname)
    for pat in patterns:
        if _dnsname_match(_canon_dns(pat), hn):
            return True
    return False


def _canon_dns(name: str) -> str:
    """
    Canonicalize a DNS name for matching.

    Transformations
    ---------------
    - Trim surrounding whitespace.
    - Remove a trailing dot.
    - Attempt IDNA encoding/decoding to ASCII (best-effort).
    - Lowercase the final representation.

    Notes
    -----
    Any IDNA conversion failure is ignored and the original string is used.
    """
    s = name.strip().rstrip(".")

    # noinspection PyBroadException
    try:
        s = s.encode("idna").decode("ascii")
    except Exception:
        pass
    return s.lower()


def _dnsname_match(pattern: str, hostname: str) -> bool:
    """
    Match a canonicalized DNS `hostname` against a canonicalized `pattern`.

    Supported forms
    ---------------
    - Exact match when pattern contains no wildcard.
    - A single-label wildcard pattern:
        "*.example.com" matches "a.example.com" but NOT "a.b.example.com".

    Rejected forms (return False)
    -----------------------------
    - Empty pattern.
    - Multiple wildcards.
    - Wildcard not as the full left-most label.
    - Different label count between pattern and hostname.

    This intentionally mirrors conservative TLS wildcard semantics.
    """
    if not pattern:
        return False

    if "*" not in pattern:
        return pattern == hostname

    # Only allow exactly one wildcard and only as the full left-most label.
    if pattern.count("*") != 1:
        return False

    labels_p = pattern.split(".")
    labels_h = hostname.split(".")

    if not labels_p or labels_p[0] != "*":
        return False

    # Single-label wildcard only: must have same label count.
    if len(labels_p) != len(labels_h):
        return False

    return labels_p[1:] == labels_h[1:]


def _extract_common_names(cert: PeerCert) -> list[str]:
    """
    Extract all non-empty Common Name (CN) values from the certificate subject.

    Returns
    -------
    List of CN strings in the order encountered.

    Notes
    -----
    CN is used only as a fallback for DNS validation when SAN DNS is absent.
    """
    out: list[str] = []
    subject = cert.get("subject") or ()
    for rdn in subject:
        for key, value in rdn:
            if key == "commonName" and isinstance(value, str) and value:
                out.append(value)
    return out
