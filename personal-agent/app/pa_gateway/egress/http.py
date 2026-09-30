"""Egress filter inside pa-gateway (spec 11.2, 14.2 layer 2).

For every outbound request:
  - scheme allowlist (https; http only if the user enabled it)
  - port allowlist (443 [+80 if http] + user extras)
  - DNS resolved HERE; every resolved address must be public (no private, loopback, link-local,
    CGNAT, multicast, reserved, metadata, IPv4-mapped/6to4/Teredo-embedded private); we then
    connect to the pinned IP with the original hostname for SNI + certificate validation
  - redirects handled manually (max 5) and every hop re-checked from scratch
  - response size cap (streamed), timeout, content-type allowlist
  - no cookies, no auth headers except those the gateway injects for bound secrets
  - system proxy settings are ignored (trust_env=False) so pinning cannot be bypassed
"""
from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass, field
from typing import Callable, Iterable
from urllib.parse import urljoin, urlsplit

import httpx

BLOCKED_SCHEMES_MSG = "only https (and optionally http) URLs may be fetched"
METADATA_HOSTS = {"metadata.google.internal", "metadata", "instance-data", "169.254.169.254"}
DEFAULT_CONTENT_TYPES = ("text/html", "text/plain", "application/pdf", "application/json", "application/xhtml+xml",
                         "text/markdown", "text/csv", "application/xml", "text/xml", "application/rss+xml",
                         "application/atom+xml")
CGNAT = ipaddress.ip_network("100.64.0.0/10")


class EgressDenied(Exception):
    def __init__(self, reason: str, code: str = "egress_denied"):
        super().__init__(reason)
        self.code = code


def ip_is_public(ip: ipaddress._BaseAddress) -> bool:
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped:
            return ip_is_public(ip.ipv4_mapped)
        if ip.sixtofour:
            return ip_is_public(ip.sixtofour)
        if ip.teredo:
            return ip_is_public(ip.teredo[1])
        if ip in ipaddress.ip_network("64:ff9b::/96"):  # NAT64 well-known prefix
            return ip_is_public(ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF))
    if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved
            or ip.is_unspecified or not ip.is_global):
        return False
    if isinstance(ip, ipaddress.IPv4Address) and ip in CGNAT:
        return False
    return True


Resolver = Callable[[str, int], list[str]]


def system_resolver(host: str, port: int) -> list[str]:
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return list(dict.fromkeys(i[4][0] for i in infos))


def host_matches(host: str, domains: Iterable[str]) -> bool:
    host = host.lower().rstrip(".")
    for d in domains:
        d = d.lower().strip().lstrip("*.").rstrip(".")
        if d and (host == d or host.endswith("." + d)):
            return True
    return False


@dataclass
class EgressPolicy:
    allow_http: bool = False
    extra_ports: tuple[int, ...] = ()
    allowlist: tuple[str, ...] = ()
    blocklist: tuple[str, ...] = ()
    any_site: bool = False
    denied_domains: tuple[str, ...] = ()  # e.g. connector-owned domains while that connector is OFF
    max_bytes: int = 5 * 1024 * 1024
    timeout: float = 20.0
    max_redirects: int = 5
    content_types: tuple[str, ...] = DEFAULT_CONTENT_TYPES
    enforce_domain_list: bool = True


@dataclass
class FetchResult:
    url: str
    final_url: str
    status: int
    content_type: str
    body: bytes
    redirects: list[str] = field(default_factory=list)
    ip: str = ""

    @property
    def size(self) -> int:
        return len(self.body)


class EgressClient:
    def __init__(self, resolver: Resolver | None = None, transport: httpx.BaseTransport | None = None):
        self.resolver = resolver or system_resolver
        self.transport = transport

    def check_url(self, url: str, pol: EgressPolicy) -> tuple[str, int, str, str]:
        """Returns (scheme, port, host, pinned_ip) or raises EgressDenied."""
        parts = urlsplit(url)
        scheme = (parts.scheme or "").lower()
        if scheme not in ("https", "http") or (scheme == "http" and not pol.allow_http):
            raise EgressDenied(BLOCKED_SCHEMES_MSG, "scheme_blocked")
        if parts.username or parts.password:
            raise EgressDenied("credentials in URLs are not allowed", "userinfo_blocked")
        host = (parts.hostname or "").lower().rstrip(".")
        if not host:
            raise EgressDenied("URL has no host", "bad_url")
        try:
            port = parts.port or (443 if scheme == "https" else 80)
        except ValueError as e:
            raise EgressDenied("invalid port", "bad_url") from e
        allowed_ports = {443} | ({80} if pol.allow_http else set()) | set(pol.extra_ports)
        if port not in allowed_ports:
            raise EgressDenied(f"port {port} is not allowed", "port_blocked")
        if host in METADATA_HOSTS:
            raise EgressDenied("metadata endpoints are blocked", "ssrf_blocked")
        if host_matches(host, pol.denied_domains):
            raise EgressDenied("this destination belongs to a connector that is turned off", "connector_off")
        if host_matches(host, pol.blocklist):
            raise EgressDenied("domain is on your blocklist", "domain_blocked")
        if pol.enforce_domain_list and not pol.any_site and not host_matches(host, pol.allowlist):
            raise EgressDenied("domain is not on the allowlist (Settings > Web Access)", "domain_not_allowed")
        # literal IPs are checked directly; names are resolved here and pinned
        try:
            literal = ipaddress.ip_address(host)
            ips = [str(literal)]
        except ValueError:
            try:
                ips = self.resolver(host, port)
            except OSError as e:
                raise EgressDenied(f"DNS lookup failed: {e}", "dns_failed") from e
        if not ips:
            raise EgressDenied("DNS returned no addresses", "dns_failed")
        for ip in ips:
            if not ip_is_public(ipaddress.ip_address(ip.split("%")[0])):
                raise EgressDenied("destination resolves to a private or reserved address", "ssrf_blocked")
        return scheme, port, host, ips[0]

    def fetch(self, url: str, pol: EgressPolicy, method: str = "GET", headers: dict[str, str] | None = None,
              body: bytes | None = None) -> FetchResult:
        redirects: list[str] = []
        current = url
        for _hop in range(pol.max_redirects + 1):
            scheme, port, host, ip = self.check_url(current, pol)
            parts = urlsplit(current)
            ip_host = f"[{ip}]" if ":" in ip else ip
            path = parts.path or "/"
            if parts.query:
                path += "?" + parts.query
            pinned = f"{scheme}://{ip_host}:{port}{path}"
            hdrs = {"Host": host if port in (80, 443) else f"{host}:{port}", "User-Agent": "PersonalAgent/0.1",
                    "Accept": ", ".join(pol.content_types)}
            for k, v in (headers or {}).items():
                if k.lower() not in ("host", "cookie"):
                    hdrs[k] = v
            with httpx.Client(transport=self.transport, trust_env=False, follow_redirects=False,
                              timeout=pol.timeout, http2=False) as client:
                req = client.build_request(method, pinned, headers=hdrs, content=body,
                                           extensions={"sni_hostname": host})
                resp = client.send(req, stream=True)
                try:
                    if resp.status_code in (301, 302, 303, 307, 308):
                        loc = resp.headers.get("location")
                        if not loc:
                            raise EgressDenied("redirect without location", "bad_redirect")
                        current = urljoin(current, loc)
                        redirects.append(current)
                        if resp.status_code == 303:
                            method, body = "GET", None
                        continue
                    ctype = resp.headers.get("content-type", "").split(";")[0].strip().lower()
                    if pol.content_types and ctype and ctype not in pol.content_types:
                        raise EgressDenied(f"content type {ctype} is not allowed", "content_type_blocked")
                    declared = resp.headers.get("content-length")
                    if declared and declared.isdigit() and int(declared) > pol.max_bytes:
                        raise EgressDenied("response too large", "too_large")
                    buf = bytearray()
                    for chunk in resp.iter_bytes():
                        buf.extend(chunk)
                        if len(buf) > pol.max_bytes:
                            raise EgressDenied("response too large", "too_large")
                    return FetchResult(url, current, resp.status_code, ctype, bytes(buf), redirects, ip)
                finally:
                    resp.close()
        raise EgressDenied("too many redirects", "too_many_redirects")
