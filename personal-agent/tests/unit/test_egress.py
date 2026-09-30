"""Egress filter / SSRF (spec 11.2, 31 'Web')."""
import httpx
import pytest

from pa_gateway.egress.http import EgressClient, EgressDenied, EgressPolicy, ip_is_public
import ipaddress

POL = EgressPolicy(allowlist=("example.com", "wikipedia.org"))


def resolver(mapping):
    return lambda host, port: mapping.get(host, ["93.184.216.34"])


@pytest.mark.parametrize("ip", ["127.0.0.1", "10.1.2.3", "192.168.1.1", "172.16.0.1", "169.254.169.254", "100.64.1.1",
                                "::1", "fe80::1", "::ffff:127.0.0.1", "fc00::1", "0.0.0.0", "224.0.0.1", "2002:7f00:1::1"])
def test_private_ranges_blocked(ip):
    assert not ip_is_public(ipaddress.ip_address(ip))


def test_public_ok():
    assert ip_is_public(ipaddress.ip_address("93.184.216.34"))


@pytest.mark.parametrize("url,code", [
    ("http://example.com/", "scheme_blocked"), ("ftp://example.com/", "scheme_blocked"), ("file:///c:/x", "scheme_blocked"),
    ("https://evil.com/", "domain_not_allowed"), ("https://example.com:8443/", "port_blocked"),
    ("https://user:pw@example.com/", "userinfo_blocked"), ("https://169.254.169.254/", "ssrf_blocked"),
])
def test_url_checks(url, code):
    c = EgressClient(resolver({}))
    pol = EgressPolicy(allowlist=("example.com", "169.254.169.254"))
    with pytest.raises(EgressDenied) as e:
        c.check_url(url, pol)
    assert e.value.code == code


def test_dns_rebinding_to_private_blocked():
    c = EgressClient(resolver({"example.com": ["93.184.216.34", "127.0.0.1"]}))
    with pytest.raises(EgressDenied) as e:
        c.check_url("https://example.com/", POL)
    assert e.value.code == "ssrf_blocked"


def test_connector_domains_denied():
    c = EgressClient(resolver({}))
    pol = EgressPolicy(any_site=True, denied_domains=("graph.microsoft.com",))
    with pytest.raises(EgressDenied) as e:
        c.check_url("https://graph.microsoft.com/v1.0/me", pol)
    assert e.value.code == "connector_off"


def test_redirect_to_private_is_rechecked():
    def handler(req: httpx.Request):
        return httpx.Response(302, headers={"location": "https://internal.example.com/secret"})
    c = EgressClient(resolver({"internal.example.com": ["10.0.0.5"]}), transport=httpx.MockTransport(handler))
    with pytest.raises(EgressDenied) as e:
        c.fetch("https://example.com/", EgressPolicy(allowlist=("example.com",)))
    assert e.value.code == "ssrf_blocked"


def test_size_limit_and_content_type():
    big = b"a" * 2000

    def handler(req):
        return httpx.Response(200, headers={"content-type": "text/html"}, content=big)
    c = EgressClient(resolver({}), transport=httpx.MockTransport(handler))
    with pytest.raises(EgressDenied) as e:
        c.fetch("https://example.com/", EgressPolicy(allowlist=("example.com",), max_bytes=1000))
    assert e.value.code == "too_large"

    def exe(req):
        return httpx.Response(200, headers={"content-type": "application/x-msdownload"}, content=b"MZ")
    c = EgressClient(resolver({}), transport=httpx.MockTransport(exe))
    with pytest.raises(EgressDenied) as e:
        c.fetch("https://example.com/", EgressPolicy(allowlist=("example.com",)))
    assert e.value.code == "content_type_blocked"


def test_pinned_ip_and_host_header():
    seen = {}

    def handler(req):
        seen["url"] = str(req.url)
        seen["host"] = req.headers["host"]
        seen["cookie"] = req.headers.get("cookie")
        return httpx.Response(200, headers={"content-type": "text/plain"}, content=b"ok")
    c = EgressClient(resolver({"example.com": ["93.184.216.34"]}), transport=httpx.MockTransport(handler))
    r = c.fetch("https://example.com/a?b=1", POL, headers={"Cookie": "x=1"})
    assert r.body == b"ok" and "93.184.216.34" in seen["url"] and seen["host"] == "example.com" and seen["cookie"] is None
