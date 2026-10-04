"""SSRF address-notation tricks against the REAL system resolver (numeric forms need no network):
decimal / hex / octal / short forms of loopback, IPv6 forms, trailing dots, userinfo tricks. All must be denied even with 'fetch any site'."""
import pytest

from pa_gateway.egress.http import EgressClient, EgressDenied, EgressPolicy

ANY = EgressPolicy(any_site=True)


@pytest.mark.parametrize("url", [
    "https://2130706433/", "https://0x7f000001/", "https://0x7f.0.0.1/", "https://017700000001/", "https://127.1/", "https://127.0.1/",
    "https://0/", "https://0.0.0.0/", "https://[::1]/", "https://[::ffff:127.0.0.1]/", "https://[::ffff:7f00:1]/", "https://[0:0:0:0:0:0:0:1]/",
    "https://localhost/", "https://localhost./", "https://LOCALHOST/", "https://169.254.169.254/latest/meta-data/", "https://[fd00:ec2::254]/",
    "https://3232235777/", "https://10.0.0.1/", "https://192.168.0.1:443/", "https://[::]/", "https://[fe80::1%25eth0]/",
])
def test_exotic_loopback_and_private_forms_are_denied(url):
    with pytest.raises(EgressDenied):
        EgressClient().check_url(url, ANY)


@pytest.mark.parametrize("url", [
    "https://example.com@127.0.0.1/", "https://127.0.0.1#@example.com/", "https://user:pass@example.com/", "ftp://example.com/", "file:///C:/Windows/win.ini",
    "gopher://example.com/", "data:text/html,<script>1</script>", "javascript:alert(1)", "https:///nohost", "https://exa mple.com/", "https://example.com:99999/",
    "//example.com/", "\\\\server\\share",
])
def test_scheme_userinfo_and_malformed_urls_are_denied(url):
    with pytest.raises(EgressDenied):
        EgressClient().check_url(url, ANY)
