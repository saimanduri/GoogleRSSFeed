from pa_gateway.dlp.dlp import DLPEngine, iban_ok, luhn_ok


def eng(**settings):
    return DLPEngine(b"d" * 32, lambda k: settings.get(k, []))


def test_card_and_iban():
    e = eng()
    assert luhn_ok("4111111111111111") and iban_ok("GB82WEST12345698765432")
    assert e.check_outbound("pay with 4111 1111 1111 1111 please")["blocked"]
    assert e.check_outbound("IBAN GB82 WEST 1234 5698 7654 32")["blocked"]
    assert not e.check_outbound("order number 1234567890123")["blocked"]


def test_api_key_formats():
    e = eng()
    assert e.check_outbound("key AKIAABCDEFGHIJKLMNOP")["blocked"]
    assert e.check_outbound("ghp_" + "a" * 36)["blocked"]
    assert e.check_outbound("-----BEGIN PRIVATE KEY-----")["blocked"]


def test_vault_secret_fingerprint_without_plaintext():
    e = eng()
    e.add_secret_value("S3cr3t-canary-VALUE")
    assert e.contains_secret("look: xxS3cr3t-canary-VALUEyy")
    assert e.check_outbound("q=S3cr3t-canary-VALUE")["secret_match"]
    assert not e.contains_secret("S3cr3t-canary-VALU")
    assert "S3cr3t" not in e.redact("x S3cr3t-canary-VALUE y")


def test_custom_patterns_and_keywords():
    e = eng(**{"dlp.custom_patterns": [r"ACCT-\d{6}"], "dlp.blocked_keywords": ["Project Falcon"]})
    assert e.check_outbound("see ACCT-123456")["blocked"]
    assert e.check_outbound("about project falcon")["blocked"]
