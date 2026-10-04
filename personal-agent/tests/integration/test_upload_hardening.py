"""Findings of the corpus DAST run (scripts/corpus_dast.py), pinned as regression tests:
non-English text must survive the parser; hostile names are neutralised; image/Office structures are checked before use."""
import base64
import io
import struct
import zipfile

import pytest

from pa_gateway.files import checks


def _upload(env, name: str, data: bytes) -> dict:
    up = env.ui.call("files.upload", {"name": name, "data_b64": base64.b64encode(data).decode()})
    assert env.wait(lambda: env.ui.call("files.preview", {"file_id": up["id"]})["status"] in ("READY", "REJECTED"), 60)
    return env.ui.call("files.preview", {"file_id": up["id"]})


def _docx(text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>')
        z.writestr("word/document.xml", '<?xml version="1.0" encoding="UTF-8"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                   f"<w:body><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:body></w:document>")
    return buf.getvalue()


TEXT = "नमस्ते दुनिया - مرحبا - 你好 - café - 😀"


def test_non_english_text_survives_the_parser(env_nocore):
    env_nocore.setup(model=False)
    for name, data in (("notes.txt", TEXT.encode("utf-8")), ("hindi.docx", _docx(TEXT))):
        row = _upload(env_nocore, name, data)
        assert row["status"] == "READY", (name, row.get("status_reason"))
        assert "नमस्ते" in row["text"] and "你好" in row["text"]


@pytest.mark.parametrize("raw,ok", [
    ("../../../../tmp/traversal.pdf", "traversal.pdf"), (chr(92) + chr(92) + "server" + chr(92) + "share" + chr(92) + "x.txt", "x.txt"),
    ("resume.pdf\x00.exe", "resume.pdf.exe"), ("\u202eexe.cod", "exe.cod"), ("report.pdf\u200b", "report.pdf"),
    ("report.pdf.", "report.pdf"), ("report.pdf ", "report.pdf"), ("file.txt:stream", "file.txt_stream"),
    ("CON.pdf", "_CON.pdf"), ("lpt1.docx", "_lpt1.docx"), ("<b>x</b>.txt", "b_.txt"), ("", "file"), ("...", "file")])
def test_safe_filename(raw, ok):
    got = checks.safe_filename(raw)
    assert got == ok
    assert not any(c in got for c in "/\\:\x00\u202e\u200b<>")


def test_hostile_name_is_stored_clean(env_nocore):
    env_nocore.setup(model=False)
    row = _upload(env_nocore, "a\u202egnp.txt\x00.exe", b"harmless")
    assert row["name"] == "agnp.txt.exe" or "\u202e" not in row["name"]
    assert "\x00" not in row["name"] and "\u202e" not in row["name"]


def _png(w, h, iend=True, big_len=False) -> bytes:
    def chunk(t, d):
        return struct.pack(">I", len(d)) + t + d + b"\0\0\0\0"
    out = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)) + chunk(b"IDAT", b"x" * 10)
    if big_len:
        out += struct.pack(">I", 0xFFFFFFF0) + b"IDAT" + b"\0" * 8
    return out + (chunk(b"IEND", b"") if iend else b"")


def test_image_bombs_and_broken_structures_are_rejected():
    checks.check_image("png", _png(200, 100))
    for bad in (_png(100000, 100000), _png(25000, 25000), _png(60000, 1), _png(10, 10, iend=False), _png(10, 10, big_len=True)):
        with pytest.raises(checks.Rejected):
            checks.check_image("png", bad)
    with pytest.raises(checks.Rejected):
        checks.check_image("jpeg", b"\xff\xd8\xff\xc0\x00\x0b\x08\x00\x10\x00\x10\x01\x01\x11\x00")        # no EOI: truncated
    with pytest.raises(checks.Rejected):
        checks.check_image("gif", b"GIF89a" + struct.pack("<HH", 60000, 60000) + b"\x00\x00\x00;")


def test_gif_frame_bomb_is_rejected():
    frame = b"\x2c" + b"\0" * 8 + b"\x00" + b"\x02" + b"\x01\x00" + b"\x00"
    gif = b"GIF89a" + struct.pack("<HH", 10, 10) + b"\x00\x00\x00" + frame * 2000 + b"\x3b"
    with pytest.raises(checks.Rejected):
        checks.check_image("gif", gif)
    checks.check_image("gif", b"GIF89a" + struct.pack("<HH", 10, 10) + b"\x00\x00\x00" + frame * 3 + b"\x3b")


def test_dde_and_external_reference_are_flagged():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("word/document.xml", '<w:p xmlns:w="x"><w:r><w:instrText> DDEAUTO c:\\\\windows\\\\system32\\\\cmd.exe "/k calc" </w:instrText></w:r></w:p>')
        z.writestr("word/_rels/document.xml.rels", '<Relationships><Relationship Id="1" Target="http://x.invalid/a" TargetMode="External"/></Relationships>')
    info = checks.validate("evil.docx", buf.getvalue(), 10_000_000)
    assert {"office_dde_field", "office_external_reference"} <= set(info["active_content"])


def test_more_script_like_extensions_are_blocked():
    for ext in (".url", ".iqy", ".slk", ".scf", ".settingcontent-ms", ".library-ms", ".rdp", ".xll"):
        with pytest.raises(checks.Rejected):
            checks.validate("x" + ext, b"hello world", 1000)


def test_legacy_windows_text_is_text_but_binary_is_not():
    assert checks.sniff("Café – naïve “quotes” ₹".encode("cp1252", errors="replace") * 3) == "text"
    assert checks.sniff(b"\x00\x01\x02\xff\xfe" * 100) == "binary"


def test_large_but_plausible_image_is_accepted():
    checks.check_image("png", _png(20000, 20000))


def test_table_reader_refuses_dtd_xml():
    from pa_workers.parser import tables
    bomb = b'<?xml version="1.0"?><!DOCTYPE lolz [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;">]><workbook>&b;</workbook>'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("xl/workbook.xml", bomb)
        z.writestr("xl/styles.xml", bomb)
    with zipfile.ZipFile(io.BytesIO(buf.getvalue())) as z:
        with pytest.raises(ValueError):
            tables._read_xml(z, "xl/workbook.xml")
        with pytest.raises(ValueError):
            tables._open_xml(z, "xl/styles.xml")


# ---------------------------------------------------------------- second round (2026-10-02): archives, Office structure, PDF, pictures
def _zip(entries, raw_names=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, body in entries:
            z.writestr(name, body)
    return buf.getvalue()


@pytest.mark.parametrize("bad", ["../evil.txt", ".." + chr(92) + "evil.txt", "a/../../evil.txt", "/abs.txt", "C:/win.txt"])
def test_zip_slip_names_are_rejected(bad):
    with pytest.raises(checks.Rejected) as e:
        checks.validate("x.zip", _zip([(bad, b"x")]), 10_000_000)
    assert "unsafe path" in str(e.value)


def test_near_miss_names_with_dots_are_fine():
    checks.validate("x.zip", _zip([("a..b.txt", b"x"), ("dir/..hidden", b"y")]), 10_000_000)


def test_duplicate_entries_and_encrypted_entries_are_rejected():
    dup = io.BytesIO()
    with zipfile.ZipFile(dup, "w") as z:
        z.writestr("same.txt", b"one")
        with pytest.warns(UserWarning):
            z.writestr("same.txt", b"two")
    with pytest.raises(checks.Rejected) as e:
        checks.validate("x.zip", dup.getvalue(), 10_000_000)
    assert "duplicate" in str(e.value)
    raw = bytearray(_zip([("secret.txt", b"classified")]))
    raw[raw.index(b"PK\x03\x04") + 6] |= 1            # "encrypted" flag in the local header
    raw[raw.index(b"PK\x01\x02") + 8] |= 1            # and in the central directory
    with pytest.raises(checks.Rejected) as e:
        checks.validate("x.zip", bytes(raw), 10_000_000)
    assert "password protected" in str(e.value)


def test_office_files_must_match_their_extension():
    xlsx_inside = _zip([("[Content_Types].xml", b"<x/>"), ("xl/workbook.xml", b"<workbook/>")])
    with pytest.raises(checks.Rejected):
        checks.validate("report.docx", xlsx_inside, 10_000_000)
    with pytest.raises(checks.Rejected):
        checks.validate("slides.pptx", xlsx_inside, 10_000_000)
    assert checks.validate("sheet.xlsx", xlsx_inside, 10_000_000)["family"] == "zip"
    assert checks.validate("letter.docx", _docx("hello"), 10_000_000)["family"] == "zip"


def test_pdf_features_and_trailing_data_are_flagged():
    pdf = b"%PDF-1.4\n1 0 obj<< /Type /Catalog /AA << /O << /S /GoToR /F (x) >> >> /SubmitForm /Encrypt 5 0 R /XFA 1 >>endobj\n%%EOF\n" + b"A" * 5000
    flags = set(checks.validate("a.pdf", pdf, 10_000_000)["active_content"])
    assert {"pdf_remote_goto", "pdf_submit_form", "pdf_encrypted", "pdf_xfa", "pdf_trailing_data"} <= flags


def test_pictures_with_hidden_trailing_data_are_flagged():
    png = _png(20, 20) + b"PK\x03\x04" + b"Z" * 200
    assert "image_trailing_data" in checks.validate("a.png", png, 10_000_000)["active_content"]
    assert checks.validate("a.png", _png(20, 20), 10_000_000)["active_content"] == []
    from tests.helpers_pdf import tiny_jpeg
    assert "image_trailing_data" in checks.validate("a.jpg", tiny_jpeg() + b"PK\x03\x04" + b"Z" * 200, 10_000_000)["active_content"]
    assert checks.validate("a.jpg", tiny_jpeg(), 10_000_000)["active_content"] == []


@pytest.mark.parametrize("name,body", [("note.txt", b"\x4c\x00\x00\x00\x01\x14\x02\x00" + b"\0" * 80), ("readme.txt", b"MSCF\0\0\0\0" + b"\0" * 80),
                                       ("doc.pdf.exe", b"hello"), ("a.scr", b"hello"), ("macro.xll", b"hello")])
def test_shortcuts_cabinets_and_double_extensions_are_blocked(name, body):
    with pytest.raises(checks.Rejected):
        checks.validate(name, body, 10_000_000)
