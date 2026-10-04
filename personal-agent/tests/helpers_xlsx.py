"""Minimal .xlsx writer for tests and the performance script (standard library only, streams rows to disk)."""
from __future__ import annotations

import zipfile
from typing import Iterable
from xml.sax.saxutils import escape


def col_letter(i: int) -> str:
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s


def write_xlsx(path, sheets: dict[str, Iterable[list]], shared: bool = True, date_cols: tuple[int, ...] = ()) -> None:
    """sheets: name -> rows (lists of str/int/float/None/bool). Strings are shared strings (or inline when shared=False).
    Columns listed in date_cols are written as Excel date serials with a date style (style id 1)."""
    strings: dict[str, int] = {}
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        names = list(sheets)
        z.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                   '<Default Extension="xml" ContentType="application/xml"/></Types>')
        z.writestr("xl/workbook.xml", '<?xml version="1.0"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                   'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>'
                   + "".join(f'<sheet name="{escape(n)}" sheetId="{i + 1}" r:id="rId{i + 1}"/>' for i, n in enumerate(names)) + "</sheets></workbook>")
        z.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   + "".join(f'<Relationship Id="rId{i + 1}" Type="worksheet" Target="worksheets/sheet{i + 1}.xml"/>' for i in range(len(names))) + "</Relationships>")
        z.writestr("xl/styles.xml", '<?xml version="1.0"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                   '<cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="14"/></cellXfs></styleSheet>')
        for si, n in enumerate(names):
            with z.open(f"xl/worksheets/sheet{si + 1}.xml", "w") as f:
                f.write(b'<?xml version="1.0"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>')
                for ri, row in enumerate(sheets[n], start=1):
                    cells = []
                    for ci, v in enumerate(row):
                        if v is None:
                            continue
                        ref = f"{col_letter(ci)}{ri}"
                        if isinstance(v, bool):
                            cells.append(f'<c r="{ref}" t="b"><v>{int(v)}</v></c>')
                        elif isinstance(v, (int, float)):
                            style = ' s="1"' if ci in date_cols and ri > 1 else ""
                            cells.append(f'<c r="{ref}"{style}><v>{v}</v></c>')
                        elif shared:
                            idx = strings.setdefault(v, len(strings))
                            cells.append(f'<c r="{ref}" t="s"><v>{idx}</v></c>')
                        else:
                            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{escape(v)}</t></is></c>')
                    f.write(f'<row r="{ri}">{"".join(cells)}</row>'.encode())
                f.write(b"</sheetData></worksheet>")
        z.writestr("xl/sharedStrings.xml", '<?xml version="1.0"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                   + "".join(f"<si><t>{escape(s)}</t></si>" for s in strings) + "</sst>")
