"""Lok Sabha debates scraper for the `debates` corpus.

Source: sansad.in's `api_ls/debate/*` endpoints, validated via recon
(plan/debates-recon-001.md). Two-phase per record:

  1. ENUMERATE   — debate-search?loksabha=N&page=K returns metadata
                   only. `contents` field is empty here.
  2. FETCH BODY  — debate-details?loksabha=N&sessionNumber=S&dbSlNo=I
                   returns HTML body in `debateDesc`. Word-export-style
                   markup — strip to plain text.

Granularity: SPEECH-LEVEL. One record per floor intervention (ruling,
paper laid, speech, special-mention, etc.). ~64K records across LS-13
to LS-18.

NO PDFs. Per the storage strategy (Option 2 in the recon doc), we
fetch HTML, strip to text, persist text. No PDF download involved.

Composite primary key: (lok_sabha, session, dbSlno). The same dbSlno
can recur across LS terms (it's session-scoped, not corpus-scoped).
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Iterator, Optional

from html.parser import HTMLParser

from debates.common import RateLimited, http_get


BASE_URL = "https://sansad.in"
SEARCH_API  = f"{BASE_URL}/api_ls/debate/debate-search"
DETAILS_API = f"{BASE_URL}/api_ls/debate/debate-details"

# Lok Sabha terms covered. Matches DRSC's LS-14..18 depth for cross-
# corpus consistency. LS-13 (1999-2004) IS available via this API (recon
# verified ~7,616 records) but excluded for symmetry with DRSC; one env
# var change to bring it back. Pre-LS-13 NOT available via API (returns
# errors) — would require a separate scraper for the digitized PDF
# route. Defer that to v1.x.
DEFAULT_LOK_SABHAS = [14, 15, 16, 17, 18]

_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://sansad.in/ls/debates/digitized",
}


# ── Records ────────────────────────────────────────────────────────────────


@dataclass
class LSDebate:
    """One LS debate intervention. Primary key: (lok_sabha, session, dbSlno).
    """
    lok_sabha:     int
    session:       int
    db_slno:       int
    title:         str
    debate_date:   str            # upstream: DD/MM/YYYY
    debate_type:   int
    debate_type_desc: str
    members:       list[dict]      # list of {mp_name, mp_code, mp_part_code}
    keywords:      list[str]


def _record_to_lsdebate(r: dict) -> Optional[LSDebate]:
    lok_sabha = r.get("loksabha")
    session   = r.get("session")
    db_slno   = r.get("dbSlno")
    if lok_sabha is None or session is None or db_slno is None:
        return None
    members = []
    for m in (r.get("mpPartDetailList") or []):
        members.append({
            "mp_name":      (m.get("mpName") or "").strip(),
            "mp_code":      m.get("mpCode"),
            "mp_part_code": m.get("mpPartCode"),
        })
    return LSDebate(
        lok_sabha=        int(lok_sabha),
        session=          int(session),
        db_slno=          int(db_slno),
        title=            (r.get("debateTitle") or "").strip(),
        debate_date=      r.get("debateDate") or "",
        debate_type=      r.get("debateType") or 0,
        debate_type_desc= (r.get("debateTypeDesc") or "").strip(),
        members=          members,
        keywords=         [k for k in (r.get("keywordUsed") or []) if k],
    )


# ── Enumerate ──────────────────────────────────────────────────────────────


def fetch_page(loksabha: int, page: int = 1, size: int = 200) -> tuple[list[LSDebate], dict]:
    """One page of debate-search. Returns (records, _metadata).

    No sort params — passing `sortOn=debateDate&sortBy=desc` causes
    the API to 500 (the field name isn't accepted). Fortunately the
    default order IS desc-by-dbSlno (which monotonically tracks
    date), so we get newest-first naturally.
    """
    params = {
        "loksabha": loksabha,
        "page":     page,
        "size":     size,
    }
    resp = http_get(SEARCH_API, headers=_HEADERS, params=params)
    data = resp.json()
    meta = data.get("_metadata", {})
    out = []
    for r in (data.get("records") or []):
        rec = _record_to_lsdebate(r)
        if rec is not None:
            out.append(rec)
    return out, meta


def walk_loksabha(loksabhas: Optional[list[int]] = None,
                  page_size: int = 200) -> Iterator[LSDebate]:
    """Iterator over all LS debate records across requested LS terms.

    Work backwards: process LS terms in descending order (most recent
    first), and within each term the API already gives us
    sortBy=desc on debateDate.

    The upstream API pages through cleanly with totalPages in the
    metadata. We loop until the last page.
    """
    loksabhas = loksabhas if loksabhas is not None else DEFAULT_LOK_SABHAS
    # Descending — recency-first. Matches "work backwards" guidance.
    for ls in sorted(loksabhas, reverse=True):
        print(f"[walk] LS-{ls}: enumerating debate records...")
        page = 1
        seen = 0
        total_pages = 1
        while page <= total_pages:
            records, meta = fetch_page(ls, page=page, size=page_size)
            total_pages = meta.get("totalPages") or 1
            total_elems = meta.get("totalElements") or 0
            for rec in records:
                yield rec
                seen += 1
            print(f"  page {page}/{total_pages}: {len(records)} records "
                  f"(running={seen}/{total_elems})")
            page += 1
        print(f"  LS-{ls} done: {seen} records yielded")


# ── Fetch body + strip HTML ────────────────────────────────────────────────


# Note: this regex matches ALL whitespace including newlines, because the
# upstream HTML source is column-wrapped at ~80 chars, scattering newlines
# inside text nodes that aren't meaningful paragraph breaks. The actual
# paragraph structure comes from block-tag transitions (<p>, <div>, <br>),
# which we insert as explicit `\n` outside `handle_data`. Final blank-line
# collapse then normalises to at-most-double newlines for paragraph
# separation.
_RE_WHITESPACE = re.compile(r"\s+")
_RE_BLANK_LINES = re.compile(r"\n[ \t]*\n[ \t\n]*")


class _HTMLToText(HTMLParser):
    """Strip Word-export HTML to plain text. Preserves paragraph breaks
    (<p>, <div>, <br>, end of block tags). Drops inline styling.
    """
    _BLOCK_TAGS = {"p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4",
                   "h5", "h6", "blockquote", "pre", "section", "article"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._buf: list[str] = []
        self._suppress_depth = 0   # in <style>/<script>/<head>

    def handle_starttag(self, tag, attrs):
        if tag in ("style", "script", "head"):
            self._suppress_depth += 1
            return
        if tag in self._BLOCK_TAGS and self._buf and not self._buf[-1].endswith("\n"):
            self._buf.append("\n")

    def handle_endtag(self, tag):
        if tag in ("style", "script", "head"):
            self._suppress_depth = max(0, self._suppress_depth - 1)
            return
        if tag in self._BLOCK_TAGS:
            self._buf.append("\n")

    def handle_data(self, data):
        if self._suppress_depth:
            return
        # Collapse runs of whitespace (tabs, multi-space). Preserve
        # newlines via block-tag handling.
        self._buf.append(_RE_WHITESPACE.sub(" ", data))

    def get_text(self) -> str:
        text = "".join(self._buf)
        # Decode any remaining HTML entities (convert_charrefs handles
        # most, but mso/o:p fragments occasionally slip through).
        text = html.unescape(text)
        # Collapse 3+ blank lines into 2 (paragraph separator).
        text = _RE_BLANK_LINES.sub("\n\n", text)
        return text.strip()


def strip_html_body(html_body: str) -> str:
    """Strip Word-export HTML markup → plain text. Stable across rebuilds.
    """
    if not html_body:
        return ""
    p = _HTMLToText()
    try:
        p.feed(html_body)
        p.close()
    except Exception:
        # Defensive — if the parser blows up on malformed HTML, fall
        # back to a coarse regex strip. Better to lose some structure
        # than to lose the content entirely.
        return _RE_BLANK_LINES.sub("\n\n",
            re.sub(r"<[^>]+>", " ", html.unescape(html_body))).strip()
    return p.get_text()


def fetch_body(lok_sabha: int, session: int, db_slno: int) -> Optional[str]:
    """Fetch debate-details for one record. Returns plain text (after
    HTML strip), or None if upstream had no body.
    """
    params = {
        "loksabha":      lok_sabha,
        "sessionNumber": session,
        "dbSlNo":        db_slno,        # note: capital N in dbSlNo
    }
    resp = http_get(DETAILS_API, headers=_HEADERS, params=params)
    try:
        data = resp.json()
    except Exception:
        return None
    body = data.get("debateDesc") or ""
    if not body.strip():
        return None
    return strip_html_body(body)


# ── File ID + key conventions ───────────────────────────────────────────────


def file_id(lok_sabha: int, session: int, db_slno: int) -> str:
    """Stable id for the text file. `LS<ls>_S<session>_<dbslno>`.
    The composite key prevents collisions across LS terms (where the
    same dbSlno can recur in a different LS).
    """
    return f"LS{lok_sabha}_S{session}_{db_slno}"


def report_key(lok_sabha: int, session: int, db_slno: int) -> str:
    """App-side primary key: `debates|ls|<ls>|<session>|<dbSlno>`."""
    return f"debates|ls|{lok_sabha}|{session}|{db_slno}"


# ── Extract text (download body + save to disk + per-attempt markers) ────


def extract_text(lok_sabha: int, session: int, db_slno: int,
                 *, text_dir: str) -> Optional[str]:
    """Fetch + strip + save text/<file_id>.txt. Idempotent.

    Per-attempt status markers (sidecar files in text_dir, same contract
    as cag/lc/fc; see CONV.md "Per-attempt status markers"):
      .txt          — successful extraction
      .empty        — debate-details returned no body (rare; some short
                      records have empty debateDesc)
      .error        — HTTP/parse error (retryable next run)
    """
    import os

    os.makedirs(text_dir, exist_ok=True)
    fid = file_id(lok_sabha, session, db_slno)
    text_path  = os.path.join(text_dir, f"{fid}.txt")
    empty_path = os.path.join(text_dir, f"{fid}.empty")
    error_path = os.path.join(text_dir, f"{fid}.error")
    if os.path.exists(text_path):
        with open(text_path, "r", encoding="utf-8") as f:
            return f.read()

    try:
        text = fetch_body(lok_sabha, session, db_slno)
    except RateLimited:
        raise
    except Exception as e:
        print(f"  Failed to fetch body for {fid}: {e}")
        try:
            with open(error_path, "w", encoding="utf-8") as f:
                f.write(f"fetch error for {fid}: {e}\n")
        except Exception:
            pass
        return None

    if text is None or not text.strip():
        # Some records (rulings, papers laid, very short procedural
        # items) have empty bodies upstream. Mark and skip.
        with open(empty_path, "w", encoding="utf-8") as f:
            f.write(f"debate-details returned no body for {fid}\n")
        return None

    with open(text_path, "w", encoding="utf-8") as f:
        f.write(text)
    return text
