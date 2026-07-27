"""Rajya Sabha debates scraper for the `debates` corpus.

Source: sansad.in's `api_rs/debate/*` endpoints + PDFs hosted on
`cms.rajyasabha.nic.in`, validated via recon
(plan/debates-recon-001.md).

Architecture (different from LS):

  - LS = speech-level records, HTML body via debate-details API,
    one record per floor intervention.
  - RS = daily-proceedings PDFs, one record per sitting DAY. Each day
    has up to 4 file versions: Floor / English / Hindi / Part-1
    Questions and Answers. Per the user's decisions, we ingest
    Floor + English + Part-1 (Hindi skipped — it's a translation
    sibling of English, and we'd surface both via the AI features
    anyway).

Enumeration:
  GET https://sansad.in/api_rs/debate/rs-session
      → [189, 190, ..., 270]                      (~82 sessions)
  GET https://sansad.in/api_rs/debate/date-wise-debate-dates?sessionNo=N
      → ["22/07/2024", "23/07/2024", ...]
  GET https://sansad.in/api_rs/debate/date-wise-debate?sessionNo=N&debateDate=DD/MM/YYYY
      → [{date, sessionNumber, fileDetail: [{fileVersion, fileUrl}, ...]}]

PDFs hosted on cms.rajyasabha.nic.in/UploadedFiles/Debates/... — direct
download with Referer header, no anti-bot. Modern (post-2010) PDFs are
typeset and pypdf-friendly; older sessions may need OCR (slow lane).

Storage strategy: fetch-extract-delete. PDFs are downloaded only as
working files during text extraction; the only persistent artifacts
are the extracted text files + per-attempt markers.

Per-sitting-day primary key: (session_number, date_iso).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Iterator, Optional

from debates.common import RateLimited, http_get


BASE_URL              = "https://sansad.in"
RS_SESSION_API        = f"{BASE_URL}/api_rs/debate/rs-session"
SESSION_DATES_API     = f"{BASE_URL}/api_rs/debate/date-wise-debate-dates"
DATE_FILES_API        = f"{BASE_URL}/api_rs/debate/date-wise-debate"

_HEADERS_API = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://sansad.in/rs/debates/verbatim",
}
_HEADERS_PDF = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://sansad.in/rs/debates/verbatim",
}


# Versions to ingest. Hindi is skipped — it's a translation sibling of
# English, and our AI features (Summary, Ask) bridge any English/Hindi
# divide on demand. Order matters: extraction prioritizes Floor (the
# canonical record), then English, then Part-1.
INGESTED_VERSIONS = ("floor", "english", "part1")

# Map upstream `fileVersion` strings (which vary in spacing/case across
# the corpus) onto our canonical short codes.
_VERSION_MAP_PATTERNS = [
    (re.compile(r"^floor",                       re.I), "floor"),
    (re.compile(r"^english",                     re.I), "english"),
    (re.compile(r"^part[- ]?1",                  re.I), "part1"),
    # Explicit ignore — kept here so debug logs note it as "skipped"
    # rather than "unknown".
    (re.compile(r"^hindi",                       re.I), "_skip_hindi"),
]


def normalize_version(upstream: str) -> Optional[str]:
    """Map upstream fileVersion string → canonical short code.
    Returns None if the version is unknown or skipped.
    """
    s = (upstream or "").strip()
    for pat, code in _VERSION_MAP_PATTERNS:
        if pat.match(s):
            if code.startswith("_skip_"):
                return None
            return code
    return None


# ── Records ────────────────────────────────────────────────────────────────


@dataclass
class RSDebate:
    """One RS sitting-day record. Composite primary key: (session_number,
    date_iso).
    """
    session_number: int
    date_iso:       str          # YYYY-MM-DD (parsed from upstream DD/MM/YYYY)
    date_raw:       str          # DD/MM/YYYY (preserved for display)
    # versions: dict of canonical version code → upstream PDF URL.
    # e.g. {"floor": "https://cms.rajyasabha.../...pdf", "english": "...", "part1": "..."}
    versions:       dict[str, str] = field(default_factory=dict)


def _parse_date(raw: str) -> Optional[str]:
    """Convert DD/MM/YYYY → YYYY-MM-DD ISO. Returns None on malformed."""
    if not raw: return None
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw.strip())
    if not m: return None
    d, mo, y = m.groups()
    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"


def _url_date_matches(url: str, date_iso: str) -> bool:
    """Verify that a PDF URL is the one for the requested sitting day
    (and not a stand-in for some other date).

    The check exists because sansad.in's `date-wise-debate` endpoint
    sometimes returns the same placeholder URL for many sitting days
    in a row — typically the most recent uploaded PDF, parroted back
    for older dates whose own PDFs haven't been transcribed yet.
    Observed concretely in RS-269 (Dec 2025): all 6 sitting dates
    returned `/Floor/269/4122025/04122025.pdf` (the Dec 4 PDF),
    regardless of `debateDate`. Without this filter we'd persist 6
    identical text files under 6 different file_ids.

    The corpus uses several URL conventions that have shifted over
    time, so we cover all of them by substring-matching the requested
    date against the full URL — for the placeholder-bug case only the
    *actually-correct* date will match; for every other date the
    digits don't appear anywhere in the URL and we reject.

    Conventions seen (sessions 189..present):

      Sessions 189..247 (Feb 2000 – Dec 2018):
        Floor:    `/Floor/<N>/F<DD.MM.YYYY>.pdf`        e.g. F30.12.2003.pdf
        English:  `/English/<N>/E<DD.MM.YYYY>.pdf`
        Hindi:    `/Hindi/<N>/<DD.MM.YYYY>.pdf`
      Session 248 (Jan 2019 — transition, mixed):
        Floor/English still use the old `<X><DD.MM.YYYY>.pdf` form.
        Part-1 jumps to `/Part-1.../<N>/<D[D]M[M]YYYY>/F1<DDMMYYYY>.pdf`
        Hindi    likewise to `/Hindi/<N>/<D[D]M[M]YYYY>/Debate-<DDMMYYYY>.pdf`
      Sessions ~249..present (modern):
        `/Floor/<N>/<DDMMYYYY>/<DDMMYYYY>.pdf` (Floor)
        `/Part-1QuestionAnswers/<N>/<DDMMYYYY>/<YYYYMMDD>.pdf` (Part-1)

    The transitional folder-name format uses an *unpadded* day-month
    (e.g. `722019` for 7 Feb 2019), so we generate unpadded variants
    alongside the padded ones.
    """
    if not url or not date_iso: return False
    y, mo, d = date_iso[:4], date_iso[5:7], date_iso[8:10]
    d_unp, mo_unp = str(int(d)), str(int(mo))
    candidates = (
        f"{y}{mo}{d}",            # YYYYMMDD      — 20251211 modern Part-1 filename
        f"{d}{mo}{y}",            # DDMMYYYY      — 12122025 modern Floor filename/folder
        f"{d_unp}{mo}{y}",        # D-MM-YYYY     — 9122019  older folder, unpadded day
        f"{d_unp}{mo_unp}{y}",    # D-M-YYYY      — 722019   older folder, both unpadded
        f"{d}.{mo}.{y}",          # DD.MM.YYYY    — 30.12.2003 older filename
    )
    return any(c in url for c in candidates)


# ── Enumerate sessions ──────────────────────────────────────────────────


def fetch_sessions() -> list[int]:
    """Returns RS sessions visible in the API, ascending.
    e.g. [189, 190, ..., 270].
    """
    resp = http_get(RS_SESSION_API, headers=_HEADERS_API)
    return sorted(int(x) for x in resp.json())


# ── Enumerate dates within a session ────────────────────────────────────


def fetch_session_dates(session_no: int) -> list[str]:
    """List of sitting dates for one session. Returns raw DD/MM/YYYY
    strings (upstream format).
    """
    resp = http_get(SESSION_DATES_API, headers=_HEADERS_API,
                    params={"sessionNo": session_no})
    raw_list = resp.json() or []
    return [d for d in raw_list if isinstance(d, str)]


# ── Get PDFs for a (session, date) tuple ─────────────────────────────────


def fetch_date_pdfs(session_no: int, date_raw: str) -> Optional[RSDebate]:
    """For one (session, date), get the list of file versions + URLs.
    Returns RSDebate (with normalized version map) or None if upstream
    returned no usable file.
    """
    resp = http_get(DATE_FILES_API, headers=_HEADERS_API,
                    params={"sessionNo": session_no, "debateDate": date_raw})
    data = resp.json() or []
    if not data: return None
    date_iso = _parse_date(date_raw)
    if date_iso is None:
        print(f"  ! couldn't parse RS date '{date_raw}' for session {session_no}; skipping")
        return None

    # Upstream sometimes returns more than one entry per date (different
    # presentations); collapse versions across all entries. Filter out
    # placeholder URLs whose filename doesn't match the requested date
    # (see _url_date_matches docstring for the upstream bug we're
    # defending against).
    versions: dict[str, str] = {}
    rejected_stale = 0
    for entry in data:
        for fd in (entry.get("fileDetail") or []):
            ver = normalize_version(fd.get("fileVersion"))
            if ver is None: continue
            url = fd.get("fileUrl")
            if not url or ver in versions: continue
            # Sanitize backslash-separated URLs (occasional upstream bug).
            url = url.replace("\\", "/")
            if not _url_date_matches(url, date_iso):
                rejected_stale += 1
                continue
            versions[ver] = url
    if not versions:
        if rejected_stale:
            print(f"  RS{session_no} {date_raw}: all {rejected_stale} version URL(s) "
                  f"were stale placeholders (pointing at a different date) — "
                  f"recording sitting metadata with no versions; "
                  f"will populate when upstream uploads correct PDFs")
        else:
            # No fileDetail entries at all — also persist as a discovered
            # sitting (title-only) so consumers know the date existed.
            pass
    return RSDebate(
        session_number=session_no,
        date_iso=      date_iso,
        date_raw=      date_raw,
        versions=      versions,
    )


def walk_rajyasabha(sessions: Optional[list[int]] = None,
                    max_sessions: Optional[int] = None) -> Iterator[RSDebate]:
    """Iterator over all RS sitting-day records, working backwards
    (newest session first, then newest date within each session).

    `sessions=None` means "use all sessions visible in the API".
    `max_sessions` caps how many we walk this run — useful when the
    orchestrator wants to do partial backfill bursts.
    """
    if sessions is None:
        sessions = fetch_sessions()
    # Descending — newest first
    sessions = sorted(set(int(s) for s in sessions), reverse=True)
    if max_sessions is not None:
        sessions = sessions[:max_sessions]
    for s in sessions:
        try:
            dates = fetch_session_dates(s)
        except RateLimited:
            raise
        except Exception as e:
            print(f"  ! RS{s} dates fetch failed: {e} — skipping session")
            continue
        # API returns dates in upstream order; sort descending by parsed ISO.
        dated = []
        for d in dates:
            iso = _parse_date(d)
            if iso is None: continue
            dated.append((iso, d))
        dated.sort(reverse=True)
        for iso, raw in dated:
            try:
                rec = fetch_date_pdfs(s, raw)
            except RateLimited:
                raise
            except Exception as e:
                print(f"  ! RS{s} {raw}: file fetch failed: {e}")
                continue
            if rec is not None:
                yield rec


# ── File ID + key conventions ───────────────────────────────────────────────


def base_file_id(session_no: int, date_iso: str) -> str:
    """Base ID per (session, date). e.g. 'RS265_2024-08-09'."""
    return f"RS{session_no}_{date_iso}"


def versioned_file_id(session_no: int, date_iso: str, version: str) -> str:
    """Versioned ID. e.g. 'RS265_2024-08-09_floor'.
    Persisted file name without extension.
    """
    return f"{base_file_id(session_no, date_iso)}_{version}"


def report_key(session_no: int, date_iso: str) -> str:
    """App-side primary key: `debates|rs|<session>|<date_iso>`.
    Per-sitting-day, not per-version (the dialog shows all versions).
    """
    return f"debates|rs|{session_no}|{date_iso}"


# ── PDF download (fetch-extract-delete model) ────────────────────────────


def download_pdf(pdf_url: str, *, session_no: int, date_iso: str,
                 version: str, pdfs_dir: str) -> Optional[str]:
    """Download one PDF to pdfs_dir/<file_id>.pdf and return the local
    path. PDFs are transient — caller is responsible for deletion after
    extraction (per the storage strategy in plan/debates-recon-001.md).

    pdfs_dir is gitignored — these files live only for the runner's
    lifetime.
    """
    os.makedirs(pdfs_dir, exist_ok=True)
    fid = versioned_file_id(session_no, date_iso, version)
    pdf_path = os.path.join(pdfs_dir, f"{fid}.pdf")
    if os.path.exists(pdf_path):
        return pdf_path
    try:
        resp = http_get(pdf_url, headers=_HEADERS_PDF, timeout=180, stream=True)
        with open(pdf_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return pdf_path
    except RateLimited:
        raise
    except Exception as e:
        print(f"    download failed for {fid}: {e}")
        return None


# ── pypdf extraction with per-attempt markers ────────────────────────────


def extract_pdf_text(pdf_path: str, *, session_no: int, date_iso: str,
                     version: str, text_dir: str) -> Optional[str]:
    """Extract text from pdf_path → text_dir/<file_id>.txt.

    Per-attempt status markers (same contract as cag/lc/fc; see CONV.md
    "Per-attempt status markers"):
      .txt           — successful extraction
      .pypdf-empty   — pypdf returned empty (scanned/encrypted) → OCR target
      .pypdf-error   — pypdf raised an exception (corrupt/unusual) → retryable
      .ocr-failed    — OCR slow lane also returned nothing (permanent tombstone)
    """
    from pypdf import PdfReader  # lazy

    os.makedirs(text_dir, exist_ok=True)
    fid = versioned_file_id(session_no, date_iso, version)
    text_path        = os.path.join(text_dir, f"{fid}.txt")
    pypdf_empty_path = os.path.join(text_dir, f"{fid}.pypdf-empty")
    pypdf_error_path = os.path.join(text_dir, f"{fid}.pypdf-error")
    if os.path.exists(text_path):
        with open(text_path, "r", encoding="utf-8") as f:
            return f.read()
    try:
        reader = PdfReader(pdf_path)
        parts = []
        for page in reader.pages:
            t = page.extract_text()
            if t: parts.append(t)
        full = "\n\n".join(parts)
        if not full.strip():
            print(f"    pypdf empty for {fid} — marking .pypdf-empty (OCR candidate)")
            with open(pypdf_empty_path, "w", encoding="utf-8") as f:
                f.write(f"pypdf returned empty for {fid} at {pdf_path}\n")
            return None
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(full)
        return full
    except Exception as e:
        print(f"    pypdf error for {fid} ({pdf_path}): {e} — marking .pypdf-error")
        try:
            with open(pypdf_error_path, "w", encoding="utf-8") as f:
                f.write(f"pypdf error for {fid} at {pdf_path}: {e}\n")
        except Exception:
            pass
        return None


def fetch_and_extract(pdf_url: str, *, session_no: int, date_iso: str,
                      version: str, text_dir: str, pdfs_dir: str,
                      delete_pdf: bool = True) -> Optional[str]:
    """End-to-end: download → extract → optionally delete the PDF.
    Returns extracted text or None.

    `delete_pdf=True` enforces the corpus's fetch-extract-delete
    storage strategy. Set False only when the caller wants the PDF
    cached for re-use (e.g. OCR slow lane re-processing the same
    file).
    """
    pdf_path = download_pdf(pdf_url, session_no=session_no, date_iso=date_iso,
                            version=version, pdfs_dir=pdfs_dir)
    if not pdf_path:
        return None
    try:
        return extract_pdf_text(pdf_path, session_no=session_no, date_iso=date_iso,
                                version=version, text_dir=text_dir)
    finally:
        if delete_pdf:
            try:
                os.remove(pdf_path)
            except Exception:
                pass
