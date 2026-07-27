"""Rajya Sabha questions scraper for the `questions` corpus.

Source: sansad.in's `api_rs/business/{starredQuestionsItemWise,
unStarredQuestionsItemWise}` endpoints, validated via recon
(plan/questions-recon-001.md §"Endpoints (verified)" §"RS").

Architecture (different from LS):

  - LS = per-question records. The listing endpoint serves rich
    metadata (member, ministry, subject, type, date) plus a per-question
    PDF URL.
  - RS = per-sitting-day BUNDLED PDF records. The listing endpoint
    serves only `{name, url, date}` per PDF; each PDF contains ~15
    questions for that sitting day. Per-question metadata is NOT
    exposed by the API — it'd require parsing the PDF body to recover.

v1 ships per-PDF records (one record per sitting-day × type). v1.x will
parse PDFs into per-question records sharing the same composite-key
shape as LS once derived. See recon doc §"RS granularity".

Enumeration:
  GET https://sansad.in/api_rs/business/getSessionsList?docType=SQ
      → [{"session": 194}, ..., {"session": 270}]    (72 sessions, RS-194..270)
  GET https://sansad.in/api_rs/business/starredQuestionsItemWise?session=N&type=STARRED
      → [{"name": "...", "url": "...", "date": "DD/MM/YYYY"}, ...]
  GET https://sansad.in/api_rs/business/unStarredQuestionsItemWise?session=N&type=UNSTARRED
      → same shape

PDFs hosted on sansad.in/getFile/UploadedFiles/Questions/QuestionsList/.
Recent sessions are typeset (pypdf-friendly). Older sessions (RS-194
in 2001) may be scanned — markers will route those to a future OCR
slow lane.

Composite primary key: (session, date_iso, question_type).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterator, Optional

from questions.common import RateLimited, http_get


BASE_URL          = "https://sansad.in"
SESSIONS_API      = f"{BASE_URL}/api_rs/business/getSessionsList"
LISTING_STARRED   = f"{BASE_URL}/api_rs/business/starredQuestionsItemWise"
LISTING_UNSTARRED = f"{BASE_URL}/api_rs/business/unStarredQuestionsItemWise"

# Question types served. Mirrors LS' constant for cross-scraper symmetry.
QUESTION_TYPES = ("STARRED", "UNSTARRED")

# Per-house Referer — sansad.in distinguishes the RS sub-app via Referer.
_HEADERS_API = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://sansad.in/rs/questions/questions-and-answers",
}
_HEADERS_PDF = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://sansad.in/rs/questions/questions-and-answers",
}


# ── Records ────────────────────────────────────────────────────────────────


@dataclass
class RSQuestionBundle:
    """One RS sitting-day bundle. Composite key: (session, date_iso, type).

    A bundle PDF contains ~15 questions of one type for one sitting day.
    Per-question metadata (member, ministry, subject) is inside the PDF
    body; v1 doesn't surface it as separate records.
    """
    session:       int
    date_iso:      str         # YYYY-MM-DD
    date_raw:      str         # DD/MM/YYYY (preserved for display)
    question_type: str         # "STARRED" / "UNSTARRED"
    pdf_url:       str


def _parse_date(raw: str) -> Optional[str]:
    """DD/MM/YYYY → YYYY-MM-DD ISO. Returns None on malformed."""
    if not raw:
        return None
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw.strip())
    if not m:
        return None
    d, mo, y = m.groups()
    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"


# ── Enumerate sessions ──────────────────────────────────────────────────────


def fetch_sessions() -> list[int]:
    """Returns RS sessions that have starred questions visible to the
    listing API, ascending.

    Probed both docType=SQ and docType=UQ during recon — same set, so
    we use SQ.
    """
    resp = http_get(SESSIONS_API, headers=_HEADERS_API, params={"docType": "SQ"})
    data = resp.json() or []
    out: list[int] = []
    for row in data:
        s = row.get("session") if isinstance(row, dict) else None
        if s is not None:
            try:
                out.append(int(s))
            except (TypeError, ValueError):
                continue
    return sorted(set(out))


# ── Enumerate PDFs within a session ────────────────────────────────────────


def fetch_session_pdfs(session_no: int,
                       qtypes: Optional[tuple[str, ...]] = None
                       ) -> list[RSQuestionBundle]:
    """All sitting-day bundles for one session, across requested types."""
    qtypes = qtypes if qtypes is not None else QUESTION_TYPES
    out: list[RSQuestionBundle] = []
    for qtype in qtypes:
        url = LISTING_STARRED if qtype == "STARRED" else LISTING_UNSTARRED
        try:
            resp = http_get(url, headers=_HEADERS_API,
                            params={"session": session_no, "type": qtype})
        except RateLimited:
            raise
        except Exception as e:
            print(f"    ERR RS-{session_no} {qtype} listing: {e}")
            continue
        data = resp.json() or []
        if not isinstance(data, list):
            continue
        for entry in data:
            if not isinstance(entry, dict):
                continue
            raw_date = entry.get("date") or ""
            iso = _parse_date(raw_date)
            url_v = entry.get("url") or ""
            if not iso or not url_v:
                continue
            out.append(RSQuestionBundle(
                session=       session_no,
                date_iso=      iso,
                date_raw=      raw_date,
                question_type= qtype,
                pdf_url=       url_v,
            ))
    return out


def walk_rajyasabha(sessions: Optional[list[int]] = None,
                    max_sessions: Optional[int] = None,
                    qtypes: Optional[tuple[str, ...]] = None
                    ) -> Iterator[RSQuestionBundle]:
    """Iterator over all RS question-bundle records.

    `sessions` overrides the discovered list (e.g. for catch-up).
    `max_sessions` caps the walk at the N most recent sessions (used
    for steady-state mode — new content only accrues in the current
    session, so re-walking 72 sessions on every cron is wasteful).
    """
    if sessions is None:
        try:
            discovered = fetch_sessions()
        except RateLimited:
            raise
        except Exception as e:
            print(f"  ! fetch_sessions failed: {e} — using empty walk")
            return
        sessions = discovered
    sessions = sorted(set(sessions), reverse=True)
    if max_sessions is not None and max_sessions > 0:
        sessions = sessions[:max_sessions]
    for ses in sessions:
        try:
            bundles = fetch_session_pdfs(ses, qtypes=qtypes)
        except RateLimited:
            raise
        except Exception as e:
            print(f"  ERR RS-{ses}: {e}")
            continue
        print(f"  RS-{ses}: {len(bundles)} bundle records")
        for b in bundles:
            yield b


# ── File ID + key conventions ───────────────────────────────────────────────


def file_id(session: int, date_iso: str, qtype: str) -> str:
    """Stable id for the text file. `RS{session}_{YYYY-MM-DD}_{S|U}`.
    ISO date keeps the filename sort-stable. Single-letter type prefix
    matches the LS convention (LS18_S4_S_1 / LS18_S4_U_1234).
    """
    t = "S" if qtype.upper() == "STARRED" else "U"
    return f"RS{session}_{date_iso}_{t}"


def report_key(session: int, date_iso: str, qtype: str) -> str:
    """App-side primary key: `questions|rs|<session>|<date_iso>|<type>`."""
    return f"questions|rs|{session}|{date_iso}|{qtype.upper()}"


# ── PDF download (fetch-extract-delete model) ─────────────────────────────


def download_pdf(pdf_url: str, *, session: int, date_iso: str, qtype: str,
                 pdfs_dir: str) -> Optional[str]:
    """Download one bundle PDF to pdfs_dir/<file_id>.pdf and return the
    local path. PDFs are transient — caller deletes after extraction
    per the storage strategy. pdfs_dir is expected to be gitignored.
    """
    os.makedirs(pdfs_dir, exist_ok=True)
    fid = file_id(session, date_iso, qtype)
    pdf_path = os.path.join(pdfs_dir, f"{fid}.pdf")
    if os.path.exists(pdf_path):
        return pdf_path
    try:
        # Some upstream URLs contain literal spaces (recon found one like
        # "English_20241128_SQ 28112024.pdf"). requests will URL-encode the
        # spaces on the wire; we don't manipulate the URL beyond that.
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


def extract_pdf_text(pdf_path: str, *, session: int, date_iso: str,
                     qtype: str, text_dir: str) -> Optional[str]:
    """Extract text from pdf_path → text_dir/<file_id>.txt.

    Per-attempt status markers (same contract as cag/lc/fc/debates):
      .txt           — successful extraction
      .pypdf-empty   — pypdf returned empty (scanned/encrypted) → OCR target
      .pypdf-error   — pypdf raised an exception → retryable
      .ocr-failed    — OCR slow lane returned nothing (permanent tombstone)
    """
    from pypdf import PdfReader  # lazy

    os.makedirs(text_dir, exist_ok=True)
    fid = file_id(session, date_iso, qtype)
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
            if t:
                parts.append(t)
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


def fetch_and_extract(pdf_url: str, *, session: int, date_iso: str, qtype: str,
                      text_dir: str, pdfs_dir: str,
                      delete_pdf: bool = True) -> Optional[str]:
    """End-to-end: download → extract → optionally delete the PDF.

    `delete_pdf=True` enforces fetch-extract-delete per the corpus's
    storage strategy. Set False for OCR slow lane re-processing.
    """
    pdf_path = download_pdf(pdf_url, session=session, date_iso=date_iso,
                            qtype=qtype, pdfs_dir=pdfs_dir)
    if not pdf_path:
        return None
    try:
        return extract_pdf_text(pdf_path, session=session, date_iso=date_iso,
                                qtype=qtype, text_dir=text_dir)
    finally:
        if delete_pdf:
            try:
                os.unlink(pdf_path)
            except OSError:
                pass
