"""Lok Sabha questions scraper for the `questions` corpus.

Source: sansad.in's `api_ls/question/qetFilteredQuestionsAns` endpoint,
validated via recon (plan/questions-recon-001.md). Note the typo in the
upstream endpoint name — `qet` not `get` — preserved as-is.

Single endpoint, dual mode:
  - With `questionNumber` → detail (one record).
  - Without `questionNumber` → listing (paginated, sized via pageSize).

Granularity: PER-QUESTION. Each record = one MP-tabled question + the
government's answer. The body of both the question and the answer is in
a per-question PDF (~200 KB, 2 pages, typeset, pypdf-friendly). Question
text and answer text are NEVER populated in the API JSON — always null.

Composite primary key: (lok_sabha, session, question_type, question_no).
The same question_no recurs across (LS, session, type), so all four
discriminate.

LS-14 and earlier are not in this API (verified — they return 0 records).
LS-15..18 are covered. Pre-LS-15 questions would require a separate
scraper for the digitized PDF route on loksabhadocs.nic.in or the
older nic.in archives — deferred to v1.x.
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from typing import Iterator, Optional

from questions.common import RateLimited, http_get


BASE_URL    = "https://sansad.in"
LISTING_API = f"{BASE_URL}/api_ls/question/qetFilteredQuestionsAns"

# Lok Sabha terms covered by this endpoint. Recon verified LS-15..18; LS-14
# and earlier return 0 records (the modern API doesn't index them).
DEFAULT_LOK_SABHAS = (15, 16, 17, 18)

# Question types served by the endpoint. Recon: STARRED ~500/session,
# UNSTARRED ~5000-6000/session.
QUESTION_TYPES = ("STARRED", "UNSTARRED")

# Sessions to probe per LS term. Real LS terms have at most 17-18 sessions;
# walk 1..20 and let empty sessions fall out naturally. Each empty session
# costs one API call — cheap relative to record extraction.
DEFAULT_SESSION_RANGE = range(1, 21)

# Default page size for the listing API. Recon: honoured up to at least 600.
# 500 lets a full STARRED session (~500 records) come back in one call;
# UNSTARRED (~5K-6K) needs ~10-12 pages.
LISTING_PAGE_SIZE = int(os.environ.get("QUESTIONS_LS_PAGE_SIZE", "500"))

# Per-house Referer — sansad.in distinguishes the LS sub-app via Referer.
_HEADERS_API = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://sansad.in/ls/questions/questions-and-answers",
}
_HEADERS_PDF = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://sansad.in/ls/questions/view-question",
}


# ── Records ────────────────────────────────────────────────────────────────


@dataclass
class LSQuestion:
    """One LS question record. Primary key: (lok_sabha, session, type, qno).
    """
    lok_sabha:      int
    session:        int
    question_no:    int
    question_type:  str            # "STARRED" / "UNSTARRED"
    subject:        str
    members:        list[str]      # free-text list — upstream gives no MP codes
    ministry:       str
    date:           str            # upstream DD.MM.YYYY (raw, preserved)
    pdf_url:        str
    pdf_url_hindi:  Optional[str]
    supplementary:  bool


def _record_to_lsquestion(r: dict) -> Optional[LSQuestion]:
    lok = r.get("lokNo")
    ses = r.get("sessionNo")
    qno = r.get("quesNo")
    qtype = r.get("type")
    if lok is None or ses is None or qno is None or not qtype:
        return None
    members_raw = r.get("member") or []
    members = [m.strip() for m in members_raw if isinstance(m, str) and m.strip()]
    return LSQuestion(
        lok_sabha=     int(lok),
        session=       int(ses),
        question_no=   int(qno),
        question_type= str(qtype).upper(),
        subject=       (r.get("subjects") or "").strip(),
        members=       members,
        ministry=      (r.get("ministry") or "").strip(),
        date=          r.get("date") or "",
        pdf_url=       r.get("questionsFilePath") or "",
        pdf_url_hindi= r.get("questionsFilePathHindi") or None,
        supplementary= bool(r.get("supplementaryType")),
    )


# ── Enumerate ──────────────────────────────────────────────────────────────


def fetch_page(loksabha: int, session: int, qtype: str,
               page: int = 1, size: int = LISTING_PAGE_SIZE
               ) -> tuple[list[LSQuestion], int]:
    """One page of the listing endpoint. Returns (records, total_record_size).

    The response wraps a single-element array containing `listOfQuestions`
    plus `totalRecordSize`. We unwrap and surface the total to the caller
    so they can compute totalPages = ceil(total / size) once.
    """
    params = {
        "loksabhaNo":     loksabha,
        "sessionNumber":  session,
        "questionType":   qtype,
        "pageNo":         page,
        "pageSize":       size,
    }
    resp = http_get(LISTING_API, headers=_HEADERS_API, params=params)
    data = resp.json()
    if not isinstance(data, list) or not data:
        return [], 0
    inner = data[0]
    total = int(inner.get("totalRecordSize") or 0)
    out: list[LSQuestion] = []
    for r in (inner.get("listOfQuestions") or []):
        rec = _record_to_lsquestion(r)
        if rec is not None:
            out.append(rec)
    return out, total


def walk_loksabha(loksabhas: Optional[tuple[int, ...]] = None,
                  session_range: Optional[range] = None,
                  qtypes: Optional[tuple[str, ...]] = None,
                  page_size: int = LISTING_PAGE_SIZE
                  ) -> Iterator[LSQuestion]:
    """Iterator over all LS question records across requested
    (LS term, session, type) tuples.

    Order: newest LS first (LS-18 before LS-15), descending session within
    each LS, both types per session. Within each page the API's natural
    order is preserved (asc by quesNo).

    Walking is cheap (one API call per page; a STARRED session ~1 page,
    UNSTARRED ~10-12 pages). The expensive part is the per-record PDF
    extraction, done later by the orchestrator's extract phase.
    """
    loksabhas    = loksabhas    if loksabhas    is not None else DEFAULT_LOK_SABHAS
    session_range = session_range if session_range is not None else DEFAULT_SESSION_RANGE
    qtypes       = qtypes       if qtypes       is not None else QUESTION_TYPES

    for ls in sorted(loksabhas, reverse=True):
        for ses in sorted(session_range, reverse=True):
            for qtype in qtypes:
                page = 1
                _, total = fetch_page(ls, ses, qtype, page=1, size=page_size)
                if total <= 0:
                    continue
                total_pages = max(1, math.ceil(total / page_size))
                print(f"  LS-{ls} S-{ses} {qtype}: total={total}, pages={total_pages}")
                # Re-walk page 1 explicitly so the generator semantics are
                # consistent (the page-1 result above is metadata-only here).
                yielded = 0
                for page in range(1, total_pages + 1):
                    try:
                        recs, _ = fetch_page(ls, ses, qtype, page=page, size=page_size)
                    except RateLimited:
                        raise
                    except Exception as e:
                        print(f"    ERR page {page}: {e} — aborting this (ls, session, type)")
                        break
                    for rec in recs:
                        yield rec
                        yielded += 1
                print(f"    LS-{ls} S-{ses} {qtype} done: {yielded}/{total} records yielded")


# ── File ID + key conventions ───────────────────────────────────────────────


def file_id(loksabha: int, session: int, qtype: str, question_no: int) -> str:
    """Stable id for the text file. `LS{ls}_S{session}_{S|U}_{qno}`.

    Composite key prevents collisions across LS terms / sessions / types.
    The same qno recurs across each axis, so all four discriminators are
    in the filename. Single-letter type prefix keeps filenames short.
    """
    t = "S" if qtype.upper() == "STARRED" else "U"
    return f"LS{loksabha}_S{session}_{t}_{question_no}"


def report_key(loksabha: int, session: int, qtype: str, question_no: int) -> str:
    """App-side primary key: `questions|ls|<ls>|<session>|<type>|<qno>`.
    """
    return f"questions|ls|{loksabha}|{session}|{qtype.upper()}|{question_no}"


# ── PDF download (fetch-extract-delete model) ─────────────────────────────


def download_pdf(pdf_url: str, *, loksabha: int, session: int, qtype: str,
                 question_no: int, pdfs_dir: str) -> Optional[str]:
    """Download one PDF to pdfs_dir/<file_id>.pdf and return the local
    path. PDFs are transient — caller deletes after extraction per the
    storage strategy.

    pdfs_dir is expected to be gitignored — these files live only for
    the runner's lifetime.
    """
    os.makedirs(pdfs_dir, exist_ok=True)
    fid = file_id(loksabha, session, qtype, question_no)
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


def extract_pdf_text(pdf_path: str, *, loksabha: int, session: int,
                     qtype: str, question_no: int, text_dir: str
                     ) -> Optional[str]:
    """Extract text from pdf_path → text_dir/<file_id>.txt.

    Per-attempt status markers (same contract as cag/lc/fc/debates):
      .txt           — successful extraction
      .pypdf-empty   — pypdf returned empty (scanned/encrypted) → OCR target
      .pypdf-error   — pypdf raised an exception → retryable
      .ocr-failed    — OCR slow lane returned nothing (permanent tombstone)
    """
    from pypdf import PdfReader  # lazy

    os.makedirs(text_dir, exist_ok=True)
    fid = file_id(loksabha, session, qtype, question_no)
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


def fetch_and_extract(pdf_url: str, *, loksabha: int, session: int, qtype: str,
                      question_no: int, text_dir: str, pdfs_dir: str,
                      delete_pdf: bool = True) -> Optional[str]:
    """End-to-end: download → extract → optionally delete the PDF. Returns
    extracted text or None.

    `delete_pdf=True` enforces fetch-extract-delete per the corpus's
    storage strategy. Set False only when the caller wants the PDF
    cached for re-use (OCR slow lane re-processing the same file).
    """
    pdf_path = download_pdf(pdf_url, loksabha=loksabha, session=session,
                            qtype=qtype, question_no=question_no, pdfs_dir=pdfs_dir)
    if not pdf_path:
        return None
    try:
        return extract_pdf_text(pdf_path, loksabha=loksabha, session=session,
                                qtype=qtype, question_no=question_no, text_dir=text_dir)
    finally:
        if delete_pdf:
            try:
                os.unlink(pdf_path)
            except OSError:
                pass
