#!/usr/bin/env python3
"""Questions corpus builder — orchestrator.

Per scheduled (or workflow_dispatch) run:
  1. Walk the LS qetFilteredQuestionsAns API across LOK_SABHAS × sessions
     × types, newest-first. Merge metadata into reports.json under the
     "ls" key.
  2. For each record missing extracted text, download the per-question PDF,
     run pypdf extract, persist to text/ls/<file_id>.txt. PDF is deleted
     after extraction (fetch-extract-delete model).
  3. (Derive phase) Build manifest + sharded bundle + sharded index +
     meta + audit.

Output goes under docs/questions/, served at
sansadsaar-data.naklitechie.com/questions/.

PHASE A scope: LS only. Phase B (Rajya Sabha) extends this orchestrator
with a parallel call into questions/scrapers/rajyasabha.py — the structure
here (house-keyed reports.json, house-stratified text/ subdirs) is
designed so the RS addition is strictly additive, no refactor needed.

Storage strategy (per plan/questions-recon-001.md): fetch-extract-delete.
PDFs are downloaded only as working files during text extraction; the
only persistent artifacts are extracted text files + per-attempt markers
+ bundled text shards.

Independence Principle: no imports from cag, lc, fc, drsc, bills, or
debates scrapers. The HTTP layer + RateLimited + jitter + checkpoint
primitives are re-implemented under questions/common.py.
"""

from __future__ import annotations

import json
import math
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from parliamentwatch_text_shards import (  # noqa: E402
    write_text_shards, consolidate_markers, load_markers, write_json_idempotent,
    shard_group, SEARCH_SHARD_STRIDE, bucket_for, load_bundled_ids,
)

from questions.common import RateLimited
from questions.scrapers.loksabha import (
    DEFAULT_LOK_SABHAS,
    DEFAULT_SESSION_RANGE,
    LSQuestion,
    fetch_page as ls_fetch_page,
    fetch_and_extract as ls_fetch_and_extract,
    file_id as ls_file_id,
    report_key as ls_report_key,
)
from questions.scrapers.rajyasabha import (
    RSQuestionBundle,
    walk_rajyasabha,
    fetch_and_extract as rs_fetch_and_extract,
    file_id as rs_file_id,
    report_key as rs_report_key,
)

# ── Paths ───────────────────────────────────────────────────────────────────

ASSETS    = ROOT / "docs"
DOCS      = ASSETS / "questions"
TEXT_DIR  = DOCS / "text"
PDFS_DIR  = DOCS / "pdfs"   # transient — gitignored
DOCS.mkdir(parents=True, exist_ok=True)

REPORTS_JSON      = DOCS / "reports.json"        # legacy single-file (pre-sharding)
REPORTS_META_JSON = DOCS / "reports-meta.json"   # shard manifest the app fetches first
MANIFEST_JSON     = DOCS / "manifest.json"
META_JSON         = DOCS / "meta.json"
AUDIT_JSON        = DOCS / "audit.json"

# Per-house shard size. Same as debates — picked to stay under CF's
# 25 MiB per-file cap with headroom. Question records carry similar
# metadata weight to LS debates (~500 B per record), so 2500 × ~500 B =
# ~1.25 MB per shard, very comfortable.
SHARD_SIZE = 2500

# Houses we cover. Phase B adds "rs".
HOUSES = ["ls", "rs"]

# ── Per-run budget ─────────────────────────────────────────────────────────
#
# Per recon (plan/questions-recon-001.md §"Politeness budget"): ~280K LS
# records across LS-15..18. At MAX_EXTRACTIONS=50, 12×/day cron = 600/day
# → ~467 days backfill. That's the politeness floor; raising it requires
# explicit upstream consent (which we don't have). See playbook
# §"Politeness policy".

MAX_EXTRACTIONS_PER_RUN     = int(os.environ.get("MAX_EXTRACTIONS_PER_RUN", "50"))
# RS PDFs are bundled (~15 questions per file, ~150 KB) — heavier than LS
# per-Q PDFs (~200 KB single page). Default smaller per run because each
# bundle takes more wall-clock + bandwidth. 25 × 12/day = 300/day → clears
# the ~2,700-bundle RS backfill in ~9 days.
MAX_RS_EXTRACTIONS_PER_RUN  = int(os.environ.get("MAX_RS_EXTRACTIONS_PER_RUN", "25"))
MAX_RUN_SECONDS             = int(os.environ.get("MAX_RUN_SECONDS", "900"))   # 15 min
EXTRACT_WORKERS             = int(os.environ.get("EXTRACT_WORKERS", "4"))

# LS terms to enumerate. Comma-separated env var. Default = LS-15..18
# per recon coverage. The merge-with-existing logic preserves entries
# from un-enumerated terms across runs, so narrowing LOK_SABHAS to (say)
# "18" for daily steady-state is safe.
_LS_RAW = os.environ.get("LOK_SABHAS", "").strip()
LOK_SABHAS = (tuple(int(x) for x in _LS_RAW.split(",") if x.strip())
              if _LS_RAW else DEFAULT_LOK_SABHAS)

# Session range to probe per LS. CSV (e.g. "1,2,3") or range "1-20".
# Default is the scraper's DEFAULT_SESSION_RANGE (1..20).
_SES_RAW = os.environ.get("LS_SESSIONS", "").strip()
if _SES_RAW:
    if "-" in _SES_RAW and "," not in _SES_RAW:
        a, b = _SES_RAW.split("-", 1)
        SESSION_RANGE = range(int(a), int(b) + 1)
    else:
        # Build a range that covers all explicit values
        vals = sorted({int(x) for x in _SES_RAW.split(",") if x.strip()})
        SESSION_RANGE = range(vals[0], vals[-1] + 1) if vals else DEFAULT_SESSION_RANGE
else:
    SESSION_RANGE = DEFAULT_SESSION_RANGE

QTYPES_RAW = os.environ.get("LS_QTYPES", "").strip().upper()
QTYPES = (tuple(x for x in QTYPES_RAW.split(",") if x in ("STARRED", "UNSTARRED"))
          if QTYPES_RAW else ("STARRED", "UNSTARRED"))

# RS sessions to enumerate. Three modes (mirrors build_debates.py):
#   - RS_SESSIONS unset (default): walk the 2 most recent sessions visible
#     upstream. Steady-state mode — captures new content (RS only accrues
#     sittings in the current session) without re-walking the historical
#     archive on every cron firing.
#   - RS_SESSIONS=all: walk every session the API exposes (72 sessions,
#     ~150 metadata calls × 2 types = ~300 enumeration calls). One-shot
#     backfill mode — use via workflow_dispatch, not via the regular cron.
#   - RS_SESSIONS=<csv>: walk exactly those sessions. Used for targeted
#     catch-up.
_RS_RAW = os.environ.get("RS_SESSIONS", "").strip().lower()
if _RS_RAW == "all":
    RS_SESSIONS: Optional[list[int]] = None
    _RS_MAX = None
    _RS_MODE = "all-sessions"
elif _RS_RAW:
    RS_SESSIONS = [int(x) for x in _RS_RAW.split(",") if x.strip()]
    _RS_MAX = None
    _RS_MODE = f"explicit={RS_SESSIONS}"
else:
    RS_SESSIONS = None
    _RS_MAX = 2
    _RS_MODE = "recent-2"

# Cooldown after 429/403 — defensive.
RATE_LIMIT_COOLDOWN_SECONDS = int(os.environ.get("RATE_LIMIT_COOLDOWN_SECONDS", str(6 * 3600)))

# In-flight checkpointing — same pattern as cag/lc/fc/debates.
CHECKPOINT_EVERY_N = int(os.environ.get("CHECKPOINT_EVERY_N", "100"))
CHECKPOINT_EVERY_S = int(os.environ.get("CHECKPOINT_EVERY_S", "300"))


def _git(*args) -> tuple[int, str, str]:
    p = subprocess.run(["git", *args], capture_output=True, text=True, check=False)
    return p.returncode, p.stdout, p.stderr


def checkpoint_commit(message: str, paths: list[str]) -> bool:
    rc, _, err = _git("add", "--", *paths)
    if rc != 0:
        print(f"  [checkpoint] git add failed: {err.strip() or 'unknown'}")
        return False
    rc, _, _ = _git("diff", "--cached", "--quiet")
    if rc == 0:
        return True
    rc, _, err = _git("commit", "-m", message)
    if rc != 0:
        print(f"  [checkpoint] git commit failed: {err.strip()}")
        return False
    rc, _, err = _git("pull", "--rebase", "origin", "main")
    if rc != 0:
        print(f"  [checkpoint] git pull --rebase failed: {err.strip()} — aborting")
        _git("rebase", "--abort")
        return False
    rc, _, err = _git("push")
    if rc != 0:
        print(f"  [checkpoint] git push failed: {err.strip()}")
        return False
    print(f"  [checkpoint] pushed: {message}")
    return True


# ── Phase 1: walk + merge LS records ───────────────────────────────────────


def load_existing_reports() -> dict[str, list[dict]]:
    """Load house → list-of-reports dict.

    Reads the sharded format (reports-meta.json + reports-<house>-<NN>.json
    shards) when present; falls back to legacy single-file reports.json
    on the first run after migration or on a fresh checkout.
    """
    if REPORTS_META_JSON.exists():
        try:
            with open(REPORTS_META_JSON, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"  ! reports-meta.json unreadable ({e}); falling back to legacy reports.json")
        else:
            result: dict[str, list[dict]] = {h: [] for h in HOUSES}
            for house, shard_entries in (meta.get("shards") or {}).items():
                for entry in shard_entries:
                    path = DOCS / entry["file"]
                    if not path.exists():
                        print(f"  ! shard missing on disk: {entry['file']} — skipping")
                        continue
                    with open(path, "r", encoding="utf-8") as f:
                        result.setdefault(house, []).extend(
                            json.load(f).get("records", []))
            return result

    if REPORTS_JSON.exists():
        with open(REPORTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {h: [] for h in HOUSES}


def _reports_bucket(house: str, r: dict) -> str:
    """Bucket for a reports record.

    COARSER than the text/search buckets, and deliberately so. Reports shards
    are read IN FULL at app startup (fetchReports pulls every shard listed in
    reports-meta.json to build the record list), whereas text and search
    shards are fetched one at a time on demand. Many small files are ideal
    for the latter and actively harmful for the former: same bytes, far more
    round trips.

    Bucketing these on the text bucket keyed RS by sitting DATE, which took
    the count to 1,490 (questions) / 1,777 (debates). The app fired them all
    through Promise.all and the browser rejected the burst - every request
    failed with a bare "Failed to fetch", taking questions and debates down
    on 2026-07-27. Keyed by SESSION instead: 121 / 150 files, largest 3.4 MB,
    and a walk still rewrites ~3 MB rather than all 126 MB.
    """
    try:
        if house == "ls":
            return f"ls-LS{int(r['lok_sabha'])}-S{int(r['session'])}"
        return f"rs-RS{int(r['session'])}"
    except (KeyError, TypeError, ValueError):
        return f"{house}-misc"


def _shard_filename(bucket: str) -> str:
    return f"reports-{bucket}.json"


def _write_sharded_reports(reports: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Partition each house's records into SHARD_SIZE-sized shards and write
    them. Cleans up orphan shards from previous runs with more records or
    a different house set.
    """
    for path in DOCS.glob("reports-*.json"):
        if path.name == REPORTS_META_JSON.name:
            continue
        try:
            path.unlink()
        except OSError:
            pass

    # Group by natural bucket, never by position. Slicing `items` meant a
    # new record — which merges in newest-first, i.e. at the FRONT — shifted
    # every later record across a shard boundary, rewriting all 98
    # reports-ls shards on every walk. Grouping keeps completed sessions
    # byte-identical.
    #
    # Bucket ORDER (and hence record order seen by the app) comes from first
    # appearance in `items`, so the canonical newest-first ordering is
    # preserved without storing an index inside any shard — an index in the
    # payload would itself cascade whenever a new session appeared.
    shard_entries: dict[str, list[dict]] = {}
    for house in reports:
        grouped: dict[str, list[dict]] = {}
        for r in reports[house]:
            grouped.setdefault(_reports_bucket(house, r), []).append(r)
        entries: list[dict] = []
        for bucket, chunk in grouped.items():
            fname = _shard_filename(bucket)
            path = DOCS / fname
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "records": chunk,
                    "count": len(chunk),
                    "house": house,
                    "bucket": bucket,
                }, f, ensure_ascii=False, indent=2)
            entries.append({"file": fname, "count": len(chunk)})
        shard_entries[house] = entries
    return shard_entries


def _write_reports_meta(reports: dict[str, list[dict]],
                       shard_entries: dict[str, list[dict]]) -> None:
    meta = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "shard_size": SHARD_SIZE,
        "totals": {h: len(reports.get(h, [])) for h in reports},
        "shards": shard_entries,
    }
    if not write_json_idempotent(REPORTS_META_JSON, meta):
        print("  [skip] reports-meta.json unchanged (besides timestamp)")


def save_reports(reports: dict[str, list[dict]]) -> None:
    """Sort each house's list deterministically and write as sharded files.
    LS sort: lok_sabha desc, session desc, type asc (S before U), qno asc.
    Newest-LS / newest-session first; within a (LS, session, type) bucket,
    ascending qno for stable file IDs.
    """
    out: dict[str, list[dict]] = {}
    for h in HOUSES:
        items = reports.get(h, [])
        if h == "ls":
            items.sort(key=lambda r: (
                -int(r.get("lok_sabha") or 0),
                -int(r.get("session")   or 0),
                str(r.get("type")       or ""),
                int(r.get("question_no") or 0)))
        elif h == "rs":
            # Newest session first, then date desc, type asc.
            items.sort(key=lambda r: (
                -int(r.get("session") or 0),
                # date_iso sorts desc when negated via tuple of ints; we
                # keep a string-desc sort by reversing via .sort() twice.
                str(r.get("type") or ""),
            ))
            items.sort(key=lambda r: str(r.get("date_iso") or ""), reverse=True)
            items.sort(key=lambda r: -int(r.get("session") or 0))
        out[h] = items
    for h, items in reports.items():
        if h not in HOUSES and h not in out:
            out[h] = items

    shard_entries = _write_sharded_reports(out)
    _write_reports_meta(out, shard_entries)

    if REPORTS_JSON.exists():
        try:
            REPORTS_JSON.unlink()
        except OSError as e:
            print(f"  ! could not delete legacy reports.json: {e}")


def _record_from_lsquestion(q: LSQuestion) -> dict:
    return {
        "house":         "ls",
        "lok_sabha":     q.lok_sabha,
        "session":       q.session,
        "question_no":   q.question_no,
        "type":          q.question_type,
        "subject":       q.subject,
        "members":       q.members,
        "ministry":      q.ministry,
        "date":          q.date,
        "pdf_url":       q.pdf_url,
        "pdf_url_hindi": q.pdf_url_hindi,
        "supplementary": q.supplementary,
    }


def _ls_key_tuple(r: dict) -> tuple[int, int, str, int]:
    return (int(r["lok_sabha"]), int(r["session"]),
            str(r["type"]), int(r["question_no"]))


def walk_ls_and_merge(existing: dict[str, list[dict]]
                      ) -> tuple[dict[str, list[dict]], dict]:
    """Walk LS qetFilteredQuestionsAns across LOK_SABHAS × SESSION_RANGE
    × QTYPES, merge with existing records. Fresh wins on conflict;
    entries in un-walked (LS, session, type) tuples are preserved.

    Order: newest LS first (LS-18 → LS-15), descending session within
    each LS, both types per session.

    Early-stop (per bucket): the listing API returns records in a
    stable order within each (LS, session, type) bucket. Once a full
    page yields zero new keys (all already in existing), the rest of
    that bucket is necessarily already-known. Skip it. Saves ~40+
    minutes/run in steady state — leaving the MAX_RUN_SECONDS budget
    free for the extract phase. Updates to records on pages 2+ are
    NOT detected by the truncated walk; set WALK_NO_EARLY_STOP=1 to
    disable early-stop and force a full re-walk (occasional manual
    sweep).
    """
    print(f"[Walk] LS terms {LOK_SABHAS} sessions {SESSION_RANGE} types {QTYPES}...")
    t0 = time.time()
    no_early_stop = os.environ.get("WALK_NO_EARLY_STOP", "").strip() not in ("", "0", "false", "False")
    known_keys: set[tuple[int, int, str, int]] = {
        _ls_key_tuple(r) for r in existing.get("ls", [])
    }
    fresh_ls: dict[tuple[int, int, str, int], dict] = {}
    walked_buckets: set[tuple[int, int, str]] = set()
    buckets_early_stopped = 0
    pages_skipped_total = 0
    page_size = int(os.environ.get("QUESTIONS_LS_PAGE_SIZE", "500"))
    for ls in sorted(LOK_SABHAS, reverse=True):
        for ses in sorted(SESSION_RANGE, reverse=True):
            for qtype in QTYPES:
                walked_buckets.add((ls, ses, qtype))
                # First page also tells us totalPages
                try:
                    page1_recs, total = ls_fetch_page(ls, ses, qtype, page=1, size=page_size)
                except RateLimited:
                    raise
                except Exception as e:
                    print(f"  ERR LS{ls} S{ses} {qtype} page 1: {e}")
                    continue
                if total <= 0:
                    continue
                total_pages = max(1, math.ceil(total / page_size))
                if total_pages == 1:
                    print(f"  LS{ls} S{ses} {qtype}: 1 page, {len(page1_recs)}/{total} records")
                else:
                    print(f"  LS{ls} S{ses} {qtype}: total={total}, pages={total_pages}")
                # Record page 1 — track whether any keys were new vs already-known.
                page1_new = 0
                for rec in page1_recs:
                    key = (rec.lok_sabha, rec.session, rec.question_type, rec.question_no)
                    fresh_ls[key] = _record_from_lsquestion(rec)
                    if key not in known_keys:
                        page1_new += 1
                # Early-stop: if page 1 had zero new keys, the rest of this
                # bucket is already-known (records come in stable order).
                if (not no_early_stop) and page1_recs and page1_new == 0 and total_pages > 1:
                    pages_skipped = total_pages - 1
                    pages_skipped_total += pages_skipped
                    buckets_early_stopped += 1
                    print(f"    early-stop after page 1/{total_pages} "
                          f"({pages_skipped} pages skipped — all-known)")
                    continue
                for page in range(2, total_pages + 1):
                    try:
                        recs, _ = ls_fetch_page(ls, ses, qtype, page=page, size=page_size)
                    except RateLimited:
                        raise
                    except Exception as e:
                        print(f"    ERR page {page}: {e} — stopping this bucket")
                        break
                    page_new = 0
                    for rec in recs:
                        key = (rec.lok_sabha, rec.session, rec.question_type, rec.question_no)
                        fresh_ls[key] = _record_from_lsquestion(rec)
                        if key not in known_keys:
                            page_new += 1
                    if page % 5 == 0 or page == total_pages:
                        print(f"    page {page}/{total_pages} (new={page_new})")
                    if (not no_early_stop) and recs and page_new == 0 and page < total_pages:
                        pages_skipped = total_pages - page
                        pages_skipped_total += pages_skipped
                        buckets_early_stopped += 1
                        print(f"    early-stop after page {page}/{total_pages} "
                              f"({pages_skipped} pages skipped — all-known)")
                        break
    walk_secs = time.time() - t0
    es_note = (f" (early-stop on {buckets_early_stopped} buckets, "
               f"skipped {pages_skipped_total} pages)") if pages_skipped_total else ""
    print(f"  walked in {walk_secs:.1f}s — {len(fresh_ls)} records across "
          f"{len(walked_buckets)} (ls, session, type) buckets{es_note}")

    # Merge: fresh-LS overrides; existing LS entries in NON-walked buckets
    # are preserved; existing LS entries in walked buckets that aren't in
    # fresh are also preserved (archival promise — upstream delisting
    # doesn't drop us).
    merged: dict[str, list[dict]] = {h: list(existing.get(h, [])) for h in HOUSES}
    old_ls_by_key = {_ls_key_tuple(r): r for r in merged.get("ls", [])}
    new_count = 0; updated_count = 0; kept_legacy = 0
    out_ls: list[dict] = []
    combined_keys = set(old_ls_by_key.keys()) | set(fresh_ls.keys())
    for key in combined_keys:
        if key in fresh_ls:
            new_rec = fresh_ls[key]
            if key in old_ls_by_key:
                if old_ls_by_key[key] != new_rec:
                    updated_count += 1
            else:
                new_count += 1
            out_ls.append(new_rec)
        else:
            bucket = (key[0], key[1], key[2])
            if bucket in walked_buckets:
                kept_legacy += 1
            out_ls.append(old_ls_by_key[key])
    merged["ls"] = out_ls
    for h, items in existing.items():
        if h not in HOUSES:
            merged[h] = items
    stats = {"new": new_count, "updated": updated_count, "kept_legacy": kept_legacy}
    total = sum(len(merged.get(h, [])) for h in HOUSES)
    print(f"  merged: {total} total LS records "
          f"(new={new_count}, updated={updated_count}, kept_legacy={kept_legacy})")
    return merged, stats


def _record_from_rsbundle(b: RSQuestionBundle) -> dict:
    return {
        "house":    "rs",
        "session":  b.session,
        "date_iso": b.date_iso,
        "date":     b.date_raw,
        "type":     b.question_type,
        "pdf_url":  b.pdf_url,
        # Synthetic subject used by the app as a display title. Real
        # per-question subjects live inside the PDF body and are not
        # surfaced by the listing API.
        "subject":  f"Rajya Sabha · Session {b.session} · {b.date_raw} · "
                    + ("Starred" if b.question_type == "STARRED" else "Unstarred"),
    }


def _rs_key_tuple(r: dict) -> tuple[int, str, str]:
    return (int(r["session"]), str(r.get("date_iso") or ""), str(r.get("type") or ""))


def walk_rs_and_merge(existing: dict[str, list[dict]]
                      ) -> tuple[dict[str, list[dict]], dict, set[int]]:
    """Walk RS starred + unstarred listings for the requested sessions,
    merge with existing records. Returns (merged_reports, walk_stats,
    walked_sessions_set).
    """
    print(f"[Walk RS] mode={_RS_MODE}, max_sessions={_RS_MAX}, explicit={RS_SESSIONS}...")
    t0 = time.time()
    fresh_rs: dict[tuple[int, str, str], dict] = {}
    walked_sessions: set[int] = set()
    try:
        for rec in walk_rajyasabha(sessions=RS_SESSIONS, max_sessions=_RS_MAX):
            walked_sessions.add(rec.session)
            key = (rec.session, rec.date_iso, rec.question_type)
            if key in fresh_rs:
                continue
            fresh_rs[key] = _record_from_rsbundle(rec)
    except RateLimited:
        raise
    except Exception as e:
        print(f"  ERR walk_rajyasabha: {e}")
    print(f"  walked in {time.time()-t0:.1f}s — {len(fresh_rs)} bundle records "
          f"across {len(walked_sessions)} sessions")

    # Merge: existing RS entries in non-walked sessions preserved;
    # existing RS entries in walked sessions that aren't in fresh also
    # preserved (archival promise — upstream delisting doesn't drop us).
    merged: dict[str, list[dict]] = {h: list(existing.get(h, [])) for h in HOUSES}
    old_rs_by_key = {_rs_key_tuple(r): r for r in merged.get("rs", []) if r.get("date_iso")}
    new_count = 0; updated_count = 0; kept_legacy = 0
    out_rs: list[dict] = []
    combined_keys = set(old_rs_by_key.keys()) | set(fresh_rs.keys())
    for key in combined_keys:
        if key in fresh_rs:
            new_rec = fresh_rs[key]
            if key in old_rs_by_key:
                if old_rs_by_key[key] != new_rec:
                    updated_count += 1
            else:
                new_count += 1
            out_rs.append(new_rec)
        else:
            if key[0] in walked_sessions:
                kept_legacy += 1
            out_rs.append(old_rs_by_key[key])
    merged["rs"] = out_rs
    for h, items in existing.items():
        if h not in HOUSES:
            merged[h] = items
    stats = {"new": new_count, "updated": updated_count, "kept_legacy": kept_legacy}
    print(f"  merged: {len(merged.get('rs', []))} total RS records "
          f"(new={new_count}, updated={updated_count}, kept_legacy={kept_legacy})")
    return merged, stats, walked_sessions


# ── Phase 2: extract missing bodies ───────────────────────────────────────


def _check_cooldown_and_skip() -> bool:
    if not META_JSON.exists():
        return False
    try:
        with open(META_JSON, "r", encoding="utf-8") as f:
            prev = json.load(f)
    except Exception:
        return False
    if not prev.get("rate_limited"):
        return False
    last_at = prev.get("rate_limited_at")
    if not last_at:
        return False
    try:
        last_dt = datetime.fromisoformat(last_at.replace("Z", "+00:00"))
    except Exception:
        return False
    elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
    if elapsed < RATE_LIMIT_COOLDOWN_SECONDS:
        print(f"  COOLDOWN: previous run rate-limited {elapsed:.0f}s ago; skipping (limit {RATE_LIMIT_COOLDOWN_SECONDS}s)")
        return True
    return False


def extract_missing_bodies(reports: dict[str, list[dict]], *, deadline: float) -> dict:
    """For each LS record without a `.txt` and without a marker, download
    the per-question PDF, run pypdf, save text. Bounded by
    MAX_EXTRACTIONS_PER_RUN.
    """
    if _check_cooldown_and_skip():
        return {"extracted": [], "failed": [], "rate_limited": True,
                "budget_hit": False, "skipped_due_to_cooldown": True,
                "candidates_total": 0}

    # Bundled-records skip: the text shards are the
    # source of truth post-bundling. LS composite key matches the build
    # adapter: `ls|<file_id>`.
    bundled_ids: set = set()
    texts_meta_path = DOCS / "texts-meta.json"
    if texts_meta_path.exists():
        try:
            with open(texts_meta_path, "r", encoding="utf-8") as f:
                texts_meta = json.load(f)
            bundled_ids = load_bundled_ids(DOCS)
        except (OSError, json.JSONDecodeError) as e:
            print(f"  ! couldn't read texts-meta.json — proceeding without shard skip ({e})")

    bundled_markers = load_markers(DOCS)
    ls_text_dir = TEXT_DIR / "ls"
    candidates: list[tuple[int, int, str, int, str]] = []  # +pdf_url
    skipped_marked = 0
    skipped_bundled = 0
    for r in reports.get("ls", []):
        ls = r.get("lok_sabha"); ses = r.get("session")
        qtype = r.get("type"); qno = r.get("question_no")
        pdf_url = r.get("pdf_url") or ""
        if (ls is None or ses is None or qno is None
                or not qtype or not pdf_url):
            continue
        fid = ls_file_id(int(ls), int(ses), qtype, int(qno))
        cid = f"ls|{fid}"
        if cid in bundled_ids:
            skipped_bundled += 1; continue
        bm = bundled_markers.get(cid)
        if bm in ("pypdf-empty", "ocr-failed"):
            skipped_marked += 1; continue
        if (ls_text_dir / f"{fid}.txt").exists():
            skipped_marked += 1; continue
        if (ls_text_dir / f"{fid}.pypdf-empty").exists():
            skipped_marked += 1; continue
        if (ls_text_dir / f"{fid}.ocr-failed").exists():
            skipped_marked += 1; continue
        # NB: .pypdf-error markers are retryable — fall through.
        candidates.append((int(ls), int(ses), qtype, int(qno), pdf_url))

    # Priority: newest LS first, then highest session, STARRED before
    # UNSTARRED (less plentiful, easier to clear), then highest qno.
    candidates.sort(key=lambda c: (-c[0], -c[1],
                                    0 if c[2] == "STARRED" else 1,
                                    -c[3]))
    print(f"  candidates: {len(candidates)} LS records missing text "
          f"({skipped_marked} skipped — already marked; "
          f"{skipped_bundled} skipped — already in shards)")

    if not candidates:
        return {"extracted": [], "failed": [], "rate_limited": False,
                "budget_hit": False, "candidates_total": 0}

    remaining = deadline - time.monotonic()
    if remaining <= 60:
        print(f"  only {remaining:.0f}s left in budget — skipping extract phase")
        return {"extracted": [], "failed": [], "rate_limited": False,
                "budget_hit": True, "candidates_total": len(candidates)}

    target = candidates[:MAX_EXTRACTIONS_PER_RUN]
    print(f"  budget: extracting up to {len(target)} this run "
          f"(MAX_EXTRACTIONS_PER_RUN={MAX_EXTRACTIONS_PER_RUN}, "
          f"remaining={remaining:.0f}s, EXTRACT_WORKERS={EXTRACT_WORKERS})")

    extracted: list[tuple[int, int, str, int]] = []
    failed: list[tuple[int, int, str, int]] = []
    rate_limited = False; budget_hit = False
    last_checkpoint_at = time.monotonic()
    extracted_since_checkpoint = 0

    def _do(c):
        ls, ses, qt, qno, url = c
        try:
            text = ls_fetch_and_extract(
                url, loksabha=ls, session=ses, qtype=qt,
                question_no=qno, text_dir=str(ls_text_dir),
                pdfs_dir=str(PDFS_DIR / "ls"))
            return (ls, ses, qt, qno), text, None
        except RateLimited as rl:
            return (ls, ses, qt, qno), None, rl
        except Exception as e:
            return (ls, ses, qt, qno), None, e

    with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as ex:
        futures = {ex.submit(_do, c): c for c in target}
        for fut in as_completed(futures):
            c, text, err = fut.result()
            if isinstance(err, RateLimited):
                print(f"  [RATE-LIMITED] LS{c[0]} S{c[1]} {c[2]} #{c[3]}: {err}")
                rate_limited = True
                for f in futures:
                    if not f.done():
                        f.cancel()
                break
            elif text:
                extracted.append(c)
                extracted_since_checkpoint += 1
            else:
                failed.append(c)
                extracted_since_checkpoint += 1
            now = time.monotonic()
            if (extracted_since_checkpoint >= CHECKPOINT_EVERY_N or
                (extracted_since_checkpoint > 0 and now - last_checkpoint_at >= CHECKPOINT_EVERY_S)):
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
                checkpoint_commit(
                    f"Auto-checkpoint questions primary data (extracted={len(extracted)} this run) [{ts}]",
                    ["docs/questions/"],
                )
                extracted_since_checkpoint = 0
                last_checkpoint_at = now
            if now > deadline:
                print(f"  [BUDGET] wall-clock budget hit after {len(extracted)} successes")
                budget_hit = True
                for f in futures:
                    if not f.done():
                        f.cancel()
                break

    if extracted_since_checkpoint > 0:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        checkpoint_commit(
            f"Auto-checkpoint questions primary data (final, extracted={len(extracted)} this run) [{ts}]",
            ["docs/questions/"],
        )

    return {
        "extracted": extracted, "failed": failed,
        "rate_limited": rate_limited, "budget_hit": budget_hit,
        "candidates_total": len(candidates),
    }


def extract_missing_rs_bodies(reports: dict[str, list[dict]], *, deadline: float) -> dict:
    """RS analogue of extract_missing_bodies. Each RS record is one
    sitting-day bundle PDF; we download, run pypdf, save as text. Bounded
    by MAX_RS_EXTRACTIONS_PER_RUN.
    """
    if _check_cooldown_and_skip():
        return {"extracted": [], "failed": [], "rate_limited": True,
                "budget_hit": False, "skipped_due_to_cooldown": True,
                "candidates_total": 0}

    bundled_ids: set = set()
    texts_meta_path = DOCS / "texts-meta.json"
    if texts_meta_path.exists():
        try:
            with open(texts_meta_path, "r", encoding="utf-8") as f:
                texts_meta = json.load(f)
            bundled_ids = load_bundled_ids(DOCS)
        except (OSError, json.JSONDecodeError):
            pass

    bundled_markers = load_markers(DOCS)
    rs_text_dir = TEXT_DIR / "rs"
    candidates: list[tuple[int, str, str, str]] = []   # session, date_iso, qtype, pdf_url
    skipped_marked = 0
    skipped_bundled = 0
    for r in reports.get("rs", []):
        ses = r.get("session"); iso = r.get("date_iso")
        qtype = r.get("type"); pdf_url = r.get("pdf_url") or ""
        if ses is None or not iso or not qtype or not pdf_url:
            continue
        fid = rs_file_id(int(ses), iso, qtype)
        cid = f"rs|{fid}"
        if cid in bundled_ids:
            skipped_bundled += 1; continue
        bm = bundled_markers.get(cid)
        if bm in ("pypdf-empty", "ocr-failed"):
            skipped_marked += 1; continue
        if (rs_text_dir / f"{fid}.txt").exists():
            skipped_marked += 1; continue
        if (rs_text_dir / f"{fid}.pypdf-empty").exists():
            skipped_marked += 1; continue
        if (rs_text_dir / f"{fid}.ocr-failed").exists():
            skipped_marked += 1; continue
        # .pypdf-error markers are retryable — fall through.
        candidates.append((int(ses), iso, qtype, pdf_url))

    # Newest session first, then date desc, STARRED before UNSTARRED.
    candidates.sort(key=lambda c: (-c[0], c[1], 0 if c[2] == "STARRED" else 1), reverse=False)
    # Make date_iso sort desc by reversing string compare result via tuple inversion.
    # Simpler: do two passes (stable sort).
    candidates.sort(key=lambda c: c[1], reverse=True)
    candidates.sort(key=lambda c: -c[0])
    print(f"  RS candidates: {len(candidates)} bundles missing text "
          f"({skipped_marked} skipped — already marked; "
          f"{skipped_bundled} skipped — already in shards)")

    if not candidates:
        return {"extracted": [], "failed": [], "rate_limited": False,
                "budget_hit": False, "candidates_total": 0}

    remaining = deadline - time.monotonic()
    if remaining <= 60:
        print(f"  only {remaining:.0f}s left in budget — skipping RS extract")
        return {"extracted": [], "failed": [], "rate_limited": False,
                "budget_hit": True, "candidates_total": len(candidates)}

    target = candidates[:MAX_RS_EXTRACTIONS_PER_RUN]
    print(f"  RS budget: extracting up to {len(target)} this run "
          f"(MAX_RS_EXTRACTIONS_PER_RUN={MAX_RS_EXTRACTIONS_PER_RUN}, "
          f"remaining={remaining:.0f}s, EXTRACT_WORKERS={EXTRACT_WORKERS})")

    extracted: list[tuple[int, str, str]] = []
    failed: list[tuple[int, str, str]] = []
    rate_limited = False; budget_hit = False
    last_checkpoint_at = time.monotonic()
    extracted_since_checkpoint = 0

    def _do(c):
        ses, iso, qt, url = c
        try:
            text = rs_fetch_and_extract(
                url, session=ses, date_iso=iso, qtype=qt,
                text_dir=str(rs_text_dir),
                pdfs_dir=str(PDFS_DIR / "rs"))
            return (ses, iso, qt), text, None
        except RateLimited as rl:
            return (ses, iso, qt), None, rl
        except Exception as e:
            return (ses, iso, qt), None, e

    with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as ex:
        futures = {ex.submit(_do, c): c for c in target}
        for fut in as_completed(futures):
            c, text, err = fut.result()
            if isinstance(err, RateLimited):
                print(f"  [RATE-LIMITED] RS{c[0]} {c[1]} {c[2]}: {err}")
                rate_limited = True
                for f in futures:
                    if not f.done():
                        f.cancel()
                break
            elif text:
                extracted.append(c)
                extracted_since_checkpoint += 1
            else:
                failed.append(c)
                extracted_since_checkpoint += 1
            now = time.monotonic()
            if (extracted_since_checkpoint >= CHECKPOINT_EVERY_N or
                (extracted_since_checkpoint > 0 and now - last_checkpoint_at >= CHECKPOINT_EVERY_S)):
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
                checkpoint_commit(
                    f"Auto-checkpoint questions RS primary data (extracted={len(extracted)} this run) [{ts}]",
                    ["docs/questions/"],
                )
                extracted_since_checkpoint = 0
                last_checkpoint_at = now
            if now > deadline:
                print(f"  [BUDGET] wall-clock budget hit after {len(extracted)} RS successes")
                budget_hit = True
                for f in futures:
                    if not f.done():
                        f.cancel()
                break

    if extracted_since_checkpoint > 0:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        checkpoint_commit(
            f"Auto-checkpoint questions RS primary data (final, extracted={len(extracted)} this run) [{ts}]",
            ["docs/questions/"],
        )

    return {
        "extracted": extracted, "failed": failed,
        "rate_limited": rate_limited, "budget_hit": budget_hit,
        "candidates_total": len(candidates),
    }


# ── Phase 3: derived files ─────────────────────────────────────────────────


def build_manifest() -> dict:
    """House-keyed manifest. Entries: texts.<house>[<file_id>] = {size, url}.

    After bundling (write_text_shards), text/<house>/ is empty of *.txt
    files, so the manifest from this scan reflects only transient
    in-flight content between extract and derive. The canonical
    record-to-shard map lives in texts-meta.json.
    """
    out: dict[str, dict] = {}
    if not TEXT_DIR.exists():
        return {"texts": out}
    for house in ("ls", "rs"):
        hdir = TEXT_DIR / house
        if not hdir.exists():
            continue
        out[house] = {}
        for text_file in sorted(hdir.glob("*.txt")):
            fid = text_file.stem
            out[house][fid] = {
                "size": text_file.stat().st_size,
                "url":  f"text/{house}/{text_file.name}",
            }
    return {"texts": out}


def compute_audit(reports: dict[str, list[dict]]) -> dict:
    """Per-record status for both houses. Each record contributes 1."""
    counts = {
        "ls_records":                 len(reports.get("ls", [])),
        "ls_with_text":               0,
        "ls_pypdf_empty_awaiting_ocr": 0,
        "ls_pypdf_error_retryable":   0,
        "ls_ocr_failed_permanent":    0,
        "ls_never_attempted":         0,
        "rs_records":                 len(reports.get("rs", [])),
        "rs_with_text":               0,
        "rs_pypdf_empty_awaiting_ocr": 0,
        "rs_pypdf_error_retryable":   0,
        "rs_ocr_failed_permanent":    0,
        "rs_never_attempted":         0,
    }
    # No early-return when TEXT_DIR doesn't exist — bundled records
    # still need to be counted. The marker-file checks below
    # (Path.exists()) safely return False when TEXT_DIR is missing,
    # so the audit just classifies non-bundled records as
    # never_attempted as expected.

    bundled_ids: set = set()
    texts_meta_path = DOCS / "texts-meta.json"
    if texts_meta_path.exists():
        try:
            with open(texts_meta_path, "r", encoding="utf-8") as f:
                texts_meta = json.load(f)
            bundled_ids = load_bundled_ids(DOCS)
        except (OSError, json.JSONDecodeError):
            pass
    markers = load_markers(DOCS)

    ls_dir = TEXT_DIR / "ls"
    for r in reports.get("ls", []):
        ls = r.get("lok_sabha"); ses = r.get("session")
        qtype = r.get("type"); qno = r.get("question_no")
        if ls is None or ses is None or qno is None or not qtype:
            continue
        fid = ls_file_id(int(ls), int(ses), qtype, int(qno))
        cid = f"ls|{fid}"
        if cid in bundled_ids:
            counts["ls_with_text"] += 1
        elif (ls_dir / f"{fid}.txt").exists():
            counts["ls_with_text"] += 1
        elif cid in markers:
            mt = markers[cid]
            if mt == "ocr-failed":   counts["ls_ocr_failed_permanent"] += 1
            elif mt == "pypdf-empty": counts["ls_pypdf_empty_awaiting_ocr"] += 1
            elif mt == "pypdf-error": counts["ls_pypdf_error_retryable"] += 1
            else: counts["ls_never_attempted"] += 1
        elif (ls_dir / f"{fid}.pypdf-empty").exists():
            counts["ls_pypdf_empty_awaiting_ocr"] += 1
        elif (ls_dir / f"{fid}.pypdf-error").exists():
            counts["ls_pypdf_error_retryable"] += 1
        elif (ls_dir / f"{fid}.ocr-failed").exists():
            counts["ls_ocr_failed_permanent"] += 1
        else:
            counts["ls_never_attempted"] += 1

    rs_dir = TEXT_DIR / "rs"
    for r in reports.get("rs", []):
        ses = r.get("session"); iso = r.get("date_iso")
        qtype = r.get("type")
        if ses is None or not iso or not qtype:
            continue
        fid = rs_file_id(int(ses), iso, qtype)
        cid = f"rs|{fid}"
        if cid in bundled_ids:
            counts["rs_with_text"] += 1
        elif (rs_dir / f"{fid}.txt").exists():
            counts["rs_with_text"] += 1
        elif cid in markers:
            mt = markers[cid]
            if mt == "ocr-failed":   counts["rs_ocr_failed_permanent"] += 1
            elif mt == "pypdf-empty": counts["rs_pypdf_empty_awaiting_ocr"] += 1
            elif mt == "pypdf-error": counts["rs_pypdf_error_retryable"] += 1
            else: counts["rs_never_attempted"] += 1
        elif (rs_dir / f"{fid}.pypdf-empty").exists():
            counts["rs_pypdf_empty_awaiting_ocr"] += 1
        elif (rs_dir / f"{fid}.pypdf-error").exists():
            counts["rs_pypdf_error_retryable"] += 1
        elif (rs_dir / f"{fid}.ocr-failed").exists():
            counts["rs_ocr_failed_permanent"] += 1
        else:
            counts["rs_never_attempted"] += 1
    return {
        "audited_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totals":     counts,
    }


# ── Search bundle + index (same sharding as debates) ───────────────────────

DOCS_PER_SHARD = 2500


def _load_bundled_texts() -> dict[str, dict[str, str]]:
    """Return {house → {file_id → text}} for every record currently in
    texts-NN.json shards. Reads texts-meta.json for the shard list, then
    each shard's `records` dict (composite_id `<house>|<fid>` → text).

    Records whose value is an R2 sentinel (dict, not str) are skipped —
    we'd need to fetch from R2 to surface them in the search bundle.
    """
    out: dict[str, dict[str, str]] = {"ls": {}, "rs": {}}
    texts_meta_path = DOCS / "texts-meta.json"
    if not texts_meta_path.exists():
        return out
    try:
        with open(texts_meta_path, "r", encoding="utf-8") as f:
            texts_meta = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"  ! couldn't read texts-meta.json ({e})")
        return out
    for shard_entry in (texts_meta.get("shards") or []):
        sp = DOCS / shard_entry["file"]
        if not sp.exists():
            continue
        try:
            with open(sp, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        for k, v in (payload.get("records") or {}).items():
            if not isinstance(v, str):
                continue
            house_pref, _, fid = k.partition("|")
            if house_pref in out and fid:
                out[house_pref][fid] = v
    return out


def _delete_legacy(path: Path) -> None:
    if path.exists():
        path.unlink()


def build_search_bundle(reports: dict[str, list[dict]],
                        docs_per_shard: int = DOCS_PER_SHARD) -> dict | None:
    """Title + first 5K chars per record. Sharded by sorted reportKey.
    LS: one entry per question record. "Title" here is `subject`.
    """
    if not TEXT_DIR.exists():
        return None
    HEAD = 5000
    entries = []
    truncated = 0

    bundled = _load_bundled_texts()

    ls_dir = TEXT_DIR / "ls"
    for r in reports.get("ls", []):
        ls = r.get("lok_sabha"); ses = r.get("session")
        qtype = r.get("type"); qno = r.get("question_no")
        if ls is None or ses is None or qno is None or not qtype:
            continue
        fid = ls_file_id(int(ls), int(ses), qtype, int(qno))
        text: Optional[str] = None
        if fid in bundled["ls"]:
            text = bundled["ls"][fid]
        else:
            text_path = ls_dir / f"{fid}.txt"
            if not text_path.exists():
                continue
            try:
                text = text_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
        head = (text or "")[:HEAD]
        if text and len(text) > HEAD:
            truncated += 1
        entries.append({
            "k":    ls_report_key(int(ls), int(ses), qtype, int(qno)),
            "t":    r.get("subject", ""),
            "head": head,
            "_g":   shard_group(f"ls|{fid}", SEARCH_SHARD_STRIDE),
        })

    rs_dir = TEXT_DIR / "rs"
    for r in reports.get("rs", []):
        ses = r.get("session"); iso = r.get("date_iso")
        qtype = r.get("type")
        if ses is None or not iso or not qtype:
            continue
        fid = rs_file_id(int(ses), iso, qtype)
        text: Optional[str] = None
        if fid in bundled["rs"]:
            text = bundled["rs"][fid]
        else:
            text_path = rs_dir / f"{fid}.txt"
            if not text_path.exists():
                continue
            try:
                text = text_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
        head = (text or "")[:HEAD]
        if text and len(text) > HEAD:
            truncated += 1
        entries.append({
            "k":    rs_report_key(int(ses), iso, qtype),
            "t":    r.get("subject", ""),
            "head": head,
            "_g":   shard_group(f"rs|{fid}", SEARCH_SHARD_STRIDE),
        })

    if not entries:
        return None
    _delete_legacy(DOCS / "search-bundle.json")
    # Clean orphan shards
    for path in DOCS.glob("search-bundle-*.json"):
        try: path.unlink()
        except OSError: pass

    # Partition by shard group, never by position in a global sort. Slicing
    # a sorted list (`entries[i*n:(i+1)*n]`) meant one new question shifted
    # every later entry across a shard boundary, so all 52 shards (~520 MB)
    # were rewritten on every hourly run. Grouping on each entry's own key
    # keeps completed sessions byte-identical forever.
    grouped: dict[str, list[dict]] = {}
    for e in entries:
        grouped.setdefault(e.pop("_g"), []).append(e)

    shard_sizes: dict[str, int] = {}
    max_shard = 0; total_bytes = 0
    for g in sorted(grouped):
        chunk = sorted(grouped[g], key=lambda e: e["k"])
        fname = f"search-bundle-{g}.json"
        path = DOCS / fname
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"version": "2.0", "head_chars": HEAD,
                       "group": g, "entries": chunk}, f, ensure_ascii=False)
        sz = path.stat().st_size
        shard_sizes[fname] = sz
        max_shard = max(max_shard, sz); total_bytes += sz
    return {
        "shard_count":     len(grouped),
        "shards":          list(shard_sizes.keys()),
        "shard_sizes":     shard_sizes,
        "total":           len(entries),
        "truncated":       truncated,
        "head_chars":      HEAD,
        "size_bytes":      total_bytes,
        "max_shard_bytes": max_shard,
    }


_TOKEN_RE = re.compile(r"[a-z0-9]{3,}")


def _tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def build_search_index(reports: dict[str, list[dict]],
                       docs_per_shard: int = DOCS_PER_SHARD) -> dict | None:
    """Full-body inverted token index, sharded.

    Builds (k -> token_set) sorted by k, then partitions into shards of
    docs_per_shard. Each shard's index is a token → list[doc_index] map
    where doc_index is relative to the shard's k-order.
    """
    if not TEXT_DIR.exists():
        return None

    # Build (fid, k, text) tuples in deterministic k-order, using bundled
    # shards when present + falling back to text/ls/<fid>.txt.
    docs: list[tuple[str, str, str]] = []

    bundled = _load_bundled_texts()

    ls_dir = TEXT_DIR / "ls"
    for r in reports.get("ls", []):
        ls = r.get("lok_sabha"); ses = r.get("session")
        qtype = r.get("type"); qno = r.get("question_no")
        if ls is None or ses is None or qno is None or not qtype:
            continue
        fid = ls_file_id(int(ls), int(ses), qtype, int(qno))
        k = ls_report_key(int(ls), int(ses), qtype, int(qno))
        text: Optional[str] = None
        if fid in bundled["ls"]:
            text = bundled["ls"][fid]
        else:
            text_path = ls_dir / f"{fid}.txt"
            if not text_path.exists(): continue
            try:
                text = text_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
        if text:
            docs.append((fid, k, text, shard_group(f"ls|{fid}", SEARCH_SHARD_STRIDE)))

    rs_dir = TEXT_DIR / "rs"
    for r in reports.get("rs", []):
        ses = r.get("session"); iso = r.get("date_iso")
        qtype = r.get("type")
        if ses is None or not iso or not qtype:
            continue
        fid = rs_file_id(int(ses), iso, qtype)
        k = rs_report_key(int(ses), iso, qtype)
        text: Optional[str] = None
        if fid in bundled["rs"]:
            text = bundled["rs"][fid]
        else:
            text_path = rs_dir / f"{fid}.txt"
            if not text_path.exists(): continue
            try:
                text = text_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
        if text:
            docs.append((fid, k, text, shard_group(f"rs|{fid}", SEARCH_SHARD_STRIDE)))

    if not docs:
        return None

    _delete_legacy(DOCS / "search-index.json")
    for path in DOCS.glob("search-index-*.json"):
        try: path.unlink()
        except OSError: pass

    # Same partitioning as the bundle and the text shards. Each shard is
    # self-contained (its own `keys` list, postings indexed relative to it),
    # so grouping by key rather than by position needs no format change and
    # no app change — disk-sync.js reads these as a filename list from meta.
    grouped: dict[str, list[tuple]] = {}
    for d in docs:
        grouped.setdefault(d[3], []).append(d)

    shard_sizes: dict[str, int] = {}
    max_shard = 0; total_bytes = 0
    for g in sorted(grouped):
        chunk = sorted(grouped[g], key=lambda d: d[1])
        index: dict[str, list[int]] = {}
        keys: list[str] = []
        for di, (fid, k, text, _g) in enumerate(chunk):
            keys.append(k)
            seen: set[str] = set()
            for tok in _tokenize(text):
                if tok in seen: continue
                seen.add(tok)
                index.setdefault(tok, []).append(di)
        # Sort each posting list (already in di order, but be safe)
        for tok in index:
            index[tok].sort()
        fname = f"search-index-{g}.json"
        path = DOCS / fname
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "version": "2.0",
                "group":   g,
                "keys":    keys,
                "index":   index,
            }, f, ensure_ascii=False)
        sz = path.stat().st_size
        shard_sizes[fname] = sz
        max_shard = max(max_shard, sz); total_bytes += sz
    return {
        "shard_count":     len(grouped),
        "shards":          list(shard_sizes.keys()),
        "shard_sizes":     shard_sizes,
        "total":           len(docs),
        "size_bytes":      total_bytes,
        "max_shard_bytes": max_shard,
    }


# ── Phase orchestration ───────────────────────────────────────────────────


def phase_walk() -> int:
    """Walk + merge. Writes primary report shards. No body extraction.

    Split off from phase_extract so a slower-cadence walk workflow can
    discover new records without consuming the hourly extract budget.
    """
    t_start = time.monotonic()

    print(f"[phase_walk] run start: ls={LOK_SABHAS}, "
          f"sessions={list(SESSION_RANGE)}, types={QTYPES}, "
          f"rs_mode={_RS_MODE}")

    existing = load_existing_reports()
    print(f"  existing: " + ", ".join(f"{h}={len(existing.get(h, []))}" for h in HOUSES))

    rate_limited = False
    ls_walk_stats: dict = {}
    rs_walk_stats: dict = {}
    merged = existing

    try:
        merged, ls_walk_stats = walk_ls_and_merge(merged)
    except RateLimited as rl:
        print(f"  [RATE-LIMITED during LS walk] {rl}")
        rate_limited = True

    if not rate_limited:
        try:
            merged, rs_walk_stats, _walked_rs = walk_rs_and_merge(merged)
        except RateLimited as rl:
            print(f"  [RATE-LIMITED during RS walk] {rl}")
            rate_limited = True

    save_reports(merged)

    meta = {
        "phase":         "walk",
        "generated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elapsed_s":     round(time.monotonic() - t_start, 1),
        "ls_walk_stats": ls_walk_stats,
        "rs_walk_stats": rs_walk_stats,
        "rate_limited":  rate_limited,
    }
    if rate_limited:
        meta["rate_limited_at"] = meta["generated_at"]
    write_json_idempotent(META_JSON, meta, ignore_keys=())

    print(f"[phase_walk] done in {meta['elapsed_s']}s: "
          f"ls_new={ls_walk_stats.get('new', 0)}, "
          f"rs_new={rs_walk_stats.get('new', 0)}, "
          f"rate_limited={rate_limited}")
    return 0 if not rate_limited else 1


def phase_extract() -> int:
    """Extract missing bodies from records already in the report shards.

    Does NOT walk — walks are owned by phase_walk(). Run in the same job
    as phase_derive() so the transient text/ files (gitignored) can be
    bundled into committed text shards before the runner is destroyed.
    """
    t_start = time.monotonic()
    deadline = t_start + MAX_RUN_SECONDS

    print(f"[phase_extract] run start: "
          f"budget=LS:{MAX_EXTRACTIONS_PER_RUN}+RS:{MAX_RS_EXTRACTIONS_PER_RUN}/{MAX_RUN_SECONDS}s")

    merged = load_existing_reports()
    print(f"  existing: " + ", ".join(f"{h}={len(merged.get(h, []))}" for h in HOUSES))

    rate_limited = False

    ls_extract: dict = {"extracted": [], "failed": [], "rate_limited": False,
                        "budget_hit": False, "candidates_total": 0}
    try:
        ls_extract = extract_missing_bodies(merged, deadline=deadline)
        rate_limited = ls_extract.get("rate_limited", False)
    except RateLimited as rl:
        print(f"  [RATE-LIMITED during LS extract] {rl}")
        rate_limited = True

    rs_extract: dict = {"extracted": [], "failed": [], "rate_limited": False,
                        "budget_hit": False, "candidates_total": 0}
    if not rate_limited:
        try:
            rs_extract = extract_missing_rs_bodies(merged, deadline=deadline)
            rate_limited = rs_extract.get("rate_limited", False)
        except RateLimited as rl:
            print(f"  [RATE-LIMITED during RS extract] {rl}")
            rate_limited = True

    meta = {
        "phase":         "extract",
        "generated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elapsed_s":     round(time.monotonic() - t_start, 1),
        "ls_extracted":  len(ls_extract.get("extracted", [])),
        "ls_failed":     len(ls_extract.get("failed", [])),
        "ls_candidates": ls_extract.get("candidates_total", 0),
        "rs_extracted":  len(rs_extract.get("extracted", [])),
        "rs_failed":     len(rs_extract.get("failed", [])),
        "rs_candidates": rs_extract.get("candidates_total", 0),
        "budget":        {"max_ls_extractions": MAX_EXTRACTIONS_PER_RUN,
                          "max_rs_extractions": MAX_RS_EXTRACTIONS_PER_RUN,
                          "max_run_seconds":    MAX_RUN_SECONDS},
        "rate_limited":  rate_limited,
    }
    if rate_limited:
        meta["rate_limited_at"] = meta["generated_at"]
    # Non-idempotent on purpose: always bump generated_at so the app's
    # staleness indicator reflects "last successful run" rather than
    # "last data change" — see write_meta() docstring in build_debates.py
    # for the full rationale.
    write_json_idempotent(META_JSON, meta, ignore_keys=())

    print(f"[phase_extract] done in {meta['elapsed_s']}s: "
          f"ls_extracted={meta['ls_extracted']}, "
          f"rs_extracted={meta['rs_extracted']}, "
          f"rate_limited={rate_limited}")
    return 0 if not rate_limited else 1


def _derive_would_be_destructive() -> bool:
    """True when a derive right now would rebuild manifest/audit from nothing.

    phase_derive scans docs/<corpus>/text/*.txt. That directory is gitignored
    and write_text_shards deletes it after bundling, so a derive with an empty
    scan produces empty manifest/audit and overwrites committed data with
    zeros. Observed 2026-07-27: questions manifest 40,443 -> 47 bytes, debates
    audit with_text 52,251 -> 0.

    This must SKIP, not abort. An extract run that finds nothing to do also
    leaves text/ empty, and that is a completely normal steady state - debates
    LS is already 91% extracted and RS 99.9%. Aborting would turn "nothing new
    today" into a red build on every cycle once backfill completes.

    Set ALLOW_EMPTY_DERIVE=1 to force the rebuild anyway.
    """
    if os.environ.get("ALLOW_EMPTY_DERIVE") == "1":
        return False
    if TEXT_DIR.exists() and any(TEXT_DIR.rglob("*.txt")):
        return False
    meta_path = DOCS / "texts-meta.json"
    if not meta_path.exists():
        return False
    try:
        bundled = json.loads(meta_path.read_text()).get("totals", {}).get("records_with_text", 0)
    except (OSError, json.JSONDecodeError):
        return False
    if bundled > 0:
        print(f"  [derive] text/ is empty but texts-meta.json reports {bundled} bundled "
              f"records — preserving committed manifest/audit instead of rebuilding "
              f"them from an empty scan.")
        return True
    return False


def phase_derive() -> int:
    """Derived files: manifest, audit, search bundle, search index, +
    re-emit the bundled text shards via parliamentwatch_text_shards.
    """
    t_start = time.monotonic()
    print("[phase_derive] start")

    reports = load_existing_reports()
    print(f"  reports loaded: " + ", ".join(f"{h}={len(reports.get(h, []))}" for h in HOUSES))

    if _derive_would_be_destructive():
        print("  [skip] manifest.json preserved (empty text/ scan on a bundled corpus)")
    else:
        manifest = build_manifest()
        if not write_json_idempotent(MANIFEST_JSON, manifest):
            print("  [skip] manifest.json unchanged")

    audit = compute_audit(reports)
    if not write_json_idempotent(AUDIT_JSON, audit):
        print("  [skip] audit.json unchanged (besides timestamp)")

    # Bundle texts into texts-<NN>.json shards (the post-2026-05-14 model).
    # write_text_shards preserves existing shard contents (`668082f5`
    # semantics) and itself removes per-record .txt files after bundling,
    # so no explicit cleanup needed here.
    items: list[tuple[str, Path]] = []
    ls_dir = TEXT_DIR / "ls"
    if ls_dir.exists():
        for path in sorted(ls_dir.glob("*.txt")):
            items.append((f"ls|{path.stem}", path))
    rs_dir = TEXT_DIR / "rs"
    if rs_dir.exists():
        for path in sorted(rs_dir.glob("*.txt")):
            items.append((f"rs|{path.stem}", path))
    text_meta = write_text_shards(DOCS, items)
    print(f"  bundled text shards: {text_meta['totals']}")

    # Consolidate per-record marker sidecars into markers.json. Questions
    # nested layout: text/ls/<fid>.<suffix>, text/rs/<fid>.<suffix>.
    # Composite id == "<house>|<fid>".
    bundled_ids = load_bundled_ids(DOCS)
    marker_stats = consolidate_markers(
        DOCS, TEXT_DIR,
        composite_id_from_path=lambda p: f"{p.parent.name}|{p.stem}",
        drop_record_ids=bundled_ids,
    )
    print(f"  markers: {marker_stats['totals']} consolidated "
          f"(removed {marker_stats.get('removed_sidecar_count', 0)} sidecars)")

    bundle = build_search_bundle(reports)
    index  = build_search_index(reports)

    meta = {
        "phase":         "derive",
        "generated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elapsed_s":     round(time.monotonic() - t_start, 1),
        "totals":        {h: len(reports.get(h, [])) for h in HOUSES},
        "search_bundle": bundle,
        "search_index":  index,
        "audit":         audit["totals"],
        "text_shards":   text_meta["totals"],
    }
    # Non-idempotent on purpose — see extract-phase comment above.
    write_json_idempotent(META_JSON, meta, ignore_keys=())

    print(f"[phase_derive] done in {meta['elapsed_s']}s")
    return 0


def main() -> int:
    phase = os.environ.get("BUILD_PHASE", "extract").strip().lower()
    if phase == "walk":
        return phase_walk()
    elif phase == "extract":
        return phase_extract()
    elif phase == "derive":
        return phase_derive()
    elif phase == "all":
        # all = walk → extract → derive. Used for one-off manual triggers.
        # The cron-driven path runs walk in a separate workflow (every 6h)
        # and extract+derive together in the hourly workflow.
        rc = phase_walk()
        if rc != 0:
            print(f"  [main] walk failed (rc={rc}); skipping extract + derive")
            return rc
        rc = phase_extract()
        if rc != 0:
            print(f"  [main] extract failed (rc={rc}); skipping derive")
            return rc
        return phase_derive()
    else:
        print(f"unknown BUILD_PHASE={phase!r} (walk|extract|derive|all)")
        return 2


if __name__ == "__main__":
    sys.exit(main())
