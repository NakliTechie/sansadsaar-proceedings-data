#!/usr/bin/env python3
"""Debates corpus builder — orchestrator.

Per scheduled (or workflow_dispatch) run:
  1. Walk the LS debate-search API for each LS term in LOK_SABHAS,
     newest-first. Merge metadata into reports.json under the "ls" key.
  2. For each record missing extracted text, call debate-details and
     persist the stripped-HTML body as text/ls/<file_id>.txt.
  3. (Derive phase) Build manifest + sharded bundle + sharded index +
     meta + audit.

Output goes under docs/debates/, served at
sansadsaar-data.naklitechie.com/debates/.

PHASE A scope: LS only. Phase B (Rajya Sabha) extends this orchestrator
with a parallel call into debates/scrapers/rajyasabha.py — the structure
here (house-keyed reports.json, house-stratified text/ subdirs) is
designed so the RS addition is strictly additive, no refactor needed.

Storage strategy (per plan/debates-recon-001.md §"Decisions"): fetch-
extract-delete. No PDF involvement on the LS side at all (the API
serves HTML bodies which we strip to plain text directly).

Independence Principle: no imports from cag, lc, fc, drsc, or bills
scrapers. The HTTP layer + RateLimited + jitter + checkpoint primitives
are re-implemented under debates/common.py.
"""

from __future__ import annotations

import json
import os
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

from debates.common import RateLimited
from debates.scrapers.loksabha import (
    DEFAULT_LOK_SABHAS,
    LSDebate,
    fetch_page as ls_fetch_page,
    extract_text as ls_extract_text,
    file_id as ls_file_id,
    report_key as ls_report_key,
)
from debates.scrapers.rajyasabha import (
    INGESTED_VERSIONS as RS_INGESTED_VERSIONS,
    RSDebate,
    walk_rajyasabha,
    fetch_and_extract as rs_fetch_and_extract,
    versioned_file_id as rs_versioned_file_id,
    base_file_id as rs_base_file_id,
    report_key as rs_report_key,
)

# ── Paths ───────────────────────────────────────────────────────────────────

ASSETS    = ROOT / "docs"
DOCS      = ASSETS / "debates"
TEXT_DIR  = DOCS / "text"
PDFS_DIR  = DOCS / "pdfs"   # transient RS PDFs — gitignored
DOCS.mkdir(parents=True, exist_ok=True)

REPORTS_JSON      = DOCS / "reports.json"        # legacy single-file (pre-sharding)
REPORTS_META_JSON = DOCS / "reports-meta.json"   # shard manifest the app fetches first
MANIFEST_JSON     = DOCS / "manifest.json"
META_JSON         = DOCS / "meta.json"

# Per-house shard size. Picked so each shard stays under CF's 25 MiB
# per-file cap with headroom for record-size growth (LS body fields
# are small; RS records carry version URLs). 2500 records × ~8 KB =
# ~20 MB worst-case — well clear of the limit. Bills uses 1000/shard;
# we go bigger because debates records are smaller on average and the
# total count is higher, so fewer shards = fewer parallel fetches at
# app boot. See plan/backfill-audit-001.md for why this matters.
SHARD_SIZE = 2500

# Houses we cover. Phase B adds "rs" (daily-proceedings PDFs).
HOUSES = ["ls", "rs"]

# ── Per-run budget ─────────────────────────────────────────────────────────
#
# Per the recon doc's confirmed decisions: 50 records per burst, 12×/day
# during LS phase A = 600/day. ~107 days to clear LS-13..18's ~64K
# records. Per-burst budget = 50 (within Politeness policy upper bound).
# 12×/day = 2-hour gaps between bursts.

# LS extraction budget (HTML body fetches via debate-details — cheap,
# ~100-500 ms each).
MAX_EXTRACTIONS_PER_RUN = int(os.environ.get("MAX_EXTRACTIONS_PER_RUN", "50"))
MAX_RUN_SECONDS         = int(os.environ.get("MAX_RUN_SECONDS", "900"))    # 15 min
# Walk budget, separate from the extract budget above. The walk phase had NO
# deadline at all: with RS_SESSIONS=all it enumerated ~82 sessions until the
# job's timeout-minutes killed the runner, so the "Commit and push walked
# records" step never ran and every record the walk discovered was thrown
# away. 40 of 40 debates-walk runs to 2026-07-27 ended cancelled or failed;
# not one committed. Default leaves ~20 min of the 60-minute job for the
# commit + push that actually persists the work.
WALK_MAX_SECONDS        = int(os.environ.get("WALK_MAX_SECONDS", "2400"))  # 40 min
EXTRACT_WORKERS         = int(os.environ.get("EXTRACT_WORKERS", "4"))

# RS extraction budget (PDF downloads — heavier; ~2-5 sec each). Default
# is smaller than LS because each unit costs more wall-clock + bandwidth.
# 25 × 12 runs/day = 300 RS PDFs/day → ~12 days to backfill the ~3000
# Floor+English+Part-1 versions across sessions 189-270.
MAX_RS_EXTRACTIONS_PER_RUN = int(os.environ.get("MAX_RS_EXTRACTIONS_PER_RUN", "25"))

# Per-PDF wall-clock cap inside the orchestrator. Belt-and-braces; the
# scraper's http_get also has a 180s timeout.
RS_PER_PDF_TIMEOUT = int(os.environ.get("RS_PER_PDF_TIMEOUT", "180"))

# LS terms to enumerate this run. Comma-separated env var. Default =
# LS-14..18 (matching DRSC depth). The merge-with-existing logic
# preserves entries from un-enumerated terms across runs, so narrowing
# LOK_SABHAS to (say) "18" for daily steady-state is safe.
_LS_RAW = os.environ.get("LOK_SABHAS", "").strip()
LOK_SABHAS = ([int(x) for x in _LS_RAW.split(",") if x.strip()]
              if _LS_RAW else list(DEFAULT_LOK_SABHAS))

# RS sessions to enumerate. Three modes:
#   - RS_SESSIONS unset (default): walk the 2 most recent sessions
#     visible upstream. Steady-state mode — captures all new content
#     (RS only accrues sittings in the current session) without
#     re-walking the historical archive on every cron firing.
#   - RS_SESSIONS=all: walk every session the API exposes (~82
#     sessions, ~1,700 metadata API calls). One-shot backfill mode
#     — use via workflow_dispatch, not via the regular cron.
#   - RS_SESSIONS=<csv>: walk exactly those sessions. Used for
#     targeted catch-up runs.
#
# Politeness motivation: a per-cron-firing full walk of all 82
# sessions = 1,700 metadata calls × 12 cron firings/day = ~20K
# wasted upstream calls per day at steady state. Capping to the
# 2 most recent sessions keeps cron firings tight (~40 calls / run).
# See plan/adding-a-new-corpus.md §"Politeness policy".
_RS_RAW = os.environ.get("RS_SESSIONS", "").strip().lower()
if _RS_RAW == "all":
    RS_SESSIONS: Optional[list[int]] = None       # walk all
    _RS_MAX = None
    _RS_MODE = "all-sessions"
elif _RS_RAW:
    RS_SESSIONS = [int(x) for x in _RS_RAW.split(",") if x.strip()]
    _RS_MAX = None
    _RS_MODE = f"explicit={RS_SESSIONS}"
else:
    # Default: 2 most recent. The scraper discovers session numbers
    # live, so we don't hardcode the latest.
    RS_SESSIONS = None
    _RS_MAX = 2
    _RS_MODE = "recent-2"

# Cooldown after a 429 / 403 — defensive.
RATE_LIMIT_COOLDOWN_SECONDS = int(os.environ.get("RATE_LIMIT_COOLDOWN_SECONDS", str(6 * 3600)))

# ── In-flight checkpointing ────────────────────────────────────────────────

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
    shards) when present; falls back to the legacy single-file reports.json
    on the first run after migration or on a fresh checkout. Both shapes
    are equivalent — the on-disk format is forward-compatible: missing
    house keys are tolerated.
    """
    # Preferred path: sharded format. App + future scraper runs use this.
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

    # Legacy fallback — exercised exactly once per repo, the first time
    # the new build_debates.py runs against an unsharded checkout.
    if REPORTS_JSON.exists():
        with open(REPORTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {h: [] for h in HOUSES}


def save_reports(reports: dict[str, list[dict]]) -> None:
    """Sort each house's list deterministically and write as sharded files:
       - LS: lok_sabha desc, session desc, db_slno desc (newest first).
       - RS: session desc, date_iso desc (newest first).

    Output (under docs/debates/):
      - reports-meta.json         shard manifest (small, app fetches first)
      - reports-<house>-<NN>.json one shard per SHARD_SIZE records per house
    The legacy reports.json is deleted on first run after migration. The
    in-flight checkpoint commits use `docs/debates/` as the path arg so
    new/changed/deleted shards are all picked up automatically.
    """
    out: dict[str, list[dict]] = {}
    for h in HOUSES:
        items = reports.get(h, [])
        if h == "ls":
            items.sort(key=lambda r: (
                -int(r.get("lok_sabha") or 0),
                -int(r.get("session")   or 0),
                -int(r.get("db_slno")   or 0)))
        elif h == "rs":
            # Sort twice (stable) — composite key is -session asc + date_iso
            # desc which can't be expressed as a single tuple cleanly.
            items.sort(key=lambda r: str(r.get("date_iso") or ""), reverse=True)
            items.sort(key=lambda r: -int(r.get("session") or 0))
        out[h] = items
    # Preserve any unknown-house keys for forward compatibility.
    for h, items in reports.items():
        if h not in HOUSES and h not in out:
            out[h] = items

    shard_entries = _write_sharded_reports(out)
    _write_reports_meta(out, shard_entries)

    # One-time migration: drop the legacy single-file once shards are in place.
    if REPORTS_JSON.exists():
        try:
            REPORTS_JSON.unlink()
        except OSError as e:
            print(f"  ! could not delete legacy reports.json: {e}")


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
    them. Cleans up orphan shards (from a previous run with more records or
    a different house set). Returns a per-house manifest of shard entries.
    """
    # Remove ALL existing report shards so a run with fewer records doesn't
    # leave a stale higher-index shard behind. reports-meta.json is preserved
    # — we overwrite it in the meta writer.
    for path in DOCS.glob("reports-*.json"):
        if path.name == REPORTS_META_JSON.name:
            continue
        try:
            path.unlink()
        except OSError:
            pass

    # Group by natural bucket, never by position — see build_questions.py
    # and parliamentwatch_text_shards."Stable shard assignment". Bucket order
    # follows first appearance in `items`, preserving the canonical ordering
    # without an index inside any shard payload.
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
        # Always emit an entry list per house (possibly empty) so the app's
        # discovery logic doesn't have to special-case house presence.
        shard_entries[house] = entries
    return shard_entries


def _write_reports_meta(reports: dict[str, list[dict]],
                       shard_entries: dict[str, list[dict]]) -> None:
    """Write reports-meta.json — the small shard manifest the app fetches
    first. Includes per-house totals so a corpus-status renderer can show
    counts without touching any data shard."""
    meta = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "shard_size": SHARD_SIZE,
        "totals": {h: len(reports.get(h, [])) for h in reports},
        "shards": shard_entries,
    }
    if not write_json_idempotent(REPORTS_META_JSON, meta):
        print("  [skip] reports-meta.json unchanged (besides timestamp)")


def _record_from_lsdebate(d: LSDebate) -> dict:
    return {
        "house":            "ls",
        "lok_sabha":        d.lok_sabha,
        "session":          d.session,
        "db_slno":          d.db_slno,
        "title":            d.title,
        "debate_date":      d.debate_date,
        "debate_type":      d.debate_type,
        "debate_type_desc": d.debate_type_desc,
        "members":          d.members,
        "keywords":         d.keywords,
    }


def _ls_key_tuple(r: dict) -> tuple[int, int, int]:
    return (int(r["lok_sabha"]), int(r["session"]), int(r["db_slno"]))


def _rs_record_from(d: RSDebate) -> dict:
    return {
        "house":          "rs",
        "session":        d.session_number,
        "date":           d.date_raw,         # DD/MM/YYYY — preserved for display
        "date_iso":       d.date_iso,
        "file_versions":  [
            {"version": v, "url": u} for v, u in sorted(d.versions.items())
        ],
        # No title upstream; we'll synthesize "Session N · Date" in the app.
        "title":          f"Rajya Sabha · Session {d.session_number} · {d.date_raw}",
    }


def _rs_key_tuple(r: dict) -> tuple[int, str]:
    return (int(r["session"]), str(r["date_iso"]))


def walk_rs_and_merge(existing: dict[str, list[dict]], *, deadline: float | None = None) -> tuple[dict[str, list[dict]], dict, set[int]]:
    """Walk RS sessions, merge into existing reports.json. Returns
    (merged_reports, walk_stats, walked_sessions_set).
    """
    print(f"[Walk RS] mode={_RS_MODE}, max_sessions={_RS_MAX}, explicit={RS_SESSIONS}...")
    t0 = time.time()
    fresh_rs: dict[tuple[int, str], dict] = {}
    walked_sessions: set[int] = set()
    truncated = False
    try:
        for rec in walk_rajyasabha(sessions=RS_SESSIONS, max_sessions=_RS_MAX):
            # Stop on budget rather than on the job timeout. Partial results
            # that get committed beat complete results that get killed.
            if deadline is not None and time.monotonic() > deadline:
                truncated = True
                print(f"  [budget] walk deadline reached after {len(walked_sessions)} "
                      f"sessions — stopping so the commit step can run")
                break
            walked_sessions.add(rec.session_number)
            key = (rec.session_number, rec.date_iso)
            if key in fresh_rs: continue
            fresh_rs[key] = _rs_record_from(rec)
    except RateLimited:
        raise
    except Exception as e:
        print(f"  ERR walk_rajyasabha: {e}")
    print(f"  walked in {time.time()-t0:.1f}s — {len(fresh_rs)} sitting-day records across "
          f"{len(walked_sessions)} sessions")

    # Merge with archival promise: existing RS entries in NON-walked
    # sessions are preserved; existing RS entries in walked sessions that
    # aren't in fresh are also kept (upstream delisting doesn't drop us).
    merged: dict[str, list[dict]] = {h: list(existing.get(h, [])) for h in HOUSES}
    old_rs_by_key = {_rs_key_tuple(r): r for r in merged.get("rs", []) if r.get("date_iso")}
    new_count = 0; updated_count = 0; kept_legacy = 0
    out_rs: list[dict] = []
    combined_keys = set(old_rs_by_key.keys()) | set(fresh_rs.keys())
    for key in combined_keys:
        if key in fresh_rs:
            new_rec = fresh_rs[key]
            if key in old_rs_by_key:
                old_rec = old_rs_by_key[key]
                # Never let an empty-versions discovery clobber an existing
                # record that has working URLs. Upstream sometimes serves
                # stale placeholder URLs (whose filenames don't match the
                # requested date) for a session that previously had real
                # URLs — without this guard, a re-walk would lose the
                # working version map. See rajyasabha.py's _url_date_matches
                # filter for the upstream bug we're defending against.
                if (not new_rec.get("file_versions")
                        and old_rec.get("file_versions")):
                    out_rs.append(old_rec)
                    kept_legacy += 1
                    continue
                if old_rec != new_rec: updated_count += 1
            else:
                new_count += 1
            out_rs.append(new_rec)
        else:
            if key[0] in walked_sessions:
                kept_legacy += 1
            out_rs.append(old_rs_by_key[key])
    merged["rs"] = out_rs
    # Preserve LS as-is + any unknown houses
    for h, items in existing.items():
        if h != "rs":
            merged[h] = items
    stats = {"new": new_count, "updated": updated_count, "kept_legacy": kept_legacy}
    print(f"  merged: {len(merged.get('rs', []))} total RS records "
          f"(new={new_count}, updated={updated_count}, kept_legacy={kept_legacy})")
    return merged, stats, walked_sessions


def walk_ls_and_merge(existing: dict[str, list[dict]]) -> tuple[dict[str, list[dict]], dict]:
    """Walk LS debate-search across LOK_SABHAS, merge with existing
    records. Fresh wins on conflict; entries in LS terms NOT walked
    this run are preserved (the merge contract).

    Newest-first ordering within each LS via the API's default sort
    (desc by dbSlno, which monotonically tracks date).

    Early-stop: the API returns records newest-first by db_slno, so
    once a full page yields zero new keys (all already in existing),
    the rest of that LS is necessarily already-known. We break out
    of the page loop. Saves ~30+ minutes/run in steady state when
    no new records have been published upstream — leaving the
    MAX_RUN_SECONDS budget free for the extract phase. Updates to
    records on pages 2+ are NOT detected by the truncated walk;
    set WALK_NO_EARLY_STOP=1 to disable early-stop and force a full
    re-walk (intended as an occasional manual sweep).
    """
    print(f"[Walk] LS terms {LOK_SABHAS} (newest-first within each)...")
    t0 = time.time()
    no_early_stop = os.environ.get("WALK_NO_EARLY_STOP", "").strip() not in ("", "0", "false", "False")
    known_keys: set[tuple[int, int, int]] = {
        _ls_key_tuple(r) for r in existing.get("ls", [])
    }
    fresh_ls: dict[tuple[int, int, int], dict] = {}
    seen_terms: set[int] = set()
    pages_skipped_total = 0
    page_size = 200
    for ls in sorted(LOK_SABHAS, reverse=True):
        seen_terms.add(ls)
        page = 1
        total_pages = 1
        per_term = 0
        per_term_new = 0
        while page <= total_pages:
            try:
                records, meta = ls_fetch_page(ls, page=page, size=page_size)
            except RateLimited:
                raise
            except Exception as e:
                print(f"  ERR LS{ls} page {page}: {e} — aborting this LS")
                break
            total_pages = meta.get("totalPages") or 1
            page_new = 0
            for rec in records:
                key = (rec.lok_sabha, rec.session, rec.db_slno)
                if key in fresh_ls:
                    continue
                fresh_ls[key] = _record_from_lsdebate(rec)
                if key not in known_keys:
                    page_new += 1
            per_term += len(records)
            per_term_new += page_new
            if page == 1 or page % 10 == 0 or page == total_pages:
                print(f"  LS{ls} page {page}/{total_pages}: {len(records)} records "
                      f"(running={per_term}, new={page_new})")
            if not no_early_stop and records and page_new == 0 and per_term_new == 0 and page < total_pages:
                pages_skipped = total_pages - page
                pages_skipped_total += pages_skipped
                print(f"  LS{ls} early-stop after page {page}/{total_pages} "
                      f"({pages_skipped} pages skipped — all-known)")
                break
            page += 1
        print(f"  LS{ls} done: {per_term} records ({per_term_new} new)")
    walk_secs = time.time() - t0
    es_note = f" (early-stop skipped {pages_skipped_total} pages)" if pages_skipped_total else ""
    print(f"  walked all in {walk_secs:.1f}s — {len(fresh_ls)} records across walked terms{es_note}")

    # Merge: fresh-LS overrides; existing LS entries in NON-walked
    # terms are preserved; existing LS entries in walked terms that
    # aren't in fresh are also preserved (archival promise — upstream
    # delisting doesn't drop us).
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
            # Old entry not in fresh — "kept_legacy" only if its LS was walked
            if key[0] in seen_terms:
                kept_legacy += 1
            out_ls.append(old_ls_by_key[key])
    merged["ls"] = out_ls
    # Preserve any non-known-house keys (forward compat).
    for h, items in existing.items():
        if h not in HOUSES:
            merged[h] = items
    stats = {"new": new_count, "updated": updated_count, "kept_legacy": kept_legacy}
    total = sum(len(merged.get(h, [])) for h in HOUSES)
    print(f"  merged: {total} total LS records "
          f"(new={new_count}, updated={updated_count}, kept_legacy={kept_legacy})")
    return merged, stats


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
    """For each LS record without a `.txt` and without an empty/error
    marker, fetch debate-details, strip HTML, save as text. Bounded by
    MAX_EXTRACTIONS_PER_RUN.
    """
    if _check_cooldown_and_skip():
        return {"extracted": [], "failed": [], "rate_limited": True,
                "budget_hit": False, "skipped_due_to_cooldown": True,
                "candidates_total": 0}

    # Bundled-records skip: texts-meta.json is the source of truth post-
    # 2026-05-14 cleanup. LS composite key matches the build adapter:
    # `ls|<file_id>`.
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
    candidates: list[tuple[int, int, int]] = []  # (loksabha, session, dbslno)
    skipped_marked = 0
    skipped_bundled = 0
    for r in reports.get("ls", []):
        ls = r.get("lok_sabha"); ses = r.get("session"); dn = r.get("db_slno")
        if ls is None or ses is None or dn is None:
            continue
        fid = ls_file_id(int(ls), int(ses), int(dn))
        if f"ls|{fid}" in bundled_ids:
            skipped_bundled += 1; continue
        # Markers.json: ".empty" is the permanent skip (legit upstream
        # silence); ".error" is retryable.
        if bundled_markers.get(f"ls|{fid}") == "empty":
            skipped_marked += 1; continue
        if (ls_text_dir / f"{fid}.txt").exists():
            skipped_marked += 1; continue
        if (ls_text_dir / f"{fid}.empty").exists():
            # Upstream had no body — don't retry. Empty bodies are
            # legitimate for procedural items (rulings, papers laid).
            skipped_marked += 1; continue
        # NB: .error markers are retryable — fall through to candidate.
        candidates.append((int(ls), int(ses), int(dn)))

    # Priority: newest LS first, then highest dbSlno (newest record).
    candidates.sort(key=lambda c: (-c[0], -c[1], -c[2]))
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

    extracted: list[tuple[int, int, int]] = []
    failed: list[tuple[int, int, int]] = []
    rate_limited = False; budget_hit = False
    last_checkpoint_at = time.monotonic()
    extracted_since_checkpoint = 0

    def _do(c):
        ls, ses, dn = c
        try:
            text = ls_extract_text(ls, ses, dn, text_dir=str(ls_text_dir))
            return c, text, None
        except RateLimited as rl:
            return c, None, rl
        except Exception as e:
            return c, None, e

    with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as ex:
        futures = {ex.submit(_do, c): c for c in target}
        for fut in as_completed(futures):
            c, text, err = fut.result()
            if isinstance(err, RateLimited):
                print(f"  [RATE-LIMITED] LS{c[0]} S{c[1]} #{c[2]}: {err}")
                rate_limited = True
                for f in futures:
                    if not f.done(): f.cancel()
                break
            elif text:
                extracted.append(c)
                extracted_since_checkpoint += 1
            else:
                # Empty body (.empty marker written) or fetch error
                # (.error marker written). Either way it's resolved-
                # for-this-run.
                failed.append(c)
                extracted_since_checkpoint += 1
            now = time.monotonic()
            if (extracted_since_checkpoint >= CHECKPOINT_EVERY_N or
                (extracted_since_checkpoint > 0 and now - last_checkpoint_at >= CHECKPOINT_EVERY_S)):
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
                checkpoint_commit(
                    f"Auto-checkpoint debates primary data (extracted={len(extracted)} this run) [{ts}]",
                    ["docs/debates/"],
                )
                extracted_since_checkpoint = 0
                last_checkpoint_at = now
            if now > deadline:
                print(f"  [BUDGET] wall-clock budget hit after {len(extracted)} successes")
                budget_hit = True
                for f in futures:
                    if not f.done(): f.cancel()
                break

    if extracted_since_checkpoint > 0:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        checkpoint_commit(
            f"Auto-checkpoint debates primary data (final, extracted={len(extracted)} this run) [{ts}]",
            ["docs/debates/"],
        )

    return {
        "extracted": extracted, "failed": failed,
        "rate_limited": rate_limited, "budget_hit": budget_hit,
        "candidates_total": len(candidates),
    }


# ── Phase 3: derived files ─────────────────────────────────────────────────


def build_manifest() -> dict:
    """House-keyed manifest.

    LS entries are simple: texts.ls[<file_id>] = {size, url}.

    RS entries are multi-version: texts.rs[<base_id>] = {floor: {size, url},
    english: {...}, part1: {...}}. Each (session, date) can have up to 3
    versions; we collapse them under the base_id.
    """
    out: dict[str, dict] = {}
    if not TEXT_DIR.exists():
        return {"texts": out}

    # LS — flat
    ls_dir = TEXT_DIR / "ls"
    if ls_dir.exists():
        out["ls"] = {}
        for text_file in sorted(ls_dir.glob("*.txt")):
            fid = text_file.stem
            out["ls"][fid] = {
                "size": text_file.stat().st_size,
                "url":  f"text/ls/{text_file.name}",
            }

    # RS — group versions under base id (RS<session>_<iso>).
    rs_dir = TEXT_DIR / "rs"
    if rs_dir.exists():
        rs_out: dict[str, dict] = {}
        # versioned_file_id is "<base_id>_<version>"; reverse it by
        # finding the trailing _<version> suffix among our known versions.
        for text_file in sorted(rs_dir.glob("*.txt")):
            stem = text_file.stem
            base_id = None; version = None
            for v in RS_INGESTED_VERSIONS:
                suffix = f"_{v}"
                if stem.endswith(suffix):
                    base_id = stem[:-len(suffix)]; version = v
                    break
            if not base_id:
                # Unknown suffix — keep at top level under its full stem
                rs_out[stem] = {
                    "_unknown": {"size": text_file.stat().st_size,
                                 "url":  f"text/rs/{text_file.name}"}}
                continue
            rs_out.setdefault(base_id, {})[version] = {
                "size": text_file.stat().st_size,
                "url":  f"text/rs/{text_file.name}",
            }
        if rs_out:
            out["rs"] = rs_out
    return {"texts": out}


def compute_audit(reports: dict[str, list[dict]]) -> dict:
    """Per-record status across both houses. RS records contribute one
    audit-status per VERSION (a sitting day with floor+english+part1
    contributes 3 to the counts), so the totals are version-weighted
    on the RS side. LS records each contribute 1.
    """
    counts = {
        "ls_records":             len(reports.get("ls", [])),
        "ls_with_text":           0,
        "ls_empty_upstream":      0,
        "ls_error_retryable":     0,
        "ls_never_attempted":     0,
        "rs_sitting_days":        len(reports.get("rs", [])),
        "rs_version_units":       0,   # versions ingested (1-3 per day)
        "rs_with_text":           0,
        "rs_pypdf_empty_awaiting_ocr": 0,
        "rs_pypdf_error_retryable":    0,
        "rs_ocr_failed_permanent":     0,
        "rs_never_attempted":          0,
    }
    # Read bundled-text composite_ids + bundled markers up front.
    bundled_ids: set = set()
    texts_meta_path = DOCS / "texts-meta.json"
    if texts_meta_path.exists():
        try:
            bundled_ids = load_bundled_ids(DOCS)
        except (OSError, json.JSONDecodeError):
            pass
    markers = load_markers(DOCS)

    # LS audit
    ls_dir = TEXT_DIR / "ls"
    for r in reports.get("ls", []):
        ls = r.get("lok_sabha"); ses = r.get("session"); dn = r.get("db_slno")
        if ls is None or ses is None or dn is None: continue
        fid = ls_file_id(int(ls), int(ses), int(dn))
        cid_text   = f"ls|{fid}"                          # shard key
        cid_marker = f"ls|{fid}"                          # marker key (matches stem)
        if cid_text in bundled_ids:
            counts["ls_with_text"] += 1
        elif ls_dir.exists() and (ls_dir / f"{fid}.txt").exists():
            counts["ls_with_text"] += 1
        elif cid_marker in markers:
            mt = markers[cid_marker]
            if mt == "empty":    counts["ls_empty_upstream"] += 1
            elif mt == "error":  counts["ls_error_retryable"] += 1
            else:                counts["ls_never_attempted"] += 1
        elif (ls_dir / f"{fid}.empty").exists():
            counts["ls_empty_upstream"] += 1
        elif (ls_dir / f"{fid}.error").exists():
            counts["ls_error_retryable"] += 1
        else:
            counts["ls_never_attempted"] += 1

    # RS audit (version-weighted)
    rs_dir = TEXT_DIR / "rs"
    for r in reports.get("rs", []):
        ses = r.get("session"); iso = r.get("date_iso")
        if ses is None or iso is None: continue
        for v in (r.get("file_versions") or []):
            ver = v.get("version")
            if ver not in RS_INGESTED_VERSIONS: continue
            counts["rs_version_units"] += 1
            fid = rs_versioned_file_id(int(ses), iso, ver)
            # Shard key is `rs|<base_id>|<ver>` where base_id = `RS<ses>_<iso>`
            # (matches the items composition in phase_derive and the
            # `f"rs|{base_id}|{ver}" in bundled_ids` check in phase_extract's
            # candidate-skip path). Marker key is `rs|<stem>` (== `rs|<fid>`).
            base_id    = rs_base_file_id(int(ses), iso)
            cid_text   = f"rs|{base_id}|{ver}"
            cid_marker = f"rs|{fid}"
            if cid_text in bundled_ids:
                counts["rs_with_text"] += 1
            elif rs_dir.exists() and (rs_dir / f"{fid}.txt").exists():
                counts["rs_with_text"] += 1
            elif cid_marker in markers:
                mt = markers[cid_marker]
                if mt == "ocr-failed":     counts["rs_ocr_failed_permanent"] += 1
                elif mt == "pypdf-empty":  counts["rs_pypdf_empty_awaiting_ocr"] += 1
                elif mt == "pypdf-error":  counts["rs_pypdf_error_retryable"] += 1
                else:                      counts["rs_never_attempted"] += 1
            elif (rs_dir / f"{fid}.ocr-failed").exists():
                counts["rs_ocr_failed_permanent"] += 1
            elif (rs_dir / f"{fid}.pypdf-empty").exists():
                counts["rs_pypdf_empty_awaiting_ocr"] += 1
            elif (rs_dir / f"{fid}.pypdf-error").exists():
                counts["rs_pypdf_error_retryable"] += 1
            else:
                counts["rs_never_attempted"] += 1
    return {
        "audited_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totals":     counts,
    }


# ── Search bundle + index (same sharding as LC/FC) ─────────────────────────

DOCS_PER_SHARD = 2500


def _delete_legacy(path: Path) -> None:
    if path.exists():
        path.unlink()


def build_search_bundle(reports: dict[str, list[dict]],
                        docs_per_shard: int = DOCS_PER_SHARD) -> dict | None:
    """Title + first 5K chars per record. Sharded by sorted reportKey.

    LS: one entry per debate record.
    RS: one entry per sitting day (Floor version is the canonical text
        in the bundle — Part-1 and English are reachable via the
        manifest but not indexed for search to avoid hit duplication).
    """
    if not TEXT_DIR.exists():
        return None
    HEAD = 5000
    entries = []
    truncated = 0

    # LS entries
    ls_dir = TEXT_DIR / "ls"
    for r in reports.get("ls", []):
        ls = r.get("lok_sabha"); ses = r.get("session"); dn = r.get("db_slno")
        if ls is None or ses is None or dn is None: continue
        fid = ls_file_id(int(ls), int(ses), int(dn))
        text_path = ls_dir / f"{fid}.txt"
        if not text_path.exists(): continue
        try:
            text = text_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        head = text[:HEAD]
        if len(text) > HEAD: truncated += 1
        entries.append({
            "k":    ls_report_key(int(ls), int(ses), int(dn)),
            "t":    r.get("title", ""),
            "head": head,
            "_g":   shard_group(f"ls|{fid}", SEARCH_SHARD_STRIDE),
        })

    # RS entries — Floor only as the canonical bundle text
    rs_dir = TEXT_DIR / "rs"
    for r in reports.get("rs", []):
        ses = r.get("session"); iso = r.get("date_iso")
        if ses is None or iso is None: continue
        floor_fid = rs_versioned_file_id(int(ses), iso, "floor")
        text_path = rs_dir / f"{floor_fid}.txt"
        if not text_path.exists(): continue
        try:
            text = text_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        head = text[:HEAD]
        if len(text) > HEAD: truncated += 1
        entries.append({
            "k":    rs_report_key(int(ses), iso),
            "t":    r.get("title", ""),
            "head": head,
            "_g":   shard_group(f"rs|{floor_fid}", SEARCH_SHARD_STRIDE),
        })

    if not entries:
        return None
    _delete_legacy(DOCS / "search-bundle.json")
    for path in DOCS.glob("search-bundle-*.json"):
        try: path.unlink()
        except OSError: pass

    # Partition by shard group, never by position in a global sort — see
    # parliamentwatch_text_shards."Stable shard assignment".
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


import re
_TOKEN_RE = re.compile(r"[a-z0-9]{3,}")


def _tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def build_search_index(docs_per_shard: int = DOCS_PER_SHARD) -> dict | None:
    """Full-body inverted token index, sharded.

    LS: indexes every record's text.
    RS: indexes the Floor version's text per sitting day. Part-1 and
    English are excluded to keep the index lean and avoid duplicate
    hits across versions of the same sitting day.
    """
    if not TEXT_DIR.exists():
        return None
    # Gather (report_key, text) in deterministic order.
    docs: list[tuple[str, str]] = []

    # LS
    ls_dir = TEXT_DIR / "ls"
    if ls_dir.exists():
        files = list(ls_dir.glob("*.txt"))
        files.sort(key=lambda p: p.stem)   # deterministic; shard ranges are reportKey-sorted
        for path in files:
            m = re.match(r"LS(\d+)_S(\d+)_(\d+)$", path.stem)
            if not m: continue
            ls, ses, dn = m.group(1), m.group(2), m.group(3)
            rk = f"debates|ls|{ls}|{ses}|{dn}"
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            docs.append((rk, text))

    # RS — Floor version only
    rs_dir = TEXT_DIR / "rs"
    if rs_dir.exists():
        files = list(rs_dir.glob("*_floor.txt"))
        files.sort(key=lambda p: p.stem)
        for path in files:
            m = re.match(r"RS(\d+)_(\d{4}-\d{2}-\d{2})_floor$", path.stem)
            if not m: continue
            ses, iso = m.group(1), m.group(2)
            rk = f"debates|rs|{ses}|{iso}"
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            docs.append((rk, text))

    if not docs:
        return None
    # Sort once by reportKey so shard ranges are stable.
    docs.sort(key=lambda d: d[0])
    token_docs: dict[str, set[int]] = {}
    for i, (_, text) in enumerate(docs):
        for tok in set(_tokenize(text)):
            token_docs.setdefault(tok, set()).add(i)
    n_docs = len(docs)
    high_cut = max(2, int(n_docs * 0.5))
    low_cut  = 2
    vocab = sorted(tok for tok, ds in token_docs.items()
                   if low_cut <= len(ds) <= high_cut)
    if not vocab:
        vocab = sorted(token_docs.keys())
        low_cut = 1; high_cut = n_docs
    _delete_legacy(DOCS / "search-index.json")
    shard_count = (n_docs + docs_per_shard - 1) // docs_per_shard
    shard_sizes: dict[str, int] = {}
    max_shard = 0; total_bytes = 0; total_postings = 0
    for i in range(shard_count):
        start = i * docs_per_shard; end = min(start + docs_per_shard, n_docs)
        shard_keys = [docs[j][0] for j in range(start, end)]
        local_postings: list[list[int]] = []
        for tok in vocab:
            ds = token_docs[tok]
            in_shard = sorted(d - start for d in ds if start <= d < end)
            local_postings.append(in_shard)
            total_postings += len(in_shard)
        path = DOCS / f"search-index-{i:02d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "version":     "1.0",
                "shard_index": i,
                "vocab":       vocab,
                "report_keys": shard_keys,
                "postings":    local_postings,
            }, f, ensure_ascii=False)
        sz = path.stat().st_size
        shard_sizes[f"search-index-{i:02d}.json"] = sz
        max_shard = max(max_shard, sz); total_bytes += sz
    return {
        "shard_count":      shard_count,
        "shards":           list(shard_sizes.keys()),
        "shard_sizes":      shard_sizes,
        "report_count":     n_docs,
        "vocab_size":       len(vocab),
        "total_postings":   total_postings,
        "size_bytes":       total_bytes,
        "max_shard_bytes":  max_shard,
        "freq_cutoff_low":  low_cut,
        "freq_cutoff_high": high_cut,
    }


def write_meta(*, total_records: int, total_with_text: int,
               bundle_stats: dict | None, index_stats: dict | None) -> dict:
    meta = {
        "version":         "1.0",
        "corpus":          "debates",
        "generated_at":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_records":   total_records,
        "total_with_text": total_with_text,
        "houses":          HOUSES,
        "lok_sabhas":      LOK_SABHAS,
        "search_bundle":   bundle_stats,
        "search_index":    index_stats,
    }
    # Non-idempotent on purpose: always bump generated_at so the app's
    # staleness indicator (which reads meta.generated_at) reflects "last
    # successful derive" rather than "last data change". When upstream
    # is quiet for days, the indicator should still show fresh — what's
    # actually failing in the broken case is the workflow itself, not
    # the data, and that's the thing worth surfacing.
    write_json_idempotent(META_JSON, meta, ignore_keys=())
    return meta


# ── Main ────────────────────────────────────────────────────────────────────


def extract_missing_rs_pdfs(reports: dict[str, list[dict]], *, deadline: float) -> dict:
    """For each RS record, identify versions without text files yet,
    fetch + extract up to MAX_RS_EXTRACTIONS_PER_RUN PDFs. Priority:
    newest session → newest date → Floor before English before Part-1.
    """
    # Bundled-records skip: texts-meta.json is the source of truth post-
    # 2026-05-14 cleanup. RS composite key matches the build adapter:
    # `rs|<base_id>|<version>` where base_id = file_id stripped of the
    # `_<version>` suffix.
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
    rs_text_dir = TEXT_DIR / "rs"
    rs_pdfs_dir = PDFS_DIR / "rs"
    candidates: list[tuple[int, str, str, str]] = []   # (session, date_iso, version, url)
    skipped_marked = 0
    skipped_bundled = 0
    for r in reports.get("rs", []):
        ses = r.get("session"); iso = r.get("date_iso")
        if ses is None or iso is None: continue
        versions = r.get("file_versions") or []
        # Build per-record version map for stable lookup
        url_by_v = {v["version"]: v["url"] for v in versions if v.get("version") and v.get("url")}
        for ver in RS_INGESTED_VERSIONS:
            url = url_by_v.get(ver)
            if not url: continue
            fid = rs_versioned_file_id(int(ses), iso, ver)
            suffix = f"_{ver}"
            base_id = fid[:-len(suffix)] if fid.endswith(suffix) else fid
            if f"rs|{base_id}|{ver}" in bundled_ids:
                skipped_bundled += 1; continue
            bm = bundled_markers.get(f"rs|{fid}")
            if bm in ("pypdf-empty", "ocr-failed"):
                skipped_marked += 1; continue
            if (rs_text_dir / f"{fid}.txt").exists():
                skipped_marked += 1; continue
            if (rs_text_dir / f"{fid}.pypdf-empty").exists():
                # OCR slow lane will handle these. Skip from daily.
                skipped_marked += 1; continue
            if (rs_text_dir / f"{fid}.ocr-failed").exists():
                skipped_marked += 1; continue
            # .pypdf-error is retryable — falls through to candidate
            candidates.append((int(ses), iso, ver, url))

    # Priority: highest session first, then highest date_iso, then version
    # order (Floor < English < Part-1 to bias toward the canonical record).
    _VER_ORDER = {"floor": 0, "english": 1, "part1": 2}
    candidates.sort(key=lambda c: (-c[0], c[1], _VER_ORDER.get(c[2], 9)), reverse=False)
    # The above sorts session asc; want desc. Fix:
    candidates.sort(key=lambda c: (-c[0], -ord(c[1][0]) if c[1] else 0))
    # Simpler stable approach:
    candidates.sort(key=lambda c: c[1], reverse=True)        # by date desc
    candidates.sort(key=lambda c: _VER_ORDER.get(c[2], 9))   # then by version (Floor first)
    candidates.sort(key=lambda c: -int(c[0]))                # then by session desc (primary)
    print(f"  RS candidates: {len(candidates)} versions missing text "
          f"({skipped_marked} skipped — already marked / OCR-pending; "
          f"{skipped_bundled} skipped — already in shards)")

    if not candidates:
        return {"extracted": [], "failed": [], "rate_limited": False,
                "budget_hit": False, "candidates_total": 0}

    remaining = deadline - time.monotonic()
    if remaining <= 30:
        print(f"  only {remaining:.0f}s left in budget — skipping RS phase")
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
        ses, iso, ver, url = c
        try:
            text = rs_fetch_and_extract(url, session_no=ses, date_iso=iso,
                                        version=ver, text_dir=str(rs_text_dir),
                                        pdfs_dir=str(rs_pdfs_dir), delete_pdf=True)
            return c, text, None
        except RateLimited as rl:
            return c, None, rl
        except Exception as e:
            return c, None, e

    with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as ex:
        futures = {ex.submit(_do, c): c for c in target}
        for fut in as_completed(futures):
            c, text, err = fut.result()
            ses, iso, ver, _ = c
            if isinstance(err, RateLimited):
                print(f"  [RATE-LIMITED] RS{ses} {iso} {ver}: {err}")
                rate_limited = True
                for f in futures:
                    if not f.done(): f.cancel()
                break
            elif text:
                extracted.append((ses, iso, ver))
                extracted_since_checkpoint += 1
            else:
                # .pypdf-empty or .pypdf-error marker already written
                failed.append((ses, iso, ver))
                extracted_since_checkpoint += 1
            now = time.monotonic()
            if (extracted_since_checkpoint >= CHECKPOINT_EVERY_N or
                (extracted_since_checkpoint > 0 and now - last_checkpoint_at >= CHECKPOINT_EVERY_S)):
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
                checkpoint_commit(
                    f"Auto-checkpoint debates RS data (extracted={len(extracted)} this run) [{ts}]",
                    ["docs/debates/"],
                )
                extracted_since_checkpoint = 0
                last_checkpoint_at = now
            if now > deadline:
                print(f"  [BUDGET] wall-clock budget hit after {len(extracted)} RS successes")
                budget_hit = True
                for f in futures:
                    if not f.done(): f.cancel()
                break

    if extracted_since_checkpoint > 0:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        checkpoint_commit(
            f"Auto-checkpoint debates RS data (final, extracted={len(extracted)} this run) [{ts}]",
            ["docs/debates/"],
        )

    return {"extracted": extracted, "failed": failed,
            "rate_limited": rate_limited, "budget_hit": budget_hit,
            "candidates_total": len(candidates)}


def phase_walk() -> None:
    """Walk LS + RS and persist report shards. No body extraction.

    Split off from phase_extract so a slower-cadence walk workflow can
    discover new records without consuming the hourly extract budget.
    """
    existing = load_existing_reports()
    print(f"  existing on disk: {sum(len(existing.get(h, [])) for h in HOUSES)} records")
    deadline = time.monotonic() + WALK_MAX_SECONDS
    print(f"  walk budget: {WALK_MAX_SECONDS}s")

    print(f"\n[Walk 1/2] Walking LS {LOK_SABHAS}...")
    try:
        merged, ls_walk_stats = walk_ls_and_merge(existing)
    except RateLimited as rl:
        print(f"  [RATE-LIMITED during LS walk] {rl}")
        return
    save_reports(merged)

    if time.monotonic() > deadline:
        print("\n[Walk 2/2] SKIPPED — LS walk consumed the budget. RS runs next cycle.")
        return
    print(f"\n[Walk 2/2] Walking RS sessions...")
    try:
        merged, rs_walk_stats, _ = walk_rs_and_merge(merged, deadline=deadline)
    except RateLimited as rl:
        print(f"  [RATE-LIMITED during RS walk] {rl}")
        return
    save_reports(merged)


def phase_extract() -> None:
    """Extract missing bodies from records already in the report shards.

    Does NOT walk — walks are owned by phase_walk(). Run in the same job
    as phase_derive() so the transient text/ files (gitignored) can be
    bundled into committed text shards before the runner is destroyed.
    """
    overall_deadline = time.monotonic() + MAX_RUN_SECONDS
    merged = load_existing_reports()
    print(f"  existing on disk: {sum(len(merged.get(h, [])) for h in HOUSES)} records")

    print(f"\n[Extract 1/2] Fetching LS debate-details bodies (priority: newest LS, highest dbSlno)...")
    ls_stats = extract_missing_bodies(merged, deadline=overall_deadline)
    print(f"  LS: extracted={len(ls_stats['extracted'])} "
          f"failed={len(ls_stats['failed'])} "
          f"rate_limited={ls_stats.get('rate_limited', False)} "
          f"budget_hit={ls_stats.get('budget_hit', False)} "
          f"remaining_after={max(0, ls_stats.get('candidates_total', 0) - len(ls_stats.get('extracted', [])) - len(ls_stats.get('failed', [])))}")

    if ls_stats.get('rate_limited'):
        print("  [RS skipped because LS rate-limited]")
        return

    print(f"\n[Extract 2/2] Downloading + extracting RS PDFs (priority: newest session, newest date, Floor first)...")
    rs_stats = extract_missing_rs_pdfs(merged, deadline=overall_deadline)
    print(f"  RS: extracted={len(rs_stats['extracted'])} "
          f"failed={len(rs_stats['failed'])} "
          f"rate_limited={rs_stats.get('rate_limited', False)} "
          f"budget_hit={rs_stats.get('budget_hit', False)} "
          f"remaining_after={max(0, rs_stats.get('candidates_total', 0) - len(rs_stats.get('extracted', [])) - len(rs_stats.get('failed', [])))}")


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


def phase_derive() -> None:
    reports = load_existing_reports()
    total = sum(len(reports.get(h, [])) for h in HOUSES)

    print("\n[Derive 1/4] Building manifest.json...")
    _preserve = _derive_would_be_destructive()
    if _preserve:
        try:
            manifest = json.loads(MANIFEST_JSON.read_text())
            print("  [skip] manifest.json preserved (empty text/ scan on a bundled corpus)")
        except (OSError, json.JSONDecodeError):
            _preserve = False
            manifest = build_manifest()
    else:
        manifest = build_manifest()
    if not _preserve and not write_json_idempotent(MANIFEST_JSON, manifest):
        print("  [skip] manifest.json unchanged")
    n_with_text = sum(len(v) for v in manifest["texts"].values())
    print(f"  manifest: {n_with_text} records with extracted text")

    # Bundle per-record text files. Debates has TWO manifest shapes:
    #   LS: texts.ls[<file_id>] = {size, url}            — flat
    #   RS: texts.rs[<base_id>][<version>] = {size, url} — multi-version
    # Composite keys flatten this for the shard: `ls|<file_id>` or
    # `rs|<base_id>|<version>`. App's fetchReportText is updated to
    # compute the same composite.
    print("\n[Derive] Building text shards...")
    items = []
    for key, entry in sorted(manifest["texts"].get("ls", {}).items()):
        items.append((f"ls|{key}", DOCS / entry["url"]))
    for base_id, versions in sorted(manifest["texts"].get("rs", {}).items()):
        for version, entry in sorted(versions.items()):
            items.append((f"rs|{base_id}|{version}", DOCS / entry["url"]))
    text_meta = write_text_shards(DOCS, items)
    t = text_meta["totals"]
    print(f"  text-shards: {t['shards']} shard(s), {t['records_with_text']} records, "
          f"{t['total_text_bytes'] / 1024 / 1024:.1f} MB, "
          f"{t['r2_fallback']} via R2 sentinel, {t['skipped_oversize_no_r2']} skipped")

    # Consolidate marker sidecars. Debates uses non-standard suffixes
    # for LS (.empty / .error from HTML body extraction failure) AND
    # the standard pypdf/ocr suffixes for RS PDF extraction. The
    # consolidator accepts the full set. drop_record_ids omitted —
    # debates' shard composite_id (`rs|<date>|<ver>`) differs from
    # marker key format (`rs|<stem>`) so we don't try to cross-reference.
    marker_stats = consolidate_markers(
        DOCS, TEXT_DIR,
        composite_id_from_path=lambda p: f"{p.parent.name}|{p.stem}",
        marker_suffixes=("empty", "error", "pypdf-empty", "pypdf-error", "ocr-failed"),
    )
    print(f"  markers: {marker_stats['totals']} consolidated "
          f"(removed {marker_stats.get('removed_sidecar_count', 0)} sidecars)")

    print("\n[Derive 2/4] Building search-bundle...")
    bundle_stats = build_search_bundle(reports)
    if bundle_stats:
        mb = bundle_stats["size_bytes"] / (1024 * 1024)
        print(f"  bundle: {bundle_stats['total']} entries × {bundle_stats['shard_count']} shards · {mb:.1f} MB")
    else:
        print("  no extracted texts yet — skipping bundle")

    print("\n[Derive 3/4] Building search-index...")
    index_stats = build_search_index()
    if index_stats:
        mb = index_stats["size_bytes"] / (1024 * 1024)
        print(f"  index: {index_stats['report_count']} docs × {index_stats['shard_count']} shards, "
              f"vocab={index_stats['vocab_size']}, postings={index_stats['total_postings']} · {mb:.1f} MB")
    else:
        print("  no extracted texts yet — skipping index")

    print("\n[Derive 4/4] Writing meta.json + audit.json...")
    meta = write_meta(total_records=total, total_with_text=n_with_text,
                      bundle_stats=bundle_stats, index_stats=index_stats)
    print(json.dumps(meta, indent=2))

    audit = compute_audit(reports)
    if not write_json_idempotent(DOCS / "audit.json", audit):
        print("  [skip] audit.json unchanged (besides timestamp)")
    t = audit["totals"]
    print(f"\n  audit (LS): with_text={t['ls_with_text']} "
          f"empty={t['ls_empty_upstream']} "
          f"error={t['ls_error_retryable']} "
          f"never={t['ls_never_attempted']} (total={t['ls_records']})")
    print(f"  audit (RS): with_text={t['rs_with_text']} "
          f"pypdf_empty={t['rs_pypdf_empty_awaiting_ocr']} "
          f"pypdf_error={t['rs_pypdf_error_retryable']} "
          f"ocr_failed={t['rs_ocr_failed_permanent']} "
          f"never={t['rs_never_attempted']} "
          f"(sitting_days={t['rs_sitting_days']}, version_units={t['rs_version_units']})")


def main():
    """Dispatch to walk / extract / derive / all based on BUILD_PHASE.

    `all` = walk + extract + derive (used for one-off manual triggers).
    The cron-driven path runs walk in a separate workflow (every 6h)
    and extract+derive together in the hourly workflow.
    """
    phase = os.environ.get("BUILD_PHASE", "all").lower()
    if phase not in ("walk", "extract", "derive", "all"):
        print(f"BUILD_PHASE={phase!r} not in walk|extract|derive|all — aborting.", file=sys.stderr)
        sys.exit(2)

    print("=== ParliamentWatch Debates (LS phase A) static builder ===")
    print(f"BUILD_PHASE             : {phase}")
    print(f"DOCS                    : {DOCS}")
    print(f"LOK_SABHAS              : {LOK_SABHAS}")
    print(f"MAX_EXTRACTIONS_PER_RUN : {MAX_EXTRACTIONS_PER_RUN}")
    print(f"MAX_RUN_SECONDS         : {MAX_RUN_SECONDS}")
    print(f"EXTRACT_WORKERS         : {EXTRACT_WORKERS}")

    if phase in ("walk", "all"):
        phase_walk()
    if phase in ("extract", "all"):
        phase_extract()
    if phase in ("derive", "all"):
        phase_derive()
    print("\nDone.")


if __name__ == "__main__":
    main()
