"""Shared text-sharding helper for the parliamentwatch corpora.

Bundles per-record text files (`docs/<corpus>/text/<name>.txt`) into
key-addressed shard files (`docs/<corpus>/texts-<bucket>-<band>.json`) plus a
manifest (`docs/<corpus>/texts-meta.json`). Reduces the static-assets
file count by 50-200x per corpus while keeping each shard well under
Cloudflare's 25 MiB per-file cap.

See `plan/cloudflare-strategy-002.md` for the design rationale (bundling
primary, R2 fallback for oversize records).

Each corpus's build script generates the input list
`[(composite_id, Path), ...]`. Order is irrelevant: a record's shard is a
pure function of its own key (see "Stable shard assignment" below), so this
module never packs by position.

R2 fallback: any single record whose text exceeds `fallback_threshold`
gets a `{"r2": true}` sentinel in the shard. App fetches that record's
body from the R2 origin (already populated by the .github/workflows/
r2-sync.yml workflow). Threshold defaults to 1 MB — at the target 4.5 MB
shard size, allowing larger inline texts means one big record could
dominate a shard, defeating the cache-locality win.
"""

from __future__ import annotations

import glob
import json
import os
import re
from pathlib import Path
from typing import Iterable, Optional


# Defaults. Per-corpus build scripts can override via kwargs.
DEFAULT_TARGET_BYTES        = 4_500_000   # ~4.5 MB per shard
# A single record's text > this gets a `{"r2":true}` sentinel instead of
# being inlined. Set well above the largest observed record (~3 MB across
# all corpora) so by default everything bundles inline. Lower the threshold
# once an R2 public URL is configured in r2_origin — that's when sentinels
# actually become useful as a fallback path.
DEFAULT_FALLBACK_THRESHOLD  = 10_000_000  # 10 MB

# Sentinel embedded in a shard when the record's text was too big to inline.
# App side checks `typeof value === 'object'` then reads the R2 origin.
R2_SENTINEL = {"r2": True}
# Byte cost of `{"r2":true}` plus the key wrapping in JSON output (commas,
# quotes, etc). Used to budget shard size so a run with many fallbacks still
# fits the target.
SENTINEL_BUDGET_BYTES = 50


def _meta_filename() -> str:
    return "texts-meta.json"


# ── Stable shard assignment ────────────────────────────────────────────────
#
# THE RULE: a record's shard is a pure function of that record's own key.
# Never of its position in a global ordering.
#
# The pre-2026-07-27 implementation sorted every key, then greedily packed
# into size-targeted shards. That made the *order* deterministic but not the
# *assignment*: inserting one record early in the sort shifted every later
# record across shard boundaries, so a run adding ~2 MB of new text rewrote
# hundreds of MB of shards. One measured debates run rewrote 39 shards
# (~195 MB). Compounded hourly, that is what drove the repo to 100 GB and
# past GitHub's hard limit on 2026-06-28.
#
# Now: shard = f(key). Adding a record touches exactly one shard, and every
# other shard is byte-identical, so git stores no new blob for it.
#
# Buckets are the record's natural, immutable coordinates — house + term +
# session (or sitting date). Parliamentary records are historical: once a
# session is fully extracted its bucket is frozen forever. Only the session
# currently being backfilled sees any churn at all.
#
# INVARIANT for anyone maintaining this: every file that is rewritten
# frequently must be small; every file that is large must be immutable once
# written. `record_to_shard` was deleted from the manifest for exactly this
# reason — it was a 3.5 MB map rewritten on every run (and downloaded by
# every app session). The app now derives the shard from the key instead,
# using the same two functions mirrored in app/text-shards.js.

_BUCKET_SPLIT_RE = re.compile(r"[_|]")
_BUCKET_CLEAN_RE = re.compile(r"[^A-Za-z0-9-]")


def bucket_for(key: str) -> str:
    """Natural immutable bucket for a composite record key.

    `ls|LS16_S10_U_1`      -> `ls-LS16-S10`
    `rs|RS241_2016-12-07_U`-> `rs-RS241-2016-12-07`
    `rs|RS239_2016-05-03|floor` -> `rs-RS239-2016-05-03`

    Mirrored byte-for-byte in app/text-shards.js `bucketFor()`. If you
    change one, change both — the app resolves shards without a lookup map.
    """
    house, sep, body = key.partition("|")
    if not sep:
        house, body = "", key
    parts = [p for p in _BUCKET_SPLIT_RE.split(body) if p]
    head = parts[0] if parts else ""
    ses = parts[1] if len(parts) > 1 else ""
    raw = "-".join(p for p in (house, head, ses) if p)
    return _BUCKET_CLEAN_RE.sub("", raw) or "misc"



# Records per band inside a bucket. Big sessions (LS18-S7 is 41 MB) need
# splitting, but the split must preserve LOCALITY: extraction walks a
# session in question-number order, so banding on the record's own ordinal
# keeps a run's writes inside one or two files.
#
# Hashing the key was tried first and measured worse — it scatters a
# session's newly-extracted records across every band of its bucket, so the
# whole bucket gets rewritten. Measured on the real corpus, one 500-record
# run against questions:
#     hash sub-split   41.5 MB rewritten
#     ordinal band 50   7.9 MB rewritten
# Stride is set by the FILE-COUNT ceiling, not by amplification. Cloudflare
# Workers Static Assets caps a deploy at 20,000 files, and the corpus is only
# ~14% extracted — projecting both corpora to full backfill (246,046 question
# records + 58,958 debate records):
#     texts  50 / search  500 -> 20,218 files   OVER CAP
#     texts 100 / search 1000 -> 16,694 files   tight
#     texts 250 / search 1000 -> 14,929 files   25% headroom  <- chosen
# Amplification barely moves across that range (questions clustered: 7.9 MB
# at stride 50 vs 8.7 MB at 250; debates 10.2 vs 10.8), so the headroom is
# nearly free. Largest shard stays ~3.7 MB, inside the 25 MiB per-file cap.
DEFAULT_SHARD_STRIDE = 250

# Search bundle + index band coarser than the text shards — they carry the
# same buckets, so a shared stride would inflate the file count for no gain.
SEARCH_SHARD_STRIDE = 1000

_ORDINAL_RE = re.compile(r"(\d+)\D*$")


def shard_group(key: str, stride: int = DEFAULT_SHARD_STRIDE) -> str:
    """Shard group for `key` — the `<bucket>[-<band>]` infix.

    Shared by the text shards, the search bundle and the search index so all
    three partition identically: a record's title, its body and its postings
    all move together, and a run touches the same handful of files in each.
    """
    bucket = bucket_for(key)
    m = _ORDINAL_RE.search(key)
    if not m:
        # No ordinal (e.g. RS date-keyed records) — the bucket is already
        # a single sitting, so it needs no banding.
        return bucket
    return f"{bucket}-{int(m.group(1)) // stride:04d}"


def shard_filename(key: str, stride: int = DEFAULT_SHARD_STRIDE) -> str:
    """Text-shard filename holding `key`. Pure function of the key.

    Mirrored in app/text-shards.js `shardFileFor()`. Change one, change both.
    """
    return f"texts-{shard_group(key, stride)}.json"


_BUNDLED_IDS_CACHE: dict = {}


def load_bundled_ids(corpus_docs_dir: Path) -> set:
    """Composite ids currently held in the text shards.

    Schema 2 has no `record_to_shard` map — it was 3.5 MB, rewritten every
    run and downloaded by every app session, so it was removed on
    2026-07-27. Membership therefore comes from the shards themselves.

    That removal silently broke nine call sites across the two builders that
    did `set(meta["record_to_shard"].keys())` and got an empty set on a
    schema-2 manifest. The audit read it (every record became
    "never_attempted"), and — far worse — so did the extract phase's
    "already bundled, skip it" check, which would have re-downloaded PDFs
    for 127,319 records that already had text. Caught before a scheduled run
    fired; the failure mode was silent because an absent key just yields {}.

    Memoized per (dir, mtime of texts-meta) so a run pays the read once.
    Schema-1 manifests still take the cheap map path.
    """
    corpus_docs_dir = Path(corpus_docs_dir)
    meta_path = corpus_docs_dir / _meta_filename()
    if not meta_path.exists():
        return set()
    try:
        cache_key = (str(corpus_docs_dir), meta_path.stat().st_mtime_ns)
    except OSError:
        return set()
    if cache_key in _BUNDLED_IDS_CACHE:
        return _BUNDLED_IDS_CACHE[cache_key]
    ids: set = set()
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    legacy = meta.get("record_to_shard")
    if legacy:
        ids = set(legacy.keys())
    else:
        for entry in meta.get("shards", []):
            sp = corpus_docs_dir / entry.get("file", "")
            if not sp.exists():
                continue
            try:
                with open(sp, "r", encoding="utf-8") as f:
                    ids.update((json.load(f).get("records") or {}).keys())
            except (OSError, json.JSONDecodeError):
                continue
    _BUNDLED_IDS_CACHE[cache_key] = ids
    return ids


def write_text_shards(
    corpus_docs_dir: Path,
    items: Iterable[tuple[str, Path]],
    *,
    target_bytes: int = DEFAULT_TARGET_BYTES,
    fallback_threshold: int = DEFAULT_FALLBACK_THRESHOLD,
    r2_origin: Optional[str] = None,
    stride: int = DEFAULT_SHARD_STRIDE,
) -> dict:
    """Pack texts into shards under `<corpus_docs_dir>/texts-NN.json`.

    Returns the meta dict (also written to `texts-meta.json`).
    Cleans up any pre-existing `texts-*.json` orphans first so a run
    with fewer records doesn't leave a stale higher-index shard
    behind.

    `items` is an iterable of (composite_id, text_file_path). The order
    determines pack order — pass records in canonical sort order so
    shard contents are stable across re-runs. Items whose file is
    missing are silently skipped (the manifest records this in
    `totals.missing_files` for observability).

    `r2_origin` is recorded in the meta for app-side fallback fetches;
    pass None if no R2 fallback is configured yet (in which case
    >threshold records are skipped entirely — caller should keep the
    threshold high enough that no records hit it).
    """
    corpus_docs_dir = Path(corpus_docs_dir)
    corpus_docs_dir.mkdir(parents=True, exist_ok=True)
    items = list(items)  # materialize so we can iterate twice

    # SHARD PRESERVATION: load existing shards' record contents so that
    # records bundled in a previous run survive the next run even when
    # text/<id>.txt is no longer on disk (post-2026-05-14 cleanup leaves
    # text/ empty between scrape extractions; build_manifest's `texts`
    # map reflects only newly-extracted records). Without this read,
    # write_text_shards would wipe the existing bundle every run.
    #
    # Semantics: shards are append-only with respect to each record key.
    # Once a record's content is in a shard, it stays until either:
    #   (a) a fresh disk read of items[<key>] overwrites it this run, or
    #   (b) the caller explicitly removes the key from items AND wipes
    #       texts-meta.json (which we'd treat as a hard reset).
    existing_records: dict = {}
    existing_meta_path = corpus_docs_dir / _meta_filename()
    if existing_meta_path.exists():
        try:
            with open(existing_meta_path, "r", encoding="utf-8") as f:
                existing_meta = json.load(f)
            for shard_entry in existing_meta.get("shards", []):
                sp = corpus_docs_dir / shard_entry["file"]
                if not sp.exists():
                    continue
                with open(sp, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                for k, v in (payload.get("records") or {}).items():
                    existing_records[k] = v
        except (OSError, json.JSONDecodeError) as e:
            print(f"  ! couldn't preload existing shards ({e}) — starting fresh")
            existing_records = {}

    # Clean up previous shards. Preserve texts-meta.json (we overwrite
    # it below) — wildcard matches both shards and meta otherwise.
    # Safe to remove now because we've cached existing_records above.
    for path in glob.glob(str(corpus_docs_dir / "texts-*.json")):
        if Path(path).name == _meta_filename():
            continue
        try:
            os.remove(path)
        except OSError:
            pass

    shards: list[dict] = []                # per-shard manifest entries

    totals = {
        "records_with_text": 0,
        "shards": 0,
        "r2_fallback": 0,
        "total_text_bytes": 0,
        "missing_files": 0,
        "skipped_oversize_no_r2": 0,
        "from_existing_shards": 0,
    }

    # Build the full set of (key → value) we want in the new shards.
    # Strategy: start from existing-shard contents (preserves prior bundle),
    # then overlay any items whose disk file exists (refreshes content).
    # Records present in items but with no disk file fall back to existing
    # content; records only in existing-shards (not in items at all) stay.
    item_paths = {key: path for key, path in items}
    final_records: dict = {}

    # Pass 1: bring existing-shard records forward as-is.
    for key, value in existing_records.items():
        final_records[key] = value

    # Pass 2: for items that have a fresh disk file, overwrite with the
    # newly-extracted content. For items whose path doesn't exist,
    # preserve the existing content (do nothing — already in final_records
    # from pass 1).
    for key, path in items:
        if not path.exists():
            if key not in final_records:
                # Not in any source — count as missing for the audit log.
                totals["missing_files"] += 1
            continue
        try:
            data = path.read_bytes()
        except OSError:
            if key not in final_records:
                totals["missing_files"] += 1
            continue
        size = len(data)
        if size > fallback_threshold:
            if r2_origin is None:
                totals["skipped_oversize_no_r2"] += 1
                if key not in final_records:
                    continue
                # If existing has content, leave it as-is; don't replace with
                # an oversize-skipped record.
                continue
            final_records[key] = R2_SENTINEL
            totals["r2_fallback"] += 1
        else:
            try:
                value = data.decode("utf-8")
            except UnicodeDecodeError:
                if key not in final_records:
                    totals["missing_files"] += 1
                continue
            final_records[key] = value
        totals["total_text_bytes"] += size

    # ── Assign every record to a shard by key, never by position ──────────
    #
    # Group by shard_filename(key) and write. A shard whose membership is
    # unchanged serialises to identical bytes, so git records no new blob
    # for it — that is the whole point of the exercise.

    groups: dict[str, list[str]] = {}
    for key in final_records:
        groups.setdefault(shard_filename(key, stride), []).append(key)

    for fname in sorted(groups):
        # Sorted only WITHIN a shard, so the output bytes are stable. This
        # ordering never decides which shard a record lands in.
        keys = sorted(groups[fname])
        path = corpus_docs_dir / fname
        payload = {
            "shard": fname,
            "records": {k: final_records[k] for k in keys},
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        shards.append({
            "file": fname,
            "count": len(keys),
            "bytes": path.stat().st_size,
        })
        totals["records_with_text"] += len(keys)

    totals["shards"] = len(shards)
    totals["from_existing_shards"] = len(existing_records) - sum(
        1 for k in existing_records if k in item_paths and item_paths[k].exists()
    )

    # Clean up per-record .txt files now that they're bundled. Leaves
    # the marker files (.pypdf-empty / .ocr-failed / .pypdf-error /
    # .empty / .error) intact — those are the "we tried, this is the
    # result" memory the extract phase needs to avoid re-attempting
    # records that produced no text. Recursive glob so we catch both
    # flat layouts (text/<id>.txt) and nested ones (text/<committee>/
    # <fid>.txt, text/<house>/<fid>.txt).
    text_root = corpus_docs_dir / "text"
    removed_txt = 0
    if text_root.exists():
        for path in text_root.rglob("*.txt"):
            try:
                path.unlink()
                removed_txt += 1
            except OSError:
                pass
    totals["txt_files_removed"] = removed_txt

    # NOTE: no `record_to_shard`. It was a per-record map (121,642 entries,
    # 3.5 MB for questions) rewritten every run and downloaded by every app
    # session. The shard is now derivable from the key, so the app computes
    # it with bucketFor()/fnv1a32() and needs only `sub_counts` — ~1 KB.
    # See the INVARIANT note above before adding anything per-record here.
    meta = {
        "shard_size_target_bytes": target_bytes,
        "fallback_threshold_bytes": fallback_threshold,
        "r2_origin": r2_origin,
        "schema": 2,
        "totals": totals,
        "shard_stride": stride,
        "shards": shards,
    }
    meta_path = corpus_docs_dir / _meta_filename()
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


# ── Marker consolidation ───────────────────────────────────────────────────
#
# Per-attempt status markers (`.pypdf-empty` / `.pypdf-error` /
# `.ocr-failed`) live as sidecar files in text_dir, one per record.
# Without consolidation they accumulate forever (write_text_shards
# cleans `*.txt` but not markers) and push corpora close to CF's
# 20,000-file deploy cap.
#
# consolidate_markers() bundles them into a single markers.json per
# corpus and DELETES the on-disk sidecars. Subsequent runs:
#   - compute_audit reads markers.json (instead of scanning the dir)
#   - extract candidate selection reads markers.json (to skip
#     records already marked, except .pypdf-error which is retryable)
#
# Shape of markers.json:
#   {
#     "generated_at": "ISO-Z",
#     "totals": {"pypdf-empty": N, "pypdf-error": N, "ocr-failed": N},
#     "markers": {"<composite_id>": "<marker-type>", ...},
#   }


from typing import Callable as _Callable
import datetime as _datetime


def _markers_filename() -> str:
    return "markers.json"


DEFAULT_MARKER_SUFFIXES = ("pypdf-empty", "pypdf-error", "ocr-failed")


def consolidate_markers(
    corpus_docs_dir: Path,
    text_dir: Path,
    *,
    composite_id_from_path: Optional[_Callable[[Path], str]] = None,
    drop_record_ids: Optional[set] = None,
    marker_suffixes: tuple = DEFAULT_MARKER_SUFFIXES,
) -> dict:
    """Scan text_dir recursively for marker files, merge them into
    markers.json under corpus_docs_dir, then DELETE the on-disk
    sidecar marker files.

    Each marker file `<...>.<suffix>` (suffix in marker_suffixes) is
    interpreted as: "record <composite_id> has marker <suffix>".

    composite_id_from_path: callable that returns the composite_id
    string for a given marker Path. Default: `path.stem` (strips the
    final `.<suffix>`). Corpora with nested text/<group>/<fid>.<suffix>
    layouts pass a function that includes the group prefix, e.g.:
        lambda p: f"{p.parent.name}|{p.stem}"

    drop_record_ids: composite_ids of records that now have bundled
    text (their markers should be REMOVED — text takes precedence
    over any past marker). Pass the set of record_to_shard keys from
    texts-meta.json.

    marker_suffixes: which sidecar suffixes to consolidate. Defaults
    to the three project-wide statuses.

    Returns the resulting markers dict (also written to disk).

    Existing markers.json is preserved and merged; never reset by this
    function alone. Manual delete needed if a clean rebuild is desired.
    """
    corpus_docs_dir = Path(corpus_docs_dir)
    text_dir = Path(text_dir)
    composite_id_from_path = composite_id_from_path or (lambda p: p.stem)
    drop_record_ids = drop_record_ids or set()

    # Load existing markers.
    existing: dict = {}
    meta_path = corpus_docs_dir / _markers_filename()
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            existing = dict(payload.get("markers") or {})
        except (OSError, json.JSONDecodeError) as e:
            print(f"  ! couldn't read existing markers.json ({e}) — starting fresh")
            existing = {}

    # Scan for new sidecar markers + remember the paths to delete.
    new_markers: dict[str, str] = {}
    scanned_paths: list[Path] = []
    if text_dir.exists():
        for suffix in marker_suffixes:
            for path in text_dir.rglob(f"*.{suffix}"):
                if not path.is_file():
                    continue
                cid = composite_id_from_path(path)
                new_markers[cid] = suffix
                scanned_paths.append(path)

    # Merge: existing carries forward; new wins on conflict (the
    # most-recent attempt's outcome is authoritative).
    final = dict(existing)
    final.update(new_markers)

    # Drop records that now have text. Text precedence.
    for cid in drop_record_ids:
        final.pop(cid, None)

    # Compute totals.
    totals: dict[str, int] = {s: 0 for s in marker_suffixes}
    for v in final.values():
        if v in totals:
            totals[v] += 1

    out = {
        "generated_at": _datetime.datetime.now(_datetime.timezone.utc)
                            .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totals":  totals,
        "markers": final,
    }
    # Idempotent: skip the markers.json write if only `generated_at`
    # would change. Cuts CF build churn for corpora that are quiet
    # between derive runs.
    wrote = write_json_idempotent(meta_path, out)
    if not wrote:
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                out["generated_at"] = json.load(f).get("generated_at", out["generated_at"])
        except (OSError, json.JSONDecodeError):
            pass

    # Delete the on-disk sidecars we just consolidated. (Defensive: only
    # delete paths we scanned this call — don't recursively unlink
    # anything else.)
    removed = 0
    for path in scanned_paths:
        try:
            path.unlink()
            removed += 1
        except OSError:
            pass
    out["removed_sidecar_count"] = removed

    return out


def load_markers(corpus_docs_dir: Path) -> dict[str, str]:
    """Read markers.json's `markers` dict (composite_id → marker_type).
    Empty dict on missing/unreadable.
    """
    p = Path(corpus_docs_dir) / _markers_filename()
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return dict((json.load(f).get("markers") or {}))
    except (OSError, json.JSONDecodeError):
        return {}


# ── Idempotent JSON write: skip no-op timestamp-only changes ───────────────
#
# Every derive run writes meta.json / audit.json / markers.json with a
# fresh `generated_at` (or `audited_at`) timestamp. If nothing else
# changed, the file still diffs against origin/main, triggers a commit,
# triggers a Cloudflare build — burning monthly build quota for no real
# user-visible change. write_json_idempotent loads the existing file,
# compares every key except the timestamp keys, and skips the write
# entirely when the meaningful content matches.
#
# When the helper skips a write, the live mirror's `generated_at` stays
# anchored to the last meaningful data change. The staleness indicator
# in the SansadSaar app reads that timestamp; a "quiet" corpus with no
# new data correctly shows the timestamp of the last real change rather
# than a misleading "just rebuilt" timestamp on every cron tick.

_DEFAULT_TIMESTAMP_KEYS: tuple = ("generated_at", "audited_at")


def write_json_idempotent(
    path,
    content: dict,
    *,
    ignore_keys: tuple = _DEFAULT_TIMESTAMP_KEYS,
    indent: int = 2,
) -> bool:
    """Write `content` to `path` as JSON, but skip the write if every
    field besides those in `ignore_keys` matches the existing file.

    Returns True if the file was written, False if it was a no-op skip.

    Defaults ignore the standard "build-time" timestamp keys. Pass a
    different `ignore_keys` to extend / override.

    Falls back to writing on any read/parse error of the existing file
    (safer to overwrite than to leave stale).
    """
    p = Path(path)
    if p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, dict) and isinstance(content, dict):
                existing_check = {k: v for k, v in existing.items() if k not in ignore_keys}
                new_check      = {k: v for k, v in content.items()  if k not in ignore_keys}
                if existing_check == new_check:
                    return False
        except (OSError, json.JSONDecodeError):
            pass   # fall through and write
    with open(p, "w", encoding="utf-8") as f:
        json.dump(content, f, ensure_ascii=False, indent=indent)
    return True
