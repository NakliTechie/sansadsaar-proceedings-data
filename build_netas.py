#!/usr/bin/env python3
"""Netas derive — pivot Parliament corpora along the MP (mpsno) dimension.

This is the Phase 0b derive that produces the per-MP shards consumed by the
Netas Astro app. Run by .github/workflows/netas-build.yml; the cron tars the
output and commits docs/netas-data.tar.gz, which Cloudflare serves from
sansadsaar-proceedings.naklitechie.com. Netas CI in indiavotes/netas curls
that tarball at deploy time. Single tarball (vs. ~16k per-MP files) keeps
the proceedings deploy comfortably under Workers Static Assets' 20k cap.

Inputs:
  docs/debates/reports-ls-*.json     (~56k records; members carry mp_code) — in this repo
  docs/questions/reports-ls-*.json   (~500+ records; members[] are free-text) — in this repo
  $NETAS_BILLS_DIR/index-*.json      (~10k records; billIntroducedBy is free-text)
                                     The workflow fetches these from
                                     naklitechie.github.io/parliamentwatch-data/bills/
                                     into a temp dir. Local default:
                                     ../parliamentwatch-data/docs/bills/.
  scripts/recon-netas-out/sansad_master.json (4,425-entry roster; produced
                                              by scripts/fetch_member_master.py)

Outputs (under docs/netas/):
  index.json                  - build manifest with totals + generated_at
  mps.json                    - slim list of all MPs (for directory/search)
  profile/<mpsno>.json        - per-MP detail rollup
  speeches/<mpsno>.json       - per-MP debate list (newest first)
  questions/<mpsno>.json      - per-MP question list
  bills/<mpsno>.json          - per-MP bill list
  stats/
    by-state.json
    by-party.json
    by-ministry.json          (driven by question / bill ministry fields)
    by-session.json
    by-house.json
  attribution.json            - {free-text-name-string: mpsno} for audit
  unmatched.json              - free-text-name-strings we couldn't attribute

The free-text resolver mirrors the matcher validated in
scripts/recon_netas_normaliser.py (see plan/netas-normaliser-recon-001-v0.1.md
for the accuracy memo).
"""

from __future__ import annotations

import json
import os
import re
import time
from collections import Counter, defaultdict
from pathlib import Path

from rapidfuzz import fuzz, process

REPO = Path(__file__).resolve().parent
DEBATES_DIR = REPO / "docs" / "debates"
QUESTIONS_DIR = REPO / "docs" / "questions"
# Bills live in NakliTechie/parliamentwatch-data and are served at its GH
# Pages CDN. In CI we fetch the index-*.json shards into a temp directory
# and point BILLS_DIR at it. Local runs default to ../parliamentwatch-data/
# docs/bills/ (which is the convention from when build_netas.py lived in
# the parliamentwatch-data repo).
BILLS_DIR = Path(
    os.environ.get(
        "NETAS_BILLS_DIR",
        str(REPO.parent / "parliamentwatch-data" / "docs" / "bills"),
    )
)
SANSAD_MASTER_FILE = REPO / "scripts" / "recon-netas-out" / "sansad_master.json"
SANSAD_TERMS_FILE = REPO / "scripts" / "recon-netas-out" / "sansad_terms.json"
TCPD_HISTORY_DIR = REPO / "tcpd" / "derived" / "history"
OUT_DIR = REPO / "docs" / "netas"

CURRENT_LS = 18  # focus term for the launch; older terms are cross-reference only

# Year in which each Lok Sabha was constituted (= election year).
LS_YEAR = {14: 2004, 15: 2009, 16: 2014, 17: 2019, 18: 2024}


# ── Normaliser + matcher (mirrors scripts/recon_netas_normaliser.py) ──────

LEADING_HONORIFIC_RE = re.compile(
    r"^\s*(shri|shrimati|smt\.?|dr\.?|ms\.?|mr\.?|mrs\.?|prof\.?|adv\.?|sh\.?|km\.?)\s+",
    re.IGNORECASE,
)
TRAILING_HONORIFIC_RE = re.compile(
    r"\s+(shri|shrimati|smt\.?|dr\.?|ms\.?|mr\.?|mrs\.?|prof\.?|adv\.?|sh\.?|km\.?)\s*$",
    re.IGNORECASE,
)
DEVANAGARI_LEADING_RE = re.compile(r"^(श्री|श्रीमती|डॉ\.?|डा\.?|कुमारी|कु\.?)\s+")


def normalize(name: str) -> str:
    if not name:
        return ""
    s = name.strip()
    while True:
        new_s = DEVANAGARI_LEADING_RE.sub("", s)
        if new_s == s:
            break
        s = new_s
    while True:
        new_s = LEADING_HONORIFIC_RE.sub("", s)
        new_s = TRAILING_HONORIFIC_RE.sub("", new_s)
        if new_s == s:
            break
        s = new_s
    s = re.sub(r"[.,;:()\[\]'\"]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _shared_tokens(a: str, b: str) -> int:
    ta = {t for t in a.split() if len(t) >= 3}
    tb = {t for t in b.split() if len(t) >= 3}
    return len(ta & tb)


def derive_sitting(sm: dict | None) -> bool:
    """Compute the `sitting` flag from a sansad master record.

    Sansad's raw `sitting` boolean is unreliable for ex-LS MPs (e.g. Advani's
    LS16 record carries sitting=True even though status="Former" and his
    last term ended in 2019). The authoritative signal is `status`:
      - LS:  status == "Sitting" AND loksabha == CURRENT_LS
      - RS:  status == "Sitting"
    Everything else is not currently sitting.
    """
    if not sm:
        return False
    status = (sm.get("status") or "").strip()
    if sm.get("house") == "ls":
        return status == "Sitting" and sm.get("loksabha") == CURRENT_LS
    if sm.get("house") == "rs":
        return status == "Sitting"
    return False


# ── Master + lookup construction ──────────────────────────────────────────


def load_sansad_terms() -> dict[tuple, list[dict]]:
    """Return {(house, mpsno): [term_record, ...]} from sansad_terms.json.

    Dedupes by (house, mpsno, loksabha): the sansad sitting=1 + sitting=0
    endpoints overlap, so we collapse to the sitting record if present.
    """
    out: dict[tuple, dict[int, dict]] = defaultdict(dict)
    if not SANSAD_TERMS_FILE.exists():
        return {k: list(v.values()) for k, v in out.items()}
    raw = json.loads(SANSAD_TERMS_FILE.read_text())
    for rec in raw:
        h = rec.get("house")
        mpsno = rec.get("mpsno")
        ls = rec.get("loksabha")
        if h is None or mpsno is None:
            continue
        key = (h, mpsno)
        # For LS, dedup by (loksabha). For RS, dedup by mpsno (only one term
        # tracked per RS member in the current data).
        subkey = ls if h == "ls" else 0
        existing = out[key].get(subkey)
        if existing is None or (rec.get("sitting") and not existing.get("sitting")):
            out[key][subkey] = rec
    return {k: list(v.values()) for k, v in out.items()}


def load_sansad_master() -> dict[int, dict]:
    if not SANSAD_MASTER_FILE.exists():
        raise FileNotFoundError(
            f"Missing {SANSAD_MASTER_FILE}. Run scripts/fetch_member_master.py first."
        )
    by_mpsno: dict[int, dict] = {}
    for rec in json.loads(SANSAD_MASTER_FILE.read_text()):
        mpsno = rec.get("mpsno")
        if mpsno is None:
            continue
        existing = by_mpsno.get(mpsno)
        # Prefer LS over RS for same mpsno, sitting over former.
        if existing is None:
            by_mpsno[mpsno] = rec
            continue
        e_house = existing.get("house")
        n_house = rec.get("house")
        if e_house == "rs" and n_house == "ls":
            by_mpsno[mpsno] = rec
        elif e_house == n_house and not existing.get("sitting") and rec.get("sitting"):
            by_mpsno[mpsno] = rec
    return by_mpsno


def load_debate_variants() -> dict[int, Counter]:
    """Walk debate shards, yield variants[mp_code] = Counter[mp_name]."""
    variants: dict[int, Counter] = defaultdict(Counter)
    for shard in sorted(DEBATES_DIR.glob("reports-ls-*.json")):
        for rec in json.loads(shard.read_text()).get("records", []):
            for m in rec.get("members", []):
                code = m.get("mp_code")
                name = (m.get("mp_name") or "").strip()
                if code is None or not name:
                    continue
                variants[code][name] += 1
    return variants


def build_unified_master(
    sansad: dict[int, dict],
    debate_variants: dict[int, Counter],
) -> dict[int, dict]:
    """Merge sansad roster + debate variants -> {mpsno: unified record}."""
    all_codes = set(sansad) | set(debate_variants)
    unified: dict[int, dict] = {}
    for code in all_codes:
        sm = sansad.get(code)
        dv = debate_variants.get(code, Counter())
        variants: set[str] = set(dv.keys())
        if sm:
            for k in ("full_name_lastfirst", "full_name_firstlast"):
                v = (sm.get(k) or "").strip()
                if v:
                    variants.add(v)
            fn = (sm.get("first_name") or "").strip()
            ln = (sm.get("last_name") or "").strip()
            if fn and ln:
                variants.add(f"{fn} {ln}")
                variants.add(f"{ln} {fn}")
        if dv:
            canonical = dv.most_common(1)[0][0]
        elif sm and sm.get("full_name_firstlast"):
            canonical = sm["full_name_firstlast"]
        elif sm and sm.get("full_name_lastfirst"):
            canonical = sm["full_name_lastfirst"]
        elif variants:
            canonical = next(iter(variants))
        else:
            continue
        unified[code] = {
            "mpsno": code,
            "canonical": canonical,
            "normalized": normalize(canonical),
            "variants": sorted(variants),
            "speech_count_raw": sum(dv.values()),
            "house": sm.get("house") if sm else "ls",  # default ls if only seen in debates
            "sansad": sm,
        }
    return unified


# ── Person-level merge (cross-house, cross-term) ──────────────────────────


_COMMA_FLIP_RE = re.compile(r"^([^,]+),\s*(.+)$")


def _person_key(name: str) -> str:
    """Normalize a name down to a comparison key.

    Sansad surfaces the same MP under multiple formats — "Smt. Sonia Gandhi"
    in LS, "Gandhi, Smt. Sonia" in RS. We flip "Last, First" to "First Last",
    strip leading honorifics, and lowercase, so both produce "sonia gandhi".
    """
    if not name:
        return ""
    s = name.strip()
    m = _COMMA_FLIP_RE.match(s)
    if m:
        s = f"{m.group(2)} {m.group(1)}"
    s = LEADING_HONORIFIC_RE.sub("", s)
    s = TRAILING_HONORIFIC_RE.sub("", s)
    s = DEVANAGARI_LEADING_RE.sub("", s)
    s = re.sub(r"[^a-zA-Z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _normalize_dob(dob: str | None) -> str | None:
    """Normalize DD/MM/YYYY or YYYY-MM-DD into a comparable string."""
    if not dob:
        return None
    s = dob.strip()
    if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", s):
        d, mo, y = s.split("/")
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", s):
        y, mo, d = s.split("-")
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    return s


def compute_person_aliases(unified: dict[int, dict]) -> dict[int, int]:
    """Return {alias_mpsno: canonical_mpsno} for mpsnos representing the same person.

    Merge strategy, in order:
      1. Same person-key + same normalized DOB (both records have DOB).
         High confidence — two records with identical name and DOB are
         essentially never different people in this dataset.
      2. Same person-key with DOB on at most one side (the other missing).
         Reasonably confident — sansad's missing-DOB rate is ~33%; when one
         side has it and the other doesn't, name match plus no contradiction
         is good enough.
      3. Same person-key but DOBs differ. Treated as different people
         (real namesakes — e.g. "Ajit Singh" with different birth dates).

    Canonical mpsno: lowest in the group. Stable across rebuilds.
    """
    # Group mpsnos by person-key
    groups: dict[str, list[tuple[int, str | None]]] = defaultdict(list)
    for code, info in unified.items():
        sm = info.get("sansad") or {}
        # Prefer first_name+last_name from master (already format-normalized);
        # fall back to canonical name.
        fn = (sm.get("first_name") or "").strip()
        ln = (sm.get("last_name") or "").strip()
        if fn and ln:
            key = _person_key(f"{fn} {ln}")
        else:
            key = _person_key(info.get("canonical") or "")
        if not key:
            continue
        groups[key].append((code, _normalize_dob(sm.get("dob"))))

    aliases: dict[int, int] = {}
    for key, members in groups.items():
        if len(members) < 2:
            continue
        # Split by DOB compatibility — records with the same DOB cluster,
        # records with NULL DOB attach to whichever cluster they're compatible
        # with (i.e. the only one, if there's just one DOB-bearing cluster).
        by_dob: dict[str, list[int]] = defaultdict(list)
        no_dob: list[int] = []
        for code, dob in members:
            if dob:
                by_dob[dob].append(code)
            else:
                no_dob.append(code)

        if len(by_dob) >= 2:
            # Multiple distinct DOBs — these are different people who share
            # a name. Leave them separate. Attach no-dob records to nothing.
            for codes in by_dob.values():
                if len(codes) > 1:
                    canonical = min(codes)
                    for c in codes:
                        if c != canonical:
                            aliases[c] = canonical
            continue

        # Either one DOB cluster or none — collapse the whole group to one canonical.
        all_codes = no_dob + [c for codes in by_dob.values() for c in codes]
        canonical = min(all_codes)
        for c in all_codes:
            if c != canonical:
                aliases[c] = canonical

    return aliases


def apply_person_aliases(
    unified: dict[int, dict],
    aliases: dict[int, int],
    debate_variants: dict[int, Counter],
) -> dict[int, dict]:
    """Collapse alias mpsnos into their canonical entries.

    Variants are unioned. Sansad metadata is picked from the most "useful"
    member: prefer currently-sitting LS18 > currently-sitting RS > most recent
    > anything. Speech-count is summed.
    """
    if not aliases:
        return unified

    # Build groups: canonical -> list of mpsnos (incl. canonical itself)
    groups: dict[int, list[int]] = defaultdict(list)
    for code in unified:
        canonical = aliases.get(code, code)
        groups[canonical].append(code)

    def _sansad_rank(sm: dict | None) -> tuple:
        # Lower is better.
        if not sm:
            return (9, 0, 0)
        if sm.get("sitting") and sm.get("house") == "ls" and sm.get("loksabha") == CURRENT_LS:
            return (0,)
        if sm.get("sitting") and sm.get("house") == "rs":
            return (1,)
        if sm.get("sitting"):
            return (2, -(sm.get("loksabha") or 0))
        return (3, -(sm.get("loksabha") or 0))

    merged: dict[int, dict] = {}
    for canonical, members in groups.items():
        if len(members) == 1:
            merged[canonical] = unified[members[0]]
            continue
        # Choose the best sansad record for display (current sitting > recent)
        best_code = min(members, key=lambda c: _sansad_rank((unified.get(c) or {}).get("sansad")))
        best = unified[best_code]
        variants: set[str] = set()
        for c in members:
            variants.update(unified[c]["variants"])
        # Pick the friendliest display name: "Honorific First Last" reads better
        # than the RS-API's "Last, Honorific First". Walk every member's sansad
        # record and grab the first firstlast we find; fall back to whatever
        # `best` had.
        display = None
        for c in members:
            sm = (unified.get(c) or {}).get("sansad") or {}
            fl = (sm.get("full_name_firstlast") or "").strip()
            if fl:
                display = fl
                break
        if not display:
            display = best["canonical"]
        merged[canonical] = {
            "mpsno": canonical,
            "canonical": display,
            "normalized": normalize(display),
            "variants": sorted(variants),
            "speech_count_raw": sum(unified[c]["speech_count_raw"] for c in members),
            "house": best["house"],
            "sansad": best["sansad"],
        }

    # Fold alias debate_variants into the canonical's bucket so the resolver
    # still picks up speech attributions from the alias's name variants.
    for alias, canonical in aliases.items():
        if alias in debate_variants:
            debate_variants.setdefault(canonical, Counter()).update(debate_variants[alias])
            debate_variants.pop(alias, None)

    return merged


def build_lookup(unified: dict[int, dict]) -> dict[str, list[int]]:
    lookup: dict[str, list[int]] = defaultdict(list)
    for code, info in unified.items():
        for v in info["variants"]:
            n = normalize(v)
            if not n:
                continue
            if code not in lookup[n]:
                lookup[n].append(code)
        n = info["normalized"]
        if code not in lookup[n]:
            lookup[n].append(code)
    return dict(lookup)


# ── Free-text -> mpsno resolution ─────────────────────────────────────────


class Attribution:
    """Caches name-string -> [mpsno, ...] lookups; tracks unmatched strings."""

    def __init__(self, unified: dict[int, dict], lookup: dict[str, list[int]]):
        self.unified = unified
        self.lookup = lookup
        self.candidates = list(lookup.keys())
        self.cache: dict[str, dict] = {}
        self.unmatched: Counter = Counter()

    def resolve(self, raw: str, prefer_house: str | None = None) -> dict:
        """Resolve a free-text name to {mpsnos: [...], status: str, score: int}.

        prefer_house: if set, when ambiguous_exact returns multiple mpsnos,
        narrow to those whose house matches prefer_house. (e.g. an LS
        question or LS bill should prefer LS-house mpsnos.)
        """
        if raw in self.cache:
            cached = self.cache[raw]
            if not prefer_house or len(cached["mpsnos"]) <= 1:
                return cached
        n = normalize(raw)
        result: dict
        if not n:
            result = {"mpsnos": [], "status": "empty", "score": 0}
        elif n in self.lookup:
            codes = list(self.lookup[n])
            if prefer_house:
                filtered = [c for c in codes if self.unified[c]["house"] == prefer_house]
                if filtered:
                    codes = filtered
            if len(codes) == 1:
                result = {"mpsnos": codes, "status": "exact", "score": 100}
            else:
                result = {"mpsnos": codes, "status": "ambiguous_exact", "score": 100}
        else:
            best = process.extractOne(n, self.candidates, scorer=fuzz.token_sort_ratio)
            if not best:
                result = {"mpsnos": [], "status": "unmatched", "score": 0}
            else:
                matched_norm, score, _ = best
                # Subset rescue
                if score < 92:
                    partial = fuzz.partial_token_set_ratio(n, matched_norm)
                    if partial >= 95 and _shared_tokens(n, matched_norm) >= 2:
                        score = max(score, partial)
                codes = list(self.lookup.get(matched_norm, []))
                if prefer_house and len(codes) > 1:
                    filtered = [c for c in codes if self.unified[c]["house"] == prefer_house]
                    if filtered:
                        codes = filtered
                if score >= 92:
                    shared = _shared_tokens(n, matched_norm)
                    confident = score >= 95 or shared >= 2
                    if confident and codes:
                        status = "fuzzy_high" if len(codes) == 1 else "ambiguous_fuzzy"
                        result = {"mpsnos": codes, "status": status, "score": int(score)}
                    else:
                        result = {"mpsnos": codes, "status": "review", "score": int(score)}
                else:
                    result = {"mpsnos": [], "status": "unmatched", "score": int(score)}
        self.cache[raw] = result
        if result["status"] in ("unmatched", "empty"):
            self.unmatched[raw] += 1
        return result


# ── Pivot ─────────────────────────────────────────────────────────────────


def pivot(unified: dict[int, dict], attrib: Attribution) -> dict:
    """Walk all corpora, build per-MP feeds + global stats."""
    # Per-MP feeds
    speeches_by_mp: dict[int, list] = defaultdict(list)
    questions_by_mp: dict[int, list] = defaultdict(list)
    bills_by_mp: dict[int, list] = defaultdict(list)

    # Global stats
    by_state: Counter = Counter()
    by_party: Counter = Counter()
    by_ministry_q: Counter = Counter()
    by_ministry_b: Counter = Counter()
    by_session: Counter = Counter()
    by_debate_type: Counter = Counter()

    print("  walking debates ...", flush=True)
    for shard in sorted(DEBATES_DIR.glob("reports-ls-*.json")):
        for rec in json.loads(shard.read_text()).get("records", []):
            entry = {
                "house": rec.get("house"),
                "lok_sabha": rec.get("lok_sabha"),
                "session": rec.get("session"),
                "db_slno": rec.get("db_slno"),
                "title": rec.get("title"),
                "date": rec.get("debate_date"),
                "type_code": rec.get("debate_type"),
                "type_desc": rec.get("debate_type_desc"),
            }
            for m in rec.get("members", []):
                code = m.get("mp_code")
                if code is None:
                    continue
                speeches_by_mp[code].append(entry)
            if rec.get("lok_sabha") == CURRENT_LS:
                by_session[("ls", rec.get("session"))] += 1
                if rec.get("debate_type_desc"):
                    by_debate_type[rec["debate_type_desc"]] += 1

    print("  walking questions ...", flush=True)
    for shard in sorted(QUESTIONS_DIR.glob("reports-ls-*.json")):
        for rec in json.loads(shard.read_text()).get("records", []):
            entry = {
                "house": rec.get("house"),
                "lok_sabha": rec.get("lok_sabha"),
                "session": rec.get("session"),
                "question_no": rec.get("question_no"),
                "type": rec.get("type"),
                "subject": rec.get("subject"),
                "ministry": rec.get("ministry"),
                "date": rec.get("date"),
                "supplementary": rec.get("supplementary"),
            }
            attributed = False
            for raw in rec.get("members", []):
                r = attrib.resolve(raw, prefer_house="ls")
                if r["mpsnos"]:
                    attributed = True
                    for code in r["mpsnos"]:
                        questions_by_mp[code].append({**entry, "attribution_status": r["status"]})
            if attributed and rec.get("ministry"):
                by_ministry_q[rec["ministry"]] += 1

    print("  walking bills ...", flush=True)
    for shard in sorted(BILLS_DIR.glob("index-*.json")):
        for rec in json.loads(shard.read_text()).get("records", []):
            entry = {
                "bill_number": rec.get("billNumber"),
                "bill_name": rec.get("billName"),
                "bill_type": rec.get("billType"),
                "bill_category": rec.get("billCategory"),
                "ministry": rec.get("ministryName"),
                "house": rec.get("billIntroducedInHouse"),
                "year": rec.get("billYear"),
                "date": rec.get("billIntroducedDate"),
                "status": rec.get("status"),
                "composite_id": rec.get("compositeId"),
            }
            raw = rec.get("billIntroducedBy")
            if not raw:
                continue
            house_pref = "ls" if rec.get("billIntroducedInHouse") == "Lok Sabha" else "rs"
            r = attrib.resolve(raw, prefer_house=house_pref)
            for code in r["mpsnos"]:
                bills_by_mp[code].append({**entry, "attribution_status": r["status"]})
            if r["mpsnos"] and rec.get("ministryName"):
                by_ministry_b[rec["ministryName"]] += 1

    # Sort feeds by date desc (best-effort — string sort on ISO-ish dates;
    # debate dates are DD/MM/YYYY, bill dates are YYYY-MM-DD, question dates
    # are DD.MM.YYYY — handle each format).
    def _sort_speeches(s):
        d = s.get("date") or ""
        # DD/MM/YYYY
        parts = d.split("/")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return d

    def _sort_questions(q):
        d = q.get("date") or ""
        parts = d.split(".")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return d

    for code in speeches_by_mp:
        speeches_by_mp[code].sort(key=_sort_speeches, reverse=True)
    for code in questions_by_mp:
        questions_by_mp[code].sort(key=_sort_questions, reverse=True)
    for code in bills_by_mp:
        bills_by_mp[code].sort(key=lambda b: b.get("date") or "", reverse=True)

    # State / party rollups for sitting LS18 MPs
    for code, info in unified.items():
        sm = info["sansad"]
        if not sm or sm.get("house") != "ls" or not derive_sitting(sm):
            continue
        if sm.get("loksabha") != CURRENT_LS:
            continue
        st = (sm.get("state") or "").strip()
        if st:
            by_state[st] += 1
        pt = sm.get("party_short") or sm.get("party_full") or ""
        if pt:
            by_party[pt] += 1

    return {
        "speeches_by_mp": speeches_by_mp,
        "questions_by_mp": questions_by_mp,
        "bills_by_mp": bills_by_mp,
        "stats": {
            "by_state": dict(by_state.most_common()),
            "by_party": dict(by_party.most_common()),
            "by_ministry_questions": dict(by_ministry_q.most_common()),
            "by_ministry_bills": dict(by_ministry_b.most_common()),
            "by_session": {f"{h}-{s}": c for (h, s), c in by_session.most_common()},
            "by_debate_type": dict(by_debate_type.most_common()),
        },
    }


# ── Output writing ────────────────────────────────────────────────────────


def load_tcpd_history(mpsno: int, house: str) -> dict | None:
    """Read TCPD-derived history for an MP. Files are keyed by `{house}-{mpsno}`
    because sansad's mpsno is NOT globally unique — LS and RS namespaces reuse
    the same integers (~274 collisions). We must read the entry that matches
    the house we ended up with in the unified master."""
    p = TCPD_HISTORY_DIR / f"{house}-{mpsno}.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())


def summarise_electoral(history: dict | None) -> dict | None:
    """Slim summary stats from a full TCPD history (for embedding in profile.json).

    Includes a `combined` block that unions GE + AE — important for the career
    view because a neta's journey crosses both houses (e.g. CM-turned-MP).
    """
    if not history:
        return None
    ge = history.get("ge_history") or []
    ae = history.get("ae_history") or []

    def summarise(rows: list[dict], source: str | None = None) -> dict | None:
        if not rows:
            return None
        wins = sum(1 for r in rows if r.get("Position") == 1)
        parties = {r.get("Party") for r in rows if r.get("Party")}
        constituencies = {r.get("Constituency_Name") for r in rows if r.get("Constituency_Name")}
        years = [r.get("Year") for r in rows if r.get("Year")]
        latest = rows[-1] if rows else None
        out = {
            "contests": len(rows),
            "wins": wins,
            "parties": sorted(parties),
            "constituencies": sorted(constituencies),
            "first_year": min(years) if years else None,
            "last_year": max(years) if years else None,
            "latest": {
                "year": latest.get("Year") if latest else None,
                "party": latest.get("Party") if latest else None,
                "position": latest.get("Position") if latest else None,
                "constituency": latest.get("Constituency_Name") if latest else None,
                "vote_share": latest.get("Vote_Share_Percentage") if latest else None,
                "margin_percentage": latest.get("Margin_Percentage") if latest else None,
            } if latest else None,
        }
        if source:
            out["source"] = source
        return out

    # Combined view — merge both streams chronologically.
    # Tag each row with its source so the consumer can keep them distinct.
    combined_rows = (
        [{**r, "_source": "GE"} for r in ge]
        + [{**r, "_source": "AE"} for r in ae]
    )
    combined_rows.sort(key=lambda r: (r.get("Year") or 0, r["_source"]))
    combined_parties = {r.get("Party") for r in combined_rows if r.get("Party")}
    combined_consts = {r.get("Constituency_Name") for r in combined_rows if r.get("Constituency_Name")}
    combined_states = {r.get("State_Name") for r in combined_rows if r.get("State_Name")}
    combined_years = [r.get("Year") for r in combined_rows if r.get("Year")]
    combined = {
        "contests": len(combined_rows),
        "ge_contests": len(ge),
        "ae_contests": len(ae),
        "wins": sum(1 for r in combined_rows if r.get("Position") == 1),
        "ge_wins": sum(1 for r in ge if r.get("Position") == 1),
        "ae_wins": sum(1 for r in ae if r.get("Position") == 1),
        "parties": sorted(combined_parties),
        "constituencies": sorted(combined_consts),
        "states": sorted(s for s in combined_states if s),
        "first_year": min(combined_years) if combined_years else None,
        "last_year": max(combined_years) if combined_years else None,
    } if combined_rows else None

    return {
        "ge_pid": history.get("ge_pid"),
        "ae_pid": history.get("ae_pid"),
        "ge": summarise(ge, "GE"),
        "ae": summarise(ae, "AE"),
        "combined": combined,
    }


def _merge_tcpd_history(canonical: int, alias_codes: list[int], house: str) -> dict | None:
    """Combine TCPD electoral records across alias mpsnos.

    A merged-person can have a TCPD record under the LS mpsno (with GE contests)
    AND under the RS mpsno (with AE contests if state-assembly history was
    linked there). We dedupe by (year, constituency, position) so the same
    contest isn't double-counted when both alias mpsnos got TCPD-linked.

    NOTE: TCPD's `pid` is a PERSON identifier, not a contest one — every
    contest the same MP fought shares the same pid. An earlier version of
    this function deduped on pid and silently collapsed every MP's full
    career to one row.
    """
    all_codes = [canonical] + alias_codes
    merged_ge: list[dict] = []
    merged_ae: list[dict] = []
    seen_ge: set[tuple] = set()
    seen_ae: set[tuple] = set()
    ge_pid = None
    ae_pid = None

    def _contest_key(r: dict) -> tuple:
        # (year, constituency, position) uniquely identifies a contest
        # for a given person (only one of them wins position 1).
        return (r.get("Year"), (r.get("Constituency_Name") or "").upper(), r.get("Position"))

    for c in all_codes:
        h = load_tcpd_history(c, house)
        if not h:
            # The opposite-house file may carry AE history for a cross-house
            # member; try the other side too.
            other = "rs" if house == "ls" else "ls"
            h = load_tcpd_history(c, other)
        if not h:
            continue
        if h.get("ge_pid") and not ge_pid:
            ge_pid = h["ge_pid"]
        if h.get("ae_pid") and not ae_pid:
            ae_pid = h["ae_pid"]
        for r in h.get("ge_history", []):
            k = _contest_key(r)
            if k in seen_ge:
                continue
            seen_ge.add(k)
            merged_ge.append(r)
        for r in h.get("ae_history", []):
            k = _contest_key(r)
            if k in seen_ae:
                continue
            seen_ae.add(k)
            merged_ae.append(r)
    if not merged_ge and not merged_ae:
        return None
    return {
        "mpsno": canonical,
        "ge_pid": ge_pid,
        "ae_pid": ae_pid,
        "ge_history": sorted(merged_ge, key=lambda r: r.get("Year") or 0),
        "ae_history": sorted(merged_ae, key=lambda r: r.get("Year") or 0),
    }


def write_outputs(unified: dict[int, dict], pivots: dict, attrib: Attribution,
                  terms_lookup: dict | None = None,
                  aliases: dict[int, int] | None = None) -> dict:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "profile").mkdir(exist_ok=True)
    (OUT_DIR / "speeches").mkdir(exist_ok=True)
    (OUT_DIR / "questions").mkdir(exist_ok=True)
    (OUT_DIR / "bills").mkdir(exist_ok=True)
    (OUT_DIR / "stats").mkdir(exist_ok=True)
    (OUT_DIR / "electoral").mkdir(exist_ok=True)

    # Invert aliases -> {canonical: [alias_codes...]} for TCPD merging below.
    aliases = aliases or {}
    aliases_by_canonical: dict[int, list[int]] = defaultdict(list)
    for alias, canonical in aliases.items():
        aliases_by_canonical[canonical].append(alias)

    # Slugger lives here so both mps.json and the stats/* outputs use the same one.
    def slugify(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"[^a-z0-9]+", "-", s)
        return s.strip("-")

    # mps.json — slim listing. state_slug + cons_slug let Astro build the
    # IV-aligned `/netas/<state>/<constituency>/` URL scheme for MP profiles.
    slim = []
    for code, info in unified.items():
        sm = info["sansad"] or {}
        state = (sm.get("state") or "").strip()
        constituency = sm.get("constituency")
        slim.append({
            "mpsno": code,
            "name": info["canonical"],
            "house": info["house"],
            "loksabha": sm.get("loksabha"),
            "sitting": derive_sitting(sm),
            "state": state,
            "state_slug": slugify(state) if state else None,
            "party": sm.get("party_short") or sm.get("party_full"),
            "party_slug": slugify(sm.get("party_short") or sm.get("party_full") or "") or None,
            "constituency": constituency,
            "cons_slug": slugify(constituency) if constituency else None,
            "image_url": sm.get("image_url"),
            "counts": {
                "speeches": len(pivots["speeches_by_mp"].get(code, [])),
                "questions": len(pivots["questions_by_mp"].get(code, [])),
                "bills": len(pivots["bills_by_mp"].get(code, [])),
            },
        })
    # Sort: LS18 sitting first by total activity desc, then everything else
    def _slim_key(m):
        is_current = m["loksabha"] == CURRENT_LS and m["sitting"] and m["house"] == "ls"
        c = m["counts"]
        return (
            0 if is_current else 1,
            -(c["speeches"] + c["questions"] + c["bills"]),
            m["name"],
        )
    slim.sort(key=_slim_key)
    (OUT_DIR / "mps.json").write_text(json.dumps(slim, ensure_ascii=False, indent=2))

    # Per-MP profile + per-corpus feeds
    electoral_linked = 0
    # Collect career stats while building profiles
    careers_pool: list[dict] = []
    for code, info in unified.items():
        sm = info["sansad"] or {}
        tcpd = _merge_tcpd_history(code, aliases_by_canonical.get(code, []), info["house"])
        electoral_summary = summarise_electoral(tcpd)
        terms_history = (
            build_terms_history(terms_lookup, code, info["house"])
            if terms_lookup else []
        )
        profile = {
            "mpsno": code,
            "canonical": info["canonical"],
            "variants": info["variants"],
            "house": info["house"],
            "loksabha": sm.get("loksabha"),
            "sitting": derive_sitting(sm),
            "status": sm.get("status"),
            "state": sm.get("state"),
            "party_full": sm.get("party_full"),
            "party_short": sm.get("party_short"),
            "constituency": sm.get("constituency"),
            "image_url": sm.get("image_url"),
            "dob": sm.get("dob"),
            "age": sm.get("age"),
            "gender": sm.get("gender"),
            "terms": sm.get("terms"),
            "profession": sm.get("profession"),
            "electoral": electoral_summary,
            "terms_history": terms_history,
            "counts": {
                "speeches": len(pivots["speeches_by_mp"].get(code, [])),
                "questions": len(pivots["questions_by_mp"].get(code, [])),
                "bills": len(pivots["bills_by_mp"].get(code, [])),
            },
        }
        if electoral_summary:
            electoral_linked += 1
        (OUT_DIR / "profile" / f"{code}.json").write_text(
            json.dumps(profile, ensure_ascii=False, indent=2)
        )
        (OUT_DIR / "speeches" / f"{code}.json").write_text(
            json.dumps(pivots["speeches_by_mp"].get(code, []), ensure_ascii=False)
        )
        (OUT_DIR / "questions" / f"{code}.json").write_text(
            json.dumps(pivots["questions_by_mp"].get(code, []), ensure_ascii=False)
        )
        (OUT_DIR / "bills" / f"{code}.json").write_text(
            json.dumps(pivots["bills_by_mp"].get(code, []), ensure_ascii=False)
        )
        if tcpd:
            (OUT_DIR / "electoral" / f"{code}.json").write_text(
                json.dumps(tcpd, ensure_ascii=False)
            )
            # Build a careers leaderboard entry. Includes both per-house stats
            # AND a `combined` view (GE + AE merged) so leaderboards can rank
            # by full career, not just one election type.
            ge_sum = electoral_summary["ge"] if electoral_summary else None
            ae_sum = electoral_summary["ae"] if electoral_summary else None
            cb_sum = electoral_summary["combined"] if electoral_summary else None
            ge = ge_sum or {}
            ae = ae_sum or {}
            cb = cb_sum or {}
            state_str = (sm.get("state") or "").strip() or None
            party_str = sm.get("party_short") or sm.get("party_full")
            cons_str = sm.get("constituency")
            careers_pool.append({
                "mpsno": code,
                "name": info["canonical"],
                "house": info["house"],
                "loksabha": sm.get("loksabha"),
                "sitting": derive_sitting(sm),
                "state": state_str,
                "state_slug": slugify(state_str) if state_str else None,
                "party": party_str,
                "party_slug": slugify(party_str) if party_str else None,
                "constituency": cons_str,
                "cons_slug": slugify(cons_str) if cons_str else None,
                # Per-house
                "ge_contests": ge.get("contests", 0),
                "ge_wins": ge.get("wins", 0),
                "ge_parties": ge.get("parties", []) or [],
                "ge_constituencies": ge.get("constituencies", []) or [],
                "ge_first_year": ge.get("first_year"),
                "ge_last_year": ge.get("last_year"),
                "ge_latest_margin": (ge.get("latest") or {}).get("margin_percentage"),
                "ge_latest_vote_share": (ge.get("latest") or {}).get("vote_share"),
                "ge_latest_position": (ge.get("latest") or {}).get("position"),
                "ae_contests": ae.get("contests", 0),
                "ae_wins": ae.get("wins", 0),
                "ae_parties": ae.get("parties", []) or [],
                "ae_constituencies": ae.get("constituencies", []) or [],
                "ae_first_year": ae.get("first_year"),
                "ae_last_year": ae.get("last_year"),
                # Combined (GE + AE) — the "neta journey" view
                "all_contests": cb.get("contests", 0),
                "all_wins": cb.get("wins", 0),
                "all_parties": cb.get("parties", []) or [],
                "all_constituencies": cb.get("constituencies", []) or [],
                "all_states": cb.get("states", []) or [],
                "all_first_year": cb.get("first_year"),
                "all_last_year": cb.get("last_year"),
                # Crossover flags
                "spans_houses": ge.get("contests", 0) > 0 and ae.get("contests", 0) > 0,
            })

    # Career leaderboards — slice careers_pool into ranked lists.
    # IMPORTANT: leaderboards measuring "neta journey" use combined GE+AE stats,
    # because a leader may have switched parties or moved between Lok Sabha and
    # State Assembly. Per-election-type leaderboards (closest GE wins etc.) keep
    # their GE-only basis.
    any_pool = [c for c in careers_pool if c["all_contests"] > 0]
    ge_pool = [c for c in careers_pool if c["ge_contests"] > 0]
    current_filter = lambda c: c["house"] == "ls" and c["sitting"] and c["loksabha"] == CURRENT_LS
    careers = {
        "totals": {
            "linked": len(careers_pool),
            "with_any": len(any_pool),
            "with_ge": len(ge_pool),
            "with_ae": sum(1 for c in careers_pool if c["ae_contests"] > 0),
            "spans_houses": sum(1 for c in careers_pool if c["spans_houses"]),
            "ls18_sitting_with_history": sum(1 for c in any_pool if current_filter(c)),
        },
        # Career-spanning leaderboards — measure across GE + AE.
        "turncoats": sorted(
            [c for c in any_pool if len(c["all_parties"]) > 1],
            key=lambda c: (-len(c["all_parties"]), -c["all_contests"]),
        )[:100],
        "constituency_hoppers": sorted(
            [c for c in any_pool if len(c["all_constituencies"]) > 1],
            key=lambda c: (-len(c["all_constituencies"]), -c["all_contests"]),
        )[:100],
        "veterans": sorted(any_pool, key=lambda c: -c["all_contests"])[:100],
        "top_winners": sorted(
            [c for c in any_pool if c["all_wins"] > 0],
            key=lambda c: (-c["all_wins"], -c["all_contests"]),
        )[:100],
        # New: leaders whose career spans both houses (state -> centre or v.v.)
        "house_crossovers": sorted(
            [c for c in any_pool if c["spans_houses"]],
            key=lambda c: -c["all_contests"],
        )[:100],
        # GE-specific (the margin numbers are only meaningful within one election type)
        "dominant_wins": sorted(
            [c for c in ge_pool
             if c.get("ge_latest_position") == 1 and c["ge_latest_margin"] is not None],
            key=lambda c: -(c["ge_latest_margin"] or 0),
        )[:50],
        "closest_wins": sorted(
            [c for c in ge_pool
             if c.get("ge_latest_position") == 1
             and c["ge_latest_margin"] is not None
             and c["ge_latest_margin"] > 0],
            key=lambda c: c["ge_latest_margin"] or 0,
        )[:50],
        "ls18_first_timers": [
            c for c in careers_pool
            if current_filter(c) and c["all_contests"] == 0
        ][:200],
    }
    (OUT_DIR / "careers.json").write_text(json.dumps(careers, ensure_ascii=False, indent=2))

    # Stats
    stats = pivots["stats"]
    (OUT_DIR / "stats" / "by-state.json").write_text(json.dumps(stats["by_state"], ensure_ascii=False, indent=2))
    (OUT_DIR / "stats" / "by-party.json").write_text(json.dumps(stats["by_party"], ensure_ascii=False, indent=2))
    (OUT_DIR / "stats" / "by-ministry-questions.json").write_text(json.dumps(stats["by_ministry_questions"], ensure_ascii=False, indent=2))
    (OUT_DIR / "stats" / "by-ministry-bills.json").write_text(json.dumps(stats["by_ministry_bills"], ensure_ascii=False, indent=2))
    (OUT_DIR / "stats" / "by-session.json").write_text(json.dumps(stats["by_session"], ensure_ascii=False, indent=2))
    (OUT_DIR / "stats" / "by-debate-type.json").write_text(json.dumps(stats["by_debate_type"], ensure_ascii=False, indent=2))
    (OUT_DIR / "stats" / "by-house.json").write_text(json.dumps({
        "ls": sum(1 for m in slim if m["house"] == "ls"),
        "rs": sum(1 for m in slim if m["house"] == "rs"),
    }, ensure_ascii=False, indent=2))

    # Per-state / per-party / per-ministry detail JSONs (drive /stats/by-state/[slug]/ etc.)
    state_dir = OUT_DIR / "stats" / "state"
    party_dir = OUT_DIR / "stats" / "party"
    ministry_dir = OUT_DIR / "stats" / "ministry"
    state_dir.mkdir(exist_ok=True)
    party_dir.mkdir(exist_ok=True)
    ministry_dir.mkdir(exist_ok=True)

    # `slugify` is defined earlier near the mps.json build — re-use it here.

    # Filter to sitting LS18 MPs for the per-state/per-party pages (since those
    # are the "active politicians" view). Older / RS members are cross-references.
    ls18 = [m for m in slim
            if m["house"] == "ls" and m["sitting"] and m["loksabha"] == CURRENT_LS]

    # Sitting RS members — also keyed by state for the per-state page.
    rs_sitting = [
        m for m in slim
        if m["house"] == "rs" and m["sitting"]
    ]
    rs_by_state: dict[str, list] = defaultdict(list)
    for m in rs_sitting:
        st = (m["state"] or "").strip()
        if st:
            rs_by_state[st].append(m)

    # Per-state aggregate + roster
    states_index = []
    by_state_detail: dict[str, dict] = defaultdict(lambda: {"mps": [], "by_party": Counter()})
    for m in ls18:
        st = (m["state"] or "").strip()
        if not st:
            continue
        by_state_detail[st]["mps"].append(m)
        if m.get("party"):
            by_state_detail[st]["by_party"][m["party"]] += 1

    # Make sure states with only RS members also get a page.
    for st in rs_by_state:
        if st not in by_state_detail:
            by_state_detail[st] = {"mps": [], "by_party": Counter()}

    for st, det in by_state_detail.items():
        roster = sorted(det["mps"], key=lambda x: -(x["counts"]["speeches"] + x["counts"]["bills"] + x["counts"]["questions"]))
        rs_roster = sorted(rs_by_state.get(st, []),
                           key=lambda x: -(x["counts"]["speeches"] + x["counts"]["bills"] + x["counts"]["questions"]))
        totals = {
            "mp_count": len(roster),
            "rs_mp_count": len(rs_roster),
            "speeches": sum(m["counts"]["speeches"] for m in roster),
            "questions": sum(m["counts"]["questions"] for m in roster),
            "bills": sum(m["counts"]["bills"] for m in roster),
        }
        by_party = dict(det["by_party"].most_common())
        page = {
            "state": st,
            "slug": slugify(st),
            "totals": totals,
            "by_party": by_party,
            "roster": roster,
            "rs_roster": rs_roster,
        }
        (state_dir / f"{slugify(st)}.json").write_text(json.dumps(page, ensure_ascii=False, indent=2))
        states_index.append({
            "state": st,
            "slug": slugify(st),
            "totals": totals,
            "by_party": by_party,
        })
    states_index.sort(key=lambda s: -s["totals"]["mp_count"])
    (OUT_DIR / "stats" / "states.json").write_text(json.dumps(states_index, ensure_ascii=False, indent=2))

    # Per-party aggregate + roster
    parties_index = []
    by_party_detail: dict[str, dict] = defaultdict(lambda: {"mps": [], "by_state": Counter()})
    for m in ls18:
        pt = m.get("party")
        if not pt:
            continue
        by_party_detail[pt]["mps"].append(m)
        st = (m["state"] or "").strip()
        if st:
            by_party_detail[pt]["by_state"][st] += 1

    for pt, det in by_party_detail.items():
        roster = sorted(det["mps"], key=lambda x: -(x["counts"]["speeches"] + x["counts"]["bills"] + x["counts"]["questions"]))
        totals = {
            "mp_count": len(roster),
            "speeches": sum(m["counts"]["speeches"] for m in roster),
            "questions": sum(m["counts"]["questions"] for m in roster),
            "bills": sum(m["counts"]["bills"] for m in roster),
        }
        by_state_for_party = dict(det["by_state"].most_common())
        page = {
            "party": pt,
            "slug": slugify(pt),
            "totals": totals,
            "by_state": by_state_for_party,
            "roster": roster,
        }
        (party_dir / f"{slugify(pt)}.json").write_text(json.dumps(page, ensure_ascii=False, indent=2))
        parties_index.append({"party": pt, "slug": slugify(pt), "totals": totals, "by_state": by_state_for_party})
    parties_index.sort(key=lambda p: -p["totals"]["mp_count"])
    (OUT_DIR / "stats" / "parties.json").write_text(json.dumps(parties_index, ensure_ascii=False, indent=2))

    # Per-ministry detail: walk per-MP question feeds to find top askers,
    # walk per-MP bill feeds to find top introducers.
    ministry_q_askers: dict[str, Counter] = defaultdict(Counter)
    ministry_b_introducers: dict[str, Counter] = defaultdict(Counter)
    for code, qs in pivots["questions_by_mp"].items():
        for q in qs:
            mn = q.get("ministry")
            if mn:
                ministry_q_askers[mn][code] += 1
    for code, bs in pivots["bills_by_mp"].items():
        for b in bs:
            mn = b.get("ministry")
            if mn:
                ministry_b_introducers[mn][code] += 1

    # Build name lookup for top-MPs presentation
    mp_lookup = {m["mpsno"]: m for m in slim}
    ministries_index = []
    all_ministries = set(stats["by_ministry_questions"]) | set(stats["by_ministry_bills"])

    # Group ministry name variants by slug. Upstream sansad.in surfaces the
    # same ministry under different casings/whitespace ("Housing and Urban
    # Affairs" from bills, "HOUSING AND URBAN AFFAIRS" from questions),
    # which slugify-collapses to one URL. Without this merge, MiniSearch
    # rejects the index with a duplicate-ID error and Astro's
    # getStaticPaths emits two pages for the same path.
    variants_by_slug: dict[str, list[str]] = defaultdict(list)
    for mn in all_ministries:
        variants_by_slug[slugify(mn)].append(mn)

    for slug, variants in variants_by_slug.items():
        # Canonical display name: prefer a mixed-case variant over an
        # all-upper one ("Housing and Urban Affairs" reads better than
        # "HOUSING AND URBAN AFFAIRS"); fall back to the first.
        canonical = next((v for v in variants if v != v.upper()), variants[0])

        q_count = sum(stats["by_ministry_questions"].get(v, 0) for v in variants)
        b_count = sum(stats["by_ministry_bills"].get(v, 0) for v in variants)

        merged_q: Counter = Counter()
        merged_b: Counter = Counter()
        for v in variants:
            merged_q.update(ministry_q_askers[v])
            merged_b.update(ministry_b_introducers[v])

        def _top(counter: Counter) -> list[dict]:
            out = []
            for code, n in counter.most_common(20):
                mp = mp_lookup.get(code)
                if not mp:
                    continue
                out.append({"mpsno": code, "name": mp["name"], "party": mp["party"],
                            "state": mp["state"], "count": n})
            return out

        page = {
            "ministry": canonical,
            "slug": slug,
            "totals": {"questions": q_count, "bills": b_count},
            "top_questioners": _top(merged_q),
            "top_introducers": _top(merged_b),
        }
        (ministry_dir / f"{slug}.json").write_text(json.dumps(page, ensure_ascii=False, indent=2))
        ministries_index.append({"ministry": canonical, "slug": slug, "totals": page["totals"]})
    ministries_index.sort(key=lambda x: -(x["totals"]["questions"] + x["totals"]["bills"]))
    (OUT_DIR / "stats" / "ministries.json").write_text(json.dumps(ministries_index, ensure_ascii=False, indent=2))

    # Audit files
    attribution = {raw: r["mpsnos"] for raw, r in attrib.cache.items() if r["mpsnos"]}
    (OUT_DIR / "attribution.json").write_text(json.dumps(attribution, ensure_ascii=False, indent=2))
    (OUT_DIR / "unmatched.json").write_text(json.dumps(dict(attrib.unmatched.most_common()), ensure_ascii=False, indent=2))

    # Index
    index = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "totals": {
            "mps": len(slim),
            "ls": sum(1 for m in slim if m["house"] == "ls"),
            "rs": sum(1 for m in slim if m["house"] == "rs"),
            "current_ls_sitting": sum(
                1 for m in slim
                if m["house"] == "ls" and m["sitting"] and m["loksabha"] == CURRENT_LS
            ),
            "speeches": sum(len(v) for v in pivots["speeches_by_mp"].values()),
            "questions": sum(len(v) for v in pivots["questions_by_mp"].values()),
            "bills": sum(len(v) for v in pivots["bills_by_mp"].values()),
            "unmatched_strings": len(attrib.unmatched),
            "electoral_linked": electoral_linked,
            "aliased_mpsnos": len(aliases),
        },
        "current_lok_sabha": CURRENT_LS,
    }
    (OUT_DIR / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=2))

    # aliases.json — audit + future redirect source. Maps {alias_mpsno: canonical_mpsno}.
    if aliases:
        (OUT_DIR / "aliases.json").write_text(
            json.dumps({str(k): v for k, v in sorted(aliases.items())}, ensure_ascii=False, indent=2)
        )

    return index


def build_terms_history(terms_lookup: dict, code: int, house: str) -> list[dict]:
    """Slim, ordered list of every parliamentary term this MP has served — LS + RS.

    After person-merge, a single canonical mpsno can have terms under both
    (house, code) keys (e.g. Kharge: LS16 + RS-sitting). The `house` arg is
    ignored when present in both — we always emit the union, sorted LS-by-
    loksabha then RS at the end.
    """
    recs = list(terms_lookup.get(("ls", code), []))
    recs.extend(terms_lookup.get(("rs", code), []))
    out = []
    for r in recs:
        ls = r.get("loksabha")
        out.append({
            "house": r.get("house"),
            "loksabha": ls,
            "year": LS_YEAR.get(ls) if ls is not None else None,
            "sitting": bool(r.get("sitting")),
            "status": (r.get("status") or "").strip() or None,
            "state": (r.get("state") or "").strip() or None,
            "constituency": r.get("constituency"),
            "party_short": r.get("party_short"),
            "party_full": r.get("party_full"),
        })
    # Sort chronologically. RS rows have loksabha=None, place them last.
    out.sort(key=lambda x: (x["year"] if x["year"] is not None else 0))
    return out


def main():
    print("Loading sansad master ...", flush=True)
    sansad = load_sansad_master()
    print(f"  -> {len(sansad)} entries")
    print("Loading sansad terms ...", flush=True)
    terms_lookup = load_sansad_terms()
    print(f"  -> {len(terms_lookup)} (house, mpsno) groups, "
          f"{sum(len(v) for v in terms_lookup.values())} term-rows")

    print("Loading debate variants ...", flush=True)
    debate_variants = load_debate_variants()
    print(f"  -> {len(debate_variants)} mp_codes seen in debates")

    print("Unifying master ...", flush=True)
    unified = build_unified_master(sansad, debate_variants)
    print(f"  -> {len(unified)} unified mpsnos (pre-person-merge)")

    print("Merging same-person mpsnos ...", flush=True)
    aliases = compute_person_aliases(unified)

    # Capture which (house, raw_mpsno) belongs to which canonical BEFORE
    # apply_person_aliases collapses unified. This is the disambiguator for
    # cross-house mpsno collisions (sansad's mpsno isn't globally unique —
    # e.g., (rs, 9) is an INC Haryana retiree, NOT Advani who is (ls, 9)).
    #
    # The map only contains (house, raw_mpsno) keys for MPs that exist in the
    # current unified master. Any term record keyed by a (house, mpsno) NOT
    # in this map belongs to a collision-loser — a person whose sansad master
    # record was overwritten by load_sansad_master's prefer-LS rule. Without
    # this filter, that lost person's RS terms would leak into whoever DID
    # win the canonical mpsno's bucket.
    canonical_for_raw_key: dict[tuple[str, int], int] = {}
    for code, info in unified.items():
        sm = (info or {}).get("sansad") or {}
        h = sm.get("house")
        if h is None:
            continue
        canonical_for_raw_key[(h, code)] = aliases.get(code, code)

    unified = apply_person_aliases(unified, aliases, debate_variants)
    print(f"  -> {len(aliases)} alias mpsnos merged, "
          f"{len(unified)} canonical persons")

    # Rebuild terms_lookup keyed by canonical mpsno, dropping records whose
    # (house, raw_mpsno) doesn't appear in canonical_for_raw_key. Replaces the
    # earlier alias-fold loop which used setdefault().extend() — that bug
    # mixed collision-loser records (e.g., RS-130 CPI(M) West Bengal MP) into
    # the canonical's bucket (Sonia Gandhi at canonical 130), producing ghost
    # rows in terms_history.
    new_terms_lookup: dict[tuple, list[dict]] = defaultdict(list)
    dropped_term_rows = 0
    for (h, raw_mpsno), recs in terms_lookup.items():
        canon = canonical_for_raw_key.get((h, raw_mpsno))
        if canon is None:
            dropped_term_rows += len(recs)
            continue
        new_terms_lookup[(h, canon)].extend(recs)
    terms_lookup = dict(new_terms_lookup)
    print(f"  -> terms_lookup: {len(terms_lookup)} (house, canonical) groups, "
          f"{dropped_term_rows} collision-loser term rows dropped")

    print("Building lookup ...", flush=True)
    lookup = build_lookup(unified)
    print(f"  -> {len(lookup)} normalized name forms")

    attrib = Attribution(unified, lookup)

    print("Pivoting corpora ...", flush=True)
    pivots = pivot(unified, attrib)

    # Fold alias-keyed pivot output into the canonical mpsno — happens when
    # a free-text speaker string resolved to an alias mpsno before the merge
    # was applied to the lookup. Belt-and-braces with the variants merge above.
    for bucket in ("speeches_by_mp", "questions_by_mp", "bills_by_mp"):
        bmap = pivots.get(bucket, {})
        for alias, canonical in aliases.items():
            if alias in bmap:
                bmap.setdefault(canonical, []).extend(bmap[alias])
                del bmap[alias]

    print("Writing outputs ...", flush=True)
    index = write_outputs(unified, pivots, attrib, terms_lookup, aliases=aliases)

    print("\n=== DONE ===")
    for k, v in index["totals"].items():
        print(f"  {k}: {v}")
    print(f"\nOutputs: {OUT_DIR}/")


if __name__ == "__main__":
    main()
