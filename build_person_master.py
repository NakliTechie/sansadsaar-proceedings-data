#!/usr/bin/env python3
"""Build persons + tenures into netas.sqlite (Phase 1 of person-centric rebuild).

This is the first stage of the new pipeline. It reads:

  - scripts/recon-netas-out/sansad_terms.json  (per-(mpsno, loksabha) tenures)
  - scripts/recon-netas-out/sansad_master.json (per-mpsno canonical info)
  - tcpd/derived/history/<house>-<mpsno>.json  (TCPD pid links, in-repo snapshot)

Applies the identity-resolution chain (sansad DOB primary, TCPD pid as the
cross-mpsno spline, fallback for the residual), mints stable `n-<int>` pids
via the checked-in person_pid_assignments.json, applies schema.sql to a
fresh netas.sqlite, and populates `persons` and `tenures`.

Not in this script (separate follow-ups):
  - TCPD-RSD historical RS rows         → build_person_master.py --rs-tcpd
  - Pre-LS14 TCPD GE winners backfill   → same script, later flag
  - Contests                            → build_electoral_iv.py
  - Corpora (speeches/questions/bills)  → build_netas.py rewrite

Run:
    python build_person_master.py
    python build_person_master.py --db /tmp/netas-test.sqlite

Identity chain summary (per plan 003):

    sansad DOB+name (66.6% coverage) ──┐
                                       ├─→ pid (n-<int>, stable across runs)
    TCPD pid (41.5%, spline)           ┘
                                       │
    fallback: one person per mpsno ────┘
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent
SCHEMA = REPO / "schema.sql"
SANSAD_MASTER = REPO / "scripts" / "recon-netas-out" / "sansad_master.json"
SANSAD_TERMS = REPO / "scripts" / "recon-netas-out" / "sansad_terms.json"
TCPD_HISTORY = REPO / "tcpd" / "derived" / "history"
PID_ASSIGNMENTS = REPO / "person_pid_assignments.json"
DEFAULT_DB = REPO / "docs" / "netas" / "netas.sqlite"

# The currently-active Lok Sabha number. Only tenures with this loksabha
# can have sitting=1 in the LS office_type. Bump when LS19 begins.
CURRENT_LS = 18


# ── Normalisation ────────────────────────────────────────────────────────


HONORIFIC_TOKENS = {
    "shri", "smt", "shrimati", "dr", "ms", "mr", "mrs", "prof", "adv",
    "sh", "km", "kumari",
}


def norm_name(s: str | None) -> str:
    """Order-insensitive name canonicalisation for *matching*.

    Sansad stores names in three flavours that all refer to the same person:
      "Shri Lalu Prasad"          (full_name_firstlast)
      "Lalu Prasad, Shri"         (sometimes full_name_lastfirst on RS)
      "Joshi, Dr. Murli Manohar"  (lastfirst with trailing honorific in middle)

    After lowercasing, stripping punctuation, dropping honorific tokens, and
    sorting the remaining tokens, all three forms collapse to the same
    string. Sorted-token comparison is intentional — for sansad data we
    treat name as an unordered bag because the upstream representation
    varies per record. DOB then disambiguates same-name people.
    """
    if not s:
        return ""
    s = re.sub(r"[.,;:()\[\]'\"]", " ", s.lower())
    tokens = sorted(t for t in s.split() if t and t not in HONORIFIC_TOKENS)
    return " ".join(tokens)


def norm_dob(dob: str | None) -> str | None:
    """sansad's dd/mm/yyyy → ISO yyyy-mm-dd. None if unparseable."""
    if not dob or not isinstance(dob, str):
        return None
    s = dob.strip()
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo}-{d}"
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return s
    return None


def slug_of(name: str) -> str:
    """URL-safe slug, preserving canonical display order.

    Distinct from norm_name (which sorts tokens for matching) — slugs are
    user-visible and must read naturally. Example: "Prof. S P Singh Baghel"
    → "s-p-singh-baghel" (not "baghel-p-s-singh").
    """
    if not name:
        return "anon"
    s = re.sub(r"[.,;:()\[\]'\"]", " ", name.lower())
    tokens = [t for t in s.split() if t and t not in HONORIFIC_TOKENS]
    n = "-".join(tokens)
    n = re.sub(r"[^a-z0-9-]+", "-", n).strip("-")
    return n or "anon"


def sha256_short(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


# ── Loaders ──────────────────────────────────────────────────────────────


def load_sansad_terms() -> dict[tuple[str, int], list[dict]]:
    """Return {(house, mpsno): [unique tenure records by loksabha]}.

    sansad_terms.json sometimes contains near-duplicates of the same
    (mpsno, loksabha) — we dedupe and keep one per (house, mpsno, loksabha).
    """
    raw = json.loads(SANSAD_TERMS.read_text())
    by_key: dict[tuple[str, int], dict[int | None, dict]] = defaultdict(dict)
    for rec in raw:
        h = rec.get("house")
        m = rec.get("mpsno")
        ls = rec.get("loksabha")  # None for RS records
        if h is None or m is None:
            continue
        key = (h, int(m))
        # Use the freshest of any duplicates: prefer Sitting > others.
        existing = by_key[key].get(ls)
        if existing is None or _term_priority(rec) > _term_priority(existing):
            by_key[key][ls] = rec
    return {k: list(v.values()) for k, v in by_key.items()}


def _term_priority(rec: dict) -> int:
    s = (rec.get("status") or "").lower()
    if s == "sitting":
        return 3
    if s in {"former", "retirement"}:
        return 2
    if rec.get("sitting") is True:
        return 1
    return 0


def load_sansad_master() -> dict[tuple[str, int], dict]:
    raw = json.loads(SANSAD_MASTER.read_text())
    out: dict[tuple[str, int], dict] = {}
    for rec in raw:
        h = rec.get("house")
        m = rec.get("mpsno")
        if h is None or m is None:
            continue
        out[(h, int(m))] = rec
    return out


def build_tcpd_link_index() -> dict[tuple[str, int], str]:
    """Walk tcpd/derived/history/*.json and extract per-mpsno TCPD pids.

    Each history file is {mpsno, house, ge_pid, ae_pid, ge_history, ae_history}.
    We prefer ge_pid (LS-relevant); fall back to ae_pid for AC-only matches.
    """
    out: dict[tuple[str, int], str] = {}
    if not TCPD_HISTORY.is_dir():
        return out
    for p in TCPD_HISTORY.glob("*.json"):
        try:
            h = json.loads(p.read_text())
        except Exception:
            continue
        m = h.get("mpsno")
        house = h.get("house")
        if m is None or house is None:
            continue
        pid = h.get("ge_pid") or h.get("ae_pid")
        if pid:
            out[(house, int(m))] = pid
    return out


# ── Identity resolution ──────────────────────────────────────────────────


class UnionFind:
    """Tiny union-find for grouping mpsnos that resolve to the same person."""

    def __init__(self) -> None:
        self.parent: dict[tuple[str, int], tuple[str, int]] = {}

    def find(self, x: tuple[str, int]) -> tuple[str, int]:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: tuple[str, int], b: tuple[str, int]) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def resolve_persons(
    terms: dict[tuple[str, int], list[dict]],
    master: dict[tuple[str, int], dict],
    tcpd: dict[tuple[str, int], str],
) -> list[dict]:
    """Group mpsnos into persons. Returns list of person dicts."""
    uf = UnionFind()
    for key in terms:
        uf.find(key)

    # Pre-compute (norm_name, dob) for each mpsno. A single mpsno can have
    # multiple tenure records with inconsistent DOB presence; scan all
    # records for any non-null DOB before discarding.
    name_dob: dict[tuple[str, int], tuple[str, str | None]] = {}
    for key, recs in terms.items():
        nname = ""
        dob: str | None = None
        for rec in recs:
            if not nname:
                raw = rec.get("full_name_firstlast") or rec.get("full_name_lastfirst") or ""
                nname = norm_name(raw)
            if dob is None:
                dob = norm_dob(rec.get("dob"))
            if nname and dob:
                break
        name_dob[key] = (nname, dob)

    # Merge by sansad (norm_name, dob) — primary spline.
    by_dob: dict[tuple[str, str], list[tuple[str, int]]] = defaultdict(list)
    for key, (nname, dob) in name_dob.items():
        if dob and nname:
            by_dob[(nname, dob)].append(key)
    for group in by_dob.values():
        for other in group[1:]:
            uf.union(group[0], other)

    # Merge by TCPD pid — secondary spline. The TCPD linker is fuzzy and
    # frequently maps different sansad MPs to the same pid (verified
    # collisions: GEUP48277 = Karan Bhushan Singh + Brijbhushan Sharan
    # Singh; GEBR2137 = two unrelated "Veena Devi"s with different DOBs).
    # Decision logic for two mpsnos sharing a TCPD pid:
    #   - DOBs both present:
    #       same DOB → MERGE (decisive — names may vary as aliases, e.g.
    #         "Shri Sushil Kumar Shinde" vs "Shinde, Shri Sushilkumar")
    #       conflicting DOB → REFUSE (different people)
    #   - At least one DOB missing:
    #       falls back to name comparison; refuse only if both names are
    #       present and differ.
    # DOB-match decisiveness is what catches the LS↔RS aliases that the
    # codex review flagged (Shinde, Katoch, Sakshi Ji Maharaj).
    by_tcpd: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for key, pid in tcpd.items():
        if key in terms:
            by_tcpd[pid].append(key)
    refused = 0
    for pid, group in by_tcpd.items():
        if len(group) < 2:
            continue
        for i, a in enumerate(group):
            an, ad = name_dob[a]
            for b in group[i + 1:]:
                bn, bd = name_dob[b]
                if ad and bd:
                    # DOB on both sides — DOB is the decisive signal.
                    if ad == bd:
                        uf.union(a, b)
                    else:
                        refused += 1
                else:
                    # At least one DOB missing — fall back to name compat.
                    if an and bn and an != bn:
                        refused += 1
                    else:
                        uf.union(a, b)
    if refused:
        print(f"  TCPD-pid merge refused for {refused} pair(s) "
              f"with conflicting DOB or names (probable upstream linker fuzz)")

    # Group by representative.
    groups: dict[tuple[str, int], list[tuple[str, int]]] = defaultdict(list)
    for key in terms:
        groups[uf.find(key)].append(key)

    persons = []
    for rep, members in groups.items():
        person = _build_person(rep, members, terms, master, tcpd)
        persons.append(person)
    return persons


def _build_person(
    rep: tuple[str, int],
    members: list[tuple[str, int]],
    terms: dict[tuple[str, int], list[dict]],
    master: dict[tuple[str, int], dict],
    tcpd: dict[tuple[str, int], str],
) -> dict:
    # Canonical info: prefer the most recent tenure across all members.
    tenure_records: list[tuple[tuple[str, int], dict]] = []
    for key in members:
        for rec in terms[key]:
            tenure_records.append((key, rec))
    # Sort by (loksabha desc, sitting desc) to pick the canonical record.
    tenure_records.sort(
        key=lambda kr: (kr[1].get("loksabha") or 0, _term_priority(kr[1])),
        reverse=True,
    )
    canonical_key, canonical_rec = tenure_records[0]

    canonical_name = (
        canonical_rec.get("full_name_firstlast")
        or canonical_rec.get("full_name_lastfirst")
        or "Unknown"
    )
    canonical_name = re.sub(r"\s+", " ", canonical_name).strip()
    dob = norm_dob(canonical_rec.get("dob"))
    for _, rec in tenure_records:
        # Prefer any non-null DOB found across the group.
        if dob is None:
            dob = norm_dob(rec.get("dob"))

    gender = canonical_rec.get("gender")
    if isinstance(gender, str):
        g = gender.strip().upper()
        gender = {"MALE": "M", "FEMALE": "F"}.get(g, g) if g else None

    image_url = master.get(canonical_key, {}).get("image_url")
    tcpd_pids = sorted({tcpd[k] for k in members if k in tcpd})
    tcpd_pid = tcpd_pids[0] if tcpd_pids else None

    has_dob = dob is not None
    has_tcpd = tcpd_pid is not None
    if has_dob and has_tcpd:
        identity_source = "both"
        anchor = "sansad_dob"
        confidence = 1.0
    elif has_dob:
        identity_source = "dob_only"
        anchor = "sansad_dob"
        confidence = 0.9
    elif has_tcpd:
        identity_source = "tcpd_only"
        anchor = "tcpd_pid"
        confidence = 0.8
    else:
        identity_source = "fallback"
        anchor = "fallback"
        confidence = 0.5

    return {
        "merge_key": _merge_key(members),
        "canonical_name": canonical_name,
        "slug": slug_of(canonical_name),
        "dob": dob,
        "gender": gender,
        "image_url": image_url,
        "identity_source": identity_source,
        "tcpd_pid": tcpd_pid,
        "sansad_dob_hash": sha256_short(f"{norm_name(canonical_name)}|{dob}") if has_dob else None,
        "anchor_signal": anchor,
        "confidence": confidence,
        "members": sorted(members),
        "tenure_records": [
            {
                "house": k[0],
                "mpsno": k[1],
                "record": rec,
            }
            for k, rec in tenure_records
        ],
    }


def _merge_key(members: list[tuple[str, int]]) -> str:
    parts = [f"{h}-{m}" for h, m in sorted(members)]
    return "sansad:" + ",".join(parts)


# ── Pid assignment (persisted) ───────────────────────────────────────────


def load_pid_assignments() -> dict:
    if not PID_ASSIGNMENTS.exists():
        return {"next_id": 1, "by_merge_key": {}, "by_pid": {}}
    return json.loads(PID_ASSIGNMENTS.read_text())


def save_pid_assignments(assignments: dict) -> None:
    PID_ASSIGNMENTS.write_text(json.dumps(assignments, indent=2, ensure_ascii=False) + "\n")


def assign_pids(persons: list[dict], assignments: dict) -> None:
    """Mint stable pids. Mutates `persons` (adds .pid) and `assignments`.

    Append-only: an existing merge_key keeps its pid forever. New
    merge_keys mint the next available `n-<int>`. We also record
    provenance so future code can reason about merges/splits.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for person in persons:
        mk = person["merge_key"]
        if mk in assignments["by_merge_key"]:
            pid = assignments["by_merge_key"][mk]
        else:
            pid = f"n-{assignments['next_id']}"
            assignments["next_id"] += 1
            assignments["by_merge_key"][mk] = pid
            assignments["by_pid"][pid] = {
                "mint_date": today,
                "identity_source": person["identity_source"],
                "anchor_signal": person["anchor_signal"],
                "merge_key": mk,
                "deprecated_pids": [],
                "split_into": [],
            }
        person["pid"] = pid


# ── DB writers ───────────────────────────────────────────────────────────


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    schema = SCHEMA.read_text()
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema)


def write_persons(conn: sqlite3.Connection, persons: list[dict]) -> None:
    conn.executemany(
        """INSERT INTO persons
           (pid, canonical_name, slug, dob, gender, image_url,
            identity_source, tcpd_pid, sansad_dob_hash, anchor_signal, confidence)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (
                p["pid"],
                p["canonical_name"],
                p["slug"],
                p["dob"],
                p["gender"],
                p["image_url"],
                p["identity_source"],
                p["tcpd_pid"],
                p["sansad_dob_hash"],
                p["anchor_signal"],
                p["confidence"],
            )
            for p in persons
        ],
    )


def write_tenures(conn: sqlite3.Connection, persons: list[dict]) -> None:
    """Write tenures with per-person derivation of `sitting` and `status`.

    Sansad's source rows carry the *person's* current standing on *every*
    historical row (Rahul Gandhi's LS14 record says sitting=True because
    he is currently sitting in LS18). Trusting the row directly would
    mark all 5 of his LS terms as sitting, breaking the roster filter.

    Per-person logic:
      LS: only the highest-loksabha tenure for this person can be sitting;
          earlier LS tenures get sitting=0 and status='Former'.
      RS: each mpsno is its own term; we trust the source's per-mpsno
          status, except we mark at most one RS tenure as sitting per
          person (the highest external_id, as a proxy for "most recent",
          since RS terms lack reliable date fields in sansad_terms).
    """
    rows = []
    for p in persons:
        # Pass 1: find latest LS loksabha + latest RS external_id for this person.
        latest_ls_loksabha: int | None = None
        latest_rs_mpsno: int | None = None
        for tr in p["tenure_records"]:
            rec = tr["record"]
            if tr["house"] == "ls":
                ls = rec.get("loksabha")
                if ls is not None:
                    if latest_ls_loksabha is None or ls > latest_ls_loksabha:
                        latest_ls_loksabha = ls
            elif tr["house"] == "rs":
                m = tr["mpsno"]
                if latest_rs_mpsno is None or m > latest_rs_mpsno:
                    latest_rs_mpsno = m

        seen_keys: set[tuple] = set()
        for tr in p["tenure_records"]:
            rec = tr["record"]
            office_type = tr["house"]
            jurisdiction = "IN"
            loksabha = rec.get("loksabha")
            term_label = f"LS{loksabha}" if office_type == "ls" and loksabha else (
                f"RS-{rec.get('term_start','?')}-{rec.get('term_end','?')}"
                if office_type == "rs"
                else None
            )
            external_id = str(tr["mpsno"])
            dedup_k = (office_type, external_id, term_label)
            if dedup_k in seen_keys:
                continue
            seen_keys.add(dedup_k)

            state = rec.get("state")
            constituency = rec.get("constituency")
            party_short = rec.get("party_short")

            # Derived sitting + status.
            raw_sitting = (
                rec.get("sitting") is True
                or (rec.get("status") or "").lower() == "sitting"
            )
            if office_type == "ls":
                # LS sitting is only true for the *currently active* Lok Sabha.
                # A person whose latest LS was LS17 is no longer sitting just
                # because their LS17 row inherits the source's current status.
                is_current_ls = (loksabha == CURRENT_LS)
                sitting_int = 1 if (is_current_ls and raw_sitting) else 0
                status = rec.get("status") if is_current_ls else "Former"
            elif office_type == "rs":
                # RS has rolling membership; trust the source per-mpsno but
                # only at most one tenure per person (the highest mpsno as
                # a proxy for "most recent" since RS term dates are absent).
                is_latest = (tr["mpsno"] == latest_rs_mpsno)
                sitting_int = 1 if (is_latest and raw_sitting) else 0
                status = rec.get("status") if is_latest else "Former"
            else:
                sitting_int = 0
                status = rec.get("status")

            # source_row_hash must be source-canonical only — independent
            # of the resolved pid, so a future pid change does not bypass
            # the dedup constraint.
            row_hash_input = f"{office_type}|{jurisdiction}|{external_id}|{term_label}"
            source_row_hash = sha256_short(row_hash_input)

            rows.append(
                (
                    p["pid"],
                    office_type,
                    jurisdiction,
                    term_label,
                    external_id,
                    rec.get("term_start"),
                    rec.get("term_end"),
                    sitting_int,
                    status,
                    state,
                    constituency,
                    party_short,
                    source_row_hash,
                )
            )
    conn.executemany(
        """INSERT INTO tenures
           (pid, office_type, jurisdiction, term_label, external_id,
            term_start, term_end, sitting, status, state, constituency,
            party_short, source_row_hash)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )


def write_build_meta(conn: sqlite3.Connection, info: dict[str, str]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO build_meta (key, value) VALUES (?, ?)",
        list(info.items()),
    )


# ── Verification gate (per plan 003 + codex review) ──────────────────────


# Known fixtures: persons we expect to be merged into exactly this set of
# (house, mpsno) members. The build *fails* if any fixture mismatches.
# Add to this set whenever a manual investigation confirms a merge.
FIXTURE_EXPECTED_MEMBERS: dict[str, set[tuple[str, int]]] = {
    "Rahul Gandhi":                  {("ls", 4074)},
    "Lalu Prasad":                   {("ls", 2439), ("rs", 1879)},
    "Pranab Mukherjee":              {("ls", 4195), ("rs", 185)},
    "Sushma Swaraj":                 {("ls", 3812), ("rs", 94)},
    "Murli Manohar Joshi":           {("ls", 172), ("rs", 325)},
    "Sushil Kumar Shinde":           {("ls", 423), ("rs", 1621)},
    "Chandresh Kumari Katoch":       {("ls", 2994), ("rs", 149)},
    "Amit Shah":                     {("ls", 5021)},
    "Mulayam Singh Yadav":           {("ls", 530)},
}


# Known TCPD-pid fuzzy-match collisions that must stay separate.
# Each tuple is (sansad key A, sansad key B, shared bogus TCPD pid).
FIXTURE_TCPD_COLLISIONS: list[tuple[tuple[str, int], tuple[str, int], str]] = [
    (("ls", 5581), ("ls", 438),  "GEUP48277"),    # Karan B. vs Brijbhushan S. Singh
    (("ls", 5223), ("ls", 4684), "GEBR2137"),     # two Veena Devis, different DOBs
    (("ls", 531),  ("ls", 4101), "GEUP100180"),   # Ramakant vs Umakant Yadav
]


def verify_sanity(persons: list[dict]) -> list[str]:
    """Return a list of *errors* (build should fail if non-empty).

    Catches the bug classes already seen:
      - more than one sitting LS tenure per person
      - same TCPD pid + same DOB across different pids
      - fixture mismatches (named persons must merge to exactly the
        expected member set)
      - known fuzzy-match collisions that must stay split
      - extreme tenure counts (defence-in-depth)
    """
    errors: list[str] = []
    by_pid = {p["pid"]: p for p in persons}
    by_member: dict[tuple[str, int], dict] = {}
    for p in persons:
        for m in p["members"]:
            by_member[m] = p

    # 1. Sitting LS uniqueness — must be at most one LS sitting tenure per
    # person (the latest-loksabha one). The write step now derives this,
    # but the *invariant* is on the source data through the writer.
    # (Tenure rows haven't been written yet at this point; check via the
    # derived flag we emit at write time. Done after DB write — see
    # verify_db_invariants below.)

    # 2. Fixture: known persons must merge to expected member sets.
    # Look up via mpsno membership (canonical_name is just an error label).
    for fixture_name, expected in FIXTURE_EXPECTED_MEMBERS.items():
        owning_pids = {by_member[m]["pid"] for m in expected if m in by_member}
        if not owning_pids:
            errors.append(
                f"FIXTURE MISS: '{fixture_name}' — no mpsno from "
                f"{sorted(expected)} is present in resolved persons"
            )
            continue
        if len(owning_pids) != 1:
            mapping = {m: by_member[m]["pid"] for m in expected if m in by_member}
            errors.append(
                f"FIXTURE SPLIT: '{fixture_name}' — expected members "
                f"{sorted(expected)} to share one pid, got {mapping}"
            )
            continue
        p = by_pid[next(iter(owning_pids))]
        actual = set(p["members"]) & expected
        if actual != expected:
            missing = expected - actual
            errors.append(
                f"FIXTURE INCOMPLETE: '{fixture_name}' on pid={p['pid']} "
                f"is missing {sorted(missing)} from expected {sorted(expected)}"
            )

    # 3. Fuzzy-collision split must hold: known bad pairs stay distinct.
    for a, b, bad_pid in FIXTURE_TCPD_COLLISIONS:
        pa = by_member.get(a)
        pb = by_member.get(b)
        if pa is None or pb is None:
            continue
        if pa["pid"] == pb["pid"]:
            errors.append(
                f"FUZZY-COLLISION FALSE MERGE: {a} and {b} both → "
                f"pid={pa['pid']} (TCPD pid {bad_pid} is a known fuzzy "
                f"collision; they must stay separate)"
            )

    # 4. Same TCPD pid + same DOB across distinct pids → suspicious split.
    by_tcpd_dob: dict[tuple[str, str], list[str]] = defaultdict(list)
    for p in persons:
        if p["tcpd_pid"] and p["dob"]:
            by_tcpd_dob[(p["tcpd_pid"], p["dob"])].append(p["pid"])
    for (pid_tcpd, dob), pids in by_tcpd_dob.items():
        if len(set(pids)) > 1:
            errors.append(
                f"SPLIT SAME-TCPD-SAME-DOB: TCPD pid {pid_tcpd} dob {dob} "
                f"appears across pids {sorted(set(pids))}"
            )

    # 5. Tenure count outliers (defence-in-depth).
    for p in persons:
        recs = p["tenure_records"]
        if len(recs) > 12:
            errors.append(
                f"OUTLIER: pid={p['pid']} ({p['canonical_name']}) has "
                f"{len(recs)} tenure rows — please review manually"
            )
        parties = {r["record"].get("party_short") for r in recs if r["record"].get("party_short")}
        if len(parties) > 5:
            errors.append(
                f"OUTLIER: pid={p['pid']} ({p['canonical_name']}) has "
                f"{len(parties)} distinct parties across tenures"
            )

    return errors


def verify_db_invariants(db_path: Path) -> list[str]:
    """Checks against the *populated* DB. Run after write."""
    errors: list[str] = []
    with sqlite3.connect(db_path) as conn:
        # Sitting LS uniqueness: at most one LS tenure per person can be sitting.
        bad = conn.execute(
            """SELECT pid, COUNT(*) AS n
               FROM tenures
               WHERE office_type = 'ls' AND sitting = 1
               GROUP BY pid
               HAVING n > 1"""
        ).fetchall()
        for pid, n in bad:
            errors.append(f"SITTING DUP: pid={pid} has {n} sitting LS tenures")
    return errors


# ── Main ─────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, default=DEFAULT_DB,
                        help=f"output SQLite path (default: {DEFAULT_DB})")
    parser.add_argument("--dry-run", action="store_true",
                        help="don't write DB or pid assignments")
    args = parser.parse_args()

    print(f"Loading sansad_terms.json …")
    terms = load_sansad_terms()
    print(f"  {len(terms)} unique (house, mpsno) keys; "
          f"{sum(len(v) for v in terms.values())} tenure rows after dedup")

    print(f"Loading sansad_master.json …")
    master = load_sansad_master()
    print(f"  {len(master)} mpsno records")

    print(f"Indexing TCPD links from tcpd/derived/history/ …")
    tcpd = build_tcpd_link_index()
    print(f"  {len(tcpd)} sansad mpsnos linked to a TCPD pid")

    print("Resolving persons …")
    persons = resolve_persons(terms, master, tcpd)
    print(f"  {len(persons)} unique persons resolved from {len(terms)} mpsnos")

    # Bucket distribution
    from collections import Counter
    buckets = Counter(p["identity_source"] for p in persons)
    for b in ("both", "dob_only", "tcpd_only", "fallback"):
        n = buckets.get(b, 0)
        print(f"    {b:10}  {n:5}  ({n/len(persons)*100:5.1f}%)")

    multi = sum(1 for p in persons if len(p["members"]) > 1)
    print(f"  Cross-mpsno merges: {multi} persons span multiple mpsnos")

    print("Assigning stable pids …")
    assignments = load_pid_assignments()
    assign_pids(persons, assignments)
    if not args.dry_run:
        save_pid_assignments(assignments)
    print(f"  {len(assignments['by_pid'])} total pids in assignment file "
          f"(next: n-{assignments['next_id']})")

    print("Running pre-write verification gate …")
    pre_errors = verify_sanity(persons)
    if pre_errors:
        print(f"\nVERIFICATION GATE FAILED — {len(pre_errors)} pre-write error(s):")
        for e in pre_errors[:30]:
            print(f"  ✗ {e}")
        if len(pre_errors) > 30:
            print(f"  … and {len(pre_errors)-30} more")
        if not args.dry_run:
            print("\nRefusing to write DB. Fix the errors and re-run.")
            return 1

    if args.dry_run:
        print("\n[dry-run] no DB write")
        return 0 if not pre_errors else 1

    print(f"Writing {args.db} …")
    init_db(args.db)
    with sqlite3.connect(args.db) as conn:
        write_persons(conn, persons)
        write_tenures(conn, persons)
        write_build_meta(
            conn,
            {
                "schema_version": "0.1",
                "stage": "persons+tenures",
                "built_at": datetime.now(timezone.utc).isoformat(),
                "persons_count": str(len(persons)),
                "tenures_count": str(sum(len(p["tenure_records"]) for p in persons)),
                "sansad_terms_input": str(SANSAD_TERMS.relative_to(REPO)),
            },
        )

    post_errors = verify_db_invariants(args.db)
    if post_errors:
        print(f"\nPOST-WRITE INVARIANT FAILURE — {len(post_errors)} error(s):")
        for e in post_errors[:30]:
            print(f"  ✗ {e}")
        return 1

    print("\nDone.")
    print(f"  persons:  {len(persons)}")
    print(f"  tenures:  {sum(len(p['tenure_records']) for p in persons)}")
    print(f"  verification: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
