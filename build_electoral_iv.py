#!/usr/bin/env python3
"""Build LS contests into netas.sqlite, IV-primary.

Replaces the prior TCPD-iterating attribution. Per the 2026-05-16 design
discussion: TCPD's 2021 cutoff is limiting; IV (vn_candidate_master) is
the canonical contest source. We iterate every IV LS row and decide
ownership.

Ownership decision tree, in priority order:

  Method A — sansad-tenure (deals with current MPs and their full history)
    The IV row's (year, state_norm, cons_norm) matches a sansad tenure.
    Attribute to that person IF name+party compatibility passes a score
    threshold. The score combines:
      • exact-norm name match (high signal)
      • token-overlap ratio (catches Bhupathiraju multi-token cases)
      • shared 6+ char substring on the longest token (catches
        Purandeswari ↔ Purandheshwari transliteration drift)
      • party_abbr exact match (a tiebreaker, since sansad's party_short
        is sometimes the long form)

  Method B — TCPD-bridge (deals with historical contests of current MPs
    and links pre-LS14 IV rows to TCPD-identified persons)
    IV row's (year, state_norm, cons_norm, party_abbr) matches a TCPD GE
    row → TCPD pid → if a person in our DB has that tcpd_pid, attribute.

  Method C — mint-historical (deals with pre-LS14 leaders never in sansad
    scope — Indira Gandhi, Sanjay Gandhi, etc.)
    IV row is a winner (position=1) AND a TCPD pid exists for the tuple
    AND no person in our DB owns that pid yet → mint a new person
    anchored on the TCPD pid, attribute, and any future IV row for the
    same pid attaches to this minted person.

  Otherwise: skip (minor candidate, not in our scope).

Refuses ALL attributions where the candidate_id is already claimed by an
earlier method. So Method A claims first, Method B fills gaps, Method C
mints last.

Run:
    python build_electoral_iv.py
    python build_electoral_iv.py --db /tmp/netas-test.sqlite
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent
DEFAULT_DB = REPO / "docs" / "netas" / "netas.sqlite"
IV_MIRROR = Path.home() / "Code/indiavotes-site/data/.cache/iv-mirror.sqlite"
# Raw TCPD GE CSV. Lives in the sibling parliamentwatch-data repo for now.
TCPD_GE_CSV = Path.home() / "Code/Browser/parliamentwatch-data/tcpd/raw"
PID_ASSIGNMENTS = REPO / "person_pid_assignments.json"

NAME_SUBSTRING_MIN    = 6     # longest common substring length
PARTY_EXACT_SCORE     = 0.5
ATTRIBUTION_MIN_SCORE = 1.0

LS_TERM_TO_YEAR = {14: 2004, 15: 2009, 16: 2014, 17: 2019, 18: 2024}
CURRENT_LS_YEAR = 2024

# Common surname/honorific tokens that don't discriminate between persons —
# matching only on these is meaningless ("Dharmendra Yadav" ≠ "Mulayam Singh
# Yadav" even though both share 'yadav'). Used by name_score to filter the
# token bag down to a "rare" set before checking overlap.
COMMON_NAME_TOKENS = {
    "singh", "kumar", "yadav", "gandhi", "sharma", "shah", "rao",
    "reddy", "patel", "naidu", "lal", "prasad", "das", "devi",
    "khan", "verma", "varma", "iyer", "menon", "gupta",
    "chowdhury", "chowdhary", "thakur", "tiwari", "mishra",
    "pandey", "pandit", "begum",
    # Single letters from initials (e.g., "D." in sansad → "d").
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
    "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
}


# ── Normalisation ─────────────────────────────────────────────────────────


HONORIFIC_TOKENS = {"shri", "smt", "shrimati", "dr", "ms", "mr", "mrs",
                    "prof", "adv", "sh", "km", "kumari"}


# State aliases: IV ↔ sansad name drift.
STATE_ALIASES = {
    "orissa":           "odisha",
    "odisha":           "odisha",
    "pondicherry":      "puducherry",
    "puducherry":       "puducherry",
    "nct of delhi":     "delhi",
    "delhi":            "delhi",
}


def norm_state(s):
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"\[.*?\]", " ", s)
    s = s.replace("&", " and ")
    s = re.sub(r"\d+", " ", s)
    s = re.sub(r"[^a-z ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return STATE_ALIASES.get(s, s)


def state_display(s):
    if not s:
        return ""
    s = re.sub(r"\[.*?\]", "", s)
    s = re.sub(r"\d+$", "", s)
    return s.strip()


# Constituency aliases — known spelling drift between TCPD's older forms,
# IV's stored names, and sansad's current names. Extend as misses surface.
CONS_ALIASES = {
    "sarguja":          "surguja",
    "surguja":          "surguja",
    "purnea":           "purnia",
    "purnia":           "purnia",
    "allahabad":        "prayagraj",
    "prayagraj":        "prayagraj",
    "burdwan":          "bardhaman",
    "bardhaman":        "bardhaman",
    "burdwan durgapur": "bardhaman durgapur",
    "burdwan purba":    "bardhaman purba",
    "calcutta north":   "kolkata uttar",
    "calcutta south":   "kolkata dakshin",
    "malda":            "maldaha",
    "maldah":           "maldaha",
    "maldaha":          "maldaha",
    "maldah dakshin":   "maldaha dakshin",
    "maldah uttar":     "maldaha uttar",
    "trivandrum":       "thiruvananthapuram",
    "thiruvananthapuram": "thiruvananthapuram",
    "mysore":           "mysuru",
    "mysuru":           "mysuru",
    "bombay north":     "mumbai north",
    "bombay south":     "mumbai south",
    "bombay north central": "mumbai north central",
    "bombay south central": "mumbai south central",
    "bombay north east":    "mumbai north east",
    "bombay north west":    "mumbai north west",
}


def norm_cons(s):
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return CONS_ALIASES.get(s, s)


def cons_match(a_norm: str, b_norm: str) -> bool:
    """True if two normalized cons names refer to the same constituency.

    Tolerates IV's 2024 truncations — "Bangalore Cent" ↔ "Bangalore
    Central" — by allowing a prefix match when both sides are ≥ 12
    chars (avoids "delhi" matching "delhi north" by accident).
    """
    if not a_norm or not b_norm:
        return False
    if a_norm == b_norm:
        return True
    if len(a_norm) >= 12 and len(b_norm) >= 12:
        if a_norm.startswith(b_norm) or b_norm.startswith(a_norm):
            return True
    return False


def norm_party(s):
    """Uppercase + strip trailing period ("Ind." → "IND")."""
    s = (s or "").strip().upper()
    return s.rstrip(".")


def name_tokens(s):
    """Lowercase, drop punctuation+honorifics, return token set (unordered)."""
    if not s:
        return frozenset()
    s = re.sub(r"[.,;:()\[\]'\"]", " ", s.lower())
    return frozenset(t for t in s.split() if t and t not in HONORIFIC_TOKENS)


def norm_name(s):
    """Sorted-token normalised name (string form for indexing)."""
    return " ".join(sorted(name_tokens(s)))


# ── Name compat scoring ──────────────────────────────────────────────────


def name_score(a_tokens: frozenset, b_tokens: frozenset) -> float:
    """0.0 to 1.0 — strength of name agreement.

    We score on the RARE-token bag: tokens after dropping common
    surnames/honorifics/initials. Otherwise "Dharmendra Yadav" matches
    "Mulayam Singh Yadav" on the shared 'yadav' alone — wrong.

    Layers:
      exact-equal rare-token bag → 1.0
      ≥1 rare-token shared        → 0.8
      6+ char substring shared on any rare-token pair → 0.6
      else → 0.0
    """
    if not a_tokens or not b_tokens:
        return 0.0
    a_rare = frozenset(t for t in a_tokens if t not in COMMON_NAME_TOKENS)
    b_rare = frozenset(t for t in b_tokens if t not in COMMON_NAME_TOKENS)
    if not a_rare or not b_rare:
        return 0.0
    if a_rare == b_rare:
        return 1.0
    if a_rare & b_rare:
        return 0.8
    # Substring overlap on long rare tokens (handles transliteration drift
    # like "Purandeswari" vs "Purandheshwari").
    a_long = [t for t in a_rare if len(t) >= NAME_SUBSTRING_MIN]
    b_long = [t for t in b_rare if len(t) >= NAME_SUBSTRING_MIN]
    for ta in a_long:
        for tb in b_long:
            if ta in tb or tb in ta:
                return 0.6
            for i in range(len(ta) - NAME_SUBSTRING_MIN + 1):
                if ta[i:i + NAME_SUBSTRING_MIN] in tb:
                    return 0.6
    # Token-split drift: one side has a single token that equals the
    # concatenation of two of the other side's tokens, in either order.
    # Handles "Rajnath" ↔ "Raj Nath", "Chandolia" ↔ "Chandoli ya", etc.
    def _concat_pairs_equal(single: str, multi: frozenset) -> bool:
        if len(single) < NAME_SUBSTRING_MIN or len(multi) < 2:
            return False
        toks = list(multi)
        for i in range(len(toks)):
            for j in range(len(toks)):
                if i != j and toks[i] + toks[j] == single:
                    return True
        return False
    for ta in a_rare:
        if _concat_pairs_equal(ta, b_rare):
            return 0.6
    for tb in b_rare:
        if _concat_pairs_equal(tb, a_rare):
            return 0.6
    return 0.0


def party_score(a, b) -> float:
    if a and b and a == b:
        return PARTY_EXACT_SCORE
    return 0.0


# ── Loaders ──────────────────────────────────────────────────────────────


def load_iv_rows(iv: sqlite3.Connection) -> list[dict]:
    cur = iv.execute("""
        SELECT
            c.candidate_id, c.candidate_name, c.year,
            s.state_name, p.pc_name, py.party_abbr,
            cr.pc_candidate_position AS position,
            cr.pc_candiadate_votes   AS votes,
            cr.pc_candidate_percentage_votes AS vote_share_pct
        FROM vn_candidate_master c
        LEFT JOIN vn_pc_master p ON p.pc_id = c.pc_id AND p.year = c.year
        LEFT JOIN vn_state_master s ON s.state_id = p.state_id
        LEFT JOIN vn_party_master py ON py.party_id = c.party_id
        LEFT JOIN vn_pc_candidate_results cr ON cr.candidate_id = c.candidate_id
    """)
    out = []
    for row in cur.fetchall():
        cid, name, year, state, cons, party, pos, votes, share_pct = row
        out.append({
            "candidate_id": cid,
            "candidate_name": name or "",
            "name_tokens": name_tokens(name),
            "year": year,
            "state": state_display(state),
            "state_norm": norm_state(state),
            "cons": cons or "",
            "cons_norm": norm_cons(cons),
            "party_abbr": norm_party(party),
            "position": pos,
            "won": 1 if (pos == 1) else 0,
            "votes": votes,
            "vote_share": (share_pct / 100.0) if share_pct is not None else None,
        })
    return out


def load_tcpd_ge_tuple_index() -> tuple[dict, dict]:
    """Read the full TCPD GE CSV.

    Returns:
      tuple_to_pid: (year, state_norm, cons_norm, party_abbr) → tcpd_pid
      pid_to_info:  tcpd_pid → {canonical_name, gender, last_year, states}
    """
    csv_files = sorted(TCPD_GE_CSV.glob("TCPD_GE_*.csv.gz"))
    if not csv_files:
        print(f"WARNING: no TCPD GE CSV at {TCPD_GE_CSV}", file=sys.stderr)
        return {}, {}
    src = csv_files[-1]
    tuple_to_pid: dict = {}
    pid_to_info: dict = {}
    split_count = 0
    with gzip.open(src, "rt") as f:
        for row in csv.DictReader(f):
            pid = row.get("pid") or ""
            if not pid:
                continue
            year = int(row["Year"]) if row.get("Year") else 0
            state_n = norm_state(row.get("State_Name") or "")
            cons_n = norm_cons(row.get("Constituency_Name") or "")
            party = norm_party(row.get("Party") or "")
            tup = (year, state_n, cons_n, party)
            # Apply upstream-merge-bug splits: if pid is in TCPD_PID_SPLITS,
            # the tuple may map to a child pid (e.g., GEKL5627#alt-idukki-ind).
            effective_pid = _apply_tcpd_pid_split(tup, pid)
            if effective_pid != pid:
                split_count += 1
            tuple_to_pid.setdefault(tup, effective_pid)
            info = pid_to_info.setdefault(effective_pid, {
                "canonical_name": row.get("Candidate") or "",
                "gender": row.get("Sex") or None,
                "first_year": year, "last_year": year,
                "states": set(),
            })
            info["last_year"] = max(info["last_year"], year)
            info["first_year"] = min(info["first_year"], year)
            if state_n:
                info["states"].add(state_n)
    if split_count:
        print(f"  applied {split_count} TCPD merge-bug split overrides "
              f"({len(TCPD_PID_SPLITS)} known bad pids)")
    return tuple_to_pid, pid_to_info


# ── Attribution ──────────────────────────────────────────────────────────


# Known by-election entrants — sansad's LS18 tenure for them must NOT
# claim the corresponding (year, state, cons) GE row from IV, because
# the GE winner is a different person who later vacated the seat.
# When pc_bye_election_* ingestion lands, these will get their real
# contests from there.
BYE_ELECTION_BLOCK: set[tuple[str, int]] = {
    ("ls", 5836),    # Priyanka Gandhi Vadra — won Wayanad by-election Nov 2024;
                     # GE Wayanad 2024 winner is Rahul Gandhi.
}


def _tcpd_history_by_pid(tcpd_tuple_to_pid: dict) -> dict[str, list[tuple]]:
    """Invert tcpd_tuple_to_pid → tcpd_pid → list of (year, state_norm, cons_norm, party)."""
    out: dict[str, list[tuple]] = defaultdict(list)
    for tup, pid in tcpd_tuple_to_pid.items():
        out[pid].append(tup)
    return dict(out)


def attribute_iv_primary(
    iv_rows: list[dict],
    tenures_by_pid: dict[str, list[dict]],
    persons_by_pid: dict[str, dict],
    persons_by_tcpd: dict[str, str],
    tcpd_tuple_to_pid: dict,
    tcpd_pid_info: dict,
) -> tuple[list[tuple], list[dict]]:
    """For each IV row, decide ownership. Returns (contests, new_persons).

    new_persons is a list of dicts for Method-C minted historical persons:
        {"tcpd_pid": ..., "canonical_name": ..., "gender": ...}
    """

    # Index sansad tenures by (year, state_norm, cons_norm). Also a
    # (year, state_norm) → set of cons_norms for prefix-tolerant fallback
    # when exact cons match fails (handles IV's 2024 truncations like
    # "Bangalore Cent" ↔ "Bangalore Central").
    tenure_idx: dict[tuple[int, str, str], list[dict]] = defaultdict(list)
    tenure_cons_by_ys: dict[tuple[int, str], set[str]] = defaultdict(set)
    for pid, ts in tenures_by_pid.items():
        for t in ts:
            if t["office_type"] != "ls":
                continue
            ls = parse_ls_number(t["term_label"])
            if ls is None or ls not in LS_TERM_TO_YEAR:
                continue
            year = LS_TERM_TO_YEAR[ls]
            state_n = norm_state(t["state"])
            cons_n = norm_cons(t["constituency"])
            key = (year, state_n, cons_n)
            person = persons_by_pid[pid]
            tenure_idx[key].append({
                "pid": pid,
                "party_abbr": norm_party(t["party_short"]),
                "name_tokens": name_tokens(person["canonical_name"]),
                "external_id": t.get("external_id"),
                "office_type": t["office_type"],
            })
            tenure_cons_by_ys[(year, state_n)].add(cons_n)

    # Precompute TCPD history (state, cons) tuples per person for Method
    # D's 2024-extension lookup. Party is NOT in the key — switchers like
    # Rajesh Ranjan (RJD/SP → IND for 2024 Purnia) would be missed.
    # Name compat is the discriminator instead.
    history_by_tcpd = _tcpd_history_by_pid(tcpd_tuple_to_pid)
    extension_2024_index: dict[tuple[str, str], list[str]] = defaultdict(list)
    ext_cons_by_state: dict[str, set[str]] = defaultdict(set)
    for tcpd_pid, hist in history_by_tcpd.items():
        person_pid = persons_by_tcpd.get(tcpd_pid)
        if person_pid is None:
            continue
        seen: set[tuple[str, str]] = set()
        for (_year, state_n, cons_n, _party) in hist:
            if (state_n, cons_n) in seen:
                continue
            seen.add((state_n, cons_n))
            extension_2024_index[(state_n, cons_n)].append(person_pid)
            ext_cons_by_state[state_n].add(cons_n)

    # Working state
    contests: list[tuple] = []
    new_persons: list[dict] = []
    claimed: set[int] = set()
    # For minted persons, track tcpd_pid → minted-pid so subsequent rows
    # for the same TCPD pid attribute to the same minted person.
    minted_by_tcpd: dict[str, str] = {}

    method_a_count = method_b_count = method_c_count = method_d_count = 0
    skipped_byelection = 0

    for row in iv_rows:
        if row["candidate_id"] in claimed:
            continue
        owner_pid: str | None = None
        method = None

        # ── Method A: sansad tenure match ──
        key = (row["year"], row["state_norm"], row["cons_norm"])
        cands = list(tenure_idx.get(key, []))
        # Prefix-tolerant fallback: scan tenures in same (year, state)
        # for cons_match hits (catches IV's 2024 cons truncations).
        if not cands:
            for tenure_cons in tenure_cons_by_ys.get(
                (row["year"], row["state_norm"]), ()
            ):
                if tenure_cons != row["cons_norm"] and cons_match(tenure_cons, row["cons_norm"]):
                    cands.extend(tenure_idx.get(
                        (row["year"], row["state_norm"], tenure_cons), []
                    ))
        if cands:
            scored = []
            for c in cands:
                # Skip known by-election blocks.
                if (c["office_type"], int(c["external_id"])) in BYE_ELECTION_BLOCK:
                    continue
                score = (
                    name_score(c["name_tokens"], row["name_tokens"])
                    + party_score(c["party_abbr"], row["party_abbr"])
                )
                scored.append((score, c))
            scored.sort(key=lambda s: -s[0])
            if scored and scored[0][0] >= ATTRIBUTION_MIN_SCORE:
                owner_pid = scored[0][1]["pid"]
                method = "A:sansad"
                method_a_count += 1
            elif cands and not scored:
                skipped_byelection += 1

        # ── Method B: TCPD bridge ──
        if owner_pid is None:
            tcpd_pid = tcpd_tuple_to_pid.get(
                (row["year"], row["state_norm"], row["cons_norm"], row["party_abbr"])
            )
            if tcpd_pid:
                if tcpd_pid in persons_by_tcpd:
                    owner_pid = persons_by_tcpd[tcpd_pid]
                    method = "B:tcpd-bridge"
                    method_b_count += 1
                elif tcpd_pid in minted_by_tcpd:
                    owner_pid = minted_by_tcpd[tcpd_pid]
                    method = "C:mint-continue"
                    method_c_count += 1

        # ── Method D: 2024 extension via TCPD historical (state, cons) ──
        # TCPD has no 2024 data; some sansad-anchored persons' 2024
        # contest is at a TCPD-historical constituency that's not their
        # sansad LS18 seat (Rahul Gandhi: LS18 Rae Bareli, but Wayanad
        # 2024 win is in his TCPD Kerala history) or under a different
        # party than they used historically (Rajesh Ranjan: RJD/SP → IND
        # for 2024 Purnia). Match by (state, cons) + strong name compat;
        # party is not a gate.
        if owner_pid is None and row["year"] == CURRENT_LS_YEAR:
            cands = list(extension_2024_index.get(
                (row["state_norm"], row["cons_norm"]), []
            ))
            # Prefix-tolerant fallback within the same state.
            if not cands:
                for ext_cons in ext_cons_by_state.get(row["state_norm"], ()):
                    if ext_cons != row["cons_norm"] and cons_match(ext_cons, row["cons_norm"]):
                        cands.extend(extension_2024_index.get(
                            (row["state_norm"], ext_cons), []
                        ))
            for cand_pid in cands:
                person = persons_by_pid.get(cand_pid)
                if not person:
                    continue
                # Method D extends a TCPD-anchored person to a new
                # constituency in 2024. The signal must be strong, since
                # a casual single-token name match ("chacko") would
                # wrongly attribute unrelated 2024 candidates (e.g.
                # "ROSILIN CHACKO" 2024 Chalakudy BSP ≠ P.C. Chacko).
                #
                # Allow attribution iff:
                #   1. Rare-token bags are exactly equal, OR
                #   2. One side's bag is a subset of the other AND both
                #      sides have ≥ 2 rare tokens (so single-token
                #      surname-only matches are rejected).
                p_rare = frozenset(t for t in name_tokens(person["canonical_name"])
                                   if t not in COMMON_NAME_TOKENS)
                r_rare = frozenset(t for t in row["name_tokens"]
                                   if t not in COMMON_NAME_TOKENS)
                strong = (p_rare and p_rare == r_rare) or (
                    len(p_rare) >= 2 and len(r_rare) >= 2 and
                    (p_rare <= r_rare or r_rare <= p_rare)
                )
                if strong:
                    owner_pid = cand_pid
                    method = "D:2024-ext"
                    method_d_count += 1
                    break

        # ── Method C: mint historical (only for winners) ──
        if owner_pid is None and row["won"]:
            tcpd_pid = tcpd_tuple_to_pid.get(
                (row["year"], row["state_norm"], row["cons_norm"], row["party_abbr"])
            )
            if tcpd_pid and tcpd_pid not in persons_by_tcpd and tcpd_pid not in minted_by_tcpd:
                info = tcpd_pid_info.get(tcpd_pid, {})
                new_persons.append({
                    "tcpd_pid": tcpd_pid,
                    "canonical_name": info.get("canonical_name") or row["candidate_name"],
                    "gender": info.get("gender"),
                    "first_year": info.get("first_year"),
                    "last_year": info.get("last_year"),
                })
                # Placeholder pid; real pid assigned in mint_historical_persons.
                # We use the tcpd_pid as a temporary key here.
                minted_by_tcpd[tcpd_pid] = f"TCPD:{tcpd_pid}"
                owner_pid = minted_by_tcpd[tcpd_pid]
                method = "C:mint-new"
                method_c_count += 1

        if owner_pid:
            claimed.add(row["candidate_id"])
            tcpd_pid = tcpd_tuple_to_pid.get(
                (row["year"], row["state_norm"], row["cons_norm"], row["party_abbr"])
            )
            contests.append((owner_pid, row, tcpd_pid, method))

    print(f"  Method A (sansad-tenure):    {method_a_count}")
    print(f"  Method B (TCPD-bridge):      {method_b_count}")
    print(f"  Method D (2024 extension):   {method_d_count}")
    print(f"  Method C (mint-historical):  {method_c_count}")
    if skipped_byelection:
        print(f"  (skipped {skipped_byelection} known by-election cases)")

    return contests, new_persons


def parse_ls_number(term_label):
    if not term_label:
        return None
    m = re.fullmatch(r"LS(\d+)", term_label)
    return int(m.group(1)) if m else None


# ── DB writes ────────────────────────────────────────────────────────────


def mint_historical_persons(
    conn: sqlite3.Connection,
    new_persons: list[dict],
    assignments: dict,
) -> dict[str, str]:
    """Insert minted persons into the persons table and return
    `tcpd_pid → new pid` map. Also updates the persisted assignment file
    so re-runs reuse the same pids.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    tcpd_to_pid: dict[str, str] = {}
    rows = []
    for p in new_persons:
        merge_key = f"tcpd_only:{p['tcpd_pid']}"
        if merge_key in assignments["by_merge_key"]:
            pid = assignments["by_merge_key"][merge_key]
        else:
            pid = f"n-{assignments['next_id']}"
            assignments["next_id"] += 1
            assignments["by_merge_key"][merge_key] = pid
            assignments["by_pid"][pid] = {
                "mint_date": today,
                "identity_source": "tcpd_backfill",
                "anchor_signal": "tcpd_pid",
                "merge_key": merge_key,
                "deprecated_pids": [],
                "split_into": [],
            }
        tcpd_to_pid[p["tcpd_pid"]] = pid
        rows.append((
            pid,
            p["canonical_name"],
            slug_of(p["canonical_name"]),
            None,  # dob — TCPD GE doesn't carry DOB
            {"M": "M", "F": "F"}.get(p.get("gender"), None),
            None,  # image_url
            "tcpd_backfill",
            p["tcpd_pid"],
            None, None,    # rs_tcpd_id, sansad_dob_hash
            "tcpd_pid", 0.7,
        ))
    conn.executemany(
        """INSERT OR IGNORE INTO persons
           (pid, canonical_name, slug, dob, gender, image_url,
            identity_source, tcpd_pid, rs_tcpd_id, sansad_dob_hash,
            anchor_signal, confidence)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    return tcpd_to_pid


def slug_of(name: str) -> str:
    if not name:
        return "anon"
    s = re.sub(r"[.,;:()\[\]'\"]", " ", name.lower())
    tokens = [t for t in s.split() if t and t not in HONORIFIC_TOKENS]
    n = "-".join(tokens)
    return re.sub(r"[^a-z0-9-]+", "-", n).strip("-") or "anon"


def write_contests(conn: sqlite3.Connection, attributions: list[tuple],
                   tcpd_minted_to_pid: dict[str, str]):
    """attributions: (owner_pid_or_placeholder, iv_row, tcpd_pid, method)."""
    # Resolve any TCPD-placeholder pids to real minted pids.
    resolved = []
    for owner, row, tcpd_pid, method in attributions:
        if isinstance(owner, str) and owner.startswith("TCPD:"):
            tp = owner[len("TCPD:"):]
            real = tcpd_minted_to_pid.get(tp)
            if not real:
                continue
            owner = real
        resolved.append((owner, row, tcpd_pid, method))

    # Dedupe by (pid, year, state_norm, cons_norm) keeping lowest position.
    by_key: dict[tuple, tuple] = {}
    for owner, row, tcpd_pid, method in resolved:
        k = (owner, row["year"], row["state_norm"], row["cons_norm"])
        existing = by_key.get(k)
        if existing is None:
            by_key[k] = (owner, row, tcpd_pid, method)
            continue
        ex_pos = existing[1]["position"] if existing[1]["position"] is not None else 10**9
        new_pos = row["position"] if row["position"] is not None else 10**9
        if new_pos < ex_pos:
            by_key[k] = (owner, row, tcpd_pid, method)

    rows = []
    for owner, row, tcpd_pid, _m in by_key.values():
        rows.append((
            owner, "ls", row["year"],
            row["state"], row["cons"], row["party_abbr"],
            row["votes"], row["vote_share"], row["position"], row["won"],
            row["candidate_id"], tcpd_pid,
        ))
    conn.executemany(
        """INSERT OR IGNORE INTO contests
           (pid, contest_type, year, state, constituency, party_short,
            votes, vote_share, position, won, iv_candidate_id, tcpd_pid)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )


# ── Verification ─────────────────────────────────────────────────────────


CONTEST_FIXTURES: dict[tuple[str, int], dict] = {
    # Rahul Gandhi — exact set including 2019 Amethi loss.
    ("ls", 4074): {
        "expected_contests": {
            (2004, "amethi", "INC", True),
            (2009, "amethi", "INC", True),
            (2014, "amethi", "INC", True),
            (2019, "amethi", "INC", False),
            (2019, "wayanad", "INC", True),
            (2024, "wayanad", "INC", True),
            (2024, "rae bareli", "INC", True),
        },
        "must_exclude_iv_ids": {89702},
    },
    # Manoj Kumar — Sasaram only.
    ("ls", 5578): {
        "expected_contests": {
            (2019, "sasaram", "BSP", False),
            (2024, "sasaram", "INC", True),
        },
        "max_distinct_cons": 1,
    },
    # Mulayam — multi-cons simultaneous-runner.
    ("ls", 530): {
        "expected_contains": {
            (2014, "mainpuri", "SP", True),
            (2014, "azamgarh", "SP", True),
            (1999, "kannauj", "SP", True),
            (1999, "sambhal", "SP", True),
        },
    },
    # Lalu — multi-cons + LS↔RS mover.
    ("ls", 2439): {
        "expected_contains": {
            (2004, "chapra", "RJD", True),
            (2004, "madhepura", "RJD", True),
            (2009, "saran", "RJD", True),
            (2009, "pataliputra", "RJD", False),
        },
    },
    # Vajpayee.
    ("ls", 499): {
        "expected_contains": {
            (1996, "gandhinagar", "BJP", True),
            (1996, "lucknow", "BJP", True),
            (1991, "vidisha", "BJP", True),
            (1991, "lucknow", "BJP", True),
        },
    },
    # Akhilesh.
    ("ls", 564): {
        "expected_contains": {
            (2009, "firozabad", "SP", True),
            (2009, "kannauj", "SP", True),
        },
    },
    # Sonia Gandhi.
    ("ls", 130): {
        "expected_contains": {
            (1999, "bellary", "INC", True),
            (1999, "amethi", "INC", True),
        },
    },
    # Modi.
    ("ls", 4589): {
        "expected_contains": {
            (2014, "vadodara", "BJP", True),
            (2014, "varanasi", "BJP", True),
        },
    },
    # Devegowda.
    ("ls", 3960): {
        "expected_contains": {
            (2004, "kanakapura", "JD(S)", False),
            (2004, "hassan", "JD(S)", True),
        },
    },
    # Nitish.
    ("ls", 277): {
        "expected_contains": {
            (2004, "barh", "JD(U)", False),
            (2004, "nalanda", "JD(U)", True),
        },
    },
    # Sharad Yadav.
    ("ls", 532): {
        "expected_contains": {
            (1991, "madhepura", "JD", True),
            (1991, "budaun", "JD", False),
        },
    },
    # Kushwaha.
    ("ls", 4645): {
        "expected_contains": {
            (2019, "ujiarpur", "BLSP", False),
            (2019, "karakat", "BLSP", False),
        },
    },
}


# Split fixtures — verify the TCPD upstream merge bugs are cleanly
# resolved. After splitting, the sansad-anchored person should NOT carry
# any contest from the "alt" clusters.
SPLIT_FIXTURES: list[dict] = [
    {
        "tcpd_pid": "GEKL5627",
        "expected_main_contests": {
            (1991, "trichur",       "INC", True),
            (1996, "mukundapuram",  "INC", True),
            (1998, "idukki",        "INC", True),
            (1999, "kottayam",      "INC", False),
            (2009, "thrissur",      "INC", True),
            (2014, "chalakudy",     "INC", False),
        },
        "must_exclude_tuples": {
            (1998, "idukki", "IND"),
        },
    },
    {
        "tcpd_pid": "GEBR73703",
        # 2018 Araria by-election is in TCPD but not in IV's main candidate
        # table, so it doesn't appear in our contests output. 2024 Araria
        # is real — Pradeep won re-election from his current LS18 sansad
        # tenure.
        "expected_main_contests": {
            (2009, "araria", "BJP", True),
            (2014, "araria", "BJP", False),
            (2019, "araria", "BJP", True),
            (2024, "araria", "BJP", True),
        },
        "must_exclude_tuples": {
            (2009, "amethi",     "BJP"),
            (1999, "chatra",     "IND"),
            (2004, "pratapgarh", "IND"),
        },
    },
    {
        "tcpd_pid": "GEBR65681",
        # 2024 Jahanabad is real — Surendra won re-election.
        "expected_main_contests": {
            (1998, "jahanabad", "RJD", True),
            (1999, "jahanabad", "RJD", False),
            (2009, "jahanabad", "RJD", False),
            (2014, "jahanabad", "RJD", False),
            (2019, "jahanabad", "RJD", False),
            (2024, "jahanabad", "RJD", True),
        },
        "must_exclude_tuples": {
            (1998, "jhanjharpur", "RJD"),
            (1999, "jhanjharpur", "RJD"),
        },
    },
]


# Historical fixtures — must be present after Method C mints them.
HISTORICAL_FIXTURES: list[dict] = [
    # Indira Gandhi 1980: Rae Bareli + Medak, won both.
    {
        "name_contains": "indira gandhi",
        "expected_contains": {
            (1980, "rae bareli", "INC(I)", True),
            (1980, "medak", "INC(I)", True),
            (1971, "rae bareli", "INC", True),
        },
    },
]


# TCPD upstream merge fixes — hand-curated splits of pids that wrongly
# group multiple distinct people.
#
# Format: tcpd_pid → [{label, name_hint, tuples}, ...]
#   - The first split (label="main") keeps the original tcpd_pid and
#     anchors any sansad-linked person on its tuples.
#   - Subsequent splits get a synthesized child pid (e.g. "GEKL5627#alt"),
#     and any winning contest among them is minted as a separate historical
#     person via Method C.
#   - tuples format: (year, state_norm, cons_norm, party_abbr) — must
#     match the normalisations the rest of this module uses.
TCPD_PID_SPLITS: dict[str, list[dict]] = {
    # P.C. Chacko: the INC politician (sansad-anchored) + a different
    # "P.C. Chacko" who lost as IND at Idukki 1998.
    "GEKL5627": [
        {
            "label": "main",
            "name_hint": "P.C. Chacko (INC, MP)",
            "tuples": [
                (1991, "kerala", "trichur",       "INC"),
                (1996, "kerala", "mukundapuram",  "INC"),
                (1998, "kerala", "idukki",        "INC"),
                (1999, "kerala", "kottayam",      "INC"),
                (2009, "kerala", "thrissur",      "INC"),
                (2014, "kerala", "chalakudy",     "INC"),
            ],
        },
        {
            "label": "alt-idukki-ind",
            "name_hint": "P.C. Chacko (1998 IND Idukki — different person)",
            "tuples": [
                (1998, "kerala", "idukki",        "IND"),
            ],
        },
    ],

    # Pradeep Kumar Singh: the Bihar Araria BJP politician (sansad-anchored)
    # + a different Pradeep Kumar Singh who lost UP Amethi 2009 as BJP.
    # 1999 Chatra IND and 2004 Pratapgarh IND are ambiguous low-position
    # losers; we drop them from both clusters (split into "alt-misc").
    "GEBR73703": [
        {
            "label": "main",
            "name_hint": "Pradeep Kumar Singh (Bihar Araria BJP)",
            "tuples": [
                (2009, "bihar", "araria", "BJP"),
                (2014, "bihar", "araria", "BJP"),
                (2018, "bihar", "araria", "BJP"),
                (2019, "bihar", "araria", "BJP"),
            ],
        },
        {
            "label": "alt-amethi",
            "name_hint": "Pradeep Kumar Singh (2009 UP Amethi BJP — different person)",
            "tuples": [
                (2009, "uttar pradesh", "amethi", "BJP"),
            ],
        },
        {
            "label": "alt-misc",
            "name_hint": "Pradeep Kumar Singh (1999/2004 IND fillers — unattributable)",
            "tuples": [
                (1999, "bihar",         "chatra",     "IND"),
                (2004, "uttar pradesh", "pratapgarh", "IND"),
            ],
        },
    ],

    # Surendra Prasad Yadav: the Jahanabad continuous-contestant
    # (sansad-anchored) + a different Jhanjharpur RJD politician.
    "GEBR65681": [
        {
            "label": "main",
            "name_hint": "Surendra Prasad Yadav (Jahanabad RJD)",
            "tuples": [
                (1998, "bihar", "jahanabad", "RJD"),
                (1999, "bihar", "jahanabad", "RJD"),
                (2009, "bihar", "jahanabad", "RJD"),
                (2014, "bihar", "jahanabad", "RJD"),
                (2019, "bihar", "jahanabad", "RJD"),
            ],
        },
        {
            "label": "alt-jhanjharpur",
            "name_hint": "Surendra P. Yadav (Jhanjharpur RJD — different person)",
            "tuples": [
                (1998, "bihar", "jhanjharpur", "RJD"),
                (1999, "bihar", "jhanjharpur", "RJD"),
            ],
        },
    ],
}


def _apply_tcpd_pid_split(tup, original_pid):
    """If the tuple belongs to a non-main split, return the split pid.
    Otherwise return the original pid unchanged."""
    splits = TCPD_PID_SPLITS.get(original_pid)
    if not splits:
        return original_pid
    for split in splits:
        if tup in split["tuples"]:
            if split["label"] == "main":
                return original_pid
            return f"{original_pid}#{split['label']}"
    # Tuple not explicitly covered by any split — leave on original pid as
    # a conservative default. (Should not happen for our 3 curated pids.)
    return original_pid


# Set of original pids that have a split — used by reporting to flag
# "this pid was split; the original now only carries the main cluster".
KNOWN_BAD_TCPD_PIDS = frozenset(TCPD_PID_SPLITS.keys())


def verify_contests(conn: sqlite3.Connection) -> list[str]:
    errors: list[str] = []
    for (house, mpsno), fixture in CONTEST_FIXTURES.items():
        pid_row = conn.execute(
            "SELECT pid FROM tenures WHERE office_type=? AND external_id=? LIMIT 1",
            (house, str(mpsno)),
        ).fetchone()
        if not pid_row:
            errors.append(f"FIXTURE: no pid for {house}-{mpsno}")
            continue
        pid = pid_row[0]
        contests = conn.execute(
            "SELECT year, constituency, party_short, won, iv_candidate_id "
            "FROM contests WHERE pid = ? ORDER BY year, constituency", (pid,),
        ).fetchall()
        actual = {(y, norm_cons(c), p, bool(w)) for y, c, p, w, _ in contests}

        expected = fixture.get("expected_contests")
        if expected:
            missing = expected - actual
            extra = actual - expected
            if missing or extra:
                errors.append(
                    f"FIXTURE {house}-{mpsno} ({pid}): "
                    f"missing={sorted(missing)} extra={sorted(extra)}"
                )
        expected_subset = fixture.get("expected_contains")
        if expected_subset:
            missing = expected_subset - actual
            if missing:
                errors.append(
                    f"FIXTURE {house}-{mpsno} ({pid}): expected to contain "
                    f"{sorted(missing)} but they're missing"
                )
        must_exclude = fixture.get("must_exclude_iv_ids", set())
        actual_iv_ids = {c[4] for c in contests}
        wrong = must_exclude & actual_iv_ids
        if wrong:
            errors.append(
                f"FIXTURE {house}-{mpsno} ({pid}): wrongly includes IV "
                f"candidate_ids {sorted(wrong)}"
            )
        max_cons = fixture.get("max_distinct_cons")
        if max_cons is not None:
            distinct = {c[1].lower() for c in contests if c[1]}
            if len(distinct) > max_cons:
                errors.append(
                    f"FIXTURE {house}-{mpsno} ({pid}): {len(distinct)} distinct "
                    f"constituencies, expected ≤ {max_cons}"
                )

    # Split fixtures: the sansad-anchored "main" cluster's pid must
    # have exactly the expected contests and NONE of the must_exclude
    # tuples (those belong to a different person via the split).
    for fixture in SPLIT_FIXTURES:
        tcpd_pid = fixture["tcpd_pid"]
        anchor = conn.execute(
            "SELECT pid FROM persons WHERE tcpd_pid = ?", (tcpd_pid,)
        ).fetchone()
        if not anchor:
            errors.append(f"SPLIT FIXTURE: no anchor person for {tcpd_pid}")
            continue
        pid = anchor[0]
        contests = conn.execute(
            "SELECT year, constituency, party_short, won FROM contests WHERE pid = ?",
            (pid,),
        ).fetchall()
        actual = {(y, norm_cons(c), p, bool(w)) for y, c, p, w in contests}
        expected = fixture["expected_main_contests"]
        missing = expected - actual
        extra = actual - expected
        if missing or extra:
            errors.append(
                f"SPLIT FIXTURE {tcpd_pid} ({pid}): "
                f"missing={sorted(missing)} extra={sorted(extra)}"
            )
        must_exclude = fixture["must_exclude_tuples"]
        for ex in must_exclude:
            for a in actual:
                if (a[0], a[1], a[2]) == ex:
                    errors.append(
                        f"SPLIT FIXTURE {tcpd_pid} ({pid}): wrongly includes "
                        f"{ex} (belongs to a separate person via the split)"
                    )

    # Historical fixtures: find via name.
    for fixture in HISTORICAL_FIXTURES:
        name = fixture["name_contains"]
        rows = conn.execute(
            "SELECT pid, canonical_name FROM persons "
            "WHERE LOWER(canonical_name) LIKE ?", (f"%{name}%",),
        ).fetchall()
        if not rows:
            errors.append(f"FIXTURE: no person found for '{name}'")
            continue
        # Pick the row with the most contests (likely the canonical entity).
        best = max(rows, key=lambda r: conn.execute(
            "SELECT COUNT(*) FROM contests WHERE pid = ?", (r[0],)
        ).fetchone()[0])
        contests = conn.execute(
            "SELECT year, constituency, party_short, won "
            "FROM contests WHERE pid = ?", (best[0],),
        ).fetchall()
        actual = {(y, norm_cons(c), p, bool(w)) for y, c, p, w in contests}
        missing = fixture["expected_contains"] - actual
        if missing:
            errors.append(
                f"HISTORICAL FIXTURE '{name}' (pid={best[0]}): expected to "
                f"contain {sorted(missing)} but missing"
            )

    # Global invariants.
    dups = conn.execute("""
        SELECT iv_candidate_id, COUNT(DISTINCT pid)
        FROM contests
        WHERE iv_candidate_id IS NOT NULL
        GROUP BY iv_candidate_id HAVING COUNT(DISTINCT pid) > 1
    """).fetchall()
    for cid, n in dups:
        errors.append(f"DUPLICATE: IV candidate_id {cid} → {n} pids")

    dup_cons = conn.execute("""
        SELECT pid, year, state, constituency, COUNT(*) AS n
        FROM contests
        GROUP BY pid, year, state, constituency HAVING n > 1
    """).fetchall()
    for pid, year, state, cons, n in dup_cons:
        errors.append(
            f"SAME-CONS-TWICE: pid={pid} {year} {state}/{cons} ×{n}"
        )

    return errors


def report_tcpd_splits(conn: sqlite3.Connection) -> list[str]:
    """Report on the curated TCPD merge-bug splits. Informational —
    shows that the upstream-buggy pids have been cleanly partitioned."""
    notes = []
    for tcpd_pid, splits in TCPD_PID_SPLITS.items():
        notes.append(f"split {tcpd_pid} → {len(splits)} cluster(s):")
        for s in splits:
            label = s["label"]
            effective = tcpd_pid if label == "main" else f"{tcpd_pid}#{label}"
            # Find the person (if any) anchored on this split pid.
            anchor = conn.execute(
                "SELECT pid, canonical_name FROM persons WHERE tcpd_pid = ?",
                (effective,),
            ).fetchone()
            anchor_str = f"{anchor[0]} ({anchor[1]})" if anchor else "unattributed"
            n = conn.execute(
                """SELECT COUNT(*) FROM contests
                   WHERE tcpd_pid = ? OR (
                     pid IN (SELECT pid FROM persons WHERE tcpd_pid = ?)
                   )""",
                (effective, effective),
            ).fetchone()[0]
            notes.append(f"  [{label}] {s['name_hint']}: {anchor_str}, {n} contests")
    return notes


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--iv", type=Path, default=IV_MIRROR)
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: {args.db} not found — run build_person_master.py first", file=sys.stderr)
        return 1
    if not args.iv.exists():
        print(f"ERROR: IV mirror not found at {args.iv}", file=sys.stderr)
        return 1

    print(f"Opening netas {args.db} …")
    netas = sqlite3.connect(args.db)
    netas.execute("DELETE FROM contests")
    # Also drop any previously-minted historical persons so the build is
    # repeatable. They're identifiable by identity_source = 'tcpd_backfill'.
    netas.execute("DELETE FROM persons WHERE identity_source = 'tcpd_backfill'")

    print(f"Opening IV mirror {args.iv} …")
    iv_conn = sqlite3.connect(f"file:{args.iv}?mode=ro", uri=True)

    print("Loading IV LS rows …")
    iv_rows = load_iv_rows(iv_conn)
    print(f"  {len(iv_rows)} rows")

    print("Loading TCPD GE raw CSV (tuple → pid index) …")
    tcpd_tuple_to_pid, tcpd_pid_info = load_tcpd_ge_tuple_index()
    print(f"  {len(tcpd_tuple_to_pid)} tuples; {len(tcpd_pid_info)} unique pids")

    print("Loading persons + tenures from netas.sqlite …")
    persons_by_pid: dict[str, dict] = {}
    persons_by_tcpd: dict[str, str] = {}
    for pid, canonical_name, tcpd_pid in netas.execute(
        "SELECT pid, canonical_name, tcpd_pid FROM persons"
    ):
        persons_by_pid[pid] = {"canonical_name": canonical_name, "tcpd_pid": tcpd_pid}
        if tcpd_pid:
            persons_by_tcpd[tcpd_pid] = pid

    tenures_by_pid: dict[str, list[dict]] = defaultdict(list)
    for pid, ot, term_label, ext_id, state, cons, party in netas.execute(
        "SELECT pid, office_type, term_label, external_id, state, constituency, party_short FROM tenures"
    ):
        tenures_by_pid[pid].append({
            "office_type": ot, "term_label": term_label, "external_id": ext_id,
            "state": state, "constituency": cons, "party_short": party,
        })

    print(f"  {len(persons_by_pid)} persons; {sum(len(v) for v in tenures_by_pid.values())} tenures")

    print("Attributing IV rows (IV-primary)…")
    attributions, new_persons = attribute_iv_primary(
        iv_rows, tenures_by_pid, persons_by_pid, persons_by_tcpd,
        tcpd_tuple_to_pid, tcpd_pid_info,
    )

    print(f"\nMinting {len(new_persons)} historical persons (Method C) …")
    assignments = json.loads(PID_ASSIGNMENTS.read_text())
    tcpd_minted_to_pid = mint_historical_persons(netas, new_persons, assignments)
    PID_ASSIGNMENTS.write_text(json.dumps(assignments, indent=2, ensure_ascii=False) + "\n")

    print("Writing contests …")
    write_contests(netas, attributions, tcpd_minted_to_pid)
    netas.commit()

    print("Running verification gate …")
    errors = verify_contests(netas)
    if errors:
        print(f"\nVERIFICATION FAILED — {len(errors)} error(s):")
        for e in errors[:40]:
            print(f"  ✗ {e}")
        if len(errors) > 40:
            print(f"  … and {len(errors) - 40} more")
        netas.close()
        return 1

    totals = netas.execute("SELECT COUNT(*), SUM(won) FROM contests").fetchone()
    n_persons = netas.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    n_historical = netas.execute(
        "SELECT COUNT(*) FROM persons WHERE identity_source = 'tcpd_backfill'"
    ).fetchone()[0]
    print(f"\nDone.")
    print(f"  contests: {totals[0]} ({totals[1]} wins)")
    print(f"  persons:  {n_persons} ({n_historical} historical, tcpd-only)")
    print("verification: OK")

    splits = report_tcpd_splits(netas)
    if splits:
        print(f"\nTCPD upstream merge-bug splits applied:")
        for n in splits:
            print(f"  {n}")
    netas.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
