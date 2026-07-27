#!/usr/bin/env python3
"""Convert per-MP electoral history into the flat-IV shape the netas Astro
app expects, and rebuild careers.json on top of it.

Runs AFTER build_netas.py in the netas-build workflow. build_netas.py
writes docs/netas/electoral/<mpsno>.json as the raw TCPD-nested object
({ge_pid, ae_pid, ge_history, ae_history}) which doesn't match what
src/lib/netas-data.ts in indiavotes/netas expects (it wants
{contests: [...], summary: {...}}).

This script:
  1. For each mpsno: reads docs/netas/profile/<mpsno>.json + the
     TCPD per-mpsno file (tcpd/derived/history/{house}-{mpsno}.json).
  2. Emits docs/netas/electoral/<mpsno>.json in the flat-IV shape, with
     UPPERCASE TCPD constituency names title-cased and vote_share
     normalized from 0-100 to 0-1.
  3. Rewrites docs/netas/profile/<mpsno>.json's `electoral` field with
     the flat-IV summary.
  4. Rebuilds docs/netas/careers.json from the flat data, using the
     ls_*/ae_* field names the netas CareerEntry interface expects
     (instead of build_netas.py's legacy ge_*/ae_* names).

For mpsnos that have no TCPD record: clears electoral.contests to []
(better to render 'no electoral history found' than mis-attributed data).

Why this script exists (and isn't just folded into build_netas.py):
  - This is a transformation pass over build_netas.py's output. Keeping
    it separate keeps build_netas.py's pivot/corpora logic untouched.
  - The post-fetch-fixups.py in indiavotes/netas runs an equivalent
    transform locally (plus IV post-2021 enrichment, which requires the
    local IV mirror sqlite). Having the upstream tarball already in flat
    shape means consumers don't strictly need post-fetch-fixups.py to
    avoid broken pages — though netas-side enrichment is still valuable.

CI usage:
  python build_electoral_flat.py
  (no env vars needed; paths are sibling to this script)
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent
TCPD_DIR = REPO / "tcpd" / "derived" / "history"
OUT_DIR = REPO / "docs" / "netas"

CURRENT_LS = 18


def slugify(s):
    if not s:
        return None
    out = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
    return out or None


def title_constituency(raw):
    if not raw:
        return None
    m = re.match(r"^(.*?)\s*\(([^)]+)\)\s*$", raw.strip())
    if m:
        return f"{m.group(1).title()} ({m.group(2).upper()})"
    return raw.strip().title()


def tcpd_row_to_contest(row, house):
    state_name = row.get("State_Name") or ""
    cons_raw = row.get("Constituency_Name") or ""
    pct = row.get("Vote_Share_Percentage")
    return {
        "year": row.get("Year"),
        "house": house,
        "state": slugify(state_name),
        "state_name": state_name,
        "constituency_slug": slugify(cons_raw),
        "constituency": title_constituency(cons_raw),
        "party": row.get("Party"),
        "party_slug": slugify(row.get("Party")),
        "votes": row.get("Votes"),
        "vote_share": round(pct / 100, 4) if pct is not None else None,
        "position": row.get("Position"),
        "won": row.get("Position") == 1,
    }


def summarize(contests):
    ls = [c for c in contests if c.get("house") == "ls"]
    ae = [c for c in contests if c.get("house") == "ae"]
    parties = sorted({c.get("party") for c in contests if c.get("party")})
    constituencies = sorted({c.get("constituency") for c in contests if c.get("constituency")})
    states = sorted({c.get("state_name") for c in contests if c.get("state_name")})
    years = [c.get("year") for c in contests if c.get("year")]
    return {
        "total_contests": len(contests),
        "ls_contests": len(ls),
        "ae_contests": len(ae),
        "wins": sum(1 for c in contests if c.get("won")),
        "ls_wins": sum(1 for c in ls if c.get("won")),
        "ae_wins": sum(1 for c in ae if c.get("won")),
        "parties": parties,
        "constituencies": constituencies,
        "states": states,
        "first_year": min(years) if years else None,
        "last_year": max(years) if years else None,
    }


def rebuild_electoral():
    """Step 1+2: convert each electoral/<mpsno>.json to flat-IV shape."""
    profile_dir = OUT_DIR / "profile"
    electoral_dir = OUT_DIR / "electoral"
    electoral_dir.mkdir(parents=True, exist_ok=True)
    n_tcpd = 0; n_empty = 0; n_contests = 0
    for pf in sorted(profile_dir.glob("*.json")):
        try:
            mpsno = int(pf.stem)
        except ValueError:
            continue
        # TCPD per-mpsno file lookup (house-prefix).
        ls_tcpd = TCPD_DIR / f"ls-{mpsno}.json"
        rs_tcpd = TCPD_DIR / f"rs-{mpsno}.json"
        tcpd_file = ls_tcpd if ls_tcpd.exists() else (rs_tcpd if rs_tcpd.exists() else None)

        contests = []
        if tcpd_file:
            tcpd = json.loads(tcpd_file.read_text())
            for r in tcpd.get("ge_history") or []:
                contests.append(tcpd_row_to_contest(r, "ls"))
            for r in tcpd.get("ae_history") or []:
                contests.append(tcpd_row_to_contest(r, "ae"))
            n_tcpd += 1
        else:
            n_empty += 1

        contests.sort(key=lambda c: (c.get("year") or 0, c.get("house") or ""))
        n_contests += len(contests)

        elec = {"contests": contests, "summary": summarize(contests)}
        (electoral_dir / f"{mpsno}.json").write_text(
            json.dumps(elec, ensure_ascii=False, indent=2)
        )

        # Profile.electoral mirrors the flat summary.
        prof = json.loads(pf.read_text())
        prof["electoral"] = elec["summary"] if contests else None
        pf.write_text(json.dumps(prof, ensure_ascii=False, indent=2))

    print(f"  electoral: rebuilt from TCPD={n_tcpd}, no-TCPD-empty={n_empty}, "
          f"total contests={n_contests}")


def rebuild_careers():
    """Step 3: rewrite careers.json on top of the flat electoral data.

    Output schema mirrors src/lib/netas-data.ts's CareerEntry +
    CareersBoard. Replaces build_netas.py's legacy ge_*/ae_* fields
    with the ls_*/ae_* the consumer expects."""
    profile_dir = OUT_DIR / "profile"
    electoral_dir = OUT_DIR / "electoral"
    mps_file = OUT_DIR / "mps.json"
    mps = {m["mpsno"]: m for m in json.loads(mps_file.read_text())}

    entries = []
    for ef in electoral_dir.glob("*.json"):
        try:
            mpsno = int(ef.stem)
        except ValueError:
            continue
        elec = json.loads(ef.read_text())
        contests = elec.get("contests") or []
        if not contests:
            continue
        slim = mps.get(mpsno)
        if not slim:
            continue
        ls = [c for c in contests if c.get("house") == "ls"]
        ae = [c for c in contests if c.get("house") == "ae"]
        ls_wins = sum(1 for c in ls if c.get("won"))
        ae_wins = sum(1 for c in ae if c.get("won"))
        parties = sorted({c.get("party") for c in contests if c.get("party")})
        constituencies = sorted({c.get("constituency") for c in contests if c.get("constituency")})
        states = sorted({c.get("state_name") for c in contests if c.get("state_name")})
        years = [c.get("year") for c in contests if c.get("year")]
        latest = max(contests, key=lambda c: c.get("year") or 0)
        entries.append({
            "mpsno": mpsno,
            "name": slim.get("name"),
            "house": slim.get("house"),
            "loksabha": slim.get("loksabha"),
            "sitting": slim.get("sitting", False),
            "state": slim.get("state"),
            "state_slug": slim.get("state_slug"),
            "party": slim.get("party"),
            "party_slug": slim.get("party_slug"),
            "constituency": slim.get("constituency"),
            "cons_slug": slim.get("cons_slug"),
            "ls_contests": len(ls),
            "ls_wins": ls_wins,
            "ae_contests": len(ae),
            "ae_wins": ae_wins,
            "all_contests": len(contests),
            "all_wins": ls_wins + ae_wins,
            "all_parties": parties,
            "all_constituencies": constituencies,
            "all_states": states,
            "all_first_year": min(years) if years else None,
            "all_last_year": max(years) if years else None,
            "latest_year": latest.get("year"),
            "latest_house": latest.get("house"),
            "latest_constituency": latest.get("constituency"),
            "latest_party": latest.get("party"),
            "latest_vote_share": latest.get("vote_share"),
            "latest_position": latest.get("position"),
            "latest_won": latest.get("won"),
            "spans_houses": len(ls) > 0 and len(ae) > 0,
        })

    def is_current(c):
        return c["house"] == "ls" and c["sitting"] and c["loksabha"] == CURRENT_LS

    careers = {
        "totals": {
            "linked": len(entries),
            "with_any": len(entries),
            "with_ls": sum(1 for c in entries if c["ls_contests"] > 0),
            "with_ae": sum(1 for c in entries if c["ae_contests"] > 0),
            "spans_houses": sum(1 for c in entries if c["spans_houses"]),
            "ls18_sitting_with_history": sum(1 for c in entries if is_current(c)),
        },
        "turncoats": sorted(
            [c for c in entries if len(c["all_parties"]) > 1],
            key=lambda c: (-len(c["all_parties"]), -c["all_contests"]),
        )[:100],
        "constituency_hoppers": sorted(
            [c for c in entries if len(c["all_constituencies"]) > 1],
            key=lambda c: (-len(c["all_constituencies"]), -c["all_contests"]),
        )[:100],
        "veterans": sorted(entries, key=lambda c: -c["all_contests"])[:100],
        "top_winners": sorted(
            [c for c in entries if c["all_wins"] > 0],
            key=lambda c: (-c["all_wins"], -c["all_contests"]),
        )[:100],
        "house_crossovers": sorted(
            [c for c in entries if c["spans_houses"]],
            key=lambda c: -c["all_contests"],
        )[:100],
        "ls18_first_timers": [
            c for c in entries
            if c["house"] == "ls" and c["sitting"]
            and c["loksabha"] == CURRENT_LS and c["all_contests"] == 0
        ][:200],
    }
    (OUT_DIR / "careers.json").write_text(
        json.dumps(careers, ensure_ascii=False, indent=2)
    )
    top_n = len(careers["turncoats"][0]["all_parties"]) if careers["turncoats"] else 0
    print(f"  careers: {len(entries)} entries, top turncoat = {top_n} parties")


def main():
    print(f"build_electoral_flat: transforming {OUT_DIR}")
    rebuild_electoral()
    rebuild_careers()
    print("Done.")


if __name__ == "__main__":
    main()
