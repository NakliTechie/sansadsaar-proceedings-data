#!/usr/bin/env python3
"""Fetch the canonical LS + RS member master from sansad.in.

Endpoints (discovered via JS-bundle archaeology):
  LS sitting: api_ls/member?loksabha={N}&sitting=1&locale=en&page=1&size=1000
  LS non-sitting: same with sitting=0
  RS sitting: api_rs/member/sitting-members?mpFlag=1&page=1&size=1000&locale=en

Output:
  scripts/recon-netas-out/sansad_master.json
"""

from __future__ import annotations

import json
import time
import urllib.parse as up
import urllib.request as ur
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "scripts" / "recon-netas-out" / "sansad_master.json"
TERMS_OUT = REPO / "scripts" / "recon-netas-out" / "sansad_terms.json"

HDRS_LS = {"User-Agent": "Mozilla/5.0", "Referer": "https://sansad.in/ls/members"}
HDRS_RS = {"User-Agent": "Mozilla/5.0", "Referer": "https://sansad.in/rs/members"}

LS_TERMS = [18, 17, 16, 15, 14]  # mirrors DEFAULT_LOK_SABHAS in debates scraper


def fetch_json(url: str, headers: dict, timeout: int = 30) -> dict:
    req = ur.Request(url, headers=headers)
    with ur.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fetch_ls_members(loksabha: int, sitting: int) -> list[dict]:
    """Pull all LS members for a term + sitting flag (1=current/sitting, 0=former)."""
    qs = up.urlencode({
        "loksabha": loksabha,
        "sitting": sitting,
        "locale": "en",
        "page": 1,
        "size": 1000,
    })
    url = f"https://sansad.in/api_ls/member?{qs}"
    d = fetch_json(url, HDRS_LS)
    return d.get("membersDtoList", []) or []


def fetch_rs_members(mp_flag: int, page_size: int = 1000) -> list[dict]:
    """Pull RS members for a given mpFlag (1=sitting, 0=former).

    The endpoint paginates; pull until we have all of them.
    """
    out: list[dict] = []
    page = 1
    while True:
        qs = up.urlencode({
            "state": "", "party": "", "gender": "",
            "page": page, "size": page_size, "mpFlag": mp_flag,
            "ageFrom": "", "ageTo": "", "terms": "", "search": "",
            "locale": "en",
        })
        url = f"https://sansad.in/api_rs/member/sitting-members?{qs}"
        d = fetch_json(url, HDRS_RS)
        recs = d.get("records", []) or []
        if not recs:
            break
        out.extend(recs)
        total = d.get("_metadata", {}).get("totalElements", len(recs))
        if len(out) >= total:
            break
        page += 1
        time.sleep(0.3)
    return out


def normalise_ls(rec: dict, term: int, sitting: int) -> dict:
    """Map LS member record to the unified shape we'll save."""
    return {
        "mpsno": rec.get("mpsno"),
        "house": "ls",
        "loksabha": term,
        "sitting": bool(sitting),
        "status": (rec.get("status") or "").strip(),
        "initial": (rec.get("initial") or "").strip(),
        "first_name": (rec.get("firstName") or "").strip(),
        "last_name": (rec.get("lastName") or "").strip(),
        "full_name_lastfirst": (rec.get("mpLastFirstName") or "").strip(),
        "full_name_firstlast": (rec.get("mpFirstLastName") or "").strip(),
        "gender": (rec.get("gender") or "").strip(),
        "party_full": (rec.get("partyFname") or "").strip(),
        "party_short": (rec.get("partySname") or "").strip(),
        "state": (rec.get("stateName") or "").strip(),
        "constituency": (rec.get("constName") or "").strip(),
        "profession": (rec.get("profession") or "").strip(),
        "age": rec.get("age"),
        "dob": (rec.get("dob") or "").strip(),
        "terms": rec.get("noOfTerms"),
        "image_url": (rec.get("imageUrl") or "").strip(),
        "email": rec.get("email"),
    }


def normalise_rs(rec: dict) -> dict:
    return {
        "mpsno": rec.get("mpsno"),
        "house": "rs",
        "loksabha": None,
        "sitting": rec.get("mpFlag") == 1,
        "status": (rec.get("status") or "").strip(),
        "initial": (rec.get("initial") or "").strip(),
        "first_name": (rec.get("firstName") or "").strip(),
        "last_name": (rec.get("lastName") or "").strip(),
        "full_name_lastfirst": (rec.get("name") or "").strip(),  # RS has only one name column
        "full_name_firstlast": "",
        "gender": (rec.get("gender") or "").strip(),
        "party_full": (rec.get("party") or "").strip(),
        "party_short": (rec.get("partyCode") or "").strip(),
        "state": (rec.get("state") or "").strip(),
        "constituency": None,
        "profession": None,
        "age": rec.get("age"),
        "dob": (rec.get("dob") or "").strip(),
        "terms": rec.get("termCount"),
        "image_url": (rec.get("imageUrl") or "").strip(),
        "email": (rec.get("emailID") or "").strip(),
    }


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    # `members` is the de-duped per-mpsno view (one record per MP — the
    # newest/sitting entry wins). `terms` retains every (mpsno, ls_term)
    # tuple, so we can render an MP's term-by-term history.
    members: dict[tuple, dict] = {}
    terms: list[dict] = []

    print("Fetching LS members ...", flush=True)
    for term in LS_TERMS:
        for sitting in (1, 0):
            try:
                recs = fetch_ls_members(term, sitting)
            except Exception as e:
                print(f"  LS{term} sitting={sitting}: {e}")
                continue
            for r in recs:
                n = normalise_ls(r, term, sitting)
                key = ("ls", n["mpsno"])
                # Per-term log — every record, even if a more recent one wins
                # the canonical slot.
                terms.append(n)
                if key not in members:
                    members[key] = n
            print(f"  LS{term} sitting={sitting}: +{len(recs):4d}  (master size now {len(members)})")
            time.sleep(0.3)

    for mp_flag, label in ((1, "sitting"), (0, "former")):
        print(f"Fetching RS {label} members ...", flush=True)
        try:
            rs = fetch_rs_members(mp_flag)
            for r in rs:
                n = normalise_rs(r)
                key = ("rs", n["mpsno"])
                terms.append(n)
                if key in members and mp_flag == 0:
                    continue
                members[key] = n
            print(f"  RS {label}: +{len(rs)}  (master size now {len(members)})")
        except Exception as e:
            print(f"  RS {label}: {e}")

    out_list = list(members.values())
    OUT.write_text(json.dumps(out_list, ensure_ascii=False, indent=2))
    TERMS_OUT.write_text(json.dumps(terms, ensure_ascii=False, indent=2))
    print(f"Terms log: {len(terms)} (mpsno, term) records -> {TERMS_OUT.name}")
    print(f"\nSaved {len(out_list)} members -> {OUT}")
    print("Houses:", {h: sum(1 for m in out_list if m["house"] == h) for h in ("ls", "rs")})


if __name__ == "__main__":
    main()
