"""Shared primitives for the questions corpus's LS + RS scrapers.

Both `questions/scrapers/loksabha.py` and `questions/scrapers/rajyasabha.py`
hit sansad.in. They need the same defensive _get() shape, jitter helper,
and RateLimited exception that cag/lc/fc/debates use. The Independence
Principle is about corpus-boundary isolation — duplicating these primitives
into each corpus keeps a renamed `debates/common.py` from causing
cascading breakage across the questions builder.

Scope: HTTP only. Each house's scraper owns its own parsing, dataclasses,
and persistence shape — even more divergent than the debates LS/RS split
because Questions LS is per-question while Questions RS is per-PDF.
"""

from __future__ import annotations

import os
import random
import time
from typing import Optional

import requests


_RETRY_AFTER_DEFAULT_SECONDS = 30
_JITTER_MIN_MS = int(os.environ.get("JITTER_MIN_MS", "250"))
_JITTER_MAX_MS = int(os.environ.get("JITTER_MAX_MS", "500"))


class RateLimited(Exception):
    """Raised on 429 / 403 with parsed Retry-After. Corpus-local per the
    Independence Principle.
    """


def jitter() -> None:
    delay = random.randint(_JITTER_MIN_MS, _JITTER_MAX_MS) / 1000.0
    time.sleep(delay)


def http_get(url: str, *, headers: Optional[dict] = None, params: Optional[dict] = None,
             timeout: int = 30, retries: int = 1,
             stream: bool = False) -> requests.Response:
    """GET with one retry, polite jitter, and 429/Retry-After handling.

    Callers supply `headers` — at minimum the per-house Referer (sansad.in
    distinguishes /ls and /rs sub-app contexts via the Referer header).
    """
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            jitter()
            resp = requests.get(url, headers=headers or {}, params=params,
                                timeout=timeout, stream=stream)
            if resp.status_code in (403, 429):
                ra = resp.headers.get("Retry-After")
                wait = _RETRY_AFTER_DEFAULT_SECONDS
                if ra:
                    try:
                        wait = int(ra)
                    except ValueError:
                        pass
                raise RateLimited(f"{resp.status_code} on {url} (retry-after {wait}s)")
            resp.raise_for_status()
            return resp
        except RateLimited:
            raise
        except Exception as e:
            last_exc = e
            if attempt < retries:
                jitter()
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError(f"unreachable http_get fallthrough for {url}")
