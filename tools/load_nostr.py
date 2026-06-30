"""Load Nostr data into the local Vespa, the same way production does.

Two stages, runnable independently:

  profiles  REQ kind:0 (profile metadata) from one or more relays and feed each
            into Vespa via the *upstream* `upsert_profile` — identical field
            mapping to production.

  nip85     Real GrapeRank scores, published by Brainstorm as NIP-85 trusted
            assertions (kind:30382). Each event: author = grapevine/observer
            pubkey, `d` tag = subject pubkey, `rank` tag = 0..100. Each becomes
            one cell quality_scores{author}=rank on the subject's doc, fed via
            the *upstream* `upsert_score`.

  all       profiles + nip85.

Default relays match upstream: kind:0 from wss://wot.grapevine.network,
kind:30382 from wss://nip85-staging.nosfabrica.com.

Examples:
  python tools/load_nostr.py profiles --limit 5000
  python tools/load_nostr.py nip85 --limit 5000             # all observers' scores
  python tools/load_nostr.py nip85 --observer <hex>         # one observer's scores
  python tools/load_nostr.py all --limit 5000
"""
import argparse
import asyncio
import json
import os
import sys

# Make the vendored upstream package importable: `app.core.vespa`.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "brainstorm_server"))

import websockets  # noqa: E402

from app.core import vespa  # noqa: E402  (upstream, vendored verbatim)

DEFAULT_RELAYS = ["wss://wot.grapevine.network"]
# Brainstorm publishes GrapeRank as NIP-85 trusted assertions (kind 30382)
# here. Each event: author = the grapevine/observer pubkey, `d` tag = subject
# pubkey, `rank` tag = 0..100 trust score.
DEFAULT_SCORE_RELAYS = ["wss://nip85-staging.nosfabrica.com"]

# kind-0 content keys -> our PROFILE_FIELDS keys. Nostr clients are inconsistent
# about display_name vs displayName, so we accept both.
_CONTENT_ALIASES = {
    "displayName": "display_name",
}


# ---------------------------------------------------------------------------
# raw relay access (one REQ, drain until EOSE)
# ---------------------------------------------------------------------------
async def req(
    relay: str, filt: dict, timeout: float = 20.0, hard_cap: int = 200_000
) -> list[dict]:
    """Open one subscription with a single filter and return events until EOSE."""
    events: list[dict] = []
    try:
        async with websockets.connect(
            relay, max_size=None, open_timeout=10, ping_interval=None
        ) as ws:
            await ws.send(json.dumps(["REQ", "s", filt]))
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break
                msg = json.loads(raw)
                if not msg:
                    continue
                if msg[0] == "EVENT" and len(msg) >= 3:
                    events.append(msg[2])
                    if len(events) >= hard_cap:
                        break
                elif msg[0] == "EOSE":
                    break
                elif msg[0] == "CLOSED":
                    break
    except Exception as exc:  # noqa: BLE001  — best-effort loader
        print(f"  ! {relay}: {exc!r}", file=sys.stderr)
    return events


def _newest_by_pubkey(events: list[dict]) -> dict[str, dict]:
    """Keep only the newest event per author (kind:0 is replaceable)."""
    best: dict[str, dict] = {}
    for e in events:
        pk = e.get("pubkey")
        if not pk:
            continue
        if pk not in best or e.get("created_at", 0) > best[pk].get("created_at", 0):
            best[pk] = e
    return best


def _parse_profile(event: dict) -> dict | None:
    """Map a kind:0 event's JSON content onto PROFILE_FIELDS."""
    try:
        content = json.loads(event.get("content") or "{}")
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(content, dict):
        return None
    for src, dst in _CONTENT_ALIASES.items():
        if src in content and dst not in content:
            content[dst] = content[src]
    return {f: content.get(f) for f in vespa.PROFILE_FIELDS}


# ---------------------------------------------------------------------------
# profiles
# ---------------------------------------------------------------------------
async def load_profiles(relays: list[str], limit: int, timeout: float) -> int:
    print(f"[profiles] requesting up to {limit} kind:0 events from {relays}")
    all_events: list[dict] = []
    for relay in relays:
        evs = await req(relay, {"kinds": [0], "limit": limit}, timeout=timeout)
        print(f"  {relay}: {len(evs)} events")
        all_events.extend(evs)

    profiles = _newest_by_pubkey(all_events)
    print(f"[profiles] {len(profiles)} unique pubkeys; feeding Vespa ...")

    sem = asyncio.Semaphore(32)
    ok = 0

    async def feed(pk: str, ev: dict) -> None:
        nonlocal ok
        prof = _parse_profile(ev)
        if prof is None:
            return
        async with sem:
            try:
                await vespa.upsert_profile(pk, prof)
                ok += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  ! upsert {pk[:12]}: {exc!r}", file=sys.stderr)

    await asyncio.gather(*(feed(pk, ev) for pk, ev in profiles.items()))
    print(f"[profiles] fed {ok}/{len(profiles)} profiles")
    return ok


# ---------------------------------------------------------------------------
# scores (real GrapeRank via NIP-85 kind:30382 trusted assertions)
# ---------------------------------------------------------------------------
def _parse_assertion(event: dict) -> tuple[str, int] | None:
    """Pull (subject_pubkey, rank) from a kind:30382 event.

    The asserting author (event['pubkey']) is the *observer*; the `d` tag is the
    subject being rated; the `rank` tag is 0..100. Returns None if either is
    missing/unparseable.
    """
    subject = None
    rank = None
    for tag in event.get("tags", []) or []:
        if len(tag) < 2:
            continue
        if tag[0] == "d":
            subject = tag[1].lower()
        elif tag[0] == "rank":
            try:
                rank = int(round(float(tag[1])))
            except (TypeError, ValueError):
                rank = None
    if not subject or rank is None:
        return None
    return subject, rank


async def load_nip85(
    relays: list[str],
    limit: int,
    timeout: float,
    observer_filter: str | None,
    min_rank: int,
) -> int:
    """Feed real GrapeRank scores from NIP-85 (kind:30382) into the tensor.

    Each assertion becomes one cell quality_scores{author}=rank on the subject's
    doc, so every observer that published assertions gets its own ranking
    perspective. Pass `observer_filter` to load just one observer's assertions.
    Ranks are 0..100 — the same scale the rank-profile's quality_boost expects
    (sigmoid centred at 50), so no rescaling is needed.
    """
    filt: dict = {"kinds": [30382], "limit": limit}
    if observer_filter:
        filt["authors"] = [observer_filter]
    print(f"[nip85] requesting up to {limit} kind:30382 assertions from {relays}"
          + (f" (author={observer_filter[:16]}…)" if observer_filter else ""))

    events: list[dict] = []
    for relay in relays:
        evs = await req(relay, filt, timeout=timeout)
        print(f"  {relay}: {len(evs)} assertions")
        events.extend(evs)

    # Keep the newest assertion per (observer, subject); 30382 is addressable.
    newest: dict[tuple[str, str], tuple[int, int]] = {}  # (obs,subj)->(rank,ts)
    observers: set[str] = set()
    for e in events:
        obs = e.get("pubkey")
        parsed = _parse_assertion(e)
        if not obs or parsed is None:
            continue
        subject, rank = parsed
        ts = e.get("created_at", 0)
        key = (obs, subject)
        if key not in newest or ts > newest[key][1]:
            newest[key] = (rank, ts)
            observers.add(obs)

    triples = [
        (subj, obs, rank)
        for (obs, subj), (rank, _ts) in newest.items()
        if rank >= min_rank
    ]
    print(f"[nip85] {len(triples)} scores (rank>={min_rank}) across "
          f"{len(observers)} observer(s); feeding Vespa ...")

    sem = asyncio.Semaphore(32)
    ok = 0

    async def feed(subject: str, observer: str, rank: int) -> None:
        nonlocal ok
        async with sem:
            try:
                await vespa.upsert_score(subject, observer, rank)
                ok += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  ! score {subject[:12]}: {exc!r}", file=sys.stderr)

    await asyncio.gather(*(feed(s, o, r) for s, o, r in triples))
    print(f"[nip85] fed {ok}/{len(triples)} scores")
    if observers:
        top = sorted(observers)[:1]
        print(f"[nip85] tip: search with --observer {top[0]} to use these scores")
    return ok


# ---------------------------------------------------------------------------
# cli
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "stage",
        choices=["profiles", "nip85", "all"],
        help="profiles=kind:0; nip85=real GrapeRank scores (kind:30382); "
             "all=profiles+nip85",
    )
    ap.add_argument("--relays", nargs="+", default=DEFAULT_RELAYS,
                    help="relays for kind:0 profile metadata")
    ap.add_argument("--score-relays", nargs="+", default=DEFAULT_SCORE_RELAYS,
                    help="relays for NIP-85 kind:30382 assertions")
    ap.add_argument("--limit", type=int, default=5000,
                    help="max kind:0 / kind:30382 events to request per relay")
    ap.add_argument("--min-rank", type=int, default=1,
                    help="skip NIP-85 assertions with rank below this (keeps the "
                         "tensor sparse; 0 keeps everything)")
    ap.add_argument("--observer", default=None,
                    help="for the nip85 stage, load only this author's assertions "
                         "(default: every observer that published)")
    ap.add_argument("--timeout", type=float, default=20.0,
                    help="per-REQ idle timeout in seconds")
    args = ap.parse_args()

    async def run() -> None:
        try:
            if args.stage in ("profiles", "all"):
                await load_profiles(args.relays, args.limit, args.timeout)
            if args.stage in ("nip85", "all"):
                # `all` loads every observer's assertions; a bare `nip85` with
                # an explicit --observer loads just that one's perspective.
                obs_filter = args.observer if args.stage == "nip85" else None
                await load_nip85(
                    args.score_relays, args.limit, args.timeout,
                    obs_filter, args.min_rank,
                )
        finally:
            await vespa.aclose()

    asyncio.run(run())


if __name__ == "__main__":
    main()
