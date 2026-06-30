"""Load Nostr data into the local Vespa, the same way production does.

Two stages, runnable independently:

  profiles  REQ kind:0 (profile metadata) from one or more relays and feed each
            into Vespa via the *upstream* `upsert_profile` — identical field
            mapping to production.

  scores    Build an observer's `quality_scores` from the kind:3 follow graph
            and feed them via the *upstream* `batch_upsert_scores`. This is a
            transparent, tunable Web-of-Trust *proxy* for GrapeRank (see
            `score_from_follow_graph`): score(t) = how many of the observer's
            follows also follow t. Swap this for real GrapeRank output when you
            have it — the Vespa side is unchanged.

Default source relay matches upstream strfry-router.conf: wss://wot.grapevine.network.

Examples:
  python tools/load_nostr.py profiles --limit 5000
  python tools/load_nostr.py scores --observer <hex> --max-expand 400
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
from app.utils.observer import default_observer_pubkey  # noqa: E402

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
    """Keep only the newest event per author (kind:0 / kind:3 are replaceable)."""
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


def _p_tags(event: dict) -> list[str]:
    """Pubkeys this contact-list (kind:3) event follows."""
    out = []
    for tag in event.get("tags", []) or []:
        if len(tag) >= 2 and tag[0] == "p" and isinstance(tag[1], str):
            out.append(tag[1].lower())
    return out


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
# scores (follow-graph proxy for GrapeRank)
# ---------------------------------------------------------------------------
async def _contact_lists(
    relays: list[str], authors: list[str], timeout: float, batch: int = 100
) -> dict[str, list[str]]:
    """Fetch kind:3 for many authors (batched author filters)."""
    result: dict[str, list[str]] = {}
    for relay in relays:
        for i in range(0, len(authors), batch):
            chunk = authors[i : i + batch]
            evs = await req(
                relay, {"kinds": [3], "authors": chunk}, timeout=timeout
            )
            for pk, ev in _newest_by_pubkey(evs).items():
                # first relay wins unless a newer one shows up
                if pk not in result:
                    result[pk] = _p_tags(ev)
    return result


def score_from_follow_graph(
    follows: list[str],
    follows_of_follows: dict[str, list[str]],
    *,
    direct_base: int = 30,
    scale: float = 1.0,
    cap: int = 100,
) -> dict[str, int]:
    """A transparent WoT proxy for GrapeRank.

    score(t) = (# of the observer's follows who also follow t) * scale,
    with the observer's own direct follows floored at `direct_base` so they
    stay rankable, and everything clamped to [1, cap] (int8-safe).

    This is intentionally simple and easy to tweak — it is the first "equation"
    you can experiment with on the data side, parallel to the rank-profile
    equations on the Vespa side. Replace it wholesale with real GrapeRank
    output when available.
    """
    follow_set = set(follows)
    counts: dict[str, int] = {}
    for f in follows:
        for t in follows_of_follows.get(f, []):
            counts[t] = counts.get(t, 0) + 1

    scores: dict[str, int] = {}
    targets = set(counts) | follow_set
    for t in targets:
        raw = counts.get(t, 0) * scale
        if t in follow_set:
            raw = max(raw, direct_base)
        s = int(round(raw))
        if s > 0:
            scores[t] = min(s, cap)
    return scores


async def load_scores(
    relays: list[str],
    observer: str,
    max_expand: int,
    timeout: float,
    direct_base: int,
    scale: float,
) -> int:
    print(f"[scores] observer={observer[:16]}… ; fetching its kind:3 follow list")
    obs_lists = await _contact_lists(relays, [observer], timeout=timeout)
    follows = obs_lists.get(observer, [])
    if not follows:
        print("[scores] observer has no kind:3 follow list on these relays — "
              "nothing to score. Pass a different --observer.", file=sys.stderr)
        return 0
    print(f"[scores] observer follows {len(follows)} accounts")

    expand = follows[:max_expand]
    print(f"[scores] expanding kind:3 for {len(expand)} follows (2nd hop) ...")
    fof = await _contact_lists(relays, expand, timeout=timeout)
    print(f"[scores] got {len(fof)} contact lists")

    scores = score_from_follow_graph(
        follows, fof, direct_base=direct_base, scale=scale
    )
    print(f"[scores] computed {len(scores)} non-zero scores; feeding Vespa ...")

    upserts = [(pk, sc) for pk, sc in scores.items()]
    ok, failed = await vespa.batch_upsert_scores(upserts, [], observer)
    print(f"[scores] fed {ok} scores ({failed} failed) for observer {observer[:16]}…")
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
        choices=["profiles", "nip85", "scores-proxy", "all"],
        help="profiles=kind:0; nip85=real GrapeRank scores (kind:30382); "
             "scores-proxy=follow-graph proxy; all=profiles+nip85",
    )
    ap.add_argument("--relays", nargs="+", default=DEFAULT_RELAYS,
                    help="relays for kind:0 / kind:3 (profiles, proxy scores)")
    ap.add_argument("--score-relays", nargs="+", default=DEFAULT_SCORE_RELAYS,
                    help="relays for NIP-85 kind:30382 assertions")
    ap.add_argument("--limit", type=int, default=5000,
                    help="max kind:0 / kind:30382 events to request per relay")
    ap.add_argument("--min-rank", type=int, default=1,
                    help="skip NIP-85 assertions with rank below this (keeps the "
                         "tensor sparse; 0 keeps everything)")
    ap.add_argument("--observer", default=None,
                    help="hex pubkey whose follow graph seeds quality_scores "
                         "(default: the server's default observer)")
    ap.add_argument("--max-expand", type=int, default=400,
                    help="how many of the observer's follows to expand for the "
                         "2nd hop (bounds relay load)")
    ap.add_argument("--direct-base", type=int, default=30,
                    help="floor score for the observer's direct follows")
    ap.add_argument("--scale", type=float, default=1.0,
                    help="multiplier on the follow-of-follow count")
    ap.add_argument("--timeout", type=float, default=20.0,
                    help="per-REQ idle timeout in seconds")
    args = ap.parse_args()

    observer = args.observer or default_observer_pubkey()

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
            if args.stage == "scores-proxy":
                await load_scores(
                    args.relays, observer, args.max_expand, args.timeout,
                    args.direct_base, args.scale,
                )
        finally:
            await vespa.aclose()

    asyncio.run(run())


if __name__ == "__main__":
    main()
