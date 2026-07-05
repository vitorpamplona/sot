# Inverted (relay-centric) sync

## The inversion

Today the sync loops **phase-outer, relay-inner**: for each phase (discover →
10002 → outboxes → providers → records bands), fan out across the relays that
phase needs. Phases are barriers — the records plane can't start until every
score is in, and a broad discovery round can't hand off until all ~1000 relays
have been swept. The machine goes idle between phases, and a single slow relay
holds up a whole round.

Invert it to **relay-outer, work-inner**: the unit of work is a **relay**, each
carrying a list of everything we owe it. A steady, oversubscribed pool always
keeps N relays in flight; the moment one drains, the next ready relay takes the
slot. There are no phases — discovery, scores, and records all ride the same
relay visits and interleave naturally.

## The work map

A persisted map, `relay -> RelayWork`, is the whole state of a sync:

```
RelayWork:
  sweep:     Boolean          # discovery: pull this relay's 10002s + 10040s
  outboxes:  Set<pubkey>      # observers whose authoritative 0/10002/10040/5/62 live here
  services:  Set<pubkey>      # 30382 service keys whose 10040 names this relay
  authors:   Set<pubkey>      # scored authors assigned here (their least-loaded outbox) for content
```

Persisting it to disk (next to the per-relay cursors) makes the whole sync
resumable and incremental at the relay level: a pass just drains the relays that
still have pending work.

## One visit per relay

When a slot picks relay R, it issues **one** combined download covering
everything in `RelayWork[R]` — the outbox kinds for its `outboxes`, kind 30382
for its `services`, every `IndexableKinds` kind for its `authors`, plus the
discovery sweep if `sweep`. One connection, maximal value, minimal churn (versus
today's separate visits per kind/phase). The batched store path already absorbs
the mixed-kind result.

## Dependency order, preserved without phases

The one invariant the phases enforce is: **a person's authoritative 10040 (on
their own outbox) must be read before their providers are chosen.** The inverted
model keeps it by **attaching work only when its dependency resolves** — a
generalization of today's `onVerified` mid-flight registration, applied to every
work type. Each arrival enqueues the next step onto the right relay:

```
sweep R → a 10040 → observer O known
   → attach "fetch 10002" to the index relays
      → O's 10002 resolves → attach O's outboxes to O's write relays
         → O's authoritative 10040 stored → attach its 30382 services to their relays
            → a 30382 → scored author A → attach A's content to A's least-loaded outbox
```

No step's work exists until its dependency has landed, so the ordering holds with
no global barrier. Supersession does the rest (the outbox 10040 still overwrites
a seed hint whenever it arrives).

## Attribution & dedup

- **Content**: an author's posts sit on all their write relays, so each author is
  assigned to exactly **one** — the least-loaded — relay's `authors` (today's
  RecordsPass load-balancing, now a per-relay assignment).
- **Scores**: a service key's 30382s are authoritative on the relay its 10040
  names; assign to that relay's `services`.
- **Discovery**: every newly-found relay gets `sweep = true` once.

Assigning before enqueue means each unit of content/score is fetched from one
place, never re-downloaded across relays.

## The pool

- **Oversubscribed**: relays are I/O-stalled, not compute-bound, so the pool runs
  far wider than the store-bound ingest wants — 256–512, or adaptive (grow while
  CPU/store sit idle, the state we keep measuring). A separate
  `discoveryConcurrency`/`relayConcurrency` knob, distinct from the store fan-out.
- **Dynamic**: grows as discovery adds relays, shrinks as work drains; done when
  no relay has pending work and nothing is in flight.
- **Dead-relay aware**: a relay that fails to become reachable is marked dead
  (keyed on *connection reachability*, not on an exception — the bug the last run
  exposed, where dead relays idle-timeout with empty results and never throw) and
  dropped from the pool for a TTL.

## Migration plan

The ingest/store/collapse/dead-cache work already landed stays underneath; only
the orchestration inverts. Incremental:

1. `RelayScheduler`: the work map + the oversubscribed pool loop + the
   dependency-gated attachment rules, covering the **scores plane**. Prove it
   matches `BlendedPass` on the in-process relay tests.
2. Fold in **records** — just another work-type (`authors`) on the same relays;
   retire `RecordsPass`.
3. Fold in **discovery** — `sweep` work-type + the `RelayUrls` collapse feeding
   the map; retire the discovery rounds.
4. Persist the work map to disk; fix dead-detection to key on reachability.

Each step keeps the old path until the new one passes the same tests.
