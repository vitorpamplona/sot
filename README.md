# SoT — Search over Trust

Search over Trust is a search engine for [Nostr](https://nostr.com), the open
social network. It answers one question: **when you search, whose posts show up
first?** The answer is *the people you trust*. Two users running the same search
see different rankings, because each result is ordered by the searcher's own web
of trust.

The system is three pieces sharing one [Vespa](https://vespa.ai):

- **[vespa-eventstore](https://github.com/vitorpamplona/vespa-eventstore)** — the
  store. A Vespa-backed Nostr event store with trust-ranked NIP-50 search.
- **[vespa-relay](https://github.com/vitorpamplona/vespa-relay)** — the relay.
  The standalone app clients talk to (NIP-01 filters + NIP-50 search over
  websockets); it also serves the web search UI.
- **SoT** (this repo) — the crawl. The trust-sync service that walks the Nostr web
  of trust and fills the store; the relay ranks searches out of what it puts in.

This repo is the crawl. To run the whole thing you also stand up vespa-relay (to
serve) and a Vespa (the shared store).

## How it works

Three ideas hold the whole system together.

**1. The search engine is the database.** Every Nostr event lives inside
[Vespa](https://vespa.ai), a search engine, and search runs right there. There
is no second database and no index to keep in sync — one copy of the data.
Events are stored as plain fields (`id`, `pubkey`, `kind`, `tags`, `content`,
`sig`, …), not as opaque blobs. When a client asks for an event back, the store
rebuilds it from those fields, and the rebuilt event still carries its original,
valid signature.

**2. The relay is the only API.** Everything happens over the standard Nostr
websocket protocol. Clients publish events, run filters, and search using
[NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md) — no
separate HTTP search endpoint to learn. The relay (**vespa-relay**) also serves
the web search UI at `GET /`.

**3. Ranking follows trust.** Every search is ranked for a specific **observer**
— the person doing the searching. When a user logs in (NIP-07 in the browser →
NIP-42 auth on **vespa-relay**), the relay ranks results by *that user's* trust
scores. Anonymous searches fall back to a **house account** the operator
configures. SoT (this repo) is what puts those scores there: other services publish
[NIP-85](https://github.com/nostr-protocol/nips/blob/master/85.md) trust
assertions about authors, SoT syncs them in, and the ranking math (borrowed from
[Brainstorm](https://github.com/Pretty-Good-Freedom-Tech/brainstorm)) multiplies
each search hit's relevance by how much the observer trusts its author. Results
below a trust floor are hidden unless you ask for them (`include:spam`).

So the data flows in two directions:

- **In:** a background sync downloads events from other relays, verifies every
  signature, and writes them into the store. Trust assertions (kinds 30382 /
  10040) update the author's trust scores on the way in.
- **Out:** a search comes in over the websocket, Vespa ranks the matches by the
  observer's trust, and complete events flow back to the client.

## What's searchable

A search matches on the text SoT pulls out of each event, and different fields
carry different weight: a **primary** field (a title or name) outweighs a
**secondary** field (a summary, description, or hashtags), which outweighs the
**body** (the event's `content`). Profiles (kind 0) are split into their own
name and identity fields. The matches are then ordered by your web of trust.

The kinds SoT indexes and the fields it reads from each (highest weight first):

| Kind(s) | What it is | Indexed fields |
| --- | --- | --- |
| **0** | profile | name, display name, about, NIP-05, lightning address, website |
| **1** | note | subject, hashtags, content |
| **11** | thread | title, content |
| **30023** | long-form article | title, summary + hashtags, content |
| **30818** | wiki article | title, summary, content |
| **30402** | classified listing | title, summary, content |
| **9802** | highlight | comment + context, content |
| **20** | picture | title, content |
| **21 / 22** | video | title, content |
| **1063** | file | summary, content |
| **2003** | torrent | title, content |
| **31337** | audio track | subject |
| **36787** | music track | title, artist + album, content |
| **34139** | music playlist | title, description, content |
| **54 / 10154** | podcast episode / show | title, description, content |
| **30617** | git repository | name, description, content |
| **1621 / 1618** | git issue / pull request | subject, content |
| **1337** | code snippet | name, description, content |
| **30311 / 1313** | live event / clip | title, summary, content |
| **31924 / 31922 / 31923** | calendar & slots | title, summary, content |
| **30312 / 30313** | meeting space / room | room or title, summary, content |
| **34550** | community | name, description + rules, content |
| **39000** | group | name, about |
| **40 / 41** | public chat channel | name, about |
| **31990** | app handler | name + display name, about |
| **32267** | software application | name, summary, content |
| **15128 / 35128** | website | title, description |
| **30009** | badge | name, description, content |
| **30030** | emoji pack | title, description, content |
| **9041** | zap goal | summary, content |
| **30000 / 39089** | people list / follow pack | title, description |
| **10003 / 30001 / 30003** | bookmark lists | title, description |
| **30015** | interest set | title, description + hashtags |
| **30004 / 30005 / 30006 / 30063 / 30267** | article / video / picture / release / app curation sets | title, description |
| **30002 / 39092 / 39701** | relay set / media starter pack / web bookmark | title, description |

Dozens of other titled kinds (fundraisers, workouts, exercise templates, feeds,
napplets, interactive stories, …) follow the same shape — a title or name as the
primary field, the `content` as the body. Any remaining kind Quartz can parse is
still indexed, by its full text content. The authoritative mapping is
[`store/…/SearchExtractors.kt`](store/src/main/kotlin/com/vitorpamplona/sot/store/SearchExtractors.kt).

## How the sync fills the store

The relay can only rank what it holds, so a background sync pulls two different
things from the Nostr network — always in this order, because the second depends
on the first:

1. **The trust** — who trusts whom, and by how much. This is what ranks results.
2. **The content** — the actual notes, articles, and profiles people search for.

Both passes verify every signature on the way in, and both only download what's
new since last time — each source remembers a cursor. Trust is small and always
runs; content is the firehose and is **off by default** (`SYNC_RECORDS=true`
turns it on). When content is on, both run on every pass.

### Pass 1 — the trust

Trust data is chased down a chain, each step feeding the next. The key rule: a
person's authoritative trust list lives on their own relays, so we have to find
their relays before we can believe their list.

1. **Find the observers.** Sweep the seed relays for trust lists (`kind 10040`).
   Whoever published one is an **observer** — someone whose web of trust can rank
   a search. The house account and anyone who logs in join this set too.
2. **Find each observer's relays.** Ask the well-known index relays where that
   observer keeps their events (their `kind 10002` relay list).
3. **Read their real trust list.** Go to those relays and download the observer's
   own `kind 10040` — the authoritative one — along with their profile and any
   deletions. Whatever a seed relay hinted earlier, this version wins.
4. **Download the scores.** Each trust list names scoring services and the relay
   to fetch them from. Pull those services' score events (`kind 30382`) — the
   actual trust numbers ranking uses.
5. **Sweep the leftovers.** Delete any score whose scoring service no trust list
   points to anymore (a provider was swapped out, an observer left). Ranking
   re-derives itself from what remains.

### Pass 2 — the content

Now that trust exists, the store knows which authors are worth indexing: exactly
the ones the scores in Pass 1 vouch for. Their content is pulled
highest-trust-first, so the index fills from the top down — the authors most
likely to rank at the top of a search land first.

1. **Rank the authors.** Take everyone the trust scores mention, sorted by their
   best score, and split them into bands — highest scores first.
2. **Find their relays, spread the load.** Look up each author's outbox relays.
   An author's posts sit on all their relays, so we fetch each author from just
   one — the least busy of theirs. That turns the overlap between authors into a
   way to spread work across many relays at once, which is what makes it fast:
   any single relay only serves a few thousand events a second, so the more
   relays streaming in parallel, the better.
3. **One request per relay, everything at once.** Authors that share a relay are
   fetched together — a single request pulls every searchable kind for a whole
   batch of them, plus their deletions.

Deletions here must be explicit (a `kind 5` delete or `kind 62` vanish): an
author's relay is one copy among many, so a missing note just means "not on this
copy," never "deleted." Trust scores in Pass 1 are the opposite — there, the
scoring service's relay is the single source of truth, so a score that vanishes
from it *is* a deletion.

## Running it

Everything is plain JVM (Kotlin, JDK 21). Docker is only used to run Vespa; you
can also point `VESPA_URL` at a Vespa you run yourself.

```bash
./gradlew build                 # compile + tests + formatting check
./gradlew :cli:installDist      # build the `sot` binary
export PATH="$PWD/cli/build/install/sot/bin:$PATH"

sot init      # interactive setup, writes .env (--yes for all defaults)
sot up        # start local Vespa and deploy the schema
sot serve     # run the relay + web UI + background trust sync
sot status    # is everything up? how many events of each kind?

./gradlew :cli:uiDemo           # develop the web UI against an in-memory relay (no Vespa)
```

Configuration resolves **environment variable → `.env` → built-in default**.
`sot init` walks you through it. The main knobs: `VESPA_URL`, `SERVER_PORT`,
`RELAY_URL` (the relay's public address), `HOUSE_NPUB` / `HOUSE_RELAY` (the
observer used for anonymous searches), `SEED_RELAYS` (where to look for trust
data), and `SERVER_NSEC` (the relay's own key, generated for you). See
`CLAUDE.md` for the full list.

## The code

The store is a **reusable library** (vespa-eventstore), consumed from JitPack and
pinned by commit in `gradle/libs.versions.toml`. The relay is a **separate
application** (vespa-relay) you run alongside — SoT doesn't link it. SoT itself is
the crawl and the CLI. Dependencies flow one way:
`:cli` → `:sync` → vespa-eventstore (`:store` → `:vespa`).

The companion projects (each its own repo):

| project | what it is |
| --- | --- |
| [vespa-eventstore](https://github.com/vitorpamplona/vespa-eventstore) | **Library.** The Vespa-backed Quartz `IEventStore` with trust-ranked NIP-50 search. `:vespa` owns all Vespa access — the schema (`app/`), `EventDoc`, `EventQuery` → `EventYql` (with `FuzzyWordGroup` building the fuzzy word matching), the `EventIndex`/`ReputationIndex` ports, and the `VespaEventIndex`/`VespaReputationIndex` clients. `:store` is `NostrEventStore` (all the Nostr insert rules — supersession, kind-5 deletion + tombstones, kind-62 vanish, NIP-40 expiry, gift-wrap ownership — over any `EventIndex`), the `TrustProjection` (recomputes each subject's `reputation` document whenever a kind 30382/10040 changes), `SearchExtractors`, `BulkInsert`, and the `VespaEventStore.open` front door. |
| [vespa-relay](https://github.com/vitorpamplona/vespa-relay) | **Application.** The standalone relay you run to serve clients: `NostrRelayServer` (Quartz's protocol engine over the store — filters, search, live subscriptions, COUNT, NIP-77) plus a Ktor server (`serveRelay`), the NIP-11 doc, and the bundled web search UI. Points at the same Vespa SoT fills. Not a dependency of SoT. |

SoT's own modules:

| module | what it does |
| --- | --- |
| `:sync` | Downloads from the network in two planes (see [How the sync fills the store](#how-the-sync-fills-the-store)). `RelaySyncer` orchestrates each download (negentropy or paged fallback, per-source cursors) and streams verified events into the store. `Identity` is the indexer's own key and first-run self-publish. `TrustSync` / `BlendedPass` walk the trust chain in the right order (find each observer's relays, then their trust lists, then the individual assertions) and clean up assertions that no longer apply; `RecordsPass` then pulls the searchable content for the scored authors, highest-trust-first (off unless `SYNC_RECORDS=true`). The crawl's own bookkeeping lives in a local file (`CrawlIndex`), not in Vespa. `SyncService` walks the house's trust graph once or on a loop. |
| `:cli` | The `sot` binary and the composition root that wires everything together. Commands: `init` (interactive setup), `serve` (the crawl on a loop), `index` (one crawl pass), `status`, and `up` / `down` / `deploy` / `destroy` for the local Vespa. |

## Nostr NIPs

Implemented: 01 (filters/publishes), 05, 07 + 42 (login → per-user ranking), 09
(deletion), 11 (relay info), 19, 40 (expiration), 45 (COUNT), 50 (search, plus
the `sort:` / `filter:rank:` / `include:spam` extensions), 62 (account vanish),
65 (outbox routing), 77 (negentropy sync), 85 (trust assertions), and kind
10086 (the relay's own indexer configuration).

More design background is in `docs/`.
