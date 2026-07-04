# SoT — Search over Trust

SoT is a search engine for [Nostr](https://nostr.com), the open social network.
It answers one question: **when you search, whose posts show up first?** SoT's
answer is *the people you trust*. Two users running the same search see
different rankings, because each result is ordered by the searcher's own web of
trust.

You don't need a new app to use it. SoT is a Nostr **relay** — the kind of
server every Nostr client already knows how to talk to. Point a client at it,
run a search, and get back trust-ranked results. The web page in `web/` is just
one such client; you could use any other.

## How it works

Three ideas hold the whole system together.

**1. The search engine is the database.** SoT stores every Nostr event inside
[Vespa](https://vespa.ai), a search engine, and searches it right there. There
is no second database and no index to keep in sync — one copy of the data.
Events are stored as plain fields (`id`, `pubkey`, `kind`, `tags`, `content`,
`sig`, …), not as opaque blobs. When a client asks for an event back, SoT
rebuilds it from those fields, and the rebuilt event still carries its original,
valid signature.

**2. The relay is the only API.** Everything happens over the standard Nostr
websocket protocol. Clients publish events, run filters, and search using
[NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md) — no
separate HTTP search endpoint to learn. The relay also serves its own web UI at
`GET /`.

**3. Ranking follows trust.** Every search is ranked for a specific **observer**
— the person doing the searching. When a user logs in (NIP-07 in the browser →
NIP-42 auth on the relay), the relay ranks results by *that user's* trust
scores. Anonymous searches fall back to a **house account** the operator
configures. Trust scores come from Nostr itself: other services publish
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

The project is split into small modules with a strict one-way dependency flow:
`:cli` → (`:relay`, `:sync`) → `:store` → `:profile` → `:vespa`. `:vespa` owns
all Vespa access; it may use Quartz's Nostr primitives (Hex, event helpers)
where they save re-implementing the same thing.

| module | what it does |
| --- | --- |
| `:vespa` | All talking to Vespa. Holds the Vespa schema (`app/`), maps an event to/from a stored document (`EventDoc`), turns a query into Vespa's query language (`EventQuery` → `EventYql`, with `BrainstormWordGroup` building the fuzzy word matching), and defines the `EventIndex` interface the store uses. `VespaEventIndex` is the real client; the testFixtures hold an in-memory version that serves as the reference for how queries should behave. |
| `:store` | `VespaEventStore` — the one event store, enforcing all Nostr rules on the way in: newer replaceable events supersede older ones, deletions (kind 5) and tombstones, account vanish (kind 62), expiry (NIP-40), gift-wrap ownership. It maps Nostr filters to `EventQuery`, extracts searchable text per event kind (`SearchExtractors`), and offers a batched fast path (`BulkInsert`) for high-volume sync. |
| `:profile` | Keeps trust scores up to date. `TrustProjection` wraps the store's index and, whenever a trust assertion (kind 30382 / 10040) is added or removed, recomputes the subject's trust tensors on their `profile` document — so every insert and delete path updates ranking with no special-case code. |
| `:relay` | `SotRelayServer` — the Nostr protocol engine (built on Quartz's relay base) over the store: full filter subscriptions, live updates, publish gating, COUNT (NIP-45), server-side sync (NIP-77), the NIP-11 info document, mounted on a Ktor websocket. NIP-42 login switches the ranking observer for that connection. |
| `:sync` | Downloads trust data from the network. `RelaySyncer` orchestrates the download (negentropy or paged fallback, per-source cursors) and streams verified events into the store. `Identity` is the relay's own key and first-run self-publish. `TrustSync` / `BlendedPass` walk the trust chain in the right order (find each observer's relays, then their trust lists, then the individual assertions) and clean up assertions that no longer apply. `SyncService` runs it once or on a loop and enrolls newly-logged-in users. |
| `:cli` | The `sot` binary and the composition root that wires everything together. Commands: `init` (interactive setup), `serve`, `index` (one sync pass), `status`, and `up` / `down` / `deploy` / `destroy` for the local Vespa. |
| `web/` | `index.html` — the search UI, itself a Nostr client. It opens a websocket to the server that served it and speaks NIP-50 directly: kind chips are filters, the sort menu and "unranked too" checkbox map to NIP-50 options, and "Search as me" is the NIP-07 → NIP-42 login that makes the results ranked by you. |

## Nostr NIPs

Implemented: 01 (filters/publishes), 05, 07 + 42 (login → per-user ranking), 09
(deletion), 11 (relay info), 19, 40 (expiration), 45 (COUNT), 50 (search, plus
the `sort:` / `filter:rank:` / `include:spam` extensions), 62 (account vanish),
65 (outbox routing), 77 (negentropy sync), 85 (trust assertions), and kind
10086 (the relay's own indexer configuration).

More design background is in `docs/`.
