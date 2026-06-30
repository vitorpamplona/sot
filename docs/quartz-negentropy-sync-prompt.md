# Prompt: add a high-level negentropy *sync-and-download* accessory to Quartz

Paste the section below to an agent working in the **Amethyst / Quartz**
repository (`github.com/vitorpamplona/amethyst`, module `quartz`). It asks for a
reusable NIP-77 helper that delivers *events* (not just ids), mirroring the
existing `fetchAllPages` accessory, so downstream apps stop hand-rolling the
`NegentropyManager` dance.

> Context for why this exists: a downstream project (a Nostr→Vespa indexer) had
> to write a ~150-line `NegentropyStage` to do reconcile → fetch-missing →
> upsert, and still couldn't handle relays that cap negentropy. That logic is
> generic and belongs in Quartz. The prompt below generalizes it.

---

## Task

Add a high-level **negentropy sync** accessory to `INostrClient` that downloads
every event a relay has matching a `Filter` — optionally only the delta versus a
caller-supplied set of local event ids — and delivers each event through an
`onEvent` callback. It must transparently handle the relay-side limits that make
the raw protocol painful to use.

Model the API, file placement, naming, and overload set on the **existing
paginated fetch accessory** `INostrClient.fetchAllPages`
(`quartz/.../nip01Core/relay/client/accessories/NostrClientFetchAllPagesExt.kt`).
Put the new code in the same `accessories` package.

## What already exists — build on it, do not reinvent

Low-level NIP-77 (package `com.vitorpamplona.quartz.nip77Negentropy`):

- `class NegentropyManager(listener: INegentropyListener) : RelayConnectionListener`
  - `fun startSync(relay: IRelayClient, subId: String, filter: Filter, localEvents: List<Event>, frameSizeLimit: Long = 0)`
  - `fun closeSync(relay: IRelayClient, subId: String)`
  - intercepts `NegMsgMessage` / `NegErrMessage` in `onIncomingMessage` and drives rounds
- `interface INegentropyListener { onHaveIds(relay, subId, haveIds); onNeedIds(relay, subId, needIds); onComplete(relay, subId); onError(relay, subId, reason) }`
  - `needIds` = ids the relay has that we lack (download these); `haveIds` = ids we have that it lacks
- `class NegentropySession` (open/processMessage/close, `ReconcileResult(nextCmd, haveIds, needIds)`)

Client + plumbing (package `...nip01Core.relay.client` and subpackages):

- `class NostrClient(websocketBuilder, scope) : INostrClient` — `addConnectionListener`, `removeConnectionListener`, `subscribe(subId, Map<NormalizedRelayUrl, List<Filter>>, SubscriptionListener?)`, `unsubscribe`, `connectedRelaysFlow()`, `close()`
- `interface RelayConnectionListener { onConnected(relay: IRelayClient, pingMillis, compressed); onIncomingMessage(relay, msgStr, msg: Message); onDisconnected(relay) }`
- `interface IRelayClient { val url; connect(); sendOrConnectAndSync(cmd); sendIfConnected(cmd); disconnect() }`
- accessories to mirror: `fetchAllPages`, `fetchAll`, `fetchFirst`, `fetchAsFlow`, `subscribeAsFlow`
- messages: `EventMessage(subId, event)`, `EoseMessage(subId)`, `NegMsgMessage`, `NegErrMessage`
- commands: `ReqCmd(subId, filters)`, `CloseCmd(subId)`
- `Filter(ids, authors, kinds, tags, since, until, limit, search)` with `copy(...)` and `match(event)`

## The problem to encapsulate

Today a caller who wants *events* from negentropy must, by hand:

1. register a `NegentropyManager` + an `INegentropyListener`, get the relay
   connected, and call `startSync(localEvents = …)`;
2. accumulate `onNeedIds` across rounds until `onComplete`;
3. issue `REQ`s for those ids, **bounded** to ≤ the relay's per-connection
   subscription cap (e.g. strfry advertises `max_subscriptions: 20`), refilling
   as each `EOSE` arrives — firing thousands at once stalls after the first few;
4. collect `EVENT` messages, dedupe, and detect overall completion;
5. handle `NEG-ERR` when the matched set exceeds the relay's
   `negentropy.maxSyncEvents` (strfry default **1,000,000**; observed reason
   string `blocked: too many query results`) — at which point raw negentropy
   simply cannot reconcile that filter as-is;
6. bound memory so buffering ids for a multi-million-event set doesn't OOM.

All of this is generic. Encapsulate it.

## Required API

```kotlin
// NostrClientNegentropySyncExt.kt, in ...relay.client.accessories

/** Outcome of a negentropy sync. */
class NegentropySyncResult(
    val needCount: Int,        // ids the relay had that we lacked
    val haveCount: Int,        // ids we had that the relay lacked
    val downloaded: Int,       // events actually delivered via onEvent
    val windows: Int,          // # of created_at windows used (1 if no split)
    val fellBackToPaging: Boolean, // true if a window degraded to fetchAllPages
)

suspend fun INostrClient.negentropySync(
    relay: NormalizedRelayUrl,
    filter: Filter,
    localIds: Set<HexKey> = emptySet(),   // for delta sync; empty = download all
    maxEvents: Int = 0,                   // 0 = unlimited
    maxConcurrentReqs: Int = 8,           // ≤ relay subscription cap
    fetchBatch: Int = 500,                // ids per REQ
    timeoutMs: Long = 30_000,             // per round / per page
    onProgress: ((needSoFar: Int, downloaded: Int) -> Unit)? = null,
    onEvent: (Event) -> Unit,
): NegentropySyncResult

// plus the usual overloads (String relay; single Filter) and a Flow variant
fun INostrClient.negentropySyncAsFlow(relay: NormalizedRelayUrl, filter: Filter, ...): Flow<List<Event>>
```

Semantics:

- Reconcile the relay's set for `filter` against `localIds`; **download only the
  `needIds`** (delta sync). With `localIds` empty this downloads everything.
- Fetch `needIds` through at most `maxConcurrentReqs` concurrent `REQ`
  subscriptions of `fetchBatch` ids each, refilled on `EOSE`; deliver each event
  once (dedupe by id) via `onEvent`; stop at `maxEvents`.
- **`maxSyncEvents` handling (the important generalization).** If reconciliation
  returns `NEG-ERR` for an over-large set, transparently **split `filter` by
  `created_at` windows** (`since`/`until`) and recurse per window. Make it
  adaptive: start wide, and on a window that still errors, halve its span and
  retry. If a minimal window still can't reconcile, **fall back to
  `fetchAllPages` for that window** and set `fellBackToPaging = true`. The caller
  should never have to know the relay's limit.
- Bound memory: never hold more than ~`maxEvents` (or a sane ceiling when
  unlimited) ids in flight at once; window-splitting is what keeps this bounded
  for huge sets.
- Coroutine-cancellable; on completion/cancel, `unsubscribe` all fetch subs and
  `closeSync`/`removeConnectionListener` so nothing leaks. No non-daemon threads
  left running.

## Conventions / constraints

- **commonMain KMP**, Kotlin coroutines, **no new dependencies**.
- Match `fetchAllPages` exactly for style: `suspend fun INostrClient.…`,
  `String`+`NormalizedRelayUrl` and `Filter`+`List<Filter>` overloads, a
  `callbackFlow`-based `…AsFlow` variant with `awaitClose` teardown.
- Thread-safe accumulators (callbacks fire on relay reader threads).
- License header + KDoc consistent with the file you're mirroring.

## Acceptance criteria

- Unit tests with an in-process / fake relay (see the existing
  `relay/server/inprocess` + test helpers) covering: full download; delta sync
  with a non-empty `localIds` (only missing ids fetched); `maxEvents` cap;
  bounded concurrency (never more than `maxConcurrentReqs` open subs); window
  split + `fetchAllPages` fallback when a relay rejects the full set; clean
  teardown / no leaked subscriptions.
- An integration smoke test against a real strfry relay that supports
  negentropy (e.g. `wss://wot.grapevine.network`) downloading a kind:0 slice.
- Verify behavior against a relay that **rejects** full reconciliation
  (`nip85-staging.nosfabrica.com`, kind 30382) — must succeed via windowing
  and/or paging fallback, not error out.

## Reference: the consumer this replaces

The downstream `NegentropyStage` (a Vespa indexer) implemented points 1–6 by
hand and ultimately defaulted to `fetchAllPages` because it couldn't window
around `maxSyncEvents`. Once this accessory lands, that consumer becomes:

```kotlin
client.negentropySync(relay, Filter(kinds = listOf(0)), localIds = knownIds, maxEvents = 25_000) { ev ->
    vespa.upsert(ev)
}
```
