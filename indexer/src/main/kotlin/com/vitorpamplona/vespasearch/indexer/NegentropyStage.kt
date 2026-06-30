package com.vitorpamplona.vespasearch.indexer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.listeners.RelayConnectionListener
import com.vitorpamplona.quartz.nip01Core.relay.client.single.IRelayClient
import com.vitorpamplona.quartz.nip01Core.relay.client.single.basic.BasicRelayClient
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EoseMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EventMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.CloseCmd
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip77Negentropy.INegentropyListener
import com.vitorpamplona.quartz.nip77Negentropy.NegErrMessage
import com.vitorpamplona.quartz.nip77Negentropy.NegMsgMessage
import com.vitorpamplona.quartz.nip77Negentropy.NegentropyManager
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CompletableDeferred

/**
 * Drives a NIP-77 negentropy sync against ONE relay for ONE filter, then
 * downloads the reconciled events and hands each to [onEvent] (which writes to
 * Vespa). Negentropy enumerates the relay's *entire* matching set, so this is
 * not bounded by the ~500-event cap a plain REQ hits.
 *
 * Two safeguards make that practical against a relay holding millions of events:
 *  - id storage is capped at [maxEvents] (0 = unlimited) so reconciliation of a
 *    huge set doesn't blow the heap;
 *  - the reconciled ids are fetched through at most [maxInFlight] concurrent REQ
 *    subscriptions, each refilled on EOSE — relays cap subscriptions per
 *    connection, so firing thousands at once would stall after the first few.
 *
 * [done] completes with the number of events processed.
 */
class NegentropyStage(
    val relayUrl: NormalizedRelayUrl,
    socketBuilder: WebsocketBuilder,
    private val filter: Filter,
    private val fetchBatch: Int,
    private val maxEvents: Int,
    private val onEvent: (Event) -> Unit,
    private val log: (String) -> Unit,
    private val maxInFlight: Int = 8,
) : RelayConnectionListener,
    INegentropyListener {
    private val manager = NegentropyManager(this)
    private val subId = "neg-" + (filter.kinds?.joinToString("_") ?: "all")
    private val relayClient: IRelayClient = BasicRelayClient(relayUrl, socketBuilder, this)

    private val lock = Any()
    private val needed = LinkedHashSet<String>()
    private val inFlight = HashSet<String>()
    private var chunks: Iterator<List<String>>? = null
    private var nextSub = 0
    private val processed = AtomicInteger(0)
    private val failures = AtomicInteger(0)

    @Volatile private var negDone = false
    @Volatile private var fetchStarted = false
    @Volatile private var stopped = false

    /** Completes with the number of events processed once the stage finishes. */
    val done = CompletableDeferred<Int>()

    private fun short() = relayUrl.url.removePrefix("wss://").removePrefix("ws://").trimEnd('/')

    private fun capReached(): Boolean = maxEvents > 0 && processed.get() >= maxEvents

    fun start() {
        log("[${short()}] connecting (kinds=${filter.kinds}) ...")
        relayClient.connect()
    }

    // ---- relay connection callbacks ----
    override fun onConnected(relay: IRelayClient, pingMillis: Int, compressed: Boolean) {
        log("[${short()}] connected; starting negentropy reconciliation")
        manager.startSync(relay, subId, filter, emptyList())
    }

    override fun onIncomingMessage(relay: IRelayClient, msgStr: String, msg: Message) {
        when (msg) {
            is NegMsgMessage, is NegErrMessage -> manager.onIncomingMessage(relay, msgStr, msg)
            is EventMessage ->
                if (msg.subId.startsWith("fetch-")) {
                    try {
                        onEvent(msg.event)
                        val p = processed.incrementAndGet()
                        if (p % 500 == 0) log("[${short()}] ... $p events synced")
                        if (maxEvents > 0 && p >= maxEvents && !stopped) stopAtCap()
                    } catch (e: Exception) {
                        if (failures.incrementAndGet() <= 5) log("  ! upsert: ${e.message}")
                    }
                }
            is EoseMessage ->
                if (msg.subId.startsWith("fetch-")) {
                    relay.sendIfConnected(CloseCmd(msg.subId))
                    synchronized(lock) { inFlight.remove(msg.subId) }
                    pump()
                }
            else -> {}
        }
    }

    override fun onDisconnected(relay: IRelayClient) {
        if (negDone && fetchStarted) finish()
    }

    // ---- negentropy callbacks ----
    override fun onHaveIds(relay: NormalizedRelayUrl, subId: String, haveIds: List<String>) {
        // We start empty, so the relay never needs anything from us. Nothing to do.
    }

    override fun onNeedIds(relay: NormalizedRelayUrl, subId: String, needIds: List<String>) {
        synchronized(lock) {
            if (maxEvents in 1..needed.size) return // already have enough ids buffered
            val before = needed.size / 5000
            for (id in needIds) {
                needed.add(id)
                if (maxEvents in 1..needed.size) break
            }
            if (needed.size / 5000 > before) log("[${short()}] reconciling... ${needed.size} ids buffered")
        }
    }

    override fun onComplete(relay: NormalizedRelayUrl, subId: String) {
        negDone = true
        val ids = synchronized(lock) { needed.toList() }
        val capNote = if (maxEvents > 0) " (capped at $maxEvents)" else ""
        log("[${short()}] negentropy done: ${ids.size} ids to fetch$capNote")
        synchronized(lock) { chunks = ids.chunked(fetchBatch).iterator() }
        fetchStarted = true
        pump()
    }

    override fun onError(relay: NormalizedRelayUrl, subId: String, reason: String) {
        log("[${short()}] negentropy error: $reason - falling back to a plain REQ")
        negDone = true
        synchronized(lock) {
            if (fetchStarted) return
            fetchStarted = true
            val sub = "fetch-fallback"
            inFlight.add(sub)
            relayClient.sendOrConnectAndSync(ReqCmd(sub, listOf(filter)))
        }
    }

    /** Keep up to [maxInFlight] fetch subscriptions running; finish when drained. */
    private fun pump() {
        val toSend = mutableListOf<Pair<String, List<String>>>()
        synchronized(lock) {
            val it = chunks
            if (!stopped && it != null) {
                while (inFlight.size < maxInFlight && it.hasNext() && !capReached()) {
                    val sub = "fetch-${nextSub++}"
                    inFlight.add(sub)
                    toSend.add(sub to it.next())
                }
            }
        }
        toSend.forEach { (sub, chunk) ->
            relayClient.sendOrConnectAndSync(ReqCmd(sub, listOf(Filter(ids = chunk))))
        }
        val drained =
            synchronized(lock) {
                val it = chunks
                inFlight.isEmpty() && (stopped || it == null || !it.hasNext() || capReached())
            }
        if (negDone && fetchStarted && drained) finish()
    }

    private fun stopAtCap() {
        val subs =
            synchronized(lock) {
                stopped = true
                val s = inFlight.toList()
                inFlight.clear()
                s
            }
        subs.forEach { relayClient.sendIfConnected(CloseCmd(it)) }
        log("[${short()}] reached cap of $maxEvents events")
        finish()
    }

    private fun finish() {
        if (done.isCompleted) return
        log("[${short()}] done: ${processed.get()} events processed")
        relayClient.disconnect()
        done.complete(processed.get())
    }
}
