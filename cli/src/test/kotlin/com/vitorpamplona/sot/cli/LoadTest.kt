/*
 * Copyright (c) 2026 Vitor Pamplona
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.vitorpamplona.sot.cli

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.TimeUtils
import com.vitorpamplona.sot.sync.Identity
import com.vitorpamplona.sot.sync.RelaySyncer
import com.vitorpamplona.sot.sync.SyncProgress
import com.vitorpamplona.sot.sync.SyncState
import com.vitorpamplona.sot.sync.okHttpWebsocketBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 * `gradle :cli:loadTest [-Prelay=…] [-Pservice=…]` — the full-corpus load
 * test: negentropy-sync a provider relay's ENTIRE kind-30382 set into the
 * local Vespa through the production path (RelaySyncer -> verify ->
 * batchInsert bulk path -> TrustProjection), with a synthetic observer whose
 * 10040 names the provider's service key so every score projects into the
 * ranking tensors during ingest.
 *
 * The observer's key is FIXED (secret 0x11…) so the search round afterwards
 * can NIP-42-authenticate as them and exercise trust-ranked search over the
 * loaded corpus.
 */
object LoadTest {
    /** Oldest kind-30382 created_at on the relay, by binary probing with limit-1 REQs. */
    private suspend fun oldestEventAt(
        client: NostrClient,
        relay: com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl,
        newest: Long,
    ): Long {
        var lo = 0L
        var hi = newest
        while (hi - lo > 3600) {
            val mid = (lo + hi) / 2
            var any = false
            runCatching {
                client.fetchAllPages(relay, listOf(Filter(kinds = listOf(ContactCardEvent.KIND), until = mid, limit = 1)), timeoutMs = 5_000) { any = true }
            }
            if (any) hi = mid else lo = mid
        }
        return lo
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val relayUrl = RelayUrlNormalizer.normalize(args.getOrNull(0) ?: "wss://nip85.nosfabrica.com")
        // The provider's rank-service keys (nip85.nosfabrica.com runs several;
        // discovered via a group-by-pubkey over a synced sample). Comma-separated
        // override in arg 1.
        val serviceKeys =
            args
                .getOrNull(1)
                ?.takeIf { it.isNotBlank() }
                ?.split(",")
                ?.filter { it.isNotBlank() } ?: listOf(
                "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377",
                "15d12c9a4c12460debd8c53c64f3e4f6bf421736a81f550de0921942432dd7f4",
                "9e381076cfbabf4d5839fc2c6e28b444d14d93262a3d207c35abffda4b46dc73",
                "c1ba68520d5d3d42389d2f3a31e15ba541b8cde553b7f3ee79b29c9819f5a1a6",
                "fb819c268c023a63f5749d33b67c75122216c685bb893e76c0b90de4046d776a",
                "4b7cb4027dc8fa97875367ed795a652f3d47242bc49f80230debbeb26bc33059",
                "64fe144b77002019c142939db77e11b727f7732ba1de4916422b8ba9bb9ab2ed",
                "78ed0837eba0ba244384195ce41d2a21575476a8e99e43f02d6e9729860e29e6",
                "600f45510f5f621caea766bc2484a63076b1a252909323737497448b72dc5839",
                "a1ebf1f89251988de7cb44588df39e931248c6c3c0cc9c980effa54870dbb690",
            )
        val maxEvents = args.getOrNull(2)?.toIntOrNull() ?: 0
        val slices = (args.getOrNull(3)?.toIntOrNull() ?: 1).coerceAtLeast(1)
        val reconcileConcurrency = (args.getOrNull(4)?.toIntOrNull() ?: 1).coerceAtLeast(1)
        val log: (String) -> Unit = {
            println(it)
            System.out.flush()
        }

        runBlocking {
            val stack = openStack()
            val store = stack.store
            val observer = Identity.signerFromSecret("11".repeat(32))!!
            // Insert the synthetic 10040 ONLY when the observer doesn't already
            // map these services. A re-run against a store that still holds the
            // prior run's scores would otherwise re-publish an identical 10040,
            // and the projection would walk the ENTIRE existing 30382 corpus to
            // re-attribute it (correct, but O(corpus) under the write lock —
            // it stalls the sync behind a multi-million-doc reprojection). The
            // scores project incrementally as they stream in regardless.
            val existing = store.query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND), authors = listOf(observer.pubKey)))
            val alreadyMapped = existing.flatMap { it.serviceProviders().map { p -> p.pubkey } }.toSet()
            if (alreadyMapped.containsAll(serviceKeys)) {
                log("[load] observer ${observer.pubKey} already maps ${serviceKeys.size} service key(s) - skipping 10040 re-insert (avoids a full reprojection walk)")
            } else {
                log("[load] observer ${observer.pubKey} trusts ${serviceKeys.size} service key(s) via a synthetic 10040")
                runCatching {
                    store.insert(
                        observer.sign<TrustProviderListEvent>(
                            TimeUtils.now(),
                            TrustProviderListEvent.KIND,
                            serviceKeys.map { arrayOf("30382:rank", it, relayUrl.url) }.toTypedArray(),
                            "",
                        ),
                    )
                }
            }

            val client = NostrClient(okHttpWebsocketBuilder(), CoroutineScope(Dispatchers.IO + SupervisorJob()))
            val state = SyncState.load("load-state.json")
            val progress = SyncProgress(log = log)
            progress.gauge(stack.feedGauge())
            progress.gauge {
                com.vitorpamplona.sot.vespa.IngestStats
                    .gauge()
            }
            val syncer = RelaySyncer(client, store, state, log, idleTimeoutMs = 30_000, progress = progress, reconcileConcurrency = reconcileConcurrency)
            val ticker = launch { progress.run() }

            val t0 = System.currentTimeMillis()
            try {
                // One relay-side negentropy session streams with long
                // inter-window stalls; disjoint created_at slices run several
                // sessions in parallel and the store interleaves their chunks.
                val filters =
                    if (slices == 1) {
                        listOf(Filter(kinds = listOf(ContactCardEvent.KIND)))
                    } else {
                        val newest = System.currentTimeMillis() / 1000
                        val oldest = oldestEventAt(client, relayUrl, newest)
                        log("[load] corpus spans $oldest..$newest; syncing in $slices parallel slices")
                        val step = ((newest - oldest) / slices).coerceAtLeast(1)
                        (0 until slices).map { i ->
                            val since = if (i == 0) null else oldest + i * step
                            val until = if (i == slices - 1) null else oldest + (i + 1) * step
                            Filter(kinds = listOf(ContactCardEvent.KIND), since = since, until = until)
                        }
                    }
                var downloaded = 0L
                var inserted = 0L
                coroutineScope {
                    val jobs =
                        filters.map { f ->
                            async(Dispatchers.IO) {
                                runCatching { syncer.sync(relayUrl, f, maxEvents = maxEvents) }
                                    .onFailure { log("  ! slice ${f.since}..${f.until} failed: ${it.message}") }
                                    .onSuccess { log("[load] slice ${f.since}..${f.until} done: downloaded=${it.downloaded} inserted=${it.inserted}${if (it.usedNegentropy) " (neg)" else ""}") }
                                    .getOrNull()
                            }
                        }
                    jobs.awaitAll().filterNotNull().forEach {
                        downloaded += it.downloaded
                        inserted += it.inserted
                    }
                }
                val secs = (System.currentTimeMillis() - t0) / 1000
                log("DONE - downloaded=$downloaded inserted=$inserted in ${secs}s (${if (secs > 0) downloaded / secs else 0}/s)")
            } finally {
                ticker.cancel()
                SyncState.save("load-state.json", state)
            }
            log("[load] stored 30382s: ${store.count(Filter(kinds = listOf(ContactCardEvent.KIND)))}")
            client.close()
            store.close()
        }
    }
}
