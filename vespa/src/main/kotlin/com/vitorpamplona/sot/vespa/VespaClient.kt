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
package com.vitorpamplona.sot.vespa

import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.FeedClient
import ai.vespa.feed.client.FeedClientBuilder
import ai.vespa.feed.client.OperationParameters
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * Writes ready-made objects to the local Vespa document API — the sink the
 * indexer's projection calls. Knows the Vespa schema, not Nostr. Every write is
 * a partial update (`create=true`) with a `{"fields":{…}}` body — profile fields
 * for a [Profile], or a `quality_scores` tensor op for a score.
 *
 * Writes go through Vespa's async feed client — many operations multiplexed
 * over few HTTP/2 connections, with per-document ordering, retries, and
 * throttling built in — so each method returns a future the caller can count
 * or await; reads stay simple blocking queries. [close] drains what's in flight.
 *
 * Each write also records the id of the source event (`event_id` for the profile,
 * `score_event_ids{observer}` for a score cell), and [findProfileByEventId] /
 * [findScoreByEventId] resolve those ids back to a doc — so a deletion that
 * references only an event id can be mirrored into the index.
 */
class VespaClient(
    private val baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
) : AutoCloseable {
    private val feed: FeedClient =
        FeedClientBuilder
            .create(URI.create(baseUrl))
            .setRetryStrategy(
                object : FeedClient.RetryStrategy {
                    // Bounded: a dead Vespa should surface as failed futures, not hang a drain.
                    override fun retries() = 5
                },
            ).build()

    private val http =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            // Vespa is local; never route queries through the egress proxy.
            .proxy(java.net.ProxySelector.of(null))
            .build()

    /** Partial-update the standard profile fields, creating the doc if absent. */
    fun upsertProfile(p: Profile): CompletableFuture<Unit> =
        putFields(p.pubkey) {
            put("pubkey", assign(p.pubkey))
            for ((field, value) in p.indexFields()) put(field, assign(value))
        }

    /** Blank the profile fields (deletion of the kind:0); keep the doc so its scores survive. */
    fun blankProfile(pubkey: String): CompletableFuture<Unit> = upsertProfile(Profile(pubkey))

    /** Set quality_scores{observer}=rank on the subject's doc, remembering the source [eventId]. */
    fun upsertScore(
        subject: String,
        observer: String,
        rank: Int,
        eventId: String,
    ): CompletableFuture<Unit> =
        putFields(subject) {
            putJsonObject("quality_scores") {
                putJsonObject("add") {
                    putJsonArray("cells") {
                        addJsonObject {
                            putJsonObject("address") { put("user", observer) }
                            put("value", rank)
                        }
                    }
                }
            }
            putJsonObject("score_event_ids{$observer}") { put("assign", eventId) }
        }

    /** Remove one observer's score cell (and its source-id entry) from a subject's doc. */
    fun removeScore(
        subject: String,
        observer: String,
    ): CompletableFuture<Unit> =
        putFields(subject) {
            putJsonObject("quality_scores") {
                putJsonObject("remove") {
                    putJsonArray("addresses") {
                        addJsonObject { put("user", observer) }
                    }
                }
            }
            putJsonObject("score_event_ids{$observer}") { put("remove", 0) }
        }

    /**
     * The pubkey whose profile fields were written from [eventId], or null if none.
     * [eventId] is embedded in the YQL lookup — callers pass only validated ids.
     */
    fun findProfileByEventId(eventId: String): String? =
        searchHits("""select pubkey from doc where event_id contains "$eventId"""")
            .firstNotNullOfOrNull { it.stringField("pubkey") }

    /**
     * Subjects (doc pubkeys) currently holding a score cell from [observer] — one
     * page of up to 400; callers remove and re-query until empty to sweep them all.
     * [observer] is embedded in the YQL lookup — callers pass only validated keys.
     */
    fun findSubjectsByObserver(observer: String): List<String> =
        searchHits("""select pubkey from doc where score_event_ids contains sameElement(key contains "$observer")""", hits = 400)
            .mapNotNull { it.stringField("pubkey") }

    /**
     * The (subject, observer) whose score cell was written from [eventId], or null if none.
     * [eventId] is embedded in the YQL lookup — callers pass only validated ids.
     */
    fun findScoreByEventId(eventId: String): Pair<String, String>? {
        val yql = """select pubkey, score_event_ids from doc where score_event_ids contains sameElement(value contains "$eventId")"""
        for (hit in searchHits(yql)) {
            val subject = hit.stringField("pubkey") ?: continue
            val observer =
                mapEntries(hit["fields"]?.jsonObject?.get("score_event_ids"))
                    .firstOrNull { (_, id) -> id == eventId }
                    ?.first ?: continue
            return subject to observer
        }
        return null
    }

    /** Feed a partial `{"fields": {…}}` update to [docId]'s doc, creating it if absent. */
    private fun putFields(
        docId: String,
        fields: JsonObjectBuilder.() -> Unit,
    ): CompletableFuture<Unit> =
        feed
            .update(
                DocumentId.of("doc", "doc", docId),
                buildJsonObject { putJsonObject("fields", fields) }.toString(),
                OperationParameters.empty().createIfNonExistent(true),
            ).thenApply { }

    private fun assign(value: String?): JsonObject = buildJsonObject { put("assign", value ?: "") }

    /** Unranked lookup query; returns the result children (empty on no match). */
    private fun searchHits(
        yql: String,
        hits: Int = 2,
    ): List<JsonObject> {
        val url = "$baseUrl/search/?yql=${URLEncoder.encode(yql, "UTF-8")}&hits=$hits&ranking=unranked"
        val req =
            HttpRequest
                .newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build()
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() >= 400) throw RuntimeException("vespa ${resp.statusCode()}: ${resp.body().take(300)}")
        return Json
            .parseToJsonElement(resp.body())
            .jsonObject["root"]
            ?.jsonObject
            ?.get("children")
            ?.jsonArray
            ?.filterIsInstance<JsonObject>()
            ?: emptyList()
    }

    private fun JsonObject.stringField(name: String): String? =
        this["fields"]
            ?.jsonObject
            ?.get(name)
            ?.jsonPrimitive
            ?.contentOrNull

    /** A map<string,string> summary field: rendered as `{"k":"v"}` or `[{key,value}]` — accept both. */
    private fun mapEntries(el: JsonElement?): List<Pair<String, String>> =
        when (el) {
            is JsonObject -> {
                el.entries.mapNotNull { (k, v) -> v.jsonPrimitive.contentOrNull?.let { k to it } }
            }

            is JsonArray -> {
                el.mapNotNull { e ->
                    val o = e as? JsonObject ?: return@mapNotNull null
                    val k = o["key"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                    val v = o["value"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                    k to v
                }
            }

            else -> {
                emptyList()
            }
        }

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    override fun close() = feed.close(true)
}
