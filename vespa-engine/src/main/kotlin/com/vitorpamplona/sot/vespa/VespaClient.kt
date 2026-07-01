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

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Writes ready-made objects to the local Vespa document API — the sink the
 * indexer's projection calls. Knows the Vespa schema, not Nostr. Every write is
 * a partial update (`create=true`): a `PUT .../docid/<id>` with a `{"fields":{…}}`
 * body — profile fields for a [Profile], or a `quality_scores` tensor op for a score.
 */
class VespaClient(private val baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080") {
    private val http =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            // Vespa is local; never route document writes through the egress proxy.
            .proxy(java.net.ProxySelector.of(null))
            .build()

    /** Partial-update the standard profile fields, creating the doc if absent. */
    fun upsertProfile(p: Profile) = putFields(p.pubkey) {
        put("pubkey", assign(p.pubkey))
        for ((field, value) in p.indexFields()) put(field, assign(value))
    }

    /** Blank the profile fields (NIP-09 deletion of a kind:0); keep the doc so its scores survive. */
    fun blankProfile(pubkey: String) = upsertProfile(Profile(pubkey))

    /** Set quality_scores{observer}=rank on the subject's doc (upserts the tensor cell). */
    fun upsertScore(subject: String, observer: String, rank: Int) = putQualityScores(subject, "add") {
        putJsonArray("cells") {
            addJsonObject {
                putJsonObject("address") { put("user", observer) }
                put("value", rank)
            }
        }
    }

    /** Remove one observer's score cell from a subject's doc (NIP-09 deletion of a 30382). */
    fun removeScore(subject: String, observer: String) = putQualityScores(subject, "remove") {
        putJsonArray("addresses") {
            addJsonObject { put("user", observer) }
        }
    }

    /** Apply a tensor op (`add` / `remove`) to the subject doc's `quality_scores` field. */
    private fun putQualityScores(subject: String, op: String, opBody: JsonObjectBuilder.() -> Unit) = putFields(subject) { putJsonObject("quality_scores") { putJsonObject(op, opBody) } }

    /** PUT a partial `{"fields": {…}}` update to [docId]'s doc, creating it if absent. */
    private fun putFields(docId: String, fields: JsonObjectBuilder.() -> Unit) = send(
        "$baseUrl/document/v1/doc/doc/docid/$docId?create=true",
        buildJsonObject { putJsonObject("fields", fields) }.toString(),
    )

    private fun assign(value: String?): JsonObject = buildJsonObject { put("assign", value ?: "") }

    private fun send(url: String, json: String) {
        val req =
            HttpRequest
                .newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .PUT(HttpRequest.BodyPublishers.ofString(json))
                .build()
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() >= 400) {
            throw RuntimeException("vespa ${resp.statusCode()}: ${resp.body().take(300)}")
        }
    }
}
