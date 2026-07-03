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
package com.vitorpamplona.sot.v2.vespa

import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.FeedClient
import ai.vespa.feed.client.FeedClientBuilder
import ai.vespa.feed.client.OperationParameters
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.Executors

/** The real [ProfileIndex]: the `profile` document type over Vespa HTTP (same wiring as [VespaEventIndex]). */
class VespaProfileIndex(
    private val baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
) : ProfileIndex {
    private val feed: FeedClient =
        FeedClientBuilder
            .create(URI.create(baseUrl))
            .setRetryStrategy(
                object : FeedClient.RetryStrategy {
                    override fun retries() = 5
                },
            ).build()

    private val http =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .proxy(java.net.ProxySelector.of(null))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build()

    override suspend fun get(pubkey: String): ProfileDoc? {
        val req =
            HttpRequest
                .newBuilder(URI.create("$baseUrl/document/v1/$NAMESPACE/$DOCTYPE/docid/$pubkey"))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build()
        val resp = http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).await()
        if (resp.statusCode() == 404) return null
        require(resp.statusCode() < 400) { "vespa profile get ${resp.statusCode()}: ${resp.body().take(300)}" }
        val fields = Json.parseToJsonElement(resp.body()).jsonObject["fields"]?.jsonObject ?: return null
        return ProfileDoc.fromSummary(fields)
    }

    override suspend fun put(profile: ProfileDoc) {
        feed
            .put(
                DocumentId.of(NAMESPACE, DOCTYPE, profile.pubkey),
                buildJsonObject { put("fields", profile.indexFields()) }.toString(),
                OperationParameters.empty(),
            ).await()
    }

    override suspend fun remove(pubkey: String) {
        feed.remove(DocumentId.of(NAMESPACE, DOCTYPE, pubkey), OperationParameters.empty()).await()
    }

    override fun close() = feed.close(true)

    private companion object {
        const val NAMESPACE = "profile"
        const val DOCTYPE = "profile"
    }
}
