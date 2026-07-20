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
package com.vitorpamplona.sot.store

import com.vitorpamplona.sot.vespa.VespaApp
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Applies the bundled Vespa application package ([VespaApp]) to a running config
 * server. Standing up Vespa is the operator's job — like a database, it is a
 * prerequisite — but the SCHEMA travels with the code, so the store can deploy it
 * itself instead of asking the caller to run `vespa deploy` out of band.
 *
 * Uses only the JDK HTTP client (no curl/tar dependency), so it works anywhere the
 * store runs.
 */
class SchemaDeployer(
    private val configUrl: String,
    private val http: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build(),
) {
    /**
     * If no application is already serving at [queryUrl], deploy the bundled
     * package. This is the "migrate on first run" path [VespaEventStores.open]
     * takes: a fresh Vespa gets the schema; one that already serves an app is left
     * untouched (a schema upgrade is an explicit [deploy], not a silent redeploy on
     * every boot).
     */
    fun deployIfAbsent(queryUrl: String) {
        if (!isServing(queryUrl)) deploy()
    }

    /** Whether an application is live at [queryUrl] (Vespa answers /ApplicationStatus with 200 once an app is active). */
    fun isServing(queryUrl: String): Boolean =
        runCatching {
            val req =
                HttpRequest
                    .newBuilder(URI.create("$queryUrl/ApplicationStatus"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build()
            http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode() == 200
        }.getOrDefault(false)

    /**
     * POST the [zip] package to the config server's prepareandactivate endpoint —
     * the same request the CLI's `sot deploy` makes, JDK-native. Throws on any
     * non-2xx so a failed deploy surfaces instead of a store that silently can't
     * query. Returns the config server's response body.
     */
    fun deploy(zip: ByteArray = VespaApp.zipBytes()): String {
        val req =
            HttpRequest
                .newBuilder(URI.create("$configUrl/application/v2/tenant/default/prepareandactivate"))
                .header("Content-Type", "application/zip")
                .POST(HttpRequest.BodyPublishers.ofByteArray(zip))
                .build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofString())
        require(res.statusCode() in 200..299) { "Vespa schema deploy failed (${res.statusCode()}) at $configUrl: ${res.body()}" }
        return res.body()
    }
}
