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

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/** Small HTTP helpers for component health checks (status + `up` readiness polling). */
private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()

/** GET [url] with an optional Accept header; null if the request throws. */
private fun get(
    url: String,
    accept: String? = null,
): HttpResponse<String>? =
    runCatching {
        val b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(3)).GET()
        if (accept != null) b.header("Accept", accept)
        http.send(b.build(), HttpResponse.BodyHandlers.ofString())
    }.getOrNull()

/** GET [url]; true if it answers < 400. [accept] sets an Accept header (e.g. NIP-11). */
internal fun ping(
    url: String,
    accept: String? = null,
): Boolean = get(url, accept).let { it != null && it.statusCode() < 400 }

/** GET [url] and return the body on 2xx, else null. */
internal fun httpGet(url: String): String? = get(url)?.takeIf { it.statusCode() < 400 }?.body()

/** Poll [url] until it answers < 400 or [tries] attempts elapse, printing dots. */
internal fun waitUntil(
    url: String,
    tries: Int = 60,
    everyMs: Long = 2000,
): Boolean {
    repeat(tries) {
        if (ping(url)) return true
        print(".")
        System.out.flush()
        Thread.sleep(everyMs)
    }
    return false
}
