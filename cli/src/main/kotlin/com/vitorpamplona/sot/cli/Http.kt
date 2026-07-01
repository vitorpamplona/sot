package com.vitorpamplona.sot.cli

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/** Small HTTP helpers for component health checks (status + `up` readiness polling). */
private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()

/** GET [url]; true if it answers < 400. [accept] sets an Accept header (e.g. NIP-11). */
internal fun ping(url: String, accept: String? = null): Boolean =
    runCatching {
        val b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(3)).GET()
        if (accept != null) b.header("Accept", accept)
        http.send(b.build(), HttpResponse.BodyHandlers.ofString()).statusCode() < 400
    }.getOrDefault(false)

/** GET [url] and return the body on 2xx, else null. */
internal fun httpGet(url: String): String? =
    runCatching {
        val req = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(3)).GET().build()
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() < 400) resp.body() else null
    }.getOrNull()

/** Poll [url] until it answers < 400 or [tries] attempts elapse, printing dots. */
internal fun waitUntil(url: String, tries: Int = 60, everyMs: Long = 2000): Boolean {
    repeat(tries) {
        if (ping(url)) return true
        print("."); System.out.flush()
        Thread.sleep(everyMs)
    }
    return false
}
