package com.vitorpamplona.sot.vespa

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * Writes ready-made objects to the local Vespa document API — the sink the
 * indexer's projection calls. Knows the Vespa schema, not Nostr. Payloads are
 * partial updates, `create=true`:
 *
 *  - upsertProfile -> PUT .../docid/<pubkey> with {"fields":{...assign...}}
 *  - upsertScore   -> PUT .../docid/<subject> with a `quality_scores` tensor cell
 */
// The profile fields, each paired with its [Profile] accessor. One list drives
// both the upsert (assign values) and the blanking (assign "").
private val PROFILE_FIELDS: List<Pair<String, (Profile) -> String?>> =
    listOf(
        "name" to Profile::name,
        "display_name" to Profile::displayName,
        "about" to Profile::about,
        "picture" to Profile::picture,
        "banner" to Profile::banner,
        "nip05" to Profile::nip05,
        "lud06" to Profile::lud06,
        "lud16" to Profile::lud16,
        "website" to Profile::website,
    )

class VespaClient(
    private val baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
) {
    private val http =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            // Vespa is local; never route document writes through the egress proxy.
            .proxy(java.net.ProxySelector.of(null))
            .build()

    private fun docUrl(pubkey: String) =
        "$baseUrl/document/v1/doc/doc/docid/$pubkey?create=true"

    private fun assign(value: String?): JsonObject =
        buildJsonObject { put("assign", value ?: "") }

    /** Partial-update the standard profile fields, creating the doc if absent. */
    fun upsertProfile(p: Profile) {
        val body =
            buildJsonObject {
                put(
                    "fields",
                    buildJsonObject {
                        put("pubkey", assign(p.pubkey))
                        for ((field, get) in PROFILE_FIELDS) put(field, assign(get(p)))
                    },
                )
            }
        send(docUrl(p.pubkey), body.toString())
    }

    /** Wrap a `quality_scores` tensor op in the `{"fields":{"quality_scores":{op:...}}}` envelope. */
    private fun qualityScoresUpdate(op: String, inner: JsonObjectBuilder.() -> Unit): String =
        buildJsonObject {
            put("fields", buildJsonObject { put("quality_scores", buildJsonObject { put(op, buildJsonObject(inner)) }) })
        }.toString()

    /** Set quality_scores{observer}=rank on the subject's doc (upserts the tensor cell). */
    fun upsertScore(subject: String, observer: String, rank: Int) {
        val body =
            qualityScoresUpdate("add") {
                put(
                    "cells",
                    buildJsonArray {
                        add(
                            buildJsonObject {
                                put("address", buildJsonObject { put("user", observer) })
                                put("value", rank)
                            },
                        )
                    },
                )
            }
        send(docUrl(subject), body)
    }

    /** Remove one observer's score cell from a subject's doc (NIP-09 deletion of a 30382). */
    fun removeScore(subject: String, observer: String) {
        val body =
            qualityScoresUpdate("remove") {
                put("addresses", buildJsonArray { add(buildJsonObject { put("user", observer) }) })
            }
        send(docUrl(subject), body)
    }

    /** Blank the profile fields (NIP-09 deletion of a kind:0); keep the doc so its scores survive. */
    fun blankProfile(pubkey: String) {
        val body =
            buildJsonObject {
                put(
                    "fields",
                    buildJsonObject {
                        for ((field, _) in PROFILE_FIELDS) put(field, assign(""))
                    },
                )
            }
        send(docUrl(pubkey), body.toString())
    }

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
