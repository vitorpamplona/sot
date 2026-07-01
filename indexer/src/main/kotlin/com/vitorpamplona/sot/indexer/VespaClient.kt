package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.metadata.UserMetadata
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * Writes straight into the local Vespa document API, byte-for-byte the same
 * partial-update payloads the upstream Python `vespa.py` sends:
 *
 *  - upsertProfile -> PUT .../docid/<pubkey>?create=true with {"fields":{...assign...}}
 *  - upsertScore   -> PUT .../docid/<subject>?create=true with a tensor `add` cell
 *
 * No intermediate local event store — events synced from relays land directly
 * in the index.
 */
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

    /** Partial-update the standard kind-0 profile fields, creating the doc if absent. */
    fun upsertProfile(pubkey: String, m: UserMetadata) {
        val body =
            buildJsonObject {
                put(
                    "fields",
                    buildJsonObject {
                        put("pubkey", buildJsonObject { put("assign", pubkey) })
                        put("name", assign(m.name))
                        put("display_name", assign(m.displayName))
                        put("about", assign(m.about))
                        put("picture", assign(m.picture))
                        put("banner", assign(m.banner))
                        put("nip05", assign(m.nip05))
                        put("lud06", assign(m.lud06))
                        put("lud16", assign(m.lud16))
                        put("website", assign(m.website))
                    },
                )
            }
        send(docUrl(pubkey), body.toString())
    }

    /** Set quality_scores{observer}=rank on the subject's doc (upserts the tensor cell). */
    fun upsertScore(subject: String, observer: String, rank: Int) {
        val body =
            buildJsonObject {
                put(
                    "fields",
                    buildJsonObject {
                        put(
                            "quality_scores",
                            buildJsonObject {
                                put(
                                    "add",
                                    buildJsonObject {
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
                                    },
                                )
                            },
                        )
                    },
                )
            }
        send(docUrl(subject), body.toString())
    }

    /** Remove one observer's score cell from a subject's doc (NIP-09 deletion of a 30382). */
    fun removeScore(subject: String, observer: String) {
        val body =
            buildJsonObject {
                put(
                    "fields",
                    buildJsonObject {
                        put(
                            "quality_scores",
                            buildJsonObject {
                                put(
                                    "remove",
                                    buildJsonObject {
                                        put(
                                            "addresses",
                                            buildJsonArray { add(buildJsonObject { put("user", observer) }) },
                                        )
                                    },
                                )
                            },
                        )
                    },
                )
            }
        send(docUrl(subject), body.toString())
    }

    /** Blank the profile fields (NIP-09 deletion of a kind:0); keep the doc so its scores survive. */
    fun blankProfile(pubkey: String) {
        val body =
            buildJsonObject {
                put(
                    "fields",
                    buildJsonObject {
                        for (f in listOf("name", "display_name", "about", "picture", "banner", "nip05", "lud06", "lud16", "website")) {
                            put(f, buildJsonObject { put("assign", "") })
                        }
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
