package com.vitorpamplona.sot.vespa

/**
 * A ready-made profile document for the Vespa index — plain data, no Nostr types.
 * The indexer maps a kind:0 event into this; [VespaClient.upsertProfile] writes it.
 * Field names here are semantic; [VespaClient] maps them to the Vespa schema.
 */
data class Profile(
    val pubkey: String,
    val name: String? = null,
    val displayName: String? = null,
    val about: String? = null,
    val picture: String? = null,
    val banner: String? = null,
    val nip05: String? = null,
    val lud06: String? = null,
    val lud16: String? = null,
    val website: String? = null,
)
