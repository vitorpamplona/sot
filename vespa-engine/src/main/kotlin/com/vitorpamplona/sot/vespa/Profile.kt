package com.vitorpamplona.sot.vespa

/**
 * A ready-made profile document for the Vespa index — plain data, no Nostr types.
 * The indexer maps a kind:0 event into this; [VespaClient.upsertProfile] writes it.
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

/**
 * The mutable profile fields as Vespa document fields (schema field name -> value).
 * [VespaClient.upsertProfile] assigns these; a [Profile] with only a pubkey blanks
 * them all. `pubkey` is the doc id, not a field here. Kept next to [Profile] so a
 * new property and its Vespa field name are added together.
 */
fun Profile.indexFields(): Map<String, String?> =
    linkedMapOf(
        "name" to name,
        "display_name" to displayName,
        "about" to about,
        "picture" to picture,
        "banner" to banner,
        "nip05" to nip05,
        "lud06" to lud06,
        "lud16" to lud16,
        "website" to website,
    )
