package com.vitorpamplona.sot.cli

/** `sot status` — report whether Vespa / http / relay are reachable. */
internal fun status(args: List<String>) {
    val vespa = flag(args, "--vespa", "http://localhost:8080")
    val httpUrl = flag(args, "--http", "http://localhost:8081")
    val relay = flag(args, "--relay", "http://localhost:7777")
    fun line(name: String, ok: Boolean) = println("  ${if (ok) "[ UP ]" else "[DOWN]"}  $name")
    println("component status:")
    line("Vespa        ($vespa)", ping("$vespa/ApplicationStatus"))
    line("http         ($httpUrl)", ping("$httpUrl/search?text=_"))
    line("relay        ($relay)", ping("$relay/", accept = "application/nostr+json"))
}
