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

import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip19Bech32.toNsec
import com.vitorpamplona.sot.sync.Identity
import java.io.File

/**
 * `sot init` — an INTERACTIVE walkthrough that writes the `.env`: every
 * question shows its default (Enter accepts it), inputs are validated and
 * re-asked, and the relay's identity key is generated on the spot when the
 * operator doesn't paste one. `--yes` skips the questions and takes every
 * default (still generating a fresh identity); `--force` overwrites an
 * existing file; `--path <file>` writes elsewhere.
 *
 * [readLine] is injectable so tests can script the session.
 */
internal fun init(
    args: List<String>,
    readLine: () -> String? = ::readlnOrNull,
) {
    val path = flag(args, "--path", System.getenv("SOT_ENV") ?: ".env")
    val f = File(path)
    if (f.exists() && !has(args, "--force")) {
        warn("$path already exists - edit it, or re-run with --force to overwrite.")
        return
    }
    val ask = Prompter(readLine, assumeDefaults = has(args, "--yes"))
    val answers = LinkedHashMap<String, String>()

    ask.intro()

    // -- service identity ---------------------------------------------------
    ask.section("service", "What this relay calls itself - NIP-11 and the identity's kind-0 profile.")
    answers["SERVER_NAME"] = ask("service name", "sot")
    answers["SERVER_DESCRIPTION"] = ask("service description", "Search over Trust - a web-of-trust Nostr search relay")
    answers["SERVER_ICON"] = ask("service icon url", "")
    // Validators only see non-blank input (Enter already took the default),
    // and they normalize as they accept: npub/nip05 -> hex, urls -> canonical.
    answers["SERVER_PUBKEY"] = ask("admin contact (npub, hex, or name@domain)", "") { resolvePubkey(it) }

    // -- the relay's own key ------------------------------------------------
    ask.section("relay key", "Signs NIP-42 auth to upstream relays and the self-published kind 0/10002/10086.")
    val pastedNsec =
        ask("relay identity (nsec or hex secret)", "", defaultLabel = "generate a new key") { raw ->
            if (Identity.signerFromSecret(raw) != null) raw else null
        }
    val signer = if (pastedNsec.isEmpty()) null else Identity.signerFromSecret(pastedNsec)
    val keyPair = signer?.keyPair ?: KeyPair()
    answers["SERVER_NSEC"] = keyPair.privKey!!.toNsec()
    ask.note("this relay is ${keyPair.pubKey.toNpub()}")

    // -- the house account --------------------------------------------------
    ask.section("house account", "The observer behind UNAUTHENTICATED searches: results rank by this user's web of trust. NIP-42-authenticated users always rank by their own.")
    val house = ask("house account (npub, hex, or name@domain)", "", defaultLabel = "none until someone AUTHs") { resolvePubkey(it) }
    answers["HOUSE_NPUB"] = house
    answers["HOUSE_RELAY"] =
        if (house.isEmpty()) {
            ""
        } else {
            ask("house account's home relay (its kind-10002 must be readable there)", "wss://relay.damus.io", validate = ::validRelay)
        }

    // -- network ------------------------------------------------------------
    ask.section("network", "Ports and public urls. The indexer relay list is NOT config: `sot serve` publishes a kind-10086 the operator can supersede from any Nostr client.")
    val port = ask("server port", "7777") { it.toIntOrNull()?.toString() }
    answers["SERVER_PORT"] = port
    answers["RELAY_URL"] = ask("public relay url", "ws://localhost:$port", validate = ::validRelay)
    answers["SERVER_URL"] = ask("public http url", "http://localhost:$port")
    answers["VESPA_URL"] = ask("vespa endpoint", "http://localhost:8080")
    answers["SEED_RELAYS"] =
        ask("seed relays for 10040 hints (comma-separated)", "wss://purplepag.es,wss://relay.damus.io,wss://nos.lol") { raw ->
            val urls = raw.split(",").map { it.trim() }.filter { it.isNotEmpty() }
            if (urls.all { RelayUrlNormalizer.normalizeOrNull(it) != null }) urls.joinToString(",") else null
        }
    answers["SYNC_INTERVAL"] = ask("minutes between sync passes (0 = serve-only)", "15") { it.toIntOrNull()?.toString() }

    f.writeText(Config.renderDotenv(answers))
    println()
    ok("wrote $path")
    hint("relay identity: ${keyPair.pubKey.toNpub()}")
    if (house.isEmpty()) {
        hint("no house account: unauthenticated searches rank untrusted until one is set (HOUSE_NPUB) or a user AUTHs.")
    } else {
        hint("house account: unauthenticated searches rank by $house's web of trust.")
    }
    hint("next: `sot up` (start + deploy the local Vespa), then `sot serve`.")
}

/** A relay url the normalizer accepts, in its normalized form; null re-asks. */
private fun validRelay(raw: String): String? = RelayUrlNormalizer.normalizeOrNull(raw)?.url

/**
 * The question loop. [invoke] prints `name [default]:`, reads a line, and
 * validates: blank takes the default, a validator returning null re-asks (with
 * the validator's chance to normalize — npub -> hex, url -> normalized). EOF
 * (a scripted session running dry, a closed stdin) falls back to the default,
 * so `sot init < /dev/null` still completes. [assumeDefaults] (`--yes`) skips
 * the terminal entirely.
 */
internal class Prompter(
    private val readLine: () -> String?,
    private val assumeDefaults: Boolean = false,
) {
    fun intro() {
        if (assumeDefaults) return
        println(Ansi.bold("sot init") + Ansi.dim(" - Enter accepts the [default]."))
    }

    fun section(
        title: String,
        blurb: String,
    ) {
        if (assumeDefaults) return
        println()
        println(Ansi.boldCyan(title) + " " + Ansi.dim(blurb))
    }

    fun note(msg: String) {
        if (!assumeDefaults) hint("  $msg")
    }

    operator fun invoke(
        question: String,
        default: String,
        defaultLabel: String = default.ifEmpty { "none" },
        validate: (String) -> String? = { it },
    ): String {
        if (assumeDefaults) return default
        while (true) {
            print("  $question ${Ansi.dim("[$defaultLabel]")}: ")
            System.out.flush()
            val raw = readLine()?.trim() ?: return default
            if (raw.isEmpty()) return default
            validate(raw)?.let { return it }
            err("  '$raw' doesn't look right - try again, or press Enter for the default.")
        }
    }
}
