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
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip19Bech32.toNsec
import com.vitorpamplona.sot.config.Config
import java.io.File

/**
 * `sot init` — write a commented `.env` so a first-time user can configure ports,
 * VESPA_URL, DEFAULT_OBSERVER, etc. in one place. The CLI and the sot server both
 * read this file (a real environment variable still overrides any value in it).
 */
internal fun init(args: List<String>) {
    val path = flag(args, "--path", System.getenv("SOT_ENV") ?: ".env")
    val f = File(path)
    if (f.exists() && !has(args, "--force")) {
        println("$path already exists - edit it, or re-run with --force to overwrite.")
        return
    }
    // A fresh identity for this relay: NIP-11 `self` + NIP-42 auth to upstream relays.
    val identity = KeyPair()
    f.writeText(Config.sampleDotenv().replace("\nSERVER_NSEC=\n", "\nSERVER_NSEC=${identity.privKey!!.toNsec()}\n"))
    println("wrote $path - edit it to configure sot (VESPA_URL, SERVER_PORT, DEFAULT_OBSERVER, ...).")
    println("generated this relay's identity: ${identity.pubKey.toNpub()}")
}
