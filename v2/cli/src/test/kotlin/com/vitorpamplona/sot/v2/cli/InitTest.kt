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
package com.vitorpamplona.sot.v2.cli

import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip19Bech32.toNsec
import com.vitorpamplona.sot.v2.sync.Identity
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The interactive `sot init`, driven by scripted answers through the
 * injectable readLine. The written `.env` is read back with the same parser
 * the runtime uses ([Config.loadDotenv]).
 */
class InitTest {
    private fun tempEnv(): File = File.createTempFile("sot-init", ".env").also { it.delete() }

    private fun runInit(
        file: File,
        vararg answers: String,
        flags: List<String> = emptyList(),
    ): Map<String, String> {
        val queue = ArrayDeque(answers.toList())
        init(listOf("--path", file.path) + flags, readLine = { queue.removeFirstOrNull() })
        return Config.loadDotenv(file.path)
    }

    @Test
    fun `a scripted session records answers, normalizes keys, and cascades the port into url defaults`() {
        val admin = KeyPair()
        val identity = KeyPair()
        val house = KeyPair()
        val env =
            runInit(
                tempEnv(),
                "My Search", // service name
                "", // description -> default
                "", // icon -> none
                admin.pubKey.toHexKey(), // admin contact as hex
                identity.privKey!!.toNsec(), // pasted identity
                house.pubKey.toNpub(), // house account as npub
                "", // house relay -> default
                "9999", // port
                "", // relay url -> follows the port
                "", // http url -> follows the port
                "", // vespa
                "", // seeds
                "30", // sync interval
            )
        assertEquals("My Search", env["SERVER_NAME"])
        assertEquals("Search over Trust - a web-of-trust Nostr search relay", env["SERVER_DESCRIPTION"])
        assertEquals(admin.pubKey.toHexKey(), env["SERVER_PUBKEY"])
        assertEquals(identity.privKey!!.toNsec(), env["SERVER_NSEC"], "a pasted nsec is kept verbatim")
        assertEquals(house.pubKey.toHexKey(), env["HOUSE_NPUB"], "npub input normalizes to hex")
        assertEquals("wss://relay.damus.io", env["HOUSE_RELAY"])
        assertEquals("9999", env["SERVER_PORT"])
        assertEquals("ws://localhost:9999", env["RELAY_URL"], "the chosen port cascades into the url defaults")
        assertEquals("http://localhost:9999", env["SERVER_URL"])
        assertEquals("30", env["SYNC_INTERVAL"])
    }

    @Test
    fun `invalid answers are re-asked and EOF falls back to defaults`() {
        val env =
            runInit(
                tempEnv(),
                "", // name
                "", // description
                "", // icon
                "", // admin contact
                "definitely-not-an-nsec", // identity: rejected, re-asked...
                "", // ...Enter generates a fresh key
                "not a pubkey!!", // house: rejected, re-asked...
                // queue runs dry here: EOF -> every remaining question takes its default
            )
        val nsec = env.getValue("SERVER_NSEC")
        assertTrue(nsec.startsWith("nsec1"), "a fresh identity was generated")
        assertNotNull(Identity.signerFromSecret(nsec), "...and it parses back")
        assertEquals("", env["HOUSE_NPUB"], "the invalid house answer never lands")
        assertEquals("", env["HOUSE_RELAY"], "no house account, no home relay question")
        assertEquals("7777", env["SERVER_PORT"])
        assertEquals("15", env["SYNC_INTERVAL"])
    }

    @Test
    fun `--yes takes every default without touching the terminal`() {
        val env =
            runInit(
                tempEnv(),
                flags = listOf("--yes"),
                answers = arrayOf(),
            ).also { }
        assertEquals("sot", env["SERVER_NAME"])
        assertEquals("", env["HOUSE_NPUB"])
        assertTrue(env.getValue("SERVER_NSEC").startsWith("nsec1"), "--yes still generates the identity")
    }

    @Test
    fun `--yes never reads stdin`() {
        val file = tempEnv()
        init(listOf("--path", file.path, "--yes"), readLine = { fail("--yes must not prompt") })
        assertTrue(file.exists())
    }

    @Test
    fun `an existing file is preserved unless --force`() {
        val file = tempEnv().apply { writeText("SERVER_NAME=keep-me\n") }
        init(listOf("--path", file.path), readLine = { null })
        assertEquals("keep-me", Config.loadDotenv(file.path)["SERVER_NAME"], "no --force: untouched")

        init(listOf("--path", file.path, "--force", "--yes"), readLine = { null })
        assertEquals("sot", Config.loadDotenv(file.path)["SERVER_NAME"], "--force rewrites")
    }
}
