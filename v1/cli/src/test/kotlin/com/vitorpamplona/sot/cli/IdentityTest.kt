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

import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip19Bech32.toNsec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** SERVER_NSEC parsing: the key `sot init` writes must come back as the same identity. */
class IdentityTest {
    @Test
    fun `an nsec round-trips to the same pubkey sot init generated`() {
        val generated = KeyPair()

        val signer = signerFromSecret(generated.privKey!!.toNsec())

        assertEquals(generated.pubKey.toHexKey(), signer?.pubKey)
    }

    @Test
    fun `a 64-hex secret key is accepted too`() {
        val generated = KeyPair()

        val signer = signerFromSecret(generated.privKey!!.toHexKey())

        assertEquals(generated.pubKey.toHexKey(), signer?.pubKey)
    }

    @Test
    fun `garbage is rejected, not silently turned into a random identity`() {
        assertNull(signerFromSecret("npub1notasecret"))
        assertNull(signerFromSecret("nsec1invalidinvalidinvalid"))
        assertNull(signerFromSecret("deadbeef"))
    }
}
