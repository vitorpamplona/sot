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

import com.vitorpamplona.sot.config.Config

/*
 * Shared argument parsing for the sot CLI: value flags (`--name val`), boolean
 * flags (`--name`), positionals, and the default web-of-trust observer.
 */

/**
 * Default observer (hex pubkey) when `--observer` is omitted, from `DEFAULT_OBSERVER`
 * (env or `.env`). Unset means no default: text results still come back, but every
 * trust score is 0 (no trust perspective applied).
 */
internal val DEFAULT_OBSERVER: String get() = Config.defaultObserver

/** Value of a `--name <value>` flag, or [default] if absent. */
internal fun flag(args: List<String>, name: String, default: String): String {
    val i = args.indexOf(name)
    return if (i >= 0 && i + 1 < args.size) args[i + 1] else default
}

/** Whether a boolean `--name` flag is present. */
internal fun has(args: List<String>, name: String) = args.contains(name)

/** Non-flag args, skipping each valued flag's value so it's never mistaken for a positional. */
internal fun positionalArgs(args: List<String>, valuedFlags: Set<String>): List<String> {
    val out = ArrayList<String>()
    var i = 0
    while (i < args.size) {
        val a = args[i]
        when {
            // a valued flag: skip the flag AND its value
            a in valuedFlags -> i++

            // a bare/boolean flag: skip just the flag
            a.startsWith("--") -> {}

            else -> out.add(a)
        }
        i++
    }
    return out
}
