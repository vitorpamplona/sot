package com.vitorpamplona.sot.cli

import com.vitorpamplona.sot.config.Config

/**
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
            a in valuedFlags -> i++ // skip this flag's value
            a.startsWith("--") -> {} // bare/boolean flag
            else -> out.add(a)
        }
        i++
    }
    return out
}
