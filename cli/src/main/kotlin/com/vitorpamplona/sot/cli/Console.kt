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

/*
 * The one place the sot CLI turns raw log text into something worth looking at.
 *
 * Everything the long-running commands emit — sync phase headers, per-relay
 * progress, the live status line, warnings — flows through here on its way to
 * the terminal, so the whole app speaks with one visual voice. The `:indexer`
 * stays presentation-free: it emits plain, greppable strings in a consistent
 * vocabulary (`=== ... ===` headers, `[n/total]` progress, `  ! ...` trouble,
 * `~ ...` the live gauge, `DONE - ...`), and [styleLogLine] recognizes that
 * vocabulary and paints it.
 *
 * Hard rule: colour is applied ONLY on an interactive terminal (and never when
 * NO_COLOR is set). Piped or redirected output is byte-for-byte what it was
 * before this file existed — logs stay greppable and diffs stay clean. Toggle
 * the guess with SOT_COLOR=always|never.
 */

/** ANSI palette + the "should we even colour?" decision, resolved once at startup. */
internal object Ansi {
    private const val ESC = "["
    const val RESET = "${ESC}0m"

    // Honour the de-facto standard NO_COLOR (https://no-color.org) and an explicit
    // SOT_COLOR override; otherwise colour only when stdout is a real terminal
    // (System.console() is null under pipes, redirects, and CI — exactly when we
    // want plain text). Computed once: the tty-ness of stdout can't change mid-run.
    val enabled: Boolean =
        when (System.getenv("SOT_COLOR")?.lowercase()) {
            "always", "1", "true", "yes" -> true
            "never", "0", "false", "no" -> false
            else -> System.getenv("NO_COLOR") == null && System.console() != null
        }

    /** Terminal width for rules/banners, from COLUMNS when the shell exports it. */
    val width: Int = (System.getenv("COLUMNS")?.toIntOrNull() ?: 80).coerceIn(40, 120)

    private fun sgr(
        code: String,
        s: String,
    ) = if (enabled) "$ESC${code}m$s$RESET" else s

    fun dim(s: String) = sgr("2", s)

    fun bold(s: String) = sgr("1", s)

    fun red(s: String) = sgr("91", s)

    fun green(s: String) = sgr("92", s)

    fun amber(s: String) = sgr("33", s)

    fun cyan(s: String) = sgr("96", s)

    fun blue(s: String) = sgr("94", s)

    fun gray(s: String) = sgr("90", s)

    fun boldCyan(s: String) = if (enabled) "${ESC}1;96m$s$RESET" else s

    fun boldGreen(s: String) = if (enabled) "${ESC}1;92m$s$RESET" else s
}

/**
 * Turn one raw log message into its terminal-ready form. A no-op (returns [msg]
 * unchanged) unless [Ansi.enabled] — so redirected logs never gain a stray glyph
 * or escape code. Recognizes the `:indexer`'s emit vocabulary; anything it doesn't
 * recognize is left as plain informational text with a subtle gutter.
 */
internal fun styleLogLine(msg: String): String {
    if (!Ansi.enabled) return msg

    val indent = msg.takeWhile { it == ' ' }
    val body = msg.substring(indent.length)

    // `=== phase 1: ... ===` -> a full-width rule with the title inlined.
    if (body.startsWith("===")) return phaseRule(body.trim('=', ' '))

    // `~ relays 42/127 | recv ... | vespa ok ...` -> the live gauge.
    if (body.startsWith("~")) return statusGauge(body.removePrefix("~").trim())

    // `! purplepag.es sync failed: ...` -> trouble; red if it actually failed.
    if (body.startsWith("!")) {
        val text = body.removePrefix("!").trim()
        val hard = Regex("fail|timed out|MISSING|invalid|dropped|unreliable", RegexOption.IGNORE_CASE).containsMatchIn(text)
        return if (hard) "$indent${Ansi.red("✗")} ${Ansi.red(text)}" else "$indent${Ansi.amber("▲")} ${Ansi.amber(text)}"
    }

    // `DONE - profiles=... scores=...` -> the run's happy ending.
    if (body.startsWith("DONE")) return "$indent${Ansi.boldGreen("✓")} ${Ansi.boldGreen(body.removePrefix("DONE").trim('-', ' '))}"

    // `[42/127] relay.damus.io  10040=... kind0=...` -> per-item progress; the
    // `[n/total]` counter carries the eye, `+N` inserts are the good news.
    Regex("^\\[[^\\]]*\\d+/\\d+\\]").find(body)?.let { m ->
        val rest = body.substring(m.value.length)
        return indent + Ansi.cyan(m.value) + highlightCounts(rest)
    }

    // `[state] ...`, `[sync] ...`, `[discovery] ...`, `[reconcile] ...` -> a dim tag.
    Regex("^\\[[a-z0-9 ]+\\]", RegexOption.IGNORE_CASE).find(body)?.let { m ->
        val rest = body.substring(m.value.length)
        return "$indent${Ansi.gray("·")} ${Ansi.dim(m.value)}${highlightCounts(rest)}"
    }

    // Anything else is a plain note (relay identity, background-sync cadence, ...).
    return "$indent${Ansi.gray("·")} $body"
}

/** `── phase 1: 10040 / 5 / 0 from 127 relay(s) ──────────` filling the terminal. */
private fun phaseRule(title: String): String {
    val label = " ${Ansi.boldCyan(title)} "
    val used = 2 + 1 + title.length + 1 // "──" + spaces + title
    val fill = (Ansi.width - used).coerceAtLeast(3)
    return Ansi.gray("──") + label + Ansi.gray("─".repeat(fill))
}

/** The `~` live line: a spinner gutter, dim pipes, and the numbers that matter lit up. */
private fun statusGauge(text: String): String {
    var t = text
    t = Regex("\\bnew (\\S+)").replace(t) { "new " + Ansi.green(it.groupValues[1]) }
    t = Regex("\\bok (\\S+)").replace(t) { "ok " + Ansi.green(it.groupValues[1]) }
    t = Regex("\\binflight (\\S+)").replace(t) { "inflight " + Ansi.amber(it.groupValues[1]) }
    t = Regex("\\((\\S+/s)\\)").replace(t) { "(" + Ansi.cyan(it.groupValues[1]) + ")" }
    t = Regex("\\(idle ([^)]+)\\)").replace(t) { Ansi.amber("(idle " + it.groupValues[1] + ")") }
    t = t.replace(" | ", " ${Ansi.gray("│")} ")
    return "${Ansi.cyan("≈")} $t"
}

/** Light up the `+N` new-event counts green wherever they appear in a progress line. */
private fun highlightCounts(s: String): String = Regex("\\+\\d[\\d.kM]*").replace(s) { Ansi.green(it.value) }
