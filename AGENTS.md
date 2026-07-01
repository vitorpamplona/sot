# AGENTS.md

This project's guidance for AI coding agents lives in **[CLAUDE.md](./CLAUDE.md)** —
architecture, module map, build/test commands, and conventions. Read it first.

Quick reminders:

- Format before committing: `./gradlew spotlessApply` (the build fails on unformatted
  code). Every `.kt` file needs the MIT license header, which spotless adds.
- Gate: `./gradlew build` runs compile + tests + `spotlessCheck`.
- `:vespa-engine` is Nostr-agnostic — never import a Quartz/Nostr type into it.
