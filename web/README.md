# web

A tiny, dependency-free search UI for **sot** — one `index.html`, no build step.
It calls `GET /search` and shows a single trust-ranked result list: a type-ahead
popup as you type, full results on Enter, and a "Search as me" toggle that ranks
by *your* web of trust via a NIP-07 extension (off = the server's
`DEFAULT_OBSERVER`).

## Run it

The `server` module serves this file itself (bundled as a resource), same-origin
with the API — so just run the server and open it:

```bash
./gradlew :server:run
# open http://localhost:7777
```

No CORS needed (same origin). To develop the page against a **remote** server,
serve it however you like and point it with a query param:

```
http://localhost:8090/?api=https://sot.example.com
```

`TRUST` is the observer's web-of-trust score for the profile
(`quality_scores{observer}`); `RELEVANCE` is the text match score.
