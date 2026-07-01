# web

A tiny, dependency-free search UI for **sot** — one `index.html`, no build step.
It calls the `http-api` (`GET /search`) and shows a single trust-ranked result
list: a type-ahead popup as you type, full results on Enter, and a "Search as
me" toggle that ranks by *your* web of trust via a NIP-07 extension (off = the
server's `DEFAULT_OBSERVER`).

## Run it

Start the API (`./gradlew :http-api:run`, defaults to `:8081`), then open the
page. Any static server works — the API sends permissive CORS, so a `file://`
open works too:

```bash
# from the repo root
cd web && python3 -m http.server 8090
# open http://localhost:8090
```

Point it at a non-default API with a query param:

```
http://localhost:8090/?api=http://localhost:8081
```

That's the only knob. `TRUST` is the observer's web-of-trust score for the
profile (`quality_scores{observer}`); `SCORE` is the text relevance.
