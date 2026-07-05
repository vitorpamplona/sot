# Design note: a `location` search column

Status: proposal. Companion to the search-field role work in `SearchExtractors`
and `vespa/app/schemas/event.sd`.

## Problem

Several kinds carry structured geographic data that is currently **dropped** —
it lands in no search field, so "furniture in berlin" or "meetups near me"
cannot work. Unlike name/identity/affiliation, geo has **no existing role
column** to reuse: it is a genuinely new role, the geo analog of what kind 0's
dedicated identity/affiliation columns did for profiles.

Kinds with location data and the accessors that expose it:

| Kind | Accessor(s) | Shape |
|------|-------------|-------|
| `ClassifiedsEvent` (30402) | `location()` | free-text place ("Berlin, DE") |
| `CalendarDateSlotEvent` / `CalendarTimeSlotEvent` (31922/31923) | location tag + `g` geohash | text + geohash |
| `CalendarEvent` (31924) | location | text |
| `MeetingSpaceEvent` / `MeetingRoomEvent` (30312/30313) | `room()` / location | text |
| `LiveActivitiesEvent` (30311) | participants/venue | text |

Two distinct signals hide here: a **free-text place name** and a **geohash**.
They want different matching, so the column design has to decide which it
serves.

## Options

### A. Text place-name column (small, ships like the website work)

Add one field `search_location` (bm25, default stemming) and a `location`
extractor role. `ClassifiedsEvent.location()` and the calendar/meeting text
locations feed it. Ranking: a modest `w_location * bm25(search_location)` term
in `relevance()`/`tier_text()`, mirroring the secondary tier. A query word that
matches a place name recalls and lightly boosts the doc.

- Pros: one field, one extractor branch per kind, rolls out via
  `reindexFullTextSearch`. Same blast radius as the `website` routing.
- Cons: pure text — "berlin" matches the string "Berlin", but "near this
  point" / radius search does **not** work. Geohash is ignored.

### B. Geohash column with prefix recall (the real "near me")

Store the geohash as an `attribute` and match by **prefix** (a shorter geohash
prefix = a coarser bounding box, so prefix-matching approximates a radius).
Recall clause: `search_geohash matches "^u33d"` (or a set of neighbor prefixes
computed client/relay-side to cover the query point's cell edges). Ranking can
tier by prefix length (longer shared prefix = closer).

- Pros: actual proximity search, the thing users mean by "near me".
- Cons: geohash edge effects (two nearby points can share no prefix across a
  cell boundary) need the standard 8-neighbor expansion; a new attribute and a
  new YQL clause shape in `EventYql`/`BrainstormWordGroup` + `MockYql`. Bigger
  than A.

### C. Both (A for discovery, B for proximity)

`search_location` (text) recalls by place name; `search_geohash` (attribute)
enables proximity. A geo query sets both. This is the complete answer and the
likely end state, but it is the largest change.

## Recommendation

Ship **A first** — it is the same shape as the `website` routing (one field,
per-kind extractor branches, reindex to roll out) and immediately makes
place-name search work for classifieds and calendar events, the highest-traffic
geo kinds. Treat **B** as a follow-up once there is a client that actually sends
a geohash query, because it drags in a new query-clause shape (and therefore
`MockYql` parser work) that A does not.

## Open questions

- **NIP-50 surface.** Proximity needs the query to carry a point/geohash. Do we
  extend the search grammar (a `geo:` token, like `sort:`/`filter:rank:`), or
  keep geo as a plain tag filter (`#g`) and only rank by the text column? The
  `#g` filter already recalls by exact geohash cell today via `tag_index`; a
  `location` column is about *ranking* and *fuzzy place-name* recall, not exact
  cell filtering.
- **Neighbor expansion owner.** If we do B, who computes the 8 neighbor
  prefixes — the client, the relay's filter→query mapping, or `EventYql`?
- **Cross-kind disjointness.** `search_location` would be populated across many
  kinds (as `website` now is); the `max()`/sum composition tolerates it, same
  as the other role columns.
