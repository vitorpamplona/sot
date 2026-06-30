"""LOCAL SHIM — not part of upstream brainstorm_server.

Upstream this is a pydantic ``Settings`` object with the full server config.
Here we only need the two attributes that ``vespa.py`` and ``observer.py``
actually read, sourced from the environment, so the vendored upstream files
(``vespa.py``, ``observer.py``) can run standalone — unchanged — outside the
FastAPI app. Keep this file out of any upstream diff.
"""
import os


class _Settings:
    # Base URL of the local Vespa container's query/document API.
    vespa_url: str = os.environ.get("VESPA_URL", "http://localhost:8080")
    # Default observer perspective for trust scores; None falls back to the
    # hardcoded DEFAULT_OBSERVER_PUBKEY in observer.py.
    periodic_graperank_pubkey: str | None = (
        os.environ.get("PERIODIC_GRAPERANK_PUBKEY") or None
    )


settings = _Settings()
