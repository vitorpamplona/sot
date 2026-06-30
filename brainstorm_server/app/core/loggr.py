"""LOCAL SHIM — not part of upstream brainstorm_server.

Upstream ``loggr`` is the server's structured-logging helper. The vendored
``vespa.py`` only calls ``loggr.get_logger(__name__)``, so a thin wrapper over
the stdlib ``logging`` module is enough to run it standalone. Keep this file
out of any upstream diff.
"""
import logging


class loggr:
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)
