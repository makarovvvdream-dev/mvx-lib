# src/mvx/common/logger/helpers.py
from __future__ import annotations
from typing import Any

import sys
from pprint import pformat

from .models import LogPayloadProvider

__all__ = "log_internal_error"

# ---- Last resort logger ------------------------------------------------------------------


def log_internal_error(message: str, exc: Exception | None) -> None:
    payload: dict[str, Any] | None = None

    if exc is not None:
        payload = (
            exc.to_log_payload()
            if isinstance(exc, LogPayloadProvider)
            else {
                "kind": exc.__class__.__name__,
                "message": str(exc),
            }
        )

    if payload is not None:
        message = f"\n{message} ->\n{pformat(payload, indent=4, sort_dicts=False)}"

    print(
        message,
        file=sys.stderr,
    )
