# common/src/mvx/common/logger/core/log_errors_helpers.py
from __future__ import annotations

from typing import Any

# ---------- Constants ----------
ERR_LOGGED_FLAG = "_mvx_error_logged"


# ---------- Functions ----------
def build_error_payload(err: BaseException) -> dict[str, Any]:
    """
    Normalize an exception into a serializable payload.

    Rules:
      - If the exception provides a callable `to_log_extra()` method, use it.
      - Otherwise:
          * try to surface `code` and `code_desc` attributes if present;
          * always include `kind` and `message`.

    This keeps RedisAdapterError and similar exceptions compatible without
    importing them directly.
    """
    # Duck-typing: Error should expose to_log_extra()
    to_extra = getattr(err, "to_log_payload", None)

    if callable(to_extra):
        # noinspection PyBroadException
        try:
            extra = to_extra()
            if isinstance(extra, dict):
                return dict(extra)
        except Exception:
            # Fallback to generic representation below.
            pass

    payload: dict[str, Any] = {}

    code = getattr(err, "code", None)
    if code is not None:
        payload["code"] = code

    code_desc = getattr(err, "code_desc", None)
    if code_desc is not None:
        payload["code_desc"] = code_desc

    payload.setdefault("kind", type(err).__name__)
    payload.setdefault("message", str(err))

    return payload


def is_error_logged(err: BaseException) -> bool:
    """
    Check whether the given exception instance has already been logged
    with a detailed error payload.
    """
    # noinspection PyBroadException
    try:
        return bool(getattr(err, ERR_LOGGED_FLAG, False))
    except Exception:
        return False


def mark_error_logged(err: BaseException) -> None:
    """
    Best-effort marking of an exception instance as already logged.

    If the instance does not allow arbitrary attributes (e.g. __slots__ without
    __dict__), the error is silently ignored and the flag is not set.
    """
    # noinspection PyBroadException
    try:
        setattr(err, ERR_LOGGED_FLAG, True)
    except Exception:
        # Best effort: ignore if we cannot set the flag
        pass


__all__ = ["ERR_LOGGED_FLAG", "build_error_payload", "is_error_logged", "mark_error_logged"]
