# common/src/mvx/common/logger/trace_context.py
from __future__ import annotations

from contextvars import ContextVar, Token

# ---------- Constants ---------
NO_TRACE = "no-trace"

# ---------- Context variables ----------
# Global context variable for the current trace identifier.
# "no-trace" is used when no trace_id has been set explicitly.
_current_trace_id: ContextVar[str] = ContextVar(
    "trace_id",
    default=NO_TRACE,
)

# ---------- Functions ----------


def set_trace_id(value: str | None = None) -> Token[str]:
    """
    Set trace_id for the current execution context.

    If value is None, empty or consists only of whitespace characters,
    the effective trace_id will be "no-trace".

    Returns a Token that should be passed to reset_trace_id() to restore
    the previous value.

    Typical usage (HTTP request or worker task):

        token = set_trace_id(request_id_or_op_id)
        try:
            ...  # handle request / process operation
        finally:
            reset_trace_id(token)
    """
    # Normalize value: None or blank -> "no-trace"
    if value is None:
        cleaned = NO_TRACE
    else:
        cleaned = value.strip()
        if not cleaned:
            cleaned = NO_TRACE

    return _current_trace_id.set(cleaned)


def reset_trace_id(token: Token[str]) -> None:
    """
    Restore the previous trace_id value using the token returned
    by set_trace_id().

    Must be called in a finally-block to avoid leaking the trace_id
    into other requests/tasks.
    """
    # If reset is misused (wrong token), let it fail loudly:
    _current_trace_id.reset(token)


def get_trace_id() -> str:
    """
    Get the current trace_id for this execution context.

    Returns "no-trace" when no trace_id has been set explicitly.
    This function is intended to be used by logging filters/helpers.
    """
    return _current_trace_id.get()


__all__ = [
    "NO_TRACE",
    "set_trace_id",
    "reset_trace_id",
    "get_trace_id",
]
