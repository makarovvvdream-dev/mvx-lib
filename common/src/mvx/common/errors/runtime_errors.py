# common/src/mvx/common/errors/runtime_errors.py
from __future__ import annotations

from typing import Optional, Mapping, Any

from .structured_error import StructuredError

__all__ = (
    "RuntimeExtendedError",
    "RuntimeUnexpectedError",
)


class RuntimeExtendedError(StructuredError, RuntimeError):
    """
    RuntimeError variant with structured diagnostic context.

    Extends RuntimeError with the structured error payload provided by
    StructuredError and optional source metadata.

    Args:
        message: Human-readable error message.
        details: Optional log-friendly diagnostic context.
        cause: Optional underlying exception.
        module: Optional module name associated with the error.
        qualname: Optional qualified name associated with the error.
    """

    def __init__(
        self,
        *,
        message: str,
        details: Optional[Mapping[str, Any]] = None,
        cause: Optional[Exception] = None,
        module: Optional[str] = None,
        qualname: Optional[str] = None,
    ) -> None:

        self.module = None if module is None else (module.strip() or None)
        self.qualname = None if qualname is None else (qualname.strip() or None)

        RuntimeError.__init__(self, str(message))

        StructuredError.__init__(
            self,
            message=message,
            details=details,
            cause=cause,
        )

    def to_log_payload(self) -> dict[str, Any]:
        """
        Return a structured logging payload with optional source metadata.

        Returns:
            Log-friendly error payload.
        """
        base = StructuredError.to_log_payload(self)

        payload: dict[str, Any] = {}
        if self.module is not None:
            payload["module"] = self.module
        if self.qualname is not None:
            payload["qualname"] = self.qualname

        payload.update(base)
        return payload


class RuntimeUnexpectedError(Exception):
    """
    Marker base class for runtime errors classified as unexpected.

    This class is intended for multiple inheritance with concrete domain
    errors. It marks an error as unexpected without replacing the domain-specific
    error hierarchy.
    """

    ...
