# common/src/mvx/common/errors/structured_error.py
from __future__ import annotations

from typing import Any, Mapping, Optional, Self

__all__ = ("StructuredError",)


class StructuredError(Exception):
    """
    Base exception class for errors with structured diagnostic context.

    Args:
        message: Human-readable error message.
        details: Optional log-friendly diagnostic context.
        cause: Optional underlying exception.
    """

    def __init__(
        self,
        *,
        message: str,
        details: Optional[Mapping[str, Any]] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        self.message: str = message
        self.details: dict[str, Any] = dict(details or {})

        self.cause: Optional[Exception] = cause

        super().__init__(message)

    def __str__(self) -> str:
        base = f"{self.__class__.__name__}: {self.message}"
        if self.details:
            return f"{base} | details={self.details!r}"
        return base

    def with_detail(self, key: str, value: Any) -> Self:
        """
        Add or replace one detail entry.

        Args:
            key: Detail key.
            value: Detail value.

        Returns:
            This error instance.
        """
        self.details[key] = value
        return self

    def with_details(self, extra: Mapping[str, Any]) -> Self:
        """
        Merge multiple detail entries.

        Args:
            extra: Detail entries to merge.

        Returns:
            This error instance.
        """
        self.details.update(extra)
        return self

    def to_log_payload(self) -> dict[str, Any]:
        """
        Return a stable dictionary representation for structured logging.

        Returns:
            Log-friendly error payload.
        """
        payload: dict[str, Any] = {
            "kind": self.__class__.__name__,
            "message": self.message,
            "details": dict(self.details),
        }

        if self.cause is not None:
            payload["cause"] = {
                "kind": self.cause.__class__.__name__,
                "message": str(self.cause),
            }

        return payload
