# common/src/mvx/common/errors/reasoned_error.py
from __future__ import annotations
from typing import Any, Optional, Mapping

from .structured_error import StructuredError

__all__ = ("ReasonedError",)


class ReasonedError(StructuredError):
    """
    Structured error with an optional stable reason code.

    Extends StructuredError by adding `reason_code`, which can be used as a
    machine-readable classifier for logging, metrics, and tests.

    Args:
        message: Human-readable error message.
        details: Optional log-friendly diagnostic context.
        cause: Optional underlying exception.
        reason: Optional stable reason code.
    """

    def __init__(
        self,
        *,
        message: str,
        details: Optional[Mapping[str, Any]] = None,
        cause: Optional[Exception] = None,
        reason: Optional[str] = None,
    ) -> None:

        self.reason_code: Optional[str] = reason

        super().__init__(
            message=message,
            details=details,
            cause=cause,
        )

    def to_log_payload(self) -> dict[str, Any]:
        """
        Extend base log payload with the reason code.

        Returns:
            Log payload including "reason" when present.
        """

        payload: dict[str, Any] = {}

        if self.reason_code is not None:
            payload["reason"] = self.reason_code

        payload.update(super().to_log_payload())

        return payload
