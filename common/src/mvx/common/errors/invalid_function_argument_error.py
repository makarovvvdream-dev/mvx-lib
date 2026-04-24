# common/src/mvx/common/errors/invalid_function_argument_error.py

from __future__ import annotations
from typing import Any, Mapping, Optional

from .structured_error import StructuredError

__all__ = ("InvalidFunctionArgumentError",)


class InvalidFunctionArgumentError(StructuredError):
    """
    Error raised when a function argument fails validation.

    Wraps an underlying validation exception and adds structured context about
    the function, argument, offending value, and validation error type.

    Args:
        func: Function name where validation failed.
        arg: Argument name that failed validation.
        value: Offending argument value, when safe and useful to log.
        cause: Underlying validation exception.
        details: Additional log-friendly diagnostic context.
    """

    def __init__(
        self,
        *,
        func: Optional[str],
        arg: Optional[str],
        value: Optional[Any] = None,
        cause: Exception,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        msg = f"invalid argument -> {str(cause)}"

        func = func or "<unknown>"
        arg = arg or "<unknown>"

        base_details: dict[str, Any] = {
            "func": func,
            "arg": arg,
            "error_type": type(cause).__name__,
        }
        if value is not None:
            base_details["value"] = value
        if details:
            base_details.update(details)

        super().__init__(
            message=msg,
            details=base_details,
            cause=cause,
        )
