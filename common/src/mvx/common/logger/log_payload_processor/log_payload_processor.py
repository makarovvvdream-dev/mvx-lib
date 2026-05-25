# src/mvx/common/logger/log_payload_processor/log_payload_processor.py
from __future__ import annotations

from typing import Any
from collections.abc import Mapping
from enum import Enum

import threading

from ..models import LogPayloadProvider

from .types import LogVerbosityLevel, LogAdapterResolver, DEFAULT_MAX_STR_LEN, DEFAULT_MAX_ITEMS

__all__ = ("LogPayloadProcessor",)


class LogPayloadProcessor:
    __slots__ = (
        "_lock",
        "_verbosity_level",
        "_max_str_len",
        "_max_items",
        "_log_adapter_resolver",
    )

    def __init__(
        self,
        *,
        verbosity_level: LogVerbosityLevel | None = None,
        max_str_len: int | None = None,
        max_items: int | None = None,
        log_adapter_resolver: LogAdapterResolver | None = None,
    ) -> None:

        if verbosity_level is not None:
            if not isinstance(verbosity_level, LogVerbosityLevel):
                raise TypeError(
                    "argument 'verbosity_level' must be an instance of 'LogVerbosityLevel'"
                )

        if max_str_len is not None:
            if not isinstance(max_str_len, int):
                raise TypeError("argument 'max_str_len' must be integer when provided")

            if max_str_len < 1:
                raise ValueError("argument 'max_str_len' must be greater than 0")

        if max_items is not None:
            if not isinstance(max_items, int):
                raise TypeError("argument 'max_items' must be integer when provided")

            if max_items < 1:
                raise ValueError("argument 'max_items' must be greater than 0")

        if log_adapter_resolver is not None and not callable(log_adapter_resolver):
            raise TypeError("argument 'log_adapter_resolver' must be a callable")

        self._lock = threading.RLock()

        self._max_str_len = max_str_len
        self._max_items = max_items
        self._verbosity_level = verbosity_level
        self._log_adapter_resolver = log_adapter_resolver

    # ---- verbosity_level ---------------------------------------------------------------------

    @property
    def verbosity_level(self) -> LogVerbosityLevel:
        with self._lock:
            if self._verbosity_level is not None:
                return self._verbosity_level

            return LogVerbosityLevel.NORMAL

    def set_verbosity_level(self, verbosity_level: LogVerbosityLevel) -> None:
        if not isinstance(verbosity_level, LogVerbosityLevel):
            raise TypeError("argument 'verbosity_level' must be an instance of 'LogVerbosityLevel'")

        with self._lock:
            self._verbosity_level = verbosity_level

    def reset_verbosity_level(self) -> None:
        with self._lock:
            self._verbosity_level = None

    # ---- max_str_len ---------------------------------------------------------------------

    @property
    def max_str_len(self) -> int:
        with self._lock:
            if self._max_str_len is not None:
                return self._max_str_len

            return DEFAULT_MAX_STR_LEN

    def set_max_str_len(self, max_str_len: int) -> None:
        if not isinstance(max_str_len, int):
            raise TypeError("argument 'max_str_len' must be integer")

        if max_str_len < 1:
            raise ValueError("argument 'max_str_len' must be greater than 0")

        with self._lock:
            self._max_str_len = max_str_len

    def reset_max_str_len(self) -> None:
        with self._lock:
            self._max_str_len = None

    # ---- max_items -----------------------------------------------------------------------

    @property
    def max_items(self) -> int:
        with self._lock:
            if self._max_items is not None:
                return self._max_items

            return DEFAULT_MAX_ITEMS

    def set_max_items(self, max_items: int) -> None:
        if not isinstance(max_items, int):
            raise TypeError("argument 'max_items' must be integer")

        if max_items < 1:
            raise ValueError("argument 'max_items' must be greater than 0")

        with self._lock:
            self._max_items = max_items

    def reset_max_items(self) -> None:
        with self._lock:
            self._max_items = None

    # ---- log_adapter_resolver ------------------------------------------------------------

    @property
    def log_adapter_resolver(self) -> LogAdapterResolver | None:
        with self._lock:
            if self._log_adapter_resolver is not None:
                return self._log_adapter_resolver

            return None

    def set_log_adapter_resolver(self, log_adapter_resolver: LogAdapterResolver) -> None:
        if not callable(log_adapter_resolver):
            raise TypeError("argument 'log_adapter_resolver' must be a callable")

        with self._lock:
            self._log_adapter_resolver = log_adapter_resolver

    def reset_log_adapter_resolver(self) -> None:
        with self._lock:
            self._log_adapter_resolver = None

    # ---- Public API ----------------------------------------------------------------------

    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]:

        effective_max_items = self.max_items if not unbounded else None
        effective_max_str_len = self.max_str_len

        return self._normalize_dict_for_log(
            payload, max_items=effective_max_items, max_str_len=effective_max_str_len
        )

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:

        effective_max_items = self.max_items if not unbounded else None
        effective_max_str_len = self.max_str_len

        return self._normalize_value_for_log_core(
            value, max_items=effective_max_items, max_str_len=effective_max_str_len
        )

    def get_plain_verbosity_level(self) -> str | None:
        return self.verbosity_level.value.upper()

    # ---- Internal functions --------------------------------------------------------------

    def _apply_custom_normalization(self, value: Any) -> dict[str, Any] | None:

        # Explicit payload provider wins.
        if isinstance(value, LogPayloadProvider):
            # noinspection PyBroadException
            try:
                provided_payload = value.to_log_payload()
                if isinstance(provided_payload, dict):
                    return provided_payload
            except Exception:
                # Fallback to generic normalization below.
                pass

        # Type-based adapter resolver (for pure domain objects, DTOs, etc.).
        resolver = self.log_adapter_resolver
        if resolver is None:
            return None

        # noinspection PyBroadException
        try:
            adapter = resolver(value)
            if adapter is None:
                return None

            provided_payload = adapter(value, self.verbosity_level)
            if isinstance(provided_payload, dict):
                return provided_payload

            return None

        except Exception:
            return None

    @staticmethod
    def _normalize_primitive(
        value: Any, *, max_str_len: int | None
    ) -> str | int | float | bool | bytes | None:

        if isinstance(value, str):
            if max_str_len is not None and len(value) > max_str_len:
                return value[:max_str_len] + "..."
            return value

        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)

        if isinstance(value, (int, float, bool)) or value is None:
            return value

        return f"<{type(value).__name__}>"

    def _normalize_leaf(
        self,
        value: Any,
        *,
        max_items: int | None,
        max_str_len: int | None,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:

        # 1) Custom normalization if available
        custom_payload = self._apply_custom_normalization(value)

        if custom_payload is not None:
            return custom_payload

        # 2) Enum via .value recursion.
        if isinstance(value, Enum):
            return self._normalize_value_for_log_core(
                value.value,
                max_items=max_items,
                max_str_len=max_str_len,
            )

        # 3) Plain primitives.
        if (
            isinstance(value, (str, bytes, bytearray, memoryview, int, float, bool))
            or value is None
        ):
            return self._normalize_primitive(value, max_str_len=max_str_len)

        # 4) Fallback to "<TypeName>".
        return f"<{type(value).__name__}>"

    def _normalize_list_for_log(
        self,
        value: list | tuple,
        *,
        max_items: int | None,
        max_str_len: int | None,
    ) -> str | list[Any]:

        if not isinstance(value, (list, tuple)):
            return f"<{type(value).__name__}>"

        seq = list(value)
        result: list[Any] = []

        if max_items is None:
            limit = len(seq)
        else:
            limit = min(len(seq), max_items)

        for i in range(limit):
            item = seq[i]
            result.append(
                self._normalize_leaf(
                    item,
                    max_items=max_items,
                    max_str_len=max_str_len,
                )
            )

        if max_items is not None and len(seq) > max_items:
            result.append(f"... ({len(seq) - max_items} more)")

        return result

    def _normalize_dict_for_log(
        self,
        value: Mapping[str, Any],
        *,
        max_items: int | None,
        max_str_len: int | None,
    ) -> dict[str, Any]:

        if not isinstance(value, Mapping):
            return {}

        result: dict[str, Any] = {}
        items = list(value.items())

        if max_items is None:
            limit = len(items)
        else:
            limit = min(len(items), max_items)

        for i in range(limit):
            key, v = items[i]
            k_str = str(key)
            if max_str_len is not None and len(k_str) > max_str_len:
                k_str = k_str[:max_str_len] + "..."

            result[k_str] = self._normalize_leaf(
                v,
                max_items=max_items,
                max_str_len=max_str_len,
            )

        if max_items is not None and len(items) > max_items:
            result["__more__"] = f"{len(items) - max_items} more keys"

        return result

    def _normalize_value_for_log_core(
        self,
        value: Any,
        *,
        max_items: int | None,
        max_str_len: int | None,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:

        # 1) Custom normalization if available
        custom_payload = self._apply_custom_normalization(value)

        if custom_payload is not None:
            return custom_payload

        # 2) Default normalization
        if isinstance(value, (list, tuple)):
            return self._normalize_list_for_log(
                value,
                max_items=max_items,
                max_str_len=max_str_len,
            )
        if isinstance(value, Mapping):
            return self._normalize_dict_for_log(
                value,
                max_items=max_items,
                max_str_len=max_str_len,
            )

        return self._normalize_leaf(
            value,
            max_items=max_items,
            max_str_len=max_str_len,
        )
