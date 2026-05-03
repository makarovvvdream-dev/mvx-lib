# common/src/mvx/common/logger/core/log_payload_helpers.py
from __future__ import annotations

from typing import Any
import enum

from .protocols import LogPayloadProvider, LogAdapterResolver

__all__ = (
    "normalize_primitive",
    "normalize_list_for_log",
    "normalize_dict_for_log",
    "normalize_value_for_log",
)


# ---------- Apply custom normalization


def _apply_custom_normalization(
    value: Any,
    *,
    log_adapter_resolver: LogAdapterResolver | None,
    verbosity_level: str | None,
) -> dict[str, Any] | None:
    """
    Try to obtain a custom logging payload for the given value.

    Resolution order
    ----------------
    1) If value implements LogPayloadProvider, its to_log_payload() is used.
    2) Otherwise, if a callable log_adapter_resolver is provided, tryes to get
       a type-based log adapter for the value. If succeeded - the adapter is called
       to get the payload.

    In both cases:
      - Any exception raised by the provider/adapter is swallowed.
      - On success, the returned dict[str, Any] is used as-is, without any
        further normalization or item-count limits.

    Returns
    -------
    dict[str, Any] | None
      - dict payload if a provider/adapter returned a value successfully;
      - None if no provider/adapter is available or if an error occurred.
    """

    # Explicit payload provider wins.
    if isinstance(value, LogPayloadProvider):
        # noinspection PyBroadException
        try:
            return value.to_log_payload()
        except Exception:
            # Fallback to generic normalization below.
            pass

    # Type-based adapter resolver (for pure domain objects, DTOs, etc.).
    if log_adapter_resolver is None:
        return None

    # noinspection PyBroadException
    try:
        adapter = log_adapter_resolver(value)

        payload = None
        if adapter is not None and verbosity_level:
            payload = adapter(value, verbosity_level)

        return payload

    except Exception:
        return None


# ---------- Primitive / leaf normalization ----------


def normalize_primitive(
    value: Any,
    *,
    max_str_len: int | None,
) -> str | int | float | bool | bytes | None:
    """
    Normalize primitive values for logging.

    - str:
        Returned unchanged unless it exceeds DEFAULT_MAX_STR_LEN, in which
        case it is truncated and suffixed with "...".
    - int/float/bool/None:
        Returned as-is, without converting to str.
    - bytes/bytearray/memoryview:
        Returned as bytes(value) (no truncation).
    - any other type:
        Represented as a string "<TypeName>".
    """
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
    value: Any,
    *,
    log_adapter_resolver: LogAdapterResolver | None,
    verbosity_level: str | None,
    max_items: int | None,
    max_str_len: int | None,
) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
    """
    Normalize a single non-container value for logging.

    Semantics
    ---------
    - LogPayloadProvider:
        * If the object implements to_log_payload(), the returned dict is used
          as-is, without any further normalization or item-count limits.
    - Registered adapter (type-based):
        * If a log adapter is registered for value's type and active profile,
          it is called and its dict payload is used as-is.
    - Enum:
        * Delegated to normalize_value_for_log(value.value, max_items=max_items),
          so Enum instances are always normalized via their .value recursively.
    - Primitive (str/bytes/bytearray/memoryviewint/float/bool/None):
        * Normalized via normalize_primitive.
    - Any other type:
        * Represented as "<TypeName>" using the concrete runtime type.
    """
    # 1) Custom normalization if available
    custom_payload = _apply_custom_normalization(
        value,
        log_adapter_resolver=log_adapter_resolver,
        verbosity_level=verbosity_level,
    )

    if custom_payload is not None:
        return custom_payload

    # 2) Enum via .value recursion.
    if isinstance(value, enum.Enum):
        return normalize_value_for_log(
            value.value,
            log_adapter_resolver=log_adapter_resolver,
            verbosity_level=verbosity_level,
            max_items=max_items,
            max_str_len=max_str_len,
        )

    # 3) Plain primitives.
    if isinstance(value, (str, bytes, bytearray, memoryview, int, float, bool)) or value is None:
        return normalize_primitive(value, max_str_len=max_str_len)

    # 4) Fallback to "<TypeName>".
    return f"<{type(value).__name__}>"


# ---------- Container normalization ----------


def normalize_list_for_log(
    value: Any,
    *,
    log_adapter_resolver: LogAdapterResolver | None,
    verbosity_level: str | None,
    max_items: int | None,
    max_str_len: int | None,
) -> str | list[Any]:
    """
    Normalize list/tuple for logging, one level deep.

    - If value is not a list or tuple, returns a string "<TypeName>".
    - Only the first `max_items` elements are included (if max_items is not None).
    - Each element is normalized via _normalize_leaf, which may apply
      custom payloads, adapters, or primitive normalization.
    - If there are more elements and max_items is not None, the last item is a
      summary string like "... (N more)".
    """
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
            _normalize_leaf(
                item,
                log_adapter_resolver=log_adapter_resolver,
                verbosity_level=verbosity_level,
                max_items=max_items,
                max_str_len=max_str_len,
            )
        )

    if max_items is not None and len(seq) > max_items:
        result.append(f"... ({len(seq) - max_items} more)")

    return result


def normalize_dict_for_log(
    value: Any,
    *,
    log_adapter_resolver: LogAdapterResolver | None,
    verbosity_level: str | None,
    max_items: int | None,
    max_str_len: int | None,
) -> str | dict[str, Any]:
    """
    Normalize dict for logging, one level deep.

    - If value is not a dict, returns a string "<TypeName>".
    - Only the first `max_items` items are included (if max_items is not None).
    - Keys are converted to strings; if a key string exceeds DEFAULT_MAX_STR_LEN,
      it is truncated and suffixed with "...".
    - Values are normalized via _normalize_leaf, which may apply custom payloads,
      adapters, or primitive normalization.
    - If there are more keys and max_items is not None, a special entry
      "__more__" is added with a summary like "N more keys".
    """
    if not isinstance(value, dict):
        return f"<{type(value).__name__}>"

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

        result[k_str] = _normalize_leaf(
            v,
            verbosity_level=verbosity_level,
            log_adapter_resolver=log_adapter_resolver,
            max_items=max_items,
            max_str_len=max_str_len,
        )

    if max_items is not None and len(items) > max_items:
        result["__more__"] = f"{len(items) - max_items} more keys"

    return result


def normalize_value_for_log(
    value: Any,
    *,
    log_adapter_resolver: LogAdapterResolver | None,
    verbosity_level: str | None,
    max_items: int | None,
    max_str_len: int | None,
) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
    """
    Top-level normalization for values used in logging payloads (kwargs and result).

    Resolution order
    ----------------
    1) Custom normalization:
       - If value implements LogPayloadProvider or has a registered type-based
         log adapter for the active profile, the resulting dict[str, Any] is
         used as-is, without any further normalization or item-count limits.

    2) Container normalization:
       - list/tuple:
           normalized via normalize_list_for_log (one level deep).
       - dict:
           normalized via normalize_dict_for_log (one level deep).

    3) Leaf normalization:
       - Any other value (including Enum and primitives) is normalized via
         _normalize_leaf, which in turn:
           * delegates Enum to normalize_value_for_log(value.value, ...);
           * uses normalize_primitive for str/bytes/bytearray/memoryview/int/float/bool/None;
           * falls back to "<TypeName>" for other types.

    max_items
    ---------
    - If None:
        list/dict are fully included at the top level (no item-count limit).
    - If an int:
        only the first `max_items` items of list/dict are included; a summary
        entry is appended/added if there are more items.
    """

    # 1) Custom normalization if available
    custom_payload = _apply_custom_normalization(
        value,
        log_adapter_resolver=log_adapter_resolver,
        verbosity_level=verbosity_level,
    )

    if custom_payload is not None:
        return custom_payload

    # 2) Default normalization
    if isinstance(value, (list, tuple)):
        return normalize_list_for_log(
            value,
            log_adapter_resolver=log_adapter_resolver,
            verbosity_level=verbosity_level,
            max_items=max_items,
            max_str_len=max_str_len,
        )
    if isinstance(value, dict):
        return normalize_dict_for_log(
            value,
            log_adapter_resolver=log_adapter_resolver,
            verbosity_level=verbosity_level,
            max_items=max_items,
            max_str_len=max_str_len,
        )

    return _normalize_leaf(
        value,
        log_adapter_resolver=log_adapter_resolver,
        verbosity_level=verbosity_level,
        max_items=max_items,
        max_str_len=max_str_len,
    )
