# common/src/mvx/common/logger/core/invocation_logging.py
from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Optional, ParamSpec, TypeVar, Awaitable, cast
from dataclasses import dataclass
from enum import StrEnum

import logging
import inspect
import asyncio

from .log_errors_helpers import build_error_payload, is_error_logged, mark_error_logged
from .log_events_helpers import log_event
from .adapter_registry import get_active_log_profile
from .payload_helpers import DEFAULT_MAX_ITEMS as _MAX_ITEMS, normalize_value_for_log

# ---------- Types ----------

P = ParamSpec("P")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])


LogErrorPolicyRule = tuple[type[BaseException], bool]
LogErrorPolicy = tuple[LogErrorPolicyRule, ...]


class InvocationEventType(str, StrEnum):
    INVOKE = "invoke"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


SYSTEM_KEYS: frozenset[str] = frozenset({"error", "kwargs", "result", "cancelled", "closures"})


@dataclass(frozen=True, slots=True)
class _ResolvedField:
    alias: str
    value: Any
    unbounded_items: bool


# ---------- Helpers ----------


def _default_logger_for(func: Callable[..., Any], first_arg: Any | None) -> logging.Logger:
    """
    Resolve logger for a wrapped function.

    Priority:
      1) If first_arg has `get_logger()` -> use it.
      2) Else if first_arg has `.logger` attribute -> use it.
      3) Else fallback to module-level logger for the function.
    """
    if first_arg is not None:
        # noinspection PyBroadException
        try:
            get_logger = getattr(first_arg, "get_logger", None)
            if callable(get_logger):
                logger = get_logger()
                if isinstance(logger, logging.Logger):
                    return logger
        except Exception:
            pass

        # noinspection PyBroadException
        try:
            logger_attr = getattr(first_arg, "logger", None)
            if isinstance(logger_attr, logging.Logger):
                return logger_attr
        except Exception:
            pass

    module_name = getattr(func, "__module__", None)
    if not isinstance(module_name, str):
        module_name = __name__
    return logging.getLogger(module_name)


def _select_spec_for_profile(raw_spec: str, active_profile: str | None) -> str | None:
    """
    Resolve a possibly profile-aware field spec for the given active profile.

    Semantics
    ---------
    `raw_spec` may be either:

      - Plain spec (no usable profile prefix), e.g.:
          * "x"
          * "op.op_name"
          * "alias=op.op_name"
          * "payload!"

      - Profile-aware spec:
          "<p1>[,<p2>...]:<spec>"

        where:
          * left side is a comma-separated list of profile names;
          * right side `<spec>` is a non-empty field spec in the same
            format as plain specs (including optional "!" suffix).

    Behavior:

      - If `active_profile` is None:
          * Profile filtering is disabled and `raw_spec` is returned as-is
            (after stripping), unless it is empty (then None is returned).

      - If `raw_spec` is empty after stripping:
          * Return None.

      - If `raw_spec` does not contain ':' or does not form a valid
        "<profiles>:<spec>" pattern (no non-empty profiles or empty spec):
          * Return the stripped `raw_spec` (treated as plain spec).

      - If `raw_spec` is a valid profile-aware spec:
          * Build a set of profile names from the left side.
          * If `active_profile` is in that set:
              - return `<spec>` (right side, stripped), which will then be
                parsed as a plain field spec by the caller.
          * Otherwise:
              - return None (this spec is not applicable for the current
                profile and must be skipped).
    """
    if active_profile is None:
        return raw_spec

    text = raw_spec.strip()
    if not text:
        return None

    if ":" not in text:
        return text

    left, right = text.split(":", 1)

    # Right part must be non-empty to consider this profile-aware.
    spec = right.strip()
    if not spec:
        # Something like "default,full:" -> treat as legacy.
        return text

    # Build set of profile names.
    profiles = {p.strip() for p in left.split(",")}
    profiles.discard("")
    if not profiles:
        # No usable profile names -> treat as legacy.
        return text

    if active_profile in profiles:
        return spec

    # Not for this profile.
    return None


def _iter_resolved_fields(
    field_specs: tuple[str, ...],
    source_kwargs: dict[str, Any],
    *,
    active_profile: str | None = None,
) -> list[_ResolvedField]:

    resolved: list[_ResolvedField] = []

    for spec in field_specs:
        spec = spec.strip()
        if not spec:
            continue

        effective_spec = _select_spec_for_profile(spec, active_profile)
        if effective_spec is None:
            continue

        spec = effective_spec

        unbounded_items = False
        if spec.endswith("!"):
            unbounded_items = True
            spec = spec[:-1].strip()
            if not spec:
                continue

        alias: Optional[str] = None
        if "=" in spec:
            alias_part, path_part = spec.split("=", 1)
            alias_candidate = alias_part.strip()
            path_str = path_part.strip()
            if alias_candidate:
                alias = alias_candidate
        else:
            path_str = spec

        if not path_str:
            continue

        path_parts = [p for p in path_str.split(".") if p]
        if not path_parts:
            continue

        kw_name = path_parts[0]
        attr_chain = path_parts[1:]

        if kw_name not in source_kwargs:
            continue

        value = source_kwargs[kw_name]

        failed = False
        for attr in attr_chain:
            if attr == "len()":
                # noinspection PyBroadException
                try:
                    value = len(value)
                except Exception:
                    failed = True
                    break
                continue
            # noinspection PyBroadException
            try:
                value = getattr(value, attr)
            except Exception:
                failed = True
                break

        if failed:
            continue

        if alias is None:
            alias = path_parts[-1]

        resolved.append(_ResolvedField(alias=alias, value=value, unbounded_items=unbounded_items))

    return resolved


# noinspection DuplicatedCode
def _build_logged_kwargs(
    field_specs: tuple[str, ...],
    source_kwargs: dict[str, Any],
    *,
    active_profile: str | None = None,
) -> dict[str, Any]:
    """
    Build a filtered kwargs snapshot for logging on invoke.

    Parameters
    ----------
    field_specs:
        Tuple of field spec strings. Each item may be:

          Plain spec (profile-agnostic)
          -----------------------------
          - Simple name:
                "x"
                -> value = kwargs["x"] (if present).

          - Dotted path:
                "op.op_name"
                -> obj = kwargs["op"]; value = getattr(obj, "op_name", ...).
                Alias defaults to the last path segment ("op_name").

          - Alias + dotted path:
                "alias=op.op_name"
                -> same as above, but result is stored under key "alias".

          - Any of the above with "!" suffix:
                "payload!"
                "alias=payload!"
                -> if the resolved value is a dict or list/tuple, the
                   item-count limit is disabled (all top-level items are
                   included). Strings are still truncated; nesting remains
                   limited to one level.

          Profile-aware spec
          ------------------
          - "<p1>[,<p2>...]:<spec>"

                where `<spec>` is любой plain spec из формата выше (с
                возможным "!" и/или alias), а `<p1>,<p2>,...` — имена
                лог-профилей.

                For a given call:

                  * active_profile = get_active_log_profile()
                  * if active_profile is in {p1, p2, ...}:
                        `<spec>` is applied as a plain field spec;
                  * otherwise:
                        this entry is skipped.

        Missing kwargs / attributes at any step cause the spec to be ignored.

    active_profile:
        Name of the active log profile for this invocation. Used only for
        profile-aware specs; may be None to effectively treat all specs as
        profile-agnostic.

    Notes
    ---------
    - For each `raw_spec` from `field_specs`:

        1) `_select_spec_for_profile(raw_spec, active_profile)` is called:

           * returns `None`    -> spec is not applicable for this profile,
                                  skip it;
           * returns `spec`    -> string to be parsed as a plain field spec
                                  (with optional "!" and/or alias).

        2) The resulting `spec` is parsed:

           * "!" suffix is stripped and remembered to disable the item-count
             limit for dict/list values.

           * If "=" is present, left side is treated as alias, right side as
             path; otherwise the whole spec is the path and alias is inferred
             from the last path segment.

           * The first segment of the path selects the kwarg name; remaining
             segments form an attribute chain resolved via getattr(...).

           * If resolution succeeds, the value is normalized with:

                 normalize_value_for_log(
                     value,
                     max_items=None if unbounded_items else DEFAULT_MAX_ITEMS,
                 )

             and stored in the result mapping under the alias key.

    Returns
    -------
    dict[str, Any]
        Mapping of alias -> normalized value for all successfully resolved
        specs that are applicable to the active profile.
    """
    logged: dict[str, Any] = {}

    for item in _iter_resolved_fields(field_specs, source_kwargs, active_profile=active_profile):
        logged[item.alias] = normalize_value_for_log(
            item.value,
            max_items=None if item.unbounded_items else _MAX_ITEMS,
        )

    return logged


def _build_context_fragment(
    *,
    event_type: InvocationEventType,
    evt_prefix: str,
    field_specs: tuple[str, ...],
    source_kwargs: dict[str, Any],
    active_profile: str | None,
    formatter: Callable[[InvocationEventType, str, dict[str, Any]], dict[str, Any]] | None,
) -> dict[str, Any]:
    # Resolve raw fields from context_fields (if any)
    resolved: list[_ResolvedField] = []
    if field_specs:
        resolved = _iter_resolved_fields(field_specs, source_kwargs, active_profile=active_profile)

    raw_fields: dict[str, Any] = {item.alias: item.value for item in resolved}

    # Always call formatter when provided, even if raw_fields is empty

    if formatter is not None:
        # noinspection PyBroadException
        try:
            produced = formatter(event_type, evt_prefix, dict(raw_fields))
        except Exception:
            produced = None

        if isinstance(produced, dict):
            # noinspection PyUnnecessaryCast
            return cast(dict[str, Any], produced)

    # Fallback: normalized fields if we have any, otherwise empty dict
    if not resolved:
        return {}

    normalized: dict[str, Any] = {}
    for item in resolved:
        normalized[item.alias] = normalize_value_for_log(
            item.value,
            max_items=None if item.unbounded_items else _MAX_ITEMS,
        )
    return normalized


# noinspection DuplicatedCode
def _build_logged_result(
    field_specs: tuple[str, ...],
    result_obj: Any,
) -> Any:
    """
    Build a result snapshot for logging on success.

    Semantics:
      - If result_obj is a primitive (str/int/float/bool/None):
          return _normalize_for_log(result_obj)

      - If result_obj is a list or tuple:
          * If field_specs is empty:
              - return _normalize_for_log(result_obj)  (whole container)
          * If field_specs is non-empty:
              - For each spec:
                  - "0"                  -> result_obj[0]
                  - "1"                  -> result_obj[1]
                  - "op_id=1"            -> result_obj[1] stored under "op_id"
                  - "ttl_ms=2.ttl_ms"    -> getattr(result_obj[2], "ttl_ms")
                  - "field!" / "alias=field!" -> same, but with disabled item-count
                    limit for list/dict values.
              - Values are normalized via _normalize_for_log.
              - If no fields were resolved, falls back to _normalize_for_log(result_obj).

      - If result_obj is a dict:
          * field_specs are ignored and the whole dict is normalized.

      - If result_obj is any other composite object:
          * If field_specs is empty:
              - return _normalize_for_log(result_obj)  (e.g. "<ResultType>")
          * If field_specs is non-empty:
              - For each spec:
                  - "field"               -> getattr(result_obj, "field")
                  - "payload.user_id"     -> getattr(result_obj.payload, "user_id")
                  - "alias=payload.id"    -> same, but stored under key "alias"
                  - "field!" / "alias=field!" -> same, but with disabled item-count
                    limit for list/dict values.
              - Values are normalized via _normalize_for_log.
              - If no fields were resolved, falls back to _normalize_for_log(result_obj).
    """
    # Simple primitives: ignore specs and log whole result
    if isinstance(result_obj, (str, int, float, bool)) or result_obj is None:
        return normalize_value_for_log(result_obj)

    logged: dict[str, Any] = {}

    # List/tuple: allow indexed specs like "0", "op_id=1", "ttl_ms=2.ttl_ms"
    if isinstance(result_obj, (list, tuple)):
        if not field_specs:
            return normalize_value_for_log(result_obj)

        for spec in field_specs:
            spec = spec.strip()
            if not spec:
                continue

            # "!" suffix: disable item-count limit for this value
            unbounded_items = False
            if spec.endswith("!"):
                unbounded_items = True
                spec = spec[:-1].strip()
                if not spec:
                    continue

            alias = None
            if "=" in spec:
                alias_part, path_part = spec.split("=", 1)
                alias_candidate = alias_part.strip()
                path_str = path_part.strip()
                if alias_candidate:
                    alias = alias_candidate
            else:
                path_str = spec

            if not path_str:
                continue

            path_parts = [p for p in path_str.split(".") if p]
            if not path_parts:
                continue

            head = path_parts[0]
            try:
                idx = int(head)
            except ValueError:
                # Not an index -> skip for list/tuple mode
                continue

            if idx < 0 or idx >= len(result_obj):
                continue

            value = result_obj[idx]
            failed = False
            for attr in path_parts[1:]:
                if attr == "len()":
                    # noinspection PyBroadException
                    try:
                        value = len(value)
                    except Exception:
                        failed = True
                        break
                    continue

                # noinspection PyBroadException
                try:
                    value = getattr(value, attr)
                except Exception:
                    failed = True
                    break

            if failed:
                continue

            if alias is None:
                # "2.ttl_ms" -> "ttl_ms", "1" -> "item1"
                alias = path_parts[-1] if len(path_parts) > 1 else f"item{idx}"

            logged[alias] = normalize_value_for_log(
                value,
                max_items=None if unbounded_items else _MAX_ITEMS,
            )

        if not logged:
            return normalize_value_for_log(result_obj)

        return logged

    # Dict: keep old behavior – ignore specs and log whole dict
    if isinstance(result_obj, dict):
        return normalize_value_for_log(result_obj)

    # Composite result: try to pick fields according to specs
    if not field_specs:
        # No specs at all -> log composite as "<TypeName>"
        return normalize_value_for_log(result_obj)

    for spec in field_specs:
        spec = spec.strip()
        if not spec:
            continue

        # "!" suffix: disable item-count limit for this value
        unbounded_items = False
        if spec.endswith("!"):
            unbounded_items = True
            spec = spec[:-1].strip()
            if not spec:
                continue

        # Parse alias and path
        alias = None
        if "=" in spec:
            alias_part, path_part = spec.split("=", 1)
            alias_candidate = alias_part.strip()
            path_str = path_part.strip()
            if alias_candidate:
                alias = alias_candidate
        else:
            path_str = spec

        if not path_str:
            continue

        # Split path into attribute chain
        path_parts = [p for p in path_str.split(".") if p]
        if not path_parts:
            continue

        value = result_obj
        failed = False
        for attr in path_parts:
            if attr == "len()":
                # noinspection PyBroadException
                try:
                    value = len(value)
                except Exception:
                    failed = True
                    break
                continue

            # noinspection PyBroadException
            try:
                value = getattr(value, attr)
            except Exception:
                failed = True
                break

        if failed:
            continue

        if alias is None:
            alias = path_parts[-1]

        logged[alias] = normalize_value_for_log(
            value,
            max_items=None if unbounded_items else _MAX_ITEMS,
        )

    if not logged:
        # Nothing resolved -> log composite as "<TypeName>"
        return normalize_value_for_log(result_obj)

    return logged


# ---------- Decorator ----------


def log_invocation(
    evt_prefix: str,
    *,
    activation_profiles: tuple[str, ...] | None = None,
    invoke_level: int = logging.DEBUG,
    success_level: int = logging.DEBUG,
    error_level: int = logging.ERROR,
    error_level_suppressed: int = logging.DEBUG,
    cancel_level: int = logging.INFO,
    log_closures_on_invoke: dict[str, Any] | None = None,
    context_fields: tuple[str, ...] = (),
    context_formatter: (
        Callable[[InvocationEventType, str, dict[str, Any]], dict[str, Any]] | None
    ) = None,
    log_kwargs_on_invoke: tuple[str, ...] = (),
    log_result_on_success: Optional[tuple[str, ...]] = None,
    log_error_policy: LogErrorPolicy = (),
    logger: logging.Logger | None = None,
) -> Callable[[F], F]:
    """
    Decorator for structured logging around a function invocation.

    Semantics
    ---------
    For a given function and `evt_prefix`, the decorator emits events describing
    the invocation lifecycle.

    Events emitted
    --------------
    For each invocation attempt, the decorator may emit the following events.
    FAILED and CANCELLED are always emitted on those outcomes; INVOKE and SUCCESS
    depend on activation_profiles.

      - On entry:
          log_event(logger, f"{evt_prefix}.invoke", data)

      - On successful completion:
          log_event(logger, f"{evt_prefix}.success", data)

      - On failure (Exception):
          log_event(logger, f"{evt_prefix}.failed", data)

      - On cancellation (asyncio.CancelledError):
          log_event(logger, f"{evt_prefix}.cancelled", data)

    Event emission guarantee
    ------------------------
    - FAILED and CANCELLED are always emitted when such outcomes happen.
    - INVOKE and SUCCESS are emitted only when normal logging is enabled for the
      current active profile (see activation_profiles).

    When normal logging is enabled, exactly one ".invoke" is emitted per call and
    exactly one terminal event follows: ".success", ".failed", or ".cancelled".

    When normal logging is suppressed, ".invoke" and ".success" are not emitted,
    but ".failed" / ".cancelled" still are.

    Async support
    -------------
    This decorator is async-aware:

    - If the decorated function is an `async def` (coroutine function), the wrapper
      awaits it and logs the terminal event after the await completes.

    - If the decorated function is a regular `def` but returns an awaitable
      (e.g. a coroutine), the wrapper returns a proxy awaitable. The terminal event
      is logged when that awaitable is awaited (success/failed/cancelled), not at
      the moment the awaitable object is created.

    If the returned awaitable is never awaited, no terminal event is emitted.
    The ".invoke" event is emitted only when normal logging is enabled for the
    active profile; otherwise nothing is logged for that call.

    Event names
    -----------
    If `evt_prefix` is a non-empty string, the following event names are used:

      - "{evt_prefix}.invoke"
      - "{evt_prefix}.success"
      - "{evt_prefix}.failed"
      - "{evt_prefix}.cancelled"

    If `evt_prefix` is an empty string, plain event names are used:

      - "invoke"
      - "success"
      - "failed"
      - "cancelled"

    activation_profiles
    -------------------
    Controls normal (non-error) logging for INVOKE and SUCCESS events.

    - If activation_profiles is None (default):
        Normal logging is enabled for all profiles (backward-compatible).

    - If activation_profiles is a non-empty tuple of profile names:
        Normal logging (INVOKE and SUCCESS) is emitted only when the current
        active profile (get_active_log_profile()) is in this tuple.

    - If activation_profiles is an empty tuple:
        Normal logging (INVOKE and SUCCESS) is suppressed for all profiles.

    FAILED and CANCELLED events are always emitted regardless of activation_profiles.

    The active profile is resolved once per invocation (at wrapper entry) and is
    used consistently for that invocation's logging decisions.

    log_closures_on_invoke
    ----------------------
    `log_closures_on_invoke`, if provided, is a static mapping of names to values
    captured at decoration time (typically closure variables):

        some_outer_value = "tenant-a1"

        @log_invocation(
            evt_prefix="ops.idemp.ensure",
            log_closures_on_invoke={"tenant": some_outer_value},
        )
        def ensure_operation(...):
            ...

    On each invocation:

      - All entries from `log_closures_on_invoke` are normalized via
        normalize_value_for_log(..., max_items=DEFAULT_MAX_ITEMS).
      - The resulting mapping is stored under the "closures" key in
        the payload for the .invoke event only.

    If normalization of a particular value fails, its entry is set to "<unknown>".

    Payload structure
    -----------------
    All payloads start from an empty `base_data: dict[str, Any]`. Additional
    fields are added depending on the phase and options.

    Invoke payload:
      - Built and emitted only when normal logging is enabled for the active profile
        (see activation_profiles).

      - Starts from a shallow copy of `base_data`.

      - If `log_closures_on_invoke` is provided:
          - A normalized snapshot of its values is stored under `"closures"`.

      - The active log profile is resolved via `get_active_log_profile()`.

      - Positional and keyword arguments are bound to the function signature
        via `inspect.signature(func).bind(*args, **kwargs)` (with defaults
        applied). The resulting ordered mapping (including `self` / `cls` for
        bound methods) is used as the source of argument values for logging.

      - If `log_kwargs_on_invoke` is non-empty:
          - A snapshot of selected arguments is built via `_build_logged_kwargs`
            from this bound mapping, taking into account the active log profile
            (profile-aware specs are applied only when the current profile
            matches).
          - If the snapshot is non-empty, it is stored under `"kwargs"`.

    Success payload:
      - Built and emitted only when normal logging is enabled for the active profile
        (see activation_profiles).
      - Starts from a shallow copy of `base_data`.
      - If `log_result_on_success` is None (default):
          - The function result is NOT logged at all.
      - If `log_result_on_success` is provided (even as an empty tuple):
          - A snapshot of the result is built via `_build_logged_result`
            and stored under `"result"`.

    Failure payload:

      - Depends on `log_error_policy` and the internal "already logged" flag
        managed by `is_error_logged()` / `mark_error_logged()`:

          * When a detailed error payload is logged:
              - `data = {**base_data, "error": build_error_payload(err)}`

          * When the error payload is suppressed:
              - `data = base_data`

    Cancellation payload:

      - The cancellation is logged when an `asyncio.CancelledError` is raised.
      - The payload includes:
          - `"cancelled": True`
          - `"error": build_error_payload(err)`
      - The original cancellation is re-raised after logging (never swallowed).

    Normalization rules
    -------------------
    Snapshots for kwargs and result are produced via normalize_value_for_log(...):

    - Strings are truncated.
    - Lists/tuples and dicts are snapshotted one level deep with an item limit
      (unless "!" is used in the field spec to disable the top-level limit).
    - Nested composite values are represented as "<TypeName>" unless they provide
      a custom payload via the logging adapter / LogPayloadProvider pipeline.

    Field specs
    -----------
    For kwargs (invoke payload):

      - All specs in `log_kwargs_on_invoke` are resolved against the bound
        argument mapping produced by `inspect.signature(func).bind(...)`, so
        positional arguments are available under their parameter names
        (including `self` / `cls` for bound methods).

      - Each spec in `log_kwargs_on_invoke` may be:

          * "name"
          * "obj.attr"
          * "alias=obj.attr"
          * "<name>!" / "alias=<name>!" for unbounded dict/list
          * "<p1>[,<p2>...]:<spec>" — profile-aware form, where `<spec>` is
            any of the forms above; it is applied only if the active profile
            is in {p1, p2, ...}.

      - Missing arguments / attributes are silently ignored; keys like `self`
        are present in the bound mapping, but are typically not referenced.

    For results (success payload):

      - Specs in `log_result_on_success` (when provided) describe either
        tuple indices or attribute paths on the result object, with the
        same alias / "!" semantics, but without profile prefixes:

          * "0", "1", "alias=2", "ttl_ms=2.ttl_ms"
          * "field", "payload.user_id", "alias=payload.id", "field!"

      - Missing elements / attributes are silently ignored.

    This decorator does NOT guarantee that secrets will not end up in logs;
    the caller is responsible for choosing safe fields and paths.

    log_error_policy
    ----------------
    `log_error_policy` is an ordered tuple of per-operation rules controlling
    error payload logging.

    Each rule is a pair `(exc_type, force_log)` where:

      - `exc_type`: exception class to match via `isinstance(err, exc_type)`.
      - `force_log`: boolean override for the default flag-based behavior:

          * True  -> always log this error with a detailed `"error"` payload
                     on this operation (even if it was logged elsewhere before);
                     `mark_error_logged(err)` is called after logging.

          * False -> never log this error with a detailed `"error"` payload
                     on this operation; `mark_error_logged(err)` is still called
                     so that higher layers also skip detailed payload.

    The first matching rule wins. If no rule matches, default behavior is:

      - If `is_error_logged(err)` is False:
          - Log a detailed `"error"` payload once and call `mark_error_logged(err)`.
      - Otherwise:
          - Log only `base_data` (no `"error"` section).

    logger
    ------
    `logger`, if provided, must be a `logging.Logger` instance and is used
    directly for all events emitted by the decorator for the wrapped function.

    If `logger` is omitted or set to None:

      - The logger is resolved via `_default_logger_for(func, first_arg)`, which
        inspects the first positional argument of the call (usually `self` for
        bound methods) and tries, in order:

          * `first_arg.get_logger()` if it exists and returns a Logger;
          * `first_arg.logger` if it is a Logger;
          * otherwise `logging.getLogger(func.__module__)`.

    Notes
    -----
    - The decorator does NOT swallow exceptions. The original exception is
      re-raised after logging.
    - It logs cancellation (asyncio.CancelledError) via a dedicated ".cancelled"
      event and re-raises it.
    - It preserves the original function signature as far as possible and returns
      a wrapper compatible with sync and async call sites.
    - `log_event` is responsible for enforcing the structured logging contract
      (for example, using `evt` and `data` in a consistent way).
    """

    def decorate(func: F) -> F:
        if evt_prefix:
            evt_invoke = f"{evt_prefix}.invoke"
            evt_success = f"{evt_prefix}.success"
            evt_failed = f"{evt_prefix}.failed"
            evt_cancelled = f"{evt_prefix}.cancelled"
        else:
            evt_invoke = "invoke"
            evt_success = "success"
            evt_failed = "failed"
            evt_cancelled = "cancelled"

        effective_context_fields = context_fields

        sig = inspect.signature(func)

        def _should_log(active_profile: str | None) -> bool:
            """
            Decide whether to emit normal (INVOKE/SUCCESS) events for this call.

            FAILED and CANCELLED are always logged regardless of profile.
            """
            if activation_profiles is None:
                return True
            if not active_profile:
                return False
            return active_profile in activation_profiles

        def _emit_cancelled(
            effective_logger: logging.Logger,
            effective_kwargs: dict[str, Any],
            active_profile: str | None,
            err: BaseException,
        ) -> None:
            if is_error_logged(err):
                return

            payload: dict[str, Any] = {"cancelled": True, "error": build_error_payload(err)}

            context_fragment = _build_context_fragment(
                event_type=InvocationEventType.CANCELLED,
                evt_prefix=evt_prefix,
                field_specs=effective_context_fields,
                source_kwargs=effective_kwargs,
                active_profile=active_profile,
                formatter=context_formatter,
            )
            _apply_context(payload, context_fragment)

            log_event(effective_logger, evt_cancelled, payload, level=cancel_level)
            mark_error_logged(err)

        def _emit_failed(
            effective_logger: logging.Logger,
            effective_kwargs: dict[str, Any],
            active_profile: str | None,
            err: Exception,
        ) -> None:
            payload: dict[str, Any] = {}

            context_fragment = _build_context_fragment(
                event_type=InvocationEventType.FAILED,
                evt_prefix=evt_prefix,
                field_specs=effective_context_fields,
                source_kwargs=effective_kwargs,
                active_profile=active_profile,
                formatter=context_formatter,
            )
            _apply_context(payload, context_fragment)

            policy_applied = False

            if log_error_policy:
                for exc_type, force_log in log_error_policy:
                    if isinstance(err, exc_type):
                        policy_applied = True
                        if force_log:
                            payload["error"] = build_error_payload(err)
                            log_event(effective_logger, evt_failed, payload, level=error_level)
                            mark_error_logged(err)
                        else:
                            log_event(
                                effective_logger, evt_failed, payload, level=error_level_suppressed
                            )
                            mark_error_logged(err)
                        break

            if not policy_applied:
                if not is_error_logged(err):
                    payload["error"] = build_error_payload(err)
                    log_event(effective_logger, evt_failed, payload, level=error_level)
                    mark_error_logged(err)
                else:
                    log_event(effective_logger, evt_failed, payload, level=error_level_suppressed)

        def _apply_context(payload: dict[str, Any], context_fragment: dict[str, Any]) -> None:
            """
            Merge context into payload.

            If context fragment collides with SYSTEM_KEYS -> wrap under 'context'.
            """
            if not context_fragment:
                return

            if any(k in SYSTEM_KEYS for k in context_fragment.keys()):
                existing = payload.get("context")
                if isinstance(existing, dict):
                    existing.update(context_fragment)
                else:
                    payload["context"] = context_fragment
                return

            payload.update(context_fragment)

        def _prepare_invoke_context(
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> tuple[dict[str, Any], str | None]:
            active_profile = get_active_log_profile()

            try:
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                effective_kwargs = dict(bound.arguments)
            except TypeError:
                effective_kwargs = dict(kwargs)

            return effective_kwargs, active_profile

        def _emit_invoke(
            effective_logger: logging.Logger,
            effective_kwargs: dict[str, Any],
            active_profile: str | None,
        ) -> None:
            invoke_data: dict[str, Any] = {}

            if log_closures_on_invoke:
                closures: dict[str, Any] = {}
                for key, value in log_closures_on_invoke.items():
                    # noinspection PyBroadException
                    try:
                        closures[key] = normalize_value_for_log(value, max_items=_MAX_ITEMS)
                    except Exception:
                        closures[key] = "<unknown>"
                if closures:
                    invoke_data["closures"] = closures

            context_fragment = _build_context_fragment(
                event_type=InvocationEventType.INVOKE,
                evt_prefix=evt_prefix,
                field_specs=effective_context_fields,
                source_kwargs=effective_kwargs,
                active_profile=active_profile,
                formatter=context_formatter,
            )
            _apply_context(invoke_data, context_fragment)

            if log_kwargs_on_invoke:
                kwargs_snapshot = _build_logged_kwargs(
                    log_kwargs_on_invoke,
                    effective_kwargs,
                    active_profile=active_profile,
                )
                if kwargs_snapshot:
                    invoke_data["kwargs"] = kwargs_snapshot

            log_event(effective_logger, evt_invoke, invoke_data, level=invoke_level)

        def _resolve_logger_from_call(args: tuple[Any, ...]) -> logging.Logger:
            first_arg = args[0] if args else None
            if logger is not None:
                return logger
            return _default_logger_for(func, first_arg)

        def _log_success(
            effective_logger: logging.Logger,
            effective_kwargs: dict[str, Any],
            active_profile: str | None,
            result: Any,
        ) -> None:
            payload: dict[str, Any] = {}

            context_fragment = _build_context_fragment(
                event_type=InvocationEventType.SUCCESS,
                evt_prefix=evt_prefix,
                field_specs=effective_context_fields,
                source_kwargs=effective_kwargs,
                active_profile=active_profile,
                formatter=context_formatter,
            )
            _apply_context(payload, context_fragment)

            if log_result_on_success is not None:
                payload["result"] = _build_logged_result(log_result_on_success, result)

            log_event(effective_logger, evt_success, payload, level=success_level)

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
                effective_logger = _resolve_logger_from_call(args)

                effective_kwargs, active_profile = _prepare_invoke_context(args, kwargs)
                emit_normal = _should_log(active_profile)

                if emit_normal:
                    _emit_invoke(effective_logger, effective_kwargs, active_profile)

                try:
                    result = await func(*args, **kwargs)

                except asyncio.CancelledError as err:
                    _emit_cancelled(effective_logger, effective_kwargs, active_profile, err)
                    raise
                except Exception as err:
                    _emit_failed(effective_logger, effective_kwargs, active_profile, err)
                    raise
                else:
                    if emit_normal:
                        _log_success(effective_logger, effective_kwargs, active_profile, result)
                    return result

            # noinspection PyUnnecessaryCast
            return cast(F, wrapped_async)

        @wraps(func)
        def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
            effective_logger = _resolve_logger_from_call(args)

            effective_kwargs, active_profile = _prepare_invoke_context(args, kwargs)
            emit_normal = _should_log(active_profile)

            if emit_normal:
                _emit_invoke(effective_logger, effective_kwargs, active_profile)

            try:
                result = func(*args, **kwargs)
            except asyncio.CancelledError as err:
                _emit_cancelled(effective_logger, effective_kwargs, active_profile, err)
                raise
            except Exception as err:
                _emit_failed(effective_logger, effective_kwargs, active_profile, err)
                raise

            if inspect.isawaitable(result):

                async def _await_and_log() -> Any:
                    try:
                        awaited = await cast(Awaitable[Any], result)
                    except asyncio.CancelledError as exc:
                        _emit_cancelled(effective_logger, effective_kwargs, active_profile, exc)
                        raise
                    except Exception as exc:
                        _emit_failed(effective_logger, effective_kwargs, active_profile, exc)
                        raise
                    else:
                        if emit_normal:
                            _log_success(
                                effective_logger, effective_kwargs, active_profile, awaited
                            )
                        return awaited

                return _await_and_log()

            if emit_normal:
                _log_success(effective_logger, effective_kwargs, active_profile, result)
            return result

        # noinspection PyUnnecessaryCast
        return cast(F, wrapped_sync)

    return decorate
