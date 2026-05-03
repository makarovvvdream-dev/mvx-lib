# common/src/mvx/common/logger/core/log_invocation.py
from __future__ import annotations
from typing import ParamSpec, TypeVar, Callable, Awaitable, Any, cast, TypeAlias
from dataclasses import dataclass
from enum import StrEnum
import re

import inspect
from functools import wraps

import asyncio

from .protocols import LogLevel, LogContextProto, LogContextProviderProto, LogEntityIdProviderProto


class InvocationEventType(StrEnum):
    INVOKE = "invoke"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


SYSTEM_KEYS: frozenset[str] = frozenset({"error", "kwargs", "result", "cancelled", "closures"})

_EVENT_RE = re.compile(r"^[A-Za-z_.]+$")


def _resolve_context(args: tuple[Any, ...]) -> LogContextProto:
    first_arg = args[0] if args else None
    if isinstance(first_arg, LogContextProviderProto):
        return first_arg.get_log_context()

    raise RuntimeError("no logger context found")


def _resolve_entity_id(args: tuple[Any, ...]) -> str | None:
    first_arg = args[0] if args else None
    if isinstance(first_arg, LogEntityIdProviderProto):
        return first_arg.identity

    return None


def _extract_func_arguments(
    func_signature: inspect.Signature,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    try:
        bound = func_signature.bind(*args, **kwargs)
        bound.apply_defaults()
        res = dict(bound.arguments)
    except TypeError:
        res = dict(kwargs)

    return res


@dataclass(frozen=True, slots=True)
class _ResolvedField:
    alias: str
    value: Any
    unbounded_items: bool


PayloadFormatter: TypeAlias = Callable[
    [LogContextProto, InvocationEventType, str, dict[str, Any]], dict[str, Any]
]


def _apply_verbosity_filter(raw_spec: str, verbosity_level: str) -> str | None:
    raw_spec_stripped = raw_spec.strip()
    if not raw_spec_stripped:
        return None

    if ":" not in raw_spec_stripped:
        return raw_spec_stripped

    left, right = raw_spec_stripped.split(":", 1)

    # Right part must be non-empty
    spec = right.strip()
    if not spec:
        return None

    # Build set of supported verbosity level names.
    supported_verb_levels = {p.strip() for p in left.split(",")}
    supported_verb_levels.discard("")

    if not supported_verb_levels or verbosity_level in supported_verb_levels:
        return spec

    return None


def _resolve_fields(
    field_specs: tuple[str, ...],
    source_kwargs: dict[str, Any],
    verbosity_level: str,
) -> list[_ResolvedField]:

    resolved: list[_ResolvedField] = []

    for spec in field_specs:
        spec = spec.strip()
        if not spec:
            continue

        effective_spec = _apply_verbosity_filter(spec, verbosity_level)
        if effective_spec is None:
            continue

        spec = effective_spec

        unbounded_items = False
        if spec.endswith("!"):
            unbounded_items = True
            spec = spec[:-1].strip()
            if not spec:
                continue

        alias: str | None = None
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
                    # noinspection PyTypeChecker
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

        # noinspection PyTypeChecker
        resolved.append(_ResolvedField(alias=alias, value=value, unbounded_items=unbounded_items))

    return resolved


def _inject_context_payload(
    *,
    ctx: LogContextProto,
    event: str,
    event_type: InvocationEventType,
    field_specs: tuple[str, ...],
    source_kwargs: dict[str, Any],
    payload_formatter: PayloadFormatter | None,
    target_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Injects context fields and optionally formatted fields into a target payload.

    This function processes context fields specified by `field_specs`, formats them
    using a `payload_formatter` if provided, and updates the target payload `target_payload`
    with the resolved and formatted fields.

    :param ctx: A `LogContextProto` instance that provides the context for the log
        injection process.
    :param event: The event name to be associated with the log entry.
    :param event_type: The type of the invocation event, represented as an
        `InvocationEventType` instance.
    :param field_specs: A tuple of field specifications that need to be resolved
        from `source_kwargs` and included into the payload.
    :param source_kwargs: A dictionary containing source data that can be used to
        resolve context fields based on `field_specs`.
    :param payload_formatter: An optional callable or `PayloadFormatter` instance
        to format the resolved fields before injecting them into the payload.
    :param target_payload: A dictionary that represents the target payload where
        the resolved and optionally formatted fields will be injected.

    :return: The updated target payload dictionary containing the injected context
        fields and/or formatted payload.
    """

    def _inject_to_target(
        _payload: dict[str, Any] | None, _target: dict[str, Any]
    ) -> dict[str, Any]:
        if not _payload:
            return _target

        if any(k in SYSTEM_KEYS for k in _payload.keys()):
            _existing = _target.get("context")
            if isinstance(_existing, dict):
                _existing.update(_payload)
            else:
                _target["context"] = _payload

            return _target

        _target.update(_payload)
        return _target

    # Resolve raw fields from context_fields
    resolved: list[_ResolvedField] = []
    if field_specs:
        resolved = _resolve_fields(field_specs, source_kwargs, verbosity_level=ctx.verbosity_level)

    raw_fields: dict[str, Any] = {item.alias: item.value for item in resolved}

    # Always call formatter when provided, even if raw_fields is empty
    if payload_formatter is not None:
        # noinspection PyBroadException
        try:
            produced = payload_formatter(ctx, event_type, event, dict(raw_fields))
        except Exception:
            produced = None

        if isinstance(produced, dict):
            return _inject_to_target(dict(produced), target_payload)

    # Fallback: normalized fields if we have any, otherwise empty dict
    if not resolved:
        return target_payload

    normalized: dict[str, Any] = {}
    for item in resolved:
        normalized[item.alias] = ctx.normalize_value_for_log(
            item.value,
            unbounded=item.unbounded_items,
        )
    return _inject_to_target(normalized, target_payload)


def _build_logged_kwargs(
    *,
    ctx: LogContextProto,
    field_specs: tuple[str, ...],
    source_kwargs: dict[str, Any],
) -> dict[str, Any]:
    """
    Builds a dictionary of logged keyword arguments based on provided field specifications,
    source keyword arguments, and the verbosity level provided by Logging context. This function
    resolves fields, normalizes their values using the logging context, and populates the
    resulting dictionary accordingly.

    :param ctx: Logging context used to normalize field values.
    :type ctx: LogContextProto
    :param field_specs: A tuple of field specifications to be resolved from the source
        keyword arguments.
    :type field_specs: tuple[str, ...]
    :param source_kwargs: A dictionary of source keyword arguments to extract fields from.
    :type source_kwargs: dict[str, Any]
    :return: A dictionary containing normalized field values based on the resolved field
        specifications.
    :rtype: dict[str, Any]
    """
    logged: dict[str, Any] = {}

    for item in _resolve_fields(field_specs, source_kwargs, verbosity_level=ctx.verbosity_level):
        logged[item.alias] = ctx.normalize_value_for_log(
            item.value,
            unbounded=item.unbounded_items,
        )

    return logged


def _build_logged_result(
    *,
    ctx: LogContextProto,
    field_specs: tuple[str, ...],
    result_obj: Any,
) -> Any:
    """
    Generates a structured and normalized log representation of `result_obj`, handling various data types
    and specifications for selective logging.

    :param ctx: Context object implementing the `LogContextProto` interface, used to normalize values
                for logging.
    :type ctx: LogContextProto
    :param field_specs: A tuple of string specifications used to extract and alias specific fields or
                        attributes from the `result_obj` for logging; supports dot-separated paths, indexing,
                        and aliasing.
    :type field_specs: tuple[str, ...]
    :param result_obj: The object to be processed and logged; can be a primitive, list, tuple, dictionary,
                       or composite structure.
    :return: A normalized representation of the `result_obj` tailored to the specified log structure,
             or the entire `result_obj` if field specifications are undefined or invalid.
    :rtype: Any
    """
    # Simple primitives: ignore specs and log whole result
    if isinstance(result_obj, (str, int, float, bool)) or result_obj is None:
        return ctx.normalize_value_for_log(result_obj)

    payload: dict[str, Any] = {}

    # List/tuple: allow indexed specs like "0", "op_id=1", "ttl_ms=2.ttl_ms"
    if isinstance(result_obj, (list, tuple)):
        if not field_specs:
            return ctx.normalize_value_for_log(result_obj)

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
                        # noinspection PyTypeChecker
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

            payload[alias] = ctx.normalize_value_for_log(
                value,
                unbounded=unbounded_items,
            )

        if not payload:
            return ctx.normalize_value_for_log(result_obj)

        return payload

    # Dict: keep old behavior – ignore specs and log whole dict
    if isinstance(result_obj, dict):
        return ctx.normalize_value_for_log(result_obj)

    # Composite result: try to pick fields according to specs
    if not field_specs:
        # No specs at all -> log composite as "<TypeName>"
        return ctx.normalize_value_for_log(result_obj)

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
                    # noinspection PyTypeChecker
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

        payload[alias] = ctx.normalize_value_for_log(
            value,
            unbounded=unbounded_items,
        )

    if not payload:
        # Nothing resolved -> log composite as "<TypeName>"
        return ctx.normalize_value_for_log(result_obj)

    return payload


# ---------- Decorator ----------

P = ParamSpec("P")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])


LogErrorPolicyRule = tuple[type[BaseException], bool]
LogErrorPolicy = tuple[LogErrorPolicyRule, ...]


def log_invocation(
    event: str,
    *,
    invoke_level: LogLevel = LogLevel.DEBUG,
    success_level: LogLevel = LogLevel.DEBUG,
    error_level: LogLevel = LogLevel.ERROR,
    error_level_suppressed: LogLevel = LogLevel.DEBUG,
    cancel_level: LogLevel = LogLevel.INFO,
    log_closures_on_invoke: dict[str, Any] | None = None,
    context_fields: tuple[str, ...] = (),
    context_formatter: PayloadFormatter | None = None,
    log_kwargs_on_invoke: tuple[str, ...] = (),
    log_result_on_success: tuple[str, ...] | None = None,
    log_error_policy: LogErrorPolicy = (),
    ctx: LogContextProto | None = None,
    entity_id_getter: Callable[[], str] | None = None,
) -> Callable[[F], F]:

    def decorate(func: F) -> F:

        sig = inspect.signature(func)

        if not _EVENT_RE.fullmatch(event):
            raise ValueError(f"Invalid event name: {event!r}")

        def _emit_invoke(
            effective_ctx: LogContextProto,
            effective_kwargs: dict[str, Any],
            entity_id: str | None,
        ) -> None:
            invoke_data: dict[str, Any] = {}

            if log_closures_on_invoke:
                closures: dict[str, Any] = {}
                for key, value in log_closures_on_invoke.items():
                    # noinspection PyBroadException
                    try:
                        closures[key] = effective_ctx.normalize_value_for_log(value)
                    except Exception:
                        closures[key] = "<unknown>"
                if closures:
                    invoke_data["closures"] = closures

            _inject_context_payload(
                ctx=effective_ctx,
                event=event,
                event_type=InvocationEventType.INVOKE,
                field_specs=context_fields,
                source_kwargs=effective_kwargs,
                payload_formatter=context_formatter,
                target_payload=invoke_data,
            )

            if log_kwargs_on_invoke:
                kwargs_payload = _build_logged_kwargs(
                    ctx=effective_ctx,
                    field_specs=log_kwargs_on_invoke,
                    source_kwargs=effective_kwargs,
                )
                if kwargs_payload:
                    invoke_data["kwargs"] = kwargs_payload

            effective_ctx.log_event(
                event,
                invoke_level,
                invoke_data,
                event_type=InvocationEventType.INVOKE.value,
                entity_id=entity_id,
            )

        def _emit_cancelled(
            effective_ctx: LogContextProto,
            effective_kwargs: dict[str, Any],
            err: BaseException,
            entity_id: str | None,
        ) -> None:

            if effective_ctx.is_error_logged(err):
                return

            payload: dict[str, Any] = {
                "cancelled": True,
                "error": effective_ctx.build_error_payload(err),
            }

            _inject_context_payload(
                ctx=effective_ctx,
                event=event,
                event_type=InvocationEventType.CANCELLED,
                field_specs=context_fields,
                source_kwargs=effective_kwargs,
                payload_formatter=context_formatter,
                target_payload=payload,
            )

            effective_ctx.log_event(
                event,
                cancel_level,
                payload,
                event_type=InvocationEventType.CANCELLED.value,
                entity_id=entity_id,
            )

            effective_ctx.mark_error_logged(err)

        def _emit_failed(
            effective_ctx: LogContextProto,
            effective_kwargs: dict[str, Any],
            err: Exception,
            entity_id: str | None,
        ) -> None:
            payload: dict[str, Any] = {}

            _inject_context_payload(
                ctx=effective_ctx,
                event=event,
                event_type=InvocationEventType.FAILED,
                field_specs=context_fields,
                source_kwargs=effective_kwargs,
                payload_formatter=context_formatter,
                target_payload=payload,
            )

            policy_applied = False

            if log_error_policy:
                for exc_type, force_log in log_error_policy:
                    if isinstance(err, exc_type):
                        policy_applied = True
                        if force_log:
                            payload["error"] = effective_ctx.build_error_payload(err)
                            effective_ctx.log_event(
                                event,
                                error_level,
                                payload,
                                event_type=InvocationEventType.FAILED.value,
                                entity_id=entity_id,
                            )
                            effective_ctx.mark_error_logged(err)
                        else:
                            effective_ctx.log_event(
                                event,
                                error_level_suppressed,
                                payload,
                                event_type=InvocationEventType.FAILED.value,
                                entity_id=entity_id,
                            )

                            effective_ctx.mark_error_logged(err)
                        break

            if not policy_applied:
                if not effective_ctx.is_error_logged(err):
                    payload["error"] = effective_ctx.build_error_payload(err)
                    effective_ctx.log_event(
                        event,
                        error_level,
                        payload,
                        event_type=InvocationEventType.FAILED.value,
                        entity_id=entity_id,
                    )
                    effective_ctx.mark_error_logged(err)
                else:
                    effective_ctx.log_event(
                        event,
                        error_level_suppressed,
                        payload,
                        event_type=InvocationEventType.FAILED.value,
                        entity_id=entity_id,
                    )

        def _emit_success(
            effective_ctx: LogContextProto,
            effective_kwargs: dict[str, Any],
            result: Any,
            entity_id: str | None,
        ) -> None:
            payload: dict[str, Any] = {}

            _inject_context_payload(
                ctx=effective_ctx,
                event=event,
                event_type=InvocationEventType.SUCCESS,
                field_specs=context_fields,
                source_kwargs=effective_kwargs,
                payload_formatter=context_formatter,
                target_payload=payload,
            )

            if log_result_on_success is not None:
                payload["result"] = _build_logged_result(
                    ctx=effective_ctx,
                    field_specs=log_result_on_success,
                    result_obj=result,
                )

            effective_ctx.log_event(
                event,
                success_level,
                payload,
                event_type=InvocationEventType.SUCCESS.value,
                entity_id=entity_id,
            )

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
                effective_kwargs = _extract_func_arguments(sig, args, kwargs)

                effective_ctx = ctx if ctx is not None else _resolve_context(args)

                entity_id = entity_id_getter() if entity_id_getter else _resolve_entity_id(args)

                event_enabled = effective_ctx.is_enabled(event)

                if event_enabled:
                    _emit_invoke(effective_ctx, effective_kwargs, entity_id)

                try:
                    result = await func(*args, **kwargs)

                except asyncio.CancelledError as err:
                    _emit_cancelled(effective_ctx, effective_kwargs, err, entity_id)
                    raise
                except Exception as err:
                    _emit_failed(effective_ctx, effective_kwargs, err, entity_id)
                    raise
                else:
                    if event_enabled:
                        _emit_success(effective_ctx, effective_kwargs, result, entity_id)
                    return result

            # noinspection PyUnnecessaryCast
            return cast(F, wrapped_async)

        @wraps(func)
        def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
            effective_kwargs = _extract_func_arguments(sig, args, kwargs)

            effective_ctx = ctx if ctx is not None else _resolve_context(args)
            entity_id = entity_id_getter() if entity_id_getter else _resolve_entity_id(args)

            event_enabled = effective_ctx.is_enabled(event)

            if event_enabled:
                _emit_invoke(effective_ctx, effective_kwargs, entity_id)

            try:
                result = func(*args, **kwargs)
            except asyncio.CancelledError as err:
                _emit_cancelled(effective_ctx, effective_kwargs, err, entity_id)
                raise
            except Exception as err:
                _emit_failed(effective_ctx, effective_kwargs, err, entity_id)
                raise

            if inspect.isawaitable(result):

                async def _await_and_log() -> Any:
                    try:
                        awaited = await cast(Awaitable[Any], result)
                    except asyncio.CancelledError as exc:
                        _emit_cancelled(effective_ctx, effective_kwargs, exc, entity_id)
                        raise
                    except Exception as exc:
                        _emit_failed(effective_ctx, effective_kwargs, exc, entity_id)
                        raise
                    else:
                        if event_enabled:
                            _emit_success(effective_ctx, effective_kwargs, awaited, entity_id)
                        return awaited

                return _await_and_log()

            if event_enabled:
                _emit_success(effective_ctx, effective_kwargs, result, entity_id)
            return result

        # noinspection PyUnnecessaryCast
        return cast(F, wrapped_sync)

    return decorate
