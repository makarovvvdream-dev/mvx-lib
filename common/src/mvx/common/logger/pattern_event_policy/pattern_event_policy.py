# src/mvx/common/logger/pattern_event_policy/pattern_event_policy.py

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from fnmatch import fnmatchcase
from typing import Any, Mapping, Sequence

from mvx.common.logger import LogEventMeta

__all__ = (
    "PatternLogEventPolicyAction",
    "PatternLogEventPolicyRuleConfig",
    "PatternLogEventPolicyConfig",
    "PatternLogEventPolicy",
)


class PatternLogEventPolicyAction(StrEnum):
    ALLOW = "allow"
    DENY = "deny"


@dataclass(frozen=True, slots=True)
class PatternLogEventPolicyRuleConfig:
    action: PatternLogEventPolicyAction

    events: tuple[str, ...] = field(default_factory=tuple)
    event_namespaces: tuple[str, ...] = field(default_factory=tuple)
    event_names: tuple[str, ...] = field(default_factory=tuple)
    entity_ids: tuple[str, ...] = field(default_factory=tuple)
    source_paths: tuple[str, ...] = field(default_factory=tuple)
    source_funcs: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> PatternLogEventPolicyRuleConfig:
        action_raw = _get_required_str(data, "action")

        try:
            action = PatternLogEventPolicyAction(action_raw)
        except ValueError as exc:
            raise ValueError(
                "field 'action' must be one of: " f"{_enum_values(PatternLogEventPolicyAction)}"
            ) from exc

        return cls(
            action=action,
            events=_get_str_tuple(data, "events"),
            event_namespaces=_get_str_tuple(data, "event_namespaces"),
            event_names=_get_str_tuple(data, "event_names"),
            entity_ids=_get_str_tuple(data, "entity_ids"),
            source_paths=_get_str_tuple(data, "source_paths"),
            source_funcs=_get_str_tuple(data, "source_funcs"),
        )


@dataclass(frozen=True, slots=True)
class PatternLogEventPolicyConfig:
    default_enabled: bool = True
    rules: tuple[PatternLogEventPolicyRuleConfig, ...] = field(default_factory=tuple)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> PatternLogEventPolicyConfig:
        return cls(
            default_enabled=_get_bool(data, "default_enabled", default=True),
            rules=_get_rule_tuple(data, "rules"),
        )


class PatternLogEventPolicy:
    def __init__(self, config: PatternLogEventPolicyConfig) -> None:
        if not isinstance(config, PatternLogEventPolicyConfig):
            raise TypeError(
                "argument 'config' must be an instance of 'PatternLogEventPolicyConfig'"
            )

        self._config = config

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        for rule in self._config.rules:
            if _rule_matches(rule, event):
                return rule.action is PatternLogEventPolicyAction.ALLOW

        return self._config.default_enabled


def _rule_matches(rule: PatternLogEventPolicyRuleConfig, event: LogEventMeta) -> bool:
    event_key = _event_key(event)

    if rule.events and not _matches_optional_any(event_key, rule.events):
        return False

    if rule.event_namespaces and not _matches_optional_any(
        event.event_namespace,
        rule.event_namespaces,
    ):
        return False

    if rule.event_names and not _matches_optional_any(
        event.event_name,
        rule.event_names,
    ):
        return False

    if rule.entity_ids and not _matches_optional_any(
        event.entity_id,
        rule.entity_ids,
    ):
        return False

    if rule.source_paths and not _matches_optional_any(
        event.source_path,
        rule.source_paths,
    ):
        return False

    if rule.source_funcs and not _matches_optional_any(
        event.source_func,
        rule.source_funcs,
    ):
        return False

    return True


def _event_key(event: LogEventMeta) -> str | None:
    event_name = event.event_name

    if event_name is None:
        return None

    event_namespace = event.event_namespace

    if event_namespace:
        return f"{event_namespace}.{event_name}"

    return event_name


def _matches_optional_any(value: str | None, patterns: Sequence[str]) -> bool:
    if value is None:
        return False

    return _matches_any(value, patterns)


def _matches_any(value: str, patterns: Sequence[str]) -> bool:
    return any(fnmatchcase(value, pattern) for pattern in patterns)


def _get_bool(data: Mapping[str, Any], key: str, *, default: bool) -> bool:
    value = data.get(key, default)

    if not isinstance(value, bool):
        raise TypeError(f"field '{key}' must be bool")

    return value


def _get_required_str(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)

    if not isinstance(value, str):
        raise TypeError(f"field '{key}' must be string")

    value = value.strip()

    if not value:
        raise ValueError(f"field '{key}' must not be empty")

    return value


def _get_str_tuple(data: Mapping[str, Any], key: str) -> tuple[str, ...]:
    value = data.get(key, ())

    if value is None:
        return ()

    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"field '{key}' must be a sequence of strings")

    result: list[str] = []

    for item in value:
        if not isinstance(item, str):
            raise TypeError(f"field '{key}' must contain only strings")

        item = item.strip()

        if not item:
            raise ValueError(f"field '{key}' must not contain empty strings")

        result.append(item)

    return tuple(result)


def _get_rule_tuple(
    data: Mapping[str, Any],
    key: str,
) -> tuple[PatternLogEventPolicyRuleConfig, ...]:
    value = data.get(key, ())

    if value is None:
        return ()

    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"field '{key}' must be a sequence of mappings")

    result: list[PatternLogEventPolicyRuleConfig] = []

    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError(f"field '{key}' must contain only mappings")

        result.append(PatternLogEventPolicyRuleConfig.from_mapping(item))

    return tuple(result)


def _enum_values(enum_type: type[StrEnum]) -> str:
    return ", ".join(repr(item.value) for item in enum_type)
