# tests/test_logger/pattern_event_policy/test_pattern_event_policy.py

"""
Tests for mvx.networking.helpers.pattern_event_policy.PatternLogEventPolicy.

Grouping rule:
  - Group a: action values, config parsing
  - Group b: rule parsing
  - Group c: config validation
  - Group d: rule validation
  - Group e: policy construction and default behavior
  - Group f: event key matching
  - Group g: field-specific matching
  - Group h: multi-field matching
  - Group i: nullable LogEventMeta fields
  - Group j: first matching rule wins

Naming rule:
  Each test name starts with test_<group><num>_, e.g. test_a1_...
"""

from __future__ import annotations


from typing import cast
from dataclasses import dataclass

import pytest

from mvx.common.logger import LogEventMeta

from mvx.common.logger.pattern_event_policy import (
    PatternLogEventPolicy,
    PatternLogEventPolicyAction,
    PatternLogEventPolicyConfig,
    PatternLogEventPolicyRuleConfig,
)


@dataclass(frozen=True, slots=True)
class _FakeLogEventMeta:
    event_namespace: str | None = "tcp_stream_engine"
    event_name: str | None = "open"
    entity_id: str | None = "engine-1"
    source_path: str | None = "src/mvx/networking/engines/tcp_stream_engine/tcp_stream_engine.py"
    source_line: int | None = 100
    source_func: str | None = "open"


def _event(
    *,
    event_namespace: str | None = "tcp_stream_engine",
    event_name: str | None = "open",
    entity_id: str | None = "engine-1",
    source_path: str | None = "src/mvx/networking/engines/tcp_stream_engine/tcp_stream_engine.py",
    source_line: int | None = 100,
    source_func: str | None = "open",
) -> LogEventMeta:
    return cast(
        LogEventMeta,
        cast(
            object,
            _FakeLogEventMeta(
                event_namespace=event_namespace,
                event_name=event_name,
                entity_id=entity_id,
                source_path=source_path,
                source_line=source_line,
                source_func=source_func,
            ),
        ),
    )


# -------------------------
# Group a: action values, config parsing
# -------------------------


def test_a1_action_values_are_stable() -> None:
    """PatternLogEventPolicyAction values are stable."""
    assert PatternLogEventPolicyAction.ALLOW.value == "allow"
    assert PatternLogEventPolicyAction.DENY.value == "deny"


def test_a2_config_from_mapping_uses_defaults() -> None:
    """Config parser uses defaults when mapping is empty."""
    config = PatternLogEventPolicyConfig.from_mapping({})

    assert config.default_enabled is True
    assert config.rules == ()


def test_a3_config_from_mapping_parses_default_enabled_and_rules() -> None:
    """Config parser parses default_enabled and nested rules."""
    config = PatternLogEventPolicyConfig.from_mapping(
        {
            "default_enabled": False,
            "rules": [
                {
                    "action": "allow",
                    "events": ["tcp_stream_engine.open"],
                },
                {
                    "action": "deny",
                    "entity_ids": ["noisy-*"],
                },
            ],
        }
    )

    assert config.default_enabled is False
    assert len(config.rules) == 2

    assert config.rules[0].action is PatternLogEventPolicyAction.ALLOW
    assert config.rules[0].events == ("tcp_stream_engine.open",)

    assert config.rules[1].action is PatternLogEventPolicyAction.DENY
    assert config.rules[1].entity_ids == ("noisy-*",)


def test_a4_config_from_mapping_treats_none_rules_as_empty() -> None:
    """Config parser treats rules=None as empty rules tuple."""
    config = PatternLogEventPolicyConfig.from_mapping({"rules": None})

    assert config.rules == ()


# -------------------------
# Group b: rule parsing
# -------------------------


def test_b1_rule_from_mapping_parses_all_fields() -> None:
    """Rule parser parses all supported fields."""
    rule = PatternLogEventPolicyRuleConfig.from_mapping(
        {
            "action": "allow",
            "events": ["tcp_stream_engine.open"],
            "event_namespaces": ["tcp_stream_engine"],
            "event_names": ["open"],
            "entity_ids": ["engine-*"],
            "source_paths": ["*/tcp_stream_engine.py"],
            "source_funcs": ["open"],
        }
    )

    assert rule.action is PatternLogEventPolicyAction.ALLOW
    assert rule.events == ("tcp_stream_engine.open",)
    assert rule.event_namespaces == ("tcp_stream_engine",)
    assert rule.event_names == ("open",)
    assert rule.entity_ids == ("engine-*",)
    assert rule.source_paths == ("*/tcp_stream_engine.py",)
    assert rule.source_funcs == ("open",)


def test_b2_rule_from_mapping_strips_action_and_pattern_values() -> None:
    """Rule parser strips action and pattern string values."""
    rule = PatternLogEventPolicyRuleConfig.from_mapping(
        {
            "action": " allow ",
            "events": [" tcp_stream_engine.open "],
            "event_namespaces": [" tcp_stream_engine "],
            "event_names": [" open "],
            "entity_ids": [" engine-* "],
            "source_paths": [" */tcp_stream_engine.py "],
            "source_funcs": [" open "],
        }
    )

    assert rule.action is PatternLogEventPolicyAction.ALLOW
    assert rule.events == ("tcp_stream_engine.open",)
    assert rule.event_namespaces == ("tcp_stream_engine",)
    assert rule.event_names == ("open",)
    assert rule.entity_ids == ("engine-*",)
    assert rule.source_paths == ("*/tcp_stream_engine.py",)
    assert rule.source_funcs == ("open",)


def test_b3_rule_from_mapping_treats_none_pattern_fields_as_empty() -> None:
    """Rule parser treats None pattern fields as empty tuples."""
    rule = PatternLogEventPolicyRuleConfig.from_mapping(
        {
            "action": "allow",
            "events": None,
            "event_namespaces": None,
            "event_names": None,
            "entity_ids": None,
            "source_paths": None,
            "source_funcs": None,
        }
    )

    assert rule.events == ()
    assert rule.event_namespaces == ()
    assert rule.event_names == ()
    assert rule.entity_ids == ()
    assert rule.source_paths == ()
    assert rule.source_funcs == ()


def test_b4_rule_from_mapping_parses_deny_action() -> None:
    """Rule parser parses deny action."""
    rule = PatternLogEventPolicyRuleConfig.from_mapping(
        {
            "action": "deny",
            "events": ["tcp_stream_engine.read"],
        }
    )

    assert rule.action is PatternLogEventPolicyAction.DENY
    assert rule.events == ("tcp_stream_engine.read",)


# -------------------------
# Group c: config validation
# -------------------------


@pytest.mark.parametrize(
    "value",
    [
        None,
        "true",
        1,
        0,
        [],
        {},
        object(),
    ],
)
def test_c1_config_from_mapping_rejects_non_bool_default_enabled(value: object) -> None:
    """Config parser rejects non-bool default_enabled."""
    with pytest.raises(TypeError, match="field 'default_enabled' must be bool"):
        PatternLogEventPolicyConfig.from_mapping({"default_enabled": value})


@pytest.mark.parametrize(
    "value",
    [
        "not-a-sequence",
        b"not-a-sequence",
        bytearray(b"not-a-sequence"),
        1,
        True,
        object(),
    ],
)
def test_c2_config_from_mapping_rejects_invalid_rules_container(value: object) -> None:
    """Config parser rejects invalid rules container."""
    with pytest.raises(TypeError, match="field 'rules' must be a sequence of mappings"):
        PatternLogEventPolicyConfig.from_mapping({"rules": value})


def test_c3_config_from_mapping_rejects_non_mapping_rule_item() -> None:
    """Config parser rejects non-mapping rule items."""
    with pytest.raises(TypeError, match="field 'rules' must contain only mappings"):
        PatternLogEventPolicyConfig.from_mapping(
            {
                "rules": [
                    {
                        "action": "allow",
                        "events": ["tcp_stream_engine.open"],
                    },
                    "bad-rule",
                ]
            }
        )


# -------------------------
# Group d: rule validation
# -------------------------


@pytest.mark.parametrize(
    ("data", "expected_error"),
    [
        ({}, TypeError),
        ({"action": None}, TypeError),
        ({"action": 1}, TypeError),
        ({"action": True}, TypeError),
        ({"action": ""}, ValueError),
        ({"action": "   "}, ValueError),
    ],
)
def test_d1_rule_from_mapping_rejects_missing_or_invalid_action(
    data: dict[str, object],
    expected_error: type[Exception],
) -> None:
    """Rule parser rejects missing or invalid action."""
    with pytest.raises(expected_error):
        PatternLogEventPolicyRuleConfig.from_mapping(data)


def test_d2_rule_from_mapping_rejects_unknown_action() -> None:
    """Rule parser rejects unknown action value."""
    with pytest.raises(ValueError, match="field 'action' must be one of:"):
        PatternLogEventPolicyRuleConfig.from_mapping({"action": "maybe"})


@pytest.mark.parametrize(
    "field_name",
    [
        "events",
        "event_namespaces",
        "event_names",
        "entity_ids",
        "source_paths",
        "source_funcs",
    ],
)
@pytest.mark.parametrize(
    "value",
    [
        "single-string-is-not-valid-here",
        b"bytes",
        bytearray(b"bytes"),
        1,
        True,
        object(),
    ],
)
def test_d3_rule_from_mapping_rejects_invalid_pattern_container(
    field_name: str,
    value: object,
) -> None:
    """Rule parser rejects invalid pattern container."""
    with pytest.raises(TypeError, match=f"field '{field_name}' must be a sequence of strings"):
        PatternLogEventPolicyRuleConfig.from_mapping(
            {
                "action": "allow",
                field_name: value,
            }
        )


@pytest.mark.parametrize(
    "field_name",
    [
        "events",
        "event_namespaces",
        "event_names",
        "entity_ids",
        "source_paths",
        "source_funcs",
    ],
)
def test_d4_rule_from_mapping_rejects_non_string_pattern_item(field_name: str) -> None:
    """Rule parser rejects non-string pattern items."""
    with pytest.raises(TypeError, match=f"field '{field_name}' must contain only strings"):
        PatternLogEventPolicyRuleConfig.from_mapping(
            {
                "action": "allow",
                field_name: ["valid", 123],
            }
        )


@pytest.mark.parametrize(
    "field_name",
    [
        "events",
        "event_namespaces",
        "event_names",
        "entity_ids",
        "source_paths",
        "source_funcs",
    ],
)
@pytest.mark.parametrize(
    "bad_item",
    [
        "",
        "   ",
    ],
)
def test_d5_rule_from_mapping_rejects_empty_pattern_item(
    field_name: str,
    bad_item: str,
) -> None:
    """Rule parser rejects empty pattern items."""
    with pytest.raises(ValueError, match=f"field '{field_name}' must not contain empty strings"):
        PatternLogEventPolicyRuleConfig.from_mapping(
            {
                "action": "allow",
                field_name: ["valid", bad_item],
            }
        )


# -------------------------
# Group e: policy construction and default behavior
# -------------------------


def test_e1_policy_rejects_invalid_config_type() -> None:
    """Policy rejects non-PatternLogEventPolicyConfig config."""
    with pytest.raises(
        TypeError,
        match="argument 'config' must be an instance of 'PatternLogEventPolicyConfig'",
    ):
        PatternLogEventPolicy(cast(PatternLogEventPolicyConfig, object()))


def test_e2_policy_returns_true_when_default_enabled_true_and_no_rules_exist() -> None:
    """Policy returns True when default is enabled and no rules exist."""
    policy = PatternLogEventPolicy(PatternLogEventPolicyConfig(default_enabled=True))

    assert policy.is_event_enabled(_event()) is True


def test_e3_policy_returns_false_when_default_enabled_false_and_no_rules_exist() -> None:
    """Policy returns False when default is disabled and no rules exist."""
    policy = PatternLogEventPolicy(PatternLogEventPolicyConfig(default_enabled=False))

    assert policy.is_event_enabled(_event()) is False


def test_e4_policy_returns_default_true_when_no_rule_matches() -> None:
    """Policy returns default True when no rule matches."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=True,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.DENY,
                    events=("other.*",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event()) is True


def test_e5_policy_returns_default_false_when_no_rule_matches() -> None:
    """Policy returns default False when no rule matches."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("other.*",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event()) is False


# -------------------------
# Group f: event key matching
# -------------------------


def test_f1_policy_matches_event_key_with_namespace_and_name() -> None:
    """Policy matches full event key from namespace and name."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("tcp_stream_engine.open",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event()) is True


def test_f2_policy_matches_event_key_without_namespace() -> None:
    """Policy matches event key when namespace is missing."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("open",),
                ),
            ),
        )
    )
    # noinspection PyArgumentEqualDefault
    assert policy.is_event_enabled(_event(event_namespace=None, event_name="open")) is True


def test_f3_policy_matches_event_key_wildcard() -> None:
    """Policy matches event key wildcard."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("tcp_stream_engine.*",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event(event_name="read")) is True


def test_f4_policy_does_not_match_event_key_when_event_name_is_none() -> None:
    """Policy does not match event key rule when event_name is None."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("*",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event(event_name=None)) is False


# -------------------------
# Group g: field-specific matching
# -------------------------


def test_g1_policy_matches_event_namespace() -> None:
    """Policy matches event_namespace."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    event_namespaces=("tcp_stream_engine",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event()) is True


def test_g2_policy_matches_event_name() -> None:
    """Policy matches event_name."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    event_names=("open",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event()) is True


def test_g3_policy_matches_entity_id() -> None:
    """Policy matches entity_id."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    entity_ids=("engine-*",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event(entity_id="engine-42")) is True


def test_g4_policy_matches_source_path() -> None:
    """Policy matches source_path."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    source_paths=("*/tcp_stream_engine.py",),
                ),
            ),
        )
    )

    # noinspection PyArgumentEqualDefault
    assert (
        policy.is_event_enabled(
            _event(source_path="src/mvx/networking/engines/tcp_stream_engine/tcp_stream_engine.py")
        )
        is True
    )


def test_g5_policy_matches_source_func() -> None:
    """Policy matches source_func."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    source_funcs=("open",),
                ),
            ),
        )
    )

    # noinspection PyArgumentEqualDefault
    assert policy.is_event_enabled(_event(source_func="open")) is True


def test_g6_policy_does_not_match_when_field_pattern_does_not_match() -> None:
    """Policy does not match when configured field pattern does not match."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    entity_ids=("other-*",),
                ),
            ),
        )
    )
    # noinspection PyArgumentEqualDefault
    assert policy.is_event_enabled(_event(entity_id="engine-1")) is False


# -------------------------
# Group h: multi-field matching
# -------------------------


def test_h1_policy_requires_all_non_empty_rule_fields_to_match() -> None:
    """Policy requires all non-empty rule fields to match."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("tcp_stream_engine.open",),
                    entity_ids=("engine-*",),
                    source_funcs=("open",),
                ),
            ),
        )
    )
    # noinspection PyArgumentEqualDefault
    assert policy.is_event_enabled(_event(entity_id="engine-1", source_func="open")) is True


def test_h2_policy_rejects_when_one_non_empty_rule_field_does_not_match() -> None:
    """Policy rejects when one non-empty rule field does not match."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("tcp_stream_engine.open",),
                    entity_ids=("engine-*",),
                    source_funcs=("close",),
                ),
            ),
        )
    )
    # noinspection PyArgumentEqualDefault
    assert policy.is_event_enabled(_event(entity_id="engine-1", source_func="open")) is False


def test_h3_empty_rule_matches_every_event() -> None:
    """Rule with no match fields matches every event."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event()) is True


# -------------------------
# Group i: nullable LogEventMeta fields
# -------------------------


@pytest.mark.parametrize(
    "rule",
    [
        PatternLogEventPolicyRuleConfig(
            action=PatternLogEventPolicyAction.ALLOW,
            events=("tcp_stream_engine.open",),
        ),
        PatternLogEventPolicyRuleConfig(
            action=PatternLogEventPolicyAction.ALLOW,
            event_namespaces=("tcp_stream_engine",),
        ),
        PatternLogEventPolicyRuleConfig(
            action=PatternLogEventPolicyAction.ALLOW,
            event_names=("open",),
        ),
        PatternLogEventPolicyRuleConfig(
            action=PatternLogEventPolicyAction.ALLOW,
            entity_ids=("engine-*",),
        ),
        PatternLogEventPolicyRuleConfig(
            action=PatternLogEventPolicyAction.ALLOW,
            source_paths=("*/tcp_stream_engine.py",),
        ),
        PatternLogEventPolicyRuleConfig(
            action=PatternLogEventPolicyAction.ALLOW,
            source_funcs=("open",),
        ),
    ],
)
def test_i1_rule_does_not_match_when_required_event_meta_field_is_none(
    rule: PatternLogEventPolicyRuleConfig,
) -> None:
    """Rule does not match when the required LogEventMeta field is None."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(rule,),
        )
    )

    assert (
        policy.is_event_enabled(
            _event(
                event_namespace=None,
                event_name=None,
                entity_id=None,
                source_path=None,
                source_func=None,
            )
        )
        is False
    )


def test_i2_empty_rule_still_matches_event_with_none_fields() -> None:
    """Empty rule matches even when nullable LogEventMeta fields are None."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                ),
            ),
        )
    )

    assert (
        policy.is_event_enabled(
            _event(
                event_namespace=None,
                event_name=None,
                entity_id=None,
                source_path=None,
                source_func=None,
            )
        )
        is True
    )


# -------------------------
# Group j: first matching rule wins
# -------------------------


def test_j1_policy_uses_first_matching_rule_allow_then_deny() -> None:
    """Policy uses first matching rule: allow before deny."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=False,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("tcp_stream_engine.*",),
                ),
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.DENY,
                    events=("tcp_stream_engine.open",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event()) is True


def test_j2_policy_uses_first_matching_rule_deny_then_allow() -> None:
    """Policy uses first matching rule: deny before allow."""
    policy = PatternLogEventPolicy(
        PatternLogEventPolicyConfig(
            default_enabled=True,
            rules=(
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.DENY,
                    events=("tcp_stream_engine.*",),
                ),
                PatternLogEventPolicyRuleConfig(
                    action=PatternLogEventPolicyAction.ALLOW,
                    events=("tcp_stream_engine.open",),
                ),
            ),
        )
    )

    assert policy.is_event_enabled(_event()) is False
