# Pattern event policy

This page documents the ready-to-use pattern event policy API.

Pattern event policy is the built-in implementation of the event policy contract.

It belongs to the logging-width layer:

```text
LogEventMeta -> policy decision -> accepted / rejected
```

The policy receives event metadata, checks it against ordered pattern rules, and returns whether the event is enabled.

It does not inspect payload data, does not normalize payload values, does not create `LogEvent`, and does not deliver anything to a sink.

## Public API

The pattern event policy API contains four public objects:

```text
PatternLogEventPolicyAction
PatternLogEventPolicyRuleConfig
PatternLogEventPolicyConfig
PatternLogEventPolicy
```

`PatternLogEventPolicyAction` defines the rule decision.

`PatternLogEventPolicyRuleConfig` describes one ordered rule.

`PatternLogEventPolicyConfig` describes the complete policy configuration.

`PatternLogEventPolicy` applies the configuration to `LogEventMeta`.

```{eval-rst}
.. autoenum:: mvx.common.logger.PatternLogEventPolicyAction

.. autoclass:: mvx.common.logger.PatternLogEventPolicyRuleConfig
   :members:
   :member-order: bysource
   :class-doc-from: both

.. autoclass:: mvx.common.logger.PatternLogEventPolicyConfig
   :members:
   :member-order: bysource
   :class-doc-from: both

.. autoclass:: mvx.common.logger.PatternLogEventPolicy
   :members:
   :member-order: bysource
   :class-doc-from: both
```

## Rule model

A pattern policy is an ordered decision table.

Each rule has:

```text
action
pattern groups
```

The action is returned when the rule matches.

The pattern groups select which metadata values the rule applies to.

Inside one rule, non-empty pattern groups are combined as logical `and`.

Inside one pattern group, patterns are alternatives.

```text
rules
    checked in order

fields inside one rule
    AND

patterns inside one field
    OR
```

The first matching rule decides the result.

If no rule matches, `PatternLogEventPolicyConfig.default_enabled` is returned.

## Pattern fields

A rule may match:

```text
events
event_namespaces
event_names
entity_ids
source_paths
source_funcs
```

`events` matches the composed event key built from event namespace and event name.

The other fields match the corresponding `LogEventMeta` fields.

Pattern matching uses shell-style patterns.

## Mapping configuration

Both configuration dataclasses support mapping-based construction:

```text
PatternLogEventPolicyRuleConfig.from_mapping()
PatternLogEventPolicyConfig.from_mapping()
```

This is intended for configuration loaded from dictionaries, files, environment-specific settings, or another configuration layer.

Mapping data is validated and converted into typed immutable configuration objects before runtime event matching starts.

## Minimal object-form example

```python
from mvx.common.logger import (
    PatternLogEventPolicy,
    PatternLogEventPolicyAction,
    PatternLogEventPolicyConfig,
    PatternLogEventPolicyRuleConfig,
)

policy = PatternLogEventPolicy(
    PatternLogEventPolicyConfig(
        default_enabled=False,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.ALLOW,
                event_namespaces=("my_app.worker",),
            ),
        ),
    )
)
```

## Minimal mapping-form example

```python
from mvx.common.logger import (
    PatternLogEventPolicy,
    PatternLogEventPolicyConfig,
)

config = PatternLogEventPolicyConfig.from_mapping(
    {
        "default_enabled": False,
        "rules": [
            {
                "action": "allow",
                "event_namespaces": ["my_app.worker"],
            },
        ],
    }
)

policy = PatternLogEventPolicy(config)
```
