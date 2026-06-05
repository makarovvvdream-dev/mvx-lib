# Pattern event policy

```{contents} Contents:
:depth: 1
:local:
```

`PatternLogEventPolicy` is the ready-to-use event policy implementation provided by `MVX Logger`.

It controls logging width by matching event metadata against ordered pattern rules.

Use it when event selection should be configurable, but the application does not need a custom policy class.

## Why pattern policy exists

Event policy answers one question:

> Should this event be allowed into the log?

A custom policy can answer this question with arbitrary Python code. That is useful when event selection depends on domain-specific rules.

However, many applications need a simpler and more common form of event selection:

* allow events from selected namespaces;
* deny noisy event names;
* enable diagnostic events for a specific entity;
* suppress events from a particular module;
* enable or deny events from a particular function;
* define a default decision and override it with a few exceptions.

These decisions are usually configuration decisions, not business logic.

`PatternLogEventPolicy` exists for this middle ground. It lets logging width be described as data: an ordered list of allow/deny rules.

The code keeps emitting the same events. The policy decides which of them are allowed.

## Basic idea

A pattern policy has two parts:

* a default decision;
* an ordered list of rules.

The default decision is used when no rule matches.

Each rule says:

* what metadata patterns should match;
* whether the matching event should be allowed or denied.

Rules are checked from top to bottom.

The first matching rule wins.

```text
event metadata
      |
      v
rule 1 matches? -> yes -> allow or deny
      |
      no
      v
rule 2 matches? -> yes -> allow or deny
      |
      no
      v
rule 3 matches? -> yes -> allow or deny
      |
      no
      v
use default decision
```

This makes rule order important.

Specific rules should usually be placed before broader rules.

## Creating a policy

A policy is created from `PatternLogEventPolicyConfig`.

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
    ),
)
```

This policy disables events by default and allows only events whose event namespace matches `my_app.worker`.

The policy can then be assigned to a log context:

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context(
    "my_app.worker",
    event_policy=policy,
)
```

Events emitted through this context are checked by the pattern policy before payload normalization starts.

## Default decision

`default_enabled` controls what happens when no rule matches.

```python
PatternLogEventPolicyConfig(
    default_enabled=True,
    rules=(),
)
```

This means:

```text
allow by default
```

With this configuration, rules are mainly used to deny selected events.

```python
PatternLogEventPolicyConfig(
    default_enabled=False,
    rules=(),
)
```

This means:

```text
deny by default
```

With this configuration, rules are mainly used to allow selected events.

Both styles are valid.

Use `default_enabled=True` when the context should normally log its events and only a few events should be suppressed.

Use `default_enabled=False` when the context should normally stay quiet and only selected events should be enabled.

## Rule actions

Each rule has an action.

The action may be:

* `allow`;
* `deny`.

When a rule matches, its action becomes the policy decision.

```python
PatternLogEventPolicyRuleConfig(
    action=PatternLogEventPolicyAction.DENY,
    event_names=("debug.*",),
)
```

This rule denies matching events.

```python
PatternLogEventPolicyRuleConfig(
    action=PatternLogEventPolicyAction.ALLOW,
    event_namespaces=("my_app.diagnostics",),
)
```

This rule allows matching events.

## What a rule can match

A rule can match these fields:

* `events`;
* `event_namespaces`;
* `event_names`;
* `entity_ids`;
* `source_paths`;
* `source_funcs`.

Each field contains one or more shell-style patterns.

```python
PatternLogEventPolicyRuleConfig(
    action=PatternLogEventPolicyAction.ALLOW,
    event_namespaces=("my_app.worker.*",),
    event_names=("job.*",),
)
```

This rule matches events whose namespace matches `my_app.worker.*` and whose event name matches `job.*`.

## Matching inside a rule

Inside one rule, all non-empty fields must match.

This means that rule fields are combined with logical `and`.

```python
PatternLogEventPolicyRuleConfig(
    action=PatternLogEventPolicyAction.ALLOW,
    event_namespaces=("my_app.worker",),
    event_names=("job.started",),
)
```

This rule means:

```text
event_namespace matches "my_app.worker"
AND
event_name matches "job.started"
```

If either field does not match, the whole rule does not match.

Inside one field, patterns are combined with logical `or`.

```python
PatternLogEventPolicyRuleConfig(
    action=PatternLogEventPolicyAction.ALLOW,
    event_names=("job.started", "job.finished"),
)
```

This means:

```text
event_name matches "job.started"
OR
event_name matches "job.finished"
```

So the general rule is:

```text
fields -> AND
patterns inside one field -> OR
rules -> first matching rule wins
```

## Composed event key

The `events` field matches a composed event key.

The composed key is built from event namespace and event name.

If both namespace and name are present, the key is:

```text
<event_namespace>.<event_name>
```

For example:

```text
my_app.worker.job.started
```

If the namespace is absent, the key is just the event name.

```text
job.started
```

This is useful when event selection should be expressed as one compact pattern.

```python
PatternLogEventPolicyRuleConfig(
    action=PatternLogEventPolicyAction.ALLOW,
    events=("my_app.worker.job.*",),
)
```

Use `events` when the namespace and event name should be treated as one logical event identifier.

Use `event_namespaces` and `event_names` when it is clearer to match them separately.

## Pattern syntax

Patterns use shell-style wildcard matching.

Common examples:

```text
*              matches anything
my_app.*      matches names that start with "my_app."
*.failed      matches names that end with ".failed"
job.?         matches "job." followed by one character
```

Pattern matching is case-sensitive.

For clarity, event names and namespaces should usually use one consistent lowercase style.

## Allow selected namespaces

A common restrictive configuration is to deny by default and allow selected namespaces.

```python
policy = PatternLogEventPolicy(
    PatternLogEventPolicyConfig(
        default_enabled=False,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.ALLOW,
                event_namespaces=("my_app.api", "my_app.worker"),
            ),
        ),
    ),
)
```

This policy allows events from `my_app.api` and `my_app.worker`.

Everything else is denied by default.

## Deny noisy events

A common permissive configuration is to allow by default and deny selected noisy events.

```python
policy = PatternLogEventPolicy(
    PatternLogEventPolicyConfig(
        default_enabled=True,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.DENY,
                event_names=("heartbeat", "cache.tick"),
            ),
        ),
    ),
)
```

This policy allows most events but suppresses `heartbeat` and `cache.tick`.

## Specific rules before broad rules

Because the first matching rule wins, more specific rules should usually come first.

```python
policy = PatternLogEventPolicy(
    PatternLogEventPolicyConfig(
        default_enabled=False,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.DENY,
                events=("my_app.worker.job.noisy",),
            ),
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.ALLOW,
                events=("my_app.worker.job.*",),
            ),
        ),
    ),
)
```

This policy allows most worker job events, but denies one noisy event.

If the rules were reversed, the broad allow rule would match first and the deny rule would never be reached for that event.

## Entity-specific diagnostics

`entity_ids` can be used when only one entity should be diagnosed.

```python
policy = PatternLogEventPolicy(
    PatternLogEventPolicyConfig(
        default_enabled=False,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.ALLOW,
                event_namespaces=("my_app.connection",),
                entity_ids=("connection-42",),
            ),
        ),
    ),
)
```

This policy allows events from `my_app.connection` only for entity id `connection-42`.

This is useful for troubleshooting one connection, job, request, user-visible operation, or domain object without enabling the same diagnostic width for all entities.

## Source-based selection

A rule can also match source metadata.

```python
policy = PatternLogEventPolicy(
    PatternLogEventPolicyConfig(
        default_enabled=False,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.ALLOW,
                source_paths=("*/my_app/networking/*",),
                source_funcs=("connect", "disconnect"),
            ),
        ),
    ),
)
```

This allows events emitted from matching source paths and source functions.

Source-based rules are useful for diagnostics, but they are more tightly coupled to code layout than namespace-based rules.

Prefer event namespace and event name for stable application configuration.

Use source path and source function when the source location itself is the intended diagnostic boundary.

## Configuration from mappings

`PatternLogEventPolicyConfig` can be built from a mapping.

This is useful when policy configuration is loaded from a file, environment-specific settings, or another configuration layer.

```python
from mvx.common.logger import PatternLogEventPolicy, PatternLogEventPolicyConfig

config = PatternLogEventPolicyConfig.from_mapping(
    {
        "default_enabled": False,
        "rules": [
            {
                "action": "allow",
                "event_namespaces": ["my_app.api", "my_app.worker"],
            },
            {
                "action": "deny",
                "event_names": ["*.debug"],
            },
        ],
    }
)

policy = PatternLogEventPolicy(config)
```

The mapping form uses the same concepts as the Python object form:

* `default_enabled` defines the fallback decision;
* `rules` defines ordered rules;
* `action` defines `allow` or `deny`;
* pattern fields define what the rule should match.

## Mapping schema

A policy mapping may contain:

```python
{
    "default_enabled": bool,
    "rules": [
        {
            "action": "allow" | "deny",
            "events": list[str],
            "event_namespaces": list[str],
            "event_names": list[str],
            "entity_ids": list[str],
            "source_paths": list[str],
            "source_funcs": list[str],
        },
    ],
}
```

`default_enabled` is optional and defaults to `True`.

`rules` is optional and defaults to an empty sequence.

Inside a rule, `action` is required.

All pattern fields are optional. Missing pattern fields are treated as empty.

An empty pattern field does not restrict the rule.

For example, this rule matches any event name in the selected namespace:

```python
{
    "action": "allow",
    "event_namespaces": ["my_app.worker"],
}
```

This rule has no `event_names` restriction, so any event name may match.

## Empty rules

A rule with only an action and no pattern fields matches every event.

```python
PatternLogEventPolicyRuleConfig(
    action=PatternLogEventPolicyAction.DENY,
)
```

This is valid, but it should be used carefully.

Because the first matching rule wins, such a rule stops all later rules from being checked.

For example, this policy denies everything:

```python
PatternLogEventPolicyConfig(
    default_enabled=True,
    rules=(
        PatternLogEventPolicyRuleConfig(
            action=PatternLogEventPolicyAction.DENY,
        ),
    ),
)
```

The rule matches every event and returns `deny`.

## Choosing between pattern policy and custom policy

Use `PatternLogEventPolicy` when event selection can be described by metadata patterns.

Good use cases include:

* application mode configuration;
* diagnostic namespace selection;
* noisy event suppression;
* entity-specific troubleshooting;
* module or function diagnostics.

Use a custom event policy when the decision requires logic that is not naturally a metadata pattern.

Examples:

* a decision based on an external feature flag service;
* a decision based on a runtime object registry;
* a decision that combines event metadata with domain-specific state;
* a decision that needs custom caching or rate-limiting rules.

Even when a custom policy is used, it follows the same contract:

```python
def is_event_enabled(event: LogEventMeta) -> bool:
    ...
```

The rest of the logging pipeline does not need to know whether the decision came from `PatternLogEventPolicy` or from a custom policy.

## Practical rule design

A good pattern policy usually follows one of two shapes.

The first shape is permissive:

```text
allow by default
deny a few noisy or unwanted events
```

This is useful when logging is normally enabled and only selected events need to be suppressed.

The second shape is restrictive:

```text
deny by default
allow selected namespaces, event names, entities, or sources
```

This is useful for production systems where diagnostic width should be explicitly enabled.

Avoid mixing too many broad allow and deny rules without a clear order. The policy remains easiest to understand when the rule list reads like a small decision table:

```text
1. deny this special noisy case
2. allow this diagnostic area
3. allow this operational area
4. fall back to default
```

## What pattern policy does not do

`PatternLogEventPolicy` does not inspect payload.

It also does not inspect:

* normalized payload;
* logging level;
* event type;
* timestamp;
* sink;
* destination.

It only receives `LogEventMeta`, like any other event policy.

This keeps it cheap and predictable. Disabled events are rejected before payload normalization, before final `LogEvent` creation, and before sink delivery.

## What to remember

* `PatternLogEventPolicy` is the ready-to-use event policy implementation.
* It controls logging width through ordered allow/deny rules.
* Rules match `LogEventMeta` fields using shell-style patterns.
* Rules are checked in order, and the first matching rule wins.
* Non-empty fields inside one rule must all match.
* Multiple patterns inside one field are alternatives.
* `default_enabled` is used when no rule matches.
* The policy can be configured through Python objects or through mappings.
* Pattern policy is best for configuration-driven event selection.
* Custom policies are still available for domain-specific logic.
