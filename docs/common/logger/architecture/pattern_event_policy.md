# Pattern event policy

```{contents} Contents:
:depth: 1
:local:
```

## Role in the logger architecture

`PatternLogEventPolicy` is a concrete implementation of the event policy boundary.

It does not introduce a new logging pipeline stage. It fills the existing event policy slot with a deterministic metadata matcher.

From the logger pipeline point of view, it has the same role as any other implementation of the event policy contract:

```text
LogEventMeta -> is_event_enabled() -> bool
```

`LogContext` builds `LogEventMeta`, passes it to the configured policy, and then either continues the pipeline or stops it.

```text
LogContext builds LogEventMeta
        |
        v
PatternLogEventPolicy checks metadata
        |
        +-- rejected -> stop
        |
        v
payload processor normalizes payload
        |
        v
LogContext builds LogEvent
        |
        v
sink receives LogEvent
```

The pattern policy is therefore part of logging-width selection. It decides whether an event is enabled. It does not decide how the payload is normalized or where the final event is delivered.

## Policy boundary

The policy boundary is intentionally small.

`PatternLogEventPolicy` receives only `LogEventMeta` and returns a boolean decision.

It does not:

* inspect the original payload;
* inspect the normalized payload;
* create `LogEvent`;
* call a sink;
* format output;
* mutate `LogContext`;
* change event metadata.

This keeps the component inside the same architectural boundary as any other event policy implementation.

The policy answers only one question:

```text
Should this event be allowed into the log?
```

Everything after that belongs to other components. Payload shape belongs to the payload processor. Event delivery belongs to the sink. Context ownership and pipeline orchestration belong to `LogContext`.

## Configuration model

Internally, the pattern policy is a small rule engine built from typed configuration objects.

The top-level configuration contains:

```text
PatternLogEventPolicyConfig
    default_enabled
    rules
```

`default_enabled` is the fallback decision.

`rules` is an ordered sequence of rule configurations.

Each rule contains:

```text
PatternLogEventPolicyRuleConfig
    action
    events
    event_namespaces
    event_names
    entity_ids
    source_paths
    source_funcs
```

`action` defines the decision returned when the rule matches.

The pattern groups define which metadata values the rule should match.

This model is deliberately flat. There are no nested rule trees, no inherited rule groups, no expression language, and no hidden scoring model.

The policy is a read-only decision table:

```text
default decision
ordered rules
    rule action
    metadata pattern groups
```

## Runtime evaluation

At runtime, evaluation is linear and deterministic.

Conceptually, the policy does this:

```python
for rule in rules:
    if rule_matches(rule, event):
        return rule.action is ALLOW

return default_enabled
```

Rules are checked in the order in which they appear in the configuration.

The first matching rule wins.

If no rule matches, the policy returns `default_enabled`.

This makes a policy decision explainable by one of two facts:

```text
rule N matched and returned its action
```

or:

```text
no rule matched, so the default decision was used
```

There is no second pass and no attempt to find a better match later in the rule list.

## Why rule order defines priority

`PatternLogEventPolicy` uses explicit rule order as the priority mechanism.

It does not try to calculate which rule is more specific.

That is intentional.

Specificity is ambiguous once several metadata dimensions exist. A rule matching an event name and entity id is not universally more or less specific than a rule matching source path and source function. A longer wildcard pattern is not always a better priority signal than a rule with more metadata fields.

Instead of embedding such judgement into the library, the policy keeps priority visible:

```text
earlier matching rule wins
```

The configuration author controls precedence by ordering rules.

This makes the matcher predictable and avoids a separate precedence language.

## Rule matching semantics

A rule matches an event when all non-empty pattern groups in that rule match the corresponding metadata.

Pattern groups inside one rule are combined with logical `and`.

```text
events match
AND event_namespaces match
AND event_names match
AND entity_ids match
AND source_paths match
AND source_funcs match
```

Empty pattern groups are not restrictions.

This means a rule may describe only the metadata dimensions it cares about. A rule with only `event_namespaces` does not restrict event name, entity id, source path, or source function.

Patterns inside one group are alternatives.

```text
event_names = ("started", "finished")
```

means:

```text
event_name matches "started"
OR
event_name matches "finished"
```

The complete matching model is therefore:

```text
rules are ordered
pattern groups inside a rule are AND
patterns inside one group are OR
empty groups do not restrict the rule
```

A rule with no non-empty pattern groups matches every event. Architecturally, this is just a catch-all rule. Because the first matching rule wins, such a rule determines the result immediately and prevents later rules from being checked.

## Missing metadata values

Some metadata values may be absent.

A restricted pattern group cannot match a missing metadata value.

For example, if a rule has `entity_ids`, but the event metadata has no `entity_id`, that rule does not match.

The same principle applies to source metadata.

This keeps matching strict: a rule that restricts a metadata dimension only matches events that actually provide that dimension.

## Composed event key

The `events` pattern group is special because it does not correspond to a single stored field in `LogEventMeta`.

It matches a composed event key derived from:

```text
event_namespace
event_name
```

When both values are present, the composed key is:

```text
<event_namespace>.<event_name>
```

When the namespace is absent, the composed key is the event name.

When the event name is absent, the composed key is absent and the `events` group cannot match.

This gives the policy a compact matching surface for event identity without changing the metadata model itself.

The separate fields still remain available:

```text
event_namespaces
event_names
```

The composed key exists beside them. It is useful when namespace and name should be treated as one logical event identifier.

## Pattern matching

Pattern groups use shell-style matching.

The matcher treats the value as a plain string. Dots have no special meaning. They are only part of the naming convention used by the application or library.

The matching is case-sensitive.

The policy does not normalize event names, namespaces, entity ids, source paths, or function names. Naming consistency is the responsibility of the code that emits events.

This keeps the matcher small and predictable. It does not become a separate query language.

## Mapping-to-config boundary

`PatternLogEventPolicyConfig` and `PatternLogEventPolicyRuleConfig` can be created directly as typed objects.

They can also be created from mappings.

The mapping parser belongs to the configuration boundary, not to the runtime decision path.

The architectural flow is:

```text
external mapping
        |
        v
validation and conversion
        |
        v
typed config objects
        |
        v
runtime event matching
```

This separation has an important consequence: runtime matching works with already validated configuration.

The policy does not repeatedly validate mapping structure while events are being checked. It reads typed config objects and evaluates rules.

Configuration mistakes are caught when the config is built, not silently discovered during unrelated event emission.

## Runtime state

`PatternLogEventPolicy` has no mutable decision state.

The policy stores its configuration and reads it during event checks.

The configuration objects are immutable. The rule list is therefore a stable decision table for the lifetime of the policy instance.

If an application needs different rules, it should create a new configuration and a new policy instance, then assign that policy to the relevant context.

The existing policy does not need an internal update protocol.

## Thread-safety

Because the policy has no mutable runtime state, event checks are read-only.

This makes a policy instance naturally safe to share as a decision object.

Thread-safety of context lookup, context configuration, and sink delivery belongs to the surrounding logger infrastructure. The pattern policy itself only performs metadata matching against immutable configuration.

## Relationship with custom policies

`PatternLogEventPolicy` is interchangeable with custom policy implementations at the event policy boundary.

`LogContext` depends on the policy contract, not on the concrete pattern implementation.

From `LogContext` point of view, these are equivalent:

```text
custom policy       -> is_event_enabled(LogEventMeta) -> bool
pattern policy      -> is_event_enabled(LogEventMeta) -> bool
```

This keeps the ready-to-use implementation optional.

Applications can use `PatternLogEventPolicy` when metadata patterns are enough. They can replace it with a custom policy when event selection requires domain-specific logic.

The rest of the logging pipeline does not change.

## Relationship with payload processing

`PatternLogEventPolicy` and payload processing are separate dimensions.

The pattern policy controls logging width:

```text
which events are allowed
```

The payload processor controls logging depth:

```text
how accepted payload values are normalized
```

The pattern policy runs before payload normalization. This preserves the early-rejection property of event policies.

If the event is rejected, payload normalization does not happen.

This is why the policy does not inspect payload. Inspecting payload would couple width selection to depth processing and could force expensive payload construction even for rejected events.

## Relationship with sinks

A sink receives completed `LogEvent` objects.

By the time a sink is called, the event has already passed the event policy and payload normalization.

`PatternLogEventPolicy` is therefore not a sink filter.

This distinction matters because filtering at the sink boundary happens too late to avoid earlier work. The pattern policy rejects disabled events before final event creation and before delivery.

The policy selects events. The sink delivers events.

## Complexity

Runtime evaluation is linear in the configured rule list.

For each rule, the policy checks the non-empty pattern groups. For each non-empty group, it checks the configured patterns until one matches or all fail.

Conceptually:

```text
rules * non-empty groups * patterns per group
```

This is appropriate for configuration-sized rule sets.

The component is not designed as a high-volume dynamic query engine. If event selection requires a more specialized data structure, caching strategy, or external decision source, that belongs in a custom event policy implementation.

## Design summary

`PatternLogEventPolicy` is a deterministic metadata matcher for the event policy boundary.

Its design follows several architectural constraints:

* event selection happens before payload normalization;
* the policy receives only `LogEventMeta`;
* the runtime decision is a boolean;
* rule priority is explicit in rule order;
* matching is based on simple pattern groups;
* external mappings are validated before runtime evaluation;
* runtime state is read-only;
* custom policies remain interchangeable through the same contract.

The result is a ready-to-use configuration-driven policy that preserves the existing logger architecture instead of adding a second filtering model.
