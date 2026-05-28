# Configuring event policy

```{contents} Contents:
:depth: 1
:local:
```

Event policy is configured on a `LogContext`.

It controls which events are allowed for that specific context.

The policy is checked after `LogContext` builds `LogEventMeta` and before payload normalization starts.

```text
LogContext builds LogEventMeta
        |
        v
context event policy checks metadata
        |
        v
accepted event -> payload processor
rejected event -> pipeline stops
```

If the policy rejects the event, the payload is not normalized, the final `LogEvent` is not created, and nothing is passed to the sink.

## Configuring policy on context creation

An event policy can be passed when the context is configured.

```python
from dataclasses import dataclass

from mvx.common.logger import LogEventMeta, configure_log_context


@dataclass(frozen=True, slots=True)
class EventNamePolicy:
    allowed_events: frozenset[str]

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.event_name in self.allowed_events


policy = EventNamePolicy(
    allowed_events=frozenset({
        "started",
        "stopped",
        "operation.failed",
    }),
)

ctx = configure_log_context(
    "my_app.worker",
    event_policy=policy,
)
```

After that, events emitted through this context are checked by the configured policy.

Events accepted by the policy continue through the pipeline. Events rejected by the policy are skipped before payload normalization.

## Reconfiguring policy later

A context can also receive a new event policy later.

```python
from dataclasses import dataclass

from mvx.common.logger import LogEventMeta, configure_log_context


@dataclass(frozen=True, slots=True)
class NamespacePolicy:
    namespace: str

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.event_namespace == self.namespace


ctx = configure_log_context("my_app.worker")

ctx.set_event_policy(
    NamespacePolicy(namespace="my_app.worker"),
)
```

After this call, the context uses the new policy for subsequent logging calls.

The code that emits events does not need to change. It keeps emitting the same event names and payloads, while the configured policy decides which events are allowed in the current situation.

## Resetting policy

A context policy can be reset.

```python
ctx.reset_event_policy()
```

After reset, the context has no local event policy.

For event policy, this means that events emitted through this context are allowed by default.

```text
no event policy -> allow event
```

Unlike some other context settings, resetting event policy is also valid for the root context. A context without an event policy simply does not restrict events at that level.

## No parent inheritance

Event policy is not inherited from the parent context.

This is different from settings such as sink, payload processor, and logging error handling policy.

If a context has no event policy, it does not ask its parent for one. It allows its own events by default.

```text
parent context has policy
child context has no policy
        |
        v
child context allows its own events by default
```

This behavior is intentional.

A logging namespace usually represents a specific application area or layer. Higher-level layers should not need to know the internal event structure of lower-level layers.

If event policy were inherited automatically, policy rules from a parent layer could accidentally depend on event names, entities, or source locations that belong to a lower-level layer. That would make logging configuration leak implementation details across layer boundaries.

Keeping event policy local protects this boundary.

A parent context can define policy for its own area. A child context can define policy for its own area. They do not need to share the same event-selection rules.

## Local responsibility

Event policy is not a global centralized controller for all descendant contexts.

It is a local event-selection mechanism inside the responsibility area of the context where it is configured.

This allows each layer or component to expose its own logging-width configuration.

For example:

* the application layer may log only high-level lifecycle events;
* the worker layer may expose diagnostic events for background jobs;
* the networking layer may expose connection-level troubleshooting events;
* a specific context may enable events only for one `entity_id`.

Each policy controls only the area where it is configured.

## Comparing inherited and local settings

Some context settings are inherited from the parent context when they are not set locally.

Examples:

* `log_sink`;
* `payload_processor`;
* `log_error_handling_policy`.

Event policy is different.

It is always local to the context.

```text
log_sink                  -> may be inherited
payload_processor         -> may be inherited
log_error_handling_policy -> may be inherited
event_policy              -> local only
```

This distinction is important when configuring a hierarchy of logging contexts.

A parent sink may be shared by many child contexts. A parent payload processor may define common payload depth rules. But event selection belongs to the context that knows its own event structure.

## What to remember

* Event policy is configured on a `LogContext`.
* It is checked before payload normalization.
* A context with no event policy allows events by default.
* Event policy can be set with `set_event_policy()` and reset with `reset_event_policy()`.
* Event policy is not inherited from the parent context.
* Each context controls event selection only inside its own responsibility area.
