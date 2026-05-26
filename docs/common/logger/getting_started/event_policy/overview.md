# Event policy

```{contents} Contents:
:depth: 1
:local:
```

An event policy controls logging width.

It decides whether a particular event is allowed to enter the log.

In the logging pipeline, event policy is applied after `LogContext` builds `LogEventMeta` and before payload normalization starts.

```text
LogContext builds LogEventMeta
        |
        v
event policy checks LogEventMeta
        |
        v
accepted event -> payload processor
rejected event -> pipeline stops
```

If the event is rejected, no payload normalization is performed, no `LogEvent` is created, and nothing is passed to a sink.

## Why event policy exists

Event policy exists to make logging width configurable without changing the code that emits events.

A context may emit many possible events, but different runtime situations require different logging width. During normal operation, the application may need only a small, stable set of events. During troubleshooting, the same application or layer may need much wider diagnostic logging.

By assigning different event policy implementations, an application can control this width from configuration, environment, runtime mode, or layer-specific settings.

For example:

* a normal mode policy may allow only high-level operational events;
* a diagnostic policy may allow additional namespaces or event names;
* a troubleshooting policy may enable events for a specific entity id or source location.

This keeps event selection outside business code. The code can keep emitting the same events, while the configured policy decides which of them are allowed into the log in a particular situation.

Event policy also keeps event selection separate from payload preparation and delivery.

A logging system often needs to answer a simple question before doing any heavier work:

> Should this event be logged at all?

That decision should not require payload normalization, object serialization, sink delivery, or formatter work. It should be made early, using only stable event metadata.

This is why event policy is placed before the payload processor in the pipeline.

It lets a context restrict its own logging area by namespace, event name, entity id, or source location, without forcing the payload processor or sink to know why the event was allowed or rejected.

Event policy is also intentionally local to the context where it is configured. A logging namespace usually represents a specific area or layer of an application. Higher-level layers should not need to know the internal event structure of lower-level layers.

For that reason, event policy is not a centralized global rule that automatically flows down the context hierarchy. It is a local selection mechanism inside the responsibility area of a particular context.

## What event policy receives

An event policy receives `LogEventMeta`.

`LogEventMeta` contains the metadata needed to identify the event:

* event namespace;
* event name;
* entity id;
* source path;
* source line;
* source function.

The policy uses this metadata to decide whether the event should be logged.

For example, a policy may allow events from one namespace, reject a noisy event name, enable diagnostics for one entity, or select events emitted from a particular source location.

## What event policy does not inspect

Event policy does not inspect the payload.

It also does not inspect:

* normalized payload;
* logging level;
* event type;
* timestamp;
* sink;
* destination.

This is intentional.

Event policy is responsible only for event selection. It answers the question:

> Should this event be allowed into the log?

It does not answer how detailed the payload should be or where the event should be delivered.

Payload depth is handled by the payload processor. Delivery is handled by the sink.

## Accepted and rejected events

If the event policy accepts the event, the pipeline continues.

The payload is passed to the configured payload processor, then `LogContext` creates the final `LogEvent`, and the event is delivered to the configured sink.

If the event policy rejects the event, the pipeline stops immediately.

In that case:

* the original payload is not normalized;
* the payload processor is not called;
* the final `LogEvent` is not created;
* the sink does not receive anything.

This keeps disabled events cheap. Expensive payload preparation does not happen for events that are not allowed by policy.

## Context-level responsibility

An event policy belongs to the context where it is configured.

Unlike settings such as sink, payload processor, and logging error handling policy, event policy is not inherited from the parent context.

This is an important architectural boundary.

A logging namespace usually represents a specific area or layer of an application. Higher-level layers should not need to know the internal event structure of lower-level layers.

For that reason, event policy is not a centralized global decision mechanism that automatically flows down the context hierarchy. It is a local decision mechanism inside the responsibility area of a particular context.

A parent context may configure its own policy for its own logging area. A child context may configure a different policy for its own area. If a context does not have a policy, its events are considered enabled by default.

## Default behavior

If no event policy is configured for a context, events emitted through that context are allowed.

In other words, the absence of a policy means:

```text
allow the event
```

A policy is needed only when a context should restrict which events are allowed into the log.

## Event policy as a contract

In `MVX Logger`, event policy is a protocol-level component.

A policy object must provide one method:

```python
from mvx.common.logger import LogEventMeta

def is_event_enabled(event: LogEventMeta) -> bool:
    ...
```

The method returns:

* `True` — the event is accepted and the pipeline continues;
* `False` — the event is rejected and the pipeline stops.

The exact implementation is up to the application or library.

## What to remember

* Event policy controls logging width.
* It receives `LogEventMeta` and decides whether the event is allowed.
* It does not inspect payload, level, event type, timestamp, sink, or destination.
* It is applied before payload normalization.
* If the event is rejected, payload normalization and sink delivery do not happen.
* Event policy is local to the context where it is configured and is not inherited from the parent context.

```{toctree}
:maxdepth: 1
:caption: What to read next

event_metadata
writing_policy
context_policy
```