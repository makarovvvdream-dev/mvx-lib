# Payload depth

```{contents} Contents:
:depth: 1
:local:
```

Payload depth means how much data an accepted event carries in its payload and how that data is represented.

It is different from logging width.

Logging width answers:

```text
Which events should be logged?
```

Payload depth answers:

```text
How much data should accepted events contain?
```

Event policy controls width. Payload processor controls depth.

```text
event policy accepts event
        |
        v
payload processor applies depth rules
        |
        v
LogEvent receives normalized payload
```

## Why payload depth exists

Payload depth exists because the same event may need different amounts of detail in different runtime situations.

During normal operation, compact payloads are usually enough. They should be stable, readable, and small.

During diagnostics or troubleshooting, the same application layer may need more detail: richer object state, longer values, larger collections, or more explicit error data.

This should not require changing the code that emits events.

The code can keep passing the same payload structure. The configured payload processor decides how that raw data becomes the final log-ready payload.

When a payload processor is configured at the root context, depth rules can be applied application-wide through context inheritance.

When a specific layer or component needs different depth rules, it can receive its own payload processor.

## Depth is a policy, not formatting

Payload depth is not about final output formatting.

It does not decide:

* whether the event is written as text or JSON;
* which file, stream, or backend receives the event;
* which formatter is used by a sink.

Those decisions belong to sinks and delivery configuration.

Payload depth is applied before the final `LogEvent` reaches a sink.

At that stage, the processor prepares the payload data itself: how much of it is included, how objects are represented, and how large values are controlled.

## Common depth decisions

Different payload processor implementations may use different rules.

In general, a depth policy may decide:

* whether an object should be represented compactly or in detail;
* how much of a collection should be included;
* how long string values may be;
* whether nested structures should be expanded or replaced with placeholders;
* how domain objects expose their logging representation;
* how large or sensitive values should be shortened or masked.

These decisions are made after the event has already been accepted.

If event policy rejects the event, depth rules are not applied at all.

## Default processor behavior

`MVX Logger` includes a default implementation, `LogPayloadProcessor`.

It implements payload depth through concrete configuration settings and normalization rules.

At a high level, the default processor can:

* request compact, normal, or more detailed representations through verbosity level;
* limit long strings;
* limit large collections;
* normalize primitive values, mappings, lists, tuples, enums, bytes-like values, and unsupported objects;
* use object-provided logging payloads;
* use external adapters for object representation.

The exact behavior of these mechanisms is described in the following implementation-focused articles.

This article only defines the role of payload depth in the logging pipeline.

## Depth and application configuration

Payload depth is often tied to application configuration.

For example:

* normal mode may use compact payloads and strict size limits;
* diagnostic mode may use more detailed object representation;
* troubleshooting mode may allow larger collections for a specific layer;
* tests may use a processor that keeps payloads predictable and easy to assert.

The important point is that these choices can be made by configuring the payload processor, not by changing every logging call.

## Local overrides

A root payload processor can define a common depth policy for the whole application.

Child contexts inherit that processor by default.

A specific context can override it by receiving its own payload processor.

This makes the common case simple while still allowing local customization for layers that need special payload rules.

For example, an application may use a compact default processor globally, while a networking layer receives a processor configured for more detailed diagnostic payloads.

## What to remember

* Payload depth controls how much data accepted events carry and how that data is represented.
* It is handled by the configured payload processor.
* It is applied after event policy accepts the event and before the final `LogEvent` is created.
* It is not output formatting and not event selection.
* A root payload processor can define application-wide depth behavior through inheritance.
* A local payload processor can override depth behavior for a specific context when needed.
