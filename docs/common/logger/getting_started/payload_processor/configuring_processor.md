# Configuring processor

```{contents} Contents:
:depth: 1
:local:
```

A payload processor is configured on `LogContext`.

It can be assigned when the context is created, changed later, or reset so that the context uses the processor inherited from its parent.

## Configuring processor on context creation

A context can receive a local payload processor through `configure_log_context()`.

```python
from mvx.common.logger import (
    LogPayloadProcessor,
    LogVerbosityLevel,
    configure_log_context,
)

processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MAXIMUM,
    max_str_len=500,
    max_items=50,
)

ctx = configure_log_context(
    "my_app.worker",
    payload_processor=processor,
)
```

This context will use the provided processor for payload normalization.

Child contexts may inherit this processor unless they define their own one.

## Changing processor later

A context can be reconfigured later with `set_payload_processor()`.

```python
from mvx.common.logger import (
    LogPayloadProcessor,
    LogVerbosityLevel,
    configure_log_context,
)

ctx = configure_log_context("my_app.worker")

processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MINIMAL,
    max_str_len=100,
    max_items=10,
)

ctx.set_payload_processor(processor)
```

After this call, subsequent events emitted through the context use the new processor.

The event-emitting code does not need to change.

## Resetting processor

A non-root context can reset its local payload processor.

```python
ctx.reset_payload_processor()
```

After reset, the context uses the processor inherited from its parent.

```text
local payload processor is set
        |
        v
ctx.reset_payload_processor()
        |
        v
processor is inherited from parent context
```

Resetting does not modify the parent processor. It only removes the local processor from the current context.

## Root context

The root context must always have a payload processor.

For this reason, `reset_payload_processor()` is not allowed on the root context.

A child context may rely on inheritance. The root context cannot, because it has no parent.

## Inheritance model

Payload processor is an inherited context setting.

If a context has a local processor, it uses that processor.

If it does not have a local processor, it asks its parent context.

This continues up the hierarchy until the root context is reached.

```text
my_app                 -> processor A
my_app.worker          -> inherits processor A
my_app.worker.tasks    -> inherits processor A
```

If a child context receives its own processor, inheritance stops at that context.

```text
my_app                 -> processor A
my_app.worker          -> processor B
my_app.worker.tasks    -> inherits processor B
```

This makes it possible to define application-wide payload normalization at the root or high-level context, and override it only where a specific layer needs different rules.

## Processor instance sharing

When several contexts inherit the same payload processor, they use the same processor object.

This is useful when one shared processor defines the common application policy.

If a context needs independent settings, create and assign a separate processor instance for that context.

```python
worker_processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MAXIMUM,
)

ctx.set_payload_processor(worker_processor)
```

## Configuring the processor itself

Processor configuration and context configuration are separate things.

Context configuration decides which processor object a context uses.

Processor configuration decides how that processor normalizes payload values.

For `LogPayloadProcessor`, processor settings include verbosity level, string length limit, collection item limit, and adapter resolver.

Those settings belong to the processor, not to `LogContext`.

## What to remember

* A payload processor can be passed to `configure_log_context()`.
* A context can change its local processor with `set_payload_processor()`.
* A non-root context can remove its local processor with `reset_payload_processor()`.
* The root context must always have a processor.
* Payload processor is inherited from parent context when no local processor is set.
* Processor settings belong to the processor object, not to `LogContext`.
