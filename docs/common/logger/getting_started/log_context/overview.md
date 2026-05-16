# Logging contexts (LogContext)

```{contents} Contents:
:depth: 1
:local:
```

`LogContext` is the main entry point into `MVX Logger` ecosystem.

By analogy with the standard `logging` package, `LogContext` plays a role similar to `Logger`: it is obtained by name and primarily used to emit events.

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app.worker")

ctx.log_info_event(
    event="started",
    payload={
        "worker_id": "worker-1",
    },
)
```

However, `LogContext` is not just a thin wrapper around `logging.Logger`.

Its role is broader: it connects the logging name, event filtering rules, payload preparation rules, and the sink to which the event will be passed.

That is why the rest of the code usually works with logging contexts rather than directly with sinks, although direct sink usage is also possible.

## Why LogContext exists

A logging context is responsible for several things at the same time.

First, it defines the default namespace of an event. If the context is named `my_app.worker`, then the event `started` belongs to that logging area: `my_app.worker.started`.

Second, it determines whether a particular event should be logged at all. For this purpose, an event policy can be passed to the context. If no policy is set, every event is considered allowed.

Third, the context stores settings that affect payload preparation: verbosity level, string length limits, collection item limits, and an adapter resolver for custom object serialization. Moreover, it provides access to payload normalization functions.

Fourth, the context knows which sink should receive the event.

As a result, user code performs only one action: it tells the context what happened.

Everything else remains the responsibility of the logging infrastructure.

## Context names

Context names use the dotted format:

```text
my_app
my_app.worker
my_app.worker.tasks
```

This is done for the same reason this format is convenient in standard `logging`: the name immediately shows where the event belongs inside the project.

For example:

```text
my_app.api
my_app.db
my_app.worker
my_app.worker.tasks
```

The name shows which part of the project emitted the event. At the same time, the dotted format naturally defines a hierarchy.

The context `my_app.worker.tasks` is a child of `my_app.worker`, and `my_app.worker` is a child of `my_app`.

This makes it possible to configure general rules at a higher level and refine them lower in the hierarchy.

## Context hierarchy

When a context with a nested name is created, missing intermediate contexts are created automatically.

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app.worker.tasks")
```

After this call, the following contexts will be configured and become available:

```text
my_app
my_app.worker
my_app.worker.tasks
```

Intermediate contexts are created for a reason. They are needed for settings inheritance.

If a context does not have its own setting, it takes that setting from its parent context.

For example, if a sink is configured at the `my_app` level, then `my_app.worker` and `my_app.worker.tasks` will use the same sink until another one is explicitly assigned to them.

The same applies to verbosity level, payload limits, the adapter resolver, and the logging error handling policy.

## Root context

Inside `MVX Logger`, the root context always exists.

It is created during package initialization and is the top point of the entire hierarchy.

The root context receives the default sink, default verbosity level, and base settings from which other contexts can inherit.

Usually, user code does not need to start from the root context. A named context is used much more often:

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")
```

However, the root context is important as the technical foundation: thanks to it, a new context already has working settings even if the user has not configured anything else.

```{toctree}
:maxdepth: 1
:caption: What to read next

context_naming
context_creation
context_configuration
context_usage
```
