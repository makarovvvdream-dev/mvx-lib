# Writing an event policy

```{contents} Contents:
:depth: 1
:local:
```

An event policy is a small object that decides whether an event is allowed into the log.

It works with `LogEventMeta` and returns a boolean decision:

* `True` — the event is accepted;
* `False` — the event is rejected.

The policy does not receive payload and should not depend on payload content.

## Policy contract

An event policy must provide one method:

```python
from mvx.common.logger import LogEventMeta


class MyPolicy:
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        ...
```

The method receives `LogEventMeta`.

It should return `True` if the event is allowed and `False` if the event should be skipped.

The class does not need to inherit from a base class. It only needs to provide the required method.

For type annotations, `LogEventPolicyProto` can be used:

```python
from mvx.common.logger import LogEventMeta, LogEventPolicyProto


class MyPolicy:
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return True


policy: LogEventPolicyProto = MyPolicy()
```

## Simple namespace policy

A common policy is to allow events only from selected namespaces.

```python
from dataclasses import dataclass

from mvx.common.logger import LogEventMeta


@dataclass(frozen=True, slots=True)
class NamespacePolicy:
    allowed_namespaces: frozenset[str]

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.event_namespace in self.allowed_namespaces
```

Example usage:

```python
from mvx.common.logger import configure_log_context

policy = NamespacePolicy(
    allowed_namespaces=frozenset({"my_app.worker"}),
)

ctx = configure_log_context(
    "my_app.worker",
    event_policy=policy,
)
```

This context will allow events whose metadata belongs to the `my_app.worker` namespace.

## Prefix-based namespace policy

Sometimes a whole namespace subtree should be enabled.

```python
from dataclasses import dataclass

from mvx.common.logger import LogEventMeta


@dataclass(frozen=True, slots=True)
class NamespacePrefixPolicy:
    namespace_prefix: str

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        namespace = event.event_namespace
        if namespace is None:
            return False
        return namespace == self.namespace_prefix or namespace.startswith(
            f"{self.namespace_prefix}."
        )
```

For example, a policy with `namespace_prefix="my_app.worker"` accepts:

```text
my_app.worker
my_app.worker.tasks
my_app.worker.tasks.cleanup
```

and rejects unrelated namespaces such as:

```text
my_app.api
my_app.db
```

## Event-name policy

A policy can also select events by name.

```python
from dataclasses import dataclass

from mvx.common.logger import LogEventMeta


@dataclass(frozen=True, slots=True)
class EventNamePolicy:
    allowed_events: frozenset[str]

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.event_name in self.allowed_events
```

This can be useful when a context emits many events, but only a small subset should be enabled in normal operation.

Example:

```python
policy = EventNamePolicy(
    allowed_events=frozenset({
        "started",
        "stopped",
        "operation.failed",
    }),
)
```

## Entity policy

`entity_id` can be used to enable logging for one concrete runtime entity.

```python
from dataclasses import dataclass

from mvx.common.logger import LogEventMeta


@dataclass(frozen=True, slots=True)
class EntityPolicy:
    entity_id: str

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.entity_id == self.entity_id
```

This is useful for troubleshooting a specific request, connection, session, worker, or another runtime object without enabling wide logging for the whole application area.

## Configuration-driven policy

Event policies are often built from application or layer configuration.

For example, a layer may expose two logging modes:

* normal mode — allow only stable operational events;
* diagnostic mode — allow additional events useful during troubleshooting.

```python
from dataclasses import dataclass
from enum import StrEnum

from mvx.common.logger import LogEventMeta


class LoggingMode(StrEnum):
    NORMAL = "normal"
    DIAGNOSTIC = "diagnostic"


@dataclass(frozen=True, slots=True)
class WorkerEventPolicy:
    mode: LoggingMode

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        if self.mode == LoggingMode.NORMAL:
            return event.event_name in {
                "started",
                "stopped",
                "operation.failed",
            }

        if self.mode == LoggingMode.DIAGNOSTIC:
            return event.event_namespace == "my_app.worker"

        return False
```

The code that emits events does not change when the mode changes.

Only the policy changes.

This keeps logging-width decisions outside business code and makes them configurable for a particular application layer.

## Combining rules

A policy may combine several metadata checks.

```python
from dataclasses import dataclass

from mvx.common.logger import LogEventMeta


@dataclass(frozen=True, slots=True)
class WorkerTroubleshootingPolicy:
    namespace: str
    entity_id: str | None = None

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        if event.event_namespace != self.namespace:
            return False

        if self.entity_id is not None:
            return event.entity_id == self.entity_id

        return event.event_name in {
            "started",
            "stopped",
            "operation.failed",
        }
```

This policy first restricts events to one namespace.

Then, if an `entity_id` is provided, it enables only events related to that entity.

Otherwise, it falls back to a smaller operational event set.

## Keep policies cheap

An event policy is executed before payload normalization.

It should be cheap and predictable.

Good policy logic usually checks:

* namespace;
* event name;
* entity id;
* source path;
* source line;
* source function.

A policy should not:

* serialize objects;
* perform network I/O;
* query a database;
* write logs itself;
* depend on any kind sink behavior.


## What to remember

* An event policy is any object that provides `is_event_enabled(event: LogEventMeta) -> bool`.
* It accepts or rejects events using metadata only.
* It is a good place to implement logging-width decisions based on application configuration, runtime mode, or layer-specific diagnostic settings.
* The code that emits events can stay unchanged while different policies enable different levels of event visibility.
