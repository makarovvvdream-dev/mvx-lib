# Event policy

This page documents the API for event selection policies.

Event policies decide whether an event should be emitted based on `LogEventMeta`.

Policies receive event metadata only. They do not receive payload data and do not perform payload normalization.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogEventPolicyProto
   :members:
```

## Role in the logging pipeline

Event policy is applied before payload normalization and before sink delivery.

The normal pipeline is:

```text
build LogEventMeta
   |
   v
check event policy
   |
   v
normalize payload
   |
   v
build LogEvent
   |
   v
emit through sink
```

If the policy disables an event, payload normalization does not run and no `LogEvent` is emitted.

## Context behavior

`LogContext` uses its local event policy when checking whether an event is enabled.

If no local event policy is configured, the event is enabled.

Event policy is local to the context. It is not inherited from parent contexts.

This is different from sink, payload processor, and logging error handling policy, which are inherited through the context tree.

## Minimal policy

A policy only needs to implement `is_event_enabled()`.

```python
from mvx.common.logger import LogEventMeta, LogEventPolicyProto


class OnlyConnectionEvents:
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.event_namespace == "example.connection"
```

The policy receives metadata and returns a boolean decision.

## What policies should inspect

Policies should normally inspect stable metadata fields:

```text
event_namespace
event_name
entity_id
source_path
source_line
source_func
```

They should not depend on payload content. Payload content is intentionally outside the event policy boundary.

Use event policy to control logging width: which events are emitted.

Use payload processing configuration to control logging depth: how selected payload values are represented.
