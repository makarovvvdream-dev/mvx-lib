# API

This page documents the public API of the `log_invocation` component.

`log_invocation` exposes one primary public function and several integration protocols used by that function.

The function is the decorator factory:

```python
from mvx.common.logger import log_invocation
```

The protocols describe the context-like objects that `log_invocation` can work with:

```python
from mvx.common.logger import (
    LogContextProto,
    LogContextProviderProto,
    LogEntityIdProviderProto,
)
```

## Decorator factory

`log_invocation` is a decorator factory.

It creates a decorator that wraps a public API operation and emits structured lifecycle outcomes through a resolved logging context.

The decorated operation is treated as one event. The operation lifecycle is represented through `event_outcome` values such as `invoke`, `success`, `failed`, and `cancelled`.

```{eval-rst}
.. autofunction:: mvx.common.logger.log_invocation
```

## Integration protocols

`log_invocation` does not require the concrete `LogContext` class.

Instead, it works with a context-like object that satisfies `LogContextProto`.

This allows the decorator to be used with `LogContext` itself or with another object that exposes the minimal behavior required by the decorator.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogContextProto
   :members:
   :member-order: bysource
```

## Method-based context resolution

For method-based usage, `log_invocation` can resolve the logging context from the first positional argument.

For instance methods, that argument is usually `self`.

The object must satisfy `LogContextProviderProto`:

```{eval-rst}
.. autoclass:: mvx.common.logger.LogContextProviderProto
   :members:
   :member-order: bysource
```

Conceptually:

```text
self -> get_log_context() -> LogContextProto-compatible object
```

If the decorator receives an explicit `ctx` argument, this protocol is not used for context resolution.

## Method-based entity id resolution

`log_invocation` can also resolve an optional entity id from the first positional argument.

The object must satisfy `LogEntityIdProviderProto`:

```{eval-rst}
.. autoclass:: mvx.common.logger.LogEntityIdProviderProto
   :members:
   :member-order: bysource
```

Conceptually:

```text
self -> identity -> LogEventMeta.entity_id
```

If the decorator receives an explicit `entity_id_getter`, that getter is used instead.

## Related logger APIs

`log_invocation` uses common logger types such as `LogLevel`, `LogEventMeta`, and `LogEvent`.

Those types are documented in the main logger API reference, not on this page.

