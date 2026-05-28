# Payload processing

This page documents payload processing APIs.

Payload processing is split into two layers:


```text
base abstractions
    protocols that define what payload processing means

default implementation
    the built-in processor and its configuration types
```

The base abstractions are independent from the default implementation. The default implementation uses them, but they are not the same API layer.

## Base abstractions

The base payload-processing API contains two protocols:

```text
LogPayloadProcessorProto
LogPayloadProvider
```

`LogPayloadProcessorProto` defines what a payload processor must provide.

`LogPayloadProvider` lets an object provide its own structured logging representation.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogPayloadProcessorProto
   :members:
   :member-order: bysource

.. autoclass:: mvx.common.logger.LogPayloadProvider
   :members:
   :member-order: bysource
```

## Processor protocol

`LogPayloadProcessorProto` is the contract used by `LogContext` and logging components.

A processor must be able to normalize:

```text
complete payload mappings
single values
```

It also exposes the current verbosity level as a plain string. Components such as `log_invocation` use that string for verbosity-gated field specifications.

The protocol does not require a specific implementation strategy. A custom processor may use any normalization rules as long as it returns log-ready values compatible with the protocol.

## Payload provider protocol

`LogPayloadProvider` is an object-level escape hatch.

When an object implements this protocol, its `to_log_payload()` result is used as the object's logging representation.

The returned payload is expected to be log-ready. The object that implements the protocol is responsible for keeping the payload reasonably sized and free of sensitive data.

In the default implementation, `LogPayloadProvider` takes precedence over type-based log adapters.

## Default implementation

`LogPayloadProcessor` is the built-in payload processor.

It provides conservative default normalization for common Python values and extension points for domain objects.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogPayloadProcessor
   :members:
   :member-order: bysource
   :class-doc-from: both
```

## Default implementation behavior

The default processor normalizes values using a bounded, log-oriented representation.

It handles:

```text
strings
bytes-like values
integers
floats
booleans
None
enums
mappings
lists and tuples
objects implementing LogPayloadProvider
objects handled by a configured log adapter
unsupported objects
```

Unsupported objects are represented by their type name.

For mappings, keys are converted to strings and values are normalized individually.

For lists and tuples, items are normalized individually.

String length and collection size are limited by the processor configuration unless item limiting is explicitly disabled for a normalization call.

## Verbosity levels

`LogVerbosityLevel` is part of the default implementation.

It is not required by the base `LogPayloadProcessorProto` contract, but the built-in processor uses it as its verbosity setting.

```{eval-rst}
.. autoenum:: mvx.common.logger.LogVerbosityLevel
```

## Default limits

The default processor uses two built-in limits when no local values are configured.

```text
DEFAULT_MAX_STR_LEN = 200
    default maximum string length

DEFAULT_MAX_ITEMS = 10
    default maximum number of mapping or sequence items
```

These constants belong to the default implementation.

## Adapter types

The default processor can use type-based log adapters.

`LogAdapter` is the callable type used to convert a custom object into a log-ready payload dictionary.

```python
LogAdapter = Callable[[Any, LogVerbosityLevel], dict[str, Any]]
```

The callable receives the value being normalized and the current verbosity level.

`LogAdapterResolver` is the callable type used to resolve an adapter for a value.

```python
LogAdapterResolver = Callable[[Any], LogAdapter | None]
```

The resolver receives the value being normalized and returns an adapter for that value, or `None` if no adapter is available.

These aliases belong to the default implementation. They are not part of the base processor protocol.

## Custom object normalization order

The default processor tries custom normalization before falling back to generic normalization.

The order is:

```text
1. LogPayloadProvider.to_log_payload()
2. configured LogAdapterResolver and returned LogAdapter
3. built-in normalization rules
```

If a `LogPayloadProvider` returns a dictionary, that dictionary is used.

If provider handling fails or does not return a dictionary, the processor falls back to the next option.

If a resolver is configured and returns an adapter, the adapter is called with the value and the current verbosity level.

If adapter handling fails or does not return a dictionary, the processor falls back to built-in normalization rules.

## Bounded normalization

The default processor limits output size by default.

`max_str_len` limits long strings and mapping keys.

`max_items` limits mapping entries and sequence items.

If a collection has more items than the effective limit, the processor adds a marker showing that more data exists.

The `unbounded` argument disables item-count limiting for that normalization call. It does not disable string length limiting.


