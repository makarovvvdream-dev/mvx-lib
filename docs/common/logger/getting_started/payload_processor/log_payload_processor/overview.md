# Default payload processor (LogPayloadProcessor)

```{contents} Contents:
:depth: 1
:local:
```

`LogPayloadProcessor` is the default payload processor provided by `MVX Logger`.

It implements the payload processor protocol and provides the standard normalization behavior used by the root logging context unless another processor is configured.

The class is the built-in implementation of the general payload processor role. It is not responsible for event selection, `LogEvent` creation, or delivery to sinks.

Its responsibility is limited to payload normalization:

```text
raw payload
    -> LogPayloadProcessor
    -> normalized payload dictionary
```

## What it provides

`LogPayloadProcessor` covers the common payload normalization needs:

* normalizing complete payload dictionaries;
* normalizing individual values;
* controlling payload detail through verbosity level;
* limiting long strings;
* limiting large lists, tuples, and mappings;
* supporting object-provided payloads through `to_log_payload()`;
* supporting external adapters through `log_adapter_resolver`;
* falling back to stable placeholder strings for unsupported objects.

These features are the default implementation strategy. A project can still provide a different payload processor when it needs different rules.

## Public API

`LogPayloadProcessor` exposes the same public methods required by the payload processor protocol.

```python
normalize_payload(payload, *, unbounded=False)
```

Normalizes a payload mapping and returns a dictionary suitable for `LogEvent.payload`.

```python
normalize_value_for_log(value, *, unbounded=False)
```

Normalizes a single value and returns a log-ready representation.

```python
get_plain_verbosity_level()
```

Returns the current verbosity level as a plain string.

The class also exposes configuration properties and setters for its own settings. Those settings belong to `LogPayloadProcessor`, not to `LogContext`.

## Main settings

The default processor is configured through four main settings:

* `verbosity_level` — controls the requested detail level for verbosity-aware logging behavior;
* `max_str_len` — limits long strings and normalized mapping keys;
* `max_items` — limits large lists, tuples, and mappings;
* `log_adapter_resolver` — resolves external adapters for values that should be represented without implementing `to_log_payload()` themselves.

Each setting can be provided when the processor is created and can be changed later through the processor API.

Detailed settings behavior is described in the settings article.

## Normalization model

`LogPayloadProcessor` normalizes payloads recursively, but it keeps the model intentionally limited.

It handles common value categories directly:

* `None`;
* strings;
* integers, floats, and booleans;
* bytes-like values;
* enums;
* mappings;
* lists and tuples;
* values with custom logging representation;
* unsupported objects.

Unsupported objects are not expanded by introspection. They are represented with a stable placeholder based on the object type.

Detailed value rules are described in the normalization rules article.

## Custom object representation

Before applying generic normalization rules, `LogPayloadProcessor` checks whether a value can provide a custom logging payload.

There are two supported mechanisms:

* object-provided payload through `to_log_payload()`;
* external type-based adapters resolved by `log_adapter_resolver`.

`to_log_payload()` has priority over adapters.

These mechanisms are useful when objects need a controlled log representation instead of generic fallback output.

They are described separately in the `to_log_payload()` and adapters articles.

## Size control

`LogPayloadProcessor` prevents accidental payload growth through string and collection limits.

Long strings and long mapping keys are shortened according to `max_str_len`.

Lists, tuples, and mappings are limited according to `max_items`.

For selected normalization calls, `unbounded=True` can disable the collection item limit. It does not disable the string length limit.

Detailed size-limit behavior is described in the settings and normalization rules articles.

## Thread safety

`LogPayloadProcessor` protects its mutable settings with an internal reentrant lock.

This allows settings to be read and updated safely when the processor is shared by several contexts.

The lock protects processor configuration state. It does not change the responsibility of the processor: normalization still remains its only role.

## What to remember

* `LogPayloadProcessor` is the default implementation of the payload processor protocol.
* It normalizes payload mappings and individual values.
* It provides built-in depth controls, size limits, object-provided payloads, and external adapters.
* It does not select events, create `LogEvent`, or deliver events.
* Implementation details are described in the following articles.

```{toctree}
:maxdepth: 1
:caption: LogPayloadProcessor

settings
normalization_rules
to_log_payload
adapters
```
