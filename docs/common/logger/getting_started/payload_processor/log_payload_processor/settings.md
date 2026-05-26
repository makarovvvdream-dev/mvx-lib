# Settings

```{contents} Contents:
:depth: 2
:local:
```

`LogPayloadProcessor` has a small set of settings that define the default payload normalization behavior.

These settings belong to the processor object. They are not `LogContext` settings.

A context selects which processor it uses. The selected processor then decides how payload values are normalized.

## Settings overview

`LogPayloadProcessor` supports four settings:

* `verbosity_level`;
* `max_str_len`;
* `max_items`;
* `log_adapter_resolver`.

They can be provided when the processor is created:

```python
from mvx.common.logger import LogPayloadProcessor, LogVerbosityLevel

processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MAXIMUM,
    max_str_len=500,
    max_items=50,
    log_adapter_resolver=my_adapter_resolver,
)
```

All settings are optional. If a setting is not provided, the processor uses its default behavior.


## Setting: `verbosity_level`

Type:

```python
LogVerbosityLevel | None
```

Default behavior:

```python
LogVerbosityLevel.NORMAL
```

`verbosity_level` describes the requested detail level for verbosity-aware payload representation.

Available values are:

```python
class LogVerbosityLevel(StrEnum):
    MINIMAL = "MINIMAL"
    NORMAL = "NORMAL"
    MAXIMUM = "MAXIMUM"
```

The processor exposes the effective value through the `verbosity_level` property.

```python
processor.verbosity_level
```

If no local value was set, the property returns `LogVerbosityLevel.NORMAL`.

The plain string form can be retrieved with:

```python
processor.get_plain_verbosity_level()
```

This returns the current verbosity value as an uppercase string.

### Updating verbosity level

```python
processor.set_verbosity_level(LogVerbosityLevel.MINIMAL)
```

The value must be an instance of `LogVerbosityLevel`.

Passing any other value raises `TypeError`.

### Resetting verbosity level

```python
processor.reset_verbosity_level()
```

After reset, the processor falls back to `LogVerbosityLevel.NORMAL`.



## Setting: `max_str_len`

Type:

```python
int | None
```

Default value:

```python
DEFAULT_MAX_STR_LEN = 200
```

`max_str_len` limits long strings during normalization.

It is applied to:

* string values;
* normalized mapping keys.

If a string is longer than the effective limit, it is shortened and `...` is appended.

Example:

```python
processor = LogPayloadProcessor(max_str_len=3)

processor.normalize_value_for_log("abcdef")
```

Result:

```python
"abc..."
```

### Updating string limit

```python
processor.set_max_str_len(100)
```

The value must be an integer greater than zero.

Invalid type raises `TypeError`.

A value less than `1` raises `ValueError`.

### Resetting string limit

```python
processor.reset_max_str_len()
```

After reset, the processor falls back to `DEFAULT_MAX_STR_LEN`.



## Setting: `max_items`

Type:

```python
int | None
```

Default value:

```python
DEFAULT_MAX_ITEMS = 10
```

`max_items` limits how many items are included from collections during normalization.

It is applied to:

* lists;
* tuples;
* mappings.

For lists and tuples, only the first `max_items` items are included. If the original collection has more items, a marker is appended.

Example:

```python
processor = LogPayloadProcessor(max_items=2)

processor.normalize_value_for_log([1, 2, 3, 4])
```

Result:

```python
[1, 2, "...(2 more)"]
```

For mappings, only the first `max_items` key-value pairs are included. If more keys are present, the result receives a `__more__` marker.

Example:

```python
processor = LogPayloadProcessor(max_items=2)

processor.normalize_payload(
    {
        "a": 1,
        "b": 2,
        "c": 3,
    }
)
```

Result:

```python
{
    "a": 1,
    "b": 2,
    "__more__": "1 more keys",
}
```

### Updating item limit

```python
processor.set_max_items(20)
```

The value must be an integer greater than zero.

Invalid type raises `TypeError`.

A value less than `1` raises `ValueError`.

### Resetting item limit

```python
processor.reset_max_items()
```

After reset, the processor falls back to `DEFAULT_MAX_ITEMS`.



## Using `unbounded=True` as a per-call option

`unbounded=True` is not a processor setting. It is a per-call option of normalization methods.

It can be passed to:

```python
processor.normalize_payload(payload, unbounded=True)
processor.normalize_value_for_log(value, unbounded=True)
```

For that call, the collection item limit is disabled.

Example:

```python
processor = LogPayloadProcessor(max_items=2)

processor.normalize_value_for_log(
    [1, 2, 3, 4],
    unbounded=True,
)
```

Result:

```python
[1, 2, 3, 4]
```

`unbounded=True` does not disable `max_str_len`.

Example:

```python
processor = LogPayloadProcessor(max_str_len=3)

processor.normalize_value_for_log(
    "abcdef",
    unbounded=True,
)
```

Result:

```python
"abc..."
```



## Setting: `log_adapter_resolver`

Type:

```python
Callable[[Any], LogAdapter | None] | None
```

The adapter resolver is used to find an external adapter for a value.

An adapter has this shape:

```python
Callable[[Any, LogVerbosityLevel], dict[str, Any]]
```

The resolver receives the original value and either returns an adapter or `None`.

```python
def my_adapter_resolver(value: object):
    ...
```

If an adapter is found, the processor calls it with the original value and the effective verbosity level.

```python
provided_payload = adapter(value, processor.verbosity_level)
```

If the adapter returns a dictionary, that dictionary is used as the custom payload representation.

If no adapter is found, or the adapter fails, the processor falls back to generic normalization.

`to_log_payload()` takes priority over adapter resolution. If a value provides a valid `to_log_payload()` result, the adapter resolver is not used for that value.

### Updating adapter resolver

```python
processor.set_log_adapter_resolver(my_adapter_resolver)
```

The resolver must be callable.

A non-callable value raises `TypeError`.

### Resetting adapter resolver

```python
processor.reset_log_adapter_resolver()
```

After reset, no external adapter resolver is used.



## Thread safety

`LogPayloadProcessor` protects its mutable settings with an internal reentrant lock.

The lock is used when reading and updating settings.

This allows the same processor instance to be shared by several contexts without exposing partially updated configuration state.



## What to remember

* `LogPayloadProcessor` settings belong to the processor, not to `LogContext`.
* `verbosity_level` controls the effective verbosity mode.
* `max_str_len` limits strings and mapping keys.
* `max_items` limits lists, tuples, and mappings.
* `unbounded=True` disables only the collection item limit for one normalization call.
* `log_adapter_resolver` connects external object adapters to the default processor.
