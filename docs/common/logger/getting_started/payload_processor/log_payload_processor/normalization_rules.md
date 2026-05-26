# Normalization rules

```{contents} Contents:
:depth: 1
:local:
```

This article describes how `LogPayloadProcessor` normalizes payload values.

The processor exposes two normalization entry points:

```python
normalize_payload(payload, *, unbounded=False)
normalize_value_for_log(value, *, unbounded=False)
```

Both methods apply the same value-normalization rules. The difference is the entry point:

* `normalize_payload()` expects a mapping and returns a payload dictionary;
* `normalize_value_for_log()` accepts one arbitrary value and returns its log-ready representation.

## Normalized value types

A normalized value may be one of these types:

```python
str | int | float | bool | bytes | dict[str, Any] | list[Any] | None
```

These are the values that `LogPayloadProcessor` places into normalized payload dictionaries and normalized containers.

Unsupported objects are not expanded by introspection. They are converted to a stable placeholder string based on the object type.

## Custom representation comes first

Before applying generic rules, `LogPayloadProcessor` checks whether a value has a custom logging representation.

The order is:

```text
to_log_payload()
        |
        v
log adapter resolver
        |
        v
generic normalization rules
```

If a value provides a valid `to_log_payload()` result, that dictionary is used.

If not, the processor tries the configured `log_adapter_resolver`.

If neither mechanism produces a dictionary, generic normalization rules are applied.

`to_log_payload()` and adapters are described in separate articles. This article focuses on the generic rules.

## Primitive values

Primitive values are kept as primitive values.

```python
processor.normalize_value_for_log(None)
# None

processor.normalize_value_for_log(True)
# True

processor.normalize_value_for_log(42)
# 42

processor.normalize_value_for_log(3.14)
# 3.14
```

String values are returned as strings, but may be shortened by `max_str_len`.

```python
processor = LogPayloadProcessor(max_str_len=3)

processor.normalize_value_for_log("abcdef")
# "abc..."
```

Byte-like values are converted to `bytes`.

This applies to:

* `bytes`;
* `bytearray`;
* `memoryview`.

```python
processor.normalize_value_for_log(bytearray(b"abc"))
# b"abc"
```

## Enums

Enum values are normalized through their `.value`.

```python
from enum import Enum


class Status(Enum):
    OK = "ok"


processor.normalize_value_for_log(Status.OK)
# "ok"
```

After `.value` is extracted, the value is normalized using the usual rules.

## Lists and tuples

Lists and tuples are normalized to lists.

```python
processor.normalize_value_for_log((1, "a", True))
# [1, "a", True]
```

Each included item is normalized as a leaf value.

This means nested mappings, lists, and tuples are not expanded recursively inside a list. They are represented as placeholders.

```python
processor.normalize_value_for_log([{"a": 1}, [1, 2]])
# ["<dict>", "<list>"]
```

`max_items` limits how many items are included.

```python
processor = LogPayloadProcessor(max_items=2)

processor.normalize_value_for_log([1, 2, 3, 4])
# [1, 2, "...(2 more)"]
```

If `unbounded=True` is passed, the item limit is not applied for that call.

```python
processor.normalize_value_for_log([1, 2, 3, 4], unbounded=True)
# [1, 2, 3, 4]
```

`unbounded=True` does not disable string shortening.

## Mappings

Mappings are normalized to dictionaries.

Keys are converted to strings.

```python
processor.normalize_value_for_log({1: "one"})
# {"1": "one"}
```

Long keys are shortened by `max_str_len`.

```python
processor = LogPayloadProcessor(max_str_len=3)

processor.normalize_value_for_log({"abcdef": 1})
# {"abc...": 1}
```

Values are normalized as leaf values.

This means nested mappings, lists, and tuples inside a mapping are represented as placeholders.

```python
processor.normalize_value_for_log(
    {
        "items": [1, 2, 3],
        "meta": {"a": 1},
    }
)
# {
#     "items": "<list>",
#     "meta": "<dict>",
# }
```

`max_items` limits how many key-value pairs are included.

```python
processor = LogPayloadProcessor(max_items=2)

processor.normalize_payload(
    {
        "a": 1,
        "b": 2,
        "c": 3,
    }
)
# {
#     "a": 1,
#     "b": 2,
#     "__more__": "1 more keys",
# }
```

If `unbounded=True` is passed, the item limit is not applied for that call.

```python
processor.normalize_payload(
    {
        "a": 1,
        "b": 2,
        "c": 3,
    },
    unbounded=True,
)
# {
#     "a": 1,
#     "b": 2,
#     "c": 3,
# }
```

## Unsupported objects

Unsupported objects are represented by a placeholder string with the object type name.

```python
class User:
    pass


processor.normalize_value_for_log(User())
# "<User>"
```

The processor does not inspect object attributes automatically.

To provide structured output for an object, use `to_log_payload()` or an external adapter.

## `normalize_payload()`

`normalize_payload()` is the normal entry point for complete event payloads.

It expects a mapping and returns a normalized dictionary.

```python
processor.normalize_payload(
    {
        "status": "completed",
        "duration_ms": 37,
    }
)
# {
#     "status": "completed",
#     "duration_ms": 37,
# }
```

If a non-mapping value is passed to the internal dictionary normalizer, the result is an empty dictionary.

In normal usage, `normalize_payload()` should receive a payload mapping.

## Leaf normalization

Container contents are normalized as leaf values.

A leaf value may still use custom representation, enum normalization, primitive normalization, or fallback placeholder representation.

But nested containers are not expanded recursively through the full container rules when they appear as values inside another container.

Examples:

```python
processor.normalize_value_for_log([[1, 2]])
# ["<list>"]

processor.normalize_value_for_log({"nested": {"a": 1}})
# {"nested": "<dict>"}
```

This keeps payload normalization bounded and predictable.

## What to remember

* `LogPayloadProcessor` normalizes payloads into dictionaries containing log-ready values.
* Custom representation is tried before generic normalization.
* Primitive values stay primitive.
* Byte-like values become `bytes`.
* Enums are normalized through `.value`.
* Lists and tuples become lists.
* Mappings become dictionaries with string keys.
* Unsupported objects become placeholder strings.
* Nested containers inside containers are represented as placeholders unless they provide a custom representation.
