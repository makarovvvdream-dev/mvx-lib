# Error policy

This example shows how `log_error_policy` can emit a suppressed `failed` outcome for an expected exception type.

A suppressed failed outcome still records that the operation failed, but it does not include the detailed `error` payload.

## Example

```python
import pytest

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation


class ConfigLoader:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "load_optional_config",
        error_level_suppressed=LogLevel.INFO,
        log_error_policy=((FileNotFoundError, False),),
    )
    def load_optional_config(self, path: str) -> dict[str, object]:
        raise FileNotFoundError(path)
```

The decorated public API operation is `load_optional_config`.

The policy rule is:

```text
(FileNotFoundError, False)
```

This means:

```text
when FileNotFoundError is raised
emit a suppressed failed outcome
```

The example sets `error_level_suppressed=LogLevel.INFO` explicitly so the suppressed outcome level is visible in the example.

## Emitted records

A call that raises `FileNotFoundError`:

```text
with pytest.raises(FileNotFoundError):
    loader.load_optional_config("/tmp/missing.json")
```

emits records conceptually equivalent to:

```text
[
    {
        "level": LogLevel.DEBUG,
        "event_name": "load_optional_config",
        "event_outcome": "invoke",
        "payload": {},
    },
    {
        "level": LogLevel.INFO,
        "event_name": "load_optional_config",
        "event_outcome": "failed",
        "payload": {},
    },
]
```

The `failed` outcome is emitted, but the payload does not contain an `error` field.

The original `FileNotFoundError` is still re-raised.

## Why suppress an error payload

Some exceptions are expected at a particular API boundary.

For example, an optional configuration file may be missing. The operation still fails, and that fact may be useful in logs, but the full error payload may not be useful every time.

`log_error_policy` lets the decorated operation express that choice explicitly.

```text
record the failed outcome
omit detailed error payload
use a chosen suppressed level
re-raise the original exception
```

## What this example demonstrates

This example demonstrates a matching `log_error_policy` rule with `force_log=False`.

The result is:

```text
invoke outcome emitted
failed outcome emitted
failed level = error_level_suppressed
failed payload has no error field
original exception is re-raised
```

Use this pattern when the exception type is expected, already logged elsewhere, or too noisy for full error details at this operation boundary.
