# Context configuration

A context does not only send events to a sink. It also stores settings that define how those events are processed.

Through context configuration, you can answer several practical questions:

* where events should be sent;
* which events should be allowed into the log;
* how detailed the payload serialization should be;
* how long strings and large collections should be limited;
* how custom objects should be serialized;
* what should happen if an error occurs inside the logging system itself.

These settings include:

* `log_sink` — the sink to which events are passed;
* `event_policy` — the event selection policy;
* `verbosity_level` — the payload detail level;
* `max_str_len` — the maximum string length used during payload normalization;
* `max_items` — the maximum number of collection items used during payload normalization;
* `log_adapter_resolver` — the resolver for custom serialization adapters;
* `log_error_handling_policy` — the policy for handling errors raised by logging itself.

The event policy works with event metadata: namespace, event name, entity id, and optional source location. It does not inspect payload, event type, level, or timestamp.

Usually, settings are provided when the context is created:

```python
from mvx.common.logger import LogVerbosityLevel, configure_log_context

ctx = configure_log_context(
    "my_app.worker",
    verbosity_level=LogVerbosityLevel.MAXIMUM,
    max_str_len=500,
    max_items=50,
)
```

This context will log payloads in more detail than a context with regular settings, and it will cut strings and collections less aggressively during normalization.

Settings can also be changed later through the context methods:

```python
from mvx.common.logger import LogVerbosityLevel, configure_log_context

ctx = configure_log_context("my_app.worker")

ctx.set_verbosity_level(LogVerbosityLevel.MINIMAL)
ctx.set_max_str_len(100)
ctx.set_max_items(10)
```

After that, the same context will prepare payloads differently. The event-emitting code can keep sending the same event name and payload structure, while the context changes how that payload is normalized before delivery.

## Local settings and inheritance

Contexts form a hierarchy.

Most context settings can be inherited from the parent context. If a child context does not define its own value, it takes the value from its parent.

This applies to settings such as `log_sink`, `verbosity_level`, `max_str_len`, `max_items`, `log_adapter_resolver`, and `log_error_handling_policy`.

`event_policy` is different: it is configured individually for each context level and is not inherited through the hierarchy.

For example, the `my_app.worker` context may inherit the sink and payload normalization settings from `my_app`, but have its own `verbosity_level`:

```python
from mvx.common.logger import LogVerbosityLevel, configure_log_context

ctx = configure_log_context(
    "my_app.worker",
    verbosity_level=LogVerbosityLevel.MAXIMUM,
)
```

If a local setting is no longer needed, it can be reset:

```python
from mvx.common.logger import LogVerbosityLevel, configure_log_context

ctx = configure_log_context("my_app.worker")

ctx.set_verbosity_level(LogVerbosityLevel.MINIMAL)
ctx.set_max_str_len(100)
ctx.set_max_items(10)

# log something with the context 

ctx.reset_verbosity_level()
ctx.reset_max_str_len()
ctx.reset_max_items()
```

After the reset, the context will again use the value from its parent context.

For the root context, some reset operations are forbidden because the root context must always keep a basic working configuration.
