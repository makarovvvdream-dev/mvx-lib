# Context configuration

A context does not only send events to a sink. It also connects event filtering, payload processing, and event delivery.

Through context configuration, you can answer several practical questions:

* where events should be sent;
* which events should be allowed into the log;
* which payload processor should be used;
* what should happen if an error occurs inside the logging system itself.

These settings include:

* `log_sink` — the sink to which events are passed;
* `event_policy` — the event selection policy;
* `payload_processor` — the component responsible for payload normalization;
* `log_error_handling_policy` — the policy for handling errors raised by logging itself.

The event policy works with event metadata: namespace, event name, entity id, and optional source location. It does not inspect payload, event type, level, or timestamp.

Usually, settings are provided when the context is created:

```python
from mvx.common.logger import (
    LogPayloadProcessor,
    LogVerbosityLevel,
    configure_log_context,
)

payload_processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MAXIMUM,
    max_str_len=500,
    max_items=50,
)

ctx = configure_log_context(
    "my_app.worker",
    payload_processor=payload_processor,
)
```

This context will use its own payload processor. In this example, the processor produces more detailed payloads and cuts strings and collections less aggressively during normalization.

A context can also be reconfigured later. For instance, a payload processor with a different configuration can be assigned to a context:

```python
from mvx.common.logger import (
    LogPayloadProcessor,
    LogVerbosityLevel,
    configure_log_context,
)

ctx = configure_log_context("my_app.worker")

payload_processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MINIMAL,
    max_str_len=100,
    max_items=10,
)

ctx.set_payload_processor(payload_processor)
```

After that, the same context will prepare payloads differently. The event-emitting code can keep sending the same event name and payload structure, while the configured payload processor changes how that payload is normalized before delivery.

## Local settings and inheritance

Contexts form a hierarchy.

Most context settings can be inherited from the parent context. If a child context does not define its own value, it takes the value from its parent.

This applies to settings such as `log_sink`, `payload_processor`, and `log_error_handling_policy`.

`event_policy` is different: it is configured individually for each context level and is not inherited through the hierarchy.

For example, the `my_app.worker` context may inherit the sink from `my_app`, but use its own payload processor:

```python
from mvx.common.logger import (
    LogPayloadProcessor,
    LogVerbosityLevel,
    configure_log_context,
)

payload_processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MAXIMUM,
)

ctx = configure_log_context(
    "my_app.worker",
    payload_processor=payload_processor,
)
```

If a local setting is no longer needed, it can be reset:

```python
from mvx.common.logger import (
    LogPayloadProcessor,
    LogVerbosityLevel,
    configure_log_context,
)

ctx = configure_log_context("my_app.worker")

payload_processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MINIMAL,
    max_str_len=100,
    max_items=10,
)

ctx.set_payload_processor(payload_processor)

# log something with the context

ctx.reset_payload_processor()
```

After the reset, the context will again use the payload processor from its parent context.

For the root context, some reset operations are forbidden because the root context must always keep a basic working configuration.
