# Stream sink

```{contents} Contents:
:depth: 1
:local:
```

`StreamLogSink` is a sink for writing events to a standard stream.

It is used when familiar console logging is needed: `stderr` or `stdout`.

`StreamLogSink` is fully based on Python's standard `logging` ecosystem, so the familiar stream handler settings are available: output stream, log format, date format, handler level, formatter factory, and filters.

## Why StreamLogSink exists

`StreamLogSink` is used for the simplest and most familiar kind of event delivery: writing a log line to a standard stream.

It is fully based on Python's standard `logging` ecosystem. This means that users do not have to give up familiar formatting tools, levels, and filters.

At the same time, user code still works through `LogContext` and emits an event, not a preformatted string.

```text
LogContext -> StreamLogSink -> stderr / stdout
```

`StreamLogSink` is a synchronous sink. It delivers an event in the same thread in which the logging method was called.

This is appropriate for standard stream output and for simple scenarios such as local runs, debugging, or CLI usage.

## Switching to stdout

To write to `stdout` instead of `stderr`, create a stream sink with `STDOUT` configuration and attach it to a context.

```python
from mvx.common.logger import (
    LogStreamOutput,
    LoggingStreamConfig,
    StreamLogSink,
    configure_log_context,
    configure_log_sink,
)

stdout_sink = configure_log_sink(
    name="stdout",
    sink_cls=StreamLogSink,
    config=LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
    ),
)

ctx = configure_log_context(
    "my_app",
    log_sink=stdout_sink,
)

ctx.log_info_event(
    event="app.started",
    payload={
        "service": "demo",
        "mode": "local",
    },
)
```

After that, events from the `my_app` context will be delivered to `stdout`.

## Log format

`StreamLogSink` does not write the original `LogEvent` directly. It adapts it to standard `logging`.

By default, the log format is:

```text
%(asctime)s %(levelname)s: %(message)s %(payload)s
```

For a typical event, this produces a line like this:

```text
2026-05-12 14:30:25 INFO: my_app.app.started {'service': 'demo', 'mode': 'local'}
```

If a different line format is needed, it can be set through `LoggingStreamConfig`.

```python
from mvx.common.logger import (
    LogStreamOutput,
    LoggingStreamConfig,
    StreamLogSink,
    configure_log_context,
    configure_log_sink,
)

sink = configure_log_sink(
    name="stdout_compact",
    sink_cls=StreamLogSink,
    config=LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(levelname)s: %(message)s %(payload)s",
    ),
)

ctx = configure_log_context(
    "my_app",
    log_sink=sink,
)
```

Now the timestamp will not be included in the line because it is not present in `log_format`.

## Date format

If `%(asctime)s` is used in `log_format`, the date format can be changed with `date_format`.

```python
from mvx.common.logger import (
    LogStreamOutput,
    LoggingStreamConfig,
    StreamLogSink,
    configure_log_sink,
)

sink = configure_log_sink(
    name="stdout_custom_date",
    sink_cls=StreamLogSink,
    config=LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(asctime)s %(levelname)s: %(message)s %(payload)s",
        date_format="%H:%M:%S",
    ),
)
```

In this case, time will be printed in a shorter format.

## Logging handler level

`LoggingStreamConfig` allows setting the level of the standard `logging` handler.

```python
from mvx.common.logger import (
    LogStreamOutput,
    LoggingStreamConfig,
    StreamLogSink,
    configure_log_sink,
    LogLevel,
)

sink = configure_log_sink(
    name="stdout_warning",
    sink_cls=StreamLogSink,
    config=LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        level=LogLevel.WARNING,
    ),
)
```

This setting applies to the standard `logging` handler.

It does not replace the context event policy. The event policy decides which events the context passes to the sink by checking event metadata before the final payload is prepared. The handler level is applied later, inside the stream sink, during delivery through standard `logging`.

## Custom formatter factory

If `log_format` and `date_format` are not enough, a custom `formatter_factory` can be provided.

`formatter_factory` receives `log_format` and `date_format` and must return a `logging.Formatter` object.

```python
import logging

from mvx.common.logger import (
    LogStreamOutput,
    LoggingStreamConfig,
    StreamLogSink,
    configure_log_context,
    configure_log_sink,
)


def make_formatter(log_format: str, date_format: str) -> logging.Formatter:
    return logging.Formatter(
        fmt=log_format,
        datefmt=date_format,
    )


sink = configure_log_sink(
    name="stdout_custom_formatter",
    sink_cls=StreamLogSink,
    config=LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(asctime)s %(levelname)s: %(message)s %(payload)s",
        date_format="%Y-%m-%d %H:%M:%S",
        formatter_factory=make_formatter,
    ),
)

ctx = configure_log_context(
    "my_app",
    log_sink=sink,
)
```

In normal cases, a custom formatter factory is not needed.

It is useful when a custom subclass of `logging.Formatter` should be used, or when formatters should be created in a centralized way.

Important: the formatter works at the standard `logging.LogRecord` level, not directly with the original `LogEvent`.

`MVX Logger` first adapts `LogEvent` to `LogRecord`, and then the standard formatter creates the output line.

## Logging filters

`LoggingStreamConfig` can also receive standard `logging` filters.

A filter may be a `logging.Filter` object or a callable compatible with the Python `logging` filter mechanism.

```python
import logging

from mvx.common.logger import (
    LogStreamOutput,
    LoggingStreamConfig,
    StreamLogSink,
    configure_log_context,
    configure_log_sink,
)


def only_app_events(record: logging.LogRecord) -> bool:
    return record.getMessage().startswith("my_app.")


sink = configure_log_sink(
    name="stdout_filtered",
    sink_cls=StreamLogSink,
    config=LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        filters=(only_app_events,),
    ),
)

ctx = configure_log_context(
    "my_app",
    log_sink=sink,
)
```

Filters are applied inside the standard `logging` handler.

Like the handler level, filters do not replace the context event policy. They are an additional standard `logging` mechanism applied during event delivery through the stream sink.

## When to use StreamLogSink

`StreamLogSink` is useful when you need to:

* start logging quickly without external infrastructure;
* see events in `stderr`;
* write events to `stdout`;
* use familiar features of standard `logging`;
* get simple output for CLI, local runs, or debugging.

If delivery involves heavier I/O, such as file writes, network access, or an external backend, an asynchronous sink is usually a better fit.

For writing events to a file, `MVX Logger` provides `FileLogSink`.
