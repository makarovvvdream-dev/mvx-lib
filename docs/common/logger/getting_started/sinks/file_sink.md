# File sink

```{contents} Contents:
:depth: 1
:local:
```

`FileLogSink` is a sink for writing events to a file.

It is used when events need to be preserved between application runs, archived, analyzed later, or collected as a regular file log.

Unlike `StreamLogSink`, the file sink is asynchronous. User code passes an event to the sink, while the actual file write is performed inside the sink runtime.

```text
LogContext -> FileLogSink -> file
```

`FileLogSink` is based on Python's standard `logging` ecosystem, so the familiar file handler settings are available: file path, open mode, encoding, log format, date format, handler level, formatter factory, and filters.

## Why FileLogSink exists

`FileLogSink` is used for scenarios where events should be written to a file.

For example:

* a local application log file;
* a debug log during development;
* a log for later analysis;
* a log collected by an external agent;
* a simple form of persistent event storage without an external backend.

Writing to a file is an I/O operation. It may be fast, but it may also depend on the state of the disk, the file system, or the runtime environment.

For this reason, `FileLogSink` separates the calling code thread from the event delivery thread.

The code that calls `ctx.log_info_event(...)` does not write to the file directly.

## Attaching a file sink

To write events to a file, create a `FileLogSink` and attach it to a context.

```python
from mvx.common.logger import (
    FileLogSink,
    LoggingFileConfig,
    configure_log_context,
    configure_log_sink,
)

file_sink = configure_log_sink(
    name="app_file",
    sink_cls=FileLogSink,
    config=LoggingFileConfig(
        file_path="app.log",
    ),
)

ctx = configure_log_context(
    "my_app",
    log_sink=file_sink,
)

ctx.log_info_event(
    event="app.started",
    payload={
        "service": "demo",
        "mode": "local",
    },
)
```

After that, events from the `my_app` context will be written to `app.log`.

## File path, mode, and encoding

The main `LoggingFileConfig` setting is the file path.

```python
from mvx.common.logger import LoggingFileConfig

config=LoggingFileConfig(
    file_path="app.log",
)
```

The file open mode and encoding can also be specified.

```python
from mvx.common.logger import LoggingFileConfig

config=LoggingFileConfig(
    file_path="app.log",
    mode="a",
    encoding="utf-8",
)
```

`mode` is passed to the standard `logging.FileHandler` and controls how the file is opened.

For example, mode `a` appends new records to the end of the file, while mode `w` overwrites the file when the handler is opened.

`encoding` defines the encoding used for writing.

## Log format

`FileLogSink` adapts `LogEvent` to standard `logging` in the same way as the stream sink.

By default, the log format is:

```text
%(asctime)s %(levelname)s: %(message)s %(payload)s
```

For a typical event, this produces a record like this:

```text
2026-05-12 14:30:25 INFO: my_app.app.started {'service': 'demo', 'mode': 'local'}
```

If a different line format is needed, it can be set through `LoggingFileConfig`.

```python
from mvx.common.logger import (
    configure_log_sink,
    LoggingFileConfig,
    FileLogSink,
)

file_sink = configure_log_sink(
    name="app_file_compact",
    sink_cls=FileLogSink,
    config=LoggingFileConfig(
        file_path="app.log",
        log_format="%(levelname)s: %(message)s %(payload)s",
    ),
)
```

In this case, the timestamp will not be included in the line because it is not present in `log_format`.

## Date format

If `%(asctime)s` is used in `log_format`, the date format can be changed with `date_format`.

```python
from mvx.common.logger import (
    configure_log_sink,
    LoggingFileConfig,
    FileLogSink,
)

file_sink = configure_log_sink(
    name="app_file_custom_date",
    sink_cls=FileLogSink,
    config=LoggingFileConfig(
        file_path="app.log",
        log_format="%(asctime)s %(levelname)s: %(message)s %(payload)s",
        date_format="%H:%M:%S",
    ),
)
```

In this case, time will be written in a shorter format.

## Logging handler level

`LoggingFileConfig` allows setting the level of the standard `logging` handler.

```python
from mvx.common.logger import (
    configure_log_sink,
    LoggingFileConfig,
    FileLogSink,
    LogLevel,
)

file_sink = configure_log_sink(
    name="app_file_warning",
    sink_cls=FileLogSink,
    config=LoggingFileConfig(
        file_path="app.log",
        level=LogLevel.WARNING,
    ),
)
```

This setting applies to the standard `logging` handler.

It does not replace the context event policy. The event policy decides which events the context passes to the sink by checking event metadata before the final payload is prepared. The handler level is applied later, inside the file sink, during delivery through standard `logging`.

## Custom formatter factory

If `log_format` and `date_format` are not enough, a custom `formatter_factory` can be provided.

`formatter_factory` receives `log_format` and `date_format` and must return a `logging.Formatter` object.

```python
import logging

from mvx.common.logger import (
    FileLogSink,
    LoggingFileConfig,
    configure_log_context,
    configure_log_sink,
)


def make_formatter(log_format: str, date_format: str) -> logging.Formatter:
    return logging.Formatter(
        fmt=log_format,
        datefmt=date_format,
    )


file_sink = configure_log_sink(
    name="app_file_custom_formatter",
    sink_cls=FileLogSink,
    config=LoggingFileConfig(
        file_path="app.log",
        log_format="%(asctime)s %(levelname)s: %(message)s %(payload)s",
        date_format="%Y-%m-%d %H:%M:%S",
        formatter_factory=make_formatter,
    ),
)

ctx = configure_log_context(
    "my_app",
    log_sink=file_sink,
)
```

In normal cases, a custom formatter factory is not needed.

It is useful when a custom subclass of `logging.Formatter` should be used, or when formatters should be created in a centralized way.

Important: the formatter works at the standard `logging.LogRecord` level, not directly with the original `LogEvent`.

`MVX Logger` first adapts `LogEvent` to `LogRecord`, and then the standard formatter creates the line written to the file.

## Logging filters

`LoggingFileConfig` can also receive standard `logging` filters.

A filter may be a `logging.Filter` object or a callable compatible with the Python `logging` filter mechanism.

```python
import logging

from mvx.common.logger import (
    FileLogSink,
    LoggingFileConfig,
    configure_log_context,
    configure_log_sink,
)


def only_app_events(record: logging.LogRecord) -> bool:
    return record.getMessage().startswith("my_app.")


file_sink = configure_log_sink(
    name="app_file_filtered",
    sink_cls=FileLogSink,
    config=LoggingFileConfig(
        file_path="app.log",
        filters=(only_app_events,),
    ),
)

ctx = configure_log_context(
    "my_app",
    log_sink=file_sink,
)
```

Filters are applied inside the standard `logging` handler.

Like the handler level, filters do not replace the context event policy. They are an additional standard `logging` mechanism applied during event delivery through the file sink.

## Asynchronous delivery

`FileLogSink` is an asynchronous sink.

This means that the public logging call does not write to the file directly.

The calling code passes an event to the sink and continues working, while the file sink buffers the event and writes it to the file inside its own runtime.

This approach is especially important for code that should not be blocked by file I/O.

At the user level, the basic model remains the same:

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")

ctx.log_info_event(
    event="app.started",
    payload={
        "service": "demo",
    },
)
```

The code emits an event. The sink is responsible for delivery.

## When to use FileLogSink

`FileLogSink` is useful when you need to:

* write events to a file;
* preserve logs between application runs;
* get a file for local diagnostics;
* pass a file to an external agent or log collection system;
* use familiar standard `logging` features without writing to a file synchronously from the calling code.

If simple console output to `stderr` or `stdout` is needed, use `StreamLogSink`.

If delivery should go to an external backend such as Redis, PostgreSQL, syslog, or an HTTP endpoint, that is a job for a separate asynchronous sink.
