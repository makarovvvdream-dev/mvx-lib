# File sink

This page documents the ready-to-use file sink.

`FileLogSink` delivers structured logger events to a file through Python's standard `logging` package. It is built on top of `AsyncioLogSink`, so public `log()` calls enqueue events while actual file delivery is handled by the sink runtime.

Use this sink when you want log events written to a file without implementing a custom sink.

## Overview

The file sink stack has two main public pieces:

```text
LoggingFileConfig
    configures the standard-library file handler

FileLogSink
    delivers LogEvent records to the configured file
```

`FileLogSink` adapts `LogEvent` objects to standard-library `logging.LogRecord` objects and passes them to an internal `logging.Logger` configured with a file handler.

## File configuration

`LoggingFileConfig` configures the handler used by `FileLogSink`.

It controls the target file path, minimum level, file mode, encoding, formatting, date formatting, formatter factory, and handler filters.

```{eval-rst}
.. autoclass:: mvx.common.logger.LoggingFileConfig
   :members:
   :member-order: bysource
   :class-doc-from: both
```

## File sink

`FileLogSink` is the ready-to-use sink implementation.

It owns an internal standard-library logger and installs a file handler configured by `LoggingFileConfig`.

```{eval-rst}
.. autoclass:: mvx.common.logger.FileLogSink
   :members: build_descriptor, create
   :member-order: bysource
   :class-doc-from: both
```

`FileLogSink` inherits the public runtime methods from `AsyncioLogSink`, including event acceptance and lifecycle operations. The detailed async sink runtime API is documented separately.

## Package-level registration

A file sink can be registered through the package-level facade.

```python
from mvx.common.logger import (
    FileLogSink,
    LoggingFileConfig,
    configure_log_sink,
)

config = LoggingFileConfig(
    file_path="app.log",
)

sink = configure_log_sink(
    name="file",
    sink_cls=FileLogSink,
    config=config,
)
```

The configured sink can then be assigned to a context through the package-level context facade or passed directly to a `LogContext`.

## Custom file configuration

Use `LoggingFileConfig` to control file handler settings.

```python
from mvx.common.logger import (
    FileLogSink,
    LogLevel,
    LoggingFileConfig,
    configure_log_sink,
)

config = LoggingFileConfig(
    file_path="logs/app.log",
    level=LogLevel.INFO,
    mode="a",
    encoding="utf-8",
)

sink = configure_log_sink(
    name="app_file",
    sink_cls=FileLogSink,
    config=config,
)
```

The file path is passed to Python's standard `logging.FileHandler`.

## Handler behavior

`LoggingFileConfig` creates a standard-library `logging.FileHandler`.

When applied to the internal logger, the configuration:

```text
creates the file handler
sets the formatter
adds configured filters
sets logger and handler levels
removes existing handlers
turns off propagation
attaches the new handler
```

Existing file handlers are closed when replaced. Existing non-file handlers are detached but not closed.

## Asynchronous delivery

`FileLogSink` uses `AsyncioLogSink` as its delivery runtime.

That means the sink is used through the normal sink boundary, but actual file delivery is performed by the async sink dispatcher.

At a high level:

```text
LogContext emits LogEvent
   |
   v
FileLogSink.log(event)
   |
   v
event is accepted by AsyncioLogSink runtime
   |
   v
_dispatch_core(event) writes through the file logger
```

The ready-to-use file sink page does not document every async runtime method. For lifecycle states, wait handles, queue overflow behavior, and custom async sink implementation details, see the `AsyncioLogSink` component page.

## Descriptor behavior

`FileLogSink.build_descriptor()` builds a descriptor used by the package-level sink registry.

For file sinks, the descriptor identity includes:

```text
sink type
resolved file path
logger name
level
format settings
file mode
encoding
filter types
```

The file path is expanded and resolved before it is stored in the descriptor resource key.

This descriptor lets repeated `configure_log_sink()` calls be idempotent when the configuration is the same and conflicting when the same name is reused for a different file sink configuration.

## Closing behavior

The terminator returned by `FileLogSink.create()` stops the async sink runtime and closes the installed file handler.

The file handler is closed even if the sink was created but never started.

Handler closing is idempotent.

