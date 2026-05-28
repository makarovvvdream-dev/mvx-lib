# Stream sink

This page documents the ready-to-use stream sink.

`StreamLogSink` delivers structured logger events to a standard stream through Python's standard `logging` package. It is the default lightweight sink for console-style output.

Use this sink when you want log events written to `stdout` or `stderr` without building a custom sink.

## Overview

The stream sink stack has three public pieces:

```text
LogStreamOutput
    selects stdout or stderr

LoggingStreamConfig
    configures the standard-library logging handler

StreamLogSink
    delivers LogEvent records to the configured stream
```

`StreamLogSink` adapts `LogEvent` objects to standard-library `logging.LogRecord` objects and passes them to an internal `logging.Logger`.

## Stream target

`LogStreamOutput` defines supported standard stream targets.

```{eval-rst}
.. autoenum:: mvx.common.logger.LogStreamOutput
```

## Stream configuration

`LoggingStreamConfig` configures the handler used by `StreamLogSink`.

It controls the stream target, minimum level, formatting, date formatting, formatter factory, and handler filters.

```{eval-rst}
.. autoclass:: mvx.common.logger.LoggingStreamConfig
   :members:
   :member-order: bysource
   :class-doc-from: both
```

## Stream sink

`StreamLogSink` is the ready-to-use sink implementation.

It owns an internal standard-library logger and installs a stream handler configured by `LoggingStreamConfig`.

```{eval-rst}
.. autoclass:: mvx.common.logger.StreamLogSink
   :members: build_descriptor, create, log, close
   :member-order: bysource
   :class-doc-from: both
```

## Package-level registration

A stream sink can be registered through the package-level facade.

```python
from mvx.common.logger import configure_log_sink, StreamLogSink

sink = configure_log_sink(
    name="stderr",
    sink_cls=StreamLogSink,
)
```

If no configuration is supplied, `StreamLogSink` uses a default `LoggingStreamConfig` targeting `stderr`.

## Custom stream configuration

Use `LoggingStreamConfig` to choose the stream target and standard logging settings.

```python
from mvx.common.logger import (
    LogLevel,
    LogStreamOutput,
    LoggingStreamConfig,
    StreamLogSink,
    configure_log_sink,
)

config = LoggingStreamConfig(
    stream_output=LogStreamOutput.STDOUT,
    level=LogLevel.INFO,
)

sink = configure_log_sink(
    name="stdout",
    sink_cls=StreamLogSink,
    config=config,
)
```

The configured sink can then be assigned to a context through the package-level context facade or passed directly to a `LogContext`.

## Handler behavior

`LoggingStreamConfig` creates a standard-library `logging.StreamHandler`.

When applied to the internal logger, the configuration:

```text
creates the handler
sets the formatter
adds configured filters
sets logger and handler levels
removes existing handlers
turns off propagation
attaches the new handler
```

Existing file handlers are closed when replaced. Existing non-file handlers are detached but not closed.

## Descriptor behavior

`StreamLogSink.build_descriptor()` builds a descriptor used by the package-level sink registry.

For stream sinks, the descriptor identity includes:

```text
sink type
logger name
stream target
level
format settings
filter types
```

This descriptor lets repeated `configure_log_sink()` calls be idempotent when the configuration is the same and conflicting when the same name is reused for a different stream sink configuration.

## Closing behavior

`StreamLogSink.close()` removes the installed handler from the internal logger.

For standard output and standard error, streams are detached but not closed.

Repeated `close()` calls are ignored.

The package-level sink terminator returned by `StreamLogSink.create()` is also idempotent.

