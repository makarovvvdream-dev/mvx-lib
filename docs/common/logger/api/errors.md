# Errors

This page documents logger-level errors.

These errors describe failures in the logger package itself: context errors and package-level sink configuration errors.

`AsyncioLogSink` runtime errors are documented separately on the `AsyncioLogSink` API page.

## Error hierarchy

The logger error hierarchy has two main branches:

```text
LoggerError
   |
   +-- LogContextError
   |      |
   |      +-- LogContextResetError
   |      +-- LogContextUnableToLog
   |
   +-- LogSinkConfigurationError
          |
          +-- LogSinkConfigurationConflictError
          +-- LogSinkDescriptorBuildError
          +-- LogSinkCreateError
          +-- LogSinkCloseError
          +-- LogSinkIsInUseError
```

`LoggerError` is the common base class for logger-specific errors.

```{eval-rst}
.. autoclass:: mvx.common.logger.LoggerError
```

## Context errors

Context errors are raised by `LogContext` operations.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogContextError

.. autoclass:: mvx.common.logger.LogContextResetError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.LogContextUnableToLog
   :class-doc-from: both
```

### Reset errors

`LogContextResetError` is raised when code tries to reset mandatory root context infrastructure.

Root contexts have no parent fallback, so these components cannot be reset:

```text
log sink
payload processor
logging error handling policy
```

Child contexts may reset their local overrides and return to inherited behavior.

### Delivery errors

`LogContextUnableToLog` is raised when a prepared event cannot be delivered and the effective context error handling policy is `RAISE`.

This error describes a logging infrastructure failure, not a domain failure from the code being logged.

## Sink configuration errors

Sink configuration errors are raised by package-level sink registration, creation, close, and reset flows.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogSinkConfigurationError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.LogSinkConfigurationConflictError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.LogSinkDescriptorBuildError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.LogSinkCreateError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.LogSinkCloseError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.LogSinkIsInUseError
   :class-doc-from: both
```

### Configuration conflicts

`LogSinkConfigurationConflictError` is raised when a sink name is already registered with a different descriptor.

The package-level sink registry treats this as idempotent:

```text
same name + same descriptor
```

and this as a conflict:

```text
same name + different descriptor
```

### Descriptor build failures

`LogSinkDescriptorBuildError` wraps an exception raised while building a sink descriptor.

This happens before sink creation.

The descriptor step is used to decide whether a registration request is idempotent or conflicting.

### Sink creation failures

`LogSinkCreateError` wraps an exception raised while creating a sink instance and its terminator.

This error means descriptor creation succeeded, but sink construction failed.

### Sink close failures

`LogSinkCloseError` is raised when one or more sink terminators fail while closing sinks.

It can be raised by:

```text
close_log_sink(...)
reset_logger()
```

The error details contain one entry per failed sink terminator.

### Sink-in-use errors

`LogSinkIsInUseError` is raised when code attempts to close a package-level sink that is still locally assigned to one or more registered contexts.

The sink must be detached from those contexts before it can be closed.

## Boundary with operation errors

Logger errors should not be confused with domain errors raised by application code.

For example, if a decorated operation raises `RuntimeError`, `log_invocation` may emit a `failed` outcome and then re-raise the same `RuntimeError`.

If the logger itself cannot deliver the prepared event, that is a logger infrastructure failure and may surface as `LogContextUnableToLog` depending on the effective error handling policy.

```text
operation failure
    original domain exception

logger delivery failure
    logger error
```
