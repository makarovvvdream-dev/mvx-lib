# MVX Common

`mvx-common` is a Python package with common utilities used by MVX Python projects.

It provides reusable infrastructure for:

```text
structured errors
public API error normalization
asyncio cancellation handling
structured logging
```

The package is part of the larger `mvx-lib` repository, which is intended to host multiple MVX Python packages under one documentation site.

## Current version

Current package version: `0.2.0`

The version is defined in:

```text
common/pyproject.toml
```

## Requirements

`mvx-common` requires Python 3.11 or newer.

```text
Python >= 3.11
```

## Installation for development

From the package directory:

```bash
cd common
python -m pip install -e ".[dev]"
```

To install documentation dependencies as well:

```bash
python -m pip install -e ".[dev,docs]"
```

## Running tests

From the package directory:

```bash
cd common
pytest
```

## Building documentation

From the repository root:

```bash
scripts/docs.sh
```

The built HTML documentation is written to:

```text
docs/_build/html
```

## Main components

### Structured errors

`mvx-common` provides structured error classes for predictable diagnostics.

The error layer supports stable reason codes, structured details, causes, and log-ready payloads.

It is useful when public APIs should expose meaningful errors instead of leaking arbitrary implementation exceptions.

### Public API error normalization

`api_error_processor` helps define public error boundaries for library components.

It preserves declared public errors, preserves cancellation, and wraps unexpected internal exceptions into a configured public unexpected-error type.

This keeps library APIs predictable while preserving diagnostic information through exception causes.

### Asyncio cancellation helper

`run_with_cancellation_policy` provides explicit cancellation handling strategies for asyncio workflows.

It is useful when internal work must follow a clear cancellation policy instead of relying on ad-hoc task behavior.

### Structured logger

The structured logger provides a modular logging system for library-grade diagnostics.

It includes:

```text
LogEvent and LogEventMeta
LogContext
payload processing
event policies
sink contracts
ready-to-use stream and file sinks
AsyncioLogSink for custom async backends
log_invocation for public API operation logging
```

The logger can be used through package-level configuration or through directly owned `LogContext` objects.

It is designed to work inside reusable libraries without forcing a single global logging configuration or delivery backend.

## log_invocation

`log_invocation` is a logging component for public API operations.

It records operation lifecycle outcomes:

```text
invoke
success
failed
cancelled
```

It is intended for methods that represent meaningful public operations, not for every internal helper function.

When combined with `api_error_processor`, it is especially useful at library boundaries: internal errors can be normalized into public error types, and the logger records the same public operation outcome that the caller observes.

## Documentation

The documentation covers:

```text
structured errors
common helpers
logger getting started
logger architecture
logger API reference
log_invocation guide and examples
logger advanced usage
version history
```

The documentation is intentionally detailed. Public APIs, extension points, ownership boundaries, and design rules are documented explicitly.

## Project status

This is an early public project.

The implementation reflects the needs of the project at this stage. The code is covered with tests, and the documentation aims to describe both ordinary usage and extension points clearly.

Feedback, bug reports, documentation corrections, and practical feature requests are welcome.

## Contact

For questions, feedback, or collaboration proposals:

```text
makarovvv.dream@gmail.com
```

## Author

Vladimir Makarov
