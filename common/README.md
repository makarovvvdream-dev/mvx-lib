# MVX Common

[![common-ci](https://github.com/makarovvvdream-dev/mvx-lib/actions/workflows/common-ci.yml/badge.svg)](https://github.com/makarovvvdream-dev/mvx-lib/actions/workflows/common-ci.yml)

`mvx-common` is a Python package with foundation-level infrastructure for MVX Python projects.

Its main public component is **MVX Logger**.

MVX Logger is a lightweight structured event logging infrastructure for applications and reusable Python libraries that need rich and flexible diagnostic visibility.

Most logging tools focus on formatting, routing, or collecting log records. MVX Logger focuses on the step before that: how code creates structured diagnostic events, controls their diagnostic data, and hands them over for delivery.

## Key features

* log structured events with rich, flexible, and structured diagnostic info instead of hand-written log strings
* control logging width through event policies without changing calling code, focusing logs on current diagnostic needs
* keep event diagnostic data well-shaped and clean through centralized payload normalization
* change logging destination and formatting without changing event-producing code
* deliver events to standard logging streams, files, async sinks, or custom backends
* keep the logging path fast and thread-safe while slow backends are handled outside the business flow
* log standardized business operation lifecycles without writing logging code inside the operation body
* capture selected call arguments, results, errors, and context fields as structured event diagnostic info
* keep business logic focused on the operation itself instead of repetitive try/except logging blocks
* start using it almost as simply as regular logging, then move to deeper structured diagnostics when needed

## What MVX Logger is not

MVX Logger is not a replacement for `logging`, `structlog`, `loguru`, or OpenTelemetry.

It is a structured event creation layer that can sit before those tools or other delivery backends.

The goal is not to own the whole observability stack. The goal is to make diagnostic event creation explicit, structured, configurable, and safe to use across project boundaries.

## Other utilities

`mvx-common` also includes smaller foundation utilities used by MVX packages:

```text
structured errors
public API error normalization helpers
asyncio cancellation helpers
```

These utilities support predictable public errors, explicit cancellation behavior, and consistent diagnostics.

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

## Running checks

From the package directory:

```bash
scripts/check.sh
```

The check script runs:

```text
black --check
ruff check
mypy
pytest with branch coverage
```

The package requires at least 90% branch coverage.

## Documentation

Full documentation:

```text
https://mvx-lib.readthedocs.io/en/latest/
```

MVX Logger overview:

```text
https://mvx-lib.readthedocs.io/en/latest/common/logger/overview.html
```

Getting started:

```text
https://mvx-lib.readthedocs.io/en/latest/common/logger/getting_started/
```

Advanced usage:

```text
https://mvx-lib.readthedocs.io/en/latest/common/logger/advanced_usage/
```

`log_invocation` guide:

```text
https://mvx-lib.readthedocs.io/en/latest/common/logger/log_components/log_invocation/overview.html
```

Version history:

```text
https://mvx-lib.readthedocs.io/en/latest/common/version_history/
```

## Building documentation

From the repository root:

```bash
scripts/docs.sh
```

The built HTML documentation is written to:

```text
docs/_build/html/
```

## Project status

This is an early public project.

The implementation reflects the needs of the project at this stage. The code is covered with tests, CI checks, and documentation that describes both ordinary usage and extension points.

Feedback, bug reports, documentation corrections, and practical feature requests are welcome.

## Contributing

Repository-level contribution rules are documented in:

```text
../CONTRIBUTING.md
```

## License

`mvx-common` is licensed under the Apache License, Version 2.0.

See:

```text
LICENSE
NOTICE
```

## Author

Vladimir Makarov

Contact:

```text
makarovvv.dream@gmail.com
```
