# MVX Common

[![common-ci](https://github.com/makarovvvdream-dev/mvx-lib/actions/workflows/common-ci.yml/badge.svg)](https://github.com/makarovvvdream-dev/mvx-lib/actions/workflows/common-ci.yml)

`mvx-common` is a Python package with foundation-level infrastructure for MVX Python projects.

Its main public components are **MVX Logger** and **MVX Metrics**.

## MVX Logger

MVX Logger is a lightweight structured event logging infrastructure for applications and reusable Python libraries that
need rich and flexible diagnostic visibility.

Most logging tools focus on formatting, routing, or collecting log records. MVX Logger focuses on the step before that:
how code creates structured diagnostic events, controls their diagnostic data, and hands them over for delivery.

### Key features

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

### What MVX Logger is not

MVX Logger is not a replacement for `logging`, `structlog`, `loguru`, or OpenTelemetry.

It is a structured event creation layer that can sit before those tools or other delivery backends.

The goal is not to own the whole observability stack. The goal is to make diagnostic event creation explicit,
structured, configurable, and safe to use across project boundaries.

### MVX Logger documentation

Overview:

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

## MVX Metrics

MVX Metrics is a lightweight metrics infrastructure for applications and reusable Python libraries that need measurable
runtime visibility without binding production code to a specific monitoring backend.

Most monitoring tools focus on collecting, exporting, storing, querying, or visualizing metrics. MVX Metrics focuses on
the step before that: how code reports structured domain events, how metrics interpret those events, and how
quantitative runtime state is built from them.

Production code should not have to manually update raw counters or know which measurements are derived from a specific
operation. It should describe what happened. Metrics decide which events are relevant, how those events affect their
dimensions, and how the resulting state is exposed through snapshots.

### Key features

* emit structured metric events instead of manually updating raw counters in production code
* let production code describe what happened while metrics decide how to interpret events and update quantitative
  dimensions
* keep domain-specific metric meaning close to the component that owns the domain, without coupling that component to
  metric collection, export, storage, or visualization infrastructure
* keep recorders, runtimes, and adapters generic, so they can collect and deliver metric events without knowing
  business-specific metric semantics
* change the final observability backend by replacing recorders or adapters instead of changing event-producing
  production code
* keep metric emission lightweight on the production path while aggregation and delivery are handled outside the
  business operation flow
* support both synchronous and asynchronous event-producing code through a small recorder contract and runtime boundary
* safely bridge thread and event-loop boundaries for metric processing
* expose metric snapshots as stable aggregated runtime state for diagnostics, tests, dashboards, exporters, and
  higher-level health checks
* provide clear extension points through snapshots, recorder hooks, custom recorders, and metrics runtime support
  without turning the core package into a monitoring platform

### What MVX Metrics is not

MVX Metrics is not a replacement for Prometheus, OpenTelemetry, StatsD, or monitoring dashboards.

It is a backend-neutral metric event and aggregation layer that can sit before those tools or other export backends.

The goal is not to own the whole observability stack. The goal is to make metric event creation, aggregation, and
snapshot exposure explicit, structured, configurable, and safe to use across project boundaries.

### MVX Metrics documentation

Overview:

```text
https://mvx-lib.readthedocs.io/en/latest/common/metrics/overview.html
```

Getting started:

```text
https://mvx-lib.readthedocs.io/en/latest/common/metrics/getting_started/
```

Architecture:

```text
https://mvx-lib.readthedocs.io/en/latest/common/metrics/architecture/
```

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

`mvx-common` is an early public package.

The implementation reflects the needs of the project at this stage. The code is covered with tests, CI checks, and
documentation that describes both ordinary usage and extension points.

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
