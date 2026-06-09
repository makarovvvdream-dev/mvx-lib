# mvx-lib

`mvx-lib` is a monorepo for MVX Python packages.

The repository is intended to host related Python packages under one codebase and one documentation site. Each package
is developed, tested, versioned, and published as a package-level unit.

The first implemented package is `mvx-common`.

## mvx-common

`mvx-common` provides foundation-level infrastructure for MVX Python projects.

Its main public components are **MVX Logger** and **MVX Metrics**.

### MVX Logger

MVX Logger is a lightweight structured event logging infrastructure for applications and reusable Python libraries that
need rich and flexible diagnostic visibility.

Most logging tools focus on formatting, routing, or collecting log records. MVX Logger focuses on the step before that:
how code creates structured diagnostic events, controls their diagnostic data, and hands them over for delivery.

**Key features**

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

MVX Logger is not a replacement for `logging`, `structlog`, `loguru`, or OpenTelemetry. It is a structured event
creation layer that can sit before those tools or other delivery backends.

**Start here**

* [Installation](common/installation.md)
* [MVX Logger overview](common/logger/overview.md)
* [Getting started](common/logger/getting_started/index.md)
* [`log_invocation` guide](common/logger/log_components/log_invocation/overview.md)
* [Advanced usage](common/logger/advanced_usage/index.md)
* [Version history](common/version_history/index.md)

### MVX Metrics

MVX Metrics is a lightweight metrics infrastructure for applications and reusable Python libraries that need measurable
runtime visibility without binding production code to a specific monitoring backend.

Most monitoring tools focus on collecting, exporting, storing, querying, or visualizing metrics. MVX Metrics focuses on
the step before that: how code reports structured domain events, how metrics interpret those events, and how
quantitative runtime state is built from them.

Production code should not have to manually update raw counters or know which measurements are derived from a specific
operation. It should describe what happened. Metrics decide which events are relevant, how those events affect their
dimensions, and how the resulting state is exposed through snapshots.

**Key features**

* emit structured metric events instead of manually updating raw counters in production code
* let production code describe what happened while metrics decide how to interpret events and update quantitative
  dimensions
* keep domain-specific metric meaning close to the component that owns the domain, without coupling that component to
  metric collection, export, storage, or visualization infrastructure
* keep recorders, runtimes, and future adapters generic, so they can collect and deliver metric events without knowing
  business-specific metric semantics
* change the final observability backend by replacing recorders or adapters instead of changing event-producing
  production code
* keep metric emission lightweight on the production path while aggregation and delivery are handled outside the
  business operation flow
* support both synchronous and asynchronous event-producing code through a small recorder contract and runtime boundary
* safely bridge thread and event-loop boundaries for metric processing
* expose metric snapshots as stable aggregated runtime state for diagnostics, tests, dashboards, exporters, and
  higher-level health checks
* provide clear extension points for future integrations through snapshots, recorder hooks, custom recorders, and
  metrics runtime support without turning the core package into a monitoring platform

MVX Metrics is not a replacement for Prometheus, OpenTelemetry, StatsD, or monitoring dashboards. It is a
backend-neutral metric event and aggregation layer that can sit before those tools or other export backends.

**Foundation utilities**

`mvx-common` also includes smaller foundation utilities used by MVX packages, including structured errors, public API
error normalization helpers, and asyncio cancellation helpers.

## Documentation sections

```{toctree}
:maxdepth: 1
:caption: COMMON
:titlesonly:

common/from_the_author
common/version_history/index
common/installation
common/errors/index
common/helpers/index
common/logger/overview
common/metrics/overview
```

```{toctree}
:maxdepth: 1
:caption: NETWORKING
:titlesonly:

networking/version_history/index
networking/installation
networking/overview
networking/getting-started/index
networking/shared-foundation-api/index
networking/tcp-stream-engine/index
networking/tcp-pdu-engine/index
networking/message-processor/index
```

```{toctree}
:maxdepth: 1
:caption: SECURITY
```

```{toctree}
:maxdepth: 1
:caption: LDAP
```

## Project repository

Source code is available on [GitHub](https://github.com/makarovvvdream-dev/mvx-lib).