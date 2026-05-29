# mvx-lib

`mvx-lib` is a monorepo for MVX Python packages.

The repository is intended to host related Python packages under one codebase and one documentation site. Each package is developed, tested, versioned, and published as a package-level unit.

The first implemented package is `mvx-common`.

## mvx-common

`mvx-common` provides foundation-level infrastructure for MVX Python projects.

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

MVX Logger is not a replacement for `logging`, `structlog`, `loguru`, or OpenTelemetry. It is a structured event creation layer that can sit before those tools or other delivery backends.

`mvx-common` also includes smaller foundation utilities used by MVX packages, including structured errors, public API error normalization helpers, and asyncio cancellation helpers.

## Start here

* [Installation](common/installation.md)
* [MVX Logger overview](common/logger/overview.md)
* [Getting started](common/logger/getting_started/index.md)
* [`log_invocation` guide](common/logger/log_components/log_invocation/overview.md)
* [Advanced usage](common/logger/advanced_usage/index.md)
* [Version history](common/version_history/index.md)

## Documentation sections

```{toctree}
:maxdepth: 1
:caption: COMMON
:titlesonly:

common/version_history/index
common/installation
common/errors/index
common/helpers/index
common/logger/overview
```

```{toctree}
:maxdepth: 1
:caption: NETWORKING
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