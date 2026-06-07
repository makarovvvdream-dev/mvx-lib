# From the author

```{image} ../_static/vladimir_makarov.jpeg
:alt: Vladimir Makarov
:width: 180px
:align: right
```

`mvx-common` appeared because I needed foundation-level infrastructure for a real project.

The first visible need was structured logging.

I looked for an existing solution, but did not find one that matched the shape I wanted: structured events, explicit
payload control, context-based configuration, predictable sink boundaries, and operation-level logging, while still
remaining lightweight, flexible, and free from heavy infrastructure or administration
requirements.

So I built the logging component I needed.

That became **MVX Logger**.

Later, the same project needed metrics.

Observability had to be present from the beginning, but I did not want to attach production code to a specific
monitoring platform too early. At that stage, the final platform was not the most important decision. What mattered more
was to make the code measurable and testable immediately, while keeping the choice of Prometheus, OpenTelemetry, StatsD,
a custom dashboard, or another backend for later.

That later point should come when there is real load, real operational pressure, and a real need for online monitoring,
not just because the code has reached the first version of its metrics layer.

So I built a lightweight internal metrics layer where production code emits structured metric events, metrics aggregate
those events, recorders expose snapshots, and external monitoring can be connected later through adapters.

That became **MVX Metrics**.

These two components solve different problems.

MVX Logger is about diagnostic visibility:

```text
what happened
where it happened
with which context
with which payload
```

MVX Metrics is about measurable runtime behavior:

```text
how many times it happened
with which outcome
how state changes over time
which current values can be inspected
```

Together, they form the first foundation layer of `mvx-common`.

The current functionality reflects the needs of the project at this stage. It is not a claim that the package already
covers every possible logging, metrics, or observability scenario. It is a practical implementation with clear
boundaries, meaningful tests, and room to grow.

The design is intentionally modular.

Some users may only need MVX Logger for structured operation logging.

Some may use MVX Metrics to make library components observable and testable before choosing a final monitoring platform.

Some may use both: logs for diagnostic events, metrics for aggregated runtime state.

The important idea is that production code should not be tightly coupled to final delivery infrastructure. A library or
application should be able to emit structured events and measurable facts through stable internal boundaries, while
sinks, recorders, runtimes, adapters, exporters, and external platforms can evolve around that code.

I believe this gives the project a solid foundation for further development, as long as new features grow from real use
cases rather than from collecting features for their own sake.

This is my first public project. I tried to make the documentation useful, explain the architecture clearly, and cover
the code with meaningful tests.

Still, mistakes are possible. If you find an error in the code or in the documentation, please let me know.

I am open to collaboration, feedback, and practical feature requests. If you are using `mvx-common`, evaluating it, or
missing something important for your project, I will be glad to discuss it.

Source code is available on [GitHub](https://github.com/makarovvvdream-dev/mvx-lib).

For bugs and feature requests, please use GitHub Issues.

For collaboration or direct discussion, contact me at [makarovvv.dream@gmail.com](mailto:makarovvv.dream@gmail.com).

All the best,

Vladimir Makarov
