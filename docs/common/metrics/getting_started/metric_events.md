# Metric events

```{contents} Contents:
:depth: 1
:local:
```

`MetricEvent` is the input unit of `MVX Metrics`.

A metric event describes something that happened in production code.

It is not a counter.

It is not a metric snapshot.

It is not a monitoring backend message.

It is a structured domain fact that can later be interpreted by one or more metrics.

## Core idea

Production code emits facts.

Metrics decide how those facts become measurements.

For example, production code should not say:

```text
success_total += 1
```

Instead, it should say:

```text
a document save attempt completed successfully
```

Then a metric can decide that this fact should increment `success_total`.

Another metric may ignore the event.

A future metric may use the same event to calculate something else.

This is the main reason metric events exist.

They keep production code focused on what happened, while metrics own the quantitative interpretation.

## A metric event is a structured fact

In the first example, `DocumentStorage.save_document()` emits this event:

```python
DocumentSaveAttemptMetricEvent(
    outcome=DocumentSaveAttemptOutcome.SUCCESS,
)
```

The event type is:

```python
@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"
```

This event says:

```text
document_storage.save.attempt happened
outcome = SUCCESS
```

That is all.

It does not say which counters should be updated.

It does not know which metrics are registered.

It does not know how snapshots are built.

It only carries the domain fact.

## Event type

Every metric event exposes an `event_type`.

```python
@property
def event_type(self) -> str:
    return "document_storage.save.attempt"
```

The event type identifies what kind of fact the event represents.

A good event type should be stable and domain-specific.

For example:

```text
document_storage.save.attempt
tcp_stream.read.attempt
tcp_stream.bytes.received
ldap.bind.attempt
```

The event type is not a metric name.

This distinction matters.

An event type names the fact that happened.

A metric name names the aggregate that measures something based on events.

For the first example:

```text
event type:
    document_storage.save.attempt

metric name:
    document_storage.save.attempts
```

The event is the input.

The metric is the aggregate.

## Event payload

A metric event should carry useful data for metrics.

The first example keeps the payload small:

```python
outcome: DocumentSaveAttemptOutcome
```

That is enough to count:

```text
total
success_total
failure_total
```

But real events often need more information.

For example, a save operation event may carry:

```text
outcome
duration_ms
content_size
```

A network read event may carry:

```text
outcome
bytes_received
duration_ms
```

A database query event may carry:

```text
outcome
duration_ms
rows_returned
```

This data is not the metric state yet.

It is only event payload.

Metrics can later decide how to use it.

## Extending the document example

The first `DocumentStorage` example counts only attempts and outcomes.

Now imagine that later we also want to measure how much data is saved and how long saving takes.

The event can grow from this:

```python
@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"
```

to this:

```python
@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome
    content_size: int
    duration_ms: float

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"
```

The event still represents the same domain fact:

```text
a document save attempt happened
```

But now it carries more information about that fact.

A simple attempt-counting metric may use only `outcome`.

A size metric may use `content_size`.

A duration metric may use `duration_ms`.

The production method still emits one structured event.

Different metrics may use different parts of its payload.

## Payload is not interpretation

This is an important distinction.

The event may carry:

```text
duration_ms = 12.4
content_size = 1840
outcome = SUCCESS
```

But the event does not decide what those values mean.

It does not decide whether `12.4 ms` is fast or slow.

It does not decide whether `1840 bytes` should be added to a total, averaged, bucketed, ignored, or exported.

That is the metric's responsibility.

For example:

```text
DocumentSaveAttemptsMetric
    uses outcome

DocumentSavedBytesMetric
    uses content_size

DocumentSaveDurationMetric
    uses duration_ms
```

The same event can provide useful data to all of them.

## Why production code should emit events, not counters

If production code updates counters directly, it starts to know too much.

It must know:

```text
which counters exist
which counter changes on success
which counter changes on failure
which counter uses bytes
which counter uses duration
which backend receives the values
```

That creates tight coupling between business logic and observability logic.

With metric events, production code only knows:

```text
what happened
what useful data belongs to that fact
```

Metrics handle the rest.

This makes it easier to add, remove, or change metrics later without rewriting the business method.

## Where metric events should live

Metric events should normally live close to the production component that emits them.

This is because event payload is domain-specific.

The `DocumentStorage` component knows what a document save attempt means.

The TCP stream engine knows what a read attempt, write attempt, TLS error, or remote disconnect means.

The LDAP adapter knows what bind, search, or schema load attempts mean.

Those event definitions belong near the domain code, not inside generic metrics infrastructure.

The recorder and runtime should not need to understand these event types.

They only move events to metrics.

## Event fields should be useful to metrics

Metric event payload should be selected for measurement.

Good event fields are values that metrics may reasonably need.

Examples:

```text
outcome
duration_ms
bytes_sent
bytes_received
rows_returned
retry_count
refusal_reason
error_category
```

Poor event fields are values that are only useful for detailed diagnostics and not for measurement.

Those usually belong to logging, not metrics.

For example:

```text
full document content
full request body
full exception traceback
large user payload
debug-only object dumps
```

`MVX Logger` and `MVX Metrics` solve different problems.

Logs can carry diagnostic context.

Metric events should carry compact measurement input.

## Keep the first event small

A metric event does not have to include every possible value from the beginning.

The first version of the document example uses only:

```python
outcome: DocumentSaveAttemptOutcome
```

That is enough for the first metric.

Later, if the component needs more measurements, the event can be extended deliberately.

The goal is not to make every event large.

The goal is to include the data that metrics need in order to measure the behavior of the component.

## What happens after the event is emitted

After production code emits a metric event, the recorder receives it.

The recorder does not interpret the event.

It does not know the business meaning of the event payload.

It dispatches the event to registered metrics.

Each metric decides whether it can handle the event.

If a metric accepts the event, it updates its own measured state.

Later, snapshots expose that state.

The path is:

```text
production method
    -> metric event
    -> recorder
    -> registered metrics
    -> metric state
    -> snapshot
```

## Summary

A `MetricEvent` is a structured domain fact.

It should describe what happened and carry compact data that metrics may need.

It should not update counters.

It should not know which metrics will consume it.

It should not know how results will be exported or displayed.

The production component emits the event.

Metrics interpret it.
