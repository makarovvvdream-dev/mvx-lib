# Thread safety

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`MVX Logger` is designed to be safe for common multi-threaded logging usage.

The main package-level wiring functions are protected by locks. `LogContext` configuration is protected by a per-context lock. Built-in sinks protect their own mutable state. `AsyncioLogSink` is explicitly designed to accept `log(event)` calls from threads other than its own event loop thread.

At the same time, the logger does not provide a transactional configuration snapshot for every event.

A logging call may observe the currently configured event policy, payload processor, sink, and error handling policy at different stages of the pipeline. If another thread reconfigures the context at the same time, the event is still processed safely, but it is not guaranteed to use one single atomic configuration snapshot for all stages.

This distinction is important:

```text
thread-safe access       -> yes, protected mutable state is not corrupted
transactional snapshot   -> no, not guaranteed for a whole log_event() call
```

## Why it exists

Logging is often called from many places at once.

Examples:

```text
worker threads
async runtime callbacks
background services
library code used by application code
sink shutdown paths
package-level reconfiguration
```

The logger must allow concurrent event emission and configuration operations without corrupting registry state, context state, sink lifecycle state, or asynchronous sink queues.

The design uses locks around mutable infrastructure state, but it avoids holding global locks around the whole event pipeline. That keeps logging calls from turning into a giant traffic light for the entire process.

## Main guarantee

The logger provides thread-safe access to its own mutable infrastructure state.

This includes:

* package-level sink and context registries;
* package-level wiring operations;
* `LogContext` local configuration fields;
* `LogPayloadProcessor` configuration fields;
* built-in sink mutable state;
* `AsyncioLogSink` lifecycle state and pending-event accounting.

The logger does not make arbitrary user-provided objects thread-safe.

This includes custom event policies, custom payload processors, custom sinks, payload objects, payload providers, log adapters, and backend clients.

If a user-provided component stores mutable state and is used from multiple threads, that component must provide its own synchronization.

## Package-level wiring lock

Package-level operations are coordinated by:

```python
_log_context_wiring_lock = threading.RLock()
```

Public functions that read or change package-level registry state use this lock:

```text
configure_log_sink()
get_log_sink()
get_configured_log_sink_names()
has_configured_log_sinks()
close_log_sink()
get_root_log_context()
get_log_context()
configure_log_context()
get_log_context_namespaces()
has_log_context()
reset_log_contexts()
reset_logger()
```

This prevents unsafe interleaving between operations that may involve both sink and context registries.

For example, `close_log_sink(name)` checks whether the sink is locally assigned to registered contexts before unregistering and terminating it. That whole package-level operation runs under the wiring lock.

## Sink registry locks

The sink registry has two internal locks:

```text
lifecycle lock
registry lock
```

The lifecycle lock protects operations that create, unregister, or reset sinks.

The registry lock protects access to the dictionary of registered sinks.

This means repeated concurrent calls to `configure_log_sink()` cannot corrupt the registry.

The registry behavior remains deterministic at the name/descriptor level:

```text
same name + same descriptor       -> return existing sink
same name + different descriptor  -> raise conflict
```

The registry does not silently replace a sink while another thread is using the same registered name.

## Context registry lock

The context registry has its own lock.

It protects:

```text
context lookup
context insertion
namespace listing
registry clearing
context-chain creation
lookup by local sink
```

This protects the registry dictionary itself.

It does not mean that a returned `LogContext` becomes immutable. Once a context object is returned, its own per-context lock protects its local configuration.

## LogContext configuration lock

Each `LogContext` has a per-context reentrant lock:

```python
self._config_lock = threading.RLock()
```

It protects local context configuration fields:

```text
_log_sink
_event_policy
_payload_processor
_log_error_handling_policy
```

The lock is used by setters, resetters, and property resolution.

For inherited components, a child context reads its own local value under its own lock and then asks the parent if the local value is absent.

This protects context configuration from being corrupted by concurrent set/reset operations.

One small internal flag is intentionally different. `LogContext` uses an internal repeated-error marker to avoid printing the same logging infrastructure failure to `stderr` again and again. This marker is updated outside the context configuration lock on purpose. This is a deliberate tradeoff, not a missing lock around critical state. Under concurrent delivery failures, two threads may race on this marker and print the same infrastructure error more than once. That is acceptable because the possible effect is duplicated fallback output to `stderr`, not corrupted logger configuration, broken context state, or an invalid event pipeline. In other words, the repeated-error marker is best-effort noise reduction. It is not part of the logger's correctness boundary.

## Event emission and configuration changes

`LogContext.log_event()` does not hold the context configuration lock for the whole event pipeline.

Instead, it resolves the needed components at their own stages:

```text
build LogEventMeta
   |
   v
read local event policy
   |
   v
if enabled, resolve payload processor
   |
   v
normalize payload
   |
   v
resolve sink
   |
   v
emit event
```

This means that concurrent reconfiguration can affect future stages of an event.

For example, if one thread is emitting an event while another thread changes the context sink, the event may use whichever sink is resolved at delivery time.

The event pipeline remains safe, but it is not transactional.

The logger does not guarantee this stronger property:

```text
"one log_event() call observes one immutable configuration snapshot"
```

It guarantees the weaker and intended property:

```text
"concurrent reads and writes of logger infrastructure state do not corrupt that state"
```

## Event policy calls

`LogContext.is_event_enabled()` reads the local event policy under the context lock and then calls `policy.is_event_enabled(meta)` outside that lock.

This avoids holding the context lock while user-provided policy code runs.

That is important because policy code may be slow, may call other code, or may itself perform logging.

The consequence is clear:

* the context protects the policy reference;
* the policy implementation must protect its own mutable internal state if it is shared across threads.

## Payload processor thread safety

`LogPayloadProcessor` has its own reentrant lock.

It protects configuration fields:

```text
verbosity_level
max_str_len
max_items
log_adapter_resolver
```

Setters, resetters, and property reads use this lock.

Normalization itself uses effective values read from the processor configuration and then processes the supplied payload.

The processor does not make the payload object immutable.

If another thread mutates a payload mapping, list, object, or custom provider while it is being normalized, the logger does not provide additional protection for that external object.

The safe rule is simple:

```text
payload passed to logging should be treated as stable for the duration of the logging call
```

## Payload providers and adapters

`LogPayloadProvider.to_log_payload()` and type-based log adapters are user-provided code.

The logger calls them during payload normalization, but it does not serialize calls to them globally.

If a provider or adapter uses shared mutable state, it must be thread-safe by itself.

The logger catches provider/adapter failures for fallback normalization, but catching exceptions is not the same as making the provider or adapter safe for concurrent access.

## LogEvent immutability

`LogEventMeta`, `LogEvent`, and `LogSinkDescriptor` are frozen dataclasses with slots.

This makes the event object itself stable after creation:

```python
@dataclass(frozen=True, slots=True)
class LogEvent: ...
```

However, `LogEvent.payload` is a mapping reference.

When payload normalization is used, the logger creates a normalized dictionary for the final event.

When `skip_payload_normalization=True` is used, the original mapping is placed into the event as-is.

In that low-level mode, the caller is responsible for ensuring that the supplied mapping is safe to share with the sink.

## StreamLogSink

`StreamLogSink` protects its mutable state with a reentrant lock.

The lock protects:

```text
closed flag
log delivery through the internal logger
handler removal during close
```

Calls after `close()` are ignored.

The terminator returned by `StreamLogSink.create()` is idempotent and protected by its own lock.

This makes the built-in default stream sink safe for concurrent package-managed use.

## FileLogSink

`FileLogSink` is built on `AsyncioLogSink`.

Public event acceptance is handled by the asynchronous sink base class.

The file handler itself is protected during close by a handler lock:

```text
_handler_lock
_handler_closed
```

The terminator closes the asynchronous runtime and then closes the file handler. The handler close path is idempotent.

This protects file handler shutdown from double-close paths such as graceful stop plus package-level terminator cleanup.

## AsyncioLogSink thread safety

`AsyncioLogSink` is the strongest explicit thread-safety boundary in the logger.

It has a thread lock protecting:

```text
state
last error
start future
stop future
pending counter
```

It uses asyncio thread-safe scheduling primitives:

```text
asyncio.run_coroutine_threadsafe(...)
loop.call_soon_threadsafe(...)
```

This means `start()`, `stop()`, `get_status()`, and `log(event)` can be called from threads outside the sink's event loop thread.

`log(event)` increments the pending counter before scheduling queue insertion. This is why the pending counter can account for events that were accepted by a caller thread but have not yet reached the asyncio queue.

The queue itself belongs to the sink event loop. Cross-thread entry into the queue is done through `call_soon_threadsafe()`.

## AsyncioLogSink subclass responsibility

`AsyncioLogSink` protects the base runtime shell.

It does not automatically protect mutable state introduced by subclasses.

Subclass hooks run on the sink event loop:

```text
_on_starting()
_dispatch_core(event)
_on_stopped()
```

If subclass state is accessed only inside those hooks, it normally belongs to the sink event loop and does not need an additional thread lock.

If subclass state is also accessed from caller threads, terminators, tests, or external management APIs, the subclass must protect that state itself.

The base class cannot know which backend client objects are thread-safe.

## Wait handles

`AsyncioLogSinkWaitHandle` uses `concurrent.futures.Future` internally.

This lets a caller wait synchronously:

```python
result = sink.start().wait()
```

or asynchronously:

```python
result = await sink.start()
```

The wait handle is a bridge between caller threads and sink lifecycle operations scheduled into the sink event loop.

It reports operation success or failure as `AsyncioLogSinkOpResult`.

## Terminators

Package-managed sinks return a `LogSinkTerminator`.

Built-in terminators are idempotent.

`StreamLogSink.create()` uses a terminator lock to call `close()` only once.

`AsyncioLogSink.create()` uses a terminator lock to stop the sink runtime and event loop thread only once.

The threaded `AsyncioLogSink` terminator cannot be called from its own event loop thread, because that would deadlock shutdown.

## What is not guaranteed

The logger does not guarantee global serialization of all logging activity.

It does not guarantee that a `log_event()` call sees one atomic snapshot of all context configuration.

It does not make user-provided sinks, policies, processors, providers, adapters, payload objects, or backend clients thread-safe.

It does not make `skip_payload_normalization=True` payload mappings immutable or safe to mutate after logging.

It does not guarantee that closing or resetting package-level infrastructure waits for every manually created context or direct sink outside the package registries.

It does not provide cross-process safety. The guarantees here are about threads inside one Python process.

## Practical rules

Use package-level configuration functions when sinks and contexts are shared globally.

Avoid reconfiguring active contexts frequently while many threads are logging unless mixed-stage configuration visibility is acceptable.

Treat payload objects as stable during logging.

Make custom policies, payload processors, adapters, providers, and sinks thread-safe if they hold shared mutable state.

For asynchronous backend sinks, prefer subclassing `AsyncioLogSink` and keep backend state confined to the sink event loop unless there is a clear reason to expose it across threads.

Use `reset_logger()` and `close_log_sink()` as lifecycle operations, not as hot-path logging operations.

## Design summary

The logger protects its own mutable infrastructure state with focused locks.

Package-level registries are protected by the wiring and registry locks.

Each `LogContext` protects its local configuration.

`LogPayloadProcessor` protects its configuration.

Built-in sinks protect their mutable state.

`AsyncioLogSink` provides cross-thread event acceptance and lifecycle scheduling through a thread lock and asyncio thread-safe APIs.

The design provides safe concurrent access without turning the whole logging pipeline into one global transaction.
