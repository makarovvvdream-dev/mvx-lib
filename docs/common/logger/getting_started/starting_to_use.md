# Starting to use MVX Logger
```{contents} Contents:
:depth: 1
:local:
```
## First log event

You can start almost the same way as with the standard `logging` package: get a logging context and write the first event. By analogy with standard `logging`, a logging context is identified by its name. In the example below, this name is `my_app`.

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")

ctx.log_info_event(
    event="app.started",
    payload={
        "service": "demo",
        "mode": "local",
    },
)
```

After running this code, a log line will be written to `stderr`:

```text
2026-05-12 14:30:25 INFO: my_app.app.started {'service': 'demo', 'mode': 'local'}
```

Naturally, the time at the beginning of the line will be the current time.

## What matters in this example

The code looks almost as familiar as ordinary `logging`: there is a logging context name, `my_app`, and there is a logging call.

But instead of passing a preformatted string, the code passes two separate parts from which the final `stderr` record is produced:

```python
event="app.started"
```

and

```python
payload={
    "service": "demo",
    "mode": "local",
}
```

In other words, user code does not manually assemble the log text. It reports which event happened and passes the data of that event.

After that, `MVX Logger` delivers the event to its destination.

## Event delivery

In practice, neither the logging context nor the calling code knows the final destination of the event. Their job is to emit the event and forget about it.

It is important that the calling code does not manage event delivery manually. Whether the event is delivered immediately or handed off to an asynchronous runtime is the responsibility of the selected `log sink`.

Event delivery is handled by what we call a `log sink`. The logging context gives the event to the sink and no longer deals with it.

A `log sink` may be synchronous or asynchronous by nature.

In the first case, event delivery is performed synchronously with the code that calls `log_info_event`, in the same thread as the calling code. This model is reasonable if and only if event delivery is not associated with I/O waits: network access, network connections, slow disks during file writes, and so on. Otherwise, the calling code will be synchronously blocked for the whole waiting period, which is highly undesirable.

In the second case, the `log sink` runs in its own dedicated thread, buffers events internally, and delivers them to their destination as soon as it can.

At the moment, the most familiar `log sink` types for developers are implemented:

* `stream sink` — a synchronous sink fully based on the `logging` ecosystem, with the ability to choose between `stderr` and `stdout`. This is the simplest implementation, allowing the user to keep familiar logging tools while also getting the rest of the `MVX Logger` features. For more fine-grained configuration, it is possible to set a custom log format, date format, logging level, formatter factory, and logging filters.

* `file sink` — an asynchronous sink that is also based on the `logging` ecosystem, but fully separates the calling code thread from the event delivery thread.

Under the hood, `MVX Logger` provides an abstract class for implementing custom asynchronous sinks without manually dealing with queues, races, asynchronous execution, thread safety, and other runtime delivery details. It can be used, for example, to implement a Redis or PostgreSQL sink. Custom sink creation will be covered in a separate article.

## Returning to our example

What actually happens under the hood in our example, and what do we get after running just a few lines of code?

When `mvx.common.logger` is imported, the `MVX Logger` package is initialized. During this initialization, it:

1. Creates the default `log sink`.

   This is the `stream sink` described above, configured to write to `stderr`. It configures a `logging` handler with default parameters: message format, date format, logging level, and other settings.

2. Creates the root logging context.

   The previously created sink is assigned to it. No policies that restrict event logging are applied. All logging contexts configured later inherit the root logging context settings.

Then the user code runs:

```python
ctx = configure_log_context("my_app")
```

It creates a logging context named `my_app`.

Because this context does not receive its own sink, it uses the sink inherited from the root logging context. In our case, this is the `stream sink` writing to `stderr`.

After that, the following call is executed:

```python
ctx.log_info_event(
    event="app.started",
    payload={
        "service": "demo",
        "mode": "local",
    },
)
```

The logging context receives the `app.started` event with its payload, checks whether the event is enabled, prepares the final `LogEvent`, and passes it to the current sink. The sink then delivers the event to `stderr`.

As a result, we see the line:

```text
2026-05-12 14:30:25 INFO: my_app.app.started {'service': 'demo', 'mode': 'local'}
```

At the first-use level, everything looks familiar: there is a logger name, there is a logging call, and there is an output line.

But internally, `MVX Logger` does not work with a string. It works with an event. This is what later makes it possible to change the sink, delivery format, logging policies, and payload depth without rewriting the code that emits events.
