# Log components

Log components are higher-level utility tools built on top of the core logging infrastructure.

They are not required for manual structured logging with `LogContext`, but they provide reusable patterns for common logging tasks.

Currently this package contains `log_invocation`, a decorator for structured operation logging.

Future components may be added here if they follow the same principle: they should build on top of `LogContext`, events, policies, payload processing, and sinks without becoming part of the core logging infrastructure itself.

```{toctree}
:caption: Log components
:maxdepth: 1

log_invocation/overview
```