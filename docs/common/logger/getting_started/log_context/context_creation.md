# Creating and getting a context

The main function for creating a logging context is `configure_log_context()`.

Use it when you want a context to exist after the call.

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")
```

If a context with this name does not exist yet, it is created.

If it already exists, the same context object is returned.

```python
from mvx.common.logger import configure_log_context

ctx1 = configure_log_context("my_app")
ctx2 = configure_log_context("my_app")

assert ctx1 is ctx2
```

The word `configure` is important here. A repeated call can also update the existing context.

For example, this call returns the existing `my_app` context and changes its verbosity level:

```python
from mvx.common.logger import LogVerbosityLevel, configure_log_context

ctx = configure_log_context(
    "my_app",
    verbosity_level=LogVerbosityLevel.MAXIMUM,
)
```

So `configure_log_context()` should be used when the caller is allowed to create or update the context.

## Getting an existing context

If you only want to look up a context and do not want to create or update anything, use `get_log_context()`.

```python
from mvx.common.logger import get_log_context

ctx = get_log_context("my_app")
```

If the context exists, it is returned.

If no context with that name has been created, the function returns `None`.

This makes `get_log_context()` useful when context creation is expected to happen elsewhere and missing context should be handled explicitly by the caller.
