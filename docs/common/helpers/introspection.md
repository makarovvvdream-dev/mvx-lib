# Function introspection helpers

This module provides small useful helpers.

These helpers are typically used in logging, error processing, and diagnostics.

## get_func_module_and_qualname

Returns the module and qualified name of a callable.

```python
from mvx.common.helpers import get_func_module_and_qualname


def example() -> None:
    pass


module, qualname = get_func_module_and_qualname(example)
```

Example result:

```python
module = "__main__"
qualname = "example"
```

For methods:

```python
from mvx.common.helpers import get_func_module_and_qualname

class Service:
    def method(self) -> None:
        pass

module, qualname = get_func_module_and_qualname(Service.method)
```

```python
module = "__main__"
qualname = "Service.method"
```

## Fallback behavior

If the callable does not provide `__module__` or `__qualname__`, the helper
returns:

```text
<unknown>
```

This keeps the result stable and safe for logging.