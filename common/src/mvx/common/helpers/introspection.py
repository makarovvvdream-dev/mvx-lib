# common/src/mvx/common/helpers/introspection.py
from __future__ import annotations

from typing import Callable

__all__ = ("get_func_module_and_qualname",)


def get_func_module_and_qualname(func: Callable) -> tuple[str, str]:
    """
    Return module and qualified name of a callable.

    Args:
        func: Callable object.

    Returns:
        Tuple of (module, qualname).
    """
    module = getattr(func, "__module__", "<unknown>")
    qualname = getattr(func, "__qualname__", getattr(func, "__name__", "<unknown>"))
    return module, qualname
