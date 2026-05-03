# common/src/mvx/common/logger/core/log_adapter_registry.py
from __future__ import annotations

from typing import Any

from .protocols import LogAdapter

__all__ = ("LogAdapter",)


class LogAdapterRegistry:
    def __init__(self) -> None:
        self._adapters_registry: dict[type, LogAdapter] = {}

    def register_log_adapter(
        self,
        entity_type: type,
        log_adapter: LogAdapter,
    ) -> None:
        """
        Register a log adapter for a given Python entity type.

        Parameters
        ----------
        entity_type : type
            The specific or base type of the entity the adapter is responsible for.
            The adapter will be considered for all instances whose type appears in the MRO
            (method resolution order) of a value's specific type.
        log_adapter : Callable[[Any], dict[str, Any]]
            Callable that accepts an instance of `entity_type` (or a subclass) and
            returns a dict[str, Any] suitable for logging. The returned payload
            is used as-is and is not further normalized by the logger.


        Notes
        -----
        - Registering a new adapter for the same entity_type overwrites
          the previous adapter.
        """
        old = self._adapters_registry
        new = dict(old)
        new[entity_type] = log_adapter
        self._adapters_registry = new

    def resolve_log_adapter(
        self,
        value: Any,
    ) -> LogAdapter | None:
        """
        Resolve a log adapter for the given value if any.

        Resolution rules
        ----------------
        - If `value` is None:
            * Returns None immediately.

        - Lookup is performed along the MRO (method resolution order) of the
          value's specific type:

            for cls in type(value).__mro__:
                if cls is object:
                    break
                if cls is registered:
                    return that adapter

          This implies:
            * An adapter registered for a base class will be used for all subclasses,
              unless a more specific adapter is registered for the subclass itself.
            * The first matching class in the MRO wins (most specific type first).

        - If no adapter is found for any class in the MRO:
            * Returns None.

        Parameters
        ----------
        value : Any
            The value for which a log adapter is being resolved.

        Returns
        -------
        LogAdapter | None
            The resolved adapter callable, or None if no adapter matches.
        """
        if value is None:
            return None

        t = type(value)
        registry = self._adapters_registry

        for cls in t.__mro__:
            if cls is object:
                break
            adapter = registry.get(cls)
            if adapter is not None:
                return adapter

        return None
