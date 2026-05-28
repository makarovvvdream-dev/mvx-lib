# Package-level API

This page documents the package-level functions used to configure and access logger-wide objects.

These functions are entry points into the package-level registries and wiring layer. They are separate from object-level APIs such as `LogContext`, sink classes, event models, and payload processors.

Use this page when you need to configure or retrieve named logger resources through the package-level API.

## Sink facade

The sink facade manages package-level sink registration.

```{eval-rst}
.. autofunction:: mvx.common.logger.configure_log_sink

.. autofunction:: mvx.common.logger.get_log_sink

.. autofunction:: mvx.common.logger.get_configured_log_sink_names

.. autofunction:: mvx.common.logger.has_configured_log_sinks

.. autofunction:: mvx.common.logger.close_log_sink
```

## Context facade

The context facade manages package-level log contexts.

```{eval-rst}
.. autofunction:: mvx.common.logger.get_root_log_context

.. autofunction:: mvx.common.logger.get_log_context

.. autofunction:: mvx.common.logger.configure_log_context

.. autofunction:: mvx.common.logger.get_log_context_namespaces

.. autofunction:: mvx.common.logger.has_log_context

.. autofunction:: mvx.common.logger.reset_log_contexts
```

## Full logger reset

`reset_logger()` resets package-level logger state.

```{eval-rst}
.. autofunction:: mvx.common.logger.reset_logger
```

