# Events

These models describe structured log events.

`LogEventMeta` identifies an event before payload delivery. It is the metadata object used by event policies.

`LogEvent` is the completed structured event delivered to sinks.

```{eval-rst}
.. autoenum:: mvx.common.logger.LogLevel
```

```{eval-rst}
.. autoclass:: mvx.common.logger.LogEventMeta
   :members:
   :exclude-members: __init__
   :class-doc-from: class

.. autoclass:: mvx.common.logger.LogEvent
   :members:
   :exclude-members: __init__
   :class-doc-from: class
```