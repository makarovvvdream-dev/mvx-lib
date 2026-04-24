# Errors

`mvx.common.errors` provides structured exception types used across MVX packages.

These errors carry both a human-readable message and structured diagnostic data.

## Hierarchy

```text
Exception  
├── StructuredError  
│   ├── ReasonedError  
│   ├── InvalidFunctionArgumentError
│   └── RuntimeExtendedError (RuntimeError)
└── RuntimeUnexpectedError  
```

```{toctree}
:maxdepth: 1

structured_error
reasoned_error
invalid_function_argument_error
runtime_extended_error
runtime_unexpected_error
```