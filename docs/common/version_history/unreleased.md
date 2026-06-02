# Unreleased

## Changed

* Updated `LogContextProviderProto.get_log_context()` to allow returning `None`.
* Updated `log_invocation` so a method-based context provider can opt out of logging for a single call by returning `None`.
* Updated `log_invocation` documentation and examples to describe the new method-based context opt-out behavior.

## Behavior

When `ctx` is passed explicitly to `log_invocation`, behavior is unchanged.

When `ctx` is not passed and the first positional argument provides `LogContextProviderProto`, `get_log_context()` may now return either a logging context or `None`.

If it returns a context, `log_invocation` records lifecycle outcomes as before.

If it returns `None`, the decorated callable is executed normally, and `log_invocation` emits no `invoke`, `success`, `failed`, or `cancelled` outcomes for that call.

Returning `None` from `get_log_context()` is not treated as a setup failure.