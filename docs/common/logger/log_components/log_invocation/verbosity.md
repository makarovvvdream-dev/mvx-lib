# Verbosity

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`log_invocation` can make selected payload fields depend on the current logging verbosity.

This is controlled through verbosity-gated field specs:

```text
MAXIMUM:request.debug_info
NORMAL,MAXIMUM:request_id
```

Verbosity gates can be used in:

```text
context_fields
log_kwargs_on_invoke
log_result_on_success
```

The decorator asks the resolved context for its plain verbosity level:

```text
ctx.get_plain_verbosity_level()
```

Then it decides whether each gated field spec should be used.

## What verbosity controls here

In `log_invocation`, verbosity controls field selection.

It answers this question:

```text
Should this selected field be included for the current verbosity level?
```

It does not decide whether the event itself is enabled.

Event enablement belongs to event policy.

It also does not decide where the event is delivered.

Delivery belongs to the resolved sink.

So the boundaries are:

```text
event policy      -> should the event be emitted?
verbosity gate    -> should this field be selected?
payload processor -> how should the selected value be normalized?
sink              -> where should the final LogEvent go?
```

## Gate syntax

A verbosity-gated field spec has two parts:

```text
LEVELS:field_spec
```

The left side contains one or more verbosity levels separated by commas.

The right side contains the normal field spec.

Examples:

```text
MAXIMUM:payload
NORMAL,MAXIMUM:request_id
MINIMAL,NORMAL,MAXIMUM:operation_id
```

If the current plain verbosity level is listed on the left side, the field spec is enabled.

If it is not listed, the field spec is skipped.

## Ungated fields

A field spec without `:` is not gated by verbosity.

```text
request_id
```

Such a field is considered for selection regardless of the current verbosity level.

Example:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=(
        "request_id",
        "MAXIMUM:payload_size=payload.len()",
    ),
)
def send_request(self, request_id: str, payload: bytes) -> None:
    ...
```

Here `request_id` is always selected.

`payload_size` is selected only when the current plain verbosity level is `MAXIMUM`.

## Multiple allowed levels

A gate may list several verbosity levels.

```text
NORMAL,MAXIMUM:request_id
```

This means the field is selected when the current plain verbosity level is either `NORMAL` or `MAXIMUM`.

Example:

```python
@log_invocation(
    "process_message",
    context_fields=(
        "message_id",
        "NORMAL,MAXIMUM:message_size=message.size",
        "MAXIMUM:raw_message=message.raw!",
    ),
)
def process_message(self, message_id: str, message: Message) -> None:
    ...
```

The intended effect is:

```text
message_id
    selected at any verbosity

message_size
    selected at NORMAL and MAXIMUM

raw_message
    selected only at MAXIMUM
```

## Missing verbosity level

If the resolved context has no plain verbosity level, verbosity-gated field specs are skipped.

Ungated field specs are still processed.

Example:

```text
request_id
MAXIMUM:payload
```

If `ctx.get_plain_verbosity_level()` returns `None`:

```text
request_id -> selected
payload    -> skipped
```

This keeps gated fields opt-in. A gated field is included only when the context can report a matching plain verbosity level.

## Empty level list

A field spec with an empty level list behaves like an ungated spec after parsing.

For example:

```text
:request_id
```

has no actual verbosity levels on the left side.

The effective field spec becomes:

```text
request_id
```

In normal documentation and user code, prefer clear ungated specs instead:

```text
request_id
```

Do not write empty gates intentionally.

## Invalid or empty specs

Empty specs are ignored.

A gate with an empty right side is ignored.

Examples:

```text
""
"   "
"MAXIMUM:"
```

These do not produce payload fields.

This is best-effort diagnostic field selection. Bad field specs are skipped instead of failing the decorated operation.

## Verbosity and context_fields

When used with `context_fields`, verbosity gates are evaluated separately for every emitted outcome.

```python
@log_invocation(
    "open",
    context_fields=(
        "state=self.state",
        "MAXIMUM:debug_state=self.debug_state",
    ),
)
async def open(self) -> None:
    ...
```

`state` is always selected.

`debug_state` is selected only at `MAXIMUM` verbosity.

Because `context_fields` are resolved separately for every outcome, a gated dynamic field may still reflect different values for `invoke`, `success`, `failed`, or `cancelled`.

The verbosity gate decides whether the field is included. It does not freeze the field value.

## Verbosity and log_kwargs_on_invoke

When used with `log_kwargs_on_invoke`, verbosity gates affect only the `invoke` outcome.

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=(
        "request_id",
        "NORMAL,MAXIMUM:method",
        "MAXIMUM:payload_preview=payload",
    ),
)
def send_request(self, request_id: str, method: str, payload: bytes) -> None:
    ...
```

Possible behavior:

```text
MINIMAL
    kwargs.request_id

NORMAL
    kwargs.request_id
    kwargs.method

MAXIMUM
    kwargs.request_id
    kwargs.method
    kwargs.payload_preview
```

This lets public API methods keep normal invocation logs compact while allowing richer diagnostic payloads when verbosity is increased.

## Verbosity and log_result_on_success

When used with `log_result_on_success`, verbosity gates affect only the `success` outcome.

```python
@log_invocation(
    "create_session",
    log_result_on_success=(
        "session_id",
        "NORMAL,MAXIMUM:ttl_ms",
        "MAXIMUM:debug_info",
    ),
)
def create_session(self, user_id: str) -> Session:
    ...
```

Possible result payloads:

```text
MINIMAL
    result.session_id

NORMAL
    result.session_id
    result.ttl_ms

MAXIMUM
    result.session_id
    result.ttl_ms
    result.debug_info
```

This keeps result logging explicit and tiered.

## Verbosity and normalization

Verbosity gates decide whether a field is selected.

After a field is selected, its value is normalized by the resolved context.

The selected value may still be normalized differently depending on the payload processor configuration.

That is a separate concern.

```text
verbosity gate
    selects or skips the field

payload normalization
    converts the selected value to a log-ready representation
```

For example, a `MAXIMUM:` field may be skipped entirely at `NORMAL` verbosity.

If it is selected at `MAXIMUM`, the value still goes through normal value normalization.

## Verbosity and unbounded marker

The verbosity gate and the unbounded marker can be used together.

```text
MAXIMUM:headers!
```

The parsing order is:

```text
1. Apply verbosity gate.
2. If the field is enabled, parse the effective field spec.
3. Apply the ! marker to normalization of the selected value.
```

Example:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=(
        "request_id",
        "MAXIMUM:headers!",
    ),
)
def send_request(self, request_id: str, headers: dict[str, str]) -> None:
    ...
```

At `MAXIMUM` verbosity, `headers` is selected and normalized with `unbounded=True`.

At lower verbosity levels, `headers` is not selected.

## Verbosity and aliases

Aliases belong to the right side of the gate.

```text
MAXIMUM:payload_size=payload.len()
```

The left side controls inclusion.

The right side is the actual field spec.

So this means:

```text
include payload_size only at MAXIMUM verbosity
value = len(payload)
```

## Unknown level names

The decorator compares the left-side level names with the plain verbosity level returned by the context.

It does not validate level names against a fixed enum inside `log_invocation`.

This means a typo simply causes the field not to match the current level.

Example:

```text
MAXMUM:payload
```

This is parsed as a gate for the level name `MAXMUM`.

Unless the context returns exactly `MAXMUM`, the field is skipped.

Use tests or careful review for verbosity-gated specs that protect important diagnostic fields.

## Recommended pattern

Use verbosity gates to widen payloads gradually.

A common pattern is:

```python
@log_invocation(
    "send_request",
    context_fields=(
        "request_id",
    ),
    log_kwargs_on_invoke=(
        "method",
        "NORMAL,MAXIMUM:payload_size=payload.len()",
        "MAXIMUM:payload_preview=payload",
    ),
)
def send_request(self, request_id: str, method: str, payload: bytes) -> None:
    ...
```

The idea is:

```text
always
    stable identifiers and safe operation context

NORMAL
    useful compact diagnostics

MAXIMUM
    larger or more detailed diagnostic values
```

Do not use verbosity gates to hide secrets. Sensitive values should not be selected at any verbosity level unless that is an explicit and safe design decision.

## Design summary

Verbosity gates let `log_invocation` include richer payload fields only when the current context verbosity allows them.

Ungated fields are always considered.

Gated fields are selected only when `ctx.get_plain_verbosity_level()` returns a matching level name.

Verbosity controls field selection, not event enablement and not sink delivery.

After a field is selected, normal payload normalization still applies.
