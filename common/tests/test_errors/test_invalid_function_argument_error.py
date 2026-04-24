from __future__ import annotations

from typing import Any

from mvx.common.errors import StructuredError, InvalidFunctionArgumentError

# ---------- Group A: basic construction and normalization ----------


def test_a1_basic_payload_and_types() -> None:
    """
    InvalidFunctionArgumentError must capture message, details and cause correctly.
    """
    cause = ValueError("offset must be >= 0")

    err = InvalidFunctionArgumentError(
        func="read_sorted_set_range_by_score",
        arg="offset",
        value=-1,
        cause=cause,
        details={"ctx": "unit-test"},
    )

    # Must be a StructuredError subclass.
    assert isinstance(err, StructuredError)
    assert isinstance(err, InvalidFunctionArgumentError)

    # Message must be based on the underlying cause.
    assert err.message == "invalid argument: offset must be >= 0"

    # Details must include func, arg, error_type, value and merged extras.
    expected_keys = {"func", "arg", "error_type", "value", "ctx"}
    assert expected_keys.issubset(err.details.keys())

    assert err.details["func"] == "read_sorted_set_range_by_score"
    assert err.details["arg"] == "offset"
    assert err.details["error_type"] == "ValueError"
    assert err.details["value"] == -1
    assert err.details["ctx"] == "unit-test"

    # Cause must be attached and preserved.
    assert err.cause is cause


def test_a2_unknown_func_and_arg_are_normalized() -> None:
    """
    func/arg must be normalized to '<unknown>' when empty or None.
    """
    cause = TypeError("count must be > 0")

    err = InvalidFunctionArgumentError(
        func=None,
        arg="",
        value=None,
        cause=cause,
        details=None,
    )

    assert err.details["func"] == "<unknown>"
    assert err.details["arg"] == "<unknown>"
    assert err.details["error_type"] == "TypeError"


# ---------- Group B: details behavior ----------


def test_b1_value_is_optional() -> None:
    """
    Value must not appear in details if not provided.
    """
    cause = ValueError("bad")

    err = InvalidFunctionArgumentError(
        func="f",
        arg="a",
        cause=cause,
        value=None,
        details=None,
    )

    assert "value" not in err.details


def test_b2_value_is_included_when_present() -> None:
    """
    Value must be included in details when provided.
    """
    cause = ValueError("bad")

    err = InvalidFunctionArgumentError(
        func="f",
        arg="a",
        value=123,
        cause=cause,
    )

    assert err.details["value"] == 123


def test_b3_details_override_defaults() -> None:
    """
    Explicit details must override base fields if keys collide.
    """
    cause = ValueError("bad")

    err = InvalidFunctionArgumentError(
        func="f",
        arg="a",
        value=1,
        cause=cause,
        details={
            "func": "override_func",
            "arg": "override_arg",
            "error_type": "OverrideError",
        },
    )

    assert err.details["func"] == "override_func"
    assert err.details["arg"] == "override_arg"
    assert err.details["error_type"] == "OverrideError"


def test_b4_details_are_copied_from_external_mapping() -> None:
    """
    External details mapping must not affect internal state after mutation.
    """
    cause = ValueError("value must be positive")
    external_details: dict[str, Any] = {"extra": 1}

    err = InvalidFunctionArgumentError(
        func="some_func",
        arg="value",
        value=0,
        cause=cause,
        details=external_details,
    )

    external_details["extra"] = 999
    external_details["new_key"] = "mutated"

    assert err.details["extra"] == 1
    assert "new_key" not in err.details


def test_b5_value_accepts_any_type() -> None:
    """
    Value can be any type and must be stored as-is.
    """
    cause = ValueError("bad")
    value = {"complex": ["object", 123]}

    err = InvalidFunctionArgumentError(
        func="f",
        arg="a",
        value=value,
        cause=cause,
    )

    assert err.details["value"] is value


# ---------- Group C: to_log_payload behavior ----------


def test_c1_to_log_payload_basic_shape() -> None:
    """
    to_log_payload must have a stable shape.
    """
    cause = TypeError("count must be > 0")

    err = InvalidFunctionArgumentError(
        func=None,
        arg="",
        value=None,
        cause=cause,
        details=None,
    )

    payload = err.to_log_payload()

    assert payload["kind"] == "InvalidFunctionArgumentError"
    assert payload["message"] == err.message
    assert isinstance(payload["details"], dict)
    assert payload["details"]["func"] == "<unknown>"
    assert payload["details"]["arg"] == "<unknown>"
    assert payload["details"]["error_type"] == "TypeError"


def test_c2_to_log_payload_includes_cause() -> None:
    """
    Cause must be present in log payload.
    """
    cause = ValueError("boom")

    err = InvalidFunctionArgumentError(
        func="f",
        arg="a",
        value=1,
        cause=cause,
    )

    payload = err.to_log_payload()

    assert payload["cause"] == {
        "kind": "ValueError",
        "message": "boom",
    }


def test_c3_to_log_payload_details_are_isolated() -> None:
    """
    Mutating payload must not affect internal details.
    """
    cause = ValueError("bad")

    err = InvalidFunctionArgumentError(
        func="f",
        arg="a",
        value=1,
        cause=cause,
    )

    payload = err.to_log_payload()
    payload["details"]["value"] = 999

    assert err.details["value"] == 1


# ---------- Group D: string representation ----------


def test_d1_str_contains_class_message_and_details() -> None:
    """
    __str__ must include class name, message and details.
    """
    cause = ValueError("offset must be >= 0")

    err = InvalidFunctionArgumentError(
        func="f",
        arg="a",
        value=1,
        cause=cause,
    )

    s = str(err)

    assert "InvalidFunctionArgumentError" in s
    assert "invalid argument: offset must be >= 0" in s
    assert "details=" in s
