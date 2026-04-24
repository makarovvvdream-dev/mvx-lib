import pytest

from mvx.common.errors import StructuredError

# ---------- Group A: basic construction and string representation ----------


def test_a1_minimal_error_has_message_and_empty_details() -> None:
    """
    Ensure a minimal StructuredError has message set and details as an empty dict.
    """
    err = StructuredError(message="something went wrong")

    assert err.message == "something went wrong"
    assert isinstance(err.details, dict)
    assert err.details == {}

    s = str(err)
    assert "StructuredError" in s
    assert "something went wrong" in s
    # No "details=" suffix when details is empty
    assert "details=" not in s


def test_a2_details_are_copied_and_shown_in_str() -> None:
    """
    Ensure details mapping is copied into an internal dict and reflected in __str__.
    """
    original_details = {"code": "X123", "severity": "high"}
    err = StructuredError(message="boom", details=original_details)

    # Internal copy, not the same object
    assert err.details is not original_details
    assert err.details == original_details

    s = str(err)
    assert "StructuredError" in s
    assert "boom" in s
    assert "details=" in s
    assert "code" in s
    assert "X123" in s


def test_a3_exception_args_contain_message() -> None:
    """
    Ensure Exception.args contains the message passed to StructuredError.
    """
    err = StructuredError(message="boom")

    assert err.args == ("boom",)


# ---------- Group B: to_log_payload behavior ----------


def test_b1_to_log_payload_contains_kind_message_details() -> None:
    """
    Ensure to_log_payload returns a stable dict with kind, message, and details.
    """
    err = StructuredError(
        message="failed to do something",
        details={"ctx": "test", "value": 42},
    )

    payload = err.to_log_payload()
    assert payload["kind"] == "StructuredError"
    assert payload["message"] == "failed to do something"
    assert payload["details"] == {"ctx": "test", "value": 42}


def test_b2_to_log_payload_uses_copy_of_details() -> None:
    """
    Ensure to_log_payload returns a copy of details so callers cannot mutate internals.
    """
    err = StructuredError(
        message="test",
        details={"a": 1},
    )

    payload = err.to_log_payload()
    assert payload["details"] == {"a": 1}

    # Mutating payload must not affect err.details
    payload["details"]["a"] = 2
    assert err.details == {"a": 1}


def test_b3_to_log_payload_includes_cause_when_present() -> None:
    """
    Ensure cause is included in payload when provided.
    """
    root = ValueError("root cause")
    err = StructuredError(message="wrapper", cause=root)

    payload = err.to_log_payload()

    assert payload["cause"] == {
        "kind": "ValueError",
        "message": "root cause",
    }


def test_b4_to_log_payload_uses_concrete_subclass_name() -> None:
    """
    Ensure payload kind reflects the concrete subclass name.
    """

    class MyError(StructuredError):
        pass

    err = MyError(message="boom")

    payload = err.to_log_payload()

    assert payload["kind"] == "MyError"


# ---------- Group C: cause / exception chaining semantics ----------


def test_c1_cause_is_attached_and_visible_via_cause() -> None:
    """
    Ensure cause passed into StructuredError is attached to __cause__.
    """
    root = ValueError("root cause")
    err = StructuredError(message="wrapper", cause=root)

    # __cause__ is set for introspection and exception chaining
    assert getattr(err, "cause", None) is root


def test_c2_raise_from_preserves_cause() -> None:
    """
    Ensure StructuredError can be used with 'raise ... from ...' and keep cause.
    """
    root = ValueError("root cause")

    with pytest.raises(StructuredError) as exc_info:
        try:
            raise root
        except ValueError as e:
            raise StructuredError(message="wrapped", cause=e) from e

    exc = exc_info.value
    assert isinstance(exc, StructuredError)
    assert exc.message == "wrapped"
    assert getattr(exc, "__cause__", None) is root


# ---------- Group D: fluent API (details mutation helpers) ----------
def test_d1_with_detail_adds_value_and_returns_self() -> None:
    """
    Ensure with_detail adds a key/value pair and returns self.
    """
    err = StructuredError(message="x")

    result = err.with_detail("k", "v")

    assert result is err
    assert err.details == {"k": "v"}


def test_d2_with_detail_overwrites_existing_value() -> None:
    """
    Ensure with_detail overwrites an existing key.
    """
    err = StructuredError(message="x", details={"k": "old"})

    err.with_detail("k", "new")

    assert err.details == {"k": "new"}


def test_d3_with_details_merges_values_and_returns_self() -> None:
    """
    Ensure with_details merges multiple values and returns self.
    """
    err = StructuredError(message="x", details={"a": 1})

    result = err.with_details({"b": 2})

    assert result is err
    assert err.details == {"a": 1, "b": 2}
