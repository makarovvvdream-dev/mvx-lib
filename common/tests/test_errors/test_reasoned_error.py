from mvx.common.errors import ReasonedError

# ---------- Group A: basic construction and fields ----------


def test_a1_reasoned_error_sets_reason_code() -> None:
    """
    Ensure reason_code is stored correctly when provided.
    """
    err = ReasonedError(message="boom", reason="SOME_REASON")

    assert err.reason_code == "SOME_REASON"


def test_a2_reasoned_error_reason_code_defaults_to_none() -> None:
    """
    Ensure reason_code is None when not provided.
    """
    err = ReasonedError(message="boom")

    assert err.reason_code is None


# ---------- Group B: to_log_payload behavior ----------


def test_b1_to_log_payload_includes_reason_when_present() -> None:
    """
    Ensure reason is included in payload when reason_code is set.
    """
    err = ReasonedError(
        message="failed",
        reason="ERR_X",
        details={"a": 1},
    )

    payload = err.to_log_payload()

    assert payload["reason"] == "ERR_X"
    assert payload["message"] == "failed"
    assert payload["details"] == {"a": 1}
    assert payload["kind"] == "ReasonedError"


def test_b2_to_log_payload_omits_reason_when_none() -> None:
    """
    Ensure reason is not present in payload when reason_code is None.
    """
    err = ReasonedError(message="failed")

    payload = err.to_log_payload()

    assert "reason" not in payload
    assert payload["kind"] == "ReasonedError"


def test_b3_to_log_payload_includes_cause_and_reason_together() -> None:
    """
    Ensure payload contains both reason and cause when both are present.
    """
    root = ValueError("root")
    err = ReasonedError(
        message="failed",
        reason="ERR_X",
        cause=root,
    )

    payload = err.to_log_payload()

    assert payload["reason"] == "ERR_X"
    assert payload["cause"] == {
        "kind": "ValueError",
        "message": "root",
    }


def test_b4_to_log_payload_returns_independent_dict() -> None:
    """
    Ensure returned payload is independent and can be mutated safely.
    """
    err = ReasonedError(
        message="failed",
        reason="ERR_X",
        details={"a": 1},
    )

    payload = err.to_log_payload()
    payload["details"]["a"] = 2

    assert err.details == {"a": 1}


def test_b5_to_log_payload_uses_subclass_name() -> None:
    """
    Ensure payload kind reflects the concrete subclass name.
    """

    class MyError(ReasonedError):
        pass

    err = MyError(message="boom")

    payload = err.to_log_payload()

    assert payload["kind"] == "MyError"
