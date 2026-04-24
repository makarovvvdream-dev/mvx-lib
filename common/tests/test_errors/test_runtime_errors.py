from mvx.common.errors import RuntimeExtendedError, RuntimeUnexpectedError

# ---------- Group A: basic construction and fields ----------


def test_a1_runtime_extended_error_sets_basic_fields() -> None:
    """
    Ensure basic fields (message, details, cause) are set correctly.
    """
    root = ValueError("root")
    err = RuntimeExtendedError(
        message="boom",
        details={"a": 1},
        cause=root,
    )

    assert err.message == "boom"
    assert err.details == {"a": 1}
    assert err.cause is root


def test_a2_runtime_extended_error_normalizes_module_and_qualname() -> None:
    """
    Ensure module/qualname are stripped and empty strings become None.
    """
    err = RuntimeExtendedError(
        message="x",
        module="  my.module  ",
        qualname="  MyClass.method  ",
    )

    assert err.module == "my.module"
    assert err.qualname == "MyClass.method"


def test_a3_runtime_extended_error_empty_strings_become_none() -> None:
    """
    Ensure empty/whitespace-only module and qualname become None.
    """
    err = RuntimeExtendedError(
        message="x",
        module="   ",
        qualname="",
    )

    assert err.module is None
    assert err.qualname is None


def test_a4_runtime_extended_error_is_runtime_error() -> None:
    """
    Ensure RuntimeExtendedError is also a RuntimeError.
    """
    err = RuntimeExtendedError(message="x")

    assert isinstance(err, RuntimeError)


# ---------- Group B: to_log_payload behavior ----------


def test_b1_to_log_payload_includes_module_and_qualname_when_present() -> None:
    """
    Ensure module and qualname are included when provided.
    """
    err = RuntimeExtendedError(
        message="boom",
        module="mod",
        qualname="Cls.fn",
    )

    payload = err.to_log_payload()

    assert payload["module"] == "mod"
    assert payload["qualname"] == "Cls.fn"
    assert payload["message"] == "boom"
    assert payload["kind"] == "RuntimeExtendedError"


def test_b2_to_log_payload_omits_module_and_qualname_when_none() -> None:
    """
    Ensure module and qualname are not present when None.
    """
    err = RuntimeExtendedError(message="boom")

    payload = err.to_log_payload()

    assert "module" not in payload
    assert "qualname" not in payload


def test_b3_to_log_payload_includes_cause() -> None:
    """
    Ensure cause is propagated via StructuredError payload.
    """
    root = ValueError("root")
    err = RuntimeExtendedError(message="boom", cause=root)

    payload = err.to_log_payload()

    assert payload["cause"] == {
        "kind": "ValueError",
        "message": "root",
    }


def test_b4_to_log_payload_returns_independent_details_copy() -> None:
    """
    Ensure payload details are independent from internal state.
    """
    err = RuntimeExtendedError(message="x", details={"a": 1})

    payload = err.to_log_payload()
    payload["details"]["a"] = 2

    assert err.details == {"a": 1}


# ---------- Group C: RuntimeUnexpectedError ----------


def test_c1_runtime_unexpected_error_is_exception() -> None:
    """
    Ensure RuntimeUnexpectedError is a plain Exception.
    """
    err = RuntimeUnexpectedError()

    assert isinstance(err, Exception)
