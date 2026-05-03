from mvx.logger import (
    get_trace_id,
    set_trace_id,
    reset_trace_id,
)


def test_get_trace_id_default_is_no_trace() -> None:
    """
    When nothing was set, get_trace_id() should return 'no-trace'.
    """
    assert get_trace_id() == "no-trace"


def test_set_trace_id_with_normal_value() -> None:
    """
    set_trace_id() with a regular non-blank value should be returned by get_trace_id().
    """
    token = set_trace_id("req-123")
    try:
        assert get_trace_id() == "req-123"
    finally:
        reset_trace_id(token)

    # After reset, we are back to default.
    assert get_trace_id() == "no-trace"


def test_set_trace_id_strips_whitespace() -> None:
    """
    Leading and trailing whitespace must be stripped.
    """
    token = set_trace_id("   op-456   ")
    try:
        assert get_trace_id() == "op-456"
    finally:
        reset_trace_id(token)

    assert get_trace_id() == "no-trace"


def test_set_trace_id_none_becomes_no_trace() -> None:
    """
    None is normalized to 'no-trace'.
    """
    token = set_trace_id(None)
    try:
        assert get_trace_id() == "no-trace"
    finally:
        reset_trace_id(token)

    assert get_trace_id() == "no-trace"


def test_set_trace_id_blank_string_becomes_no_trace() -> None:
    """
    Blank string (after strip) is normalized to 'no-trace'.
    """
    token = set_trace_id("   ")
    try:
        assert get_trace_id() == "no-trace"
    finally:
        reset_trace_id(token)

    assert get_trace_id() == "no-trace"


def test_nested_set_and_reset_restore_previous_value() -> None:
    """
    Nested set_trace_id() calls must restore previous values when reset in reverse order.
    """
    # First value
    token1 = set_trace_id("first")
    try:
        assert get_trace_id() == "first"

        # Nested override
        token2 = set_trace_id("second")
        try:
            assert get_trace_id() == "second"
        finally:
            reset_trace_id(token2)

        # After inner reset we must see the outer value again
        assert get_trace_id() == "first"
    finally:
        reset_trace_id(token1)

    # After outer reset we are back to default
    assert get_trace_id() == "no-trace"
