from __future__ import annotations

from typing import Any, Dict

import pytest

from mvx.logger.log_errors_helpers import (
    build_error_payload,
    is_error_logged,
    mark_error_logged,
)

# --------- tests: build_error_payload ---------


class DummyWithToLogExtra(Exception):
    """Synthetic exception providing to_log_extra()."""

    def __init__(self, payload: Dict[str, Any]) -> None:
        super().__init__("dummy")
        self._payload = payload

    def to_log_payload(self) -> Dict[str, Any]:
        return self._payload


def test_build_error_payload_uses_to_log_payload() -> None:
    payload = {"code": 1234, "code_desc": "SYNTHETIC", "kind": "Custom", "extra": "x"}
    err = DummyWithToLogExtra(payload)

    result = build_error_payload(err)

    assert result is not payload  # copy, not the same object
    assert result == payload


class DummyWithCodeAttrs(Exception):
    """Synthetic exception exposing `code` and `code_desc` attributes."""

    def __init__(self, code: int, code_desc: str, msg: str) -> None:
        super().__init__(msg)
        self.code = code
        self.code_desc = code_desc


def test_build_error_payload_with_code_and_code_desc() -> None:
    err = DummyWithCodeAttrs(1001, "SOME_ERROR", "something went wrong")

    result = build_error_payload(err)

    assert result["code"] == 1001
    assert result["code_desc"] == "SOME_ERROR"
    assert result["kind"] == "DummyWithCodeAttrs"
    assert result["message"] == "something went wrong"


def test_build_error_payload_generic_exception() -> None:
    err = ValueError("bad value")

    result = build_error_payload(err)

    # no code/code_desc by default
    assert "code" not in result
    assert "code_desc" not in result

    assert result["kind"] == "ValueError"
    assert result["message"] == "bad value"


class DummyBadToLogExtra(Exception):
    """Synthetic exception with broken to_log_extra (non-dict or raises)."""

    mode: str

    def __init__(self, mode: str) -> None:
        super().__init__("dummy-bad")
        self.mode = mode

    def to_log_extra(self):
        if self.mode == "raise":
            raise RuntimeError("boom")
        return "not-a-dict"  # invalid return type


@pytest.mark.parametrize("mode", ["raise", "not_dict"])
def test_build_error_payload_fallback_on_broken_to_log_extra(mode: str) -> None:
    err = DummyBadToLogExtra(mode)

    result = build_error_payload(err)

    # falls back to generic mapping (no code/code_desc)
    assert "code" not in result
    assert "code_desc" not in result
    assert result["kind"] == "DummyBadToLogExtra"
    assert result["message"] == "dummy-bad"


# --------- tests: is_error_logged / mark_error_logged ---------


def test_is_error_logged_default_false() -> None:
    """
    By default, freshly created exceptions must not be marked as logged.
    """
    err = RuntimeError("x")
    assert is_error_logged(err) is False


def test_mark_error_logged_sets_flag() -> None:
    """
    mark_error_logged must mark the given exception instance as logged.
    """
    err = RuntimeError("x")
    assert is_error_logged(err) is False

    mark_error_logged(err)

    assert is_error_logged(err) is True


def test_mark_error_logged_idempotent() -> None:
    """
    mark_error_logged can be called multiple times without changing semantics.
    """
    err = RuntimeError("x")
    assert is_error_logged(err) is False

    mark_error_logged(err)
    assert is_error_logged(err) is True

    # second call should not break anything
    mark_error_logged(err)
    assert is_error_logged(err) is True


class NoAttrsError(Exception):
    """
    Synthetic exception that does not allow setting arbitrary attributes.

    Used to verify that mark_error_logged is best-effort and does not crash
    when setattr(...) fails.
    """

    __slots__ = ()

    def __setattr__(self, name, value) -> None:  # type: ignore[override]
        raise AttributeError("no arbitrary attributes allowed")


def test_mark_error_logged_best_effort_when_setattr_fails() -> None:
    """
    mark_error_logged must not crash when the exception instance does not allow
    arbitrary attributes. In this case the flag is not set and the error is
    treated as not logged.
    """
    err = NoAttrsError("no-attrs")

    # Должно отработать без исключения…
    mark_error_logged(err)

    # …и флаг при этом не должен считаться установленным.
    assert is_error_logged(err) is False
