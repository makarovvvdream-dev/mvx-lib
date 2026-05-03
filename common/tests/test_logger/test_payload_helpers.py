from __future__ import annotations

import enum
from typing import Any, Dict

import logging
import pytest

from mvx.logger.payload_helpers import (
    normalize_primitive,
    normalize_list_for_log,
    normalize_dict_for_log,
    normalize_value_for_log,
)
from mvx.logger import adapter_registry as ar
from mvx.logger.adapter_registry import (
    register_log_adapter,
    set_active_log_profile,
)


# ---------- Fixtures ----------


@pytest.fixture(autouse=True)
def reset_registry_and_profile() -> None:
    """
    Ensure adapter registry and active profile are isolated between tests.
    """
    ar._ADAPTERS.clear()  # type: ignore[attr-defined]
    set_active_log_profile("default")
    try:
        yield
    finally:
        ar._ADAPTERS.clear()  # type: ignore[attr-defined]
        set_active_log_profile("default")


# ---------- A: normalize_primitive ----------


def test_a1_normalize_primitive_short_string() -> None:
    """
    Short strings must be returned unchanged.
    """
    s = "hello"
    result = normalize_primitive(s)
    assert result == s


def test_a2_normalize_primitive_long_string_truncated() -> None:
    """
    Long strings must be truncated to the fixed length with "..." suffix.
    """
    s = "x" * 250
    result = normalize_primitive(s)
    # 200 chars + "..."
    assert isinstance(result, str)
    assert result.startswith("x" * 200)
    assert result.endswith("...")
    assert len(result) == 203


@pytest.mark.parametrize(
    "value",
    [
        123,
        3.14,
        True,
        False,
        None,
    ],
)
def test_a3_normalize_primitive_numeric_bool_none(value: Any) -> None:
    """
    int/float/bool/None must be returned as-is.
    """
    result = normalize_primitive(value)
    assert result is value


def test_a4_normalize_primitive_other_type() -> None:
    """
    Non-primitive values must be represented as '<TypeName>'.
    """

    class Dummy:
        pass

    value = Dummy()
    result = normalize_primitive(value)
    assert result == "<Dummy>"


def test_a5_normalize_primitive_bytes_returns_bytes() -> None:
    """
    bytes-like values must be returned as bytes(value).
    """
    b = b"abc"
    result = normalize_primitive(b)

    assert isinstance(result, bytes)
    assert result == b


# ---------- B: normalize_list_for_log ----------
def test_b1_normalize_list_top_short_primitives() -> None:
    """
    Short list of primitives must be preserved (with primitive normalization).
    """
    value = [1, "x", b"t", True, None]
    result = normalize_list_for_log(value)

    assert isinstance(result, list)
    assert result == [1, "x", b"t", True, None]


def test_b2_normalize_list_top_long_primitives() -> None:
    """
    Long list of primitives must be truncated to MAX_ITEMS + summary element.
    """
    # Assuming DEFAULT_MAX_ITEMS = 10 in implementation
    seq = list(range(20))
    result = normalize_list_for_log(seq)

    assert isinstance(result, list)
    # 10 elements + summary string
    assert len(result) == 11
    assert result[:10] == list(range(10))
    assert isinstance(result[-1], str)
    assert result[-1] == "... (10 more)"


def test_b3_normalize_list_top_with_composites() -> None:
    """
    Composite values inside list must be represented as '<TypeName>'.
    """

    class Dummy:
        pass

    value = [1, {"a": 1}, Dummy(), [1, 2]]
    result = normalize_list_for_log(value)

    assert isinstance(result, list)
    assert result == [1, "<dict>", "<Dummy>", "<list>"]


def test_b4_normalize_list_top_non_list() -> None:
    """
    Non-list/tuple input must be wrapped and represented as '<TypeName>'.
    """
    value = "abc"
    result = normalize_list_for_log(value)

    assert isinstance(result, str)
    assert result == "<str>"


# ---------- C: normalize_dict_for_log ----------
def test_c1_normalize_dict_top_short_primitives() -> None:
    """
    Short dict with primitive values must be preserved (with primitive normalization).
    """
    value = {"a": 1, "b": "x", "c": b"t", "d": True, "e": None}
    result = normalize_dict_for_log(value)

    assert isinstance(result, dict)
    assert result == {"a": 1, "b": "x", "c": b"t", "d": True, "e": None}


def test_c2_normalize_dict_top_long_dict() -> None:
    """
    Long dict must be truncated to MAX_ITEMS keys plus '__more__' summary.
    """
    # Assuming DEFAULT_MAX_ITEMS = 10
    value = {f"k{i}": i for i in range(15)}
    result = normalize_dict_for_log(value)

    assert isinstance(result, dict)

    # First 10 keys plus '__more__'
    assert len(result) == 11
    for i in range(10):
        assert result[f"k{i}"] == i
    assert result["__more__"] == "5 more keys"


def test_c3_normalize_dict_top_with_composites() -> None:
    """
    Composite values inside dict must be represented as '<TypeName>'.
    """

    class Dummy:
        pass

    value = {
        "a": 1,
        "b": Dummy(),
        "c": {"x": 1},
        "d": [1, 2],
    }
    result = normalize_dict_for_log(value)

    assert isinstance(result, dict)
    assert result["a"] == 1
    assert result["b"] == "<Dummy>"
    assert result["c"] == "<dict>"
    assert result["d"] == "<list>"


def test_c4_normalize_dict_top_non_dict() -> None:
    """
    G3.4: Non-dict input must be represented as {'<value>': '<TypeName>'}.
    """
    value = [1, 2]
    result = normalize_dict_for_log(value)

    assert isinstance(result, str)
    assert result == "<list>"


# ---------- D: normalize_value_for_log ----------
def test_d1_normalize_for_log_primitive() -> None:
    """
    Primitive values must be normalized like normalize_primitive.
    """
    # Short string
    assert normalize_value_for_log("hello") == "hello"

    # Long string is truncated
    s = "x" * 250
    result = normalize_value_for_log(s)
    assert isinstance(result, str)
    assert result.startswith("x" * 200)
    assert result.endswith("...")
    assert len(result) == 203

    # Numbers/bool/None pass through
    assert normalize_value_for_log(123) == 123
    assert normalize_value_for_log(3.14) == 3.14
    assert normalize_value_for_log(True) is True
    assert normalize_value_for_log(None) is None

    # bytes pass through as bytes
    b = b"token"
    result_b = normalize_value_for_log(b)
    assert isinstance(result_b, bytes)
    assert result_b == b


def test_d2_normalize_for_log_list_delegation() -> None:
    """
    List values must be normalized one level deep with truncation and summary.
    """
    seq = list(range(15))
    result = normalize_value_for_log(seq)

    assert isinstance(result, list)

    # 10 elements + summary string
    assert len(result) == 11
    assert result[:10] == list(range(10))
    assert isinstance(result[-1], str)
    assert result[-1] == "... (5 more)"


def test_d3_normalize_for_log_dict_delegation() -> None:
    """
    Dict values must be normalized one level deep with truncation and summary.
    """
    value = {f"k{i}": i for i in range(12)}
    result = normalize_value_for_log(value)

    assert isinstance(result, dict)

    # 10 keys + '__more__'
    assert len(result) == 11
    for i in range(10):
        assert result[f"k{i}"] == i
    assert result["__more__"] == "2 more keys"


def test_d4_normalize_for_log_other_type() -> None:
    """
    Non-primitive, non-container values must be logged as '<TypeName>'.
    """

    class Dummy:
        pass

    value = Dummy()
    result = normalize_value_for_log(value)
    assert result == "<Dummy>"


def test_d5_normalize_for_log_enum() -> None:
    """
    Enum instances must be normalized via their .value recursively.
    """

    class StrStatus(enum.Enum):
        OK = "OK"

    class IntStatus(enum.Enum):
        OK = 1

    class Complex(enum.Enum):
        P = {"x": 1, "y": 2}

    # StrEnum-like: .value is a string -> passes through primitive normalization
    assert normalize_value_for_log(StrStatus.OK) == "OK"

    # Int-like: .value is an int -> passes through primitive normalization
    assert normalize_value_for_log(IntStatus.OK) == 1

    # Complex: .value is a dict -> delegates to dict normalization
    result = normalize_value_for_log(Complex.P)
    assert isinstance(result, dict)
    assert result == {"x": 1, "y": 2}


def test_d6_normalize_for_log_uses_to_log_payload_verbatim() -> None:
    """
    Objects implementing to_log_payload() must be logged using that payload verbatim.
    """

    class PayloadProvider:
        def __init__(self) -> None:
            self.internal = "should_not_appear"

        def to_log_payload(self) -> dict[str, Any]:
            _ = self
            return {
                "user_id": "u-123",
                "details": {
                    "role": "admin",
                    "active": True,
                },
            }

    test_object = PayloadProvider()
    result = normalize_value_for_log(test_object)

    assert isinstance(result, dict)

    assert result == {
        "user_id": "u-123",
        "details": {
            "role": "admin",
            "active": True,
        },
    }


# ---------- E: type-based adapters & profiles ----------


def test_e1_type_based_adapter_used_for_plain_object() -> None:
    """
    If a type-based adapter is registered for the active profile,
    normalize_value_for_log must use its payload for objects without to_log_payload().
    """

    class User:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

    def user_adapter(obj: User) -> Dict[str, Any]:
        assert isinstance(obj, User)
        return {"kind": "user", "id": obj.user_id}

    register_log_adapter(User, user_adapter, profile="default")

    test_object = User("u-1")
    result = normalize_value_for_log(test_object)

    assert isinstance(result, dict)
    assert result == {"kind": "user", "id": "u-1"}


def test_e2_adapter_registered_for_base_used_for_subclass() -> None:
    """
    Adapter registered for base class must be used for subclass instances
    when no more specific adapter is present.
    """

    class Base:
        def __init__(self, v: int) -> None:
            self.v = v

    class Sub(Base):
        pass

    def base_adapter(obj: Base) -> Dict[str, Any]:
        assert isinstance(obj, Base)
        return {"kind": "base", "v": obj.v}

    register_log_adapter(Base, base_adapter, profile="default")

    test_object = Sub(42)
    result = normalize_value_for_log(test_object)

    assert isinstance(result, dict)
    assert result == {"kind": "base", "v": 42}


def test_e3_more_specific_adapter_overrides_base() -> None:
    """
    If adapters exist for base and subclass, subclass adapter must win
    for subclass instances.
    """

    class Base:
        pass

    class Sub(Base):
        def __init__(self, name: str) -> None:
            self.name = name

    def base_adapter(obj: Base) -> Dict[str, Any]:
        _ = obj
        return {"kind": "base"}

    def sub_adapter(obj: Sub) -> Dict[str, Any]:
        assert isinstance(obj, Sub)
        return {"kind": "sub", "name": obj.name}

    register_log_adapter(Base, base_adapter, profile="default")
    register_log_adapter(Sub, sub_adapter, profile="default")

    base_tast_object = Base()
    sub_test_object = Sub("x")

    result_base = normalize_value_for_log(base_tast_object)
    result_sub = normalize_value_for_log(sub_test_object)

    assert result_base == {"kind": "base"}
    assert result_sub == {"kind": "sub", "name": "x"}


def test_e4_profile_switch_changes_used_adapter() -> None:
    """
    Different adapters registered under different profiles must be
    selected according to the active profile.
    """

    class Base:
        def __init__(self, x: int) -> None:
            self.x = x

    def adapter_default(obj: Base) -> Dict[str, Any]:
        return {"profile": "default", "x": obj.x}

    def adapter_debug(obj: Base) -> Dict[str, Any]:
        return {"profile": "debug", "x": obj.x}

    register_log_adapter(Base, adapter_default, profile="default")
    register_log_adapter(Base, adapter_debug, profile="debug")

    test_object = Base(7)

    # active profile 'default' (fixture)
    result_default = normalize_value_for_log(test_object)
    assert result_default == {"profile": "default", "x": 7}

    # switch to 'debug'
    set_active_log_profile("debug")
    result_debug = normalize_value_for_log(test_object)
    assert result_debug == {"profile": "debug", "x": 7}


def test_e5_logpayloadprovider_overrides_type_adapter() -> None:
    """
    If an object implements to_log_payload(), it must override any
    registered type-based adapter.
    """

    class Base:
        def __init__(self, x: int) -> None:
            self.x = x

        def to_log_payload(self) -> dict[str, Any]:
            return {"kind": "provider", "x": self.x}

    def adapter_obj(obj: Base) -> Dict[str, Any]:
        return {"kind": "adapter", "x": obj.x}

    register_log_adapter(Base, adapter_obj, profile="default")

    test_object = Base(5)
    result = normalize_value_for_log(test_object)

    # Provider must win over adapter.
    assert result == {"kind": "provider", "x": 5}


# ---------- F: UnsupportedLoggerProfile handling ----------


def test_f1_unsupported_logger_profile_logs_warning_and_falls_back(monkeypatch: Any) -> None:
    """
    When a LogPayloadProvider raises UnsupportedLoggerProfile, the logger
    must emit a warning event and normalize_value_for_log must fall back
    to generic normalization ("<TypeName>").
    """
    import logging
    from mvx.logger import payload_helpers as ph
    from mvx.logger import (
        UnsupportedLoggerProfile,
    )

    calls: list[tuple[logging.Logger, str, Dict[str, Any] | None]] = []

    def fake_log_warning_event(
        logger: logging.Logger,
        evt: str,
        data: Dict[str, Any] | None = None,
    ) -> None:
        calls.append((logger, evt, data))

    monkeypatch.setattr(ph, "log_warning_event", fake_log_warning_event)

    class Provider:
        def to_log_payload(self) -> dict[str, Any]:
            raise UnsupportedLoggerProfile(
                profile="audit",
                target_type=type(self),
            )

    # Act
    obj = Provider()
    result = normalize_value_for_log(obj)

    # Assert: fallback к "<Provider>"
    assert result == "<Provider>"

    # Assert: warning было ровно одно
    assert len(calls) == 1
    logger, evt, data = calls[0]

    assert isinstance(logger, logging.Logger)
    assert evt == "logger.profile.unsupported"
    assert isinstance(data, dict)

    # Payload is produced by UnsupportedLoggerProfile.to_log_extra()
    assert data.get("profile") == "audit"

    # Fully-qualified type name: <module>.<qualname>
    expected_type_name = f"{Provider.__module__}.{Provider.__qualname__}"
    assert data.get("target_type") == expected_type_name


def test_f2_other_exception_in_to_log_payload_is_silently_ignored(monkeypatch: Any) -> None:
    """
    When to_log_payload raises a non-UnsupportedLoggerProfile exception,
    no warning event must be emitted and normalization must fall back
    to generic "<TypeName>" without noise.
    """
    from mvx.logger import log_events_helpers as leh

    calls: list[tuple[Any, str, Dict[str, Any] | None]] = []

    def fake_log_warning_event(
        logger: logging.Logger,
        evt: str,
        data: Dict[str, Any] | None = None,
    ) -> None:
        calls.append((logger, evt, data))

    monkeypatch.setattr(leh, "log_warning_event", fake_log_warning_event)

    class Provider:
        def to_log_payload(self) -> dict[str, Any]:
            raise RuntimeError("boom")

    obj = Provider()
    result = normalize_value_for_log(obj)

    # Fallback to generic representation
    assert result == "<Provider>"

    # No warning events should be emitted for arbitrary exceptions
    assert calls == []
