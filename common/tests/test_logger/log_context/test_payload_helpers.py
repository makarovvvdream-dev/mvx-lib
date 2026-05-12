# tests/test_logger/log_context/test_payload_helpers.py
from __future__ import annotations

from typing import Any
import enum

import pytest

from mvx.common.logger.log_context.log_payload_helpers import (
    normalize_primitive,
    normalize_list_for_log,
    normalize_dict_for_log,
    normalize_value_for_log,
)

DEFAULT_MAX_STR_LEN = 200
DEFAULT_MAX_ITEMS = 10
DEFAULT_VERBOSITY_LEVEL = "NORMAL"


def normalize_primitive_default(value: Any) -> str | int | float | bool | bytes | None:
    return normalize_primitive(
        value,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )


def normalize_list_default(value: Any) -> str | list[Any]:
    return normalize_list_for_log(
        value,
        log_adapter_resolver=None,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )


def normalize_dict_default(value: Any) -> str | dict[str, Any]:
    return normalize_dict_for_log(
        value,
        log_adapter_resolver=None,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )


def normalize_value_default(
    value: Any,
) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
    return normalize_value_for_log(
        value,
        log_adapter_resolver=None,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )


# ---------- A: normalize_primitive ----------


def test_a01_normalize_primitive_short_string_is_returned_unchanged() -> None:
    value = "hello"

    result = normalize_primitive_default(value)

    assert result == "hello"


def test_a02_normalize_primitive_long_string_is_truncated() -> None:
    value = "x" * 250
    expected = ("x" * DEFAULT_MAX_STR_LEN) + "..."

    result = normalize_primitive_default(value)

    assert result == expected


def test_a03_normalize_primitive_string_is_not_truncated_when_limit_is_none() -> None:
    value = "x" * 250

    result = normalize_primitive(
        value,
        max_str_len=None,
    )

    assert result == value


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
def test_a04_normalize_primitive_number_bool_none_are_returned_as_is(value: Any) -> None:
    result = normalize_primitive_default(value)

    assert result is value


@pytest.mark.parametrize(
    "value",
    [
        b"abc",
        bytearray(b"abc"),
        memoryview(b"abc"),
    ],
)
def test_a05_normalize_primitive_bytes_like_values_are_converted_to_bytes(value: Any) -> None:
    result = normalize_primitive_default(value)

    assert isinstance(result, bytes)
    assert result == b"abc"


def test_a06_normalize_primitive_other_type_becomes_type_placeholder() -> None:
    class Dummy:
        pass

    result = normalize_primitive_default(Dummy())

    assert result == "<Dummy>"


# ---------- B: normalize_list_for_log ----------


def test_b01_normalize_list_with_short_primitives() -> None:
    value = [1, "x", b"t", True, None]

    result = normalize_list_default(value)

    assert result == [1, "x", b"t", True, None]


def test_b02_normalize_list_applies_item_limit_and_summary() -> None:
    value = list(range(20))

    result = normalize_list_default(value)

    assert isinstance(result, list)
    assert result[:10] == list(range(10))
    assert result[-1] == "... (10 more)"
    assert len(result) == 11


def test_b03_normalize_list_has_no_item_limit_when_max_items_is_none() -> None:
    value = list(range(20))

    result = normalize_list_for_log(
        value,
        log_adapter_resolver=None,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=None,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == value


def test_b04_normalize_list_normalizes_items_one_level_deep() -> None:
    class Dummy:
        pass

    value = [1, {"a": 1}, Dummy(), [1, 2], (1, 2)]

    result = normalize_list_default(value)

    assert result == [1, "<dict>", "<Dummy>", "<list>", "<tuple>"]


def test_b05_normalize_list_accepts_tuple_as_sequence() -> None:
    value = (1, "x", True)

    result = normalize_list_default(value)

    assert result == [1, "x", True]


def test_b06_normalize_list_non_list_or_tuple_returns_type_placeholder() -> None:
    result = normalize_list_default("abc")

    assert result == "<str>"


def test_b07_normalize_list_applies_string_truncation_to_leaf_items() -> None:
    value = ["x" * 250]

    result = normalize_list_default(value)

    assert isinstance(result, list)
    assert result == [("x" * DEFAULT_MAX_STR_LEN) + "..."]


# ---------- C: normalize_dict_for_log ----------


def test_c01_normalize_dict_with_short_primitives() -> None:
    value = {"a": 1, "b": "x", "c": b"t", "d": True, "e": None}

    result = normalize_dict_default(value)

    assert result == {"a": 1, "b": "x", "c": b"t", "d": True, "e": None}


def test_c02_normalize_dict_applies_item_limit_and_more_marker() -> None:
    value = {f"k{i}": i for i in range(15)}

    result = normalize_dict_default(value)

    assert isinstance(result, dict)
    assert len(result) == 11

    for i in range(10):
        assert result[f"k{i}"] == i

    assert result["__more__"] == "5 more keys"


def test_c03_normalize_dict_has_no_item_limit_when_max_items_is_none() -> None:
    value = {f"k{i}": i for i in range(15)}

    result = normalize_dict_for_log(
        value,
        log_adapter_resolver=None,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=None,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == value


def test_c04_normalize_dict_normalizes_values_one_level_deep() -> None:
    class Dummy:
        pass

    value = {
        "a": 1,
        "b": Dummy(),
        "c": {"x": 1},
        "d": [1, 2],
        "e": (1, 2),
    }

    result = normalize_dict_default(value)

    assert result == {
        "a": 1,
        "b": "<Dummy>",
        "c": "<dict>",
        "d": "<list>",
        "e": "<tuple>",
    }


def test_c05_normalize_dict_non_dict_returns_type_placeholder() -> None:
    result = normalize_dict_default([1, 2])

    assert result == "<list>"


def test_c06_normalize_dict_converts_keys_to_strings() -> None:
    value = {
        1: "one",
        2.5: "two-point-five",
        None: "none",
    }

    result = normalize_dict_default(value)

    assert isinstance(result, dict)
    assert result["1"] == "one"
    assert result["2.5"] == "two-point-five"
    assert result["None"] == "none"


def test_c07_normalize_dict_truncates_long_keys() -> None:
    long_key = "x" * 250

    result = normalize_dict_default({long_key: 1})

    assert isinstance(result, dict)

    expected_key = ("x" * DEFAULT_MAX_STR_LEN) + "..."
    assert result == {expected_key: 1}


# ---------- D: normalize_value_for_log ----------


def test_d01_normalize_value_normalizes_primitive() -> None:
    assert normalize_value_default("hello") == "hello"
    assert normalize_value_default(123) == 123
    assert normalize_value_default(3.14) == 3.14
    assert normalize_value_default(True) is True
    assert normalize_value_default(False) is False
    assert normalize_value_default(None) is None
    assert normalize_value_default(b"token") == b"token"


def test_d02_normalize_value_truncates_long_string() -> None:
    value = "x" * 250

    result = normalize_value_default(value)

    assert result == ("x" * DEFAULT_MAX_STR_LEN) + "..."


def test_d03_normalize_value_delegates_list_to_list_normalizer() -> None:
    value = list(range(15))

    result = normalize_value_default(value)

    assert isinstance(result, list)
    assert result[:10] == list(range(10))
    assert result[-1] == "... (5 more)"


def test_d04_normalize_value_delegates_tuple_to_list_normalizer() -> None:
    value = (1, 2, 3)

    result = normalize_value_default(value)

    assert result == [1, 2, 3]


def test_d05_normalize_value_delegates_dict_to_dict_normalizer() -> None:
    value = {f"k{i}": i for i in range(12)}

    result = normalize_value_default(value)

    assert isinstance(result, dict)
    assert len(result) == 11

    for i in range(10):
        assert result[f"k{i}"] == i

    assert result["__more__"] == "2 more keys"


def test_d06_normalize_value_other_type_becomes_type_placeholder() -> None:
    class Dummy:
        pass

    result = normalize_value_default(Dummy())

    assert result == "<Dummy>"


def test_d07_normalize_value_enum_is_normalized_via_value_recursively() -> None:
    class StrStatus(enum.Enum):
        OK = "OK"

    class IntStatus(enum.Enum):
        OK = 1

    class DictStatus(enum.Enum):
        PAYLOAD = {"x": 1, "y": 2}

    class ListStatus(enum.Enum):
        PAYLOAD = [1, 2, 3]

    assert normalize_value_default(StrStatus.OK) == "OK"
    assert normalize_value_default(IntStatus.OK) == 1
    assert normalize_value_default(DictStatus.PAYLOAD) == {"x": 1, "y": 2}
    assert normalize_value_default(ListStatus.PAYLOAD) == [1, 2, 3]


# ---------- E: LogPayloadProvider ----------


def test_e01_log_payload_provider_is_used_verbatim() -> None:
    class PayloadProvider:
        def to_log_payload(self) -> dict[str, Any]:
            _ = self
            return {
                "user_id": "u-123",
                "details": {
                    "role": "admin",
                    "active": True,
                },
            }

    result = normalize_value_default(PayloadProvider())

    assert result == {
        "user_id": "u-123",
        "details": {
            "role": "admin",
            "active": True,
        },
    }


def test_e02_log_payload_provider_payload_is_not_normalized_or_truncated() -> None:
    class PayloadProvider:
        def to_log_payload(self) -> dict[str, Any]:
            _ = self
            return {
                "long": "x" * 250,
                "items": list(range(100)),
            }

    result = normalize_value_default(PayloadProvider())

    assert result == {
        "long": "x" * 250,
        "items": list(range(100)),
    }


def test_e03_log_payload_provider_exception_falls_back_to_type_placeholder() -> None:
    class PayloadProvider:
        def to_log_payload(self) -> dict[str, Any]:
            raise RuntimeError("boom")

    result = normalize_value_default(PayloadProvider())

    assert result == "<PayloadProvider>"


def test_e04_log_payload_provider_wins_over_adapter_resolver() -> None:
    class PayloadProvider:
        def to_log_payload(self) -> dict[str, Any]:
            _ = self
            return {"source": "provider"}

    def resolver(value: Any) -> Any:
        _ = value

        def adapter(obj: Any, verbosity_level: str) -> dict[str, Any]:
            _ = obj
            return {"source": "adapter", "verbosity_level": verbosity_level}

        return adapter

    result = normalize_value_for_log(
        PayloadProvider(),
        log_adapter_resolver=resolver,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == {"source": "provider"}


# ---------- F: type-based adapter resolver ----------


def test_f01_adapter_resolver_is_used_for_plain_object() -> None:
    class User:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

    def resolver(value: Any) -> Any:
        if isinstance(value, User):
            return lambda obj, verbosity_level: {
                "kind": "user",
                "id": obj.user_id,
                "verbosity_level": verbosity_level,
            }

        return None

    result = normalize_value_for_log(
        User("u-1"),
        log_adapter_resolver=resolver,
        verbosity_level="MAXIMUM",
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == {
        "kind": "user",
        "id": "u-1",
        "verbosity_level": "MAXIMUM",
    }


def test_f02_adapter_resolver_none_falls_back_to_type_placeholder() -> None:
    class User:
        pass

    def resolver(value: Any) -> Any:
        _ = value
        return None

    result = normalize_value_for_log(
        User(),
        log_adapter_resolver=resolver,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == "<User>"


def test_f03_adapter_is_not_called_when_verbosity_level_is_none() -> None:
    class User:
        pass

    called = False

    def resolver(value: Any) -> Any:
        _ = value

        def adapter(obj: Any, verbosity_level: str) -> dict[str, Any]:
            nonlocal called
            _ = obj
            _ = verbosity_level
            called = True
            return {"called": True}

        return adapter

    result = normalize_value_for_log(
        User(),
        log_adapter_resolver=resolver,
        verbosity_level=None,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert called is False
    assert result == "<User>"


def test_f04_resolver_exception_is_swallowed_and_falls_back_to_type_placeholder() -> None:
    class User:
        pass

    def resolver(value: Any) -> Any:
        _ = value
        raise RuntimeError("resolver failed")

    result = normalize_value_for_log(
        User(),
        log_adapter_resolver=resolver,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == "<User>"


def test_f05_adapter_exception_is_swallowed_and_falls_back_to_type_placeholder() -> None:
    class User:
        pass

    def resolver(value: Any) -> Any:
        _ = value

        def adapter(obj: Any, verbosity_level: str) -> dict[str, Any]:
            _ = obj
            _ = verbosity_level
            raise RuntimeError("adapter failed")

        return adapter

    result = normalize_value_for_log(
        User(),
        log_adapter_resolver=resolver,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == "<User>"


def test_f06_adapter_payload_is_used_verbatim() -> None:
    class User:
        pass

    def resolver(value: Any) -> Any:
        _ = value

        def adapter(obj: Any, verbosity_level: str) -> dict[str, Any]:
            _ = obj
            _ = verbosity_level
            return {
                "long": "x" * 250,
                "items": list(range(100)),
            }

        return adapter

    result = normalize_value_for_log(
        User(),
        log_adapter_resolver=resolver,
        verbosity_level=DEFAULT_VERBOSITY_LEVEL,
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == {
        "long": "x" * 250,
        "items": list(range(100)),
    }


# ---------- G: adapters inside containers ----------


def test_g01_adapter_is_used_for_list_leaf_items() -> None:
    class User:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

    def resolver(value: Any) -> Any:
        if isinstance(value, User):
            return lambda obj, verbosity_level: {
                "kind": "user",
                "id": obj.user_id,
                "verbosity_level": verbosity_level,
            }

        return None

    result = normalize_list_for_log(
        [User("u-1")],
        log_adapter_resolver=resolver,
        verbosity_level="NORMAL",
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == [
        {
            "kind": "user",
            "id": "u-1",
            "verbosity_level": "NORMAL",
        }
    ]


def test_g02_adapter_is_used_for_dict_leaf_values() -> None:
    class User:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

    def resolver(value: Any) -> Any:
        if isinstance(value, User):
            return lambda obj, verbosity_level: {
                "kind": "user",
                "id": obj.user_id,
                "verbosity_level": verbosity_level,
            }

        return None

    result = normalize_dict_for_log(
        {"user": User("u-1")},
        log_adapter_resolver=resolver,
        verbosity_level="NORMAL",
        max_items=DEFAULT_MAX_ITEMS,
        max_str_len=DEFAULT_MAX_STR_LEN,
    )

    assert result == {
        "user": {
            "kind": "user",
            "id": "u-1",
            "verbosity_level": "NORMAL",
        }
    }
