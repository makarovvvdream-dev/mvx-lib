# tests/test_logger/log_payload_processor/test_log_payload_processor.py
from __future__ import annotations

from collections.abc import Iterator, Mapping
from enum import Enum
from typing import Any
import concurrent.futures

import pytest

from mvx.common.logger.log_payload_processor.log_payload_processor import (
    LogPayloadProcessor,
)
from mvx.common.logger.log_payload_processor.types import (
    DEFAULT_MAX_ITEMS,
    DEFAULT_MAX_STR_LEN,
    LogAdapter,
    LogVerbosityLevel,
)


class UnknownObject:
    pass


class CustomMapping(Mapping[str, Any]):
    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


class PayloadProvider:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def to_log_payload(self) -> dict[str, Any]:
        return self._payload


class NonDictPayloadProvider:
    @staticmethod
    def to_log_payload() -> list[str]:
        return ["not", "a", "dict"]


class FailingPayloadProvider:
    def to_log_payload(self) -> dict[str, Any]:
        raise RuntimeError("provider failed")


class SampleEnum(Enum):
    TEXT = "alpha"
    NUMBER = 7
    LONG_TEXT = "abcdef"
    LIST_VALUE = [1, "abcdef"]
    MAPPING_VALUE = {"alpha": "abcdef"}


# ---- A. Constructor validation -----------------------------------------------------------


def test_a01_default_constructor_uses_default_effective_values() -> None:
    processor = LogPayloadProcessor()

    assert processor.verbosity_level == LogVerbosityLevel.NORMAL
    assert processor.max_str_len == DEFAULT_MAX_STR_LEN
    assert processor.max_items == DEFAULT_MAX_ITEMS
    assert processor.log_adapter_resolver is None


def test_a02_constructor_accepts_explicit_verbosity_level() -> None:
    processor = LogPayloadProcessor(verbosity_level=LogVerbosityLevel.MAXIMUM)

    assert processor.verbosity_level == LogVerbosityLevel.MAXIMUM


def test_a03_constructor_rejects_non_log_verbosity_level() -> None:
    with pytest.raises(TypeError, match="verbosity_level"):
        LogPayloadProcessor(verbosity_level="MAXIMUM")  # type: ignore[arg-type]


def test_a04_constructor_accepts_explicit_max_str_len() -> None:
    processor = LogPayloadProcessor(max_str_len=12)

    assert processor.max_str_len == 12


def test_a05_constructor_rejects_non_int_max_str_len() -> None:
    with pytest.raises(TypeError, match="max_str_len"):
        LogPayloadProcessor(max_str_len="12")  # type: ignore[arg-type]


def test_a06_constructor_rejects_max_str_len_less_than_one() -> None:
    with pytest.raises(ValueError, match="max_str_len"):
        LogPayloadProcessor(max_str_len=0)


def test_a07_constructor_accepts_explicit_max_items() -> None:
    processor = LogPayloadProcessor(max_items=3)

    assert processor.max_items == 3


def test_a08_constructor_rejects_non_int_max_items() -> None:
    with pytest.raises(TypeError, match="max_items"):
        LogPayloadProcessor(max_items="3")  # type: ignore[arg-type]


def test_a09_constructor_rejects_max_items_less_than_one() -> None:
    with pytest.raises(ValueError, match="max_items"):
        LogPayloadProcessor(max_items=0)


def test_a10_constructor_accepts_callable_log_adapter_resolver() -> None:
    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    assert processor.log_adapter_resolver is resolver


def test_a11_constructor_rejects_non_callable_log_adapter_resolver() -> None:
    with pytest.raises(TypeError, match="log_adapter_resolver"):
        LogPayloadProcessor(log_adapter_resolver="resolver")  # type: ignore[arg-type]


# ---- B. Mutable settings API -------------------------------------------------------------


def test_b01_verbosity_level_returns_normal_by_default() -> None:
    processor = LogPayloadProcessor()

    assert processor.verbosity_level == LogVerbosityLevel.NORMAL


def test_b02_set_verbosity_level_changes_effective_value() -> None:
    processor = LogPayloadProcessor()

    processor.set_verbosity_level(LogVerbosityLevel.MINIMAL)

    assert processor.verbosity_level == LogVerbosityLevel.MINIMAL


def test_b03_set_verbosity_level_rejects_invalid_type() -> None:
    processor = LogPayloadProcessor()

    with pytest.raises(TypeError, match="verbosity_level"):
        processor.set_verbosity_level("MINIMAL")  # type: ignore[arg-type]


def test_b04_reset_verbosity_level_restores_normal() -> None:
    processor = LogPayloadProcessor(verbosity_level=LogVerbosityLevel.MAXIMUM)

    processor.reset_verbosity_level()

    assert processor.verbosity_level == LogVerbosityLevel.NORMAL


def test_b05_max_str_len_returns_default_by_default() -> None:
    processor = LogPayloadProcessor()

    assert processor.max_str_len == DEFAULT_MAX_STR_LEN


def test_b06_set_max_str_len_changes_effective_value() -> None:
    processor = LogPayloadProcessor()

    processor.set_max_str_len(5)

    assert processor.max_str_len == 5


def test_b07_set_max_str_len_rejects_non_int_value() -> None:
    processor = LogPayloadProcessor()

    with pytest.raises(TypeError, match="max_str_len"):
        processor.set_max_str_len("5")  # type: ignore[arg-type]


def test_b08_set_max_str_len_rejects_value_less_than_one() -> None:
    processor = LogPayloadProcessor()

    with pytest.raises(ValueError, match="max_str_len"):
        processor.set_max_str_len(0)


def test_b09_reset_max_str_len_restores_default() -> None:
    processor = LogPayloadProcessor(max_str_len=5)

    processor.reset_max_str_len()

    assert processor.max_str_len == DEFAULT_MAX_STR_LEN


def test_b10_max_items_returns_default_by_default() -> None:
    processor = LogPayloadProcessor()

    assert processor.max_items == DEFAULT_MAX_ITEMS


def test_b11_set_max_items_changes_effective_value() -> None:
    processor = LogPayloadProcessor()

    processor.set_max_items(5)

    assert processor.max_items == 5


def test_b12_set_max_items_rejects_non_int_value() -> None:
    processor = LogPayloadProcessor()

    with pytest.raises(TypeError, match="max_items"):
        processor.set_max_items("5")  # type: ignore[arg-type]


def test_b13_set_max_items_rejects_value_less_than_one() -> None:
    processor = LogPayloadProcessor()

    with pytest.raises(ValueError, match="max_items"):
        processor.set_max_items(0)


def test_b14_reset_max_items_restores_default() -> None:
    processor = LogPayloadProcessor(max_items=5)

    processor.reset_max_items()

    assert processor.max_items == DEFAULT_MAX_ITEMS


def test_b15_log_adapter_resolver_returns_none_by_default() -> None:
    processor = LogPayloadProcessor()

    assert processor.log_adapter_resolver is None


def test_b16_set_log_adapter_resolver_changes_resolver() -> None:
    processor = LogPayloadProcessor()

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return None

    processor.set_log_adapter_resolver(resolver)

    assert processor.log_adapter_resolver is resolver


def test_b17_set_log_adapter_resolver_rejects_non_callable() -> None:
    processor = LogPayloadProcessor()

    with pytest.raises(TypeError, match="log_adapter_resolver"):
        processor.set_log_adapter_resolver("resolver")  # type: ignore[arg-type]


def test_b18_reset_log_adapter_resolver_restores_none() -> None:
    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    processor.reset_log_adapter_resolver()

    assert processor.log_adapter_resolver is None


# ---- C. normalize_payload public contract ------------------------------------------------


def test_c01_normalize_payload_returns_dict_for_mapping_input() -> None:
    processor = LogPayloadProcessor()
    payload = CustomMapping({"alpha": 1})

    result = processor.normalize_payload(payload)

    assert result == {"alpha": 1}


def test_c02_normalize_payload_returns_empty_dict_for_non_mapping_input() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload(["not", "mapping"])  # type: ignore[arg-type]

    assert result == {}


def test_c03_normalize_payload_converts_keys_to_strings() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({1: "one", None: "none"})  # type: ignore[dict-item]

    assert result == {"1": "one", "None": "none"}


def test_c04_normalize_payload_truncates_long_keys() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_payload({"abcdef": 1})

    assert result == {"abc...": 1}


def test_c05_normalize_payload_limits_number_of_top_level_keys() -> None:
    processor = LogPayloadProcessor(max_items=2)

    result = processor.normalize_payload({"a": 1, "b": 2, "c": 3})

    assert result == {"a": 1, "b": 2, "__more__": "1 more keys"}


def test_c06_normalize_payload_adds_more_when_top_level_keys_exceed_max_items() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_payload({"a": 1, "b": 2, "c": 3})

    assert result["__more__"] == "2 more keys"


def test_c07_normalize_payload_does_not_add_more_when_key_count_equals_max_items() -> None:
    processor = LogPayloadProcessor(max_items=2)

    result = processor.normalize_payload({"a": 1, "b": 2})

    assert result == {"a": 1, "b": 2}


def test_c08_normalize_payload_with_unbounded_true_does_not_limit_key_count() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_payload({"a": 1, "b": 2}, unbounded=True)

    assert result == {"a": 1, "b": 2}


def test_c09_normalize_payload_with_unbounded_true_still_truncates_keys() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_payload({"abcdef": 1}, unbounded=True)

    assert result == {"abc...": 1}


# ---- D. Primitive normalization ----------------------------------------------------------


def test_d01_str_is_preserved_when_within_max_str_len() -> None:
    processor = LogPayloadProcessor(max_str_len=5)

    assert processor.normalize_value_for_log("abc") == "abc"


def test_d02_long_str_is_truncated_with_ellipsis() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    assert processor.normalize_value_for_log("abcdef") == "abc..."


def test_d03_long_str_is_still_truncated_when_unbounded_true() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    assert processor.normalize_value_for_log("abcdef", unbounded=True) == "abc..."


def test_d04_bytes_are_preserved_as_bytes() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(b"abc") == b"abc"


def test_d05_bytearray_is_converted_to_bytes() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(bytearray(b"abc")) == b"abc"


def test_d06_memoryview_is_converted_to_bytes() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(memoryview(b"abc")) == b"abc"


def test_d07_int_is_preserved() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(123) == 123


def test_d08_float_is_preserved() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(1.25) == 1.25


def test_d09_bool_is_preserved() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(True) is True


def test_d10_none_is_preserved() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(None) is None


def test_d11_unknown_object_becomes_class_name_placeholder() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(UnknownObject()) == "<UnknownObject>"


# ---- E. List / tuple normalization -------------------------------------------------------


def test_e01_normalize_value_for_log_normalizes_list_items_as_leaves() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log(["abcdef", 1, None])

    assert result == ["abc...", 1, None]


def test_e02_normalize_value_for_log_normalizes_tuple_items_as_leaves() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log(("abcdef", 1, None))

    assert result == ["abc...", 1, None]


def test_e03_list_normalization_respects_max_items() -> None:
    processor = LogPayloadProcessor(max_items=2)

    result = processor.normalize_value_for_log([1, 2, 3])

    assert result == [1, 2, "... (1 more)"]


def test_e04_list_normalization_appends_more_marker_when_truncated() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_value_for_log([1, 2, 3])

    assert result == [1, "... (2 more)"]


def test_e05_list_normalization_does_not_append_marker_when_length_equals_max_items() -> None:
    processor = LogPayloadProcessor(max_items=3)

    result = processor.normalize_value_for_log([1, 2, 3])

    assert result == [1, 2, 3]


def test_e06_list_normalization_with_unbounded_true_does_not_limit_items() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_value_for_log([1, 2, 3], unbounded=True)

    assert result == [1, 2, 3]


def test_e07_nested_list_item_is_represented_as_list_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([[1, 2]])

    assert result == ["<list>"]


def test_e08_nested_dict_item_is_represented_as_dict_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([{"a": 1}])

    assert result == ["<dict>"]


def test_e09_nested_tuple_item_is_represented_as_tuple_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([(1, 2)])

    assert result == ["<tuple>"]


def test_e10_unknown_object_list_item_is_represented_as_class_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([UnknownObject()])

    assert result == ["<UnknownObject>"]


def test_e11_list_with_mixed_short_primitives_is_preserved() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([1, "x", b"t", True, False, None])

    assert result == [1, "x", b"t", True, False, None]


def test_e12_list_normalization_converts_bytearray_leaf_item_to_bytes() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([bytearray(b"abc")])

    assert result == [b"abc"]


def test_e13_list_normalization_converts_memoryview_leaf_item_to_bytes() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([memoryview(b"abc")])

    assert result == [b"abc"]


def test_e14_list_normalization_uses_provider_for_leaf_item() -> None:
    processor = LogPayloadProcessor()
    value = PayloadProvider({"source": "provider", "nested": [1, 2, 3]})

    result = processor.normalize_value_for_log([value])

    assert result == [
        {
            "source": "provider",
            "nested": [1, 2, 3],
        }
    ]


def test_e15_list_normalization_uses_adapter_for_leaf_item() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value
        return {
            "source": "adapter",
            "verbosity_level": verbosity_level.value,
        }

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(
        verbosity_level=LogVerbosityLevel.MAXIMUM,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_value_for_log([UnknownObject()])

    assert result == [
        {
            "source": "adapter",
            "verbosity_level": "MAXIMUM",
        }
    ]


def test_e16_list_normalization_falls_back_when_provider_leaf_returns_non_dict() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([NonDictPayloadProvider()])

    assert result == ["<NonDictPayloadProvider>"]


def test_e17_list_normalization_falls_back_when_provider_leaf_raises() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log([FailingPayloadProvider()])

    assert result == ["<FailingPayloadProvider>"]


def test_e18_list_normalization_falls_back_when_adapter_leaf_returns_non_dict() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> list[str]:
        _ = value, verbosity_level
        return ["not", "dict"]

    def resolver(value: Any) -> Any:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log([UnknownObject()])

    assert result == ["<UnknownObject>"]


def test_e19_list_normalization_falls_back_when_adapter_leaf_raises() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        raise RuntimeError("adapter failed")

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log([UnknownObject()])

    assert result == ["<UnknownObject>"]


# ---- F. Mapping normalization as value ---------------------------------------------------


def test_f01_normalize_value_for_log_normalizes_dict_as_mapping() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"a": 1})

    assert result == {"a": 1}


def test_f02_normalize_value_for_log_normalizes_custom_mapping() -> None:
    processor = LogPayloadProcessor()
    value = CustomMapping({"a": 1})

    result = processor.normalize_value_for_log(value)

    assert result == {"a": 1}


def test_f03_mapping_values_are_normalized_as_leaves() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log({"a": "abcdef", "b": 2})

    assert result == {"a": "abc...", "b": 2}


def test_f04_nested_mapping_value_is_represented_as_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"nested": {"a": 1}})

    assert result == {"nested": "<dict>"}


def test_f05_mapping_normalization_respects_max_items() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_value_for_log({"a": 1, "b": 2})

    assert result == {"a": 1, "__more__": "1 more keys"}


def test_f06_mapping_normalization_respects_max_str_len_for_keys() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log({"abcdef": 1})

    assert result == {"abc...": 1}


def test_f07_mapping_normalization_with_unbounded_true_disables_item_limit_but_not_key_truncation() -> (
    None
):
    processor = LogPayloadProcessor(max_items=1, max_str_len=3)

    result = processor.normalize_value_for_log({"abcdef": 1, "ghijkl": 2}, unbounded=True)

    assert result == {"abc...": 1, "ghi...": 2}


def test_f08_mapping_with_short_primitive_values_is_preserved() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log(
        {
            "a": 1,
            "b": "x",
            "c": b"t",
            "d": True,
            "e": False,
            "f": None,
        }
    )

    assert result == {
        "a": 1,
        "b": "x",
        "c": b"t",
        "d": True,
        "e": False,
        "f": None,
    }


def test_f09_mapping_value_bytearray_is_converted_to_bytes() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"value": bytearray(b"abc")})

    assert result == {"value": b"abc"}


def test_f10_mapping_value_memoryview_is_converted_to_bytes() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"value": memoryview(b"abc")})

    assert result == {"value": b"abc"}


def test_f11_mapping_unknown_object_value_becomes_class_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"value": UnknownObject()})

    assert result == {"value": "<UnknownObject>"}


def test_f12_mapping_tuple_value_is_represented_as_tuple_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"value": (1, 2)})

    assert result == {"value": "<tuple>"}


def test_f13_mapping_list_value_is_represented_as_list_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"value": [1, 2]})

    assert result == {"value": "<list>"}


def test_f14_mapping_custom_mapping_value_is_represented_as_class_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log(
        {
            "value": CustomMapping({"a": 1}),
        }
    )

    assert result == {"value": "<CustomMapping>"}


def test_f15_mapping_value_provider_is_used_as_leaf_custom_payload() -> None:
    processor = LogPayloadProcessor()
    value = PayloadProvider({"source": "provider", "nested": [1, 2, 3]})

    result = processor.normalize_value_for_log({"value": value})

    assert result == {
        "value": {
            "source": "provider",
            "nested": [1, 2, 3],
        }
    }


def test_f16_mapping_value_adapter_is_used_as_leaf_custom_payload() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value
        return {
            "source": "adapter",
            "verbosity_level": verbosity_level.value,
        }

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(
        verbosity_level=LogVerbosityLevel.MINIMAL,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_value_for_log({"value": UnknownObject()})

    assert result == {
        "value": {
            "source": "adapter",
            "verbosity_level": "MINIMAL",
        }
    }


def test_f17_mapping_value_provider_returning_non_dict_falls_back_to_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"value": NonDictPayloadProvider()})

    assert result == {"value": "<NonDictPayloadProvider>"}


def test_f18_mapping_value_provider_raising_falls_back_to_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log({"value": FailingPayloadProvider()})

    assert result == {"value": "<FailingPayloadProvider>"}


def test_f19_mapping_value_adapter_returning_non_dict_falls_back_to_placeholder() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> list[str]:
        _ = value, verbosity_level
        return ["not", "dict"]

    def resolver(value: Any) -> Any:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log({"value": UnknownObject()})

    assert result == {"value": "<UnknownObject>"}


def test_f20_mapping_value_adapter_raising_falls_back_to_placeholder() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        raise RuntimeError("adapter failed")

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log({"value": UnknownObject()})

    assert result == {"value": "<UnknownObject>"}


# ---- G. Enum normalization ---------------------------------------------------------------


def test_g01_enum_with_str_value_normalizes_to_str_value() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(SampleEnum.TEXT) == "alpha"


def test_g02_enum_with_int_value_normalizes_to_int_value() -> None:
    processor = LogPayloadProcessor()

    assert processor.normalize_value_for_log(SampleEnum.NUMBER) == 7


def test_g03_enum_with_long_str_value_respects_max_str_len() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    assert processor.normalize_value_for_log(SampleEnum.LONG_TEXT) == "abc..."


def test_g04_enum_with_list_value_normalizes_list_via_value_core() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log(SampleEnum.LIST_VALUE)

    assert result == [1, "abc..."]


def test_g05_enum_with_mapping_value_normalizes_mapping_via_value_core() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log(SampleEnum.MAPPING_VALUE)

    assert result == {"alp...": "abc..."}


def test_g06_enum_with_tuple_value_normalizes_tuple_via_value_core() -> None:
    class TupleEnum(Enum):
        VALUE = (1, "abcdef")

    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log(TupleEnum.VALUE)

    assert result == [1, "abc..."]


def test_g07_enum_inside_list_is_normalized_as_leaf_value() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log([SampleEnum.LONG_TEXT])

    assert result == ["abc..."]


def test_g08_enum_inside_mapping_is_normalized_as_leaf_value() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_value_for_log({"status": SampleEnum.LONG_TEXT})

    assert result == {"sta...": "abc..."}


def test_g09_enum_with_unknown_object_value_falls_back_to_object_placeholder() -> None:
    class ObjectEnum(Enum):
        VALUE = UnknownObject()

    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log(ObjectEnum.VALUE)

    assert result == "<UnknownObject>"


def test_g10_enum_with_nested_list_item_preserves_shallow_list_policy() -> None:
    class NestedListEnum(Enum):
        VALUE = [[1, 2]]

    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log(NestedListEnum.VALUE)

    assert result == ["<list>"]


def test_g11_enum_with_nested_mapping_value_preserves_shallow_mapping_policy() -> None:
    class NestedMappingEnum(Enum):
        VALUE = {"nested": {"a": 1}}

    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log(NestedMappingEnum.VALUE)

    assert result == {"nested": "<dict>"}


# ---- H. LogPayloadProvider precedence ----------------------------------------------------


def test_h01_provider_payload_is_returned_as_is() -> None:
    processor = LogPayloadProcessor()
    value = PayloadProvider({"a": {"nested": [1, 2, 3]}})

    result = processor.normalize_value_for_log(value)

    assert result == {"a": {"nested": [1, 2, 3]}}


def test_h02_provider_payload_is_not_truncated_by_max_items() -> None:
    processor = LogPayloadProcessor(max_items=1)
    value = PayloadProvider({"a": 1, "b": 2})

    result = processor.normalize_value_for_log(value)

    assert result == {"a": 1, "b": 2}


def test_h03_provider_payload_is_not_truncated_by_max_str_len() -> None:
    processor = LogPayloadProcessor(max_str_len=1)
    value = PayloadProvider({"abcdef": "ghijkl"})

    result = processor.normalize_value_for_log(value)

    assert result == {"abcdef": "ghijkl"}


def test_h04_provider_returning_non_dict_falls_back_to_default_normalization() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log(NonDictPayloadProvider())

    assert result == "<NonDictPayloadProvider>"


def test_h05_provider_raising_exception_falls_back_to_default_normalization() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_value_for_log(FailingPayloadProvider())

    assert result == "<FailingPayloadProvider>"


def test_h06_provider_takes_precedence_over_adapter_resolver() -> None:
    def adapter(_value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = _value, verbosity_level
        return {"adapter": True}

    def resolver(_value: Any) -> LogAdapter | None:
        _ = _value
        return adapter

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)
    value = PayloadProvider({"provider": True})

    result = processor.normalize_value_for_log(value)

    assert result == {"provider": True}


def test_h07_provider_inside_payload_mapping_is_used_as_leaf_custom_payload() -> None:
    processor = LogPayloadProcessor(max_items=1, max_str_len=1)
    value = PayloadProvider(
        {
            "abcdef": "ghijkl",
            "items": [1, 2, 3],
        }
    )

    result = processor.normalize_payload({"value": value})

    assert result == {
        "v...": {
            "abcdef": "ghijkl",
            "items": [1, 2, 3],
        }
    }


def test_h08_provider_inside_value_list_wins_over_adapter_resolver() -> None:
    def adapter(_value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = _value, verbosity_level
        return {"source": "adapter"}

    def resolver(_value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)
    value = PayloadProvider({"source": "provider"})

    result = processor.normalize_value_for_log([value])

    assert result == [{"source": "provider"}]


def test_h09_provider_inside_value_mapping_wins_over_adapter_resolver() -> None:
    def adapter(_value: Any, _verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        return {"source": "adapter"}

    def resolver(_value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)
    value = PayloadProvider({"source": "provider"})

    result = processor.normalize_value_for_log({"value": value})

    assert result == {"value": {"source": "provider"}}


def test_h10_provider_payload_with_many_keys_is_returned_as_is_inside_list() -> None:
    processor = LogPayloadProcessor(max_items=1)
    value = PayloadProvider({"a": 1, "b": 2, "c": 3})

    result = processor.normalize_value_for_log([value])

    assert result == [{"a": 1, "b": 2, "c": 3}]


def test_h11_provider_payload_with_long_strings_is_returned_as_is_inside_mapping() -> None:
    processor = LogPayloadProcessor(max_str_len=1)
    value = PayloadProvider({"long": "abcdef"})

    result = processor.normalize_value_for_log({"value": value})

    assert result == {"v...": {"long": "abcdef"}}


# ---- I. Adapter resolver behavior --------------------------------------------------------


def test_i01_resolver_returning_none_falls_back_to_default_normalization() -> None:
    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log(UnknownObject())

    assert result == "<UnknownObject>"


def test_i02_resolver_returning_adapter_uses_adapter_payload() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"adapted": True}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log(UnknownObject())

    assert result == {"adapted": True}


def test_i03_adapter_receives_current_verbosity_level() -> None:
    received: list[LogVerbosityLevel] = []

    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value
        received.append(verbosity_level)
        return {"adapted": True}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    processor = LogPayloadProcessor(
        verbosity_level=LogVerbosityLevel.MAXIMUM,
        log_adapter_resolver=resolver,
    )

    processor.normalize_value_for_log(UnknownObject())

    assert received == [LogVerbosityLevel.MAXIMUM]


def test_i04_adapter_payload_is_returned_as_is() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"a": {"nested": [1, 2, 3]}}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log(UnknownObject())

    assert result == {"a": {"nested": [1, 2, 3]}}


def test_i05_adapter_payload_is_not_truncated_by_max_items() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"a": 1, "b": 2}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    processor = LogPayloadProcessor(max_items=1, log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log(UnknownObject())

    assert result == {"a": 1, "b": 2}


def test_i06_adapter_payload_is_not_truncated_by_max_str_len() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"abcdef": "ghijkl"}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    processor = LogPayloadProcessor(max_str_len=1, log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log(UnknownObject())

    assert result == {"abcdef": "ghijkl"}


def test_i07_adapter_returning_non_dict_falls_back_to_default_normalization() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> list[str]:
        _ = value, verbosity_level
        return ["not", "dict"]

    def resolver(value: Any) -> Any:
        _ = value
        return adapter

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log(UnknownObject())

    assert result == "<UnknownObject>"


def test_i08_resolver_raising_exception_falls_back_to_default_normalization() -> None:
    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        raise RuntimeError("resolver failed")

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log(UnknownObject())

    assert result == "<UnknownObject>"


def test_i09_adapter_raising_exception_falls_back_to_default_normalization() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        raise RuntimeError("adapter failed")

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_value_for_log(UnknownObject())

    assert result == "<UnknownObject>"


def test_i10_changing_resolver_via_setter_affects_subsequent_normalization() -> None:
    processor = LogPayloadProcessor()

    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"adapted": True}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    assert processor.normalize_value_for_log(UnknownObject()) == "<UnknownObject>"

    processor.set_log_adapter_resolver(resolver)

    assert processor.normalize_value_for_log(UnknownObject()) == {"adapted": True}


def test_i11_resetting_resolver_disables_adapter_path() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"adapted": True}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    assert processor.normalize_value_for_log(UnknownObject()) == {"adapted": True}

    processor.reset_log_adapter_resolver()

    assert processor.normalize_value_for_log(UnknownObject()) == "<UnknownObject>"


def test_i12_adapter_is_used_for_list_leaf_items() -> None:
    class User:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        assert isinstance(value, User)
        return {
            "kind": "user",
            "id": value.user_id,
            "verbosity_level": verbosity_level.value,
        }

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, User):
            return adapter
        return None

    processor = LogPayloadProcessor(
        verbosity_level=LogVerbosityLevel.NORMAL,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_value_for_log([User("u-1")])

    assert result == [
        {
            "kind": "user",
            "id": "u-1",
            "verbosity_level": "NORMAL",
        }
    ]


def test_i13_adapter_is_used_for_mapping_leaf_values() -> None:
    class User:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        assert isinstance(value, User)
        return {
            "kind": "user",
            "id": value.user_id,
            "verbosity_level": verbosity_level.value,
        }

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, User):
            return adapter
        return None

    processor = LogPayloadProcessor(
        verbosity_level=LogVerbosityLevel.NORMAL,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_value_for_log({"user": User("u-1")})

    assert result == {
        "user": {
            "kind": "user",
            "id": "u-1",
            "verbosity_level": "NORMAL",
        }
    }


def test_i14_adapter_is_used_for_payload_mapping_leaf_values() -> None:
    class User:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        assert isinstance(value, User)
        return {
            "kind": "user",
            "id": value.user_id,
            "verbosity_level": verbosity_level.value,
        }

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, User):
            return adapter
        return None

    processor = LogPayloadProcessor(
        verbosity_level=LogVerbosityLevel.MAXIMUM,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_payload({"user": User("u-1")})

    assert result == {
        "user": {
            "kind": "user",
            "id": "u-1",
            "verbosity_level": "MAXIMUM",
        }
    }


def test_i15_adapter_result_is_not_post_processed_inside_list() -> None:
    def adapter(_value: Any, _verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        return {
            "abcdef": "ghijkl",
            "items": [1, 2, 3],
        }

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(
        max_items=1,
        max_str_len=1,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_value_for_log([UnknownObject()])

    assert result == [
        {
            "abcdef": "ghijkl",
            "items": [1, 2, 3],
        }
    ]


def test_i16_adapter_result_is_not_post_processed_inside_mapping() -> None:
    def adapter(_value: Any, _verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        return {
            "abcdef": "ghijkl",
            "items": [1, 2, 3],
        }

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(
        max_items=1,
        max_str_len=1,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_value_for_log({"value": UnknownObject()})

    assert result == {
        "v...": {
            "abcdef": "ghijkl",
            "items": [1, 2, 3],
        }
    }


def test_i17_adapter_result_is_not_post_processed_inside_payload_mapping() -> None:
    def adapter(_value: Any, _verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        return {
            "abcdef": "ghijkl",
            "items": [1, 2, 3],
        }

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(
        max_items=1,
        max_str_len=1,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_payload({"value": UnknownObject()})

    assert result == {
        "v...": {
            "abcdef": "ghijkl",
            "items": [1, 2, 3],
        }
    }


# ---- J. unbounded mode -------------------------------------------------------------------


def test_j01_unbounded_true_disables_max_items_for_payload_mapping() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_payload({"a": 1, "b": 2}, unbounded=True)

    assert result == {"a": 1, "b": 2}


def test_j02_unbounded_true_does_not_disable_max_str_len_for_payload_keys() -> None:
    processor = LogPayloadProcessor(max_str_len=1)

    result = processor.normalize_payload({"abcdef": 1}, unbounded=True)

    assert result == {"a...": 1}


def test_j03_unbounded_true_disables_max_items_for_value_list() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_value_for_log([1, 2, 3], unbounded=True)

    assert result == [1, 2, 3]


def test_j04_unbounded_true_does_not_disable_max_str_len_for_value_strings() -> None:
    processor = LogPayloadProcessor(max_str_len=1)

    result = processor.normalize_value_for_log("abcdef", unbounded=True)

    assert result == "a..."


def test_j05_unbounded_true_does_not_post_process_provider_payload() -> None:
    processor = LogPayloadProcessor(max_items=1, max_str_len=1)
    value = PayloadProvider({"abcdef": "ghijkl", "extra": [1, 2, 3]})

    result = processor.normalize_value_for_log(value, unbounded=True)

    assert result == {"abcdef": "ghijkl", "extra": [1, 2, 3]}


def test_j06_unbounded_true_does_not_post_process_adapter_payload() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"abcdef": "ghijkl", "extra": [1, 2, 3]}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    processor = LogPayloadProcessor(
        max_items=1,
        max_str_len=1,
        log_adapter_resolver=resolver,
    )

    result = processor.normalize_value_for_log(UnknownObject(), unbounded=True)

    assert result == {"abcdef": "ghijkl", "extra": [1, 2, 3]}


def test_j07_unbounded_true_does_not_disable_string_truncation_for_list_leaf_items() -> None:
    processor = LogPayloadProcessor(max_str_len=1)

    result = processor.normalize_value_for_log(["abcdef"], unbounded=True)

    assert result == ["a..."]


def test_j08_unbounded_true_does_not_disable_string_truncation_for_mapping_leaf_values() -> None:
    processor = LogPayloadProcessor(max_str_len=1)

    result = processor.normalize_value_for_log({"value": "abcdef"}, unbounded=True)

    assert result == {"v...": "a..."}


def test_j09_unbounded_true_disables_mapping_more_marker_for_value_mapping() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_value_for_log(
        {
            "a": 1,
            "b": 2,
            "c": 3,
        },
        unbounded=True,
    )

    assert result == {
        "a": 1,
        "b": 2,
        "c": 3,
    }


def test_j10_unbounded_true_disables_list_more_marker_for_tuple_value() -> None:
    processor = LogPayloadProcessor(max_items=1)

    result = processor.normalize_value_for_log((1, 2, 3), unbounded=True)

    assert result == [1, 2, 3]


def test_j11_unbounded_true_preserves_shallow_policy_for_payload_nested_containers() -> None:
    processor = LogPayloadProcessor(max_items=1, max_str_len=1)

    result = processor.normalize_payload(
        {
            "list_value": [1, 2, 3],
            "dict_value": {"a": 1},
            "tuple_value": (1, 2),
        },
        unbounded=True,
    )

    assert result == {
        "l...": "<list>",
        "d...": "<dict>",
        "t...": "<tuple>",
    }


# ---- K. Shallow payload policy -----------------------------------------------------------


def test_k01_normalize_payload_represents_top_level_list_value_as_list_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"items": [1, 2, 3]})

    assert result == {"items": "<list>"}


def test_k02_normalize_payload_represents_top_level_tuple_value_as_tuple_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"items": (1, 2, 3)})

    assert result == {"items": "<tuple>"}


def test_k03_normalize_payload_represents_top_level_dict_value_as_dict_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"details": {"a": 1}})

    assert result == {"details": "<dict>"}


def test_k04_normalize_payload_represents_top_level_custom_mapping_value_as_class_placeholder() -> (
    None
):
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"details": CustomMapping({"a": 1})})

    assert result == {"details": "<CustomMapping>"}


def test_k05_normalize_payload_expands_top_level_value_with_to_log_payload() -> None:
    processor = LogPayloadProcessor()
    value = PayloadProvider({"nested": [1, 2, 3]})

    result = processor.normalize_payload({"value": value})

    assert result == {"value": {"nested": [1, 2, 3]}}


def test_k06_normalize_payload_expands_top_level_value_handled_by_adapter() -> None:
    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"adapted": [1, 2, 3]}

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_payload({"value": UnknownObject()})

    assert result == {"value": {"adapted": [1, 2, 3]}}


def test_k07_normalize_payload_represents_unknown_object_value_as_class_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"value": UnknownObject()})

    assert result == {"value": "<UnknownObject>"}


def test_k08_normalize_payload_normalizes_enum_value_as_leaf() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_payload({"status": SampleEnum.LONG_TEXT})

    assert result == {"sta...": "abc..."}


def test_k09_normalize_payload_converts_bytearray_value_to_bytes() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"value": bytearray(b"abc")})

    assert result == {"value": b"abc"}


def test_k10_normalize_payload_converts_memoryview_value_to_bytes() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"value": memoryview(b"abc")})

    assert result == {"value": b"abc"}


def test_k11_normalize_payload_truncates_string_leaf_values() -> None:
    processor = LogPayloadProcessor(max_str_len=3)

    result = processor.normalize_payload({"value": "abcdef"})

    assert result == {"val...": "abc..."}


def test_k12_normalize_payload_preserves_short_primitive_leaf_values() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload(
        {
            "int": 1,
            "float": 1.5,
            "bool_true": True,
            "bool_false": False,
            "none": None,
            "bytes": b"abc",
        }
    )

    assert result == {
        "int": 1,
        "float": 1.5,
        "bool_true": True,
        "bool_false": False,
        "none": None,
        "bytes": b"abc",
    }


def test_k13_normalize_payload_provider_returning_non_dict_falls_back_to_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"value": NonDictPayloadProvider()})

    assert result == {"value": "<NonDictPayloadProvider>"}


def test_k14_normalize_payload_provider_raising_falls_back_to_placeholder() -> None:
    processor = LogPayloadProcessor()

    result = processor.normalize_payload({"value": FailingPayloadProvider()})

    assert result == {"value": "<FailingPayloadProvider>"}


def test_k15_normalize_payload_adapter_returning_non_dict_falls_back_to_placeholder() -> None:
    def adapter(_value: Any, _verbosity_level: LogVerbosityLevel) -> list[str]:
        return ["not", "dict"]

    def resolver(value: Any) -> Any:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_payload({"value": UnknownObject()})

    assert result == {"value": "<UnknownObject>"}


def test_k16_normalize_payload_adapter_raising_falls_back_to_placeholder() -> None:
    def adapter(_value: Any, _verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        raise RuntimeError("adapter failed")

    def resolver(value: Any) -> LogAdapter | None:
        if isinstance(value, UnknownObject):
            return adapter
        return None

    processor = LogPayloadProcessor(log_adapter_resolver=resolver)

    result = processor.normalize_payload({"value": UnknownObject()})

    assert result == {"value": "<UnknownObject>"}


# ---- L. Thread-safety smoke tests --------------------------------------------------------


def test_l01_concurrent_reads_of_effective_settings_do_not_fail() -> None:
    processor = LogPayloadProcessor(max_items=3, max_str_len=5)

    def read_settings() -> None:
        for _ in range(500):
            assert isinstance(processor.verbosity_level, LogVerbosityLevel)
            assert isinstance(processor.max_items, int)
            assert isinstance(processor.max_str_len, int)
            resolver = processor.log_adapter_resolver
            assert resolver is None or callable(resolver)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(read_settings) for _ in range(16)]

        for future in futures:
            future.result()


def test_l02_concurrent_normalize_payload_calls_do_not_fail() -> None:
    processor = LogPayloadProcessor(max_items=5, max_str_len=5)

    def normalize() -> None:
        for _ in range(500):
            result = processor.normalize_payload(
                {
                    "abcdef": "ghijkl",
                    "items": [1, 2, 3],
                    "details": {"a": 1},
                }
            )
            assert isinstance(result, dict)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(normalize) for _ in range(16)]

        for future in futures:
            future.result()


def test_l03_concurrent_setting_changes_and_normalize_payload_calls_do_not_fail() -> None:
    processor = LogPayloadProcessor()

    def writer() -> None:
        for i in range(500):
            processor.set_max_items((i % 5) + 1)
            processor.set_max_str_len((i % 7) + 1)
            processor.set_verbosity_level(
                LogVerbosityLevel.MAXIMUM if i % 2 else LogVerbosityLevel.MINIMAL
            )

    def reader() -> None:
        for _ in range(500):
            result = processor.normalize_payload(
                {
                    "abcdef": "ghijkl",
                    "key2": "value2",
                    "key3": "value3",
                    "key4": "value4",
                }
            )
            assert isinstance(result, dict)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(writer),
            executor.submit(writer),
            *[executor.submit(reader) for _ in range(10)],
        ]

        for future in futures:
            future.result()


def test_l04_concurrent_resolver_reset_and_normalize_value_for_log_calls_do_not_fail() -> None:
    processor = LogPayloadProcessor()

    def adapter(value: Any, verbosity_level: LogVerbosityLevel) -> dict[str, Any]:
        _ = value, verbosity_level
        return {"adapted": True}

    def resolver(value: Any) -> LogAdapter | None:
        _ = value
        return adapter

    def writer() -> None:
        for i in range(500):
            if i % 2:
                processor.set_log_adapter_resolver(resolver)
            else:
                processor.reset_log_adapter_resolver()

    def reader() -> None:
        for _ in range(500):
            result = processor.normalize_value_for_log(UnknownObject())
            assert result == {"adapted": True} or result == "<UnknownObject>"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(writer),
            executor.submit(writer),
            *[executor.submit(reader) for _ in range(10)],
        ]

        for future in futures:
            future.result()
