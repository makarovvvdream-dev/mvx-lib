# tests/test_helpers/test_introspection.py

from __future__ import annotations

from typing import Any, cast

from mvx.common.helpers.introspection import get_func_module_and_qualname


def module_level_function() -> None:
    pass


class SampleClass:
    def instance_method(self) -> None:
        pass

    @classmethod
    def class_method(cls) -> None:
        pass

    @staticmethod
    def static_method() -> None:
        pass


class CallableObject:
    def __call__(self) -> None:
        pass


# -------------------------
# Group a: normal Python callables
# -------------------------


def test_a01_module_level_function_returns_module_and_qualname() -> None:
    module, qualname = get_func_module_and_qualname(module_level_function)

    assert module == __name__
    assert qualname == "module_level_function"


def test_a02_nested_function_returns_nested_qualname() -> None:
    def nested_function() -> None:
        pass

    module, qualname = get_func_module_and_qualname(nested_function)

    assert module == __name__
    assert qualname.endswith(
        "test_a02_nested_function_returns_nested_qualname.<locals>.nested_function"
    )


def test_a03_lambda_returns_module_and_lambda_qualname() -> None:
    func = lambda: None  # noqa: E731

    module, qualname = get_func_module_and_qualname(func)

    assert module == __name__
    assert qualname.endswith("<lambda>")


def test_a04_instance_method_returns_class_method_qualname() -> None:
    instance = SampleClass()

    module, qualname = get_func_module_and_qualname(instance.instance_method)

    assert module == __name__
    assert qualname == "SampleClass.instance_method"


def test_a05_class_method_returns_class_method_qualname() -> None:
    module, qualname = get_func_module_and_qualname(SampleClass.class_method)

    assert module == __name__
    assert qualname == "SampleClass.class_method"


def test_a06_static_method_returns_class_method_qualname() -> None:
    module, qualname = get_func_module_and_qualname(SampleClass.static_method)

    assert module == __name__
    assert qualname == "SampleClass.static_method"


def test_a07_callable_object_returns_object_class_call_qualname() -> None:
    callable_object = CallableObject()

    module, qualname = get_func_module_and_qualname(callable_object.__call__)

    assert module == __name__
    assert qualname == "CallableObject.__call__"


# -------------------------
# Group b: fallback behavior
# -------------------------


def test_b01_falls_back_to_name_when_qualname_is_missing() -> None:
    class FunctionLike:
        __module__ = "custom.module"
        __name__ = "function_like"

    func = cast(Any, FunctionLike())

    module, qualname = get_func_module_and_qualname(func)

    assert module == "custom.module"
    assert qualname == "function_like"


def test_b02_falls_back_to_unknown_module_when_module_is_missing() -> None:
    class FunctionLike:
        def __init__(self) -> None:
            self.__qualname__ = "FunctionLike"

        def __getattribute__(self, name: str) -> Any:
            if name == "__module__":
                raise AttributeError(name)

            return super().__getattribute__(name)

    func = cast(Any, FunctionLike())

    module, qualname = get_func_module_and_qualname(func)

    assert module == "<unknown>"
    assert qualname == "FunctionLike"


def test_b03_falls_back_to_unknown_qualname_when_qualname_and_name_are_missing() -> None:
    class FunctionLike:
        __module__ = "custom.module"

    func = cast(Any, FunctionLike())

    module, qualname = get_func_module_and_qualname(func)

    assert module == "custom.module"
    assert qualname == "<unknown>"


def test_b04_falls_back_to_unknown_values_when_introspection_attributes_are_missing() -> None:
    func = cast(Any, object())

    module, qualname = get_func_module_and_qualname(func)

    assert module == "<unknown>"
    assert qualname == "<unknown>"
