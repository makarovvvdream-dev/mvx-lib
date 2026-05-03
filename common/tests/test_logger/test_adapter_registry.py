from __future__ import annotations

from typing import Any, Dict

import pytest

from mvx.logger import adapter_registry as ar


@pytest.fixture(autouse=True)
def _reset_adapter_registry() -> None:
    """
    Reset adapter registry and active profile before each test.

    This ensures tests do not interfere with each other via global state.
    """
    ar._ADAPTERS.clear()  # type: ignore[attr-defined]
    ar.set_active_log_profile("default")


def test_register_and_resolve_exact_type_default_profile() -> None:
    """
    Registering an adapter for a specific type must allow resolving it
    for instances of that exact type under the default profile.
    """

    class A:
        def __init__(self, x: int) -> None:
            self.x = x

    def adapter_a(obj: Any) -> Dict[str, Any]:
        assert isinstance(obj, A)
        return {"type": "A", "x": obj.x}

    ar.register_log_adapter(A, adapter_a)

    a = A(10)
    resolved = ar.resolve_log_adapter(a)

    assert resolved is adapter_a
    payload = resolved(a)
    assert payload == {"type": "A", "x": 10}


def test_resolve_returns_none_for_none_or_unregistered_type() -> None:
    """
    Resolve_log_adapter must return None for None or when no adapter is registered.
    """

    class B:
        pass

    assert ar.resolve_log_adapter(None) is None
    assert ar.resolve_log_adapter(B()) is None


def test_resolve_uses_active_profile_when_profile_none() -> None:
    """
    When profile is None, resolve_log_adapter must use the active profile.
    """

    class C:
        def __init__(self, v: str) -> None:
            self.v = v

    def adapter_default(obj: Any) -> Dict[str, Any]:
        return {"profile": "default", "v": obj.v}

    def adapter_debug(obj: Any) -> Dict[str, Any]:
        return {"profile": "debug", "v": obj.v}

    # Register two adapters under different profiles
    ar.register_log_adapter(C, adapter_default, profile="default")
    ar.register_log_adapter(C, adapter_debug, profile="debug")

    # Active profile is "default" by fixture
    c = C("x")
    resolved_default = ar.resolve_log_adapter(c)
    assert resolved_default is adapter_default
    assert resolved_default(c)["profile"] == "default"

    # Switch active profile to "debug"
    ar.set_active_log_profile("debug")
    resolved_debug = ar.resolve_log_adapter(c)
    assert resolved_debug is adapter_debug
    assert resolved_debug(c)["profile"] == "debug"

    # Explicit profile argument must override active profile
    resolved_explicit_default = ar.resolve_log_adapter(c, profile="default")
    assert resolved_explicit_default is adapter_default


def test_set_active_log_profile_falsy_falls_back_to_default() -> None:
    """
    set_active_log_profile with a falsy value must reset the profile to 'default'.
    """
    ar.set_active_log_profile("debug")
    assert ar.get_active_log_profile() == "debug"

    ar.set_active_log_profile("")  # falsy -> fallback
    assert ar.get_active_log_profile() == "default"


def test_mro_base_adapter_used_for_subclasses() -> None:
    """
    An adapter registered for a base class must be used for all subclasses
    when no more specific adapter is present.
    """

    class Base:
        def __init__(self, v: int) -> None:
            self.v = v

    class Sub(Base):
        pass

    def base_adapter(obj: Any) -> Dict[str, Any]:
        assert isinstance(obj, Base)
        return {"kind": "base", "v": obj.v}

    ar.register_log_adapter(Base, base_adapter, profile="default")

    instance = Sub(42)
    resolved = ar.resolve_log_adapter(instance)

    assert resolved is base_adapter
    assert resolved(instance) == {"kind": "base", "v": 42}


def test_mro_more_specific_adapter_overrides_base() -> None:
    """
    If adapters are registered for both a base class and a subclass,
    the subclass adapter must win for instances of the subclass.
    """

    class Base:
        pass

    class Sub(Base):
        pass

    def base_adapter(obj: Any) -> Dict[str, Any]:
        return {"kind": "base"}

    def sub_adapter(obj: Any) -> Dict[str, Any]:
        return {"kind": "sub"}

    ar.register_log_adapter(Base, base_adapter, profile="default")
    ar.register_log_adapter(Sub, sub_adapter, profile="default")

    base_obj = Base()
    sub_obj = Sub()

    resolved_base = ar.resolve_log_adapter(base_obj)
    resolved_sub = ar.resolve_log_adapter(sub_obj)

    assert resolved_base is base_adapter
    assert resolved_sub is sub_adapter
    assert resolved_base(base_obj) == {"kind": "base"}
    assert resolved_sub(sub_obj) == {"kind": "sub"}


def test_adapters_are_profile_isolated() -> None:
    """
    Adapters for different profiles must not interfere with each other.
    """

    class D:
        pass

    def adapter_default(obj: Any) -> Dict[str, Any]:
        return {"p": "default"}

    def adapter_audit(obj: Any) -> Dict[str, Any]:
        return {"p": "audit"}

    ar.register_log_adapter(D, adapter_default, profile="default")
    ar.register_log_adapter(D, adapter_audit, profile="audit")

    d = D()

    ar.set_active_log_profile("default")
    res_default = ar.resolve_log_adapter(d)
    assert res_default is adapter_default
    assert res_default(d)["p"] == "default"

    ar.set_active_log_profile("audit")
    res_audit = ar.resolve_log_adapter(d)
    assert res_audit is adapter_audit
    assert res_audit(d)["p"] == "audit"

    # Explicit profile must still work regardless of active profile
    res_explicit_default = ar.resolve_log_adapter(d, profile="default")
    assert res_explicit_default is adapter_default
