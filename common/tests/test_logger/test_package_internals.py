# tests/test_logger/test_package_internals.py
from __future__ import annotations

from typing import Any
from collections.abc import Callable, Iterator

import pytest

import mvx.common.logger as logger_pack

from mvx.common.logger import (
    LogEvent,
    LogSinkDescriptor,
    LogSinkConfigurationConflictError,
    LogSinkDescriptorBuildError,
    LogSinkCreateError,
    LogSinkCloseError,
    LogSinkIsInUseError,
    LogVerbosityLevel,
)

# noinspection PyProtectedMember
from mvx.common.logger.log_context.log_context import DEFAULT_MAX_ITEMS, DEFAULT_MAX_STR_LEN


class RecordingSink:
    def __init__(self, marker: str = "default") -> None:
        self.marker = marker
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        self.events.append(event)


class RecordingSinkClass:
    created_sinks: list[RecordingSink] = []
    terminators: list[Callable[[], None]] = []
    terminated_markers: list[str] = []

    @classmethod
    def reset(cls) -> None:
        cls.created_sinks.clear()
        cls.terminators.clear()
        cls.terminated_markers.clear()

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        marker = kwargs.get("marker", "default")

        return LogSinkDescriptor(
            sink_type="recording",
            resource_key=(marker,),
            config_key=(),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[RecordingSink, Callable[[], None]]:
        marker = kwargs.get("marker", "default")
        sink = RecordingSink(marker=marker)
        cls.created_sinks.append(sink)

        def terminator() -> None:
            cls.terminated_markers.append(marker)

        cls.terminators.append(terminator)

        return sink, terminator


class DescriptorFailureSinkClass:
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        raise RuntimeError("descriptor failed")

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[RecordingSink, Callable[[], None]]:
        raise AssertionError("create must not be called")


class CreateFailureSinkClass:
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        _ = kwargs
        return LogSinkDescriptor(
            sink_type="create-failure",
            resource_key=("create-failure",),
            config_key=(),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[RecordingSink, Callable[[], None]]:
        raise RuntimeError("create failed")


class TerminatorFailureSinkClass:
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        marker = kwargs.get("marker", "default")

        return LogSinkDescriptor(
            sink_type="terminator-failure",
            resource_key=(marker,),
            config_key=(),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[RecordingSink, Callable[[], None]]:
        marker = kwargs.get("marker", "default")
        sink = RecordingSink(marker=marker)

        def terminator() -> None:
            raise RuntimeError(f"terminator failed: {marker}")

        return sink, terminator


class BootstrapFailureSinkClass:
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        raise RuntimeError("bootstrap descriptor failed")

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[RecordingSink, Callable[[], None]]:
        raise AssertionError("create must not be called")


class BootstrapCreateFailureSinkClass:
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        return LogSinkDescriptor(
            sink_type="bootstrap-create-failure",
            resource_key=("bootstrap-create-failure",),
            config_key=(),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[RecordingSink, Callable[[], None]]:
        raise RuntimeError("bootstrap create failed")


class BootstrapFailingLogContext:
    def __init__(self, **kwargs: Any) -> None:
        raise RuntimeError("root context failed")


@pytest.fixture(autouse=True)
def reset_logger_environment() -> Iterator[None]:
    RecordingSinkClass.reset()
    logger_pack.reset_logger()

    yield

    logger_pack.reset_logger()
    RecordingSinkClass.reset()


# ==== A. Validators =======================================================================


def test_a01_validate_log_sink_name_accepts_valid_name() -> None:
    assert logger_pack._validate_log_sink_name("name", "file") == "file"
    assert logger_pack._validate_log_sink_name("name", "file_1") == "file_1"
    assert logger_pack._validate_log_sink_name("name", "file-1") == "file-1"


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "1file",
        ".file",
        "file.name",
        "file name",
        "file/name",
    ],
)
def test_a02_validate_log_sink_name_rejects_malformed_name(value: str) -> None:
    with pytest.raises(ValueError):
        logger_pack._validate_log_sink_name("name", value)


def test_a03_validate_log_sink_name_rejects_non_string() -> None:
    with pytest.raises(TypeError):
        logger_pack._validate_log_sink_name("name", 123)  # type: ignore[arg-type]


def test_a04_validate_namespace_accepts_valid_namespace() -> None:
    assert logger_pack._validate_namespace("namespace", "mvx") == "mvx"
    assert logger_pack._validate_namespace("namespace", "mvx.ldap") == "mvx.ldap"
    assert logger_pack._validate_namespace("namespace", "mvx.ldap_schema") == "mvx.ldap_schema"
    assert logger_pack._validate_namespace("namespace", "mvx.ldap-schema") == "mvx.ldap-schema"


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        ".mvx",
        "mvx.",
        "mvx..ldap",
        "1mvx",
        "mvx.1ldap",
        "mvx/ldap",
        "mvx ldap",
    ],
)
def test_a05_validate_namespace_rejects_malformed_namespace(value: str) -> None:
    with pytest.raises(ValueError):
        logger_pack._validate_namespace("namespace", value)


def test_a06_validate_namespace_rejects_non_string() -> None:
    with pytest.raises(TypeError):
        logger_pack._validate_namespace("namespace", object())  # type: ignore[arg-type]


# ==== B. _LogSinkRegistry ==================================================================


def test_b01_log_sink_registry_register_creates_sink() -> None:
    registry = logger_pack._LogSinkRegistry()

    sink = registry.register(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    assert isinstance(sink, RecordingSink)
    assert sink.marker == "a"
    assert registry.get("test") is sink
    assert registry.get_sinks_names() == ("test",)
    assert not registry.is_empty()


def test_b02_log_sink_registry_register_same_descriptor_returns_existing_sink() -> None:
    registry = logger_pack._LogSinkRegistry()

    first = registry.register(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )
    second = registry.register(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    assert second is first
    assert len(RecordingSinkClass.created_sinks) == 1


def test_b03_log_sink_registry_register_same_name_different_descriptor_raises() -> None:
    registry = logger_pack._LogSinkRegistry()

    registry.register(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    with pytest.raises(LogSinkConfigurationConflictError):
        registry.register(
            name="test",
            sink_cls=RecordingSinkClass,
            marker="b",
        )


def test_b04_log_sink_registry_register_descriptor_failure_raises_domain_error() -> None:
    registry = logger_pack._LogSinkRegistry()

    with pytest.raises(LogSinkDescriptorBuildError):
        registry.register(
            name="test",
            sink_cls=DescriptorFailureSinkClass,
        )

    assert registry.get("test") is None


def test_b05_log_sink_registry_register_create_failure_raises_domain_error() -> None:
    registry = logger_pack._LogSinkRegistry()

    with pytest.raises(LogSinkCreateError):
        registry.register(
            name="test",
            sink_cls=CreateFailureSinkClass,
        )

    assert registry.get("test") is None


def test_b06_log_sink_registry_unregister_unknown_returns_false() -> None:
    registry = logger_pack._LogSinkRegistry()

    assert registry.unregister("missing") is False


def test_b07_log_sink_registry_unregister_known_sink_calls_terminator_and_removes_sink() -> None:
    registry = logger_pack._LogSinkRegistry()

    registry.register(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    assert registry.unregister("test") is True
    assert registry.get("test") is None
    assert RecordingSinkClass.terminated_markers == ["a"]


def test_b08_log_sink_registry_unregister_removes_sink_even_if_terminator_fails() -> None:
    registry = logger_pack._LogSinkRegistry()

    registry.register(
        name="test",
        sink_cls=TerminatorFailureSinkClass,
        marker="a",
    )

    with pytest.raises(LogSinkCloseError):
        registry.unregister("test")

    assert registry.get("test") is None


def test_b09_log_sink_registry_reset_closes_all_sinks_in_reverse_order() -> None:
    registry = logger_pack._LogSinkRegistry()

    registry.register(
        name="first",
        sink_cls=RecordingSinkClass,
        marker="first",
    )
    registry.register(
        name="second",
        sink_cls=RecordingSinkClass,
        marker="second",
    )

    registry.reset()

    assert registry.is_empty()
    assert RecordingSinkClass.terminated_markers == ["second", "first"]


def test_b10_log_sink_registry_reset_aggregates_close_errors_and_clears_registry() -> None:
    registry = logger_pack._LogSinkRegistry()

    registry.register(
        name="first",
        sink_cls=TerminatorFailureSinkClass,
        marker="first",
    )
    registry.register(
        name="second",
        sink_cls=TerminatorFailureSinkClass,
        marker="second",
    )

    with pytest.raises(LogSinkCloseError) as exc_info:
        registry.reset()

    payload = exc_info.value.to_log_payload()

    assert registry.is_empty()
    assert payload["details"]["errors"][0]["sink_name"] == "second"
    assert payload["details"]["errors"][1]["sink_name"] == "first"


# ==== C. _LogContextRegistry ===============================================================


def test_c01_log_context_registry_initially_contains_root_context() -> None:
    sink = RecordingSink()
    root = logger_pack.LogContext(
        namespace=logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,
        log_sink=sink,
        verbosity_level=LogVerbosityLevel.NORMAL,
    )

    registry = logger_pack._LogContextRegistry(root)

    assert registry.get_root_log_context() is root
    assert registry.get(logger_pack.ROOT_LOG_CONTEXT_NAMESPACE) is root
    assert registry.contains(logger_pack.ROOT_LOG_CONTEXT_NAMESPACE)
    assert registry.list_namespaces() == (logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,)


def test_c02_log_context_registry_put_returns_existing_context_for_same_namespace() -> None:
    sink = RecordingSink()
    root = logger_pack.LogContext(
        namespace=logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,
        log_sink=sink,
        verbosity_level=LogVerbosityLevel.NORMAL,
    )
    registry = logger_pack._LogContextRegistry(root)

    first = logger_pack.LogContext(
        namespace="mvx",
        parent=root,
    )
    second = logger_pack.LogContext(
        namespace="mvx",
        parent=root,
    )

    assert registry.put(first) is first
    assert registry.put(second) is first
    assert registry.get("mvx") is first


def test_c03_log_context_registry_create_chain_creates_intermediate_contexts() -> None:
    sink = RecordingSink()
    root = logger_pack.LogContext(
        namespace=logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,
        log_sink=sink,
        verbosity_level=LogVerbosityLevel.NORMAL,
    )
    registry = logger_pack._LogContextRegistry(root)

    leaf = registry.create_log_context_chain("mvx.ldap.schema")

    assert registry.get("mvx") is not None
    assert registry.get("mvx.ldap") is not None
    assert registry.get("mvx.ldap.schema") is leaf
    assert leaf.namespace == "mvx.ldap.schema"


def test_c04_log_context_registry_create_chain_applies_settings_only_to_leaf() -> None:
    root_sink = RecordingSink(marker="root")
    leaf_sink = RecordingSink(marker="leaf")

    root = logger_pack.LogContext(
        namespace=logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,
        log_sink=root_sink,
        verbosity_level=LogVerbosityLevel.NORMAL,
    )
    registry = logger_pack._LogContextRegistry(root)

    leaf = registry.create_log_context_chain(
        "mvx.ldap.schema",
        log_sink=leaf_sink,
        verbosity_level=LogVerbosityLevel.MAXIMUM,
        max_str_len=10,
        max_items=2,
    )

    mvx = registry.get("mvx")
    ldap = registry.get("mvx.ldap")

    assert mvx is not None
    assert ldap is not None

    assert mvx.get_local_log_sink() is None
    assert ldap.get_local_log_sink() is None
    assert leaf.get_local_log_sink() is leaf_sink

    assert mvx.log_sink is root_sink
    assert ldap.log_sink is root_sink
    assert leaf.log_sink is leaf_sink

    assert mvx.verbosity_level == LogVerbosityLevel.NORMAL.value
    assert ldap.verbosity_level == LogVerbosityLevel.NORMAL.value
    assert leaf.verbosity_level == LogVerbosityLevel.MAXIMUM.value

    assert mvx.max_str_len == DEFAULT_MAX_STR_LEN
    assert ldap.max_str_len == DEFAULT_MAX_STR_LEN
    assert leaf.max_str_len == 10

    assert mvx.max_items == DEFAULT_MAX_ITEMS
    assert ldap.max_items == DEFAULT_MAX_ITEMS
    assert leaf.max_items == 2


def test_c05_log_context_registry_clear_removes_non_root_contexts_and_keeps_root() -> None:
    sink = RecordingSink()
    root = logger_pack.LogContext(
        namespace=logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,
        log_sink=sink,
        verbosity_level=LogVerbosityLevel.NORMAL,
    )
    registry = logger_pack._LogContextRegistry(root)

    registry.create_log_context_chain("mvx.ldap.schema")
    registry.clear()

    assert registry.get_root_log_context() is root
    assert registry.list_namespaces() == (logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,)
    assert registry.get("mvx") is None
    assert registry.get("mvx.ldap") is None
    assert registry.get("mvx.ldap.schema") is None


def test_c06_log_context_registry_get_contexts_by_log_sink_returns_only_local_users() -> None:
    root_sink = RecordingSink(marker="root")
    local_sink = RecordingSink(marker="local")

    root = logger_pack.LogContext(
        namespace=logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,
        log_sink=root_sink,
        verbosity_level=LogVerbosityLevel.NORMAL,
    )
    registry = logger_pack._LogContextRegistry(root)

    inherited = registry.create_log_context_chain("mvx.inherited")
    local = registry.create_log_context_chain(
        "mvx.local",
        log_sink=local_sink,
    )

    root_users = registry.get_contexts_by_log_sink(root_sink)
    local_users = registry.get_contexts_by_log_sink(local_sink)

    assert inherited.log_sink is root_sink
    assert inherited.get_local_log_sink() is None

    assert root_users == (root,)
    assert local_users == (local,)


# ==== D. Public sink/context wiring ========================================================


def test_d01_close_log_sink_raises_when_sink_is_used_by_root_context() -> None:
    with pytest.raises(LogSinkIsInUseError) as exc_info:
        logger_pack.close_log_sink(logger_pack.DEFAULT_ROOT_LOG_SINK_NAME)

    payload = exc_info.value.to_log_payload()

    assert payload["details"]["sink_name"] == logger_pack.DEFAULT_ROOT_LOG_SINK_NAME
    assert payload["details"]["context_namespaces"] == ("<root>",)


def test_d02_close_log_sink_raises_when_sink_is_used_by_local_context() -> None:
    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="local",
    )

    logger_pack.configure_log_context(
        "mvx.test",
        log_sink=sink,
    )

    with pytest.raises(LogSinkIsInUseError) as exc_info:
        logger_pack.close_log_sink("test")

    payload = exc_info.value.to_log_payload()

    assert payload["details"]["sink_name"] == "test"
    assert payload["details"]["context_namespaces"] == ("mvx.test",)


def test_d03_close_log_sink_ignores_inherited_usage() -> None:
    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="local",
    )

    parent = logger_pack.configure_log_context(
        "mvx.test",
        log_sink=sink,
    )
    child = logger_pack.configure_log_context("mvx.test.child")

    assert child.log_sink is sink
    assert child.get_local_log_sink() is None

    parent.reset_log_sink()

    assert logger_pack.close_log_sink("test") is True
    assert logger_pack.get_log_sink("test") is None
    assert RecordingSinkClass.terminated_markers == ["local"]


def test_d04_close_log_sink_unknown_name_returns_false() -> None:
    assert logger_pack.close_log_sink("missing") is False


# ==== E. _bootstrap ========================================================================
def test_e01_bootstrap_creates_registries() -> None:
    sink_registry, context_registry = logger_pack._bootstrap()

    assert isinstance(sink_registry, logger_pack._LogSinkRegistry)
    assert isinstance(context_registry, logger_pack._LogContextRegistry)


def test_e02_bootstrap_registers_default_stderr_sink() -> None:
    sink_registry, _ = logger_pack._bootstrap()

    sink = sink_registry.get(logger_pack.DEFAULT_ROOT_LOG_SINK_NAME)

    assert sink is not None
    assert sink_registry.get_sinks_names() == (logger_pack.DEFAULT_ROOT_LOG_SINK_NAME,)


def test_e03_bootstrap_creates_root_context_bound_to_default_sink() -> None:
    sink_registry, context_registry = logger_pack._bootstrap()

    sink = sink_registry.get(logger_pack.DEFAULT_ROOT_LOG_SINK_NAME)
    root = context_registry.get_root_log_context()

    assert sink is not None
    assert root.is_root
    assert root.namespace == logger_pack.ROOT_LOG_CONTEXT_NAMESPACE
    assert root.get_local_log_sink() is sink
    assert root.log_sink is sink
    assert root.verbosity_level == LogVerbosityLevel.NORMAL.value


def test_e04_bootstrap_logs_and_reraises_registry_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(logger_pack, "StreamLogSink", BootstrapFailureSinkClass)

    with pytest.raises(LogSinkDescriptorBuildError):
        logger_pack._bootstrap()

    captured = capsys.readouterr()

    assert "logger bootstrap failed" in captured.err
    assert "LOG_SINK_DESCRIPTOR_BUILD_FAILED" in captured.err
    assert "bootstrap descriptor failed" in captured.err


def test_e05_bootstrap_logs_and_reraises_root_context_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(logger_pack, "LogContext", BootstrapFailingLogContext)

    with pytest.raises(RuntimeError, match="root context failed"):
        logger_pack._bootstrap()

    captured = capsys.readouterr()

    assert "logger bootstrap failed" in captured.err
    assert "RuntimeError" in captured.err
    assert "root context failed" in captured.err
