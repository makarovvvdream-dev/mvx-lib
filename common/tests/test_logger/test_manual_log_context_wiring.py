# tests/test_logger/test_manual_log_context_wiring.py
from __future__ import annotations

from pathlib import Path

from mvx.common.logger import (
    FileLogSink,
    LogContext,
    LogLevel,
    LogVerbosityLevel,
    LoggingFileConfig,
)

# ==== A. Manual LogContext wiring =========================================================


def test_a01_manual_root_context_can_log_to_directly_created_file_sink(tmp_path: Path) -> None:
    log_file = tmp_path / "manual-root.log"

    sink, terminator = FileLogSink.create(
        config=LoggingFileConfig(
            file_path=log_file,
            level=LogLevel.DEBUG,
            log_format="%(levelname)s: %(message)s %(payload)s",
        ),
    )

    try:
        root = LogContext(
            namespace="manual",
            log_sink=sink,
            verbosity_level=LogVerbosityLevel.NORMAL,
        )

        root.log_event(
            event="manual.root.started",
            level=LogLevel.INFO,
            payload={
                "marker": "a01",
            },
        )

    finally:
        terminator()

    content = log_file.read_text()

    assert "INFO: manual.manual.root.started" in content
    assert "'marker': 'a01'" in content


def test_a02_manual_child_context_inherits_parent_file_sink(tmp_path: Path) -> None:
    log_file = tmp_path / "manual-child.log"

    sink, terminator = FileLogSink.create(
        config=LoggingFileConfig(
            file_path=log_file,
            level=LogLevel.DEBUG,
            log_format="%(levelname)s: %(message)s %(payload)s",
        ),
    )

    try:
        root = LogContext(
            namespace="manual",
            log_sink=sink,
            verbosity_level=LogVerbosityLevel.NORMAL,
        )
        child = LogContext(
            namespace="manual.child",
            parent=root,
        )

        assert child.get_local_log_sink() is None
        assert child.log_sink is sink

        child.log_event(
            event="child.started",
            level=LogLevel.INFO,
            payload={
                "marker": "a02",
            },
        )

    finally:
        terminator()

    content = log_file.read_text()

    assert "INFO: manual.child.child.started" in content
    assert "'marker': 'a02'" in content


def test_a03_manual_leaf_context_can_override_parent_sink(tmp_path: Path) -> None:
    root_log_file = tmp_path / "manual-root.log"
    leaf_log_file = tmp_path / "manual-leaf.log"

    root_sink, root_terminator = FileLogSink.create(
        config=LoggingFileConfig(
            file_path=root_log_file,
            level=LogLevel.DEBUG,
            log_format="%(levelname)s: %(message)s %(payload)s",
        ),
    )
    leaf_sink, leaf_terminator = FileLogSink.create(
        config=LoggingFileConfig(
            file_path=leaf_log_file,
            level=LogLevel.DEBUG,
            log_format="%(levelname)s: %(message)s %(payload)s",
        ),
    )

    try:
        root = LogContext(
            namespace="manual",
            log_sink=root_sink,
            verbosity_level=LogVerbosityLevel.NORMAL,
        )
        parent = LogContext(
            namespace="manual.parent",
            parent=root,
        )
        leaf = LogContext(
            namespace="manual.parent.leaf",
            parent=parent,
            log_sink=leaf_sink,
        )

        assert parent.log_sink is root_sink
        assert leaf.get_local_log_sink() is leaf_sink
        assert leaf.log_sink is leaf_sink

        parent.log_event(
            event="parent.event",
            level=LogLevel.INFO,
            payload={
                "marker": "root-file-only",
            },
        )
        leaf.log_event(
            event="leaf.event",
            level=LogLevel.INFO,
            payload={
                "marker": "leaf-file-only",
            },
        )

    finally:
        leaf_terminator()
        root_terminator()

    root_content = root_log_file.read_text()
    leaf_content = leaf_log_file.read_text()

    assert "manual.parent.parent.event" in root_content
    assert "root-file-only" in root_content
    assert "leaf.event" not in root_content
    assert "leaf-file-only" not in root_content

    assert "manual.parent.leaf.leaf.event" in leaf_content
    assert "leaf-file-only" in leaf_content
    assert "parent.event" not in leaf_content
    assert "root-file-only" not in leaf_content


def test_a04_manual_context_can_be_used_by_log_invocation_without_package_registry(
    tmp_path: Path,
) -> None:
    log_file = tmp_path / "manual-invocation.log"

    sink, terminator = FileLogSink.create(
        config=LoggingFileConfig(
            file_path=log_file,
            level=LogLevel.DEBUG,
            log_format="%(levelname)s: %(message)s %(payload)s",
        ),
    )

    try:
        ctx = LogContext(
            namespace="manual.invocation",
            log_sink=sink,
            verbosity_level=LogVerbosityLevel.NORMAL,
        )

        from mvx.common.logger import log_invocation

        @log_invocation(
            "operation.run",
            ctx=ctx,
            invoke_level=LogLevel.INFO,
            success_level=LogLevel.INFO,
            log_kwargs_on_invoke=("value",),
            log_result_on_success=(),
        )
        def operation(value: int) -> str:
            return f"done-{value}"

        assert operation(42) == "done-42"

    finally:
        terminator()

    content = log_file.read_text()

    assert "INFO: manual.invocation.operation.run [invoke]" in content
    assert "INFO: manual.invocation.operation.run [success]" in content
    assert "'kwargs': {'value': 42}" in content
    assert "'result': 'done-42'" in content
