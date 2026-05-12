# tests/test_logger/test_package_file_sink_smoke.py
from __future__ import annotations

from pathlib import Path
from collections.abc import Iterator

import pytest

import mvx.common.logger as logger_pack

from mvx.common.logger import LogLevel


@pytest.fixture(autouse=True)
def reset_logger_environment() -> Iterator[None]:
    logger_pack.reset_logger()

    yield

    logger_pack.reset_logger()


# ==== A. FileLogSink public smoke =========================================================


def test_a01_root_context_can_be_reconfigured_to_file_sink(tmp_path: Path) -> None:
    log_file = tmp_path / "mvx-root.log"

    file_sink = logger_pack.configure_log_sink(
        name="file",
        sink_cls=logger_pack.FileLogSink,
        config=logger_pack.LoggingFileConfig(
            file_path=log_file,
        ),
    )

    root = logger_pack.get_root_log_context()
    root.set_log_sink(file_sink)

    assert root.get_local_log_sink() is file_sink
    assert root.log_sink is file_sink

    root.log_event(
        event="root.file_sink.started",
        level=LogLevel.INFO,
        payload={
            "source": "root",
            "marker": "a01",
        },
    )

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "root.file_sink.started" in content
    assert "source" in content
    assert "root" in content
    assert "marker" in content
    assert "a01" in content


def test_a02_child_contexts_inherit_root_file_sink(tmp_path: Path) -> None:
    log_file = tmp_path / "mvx-chain.log"

    file_sink = logger_pack.configure_log_sink(
        name="file",
        sink_cls=logger_pack.FileLogSink,
        config=logger_pack.LoggingFileConfig(
            file_path=log_file,
        ),
    )

    root = logger_pack.get_root_log_context()
    root.set_log_sink(file_sink)

    parent = logger_pack.configure_log_context("mvx.test")
    child = logger_pack.configure_log_context("mvx.test.child")

    assert parent.get_local_log_sink() is None
    assert child.get_local_log_sink() is None

    assert parent.log_sink is file_sink
    assert child.log_sink is file_sink

    parent.log_event(
        event="parent.file_sink.started",
        level=LogLevel.INFO,
        payload={
            "source": "parent",
            "marker": "a02-parent",
        },
    )
    child.log_event(
        event="child.file_sink.started",
        level=LogLevel.INFO,
        payload={
            "source": "child",
            "marker": "a02-child",
        },
    )

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "parent.file_sink.started" in content
    assert "child.file_sink.started" in content

    assert "mvx.test" in content
    assert "mvx.test.child" in content

    assert "a02-parent" in content
    assert "a02-child" in content


def test_a03_leaf_context_can_override_root_file_sink(tmp_path: Path) -> None:
    root_log_file = tmp_path / "mvx-root.log"
    leaf_log_file = tmp_path / "mvx-leaf.log"

    root_file_sink = logger_pack.configure_log_sink(
        name="root_file",
        sink_cls=logger_pack.FileLogSink,
        config=logger_pack.LoggingFileConfig(
            file_path=root_log_file,
        ),
    )
    leaf_file_sink = logger_pack.configure_log_sink(
        name="leaf_file",
        sink_cls=logger_pack.FileLogSink,
        config=logger_pack.LoggingFileConfig(
            file_path=leaf_log_file,
        ),
    )

    root = logger_pack.get_root_log_context()
    root.set_log_sink(root_file_sink)

    parent = logger_pack.configure_log_context("mvx.test")
    leaf = logger_pack.configure_log_context(
        "mvx.test.leaf",
        log_sink=leaf_file_sink,
    )

    assert parent.log_sink is root_file_sink
    assert leaf.get_local_log_sink() is leaf_file_sink
    assert leaf.log_sink is leaf_file_sink

    parent.log_event(
        event="parent.to.root.file",
        level=LogLevel.INFO,
        payload={
            "marker": "root-file-only",
        },
    )
    leaf.log_event(
        event="leaf.to.leaf.file",
        level=LogLevel.INFO,
        payload={
            "marker": "leaf-file-only",
        },
    )

    logger_pack.reset_logger()

    root_content = root_log_file.read_text()
    leaf_content = leaf_log_file.read_text()

    assert "parent.to.root.file" in root_content
    assert "root-file-only" in root_content
    assert "leaf.to.leaf.file" not in root_content
    assert "leaf-file-only" not in root_content

    assert "leaf.to.leaf.file" in leaf_content
    assert "leaf-file-only" in leaf_content
    assert "parent.to.root.file" not in leaf_content
    assert "root-file-only" not in leaf_content


def test_a04_reset_logger_closes_file_sink_and_restores_default_environment(tmp_path: Path) -> None:
    log_file = tmp_path / "mvx-reset.log"

    file_sink = logger_pack.configure_log_sink(
        name="file",
        sink_cls=logger_pack.FileLogSink,
        config=logger_pack.LoggingFileConfig(
            file_path=log_file,
        ),
    )

    root = logger_pack.get_root_log_context()
    root.set_log_sink(file_sink)

    ctx = logger_pack.configure_log_context("mvx.test")

    ctx.log_event(
        event="before.reset",
        level=LogLevel.INFO,
        payload={
            "marker": "a04",
        },
    )

    assert logger_pack.get_log_sink("file") is file_sink
    assert logger_pack.has_log_context("mvx.test")

    logger_pack.reset_logger()

    assert logger_pack.get_log_sink("file") is None
    assert not logger_pack.has_log_context("mvx.test")
    assert logger_pack.get_configured_log_sink_names() == (logger_pack.DEFAULT_ROOT_LOG_SINK_NAME,)

    new_root = logger_pack.get_root_log_context()
    default_sink = logger_pack.get_log_sink(logger_pack.DEFAULT_ROOT_LOG_SINK_NAME)

    assert default_sink is not None
    assert new_root.get_local_log_sink() is default_sink

    content = log_file.read_text()

    assert "before.reset" in content
    assert "a04" in content
