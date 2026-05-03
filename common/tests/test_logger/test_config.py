from __future__ import annotations

import pytest

from mvx.logger import LoggerConfig, NAMESPACE


def test_namespace_constant() -> None:
    """
    NAMESPACE should be 'logger'.
    """
    assert NAMESPACE == "logger"


def test_logger_config_defaults() -> None:
    """
    Default LoggerConfig values must be as declared in the model.
    """
    cfg = LoggerConfig()

    assert cfg.level == "INFO"
    assert cfg.sink == "stderr"
    assert cfg.file_path is None
    assert cfg.formatter == "UVICORN"
    assert cfg.format == "%(levelprefix)s %(name)s: %(message)s"
    assert cfg.namespace is None
    assert cfg.profile == "default"


def test_logger_config_accepts_valid_levels() -> None:
    """
    LoggerConfig.level must accept only declared Literal values.
    """
    for lvl in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        cfg = LoggerConfig(level=lvl)
        assert cfg.level == lvl

    # Invalid level should fail validation
    with pytest.raises(ValueError):
        LoggerConfig(level="TRACE")  # type: ignore[arg-type]


def test_logger_config_sink_stdout_no_file_required() -> None:
    """
    When sink is stdout, file_path is not required and may be None.
    """
    cfg = LoggerConfig(sink="stdout")
    assert cfg.sink == "stdout"
    assert cfg.file_path is None


def test_logger_config_sink_stderr_no_file_required() -> None:
    """
    When sink is stderr, file_path is not required and may be None.
    """
    cfg = LoggerConfig(sink="stderr")
    assert cfg.sink == "stderr"
    assert cfg.file_path is None


def test_logger_config_sink_file_requires_file_path() -> None:
    """
    When sink is 'file', file_path must be provided, otherwise validation fails.
    """
    with pytest.raises(ValueError) as exc:
        LoggerConfig(sink="file")
    assert "file_path is required when sink='file'" in str(exc.value)

    cfg = LoggerConfig(sink="file", file_path="/tmp/app.log")
    assert cfg.sink == "file"
    assert cfg.file_path == "/tmp/app.log"


def test_logger_config_formatter_values() -> None:
    """
    LoggerConfig.formatter must accept only 'UVICORN' or 'CUSTOM'.
    """
    cfg = LoggerConfig(formatter="UVICORN")
    assert cfg.formatter == "UVICORN"

    cfg = LoggerConfig(formatter="CUSTOM")
    assert cfg.formatter == "CUSTOM"

    with pytest.raises(ValueError):
        LoggerConfig(formatter="SOMETHING_ELSE")  # type: ignore[arg-type]


def test_logger_config_extra_fields_are_ignored() -> None:
    """
    Extra fields in input data must be ignored due to model_config(extra="ignore").
    """
    cfg = LoggerConfig(level="DEBUG", sink="stdout", file_path="/tmp/ignored.log", unknown="x")  # type: ignore[arg-type]
    # 'unknown' must not become an attribute
    assert not hasattr(cfg, "unknown")
    # Existing fields must be set normally
    assert cfg.level == "DEBUG"
    assert cfg.sink == "stdout"
    # file_path is allowed even if sink is stdout (validator only enforces presence when sink='file')
    assert cfg.file_path == "/tmp/ignored.log"


def test_logger_config_namespace_field_optional() -> None:
    """
    Namespace field is optional and can be set or omitted.
    """
    cfg_default = LoggerConfig()
    assert cfg_default.namespace is None

    cfg_with_ns = LoggerConfig(namespace="mvx")
    assert cfg_with_ns.namespace == "mvx"


def test_logger_config_profile_can_be_custom_string() -> None:
    """
    LoggerConfig.profile must default to 'default' and accept arbitrary strings.
    """
    cfg_default = LoggerConfig()
    assert cfg_default.profile == "default"

    cfg_debug = LoggerConfig(profile="debug")
    assert cfg_debug.profile == "debug"

    cfg_audit = LoggerConfig(profile="audit")
    assert cfg_audit.profile == "audit"
