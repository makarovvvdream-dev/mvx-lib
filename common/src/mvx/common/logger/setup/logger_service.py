# # common/src/mvx/common/logger/logger_service.py
# from __future__ import annotations
# from enum import IntEnum
#
#
# import logging
# import sys
# from pathlib import Path
# from threading import RLock
#
# from .logger_params import LoggerParamsProto, LogLevel, LogSink, LogFormatter
# from .errors import InvalidLogFormatError, UnableToCreateLogFileError
#
# from .mvx_logger import MvxLogger
#
# from .trace_context import get_trace_id, NO_TRACE
# from .adapter_registry import set_active_log_profile
#
#
# class _Levels(IntEnum):
#     DEBUG = logging.DEBUG
#     INFO = logging.INFO
#     WARNING = logging.WARNING
#     ERROR = logging.ERROR
#     CRITICAL = logging.CRITICAL
#
#
# class TraceIdFilter(logging.Filter):
#     """
#     Ensure that every LogRecord has at least:
#       - trace_id: for correlation
#       - evt:      event name (fallback to message)
#       - data:     structured payload (fallback to empty dict)
#     """
#
#     def filter(self, record: logging.LogRecord) -> bool:
#         """
#         Inject trace_id, evt and data into the record if they are missing.
#         """
#         if not hasattr(record, "trace_id"):
#             # noinspection PyBroadException
#             try:
#                 record.trace_id = get_trace_id()
#             except Exception:
#                 record.trace_id = NO_TRACE
#
#         if not hasattr(record, "evt"):
#             record.evt = record.getMessage()
#
#         if not hasattr(record, "data"):
#             record.data = {}
#
#         return True
#
#
# class LoggerService:
#     """
#     Runtime logging service that owns LoggerConfig and applies it to the root logger.
#
#     Responsibilities:
#     - Hold current LoggerConfig in memory.
#     - Apply logging configuration (idempotent; supports reconfigure).
#     - Provide accessors to the current logging config.
#     - Allow replacing config at runtime (caller decides when and зачем).
#
#     Persistence (TOML/ENV/ConfigFacade) is handled outside of this service.
#     """
#
#     def __init__(self, cfg: LoggerParams, *, apply_cfg: bool = True) -> None:
#         self._cfg = cfg
#         self._lock = RLock()
#         self._cfg_applied_once = False
#
#         if apply_cfg:
#             self.apply()
#
#     # ---------------- public API ----------------
#
#     def apply(self, *, reconfigure: bool = True) -> None:
#         """
#         Apply the current LoggerConfig to the root logger.
#
#         If reconfigure=True, replace handlers even if already applied once.
#         """
#         with self._lock:
#             self._apply(self._cfg, reconfigure=reconfigure)
#
#     def get_config(self) -> LoggerParams:
#         """
#         Return the current logging config.
#         """
#         return self._cfg
#
#     def update_config(self, cfg: LoggerParams, *, apply_cfg: bool = True) -> None:
#         """
#         Replace the current logging config in memory and optionally reconfigure logging.
#         """
#         with self._lock:
#             self._cfg = cfg
#             if apply_cfg:
#                 self._apply(self._cfg, reconfigure=True)
#
#     # ---------------- internals ----------------
#
#     def _apply(self, cfg: LoggerParams, *, reconfigure: bool) -> None:
#         """
#         Apply configuration to the root logger.
#
#         - First apply sets the logger class to MvxLogger and adds a single handler.
#         - Subsequent applies replace handlers only if reconfigure=True.
#         - The active log profile for type-based adapters is updated from cfg.profile.
#         """
#         # Ensure all subsequently created loggers use MvxLogger.
#         logging.setLoggerClass(MvxLogger)
#
#         # Configure root logger level.
#         root = logging.getLogger()
#         root.setLevel(_Levels(cfg.level))
#
#         # Configure handler according to sink.
#         if cfg.sink == "stdout":
#             handler: logging.Handler = logging.StreamHandler(sys.stdout)
#         elif cfg.sink == "stderr":
#             handler = logging.StreamHandler(sys.stderr)
#         else:  # file
#             path = cfg.file_path
#             # invariant has been checked by LoggerParams
#             assert path is not None
#
#             try:
#                 path.parent.mkdir(parents=True, exist_ok=True)
#             except Exception as e:
#                 raise UnableToCreateLogFileError(log_path=path, cause=e) from e
#
#             handler = logging.FileHandler(path, encoding="utf-8")
#
#         # Formatter.
#         formatter_type = cfg.formatter
#         if formatter_type is not None:
#             try:
#                 formatter = formatter_type()
#
#
#
#             handler.setFormatter(logging.Formatter(cfg.format))
#         else:
#             handler.setFormatter(logging.Formatter(cfg.format))
#
#         # Filters: trace id + structured payload defaults.
#         handler.addFilter(TraceIdFilter())
#
#         # Apply handler configuration.
#         if reconfigure or not self._cfg_applied_once:
#             root.handlers.clear()
#             root.addHandler(handler)
#             self._cfg_applied_once = True
#
#         # Update active profile for type-based log adapters.
#         set_active_log_profile(cfg.profile)
