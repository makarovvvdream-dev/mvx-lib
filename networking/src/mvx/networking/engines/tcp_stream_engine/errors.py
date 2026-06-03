# src/mvx/networking/engines/tcp_stream_engine/errors.py
from __future__ import annotations

from mvx.common.errors import RuntimeExtendedError, RuntimeUnexpectedError

from ...models import TcpIoOperation, EngineState


class TcpStreamEngineBaseError(RuntimeExtendedError):
    pass


class TcpStreamEngineNotOpenError(TcpStreamEngineBaseError):
    """
    I/O operation attempted while the transport is not in an open/usable state.

    Typical cases
    -------------
    - state is not OPENED (VIRGIN/OPENING/CLOSING/CLOSED/ERROR)
    - reader or writer is missing

    Details
    -------
    - is_reader / is_writer: presence flags for internal stream objects
    """

    def __init__(
        self,
        *,
        io_op_type: TcpIoOperation,
        engine_state: EngineState,
        is_reader: bool,
        is_writer: bool,
    ) -> None:

        details: dict[str, object] = {
            "io_operation_type": io_op_type,
            "engine_state_at_error": engine_state.value,
            "is_reader": is_reader,
            "is_writer": is_writer,
        }
        super().__init__(
            message=f"runtime error: stream tcp engine is not open; op {io_op_type}",
            details=details,
        )


class TcpStreamEngineUnexpectedlyClosingError(TcpStreamEngineBaseError):
    """
    Transport stream exists but is already closing, so the I/O operation cannot proceed.

    Typical case
    ------------
    - writer.is_closing() is True
    """

    def __init__(
        self,
        *,
        io_op_type: TcpIoOperation,
        engine_state: EngineState,
    ) -> None:
        details: dict[str, object] = {
            "io_operation_type": io_op_type,
            "engine_state_at_error": engine_state.value,
        }

        super().__init__(
            message=f"runtime error: stream tcp engine is unexpectedly closing; op {io_op_type}",
            details=details,
        )


class TcpStreamEngineUnexpectedError(TcpStreamEngineBaseError, RuntimeUnexpectedError):
    pass
