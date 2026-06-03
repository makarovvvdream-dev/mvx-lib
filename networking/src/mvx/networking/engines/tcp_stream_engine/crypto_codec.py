# src/mvx/networking/engines/tcp_stream_engine/crypto_codec.py
"""
Generic container for stream-level crypto codec runtime.

This module defines :class:`BaseCryptoCodec`, a reusable transport-side codec
that knows nothing about any concrete security mechanism.

It operates purely as:

  * raw RX accumulator,
  * pull-based incoming loop,
  * outgoing writer adapter,
  * executor of injected runtime callables.
"""

from __future__ import annotations
from typing import Protocol, runtime_checkable, Callable, Awaitable

from ...net_errors import CryptoCodecReadError, CryptoCodecWriteError

__all__ = ("CryptoCodec",)


@runtime_checkable
class CryptoRuntimePrimitivesProto(Protocol):
    """
    Protocol for cryptographic runtime primitive functions.
    """

    @property
    def extractor(self) -> Callable[[bytes], tuple[bytes, int] | None]: ...

    @property
    def framer(self) -> Callable[[bytes], bytes]: ...
    @property
    def unwrapper(self) -> Callable[[bytes], bytes]: ...
    @property
    def wrapper(self) -> Callable[[bytes], bytes]: ...


class CryptoCodec:
    """
    Generic stateful stream crypto codec.

    This class does not implement any mechanism-specific logic on its own.
    Instead, it receives a fully prepared runtime contract describing:

      * how to extract one encoded incoming frame from raw bytes,
      * how to unwrap one incoming frame into plaintext,
      * how to wrap plaintext into one encoded outgoing payload,
      * how to frame that encoded outgoing payload for transport emission.
    """

    def __init__(self, primitives: CryptoRuntimePrimitivesProto) -> None:

        self._extract_incoming_frame = primitives.extractor
        self._frame_outgoing_payload = primitives.framer
        self._unwrap_incoming_frame = primitives.unwrapper
        self._wrap_outgoing_payload = primitives.wrapper

        self._raw_rx_buffer = bytearray()

    async def read(self, reader: Callable[[], Awaitable[bytes]]) -> bytes:
        """
        Read and decode plaintext bytes from the underlying raw stream.
        """
        try:
            while True:
                plain_parts: list[bytes] = []

                while True:
                    chunk_info = self._extract_incoming_frame(bytes(self._raw_rx_buffer))
                    if chunk_info is None:
                        break

                    frame, consumed_len = chunk_info
                    del self._raw_rx_buffer[:consumed_len]

                    plain_parts.append(self._unwrap_incoming_frame(frame))

                if plain_parts:
                    return b"".join(plain_parts)

                raw = await reader()
                self._raw_rx_buffer.extend(raw)

        except Exception as exc:
            raise CryptoCodecReadError(cause=exc) from exc

    def write(self, writer: Callable[[bytes], None], data: bytes) -> None:
        """
        Encode plaintext bytes and write them to the underlying raw stream.
        """
        if not data:
            return
        try:
            encoded_payload = self._wrap_outgoing_payload(data)
            framed_payload = self._frame_outgoing_payload(encoded_payload)
            writer(framed_payload)
        except Exception as exc:
            raise CryptoCodecWriteError(cause=exc) from exc
