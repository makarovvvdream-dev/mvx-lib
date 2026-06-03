# tests/engines/tcp_stream_engine/test_crypto_codec.py
from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

# noinspection PyProtectedMember
from mvx.networking.engines.tcp_stream_engine.crypto_codec import (
    CryptoCodec,
    CryptoRuntimePrimitivesProto,
)
from mvx.networking.net_errors import (
    CryptoCodecReadError,
    CryptoCodecWriteError,
)


class _Primitives:
    def __init__(
        self,
        *,
        extractor: Callable[[bytes], tuple[bytes, int] | None] | None = None,
        framer: Callable[[bytes], bytes] | None = None,
        unwrapper: Callable[[bytes], bytes] | None = None,
        wrapper: Callable[[bytes], bytes] | None = None,
    ) -> None:
        self._extractor = extractor or (lambda data: None)
        self._framer = framer or (lambda data: data)
        self._unwrapper = unwrapper or (lambda data: data)
        self._wrapper = wrapper or (lambda data: data)

    @property
    def extractor(self) -> Callable[[bytes], tuple[bytes, int] | None]:
        return self._extractor

    @property
    def framer(self) -> Callable[[bytes], bytes]:
        return self._framer

    @property
    def unwrapper(self) -> Callable[[bytes], bytes]:
        return self._unwrapper

    @property
    def wrapper(self) -> Callable[[bytes], bytes]:
        return self._wrapper


class _QueueReader:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)
        self.calls = 0

    async def __call__(self) -> bytes:
        self.calls += 1
        if not self._chunks:
            raise AssertionError("unexpected raw reader call")
        return self._chunks.pop(0)


def _bracket_extractor(data: bytes) -> tuple[bytes, int] | None:
    if not data.startswith(b"["):
        return None

    end = data.find(b"]")
    if end < 0:
        return None

    frame = data[1:end]
    consumed_len = end + 1
    return frame, consumed_len


# -------------------------
# Group a: construction and primitives
# -------------------------


def test_a1_crypto_codec_accepts_runtime_primitives_object() -> None:
    primitives = _Primitives()

    codec = CryptoCodec(primitives)

    assert codec is not None


def test_a2_constructor_stores_primitives_without_calling_runtime_functions() -> None:
    calls: list[str] = []

    def extractor(data: bytes) -> tuple[bytes, int] | None:
        calls.append(f"extractor:{data!r}")
        return None

    def framer(data: bytes) -> bytes:
        calls.append(f"framer:{data!r}")
        return data

    def unwrapper(data: bytes) -> bytes:
        calls.append(f"unwrapper:{data!r}")
        return data

    def wrapper(data: bytes) -> bytes:
        calls.append(f"wrapper:{data!r}")
        return data

    _ = CryptoCodec(
        _Primitives(
            extractor=extractor,
            framer=framer,
            unwrapper=unwrapper,
            wrapper=wrapper,
        )
    )

    assert calls == []


# -------------------------
# Group b: read() extraction happy paths
# -------------------------


@pytest.mark.asyncio
async def test_b1_read_reads_one_complete_frame_and_returns_plaintext() -> None:
    unwrap_calls: list[bytes] = []

    def unwrapper(frame: bytes) -> bytes:
        unwrap_calls.append(frame)
        return b"plain:" + frame

    codec = CryptoCodec(
        _Primitives(
            extractor=_bracket_extractor,
            unwrapper=unwrapper,
        )
    )
    reader = _QueueReader([b"[abc]"])

    result = await codec.read(reader)

    assert result == b"plain:abc"
    assert reader.calls == 1
    assert unwrap_calls == [b"abc"]


@pytest.mark.asyncio
async def test_b2_read_calls_extractor_before_first_raw_read() -> None:
    events: list[tuple[str, bytes]] = []

    def extractor(data: bytes) -> tuple[bytes, int] | None:
        events.append(("extractor", data))
        return _bracket_extractor(data)

    async def reader() -> bytes:
        events.append(("reader", b""))
        return b"[abc]"

    codec = CryptoCodec(
        _Primitives(
            extractor=extractor,
            unwrapper=lambda frame: frame,
        )
    )

    result = await codec.read(reader)

    assert result == b"abc"
    assert events[0] == ("extractor", b"")
    assert events[1] == ("reader", b"")


@pytest.mark.asyncio
async def test_b3_read_accumulates_multiple_raw_chunks_until_frame_is_complete() -> None:
    codec = CryptoCodec(
        _Primitives(
            extractor=_bracket_extractor,
            unwrapper=lambda frame: b"plain:" + frame,
        )
    )
    reader = _QueueReader([b"[ab", b"c]"])

    result = await codec.read(reader)

    assert result == b"plain:abc"
    assert reader.calls == 2


@pytest.mark.asyncio
async def test_b4_read_joins_multiple_plaintext_parts_from_available_frames() -> None:
    unwrap_calls: list[bytes] = []

    def unwrapper(frame: bytes) -> bytes:
        unwrap_calls.append(frame)
        return frame.upper()

    codec = CryptoCodec(
        _Primitives(
            extractor=_bracket_extractor,
            unwrapper=unwrapper,
        )
    )
    reader = _QueueReader([b"[one][two]"])

    result = await codec.read(reader)

    assert result == b"ONETWO"
    assert unwrap_calls == [b"one", b"two"]
    assert reader.calls == 1


# -------------------------
# Group c: read() buffer persistence
# -------------------------


@pytest.mark.asyncio
async def test_c1_read_preserves_partial_tail_across_reads() -> None:
    codec = CryptoCodec(
        _Primitives(
            extractor=_bracket_extractor,
            unwrapper=lambda frame: frame,
        )
    )
    reader = _QueueReader([b"[one][tw", b"o]"])

    first = await codec.read(reader)
    second = await codec.read(reader)

    assert first == b"one"
    assert second == b"two"
    assert reader.calls == 2


@pytest.mark.asyncio
async def test_c2_second_read_calls_reader_again_after_buffer_fully_consumed() -> None:
    codec = CryptoCodec(
        _Primitives(
            extractor=_bracket_extractor,
            unwrapper=lambda frame: frame,
        )
    )
    reader = _QueueReader([b"[one]", b"[two]"])

    first = await codec.read(reader)
    second = await codec.read(reader)

    assert first == b"one"
    assert second == b"two"
    assert reader.calls == 2


@pytest.mark.asyncio
async def test_c3_read_keeps_reading_while_extractor_returns_none() -> None:
    def extractor(data: bytes) -> tuple[bytes, int] | None:
        if not data.endswith(b"!"):
            return None
        return data[:-1], len(data)

    codec = CryptoCodec(
        _Primitives(
            extractor=extractor,
            unwrapper=lambda frame: frame.upper(),
        )
    )
    reader = _QueueReader([b"a", b"b", b"c!"])

    result = await codec.read(reader)

    assert result == b"ABC"
    assert reader.calls == 3


@pytest.mark.asyncio
async def test_c4_read_consumes_by_consumed_len_not_by_frame_len() -> None:
    def extractor(data: bytes) -> tuple[bytes, int] | None:
        if data.startswith(b"<abc>"):
            return b"abc", len(b"<abc>")

        if data.startswith(b"TAIL<ok>"):
            return b"ok", len(b"TAIL<ok>")

        return None

    codec = CryptoCodec(
        _Primitives(
            extractor=extractor,
            unwrapper=lambda frame: frame,
        )
    )
    reader = _QueueReader([b"<abc>TA", b"IL<ok>"])

    first = await codec.read(reader)
    second = await codec.read(reader)

    assert first == b"abc"
    assert second == b"ok"
    assert reader.calls == 2


@pytest.mark.asyncio
async def test_c5_read_allows_empty_plaintext_frame() -> None:
    codec = CryptoCodec(
        _Primitives(
            extractor=_bracket_extractor,
            unwrapper=lambda frame: b"",
        )
    )
    reader = _QueueReader([b"[empty]"])

    result = await codec.read(reader)

    assert result == b""
    assert reader.calls == 1


# -------------------------
# Group d: read() errors and cancellation
# -------------------------


@pytest.mark.asyncio
async def test_d1_read_wraps_reader_exception_into_crypto_codec_read_error() -> None:
    async def reader() -> bytes:
        raise RuntimeError("raw-read-failed")

    codec = CryptoCodec(_Primitives())

    with pytest.raises(CryptoCodecReadError) as ei:
        await codec.read(reader)

    assert isinstance(ei.value.__cause__, RuntimeError)
    assert str(ei.value.__cause__) == "raw-read-failed"


@pytest.mark.asyncio
async def test_d2_read_wraps_extractor_exception_into_crypto_codec_read_error() -> None:
    def extractor(data: bytes) -> tuple[bytes, int] | None:
        if data:
            raise ValueError("extract-failed")
        return None

    codec = CryptoCodec(_Primitives(extractor=extractor))
    reader = _QueueReader([b"raw"])

    with pytest.raises(CryptoCodecReadError) as ei:
        await codec.read(reader)

    assert isinstance(ei.value.__cause__, ValueError)
    assert str(ei.value.__cause__) == "extract-failed"


@pytest.mark.asyncio
async def test_d3_read_wraps_unwrapper_exception_into_crypto_codec_read_error() -> None:
    def unwrapper(frame: bytes) -> bytes:
        raise RuntimeError(f"unwrap-failed:{frame!r}")

    codec = CryptoCodec(
        _Primitives(
            extractor=_bracket_extractor,
            unwrapper=unwrapper,
        )
    )
    reader = _QueueReader([b"[abc]"])

    with pytest.raises(CryptoCodecReadError) as ei:
        await codec.read(reader)

    assert isinstance(ei.value.__cause__, RuntimeError)
    assert str(ei.value.__cause__) == "unwrap-failed:b'abc'"


@pytest.mark.asyncio
async def test_d4_read_wraps_bad_extractor_return_shape_into_crypto_codec_read_error() -> None:
    def extractor(data: bytes) -> Any:
        if data:
            return (b"abc",)
        return None

    codec = CryptoCodec(
        _Primitives(
            extractor=extractor,
            unwrapper=lambda frame: frame,
        )
    )
    reader = _QueueReader([b"raw"])

    with pytest.raises(CryptoCodecReadError) as ei:
        await codec.read(reader)

    assert isinstance(ei.value.__cause__, ValueError)


@pytest.mark.asyncio
async def test_d5_read_wraps_unwrapper_non_bytes_result_into_crypto_codec_read_error() -> None:
    def unwrapper(frame: bytes) -> Any:
        _ = frame
        return "not-bytes"

    codec = CryptoCodec(
        _Primitives(
            extractor=_bracket_extractor,
            unwrapper=unwrapper,
        )
    )
    reader = _QueueReader([b"[abc]"])

    with pytest.raises(CryptoCodecReadError) as ei:
        await codec.read(reader)

    assert isinstance(ei.value.__cause__, TypeError)


@pytest.mark.asyncio
async def test_d6_read_cancelled_error_propagates_unchanged() -> None:
    async def reader() -> bytes:
        raise asyncio.CancelledError

    codec = CryptoCodec(_Primitives())

    with pytest.raises(asyncio.CancelledError):
        await codec.read(reader)


# -------------------------
# Group e: write() routing
# -------------------------


def test_e1_write_empty_payload_is_noop() -> None:
    wrapper_calls: list[bytes] = []
    framer_calls: list[bytes] = []
    writer_calls: list[bytes] = []

    def wrapper(data: bytes) -> bytes:
        wrapper_calls.append(data)
        return data

    def framer(data: bytes) -> bytes:
        framer_calls.append(data)
        return data

    def writer(data: bytes) -> None:
        writer_calls.append(data)

    codec = CryptoCodec(
        _Primitives(
            wrapper=wrapper,
            framer=framer,
        )
    )

    codec.write(writer, b"")

    assert wrapper_calls == []
    assert framer_calls == []
    assert writer_calls == []


def test_e2_write_calls_wrapper_then_framer_then_writer() -> None:
    events: list[tuple[str, bytes]] = []

    def wrapper(data: bytes) -> bytes:
        events.append(("wrapper", data))
        return b"wrapped:" + data

    def framer(data: bytes) -> bytes:
        events.append(("framer", data))
        return b"framed:" + data

    def writer(data: bytes) -> None:
        events.append(("writer", data))

    codec = CryptoCodec(
        _Primitives(
            wrapper=wrapper,
            framer=framer,
        )
    )

    codec.write(writer, b"plain")

    assert events == [
        ("wrapper", b"plain"),
        ("framer", b"wrapped:plain"),
        ("writer", b"framed:wrapped:plain"),
    ]


def test_e3_write_allows_wrapper_to_return_empty_encoded_payload() -> None:
    writer_calls: list[bytes] = []

    def wrapper(data: bytes) -> bytes:
        _ = data
        return b""

    def framer(data: bytes) -> bytes:
        assert data == b""
        return b"frame-empty"

    def writer(data: bytes) -> None:
        writer_calls.append(data)

    codec = CryptoCodec(
        _Primitives(
            wrapper=wrapper,
            framer=framer,
        )
    )

    codec.write(writer, b"plain")

    assert writer_calls == [b"frame-empty"]


# -------------------------
# Group f: write() errors
# -------------------------


def test_f1_write_wraps_wrapper_exception_into_crypto_codec_write_error() -> None:
    framer_calls: list[bytes] = []
    writer_calls: list[bytes] = []

    def wrapper(data: bytes) -> bytes:
        _ = data
        raise RuntimeError("wrap-failed")

    def framer(data: bytes) -> bytes:
        framer_calls.append(data)
        return data

    def writer(data: bytes) -> None:
        writer_calls.append(data)

    codec = CryptoCodec(
        _Primitives(
            wrapper=wrapper,
            framer=framer,
        )
    )

    with pytest.raises(CryptoCodecWriteError) as ei:
        codec.write(writer, b"plain")

    assert isinstance(ei.value.__cause__, RuntimeError)
    assert str(ei.value.__cause__) == "wrap-failed"
    assert framer_calls == []
    assert writer_calls == []


def test_f2_write_wraps_framer_exception_into_crypto_codec_write_error() -> None:
    wrapper_calls: list[bytes] = []
    writer_calls: list[bytes] = []

    def wrapper(data: bytes) -> bytes:
        wrapper_calls.append(data)
        return b"wrapped"

    def framer(data: bytes) -> bytes:
        _ = data
        raise ValueError("frame-failed")

    def writer(data: bytes) -> None:
        writer_calls.append(data)

    codec = CryptoCodec(
        _Primitives(
            wrapper=wrapper,
            framer=framer,
        )
    )

    with pytest.raises(CryptoCodecWriteError) as ei:
        codec.write(writer, b"plain")

    assert isinstance(ei.value.__cause__, ValueError)
    assert str(ei.value.__cause__) == "frame-failed"
    assert wrapper_calls == [b"plain"]
    assert writer_calls == []


def test_f3_write_wraps_writer_exception_into_crypto_codec_write_error() -> None:
    def writer(data: bytes) -> None:
        _ = data
        raise OSError("raw-write-failed")

    codec = CryptoCodec(
        _Primitives(
            wrapper=lambda data: b"wrapped:" + data,
            framer=lambda data: b"framed:" + data,
        )
    )

    with pytest.raises(CryptoCodecWriteError) as ei:
        codec.write(writer, b"plain")

    assert isinstance(ei.value.__cause__, OSError)
    assert str(ei.value.__cause__) == "raw-write-failed"


# -------------------------
# Group g: protocol/runtime smoke
# -------------------------


def test_g1_primitives_object_satisfies_runtime_protocol() -> None:
    primitives = _Primitives()

    assert isinstance(primitives, CryptoRuntimePrimitivesProto)
