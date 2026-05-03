# test_concurrent_future_cross_thread_wait.py

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time


def test_future_result_blocks_thread_without_loop_until_completed_from_another_thread() -> None:
    future: concurrent.futures.Future[str] = concurrent.futures.Future()

    waiter_started = threading.Event()
    waiter_entered_result = threading.Event()
    waiter_finished = threading.Event()

    completer_started = threading.Event()
    completer_finished = threading.Event()

    result_holder: list[str] = []
    error_holder: list[BaseException] = []

    waiter_thread_id_holder: list[int] = []
    completer_thread_id_holder: list[int] = []

    def waiter_thread_target() -> None:
        try:
            waiter_thread_id_holder.append(threading.get_ident())

            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise AssertionError("waiter thread unexpectedly has a running event loop")

            waiter_started.set()
            waiter_entered_result.set()

            result = future.result()

            result_holder.append(result)

        except BaseException as exc:
            error_holder.append(exc)

        finally:
            waiter_finished.set()

    def completer_thread_target() -> None:
        try:
            completer_thread_id_holder.append(threading.get_ident())

            completer_started.set()

            future.set_result("done")

        except BaseException as exc:
            error_holder.append(exc)

        finally:
            completer_finished.set()

    # first thread
    waiter_thread = threading.Thread(
        target=waiter_thread_target,
        name="waiter-thread-without-event-loop",
    )

    completer_thread = threading.Thread(
        target=completer_thread_target,
        name="completer-thread",
    )

    waiter_thread.start()

    assert waiter_started.wait(timeout=1.0)
    assert waiter_entered_result.wait(timeout=1.0)

    time.sleep(0.05)

    assert not waiter_finished.is_set()
    assert result_holder == []

    completer_thread.start()

    assert completer_started.wait(timeout=1.0)

    completer_thread.join(timeout=1.0)
    waiter_thread.join(timeout=1.0)

    assert completer_finished.is_set()
    assert waiter_finished.is_set()

    assert not completer_thread.is_alive()
    assert not waiter_thread.is_alive()

    assert error_holder == []
    assert result_holder == ["done"]

    assert len(waiter_thread_id_holder) == 1
    assert len(completer_thread_id_holder) == 1
    assert waiter_thread_id_holder[0] != completer_thread_id_holder[0]
