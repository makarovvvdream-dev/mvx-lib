import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def load_example_module() -> Callable[..., ModuleType]:
    def inner(*relative_path_parts: str) -> ModuleType:
        example_path = Path(__file__).resolve().parents[1] / "examples" / Path(*relative_path_parts)

        module_name = "mvx_example_" + "_".join(relative_path_parts).replace(".", "_")

        spec = importlib.util.spec_from_file_location(
            module_name,
            example_path,
        )
        assert spec is not None
        assert spec.loader is not None

        module = importlib.util.module_from_spec(spec)

        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise

        return module

    return inner
