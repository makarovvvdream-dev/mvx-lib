#!/usr/bin/env bash
set -euo pipefail

black --check src tests
ruff check src tests
mypy
pytest
