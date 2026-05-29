#!/usr/bin/env bash
set -euo pipefail

black src tests
ruff check src tests --fix