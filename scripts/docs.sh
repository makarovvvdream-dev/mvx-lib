#!/usr/bin/env bash
set -euo pipefail


rm -rf docs/_build
sphinx-build -b html docs/ docs/_build/html -v

