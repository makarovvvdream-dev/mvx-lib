# Installation

```{contents} Contents:
:depth: 1
:local:
```

## Requirements

`mvx-common` requires Python 3.11 or newer.

```text
Python >= 3.11
```

Check your Python version:

```bash
python --version
```

or:

```bash
python3 --version
```

## Installing from PyPI

Install `mvx-common` from PyPI with `pip`:

```bash
python -m pip install mvx-common
```

After installation, the package can be imported through the `mvx` namespace:

```python
from mvx.common.logger import LogContext
```

## Installing a specific version

To install a specific released version:

```bash
python -m pip install "mvx-common==0.2.1"
```

To upgrade to the latest available version:

```bash
python -m pip install --upgrade mvx-common
```

## Installing in a virtual environment

A virtual environment is recommended for application and development work.

Create one:

```bash
python -m venv .venv
```

Activate it on macOS or Linux:

```bash
source .venv/bin/activate
```

Activate it on Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Then install the package:

```bash
python -m pip install mvx-common
```

## Installing from the local repository

For development, install the package from the local repository in editable mode.

From the `common` package directory:

```bash
cd common
python -m pip install -e .
```

Editable mode means that Python imports the package from the working tree. Code changes are visible without reinstalling the package.

## Installing development dependencies

Development dependencies are defined in `common/pyproject.toml` under the `dev` extra.

Install them with:

```bash
cd common
python -m pip install -e ".[dev]"
```

This installs the package in editable mode together with development tools such as test, coverage, linting, formatting, and type-checking dependencies.

## Installing documentation dependencies

Documentation dependencies are defined under the `docs` extra.

Install them with:

```bash
cd common
python -m pip install -e ".[docs]"
```

For normal documentation work, it is usually convenient to install both development and documentation dependencies:

```bash
cd common
python -m pip install -e ".[dev,docs]"
```

## Building documentation

Documentation is built from the repository root.

```bash
scripts/docs.sh
```

The generated HTML files are written to:

```text
docs/_build/html
```

Open the generated documentation in a browser from that directory.

## Running checks

Run package checks from the package directory:

```bash
cd common
scripts/check.sh
```

The check script runs formatting, linting, type checking, tests, and branch coverage checks.

## Package name and import path

The distribution package name is:

```text
mvx-common
```

The Python import namespace is:

```text
mvx.common
```

This means the package is installed with:

```bash
python -m pip install mvx-common
```

but imported like this:

```python
from mvx.common.logger import LogContext
```

This difference is normal for Python packages. The distribution name is the name used by package installers. The import path is the Python module namespace exposed by the installed package.

## Troubleshooting

### pip installs into the wrong Python

Use:

```bash
python -m pip install mvx-common
```

instead of:

```bash
pip install mvx-common
```

This avoids accidentally installing into another Python environment.

### extras fail in zsh

If your shell is `zsh`, quote extras:

```bash
python -m pip install -e ".[dev,docs]"
```

or:

```bash
python -m pip install "mvx-common[docs]"
```

Without quotes, `zsh` may try to interpret square brackets as a glob pattern.

### package is not found on PyPI

If `pip` cannot find `mvx-common`, check the package name, Python version, and selected package index.

For local development, use editable installation from the repository:

```bash
cd common
python -m pip install -e ".[dev,docs]"
```

## Summary

For normal users:

```bash
python -m pip install mvx-common
```

For a specific version:

```bash
python -m pip install "mvx-common==0.2.1"
```

For local development:

```bash
cd common
python -m pip install -e ".[dev,docs]"
```
