# mvx-lib

`mvx-lib` is a monorepo for MVX Python packages.

The repository is intended to host several related packages under one codebase and one documentation site.

## Packages

### mvx-common

`mvx-common` contains common utilities for MVX Python packages.

Current package version: `0.2.0`

It currently provides:

```text
structured errors
public API error normalization helpers
asyncio cancellation helpers
structured logger infrastructure
```

Package README:

```text
common/README.md
```

Package configuration:

```text
common/pyproject.toml
```

## Documentation

Documentation is maintained as a single site for the repository.

Source files are located in:

```text
docs/
```

To build the documentation locally:

```bash
scripts/docs.sh
```

The generated HTML documentation is written to:

```text
docs/_build/html
```

## Development

Install the `mvx-common` package in editable mode from the package directory:

```bash
cd common
python -m pip install -e ".[dev]"
```

To include documentation dependencies:

```bash
python -m pip install -e ".[dev,docs]"
```

Run package checks from the `common` directory:

```bash
scripts/check.sh
```

## Repository status

`mvx-common` is the first package in this monorepo.

Other package areas, such as networking, security, and LDAP-related components, may be added later as the project grows.

## Author

Vladimir Makarov

Contact:

```text
makarovvv.dream@gmail.com
```
