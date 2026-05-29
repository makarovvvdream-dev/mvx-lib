# Contributing

Thank you for your interest in contributing to `mvx-lib`.

`mvx-lib` is a monorepo. Packages in this repository are developed, tested, versioned, and published as package-level units.

This document defines repository-level contribution rules that apply to all packages unless a package documents additional package-specific requirements.

## Package-specific instructions

Each package may define its own:

```text
development setup
local check commands
supported Python versions
quality gates
documentation rules
release notes
```

Before working on a package, read the README file inside that package directory.

Currently implemented package:

```text
common/
```

## What can be contributed

Contributions may include:

```text
bug reports
documentation fixes
test improvements
correctness fixes
examples
practical feature proposals
```

For large changes, public API changes, or architectural changes, open an issue first.

The issue should describe:

```text
the affected package
the problem
the expected behavior
the proposed direction
any public API impact
```

## Development workflow

Work in the scope of the package you are changing.

Each package defines its own development setup, local checks, supported Python versions, and quality gates in its package README.

Before submitting a pull request, run the relevant package checks and make sure CI passes.

## Pull requests

Pull requests should be focused and easy to review.

A pull request should:

```text
describe the change
keep unrelated changes separate
include tests for behavior changes
update documentation when public behavior changes
pass CI
```

Avoid mixing formatting, refactoring, behavior changes, and documentation rewrites in one pull request unless they are part of the same focused change.

## Tests and documentation

Behavior changes should include tests.

Public API changes should include documentation updates.

Documentation changes should be checked by building the documentation site when they affect rendered documentation.

Documentation sources are located in:

```text
docs/
```

The documentation build command is documented in the repository README.

## Public API changes

Public APIs are part of the package contract.

Changes to public APIs should explain:

```text
what changes
why the change is needed
whether it is backward compatible
which tests cover it
which documentation pages were updated
```

Breaking changes should be avoided unless they are intentional and documented.

## Issues

Bug reports should include:

```text
affected package
package version
Python version
operating system
minimal reproduction when possible
expected behavior
actual behavior
relevant traceback or log output
```

Feature requests should describe the use case, not only the proposed implementation.

## License

By contributing to this repository, you agree that your contribution will be licensed under the Apache License, Version 2.0.

See:

```text
LICENSE
NOTICE
```
