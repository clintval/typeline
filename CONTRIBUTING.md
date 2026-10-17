# Development and Testing

## Setup

Install [uv](https://docs.astral.sh/uv/) and a stable [Rust](https://rustup.rs) toolchain, then install the library and its development tools:

```console
uv sync
```

## Primary Development Commands

To check and resolve linting issues in the codebase, run:

```console
uv run ruff check --fix
```

To check and resolve formatting issues in the codebase, run:

```console
uv run ruff format
```

To check the unit tests in the codebase, run:

```console
uv run pytest
```

To check the typing in the codebase, run:

```console
uv run mypy && uv run basedpyright && uv run ty check
```

The native engine in `src/` is rebuilt by `uv run` whenever its Rust code changes.
To check its formatting, lint, tests, and documentation, run:

```console
uv run poe check-rust
```

To generate a code coverage report after testing locally, run:

```console
uv run coverage html
```

To check the lock file is up-to-date:

```console
uv lock --check
```

## Shortcut Task Commands

### For Running Individual Checks

```console
uv run poe check-lock
uv run poe check-pyproject
uv run poe check-format
uv run poe check-lint
uv run poe check-tests
uv run poe check-typing
uv run poe check-rust
```

### For Running All Checks

```console
uv run poe check-all
```

### For Running Individual Fixes

```console
uv run poe fix-format
uv run poe fix-lint
uv run poe fix-rust
```

### For Running All Fixes

```console
uv run poe fix-all
```

### For Running All Fixes and Checks

```console
uv run poe fix-and-check-all
```

## Releasing

Commit titles follow [Conventional Commits](https://www.conventionalcommits.org), which group the release notes.
The version is set only in `Cargo.toml`, which `pyproject.toml` reads it from.
To release, merge a pull request titled `chore(release): bump to X.Y.Z` that sets the version in `Cargo.toml` and updates `Cargo.lock` with `cargo update --workspace`, then tag its commit on `main` with `X.Y.Z` and push the tag.
The `publish_typeline.yml` workflow then checks that the tag matches the version, runs the tests, builds the source distribution and a wheel for every platform, checks that each installs and works, publishes them to PyPI, and makes a GitHub release with notes from [git-cliff](https://git-cliff.org).
