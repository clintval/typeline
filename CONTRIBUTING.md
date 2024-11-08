# Development and Testing

## Primary Development Commands

To check and resolve linting issues in the codebase, run:

```console
poetry run ruff check --fix
```

To check and resolve formatting issues in the codebase, run:

```console
poetry run ruff format
```

To check the unit tests in the codebase, run:

```console
poetry run pytest
```

To check the typing in the codebase, run:

```console
poetry run mypy
```

To generate a code coverage report after testing locally, run:

```console
poetry run coverage html
```

To check the lock file is up-to-date:

```console
poetry check --lock
```

## Shortcut Task Commands

### For Running Individual Checks

```console
poetry task check-lock
poetry task check-format
poetry task check-lint
poetry task check-tests
poetry task check-typing
```

### For Running All Checks

```console
poetry task check-all
```

### For Running Individual Fixes

```console
poetry task fix-format
poetry task fix-lint
```

### For Running All Fixes

```console
poetry task fix-all
```

### For Running All Fixes and Checks

```console
poetry task fix-and-check-all
```
