# Contributing

Thanks for taking the time. Bug reports with a failing test are the most
useful thing you can send; pull requests are welcome too.

## Setup

You need [uv](https://docs.astral.sh/uv/) and Docker (for PostgreSQL).

```bash
make install      # uv sync: the package, test and lint tooling
make pg-up        # PostgreSQL 16 on localhost:55435
make check        # ruff, mypy, pytest
```

The tests run against real PostgreSQL on purpose: `SKIP LOCKED`, leases and
`NOTIFY` are the point of this library and cannot be faked meaningfully. Point
`DATABASE_URL` elsewhere if you already have a server. Without one, database
tests are skipped with a reason rather than failing. Set `REDIS_URL` to run the
Redis destination test.

## Pull requests

- Keep each change focused, with tests that fail without it.
- `make check` must pass: `ruff check`, `ruff format --check`, `mypy` (strict)
  and the full test suite.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/):
  `feat:`, `fix:`, `docs:`, `test:`, `ci:`, `chore:`, short and imperative.
- Update `CHANGELOG.md` under *Unreleased* for anything a user would notice.
- A change to a guarantee (ordering, leases, delivery) needs an ADR in
  `docs/adr/` explaining what changes and what it costs.

## Releasing

Bump `version` in `pyproject.toml` and `src/reliable_outbox/__init__.py`, move
the *Unreleased* changelog entries under the new version, and push a `vX.Y.Z`
tag. The release workflow builds and publishes to PyPI through trusted
publishing.
