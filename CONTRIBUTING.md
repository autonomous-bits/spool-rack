# Contributing to Spool Rack

Thank you for contributing to Spool Rack. This guide describes the local
development workflow and the checks required before opening a pull request.

## Prerequisites

- Go 1.26 or later
- Docker Engine with Docker Compose
- PostgreSQL is provided by the Compose development stack

## Set up the development environment

Start PostgreSQL:

```bash
make db-up
```

Run the full application stack, including the server and its dependencies:

```bash
docker compose up -d --build
```

The API is available at `http://127.0.0.1:8080`, and its health endpoint is
available at `http://127.0.0.1:8080/healthz`. See the [README](README.md) for
instructions on connecting a Spool workspace.

## Development workflow

1. Create a focused branch from the current default branch.
2. Make changes with accompanying tests where behavior changes.
3. Format and verify the code.
4. Open a pull request that explains the problem, solution, and test coverage.

Keep changes scoped to one concern and preserve tenant isolation, authorization,
and content-addressed storage guarantees.

## Verify your changes

Run the standard checks before submitting a pull request:

```bash
make fmt-check
make tidy-check
make test
make build
```

For changes that affect concurrent behavior or PostgreSQL storage, also run:

```bash
make test-race
make test-postgres
```

`make test-postgres` starts the PostgreSQL Compose service if it is not already
running.

## Pull requests

Write a clear title using the repository’s conventional-commit style, such as
`feat(sync): add pack validation` or `fix(gateway): reject invalid scopes`.
In the pull request description, include:

- the user-visible or operational impact;
- the implementation approach;
- tests or checks run; and
- any migration, deployment, or compatibility considerations.

Do not include credentials, API tokens, or production connection strings in
source code, tests, documentation, or pull-request content.

## Licence

By contributing, you agree that your contribution is licensed under the
[GNU Affero General Public License v3.0](LICENCE).
