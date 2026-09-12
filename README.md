# Spool Rack

Spool Rack is the remote synchronization server and repository host for
[Spool](https://github.com/autonomous-bits/spool) graph version-control
repositories. It stores immutable packs and snapshot objects in
content-addressed storage and manages repository metadata in PostgreSQL.

## Requirements

- Docker Engine with Docker Compose
- [`spl` CLI](https://github.com/autonomous-bits/spool) v1.5.0 or later to
  create, clone, push, and pull Spool workspaces

To build or run the server outside Docker, install Go 1.26.6 or later and
PostgreSQL 16.

## Install and start

Clone this repository and start Spool Rack with its PostgreSQL dependency:

```bash
git clone https://github.com/autonomous-bits/spool-rack.git
cd spool-rack
docker compose up -d --build
```

The Compose configuration:

- serves the API at `http://127.0.0.1:8080`;
- publishes PostgreSQL at `127.0.0.1:5433`;
- persists PostgreSQL and content-addressed data in Docker volumes; and
- seeds a development tenant and workspace.

Wait for the service to become healthy:

```bash
curl --fail http://127.0.0.1:8080/healthz
```

Set `SPOOL_RACK_PORT` or `POSTGRES_PORT` before starting Compose to override
the default host ports:

```bash
SPOOL_RACK_PORT=8081 POSTGRES_PORT=5434 docker compose up -d --build
```

## Use with Spool

The default local configuration accepts any non-empty bearer token and seeds
these identifiers:

```bash
export SPOOL_RACK_TOKEN=dev-token
export SPOOL_RACK_TENANT_ID=00000000-0000-4000-8000-000000000001
export SPOOL_RACK_WORKSPACE_ID=00000000-0000-4000-8000-000000000002
```

Configure a local Spool workspace to use the server:

```bash
spl remote set \
  --endpoint http://127.0.0.1:8080 \
  --tenant-id "$SPOOL_RACK_TENANT_ID" \
  --workspace-id "$SPOOL_RACK_WORKSPACE_ID" \
  --auth-mode bearer

spl remote show
```

Push a branch or retrieve a remote branch:

```bash
spl push --branch main
spl pull --branch main
```

To clone the seeded workspace into a new local directory:

```bash
spl clone \
  --endpoint http://127.0.0.1:8080 \
  --tenant-id "$SPOOL_RACK_TENANT_ID" \
  --workspace-id "$SPOOL_RACK_WORKSPACE_ID"
```

## Create tenants and workspaces

The development token grants administrator access. Create a tenant:

```bash
curl --fail-with-body -X POST http://127.0.0.1:8080/api/v1/tenants \
  -H "Authorization: Bearer $SPOOL_RACK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Acme Corporation"}'
```

Create a workspace in that tenant, using the tenant ID returned above:

```bash
curl --fail-with-body -X POST http://127.0.0.1:8080/api/v1/workspaces \
  -H "Authorization: Bearer $SPOOL_RACK_TOKEN" \
  -H "X-Tenant-ID: <tenant-id>" \
  -H "Content-Type: application/json" \
  -d '{"name":"backend-core"}'
```

Use the returned `workspaceId` with `spl remote set`.

## Develop locally

Run the test suite:

```bash
make test
```

Build the server binary:

```bash
make build
```

For direct execution, configure `CAS_ROOT`, `POSTGRES_DSN`, and
`POSTGRES_MIGRATIONS_DSN`. Set `DEV_TENANT_ID` (and optionally
`DEV_REPO_ID`) only for local development; it enables the permissive
development token verifier and seeds the specified tenant and workspace.

## Stop the stack

```bash
docker compose down
```

To also remove the persisted Docker volumes:

```bash
docker compose down -v
```

## Licence

This project is licensed under the [GNU Affero General Public License v3.0](LICENCE).
