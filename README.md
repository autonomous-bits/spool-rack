# Spool Rack

Spool Rack is the central remote synchronization server and repository host for [Spool](https://github.com/autonomous-bits/spool) graph version-control repositories.

It provides:
- **Content-Addressed Storage (CAS)** for immutable packs and snapshot object blobs.
- **Commit DAG & Branch Governance** stored in PostgreSQL with strict tenant boundary isolation enforced via Row-Level Security (RLS).
- **V2 Native Pack Exchange** supporting verified push transactions, thin packs, fast-forward branch advances, and conflict diagnosis.
- **REST Sync API** compatible with the `spl remote`, `spl push`, and `spl pull` CLI commands.

---

## Architecture & Storage

Spool Rack separates immutable payload blobs from relational DAG metadata:

```
                  ┌───────────────────────┐
                  │    Spool CLI (spl)    │
                  └──────────┬────────────┘
                             │ HTTP/JSON + V2 Pack Stream
                             ▼
                  ┌───────────────────────┐
                  │      Spool Rack       │
                  │   (Gateway & Sync)    │
                  └──────┬─────────┬──────┘
                         │         │
       CAS Object Blobs  │         │  Commit DAG & Branches
                         ▼         ▼
             ┌──────────────┐   ┌─────────────────┐
             │ File / S3    │   │   PostgreSQL    │
             │ CAS Storage  │   │  (RLS Enabled)  │
             └──────────────┘   └─────────────────┘
```

- **CAS Storage (`/var/lib/spool-rack`)**:
  Stores pack files (`.spack`) and individual graph snapshot objects indexed by BLAKE3 hashes under tenant- and repository-scoped directories:
  `/var/lib/spool-rack/tenants/<tenant_hash>/repos/<repo_hash>/[packs|objects]/`
- **PostgreSQL**:
  Stores tenants, repositories, branches, commit metadata, parent links, and pack ranges. Multi-tenancy is enforced on every query via `app.current_tenant_id`.

---

## Quickstart (Docker Compose)

### 1. Start Services

To launch Spool Rack and its PostgreSQL dependency:

```bash
docker compose up -d --build
```

This starts:
- **Spool Rack API**: listening on `http://127.0.0.1:8080` (health check at `http://127.0.0.1:8080/healthz`).
- **PostgreSQL 16**: listening on host port `5433` (internal port `5432`).

### 2. Environment Configuration

The following variables configure the server (defined in `docker-compose.yml`):

| Variable | Description | Default in Compose |
|---|---|---|
| `PORT` | HTTP server port | `8080` |
| `CAS_ROOT` | Directory for content-addressed files | `/var/lib/spool-rack` |
| `POSTGRES_DSN` | Application database connection string | `postgres://spool_app:...@postgres:5432/spool_rack` |
| `POSTGRES_MIGRATIONS_DSN` | Privileged connection string for schema migrations | `postgres://spool:...@postgres:5432/spool_rack` |
| `DEV_TENANT_ID` | Seeded tenant UUID for local development | `00000000-0000-4000-8000-000000000001` |
| `DEV_REPO_ID` | Seeded repository UUID for local development | `00000000-0000-4000-8000-000000000002` |

> [!NOTE]
> In development mode (`DEV_TENANT_ID` set), Spool Rack uses a local token verifier that automatically attributes incoming requests to the development tenant, eliminating the need for an external identity provider.

---

## Connecting a Spool Workspace (`spl`)

Follow these steps to connect a local Spool workspace to Spool Rack.

### 1. Add / Configure the Remote

Run `spl remote set` inside your Spool workspace:

```bash
spl remote set \
  --endpoint http://127.0.0.1:8080 \
  --repo-id 00000000-0000-4000-8000-000000000002 \
  --auth-mode bearer
```

- `--endpoint`: Base URL of the Spool Rack instance.
- `--repo-id`: UUID of the repository in Spool Rack.
- `--auth-mode`: `bearer` or `api_key`.

### 2. Set Authentication Token

Spool does **not** persist secrets in repository configuration. Supply the token via an environment variable:

```bash
export SPOOL_RACK_TOKEN="dev-token"
```

*(If using `--auth-mode api_key`, set `export SPOOL_RACK_API_KEY="your-key"` instead).*

### 3. Verify Remote Connection

Check that the CLI can communicate with Rack and has negotiated the pack protocol version:

```bash
spl remote show
```

Example response:
```json
{
  "endpoint": "http://127.0.0.1:8080",
  "repoId": "00000000-0000-4000-8000-000000000002",
  "authMode": "bearer",
  "versionStatus": "negotiated",
  "versions": {
    "packFormatVersion": { "local": 2, "remote": 2, "status": "match" },
    "packIndexFormatVersion": { "local": 1, "remote": 1, "status": "match" },
    "packManifestFormatVersion": { "local": 1, "remote": 1, "status": "match" }
  }
}
```

---

## Working with Remote Branches

### List Remote Branches

```bash
spl remote branch list
```

### Discover the Default Branch

```bash
spl remote branch default
```

### Push a Branch

Push local commits to Spool Rack:

```bash
spl push --branch <branch-name>
```

For linear branches, Spool builds a native pack and advances the remote branch.

#### Handling Non-Fast-Forward / Diverged Branches

If remote changes occurred concurrently, use `--reconcile`:

```bash
spl push --branch <branch-name> --reconcile
```

This fetches the remote history into a temporary local reconciliation branch, applies three-way semantic graph rebase using Spool's merge engine, and retries the push if no conflicts exist.

### Pull a Branch

Fast-forward your local branch with updates from Spool Rack:

```bash
spl pull --branch <branch-name>
```

### Create a Remote Branch

Create a remote branch directly on Spool Rack from an existing remote branch or commit:

```bash
# Create from an existing remote branch
spl remote branch create feature-auth --from-branch main

# Or create from a specific commit ID
spl remote branch create hotfix --from-commit <commit-id>
```

### Delete a Remote Branch

Safely delete a branch on Spool Rack (retained commits and objects remain intact for audit/retention):

```bash
spl remote branch delete <branch-name>
```

---

## Merging with Remote Repositories

Graph repositories support three-way semantic merges. Merges can be performed locally using the `spl merge` CLI or server-side via Spool Rack's REST API.

### 1. Local Three-Way Merge Workflow (`spl merge`)

Always compute a preview before applying a merge. A caller-owned transaction ID ensures idempotency and safe locking.

#### Step A: Preview the Merge
```bash
spl merge preview --source feature-auth --target main
```
This returns a deterministic preview JSON with a `previewId`, snapshot changes, and any semantic conflicts.

#### Step B: Apply a Clean Preview
If the preview reported zero conflicts:
```bash
spl merge apply \
  --source feature-auth \
  --target main \
  --transaction merge-101 \
  --preview <preview-id> \
  --author "Werner Swart" \
  --message "Merge feature-auth into main"
```

#### Step C: Resolving Conflicted Merges
If conflicts exist, the target branch is leased for your transaction:
```bash
# Inspect persisted conflict tokens
spl merge conflicts --target main --transaction merge-101

# Submit conflict selections (and optional property overrides)
spl merge resolve \
  --target main \
  --transaction merge-101 \
  --preview <preview-id> \
  --selections selections.json

# Finalize the merge after resolving all conflicts
spl merge finalize --target main --transaction merge-101

# (Or abort to release the lease without moving the target branch)
spl merge abort --target main --transaction merge-101
```

*`selections.json` format:*
```json
[
  { "conflictId": "conflict-token-1", "choice": "source" },
  { "conflictId": "conflict-token-2", "choice": "target" }
]
```

#### Step D: Push the Merged Target
Once merged locally, push the updated target branch:
```bash
spl push --branch main
```

---

### 2. Push Reconciliation (`spl push --reconcile`)

If you push to a remote branch that has advanced on Spool Rack since your base commit, standard push is rejected with a non-fast-forward error. Passing `--reconcile` automates the merge:

```bash
spl push --branch feature --reconcile
```

1. **Fetches** Spool Rack's current history into a local reconciliation branch.
2. **Rebases** your branch's independent local changes onto the remote head using the graph merge engine.
3. **Retries the push** automatically if the merge is clean.
4. **Reports conflicts** if semantic collisions exist, preserving both branches so you can inspect with `spl merge conflicts` and resolve them.

---

### 3. Server-Side Remote Merge API (Spool Rack)

For CI/CD runners, automated agents, and pull-request review interfaces, Spool Rack provides server-side merge endpoints:

#### Preview Merge
Computes lowest common ancestor (LCA) and deterministic three-way diff:
```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/repos/00000000-0000-4000-8000-000000000002/merge/preview \
  -H "Authorization: Bearer $SPOOL_RACK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceBranch": "feature-auth",
    "targetBranch": "main"
  }'
```

#### Acquire Target Branch Lease
Locks the target branch exclusively to prevent concurrent pushes during review:
```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/repos/00000000-0000-4000-8000-000000000002/merge/lease \
  -H "Authorization: Bearer $SPOOL_RACK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceBranch": "feature-auth",
    "targetBranch": "main"
  }'
```
Returns a `leaseToken` and lease expiration timestamp.

#### Apply Server-Side Merge
Atomically applies the preview, updates the target branch head, and releases the lease:
```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/repos/00000000-0000-4000-8000-000000000002/merge/apply \
  -H "Authorization: Bearer $SPOOL_RACK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceBranch": "feature-auth",
    "targetBranch": "main",
    "leaseToken": "<lease-token>",
    "author": "Werner Swart",
    "message": "Merge feature-auth into main",
    "resolutions": []
  }'
```

#### Release Lease (Abort / Cancel)
If a merge is cancelled without applying:
```bash
curl -s -X DELETE http://127.0.0.1:8080/api/v1/repos/00000000-0000-4000-8000-000000000002/merge/lease \
  -H "Authorization: Bearer $SPOOL_RACK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "targetBranch": "main",
    "leaseToken": "<lease-token>"
  }'
```

---

## Local Development & Testing

### Prerequisites
- Go 1.26+
- Docker & Docker Compose
- `spl` CLI v1.5.0+

### Running Tests

```bash
# Run all unit and integration tests
go test ./...

# Run sync package tests
go test ./internal/server/sync/...
```

### Code Formatting & Module Hygiene

```bash
# Format code
make fmt

# Verify code formatting and tidy modules
make fmt-check && make tidy-check
```

### Stopping Services

```bash
docker compose down
```

To clean up database and CAS volumes:
```bash
docker compose down -v
```
