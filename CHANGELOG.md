# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Azure Managed Identity Authentication**: Added support for Azure Database for PostgreSQL Flexible Server authentication using Microsoft Entra ID OAuth2 access tokens via Azure Workload Identity, VM-based Managed Identity, and Azure CLI.
- **Dynamic Token Refresh**: Integrated with `pgxpool.Pool` via `BeforeConnect` to automatically acquire fresh tokens for scope `https://ossrdbms-aad.database.windows.net/.default` on every new connection, and periodically recycle pool connections with configurable `POSTGRES_MAX_CONN_LIFETIME`.
- **Database User Mapping**: Supported setting the database user via `POSTGRES_USER` or directly in the DSN to map to the PostgreSQL Entra role (e.g. managed identity name).
- **Decoupled Schema Migrations**: Supported running migrations via Azure Managed Identity, an independently authenticated password DSN (`POSTGRES_MIGRATIONS_DSN`), or skipping startup migrations (`POSTGRES_RUN_MIGRATIONS=false`) for external jobs/pipelines (`POSTGRES_MIGRATE_ONLY=true`).
- **Helm Chart Azure Workload Identity Support**: Added Helm values and templates for Azure Workload Identity, direct DSNs without static password secrets, and added `values-azure-workload-identity.yaml` example.

## [0.3.0] - 2026-09-13

### Added
- **Kubernetes Infrastructure**: Added `infra/` folder containing the Helm chart for deploying Spool Rack to Kubernetes.
- **Configurable CSI Storage**: Supported configurable CAS data storage with first-class CSI (Container Storage Interface) driver integration:
  - Dynamic provisioning with CSI `StorageClass`.
  - Static `PersistentVolume` provisioning with CSI driver specification (`pv.yaml`).
  - Pod-inline CSI volume mounting (`persistence.csi.inline`).
  - Configurable `mountPath` and automatic synchronization with `CAS_ROOT`.
  - Subpath mounting (`persistence.subPath`).
  - Example values for CSI StorageClass, static CSI PV, and inline CSI volumes.
- **Automated OCI Packaging & Publishing**: Added GitHub Actions workflow (`publish-chart.yml`) to automatically package and publish the Helm chart as an OCI artifact to GitHub Container Registry (`ghcr.io/autonomous-bits/charts/spool-rack`) on release.
- **Documentation**: Added Kubernetes deployment section to root `README.md` and detailed deployment documentation in `infra/README.md`.

### Changed
- Configured default container image in `docker-compose.yml` to `ghcr.io/autonomous-bits/spool-rack:0.3.0`.
- Updated Helm chart version to `0.3.0`.

## [0.2.0] - 2026-09-12

### Added
- Multi-architecture container image builds (`linux/amd64` and `linux/arm64`) with provenance attestation in `publish-container.yml`.
- Default container image configuration in `docker-compose.yml`.

## [0.1.0] - 2026-09-12

### Added
- Initial release of Spool Rack remote synchronization server and repository host.
- Content-addressed storage (CAS) with local filesystem driver.
- PostgreSQL metadata store with row-level security (RLS) tenant isolation.
- Native Spool push and pull validation engines with PackFormatV2 and PackFormatV3 DAG commit support.
- Tenant and workspace lifecycle management APIs.
- Remote branch management APIs (create, list, delete, default branch).
- Seeded development environment with Docker Compose.
- Health endpoint (`/healthz`) with contract and version diagnostics.

[Unreleased]: https://github.com/autonomous-bits/spool-rack/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/autonomous-bits/spool-rack/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/autonomous-bits/spool-rack/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/autonomous-bits/spool-rack/releases/tag/v0.1.0
