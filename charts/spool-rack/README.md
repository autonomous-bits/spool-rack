# Spool Rack Helm chart

## Install

Build and publish the image, then install the chart with its image repository and
tag:

```sh
docker build -t ghcr.io/autonomous-bits/spool-rack:0.1.0-mvp .
helm upgrade --install spool-rack ./charts/spool-rack \
  --namespace spool-rack --create-namespace \
  --set image.repository=ghcr.io/autonomous-bits/spool-rack \
  --set image.tag=0.1.0-mvp
```

The chart persists CAS data at `/var/lib/spool-rack` by default. Set
`persistence.enabled=false` only for disposable deployments.

## Postgres

The server's push and pull operations require both CAS storage and
`POSTGRES_DSN`. Provide the DSN through an existing Secret rather than chart
values:

```sh
kubectl -n spool-rack create secret generic spool-rack-postgres \
  --from-literal=POSTGRES_DSN='postgres://spool_app:password@postgres:5432/spool_rack?sslmode=require'

helm upgrade --install spool-rack ./charts/spool-rack \
  --namespace spool-rack \
  --set postgres.dsnSecret.name=spool-rack-postgres
```

Set `postgres.migrationsDsnSecret` when schema migrations require a privileged
database role distinct from the application's DSN.
