# Spool Rack Helm Chart

Helm chart for deploying [Spool Rack](https://github.com/autonomous-bits/spool-rack) to Kubernetes.

## Quick Start

### 1. Configure PostgreSQL Secret

Spool Rack requires PostgreSQL metadata storage. Create a Kubernetes Secret containing your DSN:

```bash
kubectl create namespace spool-rack
kubectl -n spool-rack create secret generic spool-rack-postgres \
  --from-literal=POSTGRES_DSN='postgres://spool_app:password@postgres:5432/spool_rack?sslmode=require'
```

### 2. Install the Chart

Install directly from the published OCI registry:

```bash
helm upgrade --install spool-rack oci://ghcr.io/autonomous-bits/charts/spool-rack \
  --version 0.3.0 \
  --namespace spool-rack \
  --set postgres.dsnSecret.name=spool-rack-postgres
```

Or install from local source:

```bash
helm upgrade --install spool-rack ./infra/charts/spool-rack \
  --namespace spool-rack \
  --set postgres.dsnSecret.name=spool-rack-postgres
```

## Storage & CSI Driver Configuration

Spool Rack stores immutable CAS (content-addressed storage) data at `CAS_ROOT`, which defaults to `/var/lib/spool-rack`. The chart ensures that `persistence.mountPath` and `CAS_ROOT` remain synchronized.

### Option 1: Dynamic Provisioning with CSI StorageClass

When your cluster has a CSI driver installed with a corresponding `StorageClass` (e.g. AWS EBS `gp3`, Azure Disk `managed-csi`, or GKE `pd-ssd`):

```yaml
persistence:
  enabled: true
  storageClass: "gp3"
  size: 20Gi
  accessModes:
    - ReadWriteOnce
```

See [examples/values-csi-storageclass.yaml](./examples/values-csi-storageclass.yaml).

### Option 2: Static PersistentVolume with CSI Driver

When attaching existing cloud storage volumes (such as AWS EFS filesystems, Azure Files, SMB shares, or GCP Filestore) that require a `PersistentVolume` with `spec.csi`:

```yaml
persistence:
  enabled: true
  size: 50Gi
  accessModes:
    - ReadWriteMany
  csi:
    enabled: true
    driver: "efs.csi.aws.com"
    volumeHandle: "fs-0123456789abcdef0"
```

For drivers requiring authentication (such as SMB or Azure storage account keys):

```yaml
persistence:
  csi:
    enabled: true
    driver: "smb.csi.k8s.io"
    volumeHandle: "smb-server.local/share##"
    nodePublishSecretRef:
      name: "smb-creds"
```

See [examples/values-csi-pv.yaml](./examples/values-csi-pv.yaml).

### Option 3: Inline CSI Volume in Pod

For CSI drivers that support direct inline volume mounting in Pod specs:

```yaml
persistence:
  enabled: true
  csi:
    inline: true
    driver: "inline.storage.kubernetes.io"
    volumeAttributes:
      capacity: "10Gi"
```

See [examples/values-csi-inline.yaml](./examples/values-csi-inline.yaml).

### Option 4: Existing PersistentVolumeClaim

If you have already provisioned a PVC manually or through GitOps:

```yaml
persistence:
  enabled: true
  existingClaim: "my-existing-pvc"
```

## Values Reference

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of pods | `1` |
| `image.repository` | Image repository | `ghcr.io/autonomous-bits/spool-rack` |
| `image.tag` | Image tag (defaults to `Chart.appVersion`) | `""` |
| `persistence.enabled` | Enable persistence for CAS data | `true` |
| `persistence.mountPath` | Path in container for CAS data (sets `CAS_ROOT`) | `/var/lib/spool-rack` |
| `persistence.subPath` | Subpath inside the volume | `""` |
| `persistence.storageClass` | StorageClass name | `""` |
| `persistence.accessModes` | Access modes list | `[ReadWriteOnce]` |
| `persistence.size` | Storage request size | `10Gi` |
| `persistence.csi.enabled` | Create static PV with CSI driver spec | `false` |
| `persistence.csi.inline` | Mount CSI volume directly inline in Pod | `false` |
| `persistence.csi.driver` | CSI driver name | `""` |
| `persistence.csi.volumeHandle` | CSI volume handle / identifier | `""` |
| `persistence.csi.volumeAttributes` | Driver-specific volume attributes map | `{}` |
| `persistence.csi.nodePublishSecretRef` | Secret reference for node publish | `{}` |
| `postgres.dsnSecret.name` | Secret name holding `POSTGRES_DSN` | `""` |
| `postgres.dsnSecret.key` | Secret key holding `POSTGRES_DSN` | `POSTGRES_DSN` |
| `postgres.migrationsDsnSecret.name` | Secret name holding `POSTGRES_MIGRATIONS_DSN` | `""` |
