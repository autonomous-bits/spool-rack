# Spool Rack Infrastructure

This directory contains deployment assets, charts, and configuration examples for deploying **Spool Rack** into Kubernetes and cloud environments.

## Directory Structure

```
infra/
├── README.md
└── charts/
    └── spool-rack/                  # Helm chart for Spool Rack
        ├── Chart.yaml
        ├── README.md
        ├── values.yaml
        ├── values.schema.json
        ├── examples/
        │   ├── values-csi-storageclass.yaml  # Dynamic CSI storage class (EBS, Azure Disk, GKE PD)
        │   ├── values-csi-pv.yaml            # Static CSI PersistentVolume (EFS, SMB, Azure Files)
        │   └── values-csi-inline.yaml        # Inline CSI Pod volume
        └── templates/
            ├── _helpers.tpl
            ├── deployment.yaml
            ├── hpa.yaml
            ├── pv.yaml                       # Static PV template for CSI drivers
            ├── pvc.yaml                      # PersistentVolumeClaim template
            ├── service.yaml
            ├── serviceaccount.yaml
            └── NOTES.txt
```

## Quick Start with Helm

1. Create a namespace and PostgreSQL Secret:

```bash
kubectl create namespace spool-rack
kubectl -n spool-rack create secret generic spool-rack-postgres \
  --from-literal=POSTGRES_DSN='postgres://spool_app:password@postgres:5432/spool_rack?sslmode=require'
```

2. Deploy the chart using Helm:

From the published OCI registry:
```bash
helm upgrade --install spool-rack oci://ghcr.io/autonomous-bits/charts/spool-rack \
  --version 0.3.0 \
  --namespace spool-rack \
  --set postgres.dsnSecret.name=spool-rack-postgres
```

Or from local source:
```bash
helm upgrade --install spool-rack ./infra/charts/spool-rack \
  --namespace spool-rack \
  --set postgres.dsnSecret.name=spool-rack-postgres
```

## Storage & CSI Driver Options

Spool Rack writes immutable CAS objects to the directory designated by `CAS_ROOT`. The Helm chart provides configurable options to connect this data volume to any Kubernetes CSI (Container Storage Interface) driver:

1. **CSI StorageClass (Dynamic Provisioning)**:
   Specify `persistence.storageClass: "<csi-storage-class>"`. See [`examples/values-csi-storageclass.yaml`](./charts/spool-rack/examples/values-csi-storageclass.yaml).
2. **Static CSI PersistentVolume**:
   Specify `persistence.csi.enabled: true` along with `persistence.csi.driver` and `volumeHandle` (e.g. AWS EFS `fs-xxxx` or SMB share). See [`examples/values-csi-pv.yaml`](./charts/spool-rack/examples/values-csi-pv.yaml).
3. **Pod-Inline CSI Volume**:
   Specify `persistence.csi.inline: true` with `driver` and `volumeAttributes`. See [`examples/values-csi-inline.yaml`](./charts/spool-rack/examples/values-csi-inline.yaml).
4. **Existing Claim**:
   Specify `persistence.existingClaim: "<claim-name>"`.

For detailed documentation, refer to the [Spool Rack Helm Chart README](./charts/spool-rack/README.md).
