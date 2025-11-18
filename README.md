# Kubernetes Rollout Controller

An automatic deployment rollout controller that monitors container image digests and triggers rolling updates when images change.

## Overview

This controller watches Kubernetes Deployments labeled with `io.v0l.rollout=true` and periodically checks the container registry for image digest changes. When a new image is pushed with the same tag, the controller automatically triggers a rollout restart.

## Features

- **Automatic Rollout Detection**: Monitors image digests from container registries
- **Label-based Filtering**: Only monitors deployments with `io.v0l.rollout=true`
- **Multi-Registry Support**: Works with Docker Hub, GCR, GHCR, and other OCI-compliant registries
- **Digest Tracking**: Stores last known digest in deployment annotations
- **Configurable Check Interval**: Requeues every 5 minutes by default

## Building

### Build the binary locally

```bash
cargo build --release
```

### Build the Docker image

```bash
docker build -t k8s-rollout-controller:latest .
```

Or with a specific registry:

```bash
docker build -t gcr.io/your-project/k8s-rollout-controller:latest .
docker push gcr.io/your-project/k8s-rollout-controller:latest
```

## Deployment

### 1. Deploy the controller

```bash
kubectl apply -f k8s/deployment.yaml
```

This creates:
- A namespace `k8s-rollout-controller`
- A ServiceAccount with appropriate RBAC permissions
- The controller deployment

### 2. Update the image in the deployment

Edit `k8s/deployment.yaml` and replace `k8s-rollout-controller:latest` with your actual image.

### 3. Label your deployments

Add the label `io.v0l.rollout: "true"` to any deployment you want to monitor:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
  labels:
    io.v0l.rollout: "true"
spec:
  # ... rest of deployment spec
```

Or use kubectl:

```bash
kubectl label deployment my-app io.v0l.rollout=true
```

## Example Usage

Deploy the example applications:

```bash
kubectl apply -f k8s/example-deployment.yaml
```

This creates two example deployments that will be monitored by the controller.

## How It Works

1. The controller watches all Deployments with the label `io.v0l.rollout=true`
2. Every 5 minutes, it:
   - Fetches the current image digest from the container registry
   - Compares it with the digest stored in the annotation `io.v0l.rollout/last-digest`
   - If different, triggers a rollout restart by patching the deployment
3. The digest is stored in the deployment's annotations for tracking

## Configuration

### Environment Variables

- `RUST_LOG`: Set log level (default: `info`, options: `debug`, `info`, `warn`, `error`)

### Check Interval

The check interval is currently hardcoded to 5 minutes. To change it, modify this line in `src/main.rs`:

```rust
Ok(Action::requeue(Duration::from_secs(300))) // 300 seconds = 5 minutes
```

## RBAC Permissions

The controller requires the following permissions:

- `deployments`: `get`, `list`, `watch`, `patch`
- `deployments/status`: `get`

These are configured in `k8s/deployment.yaml`.

## Registry Authentication

### Docker Hub

The controller automatically handles Docker Hub authentication using anonymous tokens. This works for all public images without requiring credentials.

### Other Public Registries

The controller supports:
- Google Container Registry (gcr.io)
- GitHub Container Registry (ghcr.io)
- Quay.io
- Any OCI-compliant registry that allows anonymous access

### Private Registries

**Note**: Private registry authentication is not yet fully implemented. For private registries, you would need to:

1. Mount registry credentials into the controller pod
2. Modify the code to use those credentials when fetching manifests
3. Ensure the controller has network access to private registries

## Monitoring

Check the controller logs:

```bash
kubectl logs -n k8s-rollout-controller deployment/k8s-rollout-controller
```

You should see logs like:

```
[INFO] Starting k8s rollout controller
[INFO] Starting controller to watch deployments with label io.v0l.rollout=true
[INFO] Reconciling deployment: default/example-app
[INFO] Checking image: nginx:latest
[INFO] Current digest for nginx:latest: sha256:abc123...
[INFO] Digest unchanged for default/example-app
```

When a digest changes:

```
[INFO] Digest changed for default/example-app: sha256:abc123... -> sha256:def456...
[INFO] Triggering rollout for default/example-app
[INFO] Successfully triggered rollout for default/example-app
```

## Troubleshooting

### Controller not detecting changes

- Ensure the deployment has the label `io.v0l.rollout=true`
- Check that the image tag exists in the registry
- Verify the controller has network access to the registry
- Check controller logs for errors

### Permission errors

- Verify the ServiceAccount has the correct RBAC permissions
- Ensure the ClusterRoleBinding is properly configured

## Development

### Run locally

```bash
# Requires a kubeconfig file pointing to your cluster
cargo run
```

### Run tests

```bash
cargo test
```

## License

MIT
