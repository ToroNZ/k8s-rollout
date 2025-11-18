use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{controller::Action, watcher, Controller},
    Client, Error, ResourceExt,
};
use log::{error, info, warn};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::Duration;

const ROLLOUT_LABEL: &str = "io.v0l.rollout";
const DIGEST_ANNOTATION: &str = "io.v0l.rollout/last-digest";

#[derive(Debug)]
enum ControllerError {
    KubeError(Error),
    HttpError(reqwest::Error),
    ToStrError(reqwest::header::ToStrError),
    MissingField(&'static str),
}

impl std::fmt::Display for ControllerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControllerError::KubeError(e) => write!(f, "Kubernetes error: {}", e),
            ControllerError::HttpError(e) => write!(f, "HTTP error: {}", e),
            ControllerError::ToStrError(e) => write!(f, "Header conversion error: {}", e),
            ControllerError::MissingField(field) => write!(f, "Missing field: {}", field),
        }
    }
}

impl std::error::Error for ControllerError {}

impl From<Error> for ControllerError {
    fn from(e: Error) -> Self {
        ControllerError::KubeError(e)
    }
}

impl From<reqwest::Error> for ControllerError {
    fn from(e: reqwest::Error) -> Self {
        ControllerError::HttpError(e)
    }
}

impl From<reqwest::header::ToStrError> for ControllerError {
    fn from(e: reqwest::header::ToStrError) -> Self {
        ControllerError::ToStrError(e)
    }
}

impl From<&'static str> for ControllerError {
    fn from(s: &'static str) -> Self {
        ControllerError::MissingField(s)
    }
}

type Result<T> = std::result::Result<T, ControllerError>;

#[derive(Clone)]
struct ControllerContext {
    client: Client,
    http_client: reqwest::Client,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    info!("Starting k8s rollout controller");

    let client = Client::try_default()
        .await?;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let ctx = ControllerContext {
        client: client.clone(),
        http_client,
    };

    let deployments: Api<Deployment> = Api::all(client);

    info!("Starting controller to watch deployments with label {}=true", ROLLOUT_LABEL);

    Controller::new(deployments, watcher::Config::default().labels(&format!("{}=true", ROLLOUT_LABEL)))
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled: {:?}", o),
                Err(e) => error!("Reconciliation error: {}", e),
            }
        })
        .await;

    Ok(())
}

async fn reconcile(deployment: Arc<Deployment>, ctx: Arc<ControllerContext>) -> Result<Action> {
    let name = deployment.name_any();
    let namespace = deployment.namespace().unwrap_or_else(|| "default".to_string());

    info!("Reconciling deployment: {}/{}", namespace, name);

    // Extract the first container's image
    let spec = deployment.spec.as_ref().ok_or("Deployment missing spec")?;
    let pod_spec = spec
        .template
        .spec
        .as_ref()
        .ok_or("Deployment template missing spec")?;
    let container = pod_spec
        .containers
        .first()
        .ok_or("Deployment has no containers")?;
    let image = container
        .image
        .as_ref()
        .ok_or("Container missing image")?;

    info!("Checking image: {}", image);

    // Fetch current digest from registry
    match fetch_image_digest(&ctx.http_client, image).await {
        Ok(current_digest) => {
            info!("Current digest for {}: {}", image, current_digest);

            // Check if digest has changed
            let last_digest = deployment
                .annotations()
                .get(DIGEST_ANNOTATION)
                .map(|s| s.as_str());

            if let Some(last) = last_digest {
                if last != current_digest {
                    info!(
                        "Digest changed for {}/{}: {} -> {}",
                        namespace, name, last, current_digest
                    );
                    trigger_rollout(&ctx.client, &namespace, &name, &current_digest).await?;
                } else {
                    info!("Digest unchanged for {}/{}", namespace, name);
                }
            } else {
                // First time seeing this deployment, just store the digest
                info!("First check for {}/{}, storing digest", namespace, name);
                update_digest_annotation(&ctx.client, &namespace, &name, &current_digest).await?;
            }
        }
        Err(e) => {
            warn!("Failed to fetch digest for {}: {}", image, e);
        }
    }

    // Requeue after 5 minutes
    Ok(Action::requeue(Duration::from_secs(300)))
}

fn error_policy(_deployment: Arc<Deployment>, error: &ControllerError, _ctx: Arc<ControllerContext>) -> Action {
    error!("Reconciliation error: {}", error);
    Action::requeue(Duration::from_secs(60))
}

async fn fetch_image_digest(client: &reqwest::Client, image: &str) -> Result<String> {
    // Parse the image reference
    let (registry, repository, tag) = parse_image_reference(image)?;

    // For Docker Hub
    if registry == "docker.io" || registry.is_empty() {
        return fetch_dockerhub_digest(client, &repository, &tag).await;
    }

    // For other registries (gcr.io, ghcr.io, etc.)
    fetch_generic_registry_digest(client, &registry, &repository, &tag).await
}

fn parse_image_reference(image: &str) -> Result<(String, String, String)> {
    // Format: [registry/]repository[:tag]
    let parts: Vec<&str> = image.split('/').collect();

    let (registry, repo_with_tag) = if parts.len() == 1 {
        // Just "image:tag" -> Docker Hub
        ("docker.io".to_string(), image.to_string())
    } else if parts.len() == 2 {
        // Could be "registry/image:tag" or "namespace/image:tag" (Docker Hub)
        if parts[0].contains('.') || parts[0].contains(':') {
            // Has domain -> external registry
            (parts[0].to_string(), parts[1].to_string())
        } else {
            // No domain -> Docker Hub with namespace
            ("docker.io".to_string(), image.to_string())
        }
    } else {
        // "registry/namespace/image:tag"
        (parts[0].to_string(), parts[1..].join("/"))
    };

    let (repository, tag) = if let Some(idx) = repo_with_tag.rfind(':') {
        let repo = &repo_with_tag[..idx];
        let tag = &repo_with_tag[idx + 1..];
        (repo.to_string(), tag.to_string())
    } else {
        (repo_with_tag, "latest".to_string())
    };

    Ok((registry, repository, tag))
}

async fn fetch_dockerhub_digest(
    client: &reqwest::Client,
    repository: &str,
    tag: &str,
) -> Result<String> {
    // Step 1: Get authentication token from Docker Hub
    let auth_url = format!(
        "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{}:pull",
        repository
    );

    let auth_response = client.get(&auth_url).send().await?;
    let auth_json: serde_json::Value = auth_response.json().await?;
    let token = auth_json["token"]
        .as_str()
        .ok_or("No token in auth response")?;

    // Step 2: Fetch manifest with token
    let manifest_url = format!(
        "https://registry-1.docker.io/v2/{}/manifests/{}",
        repository, tag
    );

    let response = client
        .get(&manifest_url)
        .header("Authorization", format!("Bearer {}", token))
        .header(
            "Accept",
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .send()
        .await?;

    if let Some(digest) = response.headers().get("Docker-Content-Digest") {
        Ok(digest.to_str()?.to_string())
    } else {
        // Fallback: compute digest from response body if header not present
        let body = response.bytes().await?;
        let mut hasher = Sha256::new();
        hasher.update(&body);
        let hash = hasher.finalize();
        Ok(format!("sha256:{:x}", hash))
    }
}

async fn fetch_generic_registry_digest(
    client: &reqwest::Client,
    registry: &str,
    repository: &str,
    tag: &str,
) -> Result<String> {
    // Generic OCI registry API v2
    let url = format!("https://{}/v2/{}/manifests/{}", registry, repository, tag);

    let response = client
        .get(&url)
        .header("Accept", "application/vnd.docker.distribution.manifest.v2+json")
        .send()
        .await?;

    if let Some(digest) = response.headers().get("Docker-Content-Digest") {
        Ok(digest.to_str()?.to_string())
    } else {
        Err("No digest header in response".into())
    }
}

async fn trigger_rollout(
    client: &Client,
    namespace: &str,
    name: &str,
    new_digest: &str,
) -> Result<()> {
    info!("Triggering rollout for {}/{}", namespace, name);

    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);

    // Update the digest annotation and trigger restart
    let mut annotations = BTreeMap::new();
    annotations.insert(DIGEST_ANNOTATION.to_string(), new_digest.to_string());
    annotations.insert(
        "kubectl.kubernetes.io/restartedAt".to_string(),
        chrono::Utc::now().to_rfc3339(),
    );

    let patch = serde_json::json!({
        "metadata": {
            "annotations": annotations
        },
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": chrono::Utc::now().to_rfc3339()
                    }
                }
            }
        }
    });

    api.patch(
        name,
        &PatchParams::apply("k8s-rollout-controller"),
        &Patch::Merge(&patch),
    )
    .await?;

    info!("Successfully triggered rollout for {}/{}", namespace, name);
    Ok(())
}

async fn update_digest_annotation(
    client: &Client,
    namespace: &str,
    name: &str,
    digest: &str,
) -> Result<()> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);

    let patch = serde_json::json!({
        "metadata": {
            "annotations": {
                DIGEST_ANNOTATION: digest
            }
        }
    });

    api.patch(
        name,
        &PatchParams::apply("k8s-rollout-controller"),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}
