use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Secret;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{controller::Action, watcher, Controller},
    Client, Error, ResourceExt,
};
use log::{debug, error, info, warn};
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
    Other(String),
}

impl std::fmt::Display for ControllerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControllerError::KubeError(e) => write!(f, "Kubernetes error: {}", e),
            ControllerError::HttpError(e) => write!(f, "HTTP error: {}", e),
            ControllerError::ToStrError(e) => write!(f, "Header conversion error: {}", e),
            ControllerError::MissingField(field) => write!(f, "Missing field: {}", field),
            ControllerError::Other(msg) => write!(f, "{}", msg),
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

impl From<String> for ControllerError {
    fn from(s: String) -> Self {
        ControllerError::Other(s)
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

    let pull_secret_names: Vec<String> = pod_spec
        .image_pull_secrets
        .as_ref()
        .map(|refs| refs.iter().map(|r| r.name.clone()).collect())
        .unwrap_or_default();

    // Fetch current digest from registry
    match fetch_image_digest(
        &ctx.client,
        &ctx.http_client,
        image,
        &namespace,
        &pull_secret_names,
    )
    .await
    {
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

async fn fetch_image_digest(
    kube_client: &Client,
    http_client: &reqwest::Client,
    image: &str,
    namespace: &str,
    pull_secret_names: &[String],
) -> Result<String> {
    let (registry, repository, tag) = parse_image_reference(image)?;

    let creds = if pull_secret_names.is_empty() {
        None
    } else {
        resolve_pull_secret_credentials(kube_client, namespace, pull_secret_names, &registry).await
    };

    if registry == "docker.io" || registry.is_empty() {
        return fetch_dockerhub_digest(http_client, &repository, &tag, creds.as_ref()).await;
    }

    fetch_generic_registry_digest(http_client, &registry, &repository, &tag, creds.as_ref()).await
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
    creds: Option<&RegistryCredentials>,
) -> Result<String> {
    // Step 1: Get authentication token from Docker Hub
    let auth_url = format!(
        "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{}:pull",
        repository
    );

    let mut auth_req = client.get(&auth_url);
    if let Some(c) = creds {
        auth_req = auth_req.basic_auth(&c.username, Some(&c.password));
    }
    let auth_response = auth_req.send().await?;
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

const MANIFEST_ACCEPT_HEADER: &str = "application/vnd.docker.distribution.manifest.v2+json,\
application/vnd.docker.distribution.manifest.list.v2+json,\
application/vnd.oci.image.manifest.v1+json,\
application/vnd.oci.image.index.v1+json";

async fn fetch_generic_registry_digest(
    client: &reqwest::Client,
    registry: &str,
    repository: &str,
    tag: &str,
    creds: Option<&RegistryCredentials>,
) -> Result<String> {
    let url = format!("https://{}/v2/{}/manifests/{}", registry, repository, tag);

    let response = client
        .get(&url)
        .header("Accept", MANIFEST_ACCEPT_HEADER)
        .send()
        .await?;

    let response = if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        let challenge = response
            .headers()
            .get("WWW-Authenticate")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_www_authenticate)
            .ok_or_else(|| {
                ControllerError::Other(format!(
                    "{} returned 401 with no parseable WWW-Authenticate challenge",
                    registry
                ))
            })?;

        let token = fetch_bearer_token(client, &challenge, creds).await?;

        client
            .get(&url)
            .header("Accept", MANIFEST_ACCEPT_HEADER)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await?
    } else {
        response
    };

    if !response.status().is_success() {
        return Err(ControllerError::Other(format!(
            "{} returned status {} for {}/{}:{}",
            registry,
            response.status(),
            registry,
            repository,
            tag
        )));
    }

    if let Some(digest) = response.headers().get("Docker-Content-Digest") {
        return Ok(digest.to_str()?.to_string());
    }

    // Fallback: hash the manifest body. Per the OCI distribution spec the
    // manifest digest is sha256 over the bytes returned on the wire, so this
    // is equivalent to the Docker-Content-Digest header for registries that
    // omit it (some Red Hat / ghcr edge cases).
    debug!(
        "No Docker-Content-Digest from {} for {}:{}, hashing body",
        registry, repository, tag
    );
    let body = response.bytes().await?;
    let mut hasher = Sha256::new();
    hasher.update(&body);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[derive(Debug, Clone)]
struct RegistryCredentials {
    username: String,
    password: String,
}

#[derive(Debug)]
struct AuthChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

fn parse_www_authenticate(header: &str) -> Option<AuthChallenge> {
    let header = header.trim();
    let rest = header
        .strip_prefix("Bearer")
        .or_else(|| header.strip_prefix("bearer"))?
        .trim_start();

    let mut realm = None;
    let mut service = None;
    let mut scope = None;

    for part in rest.split(',') {
        let part = part.trim();
        let (key, value) = match part.split_once('=') {
            Some(kv) => kv,
            None => continue,
        };
        let value = value.trim().trim_matches('"');
        match key.trim() {
            "realm" => realm = Some(value.to_string()),
            "service" => service = Some(value.to_string()),
            "scope" => scope = Some(value.to_string()),
            _ => {}
        }
    }

    Some(AuthChallenge {
        realm: realm?,
        service,
        scope,
    })
}

async fn fetch_bearer_token(
    client: &reqwest::Client,
    challenge: &AuthChallenge,
    creds: Option<&RegistryCredentials>,
) -> Result<String> {
    let mut url = reqwest::Url::parse(&challenge.realm)
        .map_err(|e| format!("Invalid auth realm URL {:?}: {}", challenge.realm, e))?;

    {
        let mut q = url.query_pairs_mut();
        if let Some(service) = &challenge.service {
            q.append_pair("service", service);
        }
        if let Some(scope) = &challenge.scope {
            q.append_pair("scope", scope);
        }
    }

    let mut req = client.get(url);
    if let Some(c) = creds {
        req = req.basic_auth(&c.username, Some(&c.password));
    }

    let resp = req.send().await?;
    if !resp.status().is_success() {
        return Err(ControllerError::Other(format!(
            "Token endpoint returned status {}",
            resp.status()
        )));
    }
    let json: serde_json::Value = resp.json().await?;

    let token = json["token"]
        .as_str()
        .or_else(|| json["access_token"].as_str())
        .ok_or("No token in auth response")?;

    Ok(token.to_string())
}

async fn resolve_pull_secret_credentials(
    client: &Client,
    namespace: &str,
    secret_names: &[String],
    registry: &str,
) -> Option<RegistryCredentials> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);

    for name in secret_names {
        let secret = match api.get(name).await {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "Failed to read imagePullSecret {}/{}: {}",
                    namespace, name, e
                );
                continue;
            }
        };

        let data = match secret.data.as_ref() {
            Some(d) => d,
            None => continue,
        };

        let config_bytes = match data
            .get(".dockerconfigjson")
            .or_else(|| data.get("config.json"))
        {
            Some(b) => &b.0,
            None => continue,
        };

        let config: serde_json::Value = match serde_json::from_slice(config_bytes) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "Failed to parse dockerconfigjson in {}/{}: {}",
                    namespace, name, e
                );
                continue;
            }
        };

        let auths = match config.get("auths").and_then(|v| v.as_object()) {
            Some(a) => a,
            None => continue,
        };

        for (key, value) in auths {
            if !registry_key_matches(key, registry) {
                continue;
            }
            if let Some(creds) = extract_credentials_from_auth_entry(value) {
                debug!(
                    "Using credentials from imagePullSecret {}/{} for {}",
                    namespace, name, registry
                );
                return Some(creds);
            }
        }
    }

    None
}

fn registry_key_matches(key: &str, registry: &str) -> bool {
    let host = key
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or(key)
        .trim_end_matches('/');

    if host.eq_ignore_ascii_case(registry) {
        return true;
    }

    // Docker Hub uses several aliases interchangeably in dockerconfigjson.
    let docker_hub_aliases = ["docker.io", "index.docker.io", "registry-1.docker.io"];
    let host_is_hub = docker_hub_aliases
        .iter()
        .any(|a| host.eq_ignore_ascii_case(a));
    let registry_is_hub = registry.is_empty()
        || docker_hub_aliases
            .iter()
            .any(|a| registry.eq_ignore_ascii_case(a));
    host_is_hub && registry_is_hub
}

fn extract_credentials_from_auth_entry(entry: &serde_json::Value) -> Option<RegistryCredentials> {
    if let Some(b64) = entry.get("auth").and_then(|v| v.as_str())
        && !b64.is_empty()
        && let Ok(decoded) = BASE64.decode(b64)
        && let Ok(s) = String::from_utf8(decoded)
        && let Some((u, p)) = s.split_once(':')
    {
        return Some(RegistryCredentials {
            username: u.to_string(),
            password: p.to_string(),
        });
    }

    let username = entry.get("username").and_then(|v| v.as_str())?;
    let password = entry.get("password").and_then(|v| v.as_str())?;
    Some(RegistryCredentials {
        username: username.to_string(),
        password: password.to_string(),
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_image_reference_handles_common_forms() {
        assert_eq!(
            parse_image_reference("nginx").unwrap(),
            ("docker.io".into(), "nginx".into(), "latest".into())
        );
        assert_eq!(
            parse_image_reference("nginx:1.27").unwrap(),
            ("docker.io".into(), "nginx".into(), "1.27".into())
        );
        assert_eq!(
            parse_image_reference("library/nginx:1.27").unwrap(),
            ("docker.io".into(), "library/nginx".into(), "1.27".into())
        );
        assert_eq!(
            parse_image_reference("ghcr.io/librenz/runcontainers/frontend:latest").unwrap(),
            (
                "ghcr.io".into(),
                "librenz/runcontainers/frontend".into(),
                "latest".into()
            )
        );
    }

    #[test]
    fn parse_www_authenticate_extracts_bearer_params() {
        let header = r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:librenz/foo:pull""#;
        let challenge = parse_www_authenticate(header).expect("parse should succeed");
        assert_eq!(challenge.realm, "https://ghcr.io/token");
        assert_eq!(challenge.service.as_deref(), Some("ghcr.io"));
        assert_eq!(
            challenge.scope.as_deref(),
            Some("repository:librenz/foo:pull")
        );
    }

    #[test]
    fn parse_www_authenticate_rejects_basic() {
        assert!(parse_www_authenticate(r#"Basic realm="foo""#).is_none());
    }

    #[test]
    fn registry_key_matches_handles_aliases_and_paths() {
        assert!(registry_key_matches("ghcr.io", "ghcr.io"));
        assert!(registry_key_matches("https://ghcr.io", "ghcr.io"));
        assert!(registry_key_matches("https://index.docker.io/v1/", "docker.io"));
        assert!(registry_key_matches("docker.io", "index.docker.io"));
        assert!(!registry_key_matches("ghcr.io", "gcr.io"));
        assert!(!registry_key_matches("quay.io", "ghcr.io"));
    }

    #[test]
    fn extract_credentials_decodes_base64_auth() {
        let entry = serde_json::json!({
            "auth": BASE64.encode("alice:s3cret"),
        });
        let creds = extract_credentials_from_auth_entry(&entry).unwrap();
        assert_eq!(creds.username, "alice");
        assert_eq!(creds.password, "s3cret");
    }

    #[test]
    fn extract_credentials_falls_back_to_username_password() {
        let entry = serde_json::json!({
            "username": "bob",
            "password": "pw",
        });
        let creds = extract_credentials_from_auth_entry(&entry).unwrap();
        assert_eq!(creds.username, "bob");
        assert_eq!(creds.password, "pw");
    }

    #[test]
    fn extract_credentials_handles_password_with_colon() {
        let entry = serde_json::json!({
            "auth": BASE64.encode("user:pa:ss"),
        });
        let creds = extract_credentials_from_auth_entry(&entry).unwrap();
        assert_eq!(creds.username, "user");
        assert_eq!(creds.password, "pa:ss");
    }
}
