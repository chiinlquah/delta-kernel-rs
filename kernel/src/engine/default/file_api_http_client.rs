//! object_store interface to talk to the file system service

use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use reqwest::{Certificate, Client, Identity};
use serde::Deserialize;

//use url::Url;

//use anyhow::{anyhow, Result as AnyhowResult};

// Import ObjectStore types
use crate::object_store::{
    path::Path, Attributes, CopyOptions, Error as ObjectStoreError, GetOptions, GetRange,
    GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};

#[derive(Debug, Clone)]
pub struct FilesApiHttpClient {
    client: Client,
    workspace_url: String,
    auth_headers: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub name: String,
    pub is_directory: bool,
    pub file_size: Option<u64>,
    pub last_modified: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct DirectoryListResponse {
    pub contents: Vec<FileInfo>,
    pub next_page_token: Option<String>,
}

/// Convert an std::io::Error into an ObjectStore::Error
fn io_to_object_store_err(io_err: std::io::Error, path: Option<&str>) -> ObjectStoreError {
    use std::io::ErrorKind;
    match io_err.kind() {
        ErrorKind::NotFound => ObjectStoreError::NotFound {
            path: path.unwrap_or("Unknown path").to_string(),
            source: Box::new(io_err),
        },
        _ => ObjectStoreError::Generic {
            store: "files_api_client",
            source: Box::new(io_err),
        },
    }
}

/// Convert an std::io::Error into an ObjectStore::Error
fn generic_err(err: Box<dyn std::error::Error + Send + Sync + 'static>) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "files_api_client",
        source: err,
    }
}

/// Convert a `GetRange` into an HTTP `Range` header value.
fn get_range_to_header(range: &GetRange) -> String {
    match range {
        GetRange::Bounded(r) => format!("bytes={}-{}", r.start, r.end.saturating_sub(1)),
        GetRange::Offset(n) => format!("bytes={}-", n),
        GetRange::Suffix(n) => format!("bytes=-{}", n),
    }
}

/// Parse a `Content-Range: bytes start-end/total` header.
/// Returns `(byte_range, total_size)`. Falls back to `0..fallback_len` / `fallback_len`
/// if parsing fails.
fn parse_content_range(header: &str, fallback_len: u64) -> (std::ops::Range<u64>, u64) {
    // Expected format: "bytes 0-499/1234" or "bytes 0-499/*"
    let inner = header.strip_prefix("bytes ").unwrap_or(header);
    let (range_part, total_part) = inner.split_once('/').unwrap_or((inner, "*"));
    let total = total_part.parse::<u64>().unwrap_or(fallback_len);
    let range = range_part
        .split_once('-')
        .and_then(|(s, e)| Some(s.parse::<u64>().ok()?..e.parse::<u64>().ok()?.saturating_add(1)))
        .unwrap_or(0..fallback_len);
    (range, total)
}

impl FilesApiHttpClient {
    fn resolve_first_ip(host: &str, port: u16) -> std::io::Result<IpAddr> {
        // Do a normal getaddrinfo on "filesystem:9337"
        // Prefer IPv4 if present; otherwise take the first result.
        let mut ipv6: Option<IpAddr> = None;
        for sa in (host, port).to_socket_addrs()? {
            match sa.ip() {
                IpAddr::V4(v4) => return Ok(IpAddr::V4(v4)),
                IpAddr::V6(v6) => {
                    if ipv6.is_none() {
                        ipv6 = Some(IpAddr::V6(v6));
                    }
                }
            }
        }
        ipv6.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no A/AAAA records"))
    }

    // TODO: Validate how we construct here
    pub fn try_new(
        workspace_url: &str,
        user_id: &str,
        user_name: &str,
        org_id: &str,
        account_id: &str,
        bearer_token: &str,
    ) -> ObjectStoreResult<Self> {
        let mut auth_headers = HashMap::new();
        auth_headers.insert("X-Databricks-User-Id".to_string(), user_id.to_string());
        auth_headers.insert("X-Databricks-User-Name".to_string(), user_name.to_string());
        auth_headers.insert("X-Databricks-Org-Id".to_string(), org_id.to_string());
        auth_headers.insert(
            "X-Databricks-Account-Id".to_string(),
            account_id.to_string(),
        );
        auth_headers.insert("X-Databricks-System-User".to_string(), true.to_string());
        auth_headers.insert(
            "Authorization".to_string(),
            format!("Bearer {}", bearer_token),
        );

        // let id = Identity::from_pkcs12_der(&std::fs::read("/databricks/secrets/keystore.jks")?,
        // store_password)?;
        //
        //         let dir_path = std::path::Path::new("/databricks/secrets/");
        //         Self::list_directory_contents(&dir_path);

        // Cert paths default to the production layout under /databricks/secrets/, but each
        // can be overridden via env var so hermetic tests (Bazel sandbox, dev machines)
        // can point at writable temp dirs without modifying this code.
        //
        //   DATABRICKS_KERNEL_CERT_PATH   -> client cert (PEM)
        //   DATABRICKS_KERNEL_KEY_PATH    -> client private key (PEM)
        //   DATABRICKS_KERNEL_CA_PATH     -> CA chain (PEM) used to verify the server
        let cert_path = std::env::var("DATABRICKS_KERNEL_CERT_PATH")
            .unwrap_or_else(|_| "/databricks/secrets/certificate.pem".to_string());
        let key_path = std::env::var("DATABRICKS_KERNEL_KEY_PATH")
            .unwrap_or_else(|_| "/databricks/secrets/certificate.key".to_string());
        let ca_cert_path = std::env::var("DATABRICKS_KERNEL_CA_PATH")
            .unwrap_or_else(|_| "/databricks/secrets/ca.crt".to_string());

        // Load identity (client cert + private key)
        tracing::debug!("Reading certificate.pem from {cert_path}");
        let cert_bytes = std::fs::read(&cert_path)
            .map_err(|e| io_to_object_store_err(e, Some(cert_path.as_str())))?;
        tracing::debug!("Reading certificate.key from {key_path}");
        let key_bytes = std::fs::read(&key_path)
            .map_err(|e| io_to_object_store_err(e, Some(key_path.as_str())))?;
        let mut identity_pem = Vec::new();
        identity_pem.extend_from_slice(&cert_bytes);
        identity_pem.extend_from_slice(&key_bytes);

        tracing::debug!("Loading identity (client cert + private key)");
        let id = Identity::from_pem(&identity_pem).map_err(|e| generic_err(Box::new(e)))?;

        // Load CA to trust the server
        tracing::debug!("Reading ca.crt from {ca_cert_path}");
        let ca_bytes = std::fs::read(&ca_cert_path)
            .map_err(|e| io_to_object_store_err(e, Some(ca_cert_path.as_str())))?;
        tracing::debug!("Loading certificate (ca.crt)");
        let ca = Certificate::from_pem(&ca_bytes).map_err(|e| generic_err(Box::new(e)))?;

        // // Resolve filesystem to an IP addr
        // tracing::debug!("Resolving filesystem.service to an ");
        // let ip = Self::resolve_first_ip("filesystem.service", 9337)
        //     .map_err(|e| io_to_object_store_err(e, None))?;
        // let mapped = SocketAddr::new(ip, 9337);

        tracing::debug!("Building client");
        let mut client_builder = Client::builder()
            .use_rustls_tls()
            .identity(id)
            .add_root_certificate(ca)
            .timeout(Duration::from_secs(300)); // 5 minute timeout

        // Optional DNS-override env var. Format: `host=ip:port[,host=ip:port,...]`. Each entry
        // becomes a `client_builder.resolve(host, SocketAddr)` call so reqwest bypasses
        // `getaddrinfo` for that hostname and connects directly to the supplied address.
        //
        // Production never sets this — pods rely on Kube DNS (`filesystem.service` resolves
        // automatically). Hermetic tests (Bazel sandbox) use it to point a wildcard-SAN
        // hostname like `filesystem.svc.cluster.local` at the LITE deployer's bound
        // `127.0.0.1:<dynamic-port>`. Strict TLS hostname verification is preserved — only
        // DNS is overridden, not the SNI/SAN comparison.
        if let Ok(spec) = std::env::var("DATABRICKS_KERNEL_DNS_OVERRIDE") {
            for entry in spec.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                let Some((host, addr_str)) = entry.split_once('=') else {
                    return Err(generic_err(
                        format!("Invalid DATABRICKS_KERNEL_DNS_OVERRIDE entry (expected `host=ip:port`): {entry}").into(),
                    ));
                };
                let addr: SocketAddr = addr_str.parse().map_err(|e| {
                    generic_err(
                        format!("Invalid socket addr in DATABRICKS_KERNEL_DNS_OVERRIDE entry `{entry}`: {e}")
                            .into(),
                    )
                })?;
                tracing::debug!("DNS override: {} -> {}", host, addr);
                client_builder = client_builder.resolve(host, addr);
            }
        }

        let client = client_builder
            .build()
            .map_err(|e| generic_err(Box::new(e)))?;
        tracing::debug!("Built the client");

        Ok(Self {
            client,
            workspace_url: workspace_url.to_string(),
            auth_headers,
        })
    }

    pub async fn get_file(
        &self,
        path: &str,
        range: Option<&str>,
    ) -> ObjectStoreResult<(Bytes, reqwest::header::HeaderMap)> {
        let url = self.get_files_url(path);
        tracing::debug!("Sending HTTP request to: {:#?}", url);

        let range_header = range.map(|r| {
            let mut m = HashMap::new();
            m.insert("Range".to_string(), r.to_string());
            m
        });

        let response = match self
            .client
            .get(&url)
            .headers(self.build_headers(range_header)?)
            .send()
            .await
        {
            Ok(response) => {
                tracing::info!(
                    "Received HTTP response back:: {} {}",
                    response.status(),
                    url
                );
                response
            }
            Err(e) => {
                tracing::error!("Failed HTTP request for URL {}: {:?}", url, e);
                tracing::error!(
                    "Request details - method: GET, headers: {:?}",
                    self.auth_headers
                );
                return Err(generic_err(Box::new(e)));
            }
        };

        // Map HTTP 404 to ObjectStore's NotFound so callers like Delta's `_last_checkpoint`
        // hint can treat it as "no checkpoint, fall through to commit listing" rather than a
        // hard error. Other 4xx/5xx flow through as Generic via `error_for_status()` below.
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ObjectStoreError::NotFound {
                path: url,
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "files_api responded 404",
                )),
            });
        }

        let response = response
            .error_for_status()
            .map_err(|e| generic_err(Box::new(e)))?;
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(|e| generic_err(Box::new(e)))?;
        Ok((body, headers))
    }

    pub async fn list_directory(
        &self,
        path: &str,
        page_token: Option<&str>,
        start_from: Option<&str>,
        recursive: bool,
    ) -> ObjectStoreResult<DirectoryListResponse> {
        let url = self.get_directories_url(path);

        let mut req = self.client.get(&url).headers(self.build_headers(None)?);
        if let Some(token) = page_token {
            req = req.query(&[("page_token", token)]);
        }
        if let Some(start) = start_from {
            req = req.query(&[("start_from", start)]);
        }
        if recursive {
            req = req.query(&[("recursive", "true")]);
        }
        let response = req.send().await.map_err(|e| generic_err(Box::new(e)))?;

        let response = response
            .error_for_status()
            .map_err(|e| generic_err(Box::new(e)))?;

        let directory_listing: DirectoryListResponse = response
            .json()
            .await
            .map_err(|e| generic_err(Box::new(e)))?;
        Ok(directory_listing)
    }

    pub async fn get_head(&self, path: &str) -> ObjectStoreResult<ObjectMeta> {
        let url = self.get_files_url(path);

        let response = self
            .client
            .head(&url) // Use HEAD instead of GET
            .headers(self.build_headers(None)?)
            .send()
            .await
            .map_err(|e| generic_err(Box::new(e)))?;

        let response = response
            .error_for_status()
            .map_err(|e| generic_err(Box::new(e)))?;

        let headers = response.headers();

        // Parse content-length
        let size = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        // Parse last-modified
        let last_modified = headers
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .and_then(|date_str| {
                // Parse HTTP date format: "Thu, 31 Jul 2025 20:34:04 GMT"
                chrono::DateTime::parse_from_rfc2822(date_str).ok()
            })
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or(chrono::DateTime::UNIX_EPOCH);

        // Parse e_tag if present
        let e_tag = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let location = Path::parse(path)?;

        Ok(ObjectMeta {
            location,
            last_modified,
            size,
            e_tag,
            version: None,
        })
    }

    fn get_files_url(&self, path: &str) -> String {
        format!(
            "{}/api/2.0/fs/files/{}",
            self.workspace_url,
            path.trim_start_matches('/')
        )
    }

    fn get_directories_url(&self, path: &str) -> String {
        format!(
            "{}/api/2.0/fs/directories/{}",
            self.workspace_url,
            path.trim_start_matches('/')
        )
    }

    // FilesystemHttpClient.scala:408 for get headers
    fn build_headers(
        &self,
        additional: Option<HashMap<String, String>>,
    ) -> ObjectStoreResult<reqwest::header::HeaderMap> {
        let mut header_map = reqwest::header::HeaderMap::new();

        // Add auth headers
        for (key, value) in &self.auth_headers {
            header_map.insert(
                reqwest::header::HeaderName::from_bytes(key.as_bytes())
                    .map_err(|e| generic_err(Box::new(e)))?,
                reqwest::header::HeaderValue::from_str(value)
                    .map_err(|e| generic_err(Box::new(e)))?,
            );
        }

        // Add additional headers if provided
        if let Some(additional) = additional {
            for (key, value) in additional {
                header_map.insert(
                    reqwest::header::HeaderName::from_bytes(key.as_bytes())
                        .map_err(|e| generic_err(Box::new(e)))?,
                    reqwest::header::HeaderValue::from_str(&value)
                        .map_err(|e| generic_err(Box::new(e)))?,
                );
            }
        }

        Ok(header_map)
    }

    fn list_paginated(
        &self,
        prefix_str: String,
        start_from: Option<String>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let client = self.clone();

        let stream = async_stream::stream! {
            let mut page_token: Option<String> = None;
            // FilesApi rejects `recursive=true` combined with `start_from` (server enforces
            // FilesApiUnsupportedParametersCombination). The object_store contract here is a
            // recursive list with an offset, so we send the recursive listing without
            // `start_from` and filter the offset client-side. This trades a small amount of
            // wire bandwidth for compatibility with the server's parameter rules.
            loop {
                match client.list_directory(
                    &prefix_str,
                    page_token.as_deref(),
                    None,
                    true // recursive is the object_store contract
                ).await {
                    Ok(response) => {
                        for file_info in response.contents {
                            if file_info.is_directory {
                                continue;
                            }
                            if let Some(offset) = start_from.as_deref() {
                                // FS service returns paths with a leading `/` while
                                // delta_kernel hands offsets in without one. Compare on the
                                // unleading-slash form so the lexicographic check matches up.
                                let normalized = file_info.path.trim_start_matches('/');
                                let normalized_offset = offset.trim_start_matches('/');
                                if normalized < normalized_offset {
                                    continue;
                                }
                            }
                            match Self::file_info_to_object_meta(file_info) {
                                Ok(meta) => yield Ok(meta),
                                Err(e) => yield Err(ObjectStoreError::Generic {
                                    store: "FilesApiHttpClient",
                                    source: e.into(),
                                }),
                            }
                        }
                        match response.next_page_token {
                            Some(token) => page_token = Some(token),
                            None => break,
                        }
                    }
                    Err(e) => {
                        if !e.to_string().to_lowercase().contains("404") {
                            yield Err(ObjectStoreError::Generic {
                                store: "FilesApiHttpClient",
                                source: e.into(),
                            });
                        }
                        break;
                    }
                }
            }
        };

        Box::pin(stream)
    }

    // Add this helper method
    fn file_info_to_object_meta(file_info: FileInfo) -> ObjectStoreResult<ObjectMeta> {
        let path = Path::parse(&file_info.path)?;

        let last_modified = if let Some(timestamp_ms) = file_info.last_modified {
            let timestamp_secs = timestamp_ms / 1000;
            let timestamp_nanos = (timestamp_ms % 1000) * 1_000_000;
            std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::new(timestamp_secs, timestamp_nanos as u32)
        } else {
            std::time::SystemTime::UNIX_EPOCH // use epoch as sentinel since we have to return
                                              // something
        };

        Ok(ObjectMeta {
            location: path,
            last_modified: last_modified.into(),
            size: file_info.file_size.unwrap_or(0),
            e_tag: None,
            version: None,
        })
    }
}

impl fmt::Display for FilesApiHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabricksFilesObjectStore({})", self.workspace_url)
    }
}

// use presigned url if this doesnt work

// get *
// list *
// head
// put
// get_range

#[async_trait]
impl ObjectStore for FilesApiHttpClient {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let path_str = location.as_ref().trim_end_matches('/');

        if options.head {
            let meta = self.get_head(path_str).await?;
            let size = meta.size;
            use futures::stream;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(stream::empty())),
                range: 0..size,
                meta,
                attributes: Attributes::new(),
            });
        }

        let range_header = options.range.as_ref().map(get_range_to_header);
        let (content, headers) = self.get_file(path_str, range_header.as_deref()).await?;

        // Derive byte range and total file size from Content-Range (present on partial responses)
        // or fall back to the body length for full responses.
        let (range, total_size) = headers
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(|cr| parse_content_range(cr, content.len() as u64))
            .unwrap_or_else(|| (0..content.len() as u64, content.len() as u64));

        let last_modified = headers
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or(chrono::DateTime::UNIX_EPOCH);

        let e_tag = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        use futures::stream;
        let stream = Box::pin(stream::once(futures::future::ready(Ok(content))));

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified,
                size: total_size,
                e_tag,
                version: None,
            },
            range,
            attributes: Attributes::new(),
        })
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        self.list_paginated(prefix_str, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        self.list_paginated(prefix_str, Some(offset.to_string()))
    }

    // async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
    //     let path_str = location.as_ref().trim_start_matches('/');

    //     self.get_head(path_str).await.map_err(|err| {
    //         let error_msg = err.to_string().to_lowercase();
    //         if error_msg.contains("404") || error_msg.contains("not found") {
    //             ObjectStoreError::NotFound {
    //                 path: location.to_string(),
    //                 source: err.into(),
    //             }
    //         } else {
    //             ObjectStoreError::Generic {
    //                 store: "FilesApiHttpClient",
    //                 source: err.into(),
    //             }
    //         }
    //     })
    // }

    // async fn delete(&self, _location: &Path) -> ObjectStoreResult<()> {
    //     unimplemented!("we dont use this")
    // }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        unimplemented!("Not used");
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        // FilesApi maps `?overwrite=true` to "create or replace" and `?overwrite=false` to
        // "atomic create-only" (server returns 409 if the path already exists). The REST
        // surface has no equivalent for `PutMode::Update(version)` today, so reject that.
        let overwrite = match opts.mode {
            PutMode::Overwrite => "true",
            PutMode::Create => "false",
            PutMode::Update(_) => {
                return Err(ObjectStoreError::NotSupported {
                    source: "FilesApiHttpClient does not support PutMode::Update yet".into(),
                });
            }
        };

        let path_str = location.as_ref().trim_end_matches('/');
        let url = self.get_files_url(path_str);

        // Flatten the chunked payload into a single contiguous `Bytes`. `From<PutPayload>` is
        // zero-copy when the payload has exactly one chunk (the common case for Delta commit
        // JSONs produced by `to_json_bytes`).
        let body: Bytes = payload.into();
        tracing::debug!(
            "PUT {} (overwrite={}) content-length={}",
            url,
            overwrite,
            body.len()
        );

        let response = self
            .client
            .put(&url)
            .headers(self.build_headers(None)?)
            .query(&[("overwrite", overwrite)])
            .body(body)
            .send()
            .await
            .map_err(|e| generic_err(Box::new(e)))?;

        // Collision under PutMode::Create surfaces as HTTP 409. Translate to AlreadyExists so
        // delta_kernel's optimistic-concurrency loop retries with the next commit version.
        if response.status() == reqwest::StatusCode::CONFLICT {
            return Err(ObjectStoreError::AlreadyExists {
                path: url,
                source: "files_api responded 409 Conflict (PutMode::Create + path exists)".into(),
            });
        }

        let _response = response
            .error_for_status()
            .map_err(|e| generic_err(Box::new(e)))?;

        // FilesApi PUT returns 204 No Content with no headers we need; e_tag/version are
        // not part of the contract today.
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        unimplemented!("we dont use this")
    }

    // You can override the provided methods if needed for optimization
    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        unimplemented!("we dont use this")
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        unimplemented!("Not used");
    }
}

// #[tokio::test]
// async fn test_file_http() {
//     use delta_kernel::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};
//     use std::sync::Arc;

//     let files_client = FilesApiHttpClient::try_new(
//         "https://e2-dogfood.staging.cloud.databricks.com",
//         "",
//     )
//     .unwrap();

//     let object_store: Arc<dyn ObjectStore> = Arc::new(files_client);
//     let default_engine = DefaultEngine::new(object_store,
// Arc::new(TokioBackgroundExecutor::new()));

//     use crate::Snapshot;

//     let path = "/Volumes/jeremy-testing/test-schema/test-volume/test_table/_delta_log";
//     let url = Url::parse(&format!("file://{}", path)).unwrap();

//     let snapshot = Snapshot::try_new(url, &default_engine, None).unwrap();
//     assert_eq!(snapshot.version(), 1);
// }
