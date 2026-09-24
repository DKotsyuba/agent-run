//! Origin-bound HTTP authorization for explicit custom provider accounts.
//!
//! Inference gateways use this capability; external quota commands receive their
//! selected account context separately over a private stdin pipe.

use agent_run_domain::{
    catalog::{
        AccountId, AttemptCredentials, CredentialHeader, ProviderCatalog, ProviderConnection,
        ProviderId,
    },
    error::invalid,
    CredentialRef, Error, Result,
};
use reqwest::{
    header::{HeaderValue, AUTHORIZATION},
    Method, Request, Response,
};
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::str::FromStr;
use url::Url;

/// Resolves one credential only inside Rust at request time. Implementations
/// must not log or persist the returned bytes; authorized child input may carry
/// them over a private pipe.
pub trait CredentialReader {
    /// Returns live credential bytes for an explicit env/file/Keychain
    /// reference, or a safe error when unavailable; native logins stay owned
    /// by their harness and never become exportable tokens here.
    fn read(&self, reference: &CredentialRef) -> Result<String>;
}

/// Reads existing host environment, file, and macOS Keychain stores without
/// copying credential bytes into account metadata or configuration.
pub struct SystemCredentialReader;

impl CredentialReader for SystemCredentialReader {
    /// Reads the selected protected store on demand, with bounded file bytes.
    fn read(&self, reference: &CredentialRef) -> Result<String> {
        let value = match reference {
            CredentialRef::Environment(name) => std::env::var(name).ok(),
            CredentialRef::File(path) => {
                let file = std::fs::File::open(path)
                    .map_err(|_| invalid("credential file is unavailable"))?;
                let metadata = file
                    .metadata()
                    .map_err(|_| invalid("credential file is unavailable"))?;
                if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
                    return Err(invalid("credential file must be private"));
                }
                let mut bytes = Vec::new();
                file.take(64 * 1024 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|_| invalid("credential file is unavailable"))?;
                if bytes.len() > 64 * 1024 {
                    return Err(invalid("credential file is too large"));
                }
                Some(
                    String::from_utf8(bytes)
                        .map_err(|_| invalid("credential file is not UTF-8"))?
                        .trim_end_matches(['\r', '\n'])
                        .to_owned(),
                )
            }
            CredentialRef::Keychain { service, account } => {
                agent_run_platform::keychain::generic_password(account, service)
            }
            CredentialRef::Native(_) | CredentialRef::Named { .. } => {
                return Err(invalid("native login remains owned by the harness"))
            }
        };
        value
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid("credential reference is unavailable"))
    }
}

/// A selected, enabled account bound to one custom gateway origin.
///
/// Construction verifies provider/model/account scope using the catalog's
/// coupled attempt lease. It contains a storage reference, never secret bytes.
pub struct AuthorizedRequest {
    origin: Url,
    reference: CredentialRef,
    header: CredentialHeader,
}

impl AuthorizedRequest {
    /// Binds a selected account to the configured custom endpoint's origin.
    /// Native providers and native-login references cannot mint HTTP tokens.
    pub fn new(
        catalog: &ProviderCatalog,
        provider: &ProviderId,
        model: &str,
        account: &AccountId,
    ) -> Result<Self> {
        let lease = AttemptCredentials::from_selected(catalog, provider, model, account)?;
        let definition = catalog
            .provider(provider)
            .ok_or_else(|| invalid("unknown provider"))?;
        let ProviderConnection::Custom {
            endpoint,
            auth_header,
            ..
        } = &definition.connection
        else {
            return Err(invalid("native provider uses harness-owned authentication"));
        };
        let reference = CredentialRef::from_str(lease.secret().reference())?;
        if matches!(
            reference,
            CredentialRef::Native(_) | CredentialRef::Named { .. }
        ) {
            return Err(invalid(
                "custom provider requires an explicit credential store",
            ));
        }
        Ok(Self {
            origin: Url::parse(endpoint).map_err(|_| invalid("invalid provider endpoint"))?,
            reference,
            header: *auth_header,
        })
    }

    /// Builds a Rust-only request for `url` on exactly the configured
    /// scheme/host/port. A fake reader can verify it without network access.
    /// Callers must never log or serialize the returned request headers.
    pub fn prepare_request(
        &self,
        method: Method,
        url: &str,
        reader: &impl CredentialReader,
    ) -> Result<Request> {
        let target = Url::parse(url).map_err(|_| invalid("invalid authorized request URL"))?;
        if target.origin() != self.origin.origin()
            || !target.username().is_empty()
            || target.password().is_some()
            || target.fragment().is_some()
        {
            return Err(invalid("authorized request leaves the provider origin"));
        }
        let secret = reader.read(&self.reference)?;
        let mut header = match self.header {
            CredentialHeader::Bearer => HeaderValue::from_str(&format!("Bearer {secret}")),
            CredentialHeader::XApiKey => HeaderValue::from_str(&secret),
        }
        .map_err(|_| invalid("credential cannot form an HTTP header"))?;
        header.set_sensitive(true);
        let mut request = Request::new(method, target);
        request.headers_mut().insert(
            match self.header {
                CredentialHeader::Bearer => AUTHORIZATION,
                CredentialHeader::XApiKey => reqwest::header::HeaderName::from_static("x-api-key"),
            },
            header,
        );
        Ok(request)
    }

    /// Sends through a client that never follows redirects, so an origin
    /// change cannot forward the credential. Provider or transport errors
    /// return fixed diagnostics without response bodies or secret values.
    pub async fn send(
        &self,
        method: Method,
        url: &str,
        reader: &impl CredentialReader,
    ) -> Result<Response> {
        let request = self.prepare_request(method, url, reader)?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Runtime("cannot create provider HTTP client".into()))?;
        client
            .execute(request)
            .await
            .map_err(|_| Error::Runtime("provider HTTP request failed".into()))
    }
}
