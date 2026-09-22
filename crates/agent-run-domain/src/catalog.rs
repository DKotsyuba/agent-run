//! Shared provider catalog, launch authority, and quota admission contracts.
//!
//! This module is the single canonical definition point for the provider
//! orchestration types shared by the quota and session modules. It defines
//! only data contracts and validation: ranking, collection, transactional
//! selection, and attempt lifecycle are owned elsewhere and must consume these
//! types instead of redefining them.
//!
//! Two invariants hold across every type here:
//!
//! * No secret bytes. Credentials are referenced by opaque
//!   [`SecretRef`]/[`SecretHandle`] values that name a storage location; the
//!   bytes themselves never appear in configuration, the database, snapshots,
//!   fixtures, or logs.
//! * No intelligence or role scoring. Model metadata is explicit and textual;
//!   recommendations are plain prose attached by configuration, never an
//!   inferred classification.

use crate::{
    canonical,
    domain::{nonblank, Constraint},
    error::invalid,
    types::{AccountLabel, PositiveFinite, Sha256Digest},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, path::PathBuf, str::FromStr, sync::Arc};

/// Validates an identifier fragment: nonempty, starts alphanumeric, continues
/// with the established `alnum . _ -` grammar, and stays within `max` bytes.
fn identifier(label: &str, value: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || !value.as_bytes()[0].is_ascii_alphanumeric()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err(invalid(format!(
            "{label} must be 1..={max} ASCII characters starting alphanumeric"
        )));
    }
    Ok(())
}

/// The execution harness a provider binds to; exactly the two supported ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HarnessId {
    /// The Codex app-server harness.
    #[serde(rename = "codex")]
    Codex,
    /// The Claude Code harness.
    #[serde(rename = "claude-code")]
    ClaudeCode,
}

impl HarnessId {
    /// Lists every supported harness in stable declaration order.
    pub const ALL: [Self; 2] = [Self::Codex, Self::ClaudeCode];

    /// Returns the stable wire name used in configuration and catalogs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::ClaudeCode => "claude-code",
        }
    }
}

impl fmt::Display for HarnessId {
    /// Writes the stable wire name.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for HarnessId {
    type Err = Error;

    /// Parses the stable wire name, rejecting every other spelling.
    fn from_str(value: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|harness| harness.as_str() == value)
            .ok_or_else(|| invalid("unknown harness; expected codex or claude-code"))
    }
}

/// A configured provider identity; arbitrary within the identifier grammar.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct ProviderId(String);

impl ProviderId {
    /// Returns the validated provider identity as its wire representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    /// Writes the wire representation without exposing any additional state.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ProviderId {
    type Err = Error;

    /// Parses a nonempty provider identifier in the established name grammar.
    fn from_str(value: &str) -> Result<Self> {
        identifier("provider id", value, 64)?;
        Ok(Self(value.into()))
    }
}

impl<'de> Deserialize<'de> for ProviderId {
    /// Deserializes and validates the transparent provider wire value.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// A global, opaque account identity reusable across provider aliases.
///
/// The value is an opaque token: callers must never parse structure out of it.
/// The same [`AccountId`] bound to several providers or labels denotes one
/// physical account whose quota and reservations are shared, never summed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct AccountId(String);

impl AccountId {
    /// Returns the opaque account identity as its wire representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AccountId {
    /// Writes the opaque wire representation.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for AccountId {
    type Err = Error;

    /// Parses an opaque global account id: lowercase-led, `a-z0-9_.-`, at most
    /// 96 bytes. Opaque means no further meaning is derived from the text.
    fn from_str(value: &str) -> Result<Self> {
        if value.is_empty()
            || value.len() > 96
            || !value.as_bytes()[0].is_ascii_lowercase()
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_.-".contains(&b))
        {
            return Err(invalid(
                "account id must be 1..=96 ASCII characters starting lowercase",
            ));
        }
        Ok(Self(value.into()))
    }
}

impl<'de> Deserialize<'de> for AccountId {
    /// Deserializes and validates the transparent account wire value.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// The protocol authentication family an account credential belongs to.
///
/// A provider binding is valid only when the bound account's auth family is
/// equal to the provider's auth family; compatibility is exact match.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct AuthFamily(String);

impl AuthFamily {
    /// Returns the validated auth family name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AuthFamily {
    type Err = Error;

    /// Parses a lowercase auth family name such as `openai` or `anthropic`.
    fn from_str(value: &str) -> Result<Self> {
        if value.is_empty()
            || value.len() > 32
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(invalid(
                "auth family must be 1..=32 lowercase ASCII characters",
            ));
        }
        Ok(Self(value.into()))
    }
}

impl<'de> Deserialize<'de> for AuthFamily {
    /// Deserializes and validates the transparent auth family name.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// A reference naming where a credential lives; never the credential itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct SecretRef(String);

impl SecretRef {
    /// Returns the opaque storage reference, e.g. a keychain label.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for SecretRef {
    type Err = Error;

    /// Parses a nonblank, NUL-free canonical storage reference of at most
    /// 256 bytes, rejecting leading or trailing whitespace.
    fn from_str(value: &str) -> Result<Self> {
        nonblank("secret ref", value)?;
        if value.len() > 256 || value.trim() != value {
            return Err(invalid(
                "secret ref must be canonical and at most 256 bytes",
            ));
        }
        Ok(Self(value.into()))
    }
}

impl<'de> Deserialize<'de> for SecretRef {
    /// Deserializes and validates the transparent secret reference.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// The lifecycle status of a registered global account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    /// Selectable by admission.
    Enabled,
    /// Never selected; existing reservations continue until released.
    Disabled,
}

impl AccountStatus {
    /// Returns the stable wire name stored in the account registry.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

impl FromStr for AccountStatus {
    type Err = Error;

    /// Parses the stable wire name, rejecting every other spelling.
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            _ => Err(invalid("unknown account status")),
        }
    }
}

/// One registered global account: immutable identity plus current status.
///
/// The registry row carries only identity and reference data; observed quota
/// state lives in capacity samples keyed by [`PhysicalQuotaKey`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountRecord {
    /// Global opaque identity reusable across provider aliases.
    pub account_id: AccountId,
    /// Credential family; must equal a binding provider's auth family.
    pub auth_family: AuthFamily,
    /// Names the credential store entry; never contains secret bytes.
    pub secret_ref: SecretRef,
    /// Whether admission may currently select this account.
    pub status: AccountStatus,
}

/// One explicitly configured model offering of a provider.
///
/// Every field is explicit configuration text: ids, textual parameters, and
/// prose recommendations. Nothing is inferred, scored, or classified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderModel {
    /// The model id callers must pass to start; never inferred.
    pub id: String,
    /// The harness-native model to invoke when it differs from `id`.
    #[serde(default)]
    pub native_model: Option<String>,
    /// Textual request parameters such as effort choices and defaults.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// Explicit accepted values for each orchestrator-selected parameter.
    #[serde(default)]
    pub allowed_params: BTreeMap<String, Vec<String>>,
    /// Plain-text advisory recommendations; advisory only, never scoring.
    #[serde(default)]
    pub recommendations: Vec<String>,
    /// Explicit preserved hard restrictions; enforced, not inferred.
    #[serde(default)]
    pub restrictions: Vec<Constraint>,
}

impl ProviderModel {
    /// Validates nonblank ids, textual parameter keys, and unique prose-free
    /// invariants; recommendations may be empty.
    pub fn validate(&self) -> Result<()> {
        external_model_id(&self.id)?;
        if let Some(native) = &self.native_model {
            external_model_id(native)?;
        }
        for (key, value) in &self.params {
            nonblank("model param key", key)?;
            nonblank("model param value", value)?;
        }
        for (key, allowed) in &self.allowed_params {
            nonblank("allowed model param", key)?;
            if key.len() > 64
                || allowed.is_empty()
                || allowed.len() > 32
                || allowed.iter().any(|value| value.trim().is_empty())
                || allowed.iter().any(|value| value.len() > 128)
                || allowed
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != allowed.len()
                || self
                    .params
                    .get(key)
                    .is_some_and(|value| !allowed.contains(value))
            {
                return Err(invalid("invalid allowed model parameters or fixed default"));
            }
        }
        if self
            .restrictions
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != self.restrictions.len()
        {
            return Err(invalid("model restrictions must not repeat"));
        }
        Ok(())
    }
}

/// Validates one provider-visible model id: nonblank and at most 256 bytes.
fn external_model_id(value: &str) -> Result<()> {
    nonblank("model id", value)?;
    if value.len() > 256 {
        return Err(invalid("model id exceeds 256 bytes"));
    }
    Ok(())
}

/// The default priority multiplier is exactly one.
fn multiplier_one() -> PositiveFinite {
    PositiveFinite::try_from(1.0).expect("one is positive and finite")
}

/// One provider-local alias binding a global account to a provider.
///
/// `models` is `None` to inherit the provider's full explicit model set, or a
/// subset that must be contained in it. `multiplier` scales this binding's
/// quota score and defaults to one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderBinding {
    /// Provider-local alias label; unique within its provider.
    pub label: AccountLabel,
    /// The global account this alias resolves to.
    pub account: AccountId,
    /// Optional explicit model subset; `None` inherits the provider set.
    #[serde(default)]
    pub models: Option<Vec<String>>,
    /// Positive finite quota score multiplier, defaulting to one.
    #[serde(
        default = "multiplier_one",
        rename = "priority_multiplier",
        alias = "multiplier"
    )]
    pub multiplier: PositiveFinite,
}

pub use crate::provider_connection::{LimitsSource, ProviderConnection, ProviderProtocol};

/// A provider: one harness and connection, explicit models and bindings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDefinition {
    /// Provider identity used by the start API.
    pub id: ProviderId,
    /// The single harness this provider executes through.
    pub harness: HarnessId,
    /// Native login or explicit custom gateway; never an empty sentinel URL.
    pub connection: ProviderConnection,
    /// Required account auth family; bindings must match it exactly.
    pub auth_family: AuthFamily,
    /// Plain-text provider advice, never model scoring.
    #[serde(default)]
    pub recommendations: Vec<String>,
    /// Positive finite provider ranking multiplier, default one.
    #[serde(default = "multiplier_one")]
    pub priority_multiplier: PositiveFinite,
    /// Explicit collector family for quota observations.
    pub limits_source: LimitsSource,
    /// Explicit model offerings; duplicates are rejected.
    pub models: Vec<ProviderModel>,
    /// Account alias bindings; duplicate labels are rejected.
    pub bindings: Vec<ProviderBinding>,
}

impl ProviderDefinition {
    /// Validates self-containment: unique model ids, unique binding labels,
    /// and every declared binding model subset contained in the model set.
    pub fn validate(&self) -> Result<()> {
        if self.models.is_empty() {
            return Err(invalid("provider must offer at least one model"));
        }
        for advice in &self.recommendations {
            nonblank("provider recommendation", advice)?;
        }
        self.connection.validate(self.harness)?;
        if !matches!(
            (self.harness, self.auth_family.as_str()),
            (HarnessId::Codex, "openai") | (HarnessId::ClaudeCode, "anthropic")
        ) {
            return Err(invalid("provider auth family does not match harness"));
        }
        let mut model_ids = std::collections::BTreeSet::new();
        for model in &self.models {
            model.validate()?;
            if !model_ids.insert(model.id.clone()) {
                return Err(invalid("provider models must be unique"));
            }
        }
        let mut labels = std::collections::BTreeSet::new();
        for binding in &self.bindings {
            if !labels.insert(binding.label.as_str()) {
                return Err(invalid("provider binding labels must be unique"));
            }
            if let Some(subset) = &binding.models {
                let mut seen = std::collections::BTreeSet::new();
                for id in subset {
                    if !model_ids.contains(id) {
                        return Err(invalid(
                            "binding model subset must be contained in provider models",
                        ));
                    }
                    if !seen.insert(id.clone()) {
                        return Err(invalid("binding model subset must not repeat a model"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Returns the binding with `label`, or `None` when the alias is absent.
    pub fn binding(&self, label: &str) -> Option<&ProviderBinding> {
        self.bindings.iter().find(|b| b.label.as_str() == label)
    }
}

/// The validated whole catalog: account registry plus provider definitions.
///
/// Construction is the validation boundary: a [`ProviderCatalog`] exists only
/// when every binding references a registered account whose
/// auth family equals the provider's, and no identity repeats. The same
/// [`AccountId`] may appear under several providers or labels; that is the
/// alias mechanism, and it denotes one shared physical account.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProviderCatalog {
    accounts: Vec<AccountRecord>,
    providers: Vec<ProviderDefinition>,
}

impl ProviderCatalog {
    /// Validates and freezes `accounts` and `providers` into a catalog.
    ///
    /// Rejects duplicate account or provider ids, invalid provider
    /// definitions, bindings to unregistered accounts, and bindings whose
    /// auth family differs from the provider's.
    pub fn new(accounts: Vec<AccountRecord>, providers: Vec<ProviderDefinition>) -> Result<Self> {
        let mut registry = BTreeMap::new();
        let mut storage_identities = std::collections::HashSet::new();
        for account in accounts {
            if !storage_identities.insert((account.auth_family.clone(), account.secret_ref.clone()))
            {
                return Err(invalid("credential storage identity must be unique"));
            }
            if registry
                .insert(account.account_id.clone(), account)
                .is_some()
            {
                return Err(invalid("account ids must be unique"));
            }
        }
        let mut provider_ids = std::collections::BTreeSet::new();
        for provider in &providers {
            provider.validate()?;
            if !provider_ids.insert(provider.id.clone()) {
                return Err(invalid("provider ids must be unique"));
            }
            for binding in &provider.bindings {
                let Some(account) = registry.get(&binding.account) else {
                    return Err(invalid("binding references an unregistered account"));
                };
                if account.auth_family != provider.auth_family {
                    return Err(invalid(
                        "binding auth family must equal the provider auth family",
                    ));
                }
            }
        }
        Ok(Self {
            accounts: registry.into_values().collect(),
            providers,
        })
    }

    /// Returns the registered record for a global account id, if any.
    pub fn account(&self, id: &AccountId) -> Option<&AccountRecord> {
        self.accounts.iter().find(|record| &record.account_id == id)
    }

    /// Returns every provider definition, in declaration order.
    pub fn providers(&self) -> &[ProviderDefinition] {
        &self.providers
    }

    /// Returns the provider definition with `id`, if any.
    pub fn provider(&self, id: &ProviderId) -> Option<&ProviderDefinition> {
        self.providers.iter().find(|p| p.id == *id)
    }

    /// Lists every alias label bound to `account` as `(provider, label)`.
    ///
    /// Multiple results for one account are the aliases that share its single
    /// physical quota pool; capacity is never summed across them.
    pub fn aliases_of(&self, account: &AccountId) -> Vec<(&ProviderDefinition, &str)> {
        self.providers
            .iter()
            .flat_map(|provider| {
                provider
                    .bindings
                    .iter()
                    .filter(move |b| &b.account == account)
                    .map(move |b| (provider, b.label.as_str()))
            })
            .collect()
    }
}

impl<'de> Deserialize<'de> for ProviderCatalog {
    /// Reconstructs a catalog through the same checked boundary as `new`.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        /// Raw wire shape; checked construction follows deserialization.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            accounts: Vec<AccountRecord>,
            providers: Vec<ProviderDefinition>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.accounts, wire.providers).map_err(serde::de::Error::custom)
    }
}

/// The provider-independent identity of one physical quota pool.
///
/// The key is the global account id plus the physical lane (the provider-
/// independent model mapping). Provider aliases of the same account produce
/// the same key, so one pool backs them all.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct PhysicalQuotaKey(String);

impl PhysicalQuotaKey {
    /// Builds the shared pool identity for `account` on physical `lane`.
    ///
    /// `lane` must be nonblank; it is the provider-independent model mapping
    /// owned by the quota side, not a provider alias.
    pub fn new(account: &AccountId, lane: &str) -> Result<Self> {
        nonblank("quota lane", lane)?;
        if lane.len() > 128 {
            return Err(invalid("quota lane exceeds 128 bytes"));
        }
        Ok(Self(format!("{}::{}", account.as_str(), lane)))
    }

    /// Returns the canonical stored key text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this key names a lane owned by the given global account.
    pub fn belongs_to(&self, account: &AccountId) -> bool {
        self.0
            .strip_prefix(account.as_str())
            .and_then(|rest| rest.strip_prefix("::"))
            .is_some_and(|lane| !lane.is_empty() && lane.len() <= 128 && !lane.contains('\0'))
    }
}

impl<'de> Deserialize<'de> for PhysicalQuotaKey {
    /// Validates account and lane syntax when a physical key crosses a wire.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let (account, lane) = text
            .split_once("::")
            .ok_or_else(|| serde::de::Error::custom("quota key lacks account and lane"))?;
        let account = AccountId::from_str(account).map_err(serde::de::Error::custom)?;
        Self::new(&account, lane).map_err(serde::de::Error::custom)
    }
}

pub use crate::legacy_provider::{
    decode_legacy_request, legacy_runtime, DecodedLegacyRequest, LegacyRuntime,
};
pub use crate::quota_snapshot::{
    NormalizedQuotaSnapshot, QuotaModelObservation, QuotaPoolObservation, QuotaWindow,
};

/// The immutable launch authority frozen at admission, before any attempt.
///
/// Everything a launch must not change across retries lives here: provider,
/// harness, connection, explicit model and effort, role profile, workdir,
/// a sealed operative role document, the tool-assets digest, and the frozen eligible
/// account scope. Raw harness-native settings can never override these owned
/// fields. Per-attempt choices deliberately do not appear here; they belong to
/// [`AttemptCredentials`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedLaunchAuthority {
    /// Provider whose catalog entry froze this authority.
    pub provider: ProviderId,
    /// Harness the provider binds to.
    pub harness: HarnessId,
    /// Native or custom connection captured from the provider definition.
    pub connection: ProviderConnection,
    /// The explicit model id requested and validated against the catalog.
    pub model: String,
    /// Optional textual effort, validated against model parameters.
    pub effort: Option<String>,
    /// Role profile name governing grants and body.
    pub profile: String,
    /// Canonical absolute working directory.
    pub workdir: PathBuf,
    /// Canonical `ResolvedRolePlan::to_payload()` from the admitted profile:
    /// prompt, write/network/read grants, skills, MCP tools, and required
    /// constraints. Its initial auth reference is historical: consumers
    /// validate the role, then bind current credentials from the attempt lease.
    pub role_payload: serde_json::Value,
    /// Digest of immutable tool assets; admission must seal them before launch.
    pub assets_sha256: Sha256Digest,
    /// Accounts eligible at freeze time; later attempts intersect this scope
    /// with the currently registered and enabled bindings.
    pub eligible_accounts: Vec<AccountId>,
}

impl ResolvedLaunchAuthority {
    /// Validates the sealed role revision, basic scope, and unique eligible
    /// accounts. Consumers also reconstruct the config role plan and verify
    /// the asset digest against bytes before each launch or retry.
    pub fn validate(&self) -> Result<()> {
        nonblank("model", &self.model)?;
        if let Some(effort) = &self.effort {
            nonblank("effort", effort)?;
        }
        nonblank("profile", &self.profile)?;
        let mut role = self.role_payload.clone();
        let revision = role
            .as_object_mut()
            .and_then(|object| object.remove("config_revision"))
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(|| invalid("authority role payload lacks a revision"))?;
        if role["role_name"] != self.profile || canonical::sha256_hex(&role, true) != revision {
            return Err(invalid(
                "authority role payload does not match its sealed revision",
            ));
        }
        if !self.workdir.is_absolute() {
            return Err(invalid("authority workdir must be absolute"));
        }
        if self.workdir.to_string_lossy().contains('\0') {
            return Err(invalid("authority workdir must be NUL-free"));
        }
        let mut seen = std::collections::BTreeSet::new();
        if self
            .eligible_accounts
            .iter()
            .any(|a| !seen.insert(a.clone()))
        {
            return Err(invalid("eligible accounts must not repeat"));
        }
        Ok(())
    }
}

/// A live credential lease named by reference; deliberately not serializable.
///
/// The handle carries no secret bytes and implements no serde trait, so it
/// cannot cross a wire, enter configuration, or be logged by serialization.
/// The resolver mints handles only through a selected, in-scope catalog record.
#[derive(Debug, Clone)]
pub struct SecretHandle {
    reference: Arc<str>,
}

impl SecretHandle {
    /// Returns the storage reference this handle was minted from.
    pub fn reference(&self) -> &str {
        &self.reference
    }
}

/// The mutable per-attempt credential lease, separate from the authority.
///
/// Each attempt selects exactly one global account and holds its credential
/// lease for the attempt's lifetime. Only this selection may change between
/// attempts of one logical agent; the [`ResolvedLaunchAuthority`] may not.
#[derive(Debug, Clone)]
pub struct AttemptCredentials {
    /// The global account selected for this attempt.
    account: AccountId,
    /// The live credential lease; not serializable by construction.
    secret: SecretHandle,
}

impl AttemptCredentials {
    /// Leases the selected enabled account only when its provider offers the
    /// explicit model through a binding. Returns a validation error otherwise.
    pub fn from_selected(
        catalog: &ProviderCatalog,
        provider: &ProviderId,
        model: &str,
        account: &AccountId,
    ) -> Result<Self> {
        let definition = catalog
            .provider(provider)
            .ok_or_else(|| invalid("unknown provider"))?;
        if !definition
            .models
            .iter()
            .any(|offering| offering.id == model)
            || !definition.bindings.iter().any(|binding| {
                &binding.account == account
                    && binding
                        .models
                        .as_ref()
                        .is_none_or(|models| models.iter().any(|id| id == model))
            })
        {
            return Err(invalid("selected account is outside provider model scope"));
        }
        let record = catalog
            .account(account)
            .ok_or_else(|| invalid("unknown account"))?;
        if record.status != AccountStatus::Enabled || record.auth_family != definition.auth_family {
            return Err(invalid("selected account is not enabled for provider"));
        }
        Ok(Self {
            account: account.clone(),
            secret: SecretHandle {
                reference: Arc::from(record.secret_ref.as_str()),
            },
        })
    }

    /// Returns the account bound to this immutable lease.
    pub fn account(&self) -> &AccountId {
        &self.account
    }

    /// Returns the nonsecret credential storage reference bound to this lease.
    pub fn secret(&self) -> &SecretHandle {
        &self.secret
    }
}

/// Whether selection is automatic or pinned to one requested account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionIntent {
    /// Choose among eligible candidates by quota admission.
    Auto,
    /// Use exactly the requested account; pinning disables failover.
    Pinned(AccountId),
}

/// One ordered candidate account in a quota candidate set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaCandidate {
    /// The global candidate account.
    pub account: AccountId,
    /// Exact physical quota pools this model/attempt must reserve together.
    pub physical_keys: Vec<PhysicalQuotaKey>,
    /// The candidate's effective positive finite multiplier.
    pub multiplier: PositiveFinite,
}

/// The immutable, ordered candidate set quota hands to transactional admission.
///
/// Produced read-only by the quota side from physical capacity data; it never
/// reserves anything. `capacity_revision` is the committed revision the
/// ordering was computed against; admission must re-validate it in-transaction
/// and return [`QuotaAdmissionError::SelectionStale`] when it moved.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaCandidateSet {
    /// The provider the request named.
    pub provider: ProviderId,
    /// The explicit model the request named.
    pub model: String,
    /// The request's auto or pinned intent, preserved for replay.
    pub intent: SelectionIntent,
    /// Candidates in quota's deterministic order; first is preferred.
    pub candidates: Vec<QuotaCandidate>,
    /// The committed capacity revision this ordering is based on.
    pub capacity_revision: i64,
}

impl QuotaCandidateSet {
    /// Validates nonblank model, distinct accounts and host-bound physical
    /// key sets, and the pinned account's membership.
    pub fn validate(&self) -> Result<()> {
        nonblank("model", &self.model)?;
        let mut seen = std::collections::BTreeSet::new();
        for candidate in &self.candidates {
            if !seen.insert(candidate.account.clone()) {
                return Err(invalid("quota candidates must not repeat an account"));
            }
            let mut keys = std::collections::BTreeSet::new();
            if candidate.physical_keys.is_empty()
                || candidate.physical_keys.len() > 32
                || candidate
                    .physical_keys
                    .iter()
                    .any(|key| !key.belongs_to(&candidate.account) || !keys.insert(key))
            {
                return Err(invalid(
                    "candidate physical keys must be distinct and account-bound",
                ));
            }
        }
        if let SelectionIntent::Pinned(account) = &self.intent {
            if !seen.contains(account) {
                return Err(invalid("pinned account must appear among the candidates"));
            }
        }
        if self.capacity_revision < 0 {
            return Err(invalid("capacity revision must be nonnegative"));
        }
        Ok(())
    }
}

/// The typed verdicts transactional admission can return instead of admitting.
///
/// These are admission outcomes, not provider exhaustion observations, except
/// [`QuotaExhausted`] which repeats an authoritative structured exhaustion
/// fact. None of them may masquerade as another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaAdmissionError {
    /// The committed capacity revision moved before admission; quota must
    /// recompute outside the transaction and resubmit.
    SelectionStale {
        /// Revision the candidate set was computed against.
        committed_capacity_revision: i64,
        /// Revision the store held at validation time.
        current_capacity_revision: i64,
    },
    /// The stale-retry budget was exhausted without an admission attempt;
    /// store contention, never provider exhaustion.
    SelectionBusy {
        /// How many stale retries were attempted before giving up (at most 3).
        stale_retries: u32,
    },
    /// No candidate account is currently registered, enabled, and eligible
    /// for the requested provider/model scope.
    NoEligibleAccount {
        /// The provider that was asked.
        provider: ProviderId,
        /// The explicit model that was asked for.
        model: String,
    },
    /// An authoritative structured provider quota exhaustion blocks the
    /// remaining candidates; generic 429/timeout/auth errors never produce
    /// this verdict.
    QuotaExhausted {
        /// The provider that was asked.
        provider: ProviderId,
        /// The explicit model that was asked for.
        model: String,
        /// The last candidate the exhaustion fact is attached to.
        account: AccountId,
    },
}

impl QuotaAdmissionError {
    /// Returns the stable machine-readable kind name.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SelectionStale { .. } => "selection_stale",
            Self::SelectionBusy { .. } => "selection_busy",
            Self::NoEligibleAccount { .. } => "no_eligible_account",
            Self::QuotaExhausted { .. } => "quota_exhausted",
        }
    }
}

impl fmt::Display for QuotaAdmissionError {
    /// Writes the kind plus the discriminating fields.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SelectionStale {
                committed_capacity_revision,
                current_capacity_revision,
            } => write!(
                formatter,
                "selection_stale (committed revision {committed_capacity_revision}, \
                 current revision {current_capacity_revision})"
            ),
            Self::SelectionBusy { stale_retries } => {
                write!(
                    formatter,
                    "selection_busy (after {stale_retries} stale retries)"
                )
            }
            Self::NoEligibleAccount { provider, model } => write!(
                formatter,
                "no_eligible_account (provider {provider}, model {model})"
            ),
            Self::QuotaExhausted {
                provider,
                model,
                account,
            } => write!(
                formatter,
                "quota_exhausted (provider {provider}, model {model}, account {account})"
            ),
        }
    }
}

impl std::error::Error for QuotaAdmissionError {}
