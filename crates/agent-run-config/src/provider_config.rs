//! Strict v2 provider configuration beside the still-running v1 consumer path.
//!
//! The loader owns no cache: callers retain their last valid value when
//! `load_if_changed` rejects a new file revision.

use crate::config::{
    self, Capacity, Catalog, Config, Core, Delivery, Environment, Hook, Mcp, RustRoots,
};
use agent_run_domain::{
    canonical,
    catalog::{
        AccountRecord, AuthFamily, CollectorBinding, HarnessId, LimitsSource, ProviderBinding,
        ProviderCatalog, ProviderConnection, ProviderDefinition, ProviderId, ProviderModel,
    },
    error::invalid,
    types::PositiveFinite,
    Result,
};
use agent_run_platform::fs;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Stable native launch settings for one supported harness, independent of
/// provider aliases, account selection, and canonical role grants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessConfig {
    /// Existing native CLI executable.
    pub binary: PathBuf,
    /// Native state root; per-run homes are derived by the session consumer.
    pub home: PathBuf,
    /// Optional harness-wide active process cap.
    #[serde(default)]
    pub max_active_agents: Option<usize>,
    /// Unowned native settings subject to existing reserved-key checks.
    #[serde(default)]
    pub native_settings: BTreeMap<String, toml::Value>,
    /// Existing adapter hook declarations.
    #[serde(default)]
    pub hooks: Vec<Hook>,
    /// Existing plugin roots.
    #[serde(default)]
    pub plugins: Vec<PathBuf>,
    /// Explicit plugin asset allowlists.
    #[serde(default)]
    pub plugin_snapshot_assets: BTreeMap<String, Vec<String>>,
    /// Operator-authorized Codex workspace roots.
    #[serde(default)]
    pub workspace_roots: Vec<PathBuf>,
    /// Enables network only with Codex workspace roots.
    #[serde(default)]
    pub workspace_network: bool,
    /// Existing environment name, not an inline credential.
    #[serde(default)]
    pub environment: Option<String>,
    /// Existing Rust toolchain paths for materialization.
    #[serde(default)]
    pub rust: Option<RustRoots>,
}

/// TOML-owned provider metadata; account identities are resolved separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSettings {
    /// Exactly one configured execution harness.
    pub harness: HarnessId,
    /// Native login or explicit custom HTTP gateway.
    pub connection: ProviderConnection,
    /// Protocol credential family, matched against harness and accounts.
    pub auth_family: AuthFamily,
    /// Explicit model offerings and native model aliases.
    pub models: Vec<ProviderModel>,
    /// Provider-local labels referring to global account identities.
    pub bindings: Vec<ProviderBinding>,
    /// Plain-text provider advice, never a model classification.
    #[serde(default)]
    pub recommendations: Vec<String>,
    /// Positive provider ranking weight, default one.
    #[serde(default = "one")]
    pub priority_multiplier: PositiveFinite,
    /// Explicit collector selection for capacity observations.
    pub limits_source: LimitsSource,
    /// Explicit first-party collector binding; required for the Lua source.
    #[serde(default)]
    pub collector: Option<CollectorBinding>,
}

/// Returns the validated default ranking multiplier.
fn one() -> PositiveFinite {
    PositiveFinite::try_from(1.0).expect("one is positive")
}

/// Parsed v2 configuration with two stable harnesses and arbitrary providers.
///
/// The v1 `Config` remains the current consumer contract until its launch
/// paths cut over. This type owns no mutable configuration cache or secrets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// Must be exactly two.
    pub schema_version: u32,
    /// Existing service lifecycle controls.
    #[serde(default)]
    pub core: Core,
    /// Existing collection controls.
    #[serde(default)]
    pub capacity: Capacity,
    /// Existing notification controls.
    #[serde(default)]
    pub delivery: Delivery,
    /// Canonical role catalog root.
    #[serde(default)]
    pub profiles: Catalog,
    /// Canonical skill catalog root.
    #[serde(default)]
    pub skills: Catalog,
    /// Role-accessible MCP definitions.
    #[serde(default)]
    pub mcp: BTreeMap<String, Mcp>,
    /// Named nonsecret environment defaults.
    #[serde(default)]
    pub environments: BTreeMap<String, Environment>,
    /// Exactly the Codex and Claude Code native harness declarations.
    pub harnesses: BTreeMap<HarnessId, HarnessConfig>,
    /// Arbitrary named providers, resolved against the account registry.
    pub providers: BTreeMap<ProviderId, ProviderSettings>,
}

impl ProviderConfig {
    /// Returns the validated canonical profile directory.
    pub fn profiles_dir(&self) -> &Path {
        self.profiles
            .directory
            .as_deref()
            .expect("validated provider config")
    }

    /// Returns the validated canonical skill directory.
    pub fn skills_dir(&self) -> &Path {
        self.skills
            .directory
            .as_deref()
            .expect("validated provider config")
    }

    /// Parses and validates v2 TOML without writes or account-store access.
    ///
    /// `home` supplies defaults for existing role/skill catalogs. Cross
    /// references to registered accounts are checked by `resolve_catalog`.
    pub fn parse(text: &str, home: &Path) -> Result<Self> {
        let mut config: Self = toml::from_str(text)
            .map_err(|_| invalid("invalid v2 configuration shape, type, or unknown field"))?;
        config.validate(home)?;
        Ok(config)
    }

    /// Reads at most one MiB of `config.toml`; returns `None` for the same
    /// byte revision and never replaces a caller's last valid config on error.
    pub fn load_if_changed(home: &Path, revision: Option<&str>) -> Result<Option<(Self, String)>> {
        let bytes = fs::Dir::open(home)?.read(Path::new("config.toml"), 1024 * 1024)?;
        let digest = fs::sha256(&bytes);
        if revision == Some(digest.as_str()) {
            return Ok(None);
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| invalid("config must be UTF-8"))?;
        Ok(Some((Self::parse(text, home)?, digest)))
    }

    /// Loads one valid v2 file and its exact-byte revision.
    pub fn load(home: &Path) -> Result<(Self, String)> {
        Self::load_if_changed(home, None)?
            .ok_or_else(|| invalid("configuration unexpectedly matched an absent revision"))
    }

    /// Validates shared v1 controls using their existing rules and checks all
    /// v2 harness and provider declarations without consulting account state.
    pub fn validate(&mut self, home: &Path) -> Result<()> {
        if self.schema_version != 2
            || self.harnesses.len() != 2
            || !self.harnesses.contains_key(&HarnessId::Codex)
            || !self.harnesses.contains_key(&HarnessId::ClaudeCode)
            || self.providers.is_empty()
        {
            return Err(invalid(
                "v2 requires codex and claude-code harnesses and providers",
            ));
        }
        let mut shared = Config {
            schema_version: 1,
            core: self.core.clone(),
            capacity: self.capacity.clone(),
            delivery: self.delivery.clone(),
            profiles: self.profiles.clone(),
            skills: self.skills.clone(),
            mcp: self.mcp.clone(),
            environments: self.environments.clone(),
            runtimes: BTreeMap::new(),
        };
        shared.validate(home)?;
        self.core = shared.core;
        self.capacity = shared.capacity;
        self.delivery = shared.delivery;
        self.profiles = shared.profiles;
        self.skills = shared.skills;
        self.mcp = shared.mcp;
        self.environments = shared.environments;
        for (id, harness) in &mut self.harnesses {
            harness.binary = fs::expand(&harness.binary)?;
            harness.home = fs::expand(&harness.home)?;
            if !harness.binary.is_absolute() || !harness.home.is_absolute() {
                return Err(invalid("harness binary and home must be absolute"));
            }
            if harness.max_active_agents == Some(0) {
                return Err(invalid("harness max_active_agents must be positive"));
            }
            let adapter = match id {
                HarnessId::Codex => config::Adapter::Codex,
                HarnessId::ClaudeCode => config::Adapter::Claude,
            };
            config::native_settings(adapter, &harness.native_settings)?;
            if harness.workspace_network
                && (*id != HarnessId::Codex || harness.workspace_roots.is_empty())
            {
                return Err(invalid("workspace_network requires codex workspace_roots"));
            }
            if !harness.workspace_roots.is_empty() && *id != HarnessId::Codex {
                return Err(invalid("workspace_roots requires codex"));
            }
            for root in &mut harness.workspace_roots {
                *root = fs::expand(root)?;
                if !root.is_absolute() {
                    return Err(invalid("workspace root must be absolute"));
                }
            }
            if harness
                .environment
                .as_ref()
                .is_some_and(|name| !self.environments.contains_key(name))
            {
                return Err(invalid("unknown harness environment"));
            }
            for hook in &harness.hooks {
                if hook.event.trim().is_empty()
                    || hook.command.is_empty()
                    || hook
                        .command
                        .iter()
                        .any(|arg| arg.is_empty() || arg.contains('\0'))
                {
                    return Err(invalid("invalid harness hook"));
                }
            }
            for plugin in &mut harness.plugins {
                *plugin = fs::expand(plugin)?;
                if !plugin.is_dir() {
                    return Err(invalid("plugin must be an existing directory"));
                }
            }
            for (plugin, assets) in &harness.plugin_snapshot_assets {
                if harness
                    .plugins
                    .iter()
                    .filter(|path| path.file_name().is_some_and(|name| name == plugin.as_str()))
                    .count()
                    != 1
                    || assets.is_empty()
                    || assets.iter().any(|asset| {
                        fs::relative(Path::new(asset)).is_err()
                            || asset.chars().any(|c| "*?[]{}".contains(c))
                    })
                    || assets
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        != assets.len()
                {
                    return Err(invalid("invalid harness plugin assets"));
                }
            }
            if let Some(roots) = &mut harness.rust {
                roots.rustup_home = fs::expand(&roots.rustup_home)?;
                roots.cargo_bin = fs::expand(&roots.cargo_bin)?;
            }
        }
        for (id, provider) in &self.providers {
            if !self.harnesses.contains_key(&provider.harness) {
                return Err(invalid("provider names an undeclared harness"));
            }
            self.definition(id, provider).validate()?;
        }
        Ok(())
    }

    /// Resolves TOML-owned providers against explicit registered account
    /// records, rejecting missing, duplicate, or auth-incompatible bindings.
    pub fn resolve_catalog(&self, accounts: Vec<AccountRecord>) -> Result<ProviderCatalog> {
        ProviderCatalog::new(
            accounts,
            self.providers
                .iter()
                .map(|(id, provider)| self.definition(id, provider))
                .collect(),
        )
    }

    /// Returns a metadata-only snapshot digest and stable configured ids.
    ///
    /// It hashes complete validated settings but never emits environment
    /// values, native arguments, account secret references, or credentials.
    pub fn snapshot(&self) -> Result<Value> {
        let document = serde_json::to_value(self)?;
        Ok(json!({
            "schema_version": 2,
            "sha256": canonical::sha256_hex(&document, true),
            "harnesses": self.harnesses.keys().map(|id| id.as_str()).collect::<Vec<_>>(),
            "providers": self.providers.keys().map(ProviderId::as_str).collect::<Vec<_>>(),
        }))
    }

    /// Builds one public provider definition without reading account state.
    fn definition(&self, id: &ProviderId, provider: &ProviderSettings) -> ProviderDefinition {
        ProviderDefinition {
            id: id.clone(),
            harness: provider.harness,
            connection: provider.connection.clone(),
            auth_family: provider.auth_family.clone(),
            recommendations: provider.recommendations.clone(),
            priority_multiplier: provider.priority_multiplier,
            limits_source: provider.limits_source,
            collector: provider.collector.clone(),
            models: provider.models.clone(),
            bindings: provider.bindings.clone(),
        }
    }
}
