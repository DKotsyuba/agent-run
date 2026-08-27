"""T055: one skill source per runtime, codex guard hooks, extra MCP servers."""

from __future__ import annotations

import json
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.claude.adapter import ADAPTER as CLAUDE
from agent_run.adapters.codex import plugins as codex_plugins
from agent_run.adapters.codex.adapter import ADAPTER as CODEX
from agent_run.adapters.opencode.adapter import render_config
from agent_run.adapters.plugin_skills import local_skill_names, plugin_skill_dir, skill_dirs
from agent_run.config import McpConfig, RuntimeAuthConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.errors import ValidationError

GUARD = "{plugin:demo-plugin}/hooks/lsp_guard.py"
MATCHER = "^(apply_patch|mcp__agent_lsp__.*)$"


def _plugin(root: Path, name: str, *, skills: tuple[str, ...], hooks: bool = False) -> Path:
    """Build a minimal claude-manifest plugin carrying skills and optional hooks."""

    directory = root / name
    (directory / ".claude-plugin").mkdir(parents=True)
    (directory / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": name, "version": "1.0.0"}), encoding="utf-8"
    )
    for skill in skills:
        (directory / "skills" / skill).mkdir(parents=True)
        (directory / "skills" / skill / "SKILL.md").write_text(f"fresh {skill}", encoding="utf-8")
    (directory / "hooks").mkdir(parents=True)
    (directory / "hooks" / "lsp_guard.py").write_text("print('guard')\n", encoding="utf-8")
    if hooks:
        (directory / "hooks" / "hooks.json").write_text(
            json.dumps(
                {
                    "hooks": {
                        "PreToolUse": [
                            {
                                "matcher": "Edit|Write",
                                "hooks": [
                                    {
                                        "type": "command",
                                        "command": "python3 ${CLAUDE_PLUGIN_ROOT}/hooks/lsp_guard.py",
                                        "timeout": 5,
                                    }
                                ],
                            }
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
    return directory


class PluginSkillSourceTests(unittest.TestCase):
    """A declared plugin owns any skill name it ships; nothing loads it twice."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.plugin = _plugin(self.root, "demo-plugin", skills=("lsp-first", "code-reading"))
        self.stale = self.root / "stale"
        for name in ("lsp-first", "code-reading", "delegate"):
            (self.stale / name).mkdir(parents=True)
            (self.stale / name / "SKILL.md").write_text(f"stale {name}", encoding="utf-8")

    def test_plugin_wins_and_unowned_names_fall_back(self) -> None:
        resolved = skill_dirs(
            (self.plugin,), self.stale, ("delegate", "lsp-first", "code-reading")
        )
        self.assertEqual(resolved["lsp-first"], self.plugin / "skills" / "lsp-first")
        self.assertEqual(resolved["code-reading"], self.plugin / "skills" / "code-reading")
        self.assertEqual(resolved["delegate"], self.stale / "delegate")
        # Selection order is preserved so a host reading a path list keeps it.
        self.assertEqual(list(resolved), ["delegate", "lsp-first", "code-reading"])

    def test_local_names_exclude_plugin_owned_skills(self) -> None:
        self.assertEqual(
            local_skill_names((self.plugin,), ("delegate", "lsp-first", "code-reading")),
            ("delegate",),
        )
        self.assertEqual(local_skill_names((), ("delegate", "lsp-first")), ("delegate", "lsp-first"))

    def test_two_plugins_claiming_one_name_fail_closed(self) -> None:
        other = _plugin(self.root, "other-plugin", skills=("lsp-first",))
        with self.assertRaises(ValidationError) as caught:
            plugin_skill_dir((self.plugin, other), "lsp-first")
        self.assertIn("shipped by two declared plugins", str(caught.exception))

    def test_claude_does_not_project_a_plugin_owned_skill(self) -> None:
        home = self.root / "claude-home"
        home.mkdir()
        config = RuntimeConfig(
            enabled=True,
            adapter="a:b",
            binary=Path("/bin/echo"),
            home=home,
            models=("sonnet",),
            skills=("delegate", "lsp-first", "code-reading"),
            auth=RuntimeAuthConfig("environment", names=("CLAUDE_CODE_OAUTH_TOKEN",)),
            plugins=(self.plugin,),
        )
        CLAUDE.materialize(config, home, mcp_servers={}, skills_root=self.stale)
        # The plugin already exports these two; a generated plugin dir of the
        # same name would make the child see each skill twice.
        self.assertTrue((home / "plugins" / "delegate").is_dir())
        self.assertFalse((home / "plugins" / "lsp-first").exists())
        self.assertFalse((home / "plugins" / "code-reading").exists())

    def test_opencode_skill_paths_point_at_the_plugin_copy(self) -> None:
        config = RuntimeConfig(
            enabled=True,
            adapter="a:b",
            binary=Path("/bin/echo"),
            home=self.root,
            models=("omniroute/demo",),
            skills=("delegate", "lsp-first"),
            plugins=(self.plugin,),
        )
        document = json.loads(
            render_config(config, {}, skills_root=self.stale, inherited_environment={})
        )
        self.assertEqual(
            document["skills"]["paths"],
            [str(self.stale / "delegate"), str(self.plugin / "skills" / "lsp-first")],
        )


class CodexGuardHookTests(unittest.TestCase):
    """Codex runs a config-level hook only with a matching trust digest."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.plugin = _plugin(self.root, "demo-plugin", skills=("lsp-first",), hooks=True)
        self.home = self.root / "codex-home"
        self.home.mkdir()
        self.auth = self.root / "auth.json"
        self.auth.write_text("{}", encoding="utf-8")
        self.stale = self.root / "stale"
        (self.stale / "delegate").mkdir(parents=True)
        (self.stale / "delegate" / "SKILL.md").write_text("stale delegate", encoding="utf-8")

    def _config(self, **overrides: object) -> RuntimeConfig:
        base: dict[str, object] = {
            "enabled": True,
            "adapter": "a:b",
            "binary": Path("/bin/echo"),
            "home": self.home,
            "models": ("gpt",),
            "skills": ("delegate", "lsp-first"),
            "auth": RuntimeAuthConfig("file_link", self.auth, "auth.json"),
            "hooks": (
                RuntimeHookConfig(
                    "PreToolUse",
                    ("/usr/bin/python3", GUARD, "--mode=strict", "--writer=mcp"),
                    matcher=MATCHER,
                ),
                RuntimeHookConfig(
                    "PostToolUse",
                    ("/usr/bin/python3", GUARD, "--mode=strict", "--writer=mcp"),
                    matcher=MATCHER,
                ),
            ),
            "plugins": (self.plugin,),
        }
        base.update(overrides)
        return RuntimeConfig(**base)  # type: ignore[arg-type]

    def _generated(self, **overrides: object) -> dict:
        CODEX.materialize(
            self._config(**overrides), self.home, mcp_servers={}, skills_root=self.stale
        )
        return tomllib.loads((self.home / "config.toml").read_text(encoding="utf-8"))

    def test_hook_groups_carry_a_trust_entry_keyed_by_the_config_path(self) -> None:
        hooks = self._generated()["hooks"]
        self.assertEqual(hooks["PreToolUse"][0]["matcher"], MATCHER)
        handler = hooks["PreToolUse"][0]["hooks"][0]
        self.assertEqual(handler["type"], "command")
        # The written timeout is the one that was hashed; leaving it implicit
        # would trust a digest codex may normalize differently.
        self.assertEqual(handler["timeout"], codex_plugins.DEFAULT_TIMEOUT_SEC)
        config_path = self.home / "config.toml"
        for label in ("pre_tool_use", "post_tool_use"):
            key = f"{config_path}:{label}:0:0"
            self.assertIn(key, hooks["state"])
            self.assertTrue(hooks["state"][key]["trusted_hash"].startswith("sha256:"))

    def test_guard_command_resolves_to_the_copy_inside_the_home(self) -> None:
        document = self._generated()
        command = document["hooks"]["PreToolUse"][0]["hooks"][0]["command"]
        installed = self.home / "plugins/cache/personal/demo-plugin/1.0.0/hooks/lsp_guard.py"
        self.assertIn(str(installed), command)
        self.assertNotIn(str(self.plugin), command)
        # Self-contained: a real file below the home, never a bridge outward.
        self.assertTrue(installed.is_file())
        self.assertFalse(installed.is_symlink())

    def test_trusted_hash_tracks_the_command(self) -> None:
        config_path = self.home / "config.toml"
        key = f"{config_path}:pre_tool_use:0:0"
        before = self._generated()["hooks"]["state"][key]["trusted_hash"]
        changed = self._generated(
            hooks=(
                RuntimeHookConfig(
                    "PreToolUse",
                    ("/usr/bin/python3", GUARD, "--mode=nudge", "--writer=mcp"),
                    matcher=MATCHER,
                ),
            )
        )
        self.assertNotEqual(changed["hooks"]["state"][key]["trusted_hash"], before)

    def test_unknown_plugin_reference_fails_closed(self) -> None:
        with self.assertRaises(ValidationError) as caught:
            CODEX.materialize(
                self._config(
                    hooks=(
                        RuntimeHookConfig(
                            "PreToolUse",
                            ("/usr/bin/python3", "{plugin:absent}/hooks/lsp_guard.py"),
                            matcher=MATCHER,
                        ),
                    )
                ),
                self.home,
                mcp_servers={},
                skills_root=self.stale,
            )
        self.assertIn("not a declared plugin", str(caught.exception))

    def test_unsupported_hook_event_fails_closed(self) -> None:
        with self.assertRaises(ValidationError):
            codex_plugins.hook_trust(self.home / "config.toml", "NotAnEvent", 0, None, "true")


class ExtraMcpServerTests(unittest.TestCase):
    """A second declared MCP server reaches every runtime's generated config."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.servers = {
            "agent_lsp": McpConfig("stdio", Path("/bin/echo"), ("lsp",)),
            "codegraph": McpConfig("stdio", Path("/bin/echo"), ("serve", "--mcp")),
        }
        self.skills = self.root / "skills"
        (self.skills / "delegate").mkdir(parents=True)
        (self.skills / "delegate" / "SKILL.md").write_text("delegate", encoding="utf-8")

    def test_codex_config_lists_both_servers(self) -> None:
        home = self.root / "codex"
        home.mkdir()
        auth = self.root / "auth.json"
        auth.write_text("{}", encoding="utf-8")
        config = RuntimeConfig(
            enabled=True,
            adapter="a:b",
            binary=Path("/bin/echo"),
            home=home,
            models=("gpt",),
            skills=("delegate",),
            mcp=("agent_lsp", "codegraph"),
            auth=RuntimeAuthConfig("file_link", auth, "auth.json"),
        )
        CODEX.materialize(config, home, mcp_servers=self.servers, skills_root=self.skills)
        document = tomllib.loads((home / "config.toml").read_text(encoding="utf-8"))
        self.assertEqual(sorted(document["mcp_servers"]), ["agent_lsp", "codegraph"])
        self.assertEqual(document["mcp_servers"]["codegraph"]["args"], ["serve", "--mcp"])

    def test_claude_mcp_config_lists_both_servers(self) -> None:
        home = self.root / "claude"
        home.mkdir()
        config = RuntimeConfig(
            enabled=True,
            adapter="a:b",
            binary=Path("/bin/echo"),
            home=home,
            models=("sonnet",),
            skills=("delegate",),
            mcp=("agent_lsp", "codegraph"),
            auth=RuntimeAuthConfig("environment", names=("CLAUDE_CODE_OAUTH_TOKEN",)),
        )
        CLAUDE.materialize(config, home, mcp_servers=self.servers, skills_root=self.skills)
        document = json.loads((home / "mcp" / "mcp-config.json").read_text(encoding="utf-8"))
        self.assertEqual(sorted(document["mcpServers"]), ["agent_lsp", "codegraph"])
        self.assertEqual(document["mcpServers"]["codegraph"]["args"], ["serve", "--mcp"])

    def test_opencode_config_lists_both_servers(self) -> None:
        config = RuntimeConfig(
            enabled=True,
            adapter="a:b",
            binary=Path("/bin/echo"),
            home=self.root,
            models=("omniroute/demo",),
            skills=("delegate",),
            mcp=("agent_lsp", "codegraph"),
        )
        document = json.loads(
            render_config(config, self.servers, skills_root=self.skills, inherited_environment={})
        )
        self.assertEqual(sorted(document["mcp"]), ["agent_lsp", "codegraph"])
        self.assertEqual(
            document["mcp"]["codegraph"]["command"], ["/bin/echo", "serve", "--mcp"]
        )


if __name__ == "__main__":
    unittest.main()
