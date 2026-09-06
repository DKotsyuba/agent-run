"""Static Claude runtime policy shared by validation and launch preparation."""

from ..base import Capability


CAPABILITIES = frozenset(
    {
        Capability.STEER, Capability.EFFORT, Capability.OUTPUT_SCHEMA,
        Capability.READ_ROOTS, Capability.WRITE, Capability.TRANSCRIPT,
        Capability.MODEL_ROSTER, Capability.LIVE_LIMITS, Capability.MCP,
        Capability.SKILLS, Capability.HOOKS, Capability.RESUME,
    }
)
READ_TOOLS = ("Read", "Grep", "Glob")
SKILL_TOOLS = ("Skill",)
WRITE_TOOLS = ("Edit", "Write", "NotebookEdit")
SHELL_TOOLS = ("Bash",)
NETWORK_TOOLS = ("WebFetch", "WebSearch")
ALWAYS_DISALLOWED = NETWORK_TOOLS
AUTH_NAMES = frozenset({"CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"})
SUPPORTED_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})
# Adapter-owned translation of public model ids to Claude API model ids.
# Only the child argv's ``--model`` value is translated: the request model
# id, the roster, and the persisted ``model`` adapter state all keep the
# configured public id. Ids absent from this mapping pass through verbatim,
# so every other configured model is untouched.
MODEL_ALIASES = {"fable": "claude-fable-5-1"}
# Roster descriptions that name the concrete Claude release behind a public
# id; ids absent from this mapping keep the generic description.
MODEL_DESCRIPTIONS = {"fable": "Claude Fable 5.1 (API model id: claude-fable-5-1)"}
KNOWN_HOOK_EVENTS = frozenset({
    "PreToolUse", "PostToolUse", "UserPromptSubmit", "Stop", "SubagentStop",
    "Notification", "PreCompact", "SessionStart", "SessionEnd",
})
