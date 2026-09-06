"""Static OpenCode generated-config policy for the isolated service."""

from ..base import Capability
from .permissions import EXTERNAL_DIRECTORY


RUNTIME_NAME = "opencode"
VERIFY_AGENT = "agent-run-verify"
ANSWER_NAME = "answer.md"
DEFAULT_WAIT_SECONDS = 480.0
CAPABILITIES = frozenset({
    Capability.STEER, Capability.OUTPUT_SCHEMA, Capability.READ_ROOTS,
    Capability.TRANSCRIPT, Capability.MODEL_ROSTER, Capability.LIVE_LIMITS,
    Capability.MCP, Capability.SKILLS,
})
#: Every action v1's own ``PermissionConfig`` names, stated explicitly.
#:
#: An action left out of this table is not "default"; v1 1.18.18 treats it as
#: ``ask`` (proven live: with only bash/edit/write/webfetch/external_directory
#: set, a plain ``read`` of a file *inside* the session directory raised a
#: pending permission, and so did ``glob`` and ``todowrite``). That is what
#: made the runtime unusable: an unanswered ask blocks the tool, and one whose
#: metadata carries an undefined property also 400s the whole permission list
#: (see ``OpenCodeRuntimeSession.resolve_permissions``). So the read-only tools
#: this read-and-answer runtime actually needs are allowed outright, and
#: everything that writes or reaches the network is denied outright -- neither
#: can produce a pending ask.
READ_ONLY_ALLOW: tuple[tuple[str, str], ...] = (
    ("read", "allow"), ("list", "allow"), ("glob", "allow"),
    ("grep", "allow"), ("lsp", "allow"), ("skill", "allow"),
    ("todowrite", "allow"), ("task", "allow"),
)
DENY: tuple[tuple[str, str], ...] = (
    ("bash", "deny"), ("edit", "deny"), ("write", "deny"),
    ("webfetch", "deny"), ("websearch", "deny"), ("question", "deny"),
    ("doom_loop", "deny"),
)
#: Ordered on purpose: the contained one-time grant is the last word, so no
#: earlier entry can widen it and no later entry can shadow it.
SYSTEM_PERMISSION: tuple[tuple[str, str], ...] = (*DENY, *READ_ONLY_ALLOW, (EXTERNAL_DIRECTORY, "ask"))
PRIMARY_PERMISSION = SYSTEM_PERMISSION
#: A sub-agent never gets even the contained grant; only the primary may ask.
#: This is the only enforcement there is: a v1 ``PermissionV2Request`` carries
#: no agent field, so the broker cannot tell a sub-agent's ask apart.
SUBAGENT_PERMISSION: tuple[tuple[str, str], ...] = (*DENY, *READ_ONLY_ALLOW, (EXTERNAL_DIRECTORY, "deny"))
SCHEMA_KEYS = frozenset({
    "$schema", "title", "description", "type", "properties", "required",
    "items", "enum", "additionalProperties",
})
