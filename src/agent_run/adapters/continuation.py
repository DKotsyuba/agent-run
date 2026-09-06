"""Native CLI continuation arguments shared by the Claude and Qwen adapters."""

from dataclasses import replace

from .base import LaunchPlan
from ..errors import ValidationError


def cli_resume_plan(plan: LaunchPlan, *, session_option: str | None = None) -> LaunchPlan:
    """Return a plan targeting its saved native session, retaining all other settings.

    Fresh plans are unchanged. For Claude, ``session_option`` names the fresh-ID
    option to remove; Qwen has none. Missing or malformed identity/argv raises
    ValidationError before spawning. No latest-session selection or fork is used.
    """
    identity = plan.resume_session_id
    if identity is None:
        return plan
    if not isinstance(identity, str) or not identity.strip() or identity.startswith("-"):
        raise ValidationError("invalid native resume session id")
    argv = list(plan.argv)
    if session_option is not None:
        if session_option not in argv or argv.index(session_option) + 1 >= len(argv):
            raise ValidationError("native launch plan has no fresh session option")
        index = argv.index(session_option)
        del argv[index:index + 2]
    argv.extend(("--resume", identity))
    state = dict(plan.adapter_state)
    if "session_id" in state:
        state["session_id"] = identity
    return replace(plan, argv=tuple(argv), adapter_state=state)
