"""Exhaustive role isolation (CLAUDE.md rule 13): pa-core may call ONLY its own methods; the UI may not call core methods.
Generated from the registry so a new RPC can never be exposed to the wrong role by accident."""
from pa_gateway.ipc import api_core, api_ui  # noqa: F401  (registers the RPCs)
from pa_gateway.ipc.dispatch import REGISTRY


def test_core_role_can_call_only_core_methods(env_nocore):
    env_nocore.setup(model=False)
    core = env_nocore.core()
    ui_only = [n for n, m in REGISTRY.items() if "core" not in m.roles]
    assert len(ui_only) > 100
    for name in ui_only:
        res = core.raw(name, {})
        assert not res["ok"] and res["error"]["code"] == "method_not_allowed", name


def test_ui_role_cannot_call_core_only_methods(env_nocore):
    env_nocore.setup(model=False)
    core_only = [n for n, m in REGISTRY.items() if "ui" not in m.roles]
    assert core_only
    for name in core_only:
        res = env_nocore.ui.raw(name, {})
        assert not res["ok"] and res["error"]["code"] == "method_not_allowed", name


def test_sensitive_methods_are_never_shared_with_core():
    forbidden_words = ("settings.", "secrets.", "approvals.", "killswitch.", "auth.", "account.", "backup.", "connectors.", "privacy.", "skills.activate",
                       "missions.activate", "llm.add", "llm.remove", "llm.set_role", "files.save_copy", "web.test_search")
    for name, m in REGISTRY.items():
        if "core" in m.roles:
            assert not name.startswith(forbidden_words), name
