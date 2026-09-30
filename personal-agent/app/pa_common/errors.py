class PAError(Exception):
    """Base error. `code` is a stable machine-readable string shown to the UI."""

    code = "error"

    def __init__(self, message: str = "", code: str | None = None, **details):
        super().__init__(message or self.code)
        if code:
            self.code = code
        self.details = details

    def to_dict(self) -> dict:
        return {"code": self.code, "message": str(self), "details": self.details}


class AuthError(PAError):
    code = "auth_failed"


class LockedError(PAError):
    code = "locked"


class StepUpRequired(PAError):
    code = "step_up_required"


class PolicyDenied(PAError):
    code = "policy_denied"


class NotAllowed(PAError):
    code = "method_not_allowed"


class ValidationError(PAError):
    code = "invalid_request"


class KillSwitchActive(PAError):
    code = "kill_switch_active"


class AuditFailure(PAError):
    code = "audit_failed"


class Unavailable(PAError):
    code = "unavailable"
