"""API-key authentication and role-based authorisation (framework independent, so it is fully unit tested)."""
from __future__ import annotations

import hashlib
import hmac
import secrets

PERMISSIONS = {"viewer": {"read"}, "agent": {"ingest"}, "admin": {"read", "ingest", "admin"}}
HASH_PREFIX = "sha256$"


class AuthError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status, self.message = status, message


def hash_key(raw: str) -> str:
    """Config-file form of a key. Store this in CLOUDPULSE_API_KEYS so a leaked config does not leak credentials."""
    return HASH_PREFIX + hashlib.sha256(raw.encode()).hexdigest()


def generate_key() -> str:
    return secrets.token_hex(24)


class Authenticator:
    def __init__(self, api_keys: dict[str, str] | None, disabled: bool = False):
        self.keys, self.disabled = dict(api_keys or {}), disabled

    def authorize(self, presented: str | None, permission: str | None = None) -> str:
        """Return the caller's role, or raise AuthError(401 | 403)."""
        if self.disabled:
            return "admin"
        if not presented:
            raise AuthError(401, "Missing X-API-Key header")
        digest = hashlib.sha256(presented.encode()).hexdigest()
        role = None
        for stored, r in self.keys.items():  # check every key: no early exit, constant-time comparisons
            if stored.startswith(HASH_PREFIX):
                match = hmac.compare_digest(stored[len(HASH_PREFIX):].lower(), digest)
            else:
                match = hmac.compare_digest(stored.encode(), presented.encode())
            if match:
                role = r
        if role is None:
            raise AuthError(401, "Invalid API key")
        if permission and permission not in PERMISSIONS[role]:
            raise AuthError(403, f"Role '{role}' lacks '{permission}' permission")
        return role
