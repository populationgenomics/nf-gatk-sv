"""Cached ID-token provider for the Cromwell audience (design §7.3)."""

import base64
import json
import logging
import threading
import time
from collections.abc import Callable

from cpg_cromwell.errors import TransientError

logger = logging.getLogger(__name__)

REFRESH_MARGIN_SECONDS = 300
FALLBACK_TTL_SECONDS = 50 * 60  # used if a token has no readable `exp`


def _default_minter(audience: str) -> Callable[[], str]:
    def mint() -> str:
        from cpg_utils.cloud import get_google_identity_token  # noqa: PLC0415  (keeps import light)

        return get_google_identity_token(audience)

    return mint


def jwt_expiry(token: str) -> float | None:
    """Return the `exp` claim of a JWT without verifying it (we only use it for caching)."""
    try:
        payload = token.split('.')[1]
        payload += '=' * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload))['exp'])
    except (IndexError, KeyError, TypeError, ValueError):
        return None


class TokenProvider:
    """
    One per process. Mints an ID token with the ambient credentials (the VM's attached SA) and
    reuses it until about five minutes before it expires. Thread-safe.
    """

    def __init__(
        self,
        audience: str,
        mint: Callable[[], str] | None = None,
        *,
        refresh_margin: float = REFRESH_MARGIN_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._mint = mint or _default_minter(audience)
        self._margin = refresh_margin
        self._clock = clock
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at = 0.0

    def token(self) -> str:
        with self._lock:
            now = self._clock()
            if self._token is None or now >= self._expires_at - self._margin:
                try:
                    token = self._mint()
                except Exception as e:
                    raise TransientError(f'could not mint an ID token for Cromwell: {e}') from e
                if not token:
                    raise TransientError('could not mint an ID token for Cromwell: empty token')
                self._token = token
                self._expires_at = jwt_expiry(token) or now + FALLBACK_TTL_SECONDS
                logger.debug('minted Cromwell ID token, valid until %s', self._expires_at)
            return self._token

    def invalidate(self) -> None:
        with self._lock:
            self._token = None


class StaticTokenProvider(TokenProvider):
    """Always returns the same token (tests, or a token minted elsewhere)."""

    def __init__(self, token: str) -> None:
        super().__init__('', mint=lambda: token)
