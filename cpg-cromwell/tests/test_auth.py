import base64
import json

import pytest

from cpg_cromwell.auth import TokenProvider, jwt_expiry
from cpg_cromwell.errors import TransientError


def jwt(exp: float) -> str:
    def enc(d: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).decode().rstrip('=')

    return f'{enc({"alg": "none"})}.{enc({"exp": exp})}.sig'


def test_jwt_expiry() -> None:
    assert jwt_expiry(jwt(1234)) == 1234
    assert jwt_expiry('not-a-jwt') is None


def test_token_is_cached_until_near_expiry() -> None:
    now = [1000.0]
    minted: list[str] = []

    def mint() -> str:
        minted.append(jwt(now[0] + 3600))
        return minted[-1]

    tp = TokenProvider('aud', mint, clock=lambda: now[0])
    first = tp.token()
    now[0] += 3000  # 600s left: still outside the 300s margin
    assert tp.token() == first
    now[0] += 400  # 200s left: refresh
    assert tp.token() != first
    assert len(minted) == 2
    tp.invalidate()
    tp.token()
    assert len(minted) == 3


def test_mint_failure_is_transient() -> None:
    def boom() -> str:
        raise RuntimeError('metadata server down')

    with pytest.raises(TransientError):
        TokenProvider('aud', boom).token()
