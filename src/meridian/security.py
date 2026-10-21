"""Bearer authentication with optional read-only status identity."""
import hmac
import os
from pathlib import Path
import secrets


def operator_token(state):
    configured = os.environ.get('GENERATOR_API_TOKEN')
    if configured: return configured
    token_file = os.environ.get('GENERATOR_API_TOKEN_FILE')
    if token_file: return Path(token_file).read_text().strip()
    state.mkdir(parents=True,exist_ok=True,mode=0o700)
    path = state/'operator.token'
    try:
        fd = os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as stream: stream.write(secrets.token_urlsafe(32))
    except FileExistsError:
        pass
    return path.read_text().strip()


def identity(header,state):
    if not header.startswith('Bearer '): return None
    provided = header[7:]
    if not provided: return None
    expected = operator_token(state)
    if expected and hmac.compare_digest(provided,expected): return 'operator'
    viewer = os.environ.get('GENERATOR_VIEWER_TOKEN')
    if viewer and hmac.compare_digest(provided,viewer): return 'viewer'
    return None
