"""Separate Impala identities, configured only from the environment or mounted secret files.

Endpoint settings come from MERIDIAN_IMPALA_<KEY> and may be overridden per role
with MERIDIAN_<ROLE>_<KEY>. Credentials are per role only. Secrets are never
included in reprs, reports or exceptions raised here.
"""
from dataclasses import dataclass, field
from pathlib import Path
import os

ROLES = {'public_writer': 'PUBLIC_WRITER', 'private_writer': 'PRIVATE_WRITER', 'agent': 'AGENT'}
AUTH_MECHANISMS = ('NOSASL', 'PLAIN', 'LDAP', 'GSSAPI', 'JWT')


def _truthy(value): return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


@dataclass(frozen=True)
class Identity:
    role: str
    host: str
    port: int = 21050
    auth_mechanism: str = 'NOSASL'
    user: str = ''
    password: str = field(default='', repr=False)
    jwt: str = field(default='', repr=False)
    use_http: bool = False
    http_path: str = 'cliservice'
    use_ssl: bool = False
    ca_cert: str = ''
    verify_cert: bool = True
    kerberos_service_name: str = 'impala'
    timeout_s: int = 600
    query_options: tuple = ()

    @property
    def authenticated(self): return self.auth_mechanism != 'NOSASL'

    @property
    def principal(self):
        """Name used to compare identities; unauthenticated connections have none."""
        return self.user if self.authenticated else ''

    def describe(self):
        """Secret-free summary for preflight reports."""
        return dict(role=self.role, host=self.host, port=self.port, auth_mechanism=self.auth_mechanism,
                    user=self.user or None, transport='http' if self.use_http else 'binary', ssl=self.use_ssl)

    @classmethod
    def environment(cls, role, env=None):
        env = os.environ if env is None else env
        prefix = ROLES[role]
        def value(key, default=''):
            return env.get(f'MERIDIAN_{prefix}_{key}', env.get(f'MERIDIAN_IMPALA_{key}', default))
        def secret(key):
            # Secrets are per role only, never inherited from the shared endpoint settings.
            path = env.get(f'MERIDIAN_{prefix}_{key}_FILE')
            if path: return Path(path).read_text().strip()
            return env.get(f'MERIDIAN_{prefix}_{key}', '')
        host = value('HOST')
        if not host: return None
        auth = value('AUTH_MECHANISM', 'NOSASL').upper()
        if auth not in AUTH_MECHANISMS: raise ValueError(f'{role}: unsupported auth mechanism {auth}')
        options = tuple(o.strip() for o in value('QUERY_OPTIONS').split(',') if o.strip())
        for option in options:
            name, _, setting = option.partition('=')
            if not name.replace('_', '').isalnum() or not setting or any(c in setting for c in ";'\"`"):
                raise ValueError(f'{role}: query options must be NAME=value pairs')
        identity = cls(role=role, host=host, port=int(value('PORT', '21050')), auth_mechanism=auth,
                       user=env.get(f'MERIDIAN_{prefix}_USER', ''), password=secret('PASSWORD'), jwt=secret('JWT'),
                       use_http=_truthy(value('USE_HTTP', 'false')), http_path=value('HTTP_PATH', 'cliservice'),
                       use_ssl=_truthy(value('USE_SSL', 'false')), ca_cert=value('CA_CERT'),
                       verify_cert=_truthy(value('VERIFY_CERT', 'true')),
                       kerberos_service_name=value('KERBEROS_SERVICE_NAME', 'impala'),
                       timeout_s=int(value('TIMEOUT_S', '600')), query_options=options)
        if auth in ('PLAIN', 'LDAP') and not (identity.user and identity.password):
            raise ValueError(f'{role}: {auth} requires MERIDIAN_{prefix}_USER and MERIDIAN_{prefix}_PASSWORD(_FILE)')
        if auth == 'JWT' and not identity.jwt:
            raise ValueError(f'{role}: JWT requires MERIDIAN_{prefix}_JWT(_FILE)')
        return identity

    def connect(self):
        from impala.dbapi import connect  # optional dependency: requirements-warehouse.txt
        kwargs = dict(host=self.host, port=self.port, auth_mechanism=self.auth_mechanism,
                      use_ssl=self.use_ssl, timeout=self.timeout_s)
        # impyla defaults to verify_cert=False; TLS without certificate checks is opt-out only.
        if self.use_ssl: kwargs['verify_cert'] = self.verify_cert
        if self.ca_cert: kwargs['ca_cert'] = self.ca_cert
        if self.user: kwargs['user'] = self.user
        if self.auth_mechanism in ('PLAIN', 'LDAP'): kwargs['password'] = self.password
        if self.auth_mechanism == 'GSSAPI': kwargs['kerberos_service_name'] = self.kerberos_service_name
        if self.auth_mechanism == 'JWT': kwargs['jwt'] = self.jwt
        if self.use_http: kwargs.update(use_http_transport=True, http_path=self.http_path)
        return connect(**kwargs)


def identities(env=None):
    return {role: Identity.environment(role, env) for role in ROLES}
