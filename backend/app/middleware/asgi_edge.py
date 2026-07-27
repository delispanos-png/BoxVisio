"""Pure-ASGI replacements for the two cheapest HTTP middlewares.

Every `app.middleware('http')` layer is a Starlette BaseHTTPMiddleware, and each
one wraps the request in an anyio task group with a pair of memory object
streams. With fourteen of them stacked, a request that touches no database at
all still spent ~3.6 ms just traversing the chain.

Security headers and canonical-path redirects need nothing from the Request
object beyond the raw scope, so they are implemented directly against the ASGI
interface and cost a dict lookup instead of a task group.

Behaviour is intentionally identical to the middlewares they replace.
"""

from __future__ import annotations

from collections.abc import Iterable


_SECURITY_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b'x-content-type-options', b'nosniff'),
    (b'x-frame-options', b'DENY'),
    (b'referrer-policy', b'strict-origin-when-cross-origin'),
    (b'permissions-policy', b'geolocation=(), microphone=(), camera=()'),
    (
        b'content-security-policy',
        b"default-src 'self'; "
        b"script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        b"style-src 'self' 'unsafe-inline'; "
        b"img-src 'self' data:; "
        b"font-src 'self' data:; "
        b"connect-src 'self';",
    ),
    # Keep API and admin paths out of search engines.
    (b'x-robots-tag', b'noindex, nofollow'),
)

_HSTS = (b'strict-transport-security', b'max-age=31536000; includeSubDomains; preload')


def _is_https(scope) -> bool:
    if scope.get('scheme') == 'https':
        return True
    for key, value in scope.get('headers') or ():
        if key == b'x-forwarded-proto':
            return value.decode('latin-1').lower() == 'https'
    return False


def _canonical_target(path: str) -> str | None:
    if path in {'/login.', '/logout.'}:
        return path[:-1]
    if (path.startswith('/tenant/') or path.startswith('/admin/')) and path.endswith('.'):
        return path.rstrip('.')
    return None


class SecureHeadersMiddleware:
    """Attach the standard security headers to every response."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        extra: Iterable[tuple[bytes, bytes]] = _SECURITY_HEADERS
        if _is_https(scope):
            extra = (*_SECURITY_HEADERS, _HSTS)

        async def send_with_headers(message):
            if message['type'] == 'http.response.start':
                headers = message.setdefault('headers', [])
                # Mirror the previous `response.headers[k] = v` semantics: the
                # value we set wins over anything the route already emitted.
                present = {k.lower() for k, _ in headers}
                for key, value in extra:
                    if key in present:
                        headers[:] = [(k, v) for k, v in headers if k.lower() != key]
                    headers.append((key, value))
            await send(message)

        await self.app(scope, receive, send_with_headers)


class CanonicalPathMiddleware:
    """Redirect the trailing-dot variants of UI paths to their canonical form."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        target_path = _canonical_target(scope.get('path', ''))
        if target_path and scope.get('method', '').upper() in {'GET', 'HEAD'}:
            query = scope.get('query_string') or b''
            location = target_path.encode('latin-1')
            if query:
                location = location + b'?' + query
            await send(
                {
                    'type': 'http.response.start',
                    'status': 308,
                    'headers': [(b'location', location), (b'content-length', b'0')],
                }
            )
            await send({'type': 'http.response.body', 'body': b''})
            return

        await self.app(scope, receive, send)
