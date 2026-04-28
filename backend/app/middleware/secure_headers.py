from fastapi import Request


async def secure_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response.headers['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "font-src 'self' data:; "
        "connect-src 'self';"
    )
    # Prevent search engines from indexing any API or admin paths.
    response.headers['X-Robots-Tag'] = 'noindex, nofollow'
    # Always set HSTS — nginx already enforces TLS; the app layer redundantly
    # declares it so direct container access also carries the header.
    forwarded_proto = (request.headers.get('x-forwarded-proto') or '').lower()
    if request.url.scheme == 'https' or forwarded_proto == 'https':
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains; preload'
    return response
