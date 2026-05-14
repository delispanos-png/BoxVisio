from fastapi import Request
from fastapi.responses import RedirectResponse


def _canonical_target(path: str) -> str | None:
    if path in {'/login.', '/logout.'}:
        return path[:-1]
    if (path.startswith('/tenant/') or path.startswith('/admin/')) and path.endswith('.'):
        return path.rstrip('.')
    return None


async def canonical_path_middleware(request: Request, call_next):
    target_path = _canonical_target(request.url.path)
    if target_path and request.method.upper() in {'GET', 'HEAD'}:
        target = target_path
        if request.url.query:
            target = f'{target}?{request.url.query}'
        return RedirectResponse(url=target, status_code=308)

    return await call_next(request)
