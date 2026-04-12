from contextvars import ContextVar


_allowed_branch_scope_ctx: ContextVar[tuple[str, ...] | None] = ContextVar('allowed_branch_scope', default=None)


def set_allowed_branch_scope(branches: list[str] | tuple[str, ...] | None):
    if branches is None:
        return _allowed_branch_scope_ctx.set(None)
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in branches:
        value = str(raw or '').strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return _allowed_branch_scope_ctx.set(tuple(normalized))


def get_allowed_branch_scope() -> tuple[str, ...] | None:
    return _allowed_branch_scope_ctx.get()


def reset_allowed_branch_scope(token) -> None:
    _allowed_branch_scope_ctx.reset(token)
