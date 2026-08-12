"""Co-Pilot conversation history — saved per tenant user, shown on the Co-Pilot page."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete as sa_delete, desc, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.control import CopilotConversation, CopilotMessage

_MAX_TITLE = 80


def _make_title(text: str) -> str:
    t = ' '.join(str(text or '').split())[:_MAX_TITLE].strip()
    return t or 'Συνομιλία'


async def list_conversations(db: AsyncSession, tenant_id: int, user_id: int, limit: int = 60) -> list[dict]:
    rows = (
        await db.execute(
            select(CopilotConversation)
            .where(CopilotConversation.tenant_id == int(tenant_id), CopilotConversation.user_id == int(user_id))
            .order_by(desc(CopilotConversation.updated_at))
            .limit(limit)
        )
    ).scalars().all()
    return [{'id': c.id, 'title': c.title, 'updated_at': c.updated_at.strftime('%d/%m/%Y %H:%M')} for c in rows]


async def get_conversation(db: AsyncSession, tenant_id: int, user_id: int, conv_id: int) -> dict | None:
    conv = (
        await db.execute(
            select(CopilotConversation).where(
                CopilotConversation.id == int(conv_id),
                CopilotConversation.tenant_id == int(tenant_id),
                CopilotConversation.user_id == int(user_id),
            )
        )
    ).scalar_one_or_none()
    if conv is None:
        return None
    msgs = (
        await db.execute(
            select(CopilotMessage).where(CopilotMessage.conversation_id == int(conv_id)).order_by(CopilotMessage.id)
        )
    ).scalars().all()
    return {
        'id': conv.id,
        'title': conv.title,
        'messages': [{'role': m.role, 'content': m.content} for m in msgs],
    }


async def add_message(db: AsyncSession, conv_id: int, role: str, content: str) -> None:
    db.add(CopilotMessage(conversation_id=int(conv_id), role=str(role), content=str(content or '')))
    await db.execute(
        update(CopilotConversation)
        .where(CopilotConversation.id == int(conv_id))
        .values(updated_at=datetime.utcnow())
    )
    await db.commit()


async def ensure_conversation(
    db: AsyncSession, tenant_id: int, user_id: int, conv_id: int | None, first_user_text: str
) -> tuple[int, str, bool]:
    """Return (conversation_id, title, is_new). Creates a conversation when conv_id is None
    or does not belong to this user. Appends the user message."""
    conv = None
    if conv_id:
        conv = (
            await db.execute(
                select(CopilotConversation).where(
                    CopilotConversation.id == int(conv_id),
                    CopilotConversation.tenant_id == int(tenant_id),
                    CopilotConversation.user_id == int(user_id),
                )
            )
        ).scalar_one_or_none()
    is_new = conv is None
    if conv is None:
        conv = CopilotConversation(tenant_id=int(tenant_id), user_id=int(user_id), title=_make_title(first_user_text))
        db.add(conv)
        await db.flush()
    db.add(CopilotMessage(conversation_id=int(conv.id), role='user', content=str(first_user_text or '')))
    await db.execute(
        update(CopilotConversation).where(CopilotConversation.id == int(conv.id)).values(updated_at=datetime.utcnow())
    )
    await db.commit()
    return int(conv.id), conv.title, is_new


async def delete_conversation(db: AsyncSession, tenant_id: int, user_id: int, conv_id: int) -> bool:
    conv = (
        await db.execute(
            select(CopilotConversation).where(
                CopilotConversation.id == int(conv_id),
                CopilotConversation.tenant_id == int(tenant_id),
                CopilotConversation.user_id == int(user_id),
            )
        )
    ).scalar_one_or_none()
    if conv is None:
        return False
    await db.execute(sa_delete(CopilotMessage).where(CopilotMessage.conversation_id == int(conv_id)))
    await db.execute(sa_delete(CopilotConversation).where(CopilotConversation.id == int(conv_id)))
    await db.commit()
    return True
