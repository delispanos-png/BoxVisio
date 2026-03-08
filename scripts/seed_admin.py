#!/usr/bin/env python
import asyncio
import sys

from sqlalchemy import select

sys.path.append('/app')
sys.path.append('/opt/cloudon-bi/backend')

from app.core.config import settings  # noqa: E402
from app.core.security import get_password_hash  # noqa: E402
from app.db.control_session import ControlSessionLocal  # noqa: E402
from app.models.control import ProfessionalProfile, RoleName, User  # noqa: E402


async def run() -> None:
    if not settings.default_admin_password:
        raise RuntimeError('DEFAULT_ADMIN_PASSWORD must be set in environment')

    async with ControlSessionLocal() as db:
        owner_profile = (
            await db.execute(select(ProfessionalProfile).where(ProfessionalProfile.profile_code == 'OWNER'))
        ).scalar_one_or_none()
        if owner_profile is None:
            raise RuntimeError('Missing OWNER professional profile. Run control migrations first.')

        existing = await db.execute(select(User).where(User.email == settings.default_admin_email))
        user = existing.scalar_one_or_none()
        if user:
            print('Admin already exists')
            return

        user = User(
            email=settings.default_admin_email,
            password_hash=get_password_hash(settings.default_admin_password),
            role=RoleName.cloudon_admin,
            tenant_id=None,
            professional_profile_id=owner_profile.id,
            is_active=True,
        )
        db.add(user)
        await db.commit()
        print(f'Created admin {settings.default_admin_email}')


if __name__ == '__main__':
    asyncio.run(run())
