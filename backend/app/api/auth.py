from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import create_access_token, create_refresh_token, get_password_hash, safe_decode, verify_password
from app.db.control_session import get_control_db
from app.models.control import RefreshToken, User
from app.schemas.auth import LoginRequest, RefreshTokenRequest, ResetPasswordRequest, TokenResponse

router = APIRouter(prefix='/v1/auth', tags=['auth'])


@router.post('/login', response_model=TokenResponse)
async def login(payload: LoginRequest, db: AsyncSession = Depends(get_control_db)) -> TokenResponse:
    result = await db.execute(select(User).where(User.email == payload.email, User.is_active.is_(True)))
    user = result.scalar_one_or_none()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid credentials')

    access_token = create_access_token(subject=str(user.id), tenant_id=user.tenant_id, role=user.role.value)
    refresh_token, refresh_jti, refresh_exp = create_refresh_token(subject=str(user.id))
    db.add(
        RefreshToken(
            user_id=user.id,
            token_jti=refresh_jti,
            expires_at=refresh_exp.astimezone(timezone.utc).replace(tzinfo=None),
            revoked_at=None,
        )
    )
    await db.commit()
    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


@router.post('/refresh', response_model=TokenResponse)
async def refresh_access_token(payload: RefreshTokenRequest, db: AsyncSession = Depends(get_control_db)) -> TokenResponse:
    decoded = safe_decode(payload.refresh_token)
    if not decoded or decoded.get('typ') != 'refresh':
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid refresh token')

    user_id = decoded.get('sub')
    jti = decoded.get('jti')
    if not user_id or not jti:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid refresh token payload')

    db_token = (
        await db.execute(
            select(RefreshToken).where(
                RefreshToken.token_jti == str(jti),
                RefreshToken.user_id == int(user_id),
            )
        )
    ).scalar_one_or_none()
    if not db_token or db_token.revoked_at is not None or db_token.expires_at < datetime.utcnow():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Refresh token revoked/expired')

    user = (await db.execute(select(User).where(User.id == int(user_id), User.is_active.is_(True)))).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='User not found')

    new_access = create_access_token(subject=str(user.id), tenant_id=user.tenant_id, role=user.role.value)
    new_refresh, new_jti, new_exp = create_refresh_token(subject=str(user.id))

    db_token.revoked_at = datetime.utcnow()
    db_token.replaced_by_jti = new_jti
    db.add(
        RefreshToken(
            user_id=user.id,
            token_jti=new_jti,
            expires_at=new_exp.astimezone(timezone.utc).replace(tzinfo=None),
            revoked_at=None,
        )
    )
    await db.commit()
    return TokenResponse(access_token=new_access, refresh_token=new_refresh)


@router.post('/logout')
async def logout(payload: RefreshTokenRequest, db: AsyncSession = Depends(get_control_db)):
    decoded = safe_decode(payload.refresh_token)
    if not decoded or decoded.get('typ') != 'refresh':
        return {'status': 'ok'}

    user_id = decoded.get('sub')
    jti = decoded.get('jti')
    if not user_id or not jti:
        return {'status': 'ok'}

    db_token = (
        await db.execute(
            select(RefreshToken).where(
                RefreshToken.token_jti == str(jti),
                RefreshToken.user_id == int(user_id),
            )
        )
    ).scalar_one_or_none()
    if db_token and db_token.revoked_at is None:
        db_token.revoked_at = datetime.utcnow()
        await db.commit()
    return {'status': 'ok'}


@router.post('/reset-password')
async def reset_password(payload: ResetPasswordRequest, db: AsyncSession = Depends(get_control_db)):
    user = (await db.execute(select(User).where(User.reset_token == payload.token))).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=400, detail='Invalid reset token')
    if not user.reset_token_expires_at or user.reset_token_expires_at < datetime.utcnow():
        raise HTTPException(status_code=400, detail='Expired reset token')

    user.password_hash = get_password_hash(payload.new_password)
    user.reset_token = None
    user.reset_token_expires_at = None
    await db.commit()
    return {'status': 'password_updated'}
