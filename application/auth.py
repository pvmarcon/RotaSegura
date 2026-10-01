from typing import Optional

from fastapi import Request, status
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from config import SESSION_COOKIE_NAME, SESSION_MAX_AGE_SECONDS, SESSION_SECRET, SESSION_COOKIE_SECURE

serializer = URLSafeTimedSerializer(SESSION_SECRET, salt="admin-session")


def get_session_user(request: Request) -> Optional[str]:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    try:
        data = serializer.loads(token, max_age=SESSION_MAX_AGE_SECONDS)
        return data.get("user")
    except (BadSignature, SignatureExpired):
        return None


def get_session_role(request: Request) -> Optional[str]:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    try:
        data = serializer.loads(token, max_age=SESSION_MAX_AGE_SECONDS)
        return data.get("role")
    except (BadSignature, SignatureExpired):
        return None


def is_admin(request: Request) -> bool:
    return get_session_role(request) == "ADMINISTRADOR"


def require_admin(request: Request) -> Optional[RedirectResponse]:
    if not get_session_user(request) or not get_session_role(request):
        return RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)
    if not is_admin(request):
        return RedirectResponse(url="/monitor", status_code=status.HTTP_303_SEE_OTHER)
    return None


def create_admin_session(username: str, role: str = "ADMINISTRADOR", **claims: object) -> str:
    return serializer.dumps({"user": username, "role": role, **claims})
