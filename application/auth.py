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


def require_admin(request: Request) -> Optional[RedirectResponse]:
    if not get_session_user(request):
        return RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)
    return None


def create_admin_session(username: str) -> str:
    return serializer.dumps({"user": username})
