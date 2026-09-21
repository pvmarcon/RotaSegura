import base64
import hashlib
import hmac
import json
import time
from typing import Any


class QRTokenError(ValueError):
    """Erro de formato, assinatura ou validade do token QR."""


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def gerar_token_qr(aluno_id: str, segredo: str, validade_segundos: int = 300) -> str:
    """Gera `payload.assinatura`, com payload JSON contendo aluno_id e exp."""
    if not aluno_id.strip():
        raise ValueError("aluno_id não pode ser vazio.")
    if validade_segundos <= 0:
        raise ValueError("A validade deve ser maior que zero.")

    payload = {
        "aluno_id": aluno_id.strip(),
        "exp": int(time.time()) + validade_segundos,
    }
    payload_encoded = _encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
    assinatura = hmac.new(
        segredo.encode("utf-8"), payload_encoded.encode("ascii"), hashlib.sha256
    ).digest()
    return f"{payload_encoded}.{_encode(assinatura)}"


def validar_token_qr(token: str, segredo: str) -> dict[str, Any]:
    """Valida formato, HMAC-SHA256 e expiração, retornando o payload."""
    try:
        payload_encoded, assinatura_encoded = token.strip().split(".", 1)
        payload_bytes = _decode(payload_encoded)
        assinatura = _decode(assinatura_encoded)
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (AttributeError, ValueError, UnicodeDecodeError, json.JSONDecodeError, base64.binascii.Error) as exc:
        raise QRTokenError("QR Code malformado.") from exc

    assinatura_esperada = hmac.new(
        segredo.encode("utf-8"), payload_encoded.encode("ascii"), hashlib.sha256
    ).digest()
    if not hmac.compare_digest(assinatura, assinatura_esperada):
        raise QRTokenError("Assinatura do QR Code inválida.")

    if not isinstance(payload, dict) or not isinstance(payload.get("aluno_id"), str):
        raise QRTokenError("Dados do aluno ausentes no QR Code.")

    exp = payload.get("exp")
    if not isinstance(exp, int) or isinstance(exp, bool):
        raise QRTokenError("Validade do QR Code inválida.")
    if exp <= int(time.time()):
        raise QRTokenError("QR Code expirado.")

    return {"aluno_id": payload["aluno_id"], "exp": exp}