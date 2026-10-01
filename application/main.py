import asyncio
import hashlib
import hmac
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import aio_pika
import psycopg2
import redis
from fastapi import FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from auth import create_admin_session, get_session_role, get_session_user, is_admin, require_admin
from config import (
    ADMIN_PASSWORD,
    ADMIN_SIMULATE_PASSWORD,
    ADMIN_USER,
    GRAFANA_URL,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    RABBITMQ_HOST,
    REDIS_HOST,
    SESSION_COOKIE_NAME,
    SESSION_COOKIE_SECURE,
    SESSION_MAX_AGE_SECONDS,
    QR_HMAC_SECRET,
    QR_TOKEN_TTL_SECONDS,
    SERVICE_CONNECT_TIMEOUT_SECONDS,
    SIMULATION_HARD_CAP,
)
from qr_tokens import QRTokenError, gerar_token_qr, validar_token_qr
from simulation import active_job_id, event_stream, simulation_jobs, start_simulation, stop_simulation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_gateway")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    db=0,
    decode_responses=True,
    socket_connect_timeout=SERVICE_CONNECT_TIMEOUT_SECONDS,
    socket_timeout=SERVICE_CONNECT_TIMEOUT_SECONDS,
)

rabbitmq_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
rabbitmq_channel: Optional[aio_pika.abc.AbstractChannel] = None


def get_db_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
    )


def ensure_management_schema() -> None:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS students (
                    student_id VARCHAR(5) PRIMARY KEY,
                    student_name VARCHAR(100) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE SEQUENCE IF NOT EXISTS student_id_sequence START WITH 1;
                CREATE TABLE IF NOT EXISTS student_events (
                    id SERIAL PRIMARY KEY,
                    student_id VARCHAR(50) NOT NULL,
                    student_name VARCHAR(100) NOT NULL DEFAULT 'Aluno',
                    bus_id VARCHAR(50) NOT NULL,
                    monitor_id VARCHAR(50),
                    boarding_location VARCHAR(120) NOT NULL DEFAULT 'Não informado',
                    event_type VARCHAR(50) NOT NULL,
                    timestamp DOUBLE PRECISION NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS notifications (
                    id SERIAL PRIMARY KEY,
                    student_id VARCHAR(50) NOT NULL,
                    student_name VARCHAR(100) NOT NULL DEFAULT 'Aluno',
                    bus_id VARCHAR(50) NOT NULL,
                    event_type VARCHAR(50) NOT NULL,
                    boarding_at DOUBLE PRECISION NOT NULL,
                    message TEXT NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'GERADA',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS buses (
                    id SERIAL PRIMARY KEY,
                    bus_number VARCHAR(50) UNIQUE NOT NULL,
                    plate VARCHAR(20) UNIQUE,
                    capacity INTEGER NOT NULL DEFAULT 40,
                    driver VARCHAR(100),
                    monitor VARCHAR(100),
                    status VARCHAR(20) NOT NULL DEFAULT 'ATIVO',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS routes (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(120) NOT NULL,
                    bus_id INTEGER REFERENCES buses(id) ON DELETE SET NULL,
                    driver VARCHAR(100),
                    monitor VARCHAR(100),
                    status VARCHAR(20) NOT NULL DEFAULT 'ATIVA',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS route_stops (
                    id SERIAL PRIMARY KEY,
                    route_id INTEGER NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
                    name VARCHAR(120) NOT NULL,
                    address VARCHAR(200),
                    stop_order INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS route_students (
                    route_id INTEGER NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
                    student_id VARCHAR(5) NOT NULL REFERENCES students(student_id) ON DELETE CASCADE,
                    PRIMARY KEY (route_id, student_id)
                );
                CREATE TABLE IF NOT EXISTS qr_codes (
                    id SERIAL PRIMARY KEY,
                    token TEXT UNIQUE NOT NULL,
                    student_id VARCHAR(5) NOT NULL REFERENCES students(student_id) ON DELETE CASCADE,
                    bus_id VARCHAR(50) NOT NULL,
                    boarding_location VARCHAR(120) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMPTZ NOT NULL,
                    used_at TIMESTAMPTZ,
                    invalidated_at TIMESTAMPTZ
                );
                CREATE INDEX IF NOT EXISTS idx_qr_codes_student ON qr_codes(student_id);
                CREATE TABLE IF NOT EXISTS app_users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(80) UNIQUE NOT NULL,
                    display_name VARCHAR(120) NOT NULL,
                    email VARCHAR(160),
                    role VARCHAR(20) NOT NULL DEFAULT 'MONITOR',
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    password_hash VARCHAR(128),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS app_settings (
                    key VARCHAR(100) PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                ALTER TABLE students ADD COLUMN IF NOT EXISTS class_name VARCHAR(80);
                ALTER TABLE students ADD COLUMN IF NOT EXISTS guardian_name VARCHAR(120);
                ALTER TABLE students ADD COLUMN IF NOT EXISTS guardian_phone VARCHAR(40);
                ALTER TABLE students ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'ATIVO';
                ALTER TABLE student_events ADD COLUMN IF NOT EXISTS qr_token TEXT;
                ALTER TABLE student_events ADD COLUMN IF NOT EXISTS route_id INTEGER;
                ALTER TABLE app_users ADD COLUMN IF NOT EXISTS bus_id INTEGER REFERENCES buses(id) ON DELETE SET NULL;
                ALTER TABLE app_users ADD COLUMN IF NOT EXISTS route_id INTEGER REFERENCES routes(id) ON DELETE SET NULL;
            """)
        conn.commit()
    except psycopg2.Error:
        if conn is not None:
            conn.rollback()
        logger.exception("Não foi possível atualizar o esquema de gestão.")
    finally:
        if conn is not None:
            conn.close()


def claim_qr_scan(raw_qr_code: str, expires_at: int) -> bool:
    """Registra o primeiro uso do token; repetições do mesmo QR são recusadas."""
    fingerprint = hashlib.sha256(raw_qr_code.strip().encode("utf-8")).hexdigest()
    ttl = max(1, expires_at - int(time.time()))
    try:
        return bool(redis_client.set(f"qr_scan:{fingerprint}", "1", nx=True, ex=ttl))
    except redis.RedisError as exc:
        logger.error("Não foi possível verificar idempotência do QR: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Serviço de controle de scans temporariamente indisponível.",
        ) from exc


def release_qr_scan(raw_qr_code: str) -> None:
    """Libera um token quando a publicação do embarque falha."""
    fingerprint = hashlib.sha256(raw_qr_code.strip().encode("utf-8")).hexdigest()
    try:
        redis_client.delete(f"qr_scan:{fingerprint}")
    except redis.RedisError:
        logger.exception("Não foi possível liberar o QR Code após falha de publicação.")


async def ensure_rabbitmq_channel() -> Optional[aio_pika.abc.AbstractChannel]:
    global rabbitmq_connection, rabbitmq_channel
    if rabbitmq_channel is not None and not rabbitmq_channel.is_closed:
        return rabbitmq_channel
    try:
        rabbitmq_connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST, port=5672, timeout=5
        )
        rabbitmq_channel = await rabbitmq_connection.channel()
        await rabbitmq_channel.declare_queue("student_events", durable=True)
        logger.info("RabbitMQ conectado.")
        return rabbitmq_channel
    except Exception as e:
        logger.error("RabbitMQ indisponível: %s", e)
        rabbitmq_channel = None
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # RabbitMQ é conectado sob demanda ao registrar um embarque.
    await asyncio.to_thread(ensure_management_schema)
    yield
    if rabbitmq_connection is not None:
        await rabbitmq_connection.close()


app = FastAPI(
    title="Smart City Bus Tracking API",
    description="API Gateway com validação síncrona e despacho assíncrono para RabbitMQ",
    version="1.0.0",
    lifespan=lifespan,
)

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.middleware("http")
async def enforce_role_access(request: Request, call_next):
    path = request.url.path
    protected_admin_page = path == "/admin" or path.startswith("/admin/")
    protected_admin_api = path.startswith("/api/v1/admin/")
    public_admin_path = path in {"/admin/login", "/admin/logout"}
    if (protected_admin_page and not public_admin_path) or protected_admin_api:
        role = get_session_role(request)
        if get_session_user(request) and role != "ADMINISTRADOR":
            if protected_admin_api:
                return JSONResponse({"detail": "Acesso restrito ao Administrador."}, status_code=403 if role == "MONITOR" else 401)
            destination = "/monitor" if role == "MONITOR" else "/admin/login"
            return RedirectResponse(url=destination, status_code=status.HTTP_303_SEE_OTHER)
    return await call_next(request)


class ScanPayload(BaseModel):
    student_id: str
    bus_id: str
    monitor_id: str
    student_name: str = Field(default="Aluno", max_length=100)
    boarding_location: str = Field(default="Não informado", max_length=120)
    timestamp: float = Field(default_factory=time.time)


class QRValidationPayload(BaseModel):
    qr_code: str = Field(min_length=1)


class QRGenerationPayload(BaseModel):
    aluno_id: str = Field(pattern=r"^\d{5}$")
    bus_id: str = Field(min_length=1, max_length=50)
    boarding_location: str = Field(min_length=1, max_length=120)
    validade_segundos: int = Field(default=300, ge=60, le=86400)


class StudentBlockPayload(BaseModel):
    student_id: str = Field(min_length=1, max_length=100)


class StudentRegistrationPayload(BaseModel):
    student_name: str = Field(min_length=1, max_length=100)
    class_name: str = Field(default="", max_length=80)
    guardian_name: str = Field(default="", max_length=120)
    guardian_phone: str = Field(default="", max_length=40)
    status: str = Field(default="ATIVO", pattern=r"^(ATIVO|INATIVO)$")


class StudentUpdatePayload(StudentRegistrationPayload):
    pass


class BusPayload(BaseModel):
    bus_number: str = Field(min_length=1, max_length=50)
    plate: str = Field(default="", max_length=20)
    capacity: int = Field(default=40, ge=1, le=200)
    driver: str = Field(default="", max_length=100)
    monitor: str = Field(default="", max_length=100)
    status: str = Field(default="ATIVO", pattern=r"^(ATIVO|INATIVO)$")


class RoutePayload(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    bus_id: Optional[int] = None
    driver: str = Field(default="", max_length=100)
    monitor: str = Field(default="", max_length=100)
    status: str = Field(default="ATIVA", pattern=r"^(ATIVA|INATIVA)$")


class StopPayload(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    address: str = Field(default="", max_length=200)
    stop_order: int = Field(default=1, ge=1)


class UserPayload(BaseModel):
    username: str = Field(min_length=3, max_length=80)
    display_name: str = Field(min_length=1, max_length=120)
    email: str = Field(default="", max_length=160)
    role: str = Field(default="MONITOR", pattern=r"^(ADMINISTRADOR|MONITOR)$")
    is_active: bool = True
    password: str = Field(default="", max_length=200)
    bus_id: Optional[int] = None
    route_id: Optional[int] = None


class SettingPayload(BaseModel):
    settings: dict[str, str]


def get_monitor_data(request: Request) -> dict:
    username = get_session_user(request)
    data = {
        "monitor_name": username or "Monitor",
        "bus_number": "Não atribuído",
        "route_name": "Sem rota atribuída",
        "driver": "Não informado",
        "expected_students": 0,
        "boarded_students": 0,
        "remaining_students": 0,
        "recent_boardings": [],
        "notifications": [],
    }
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""SELECT u.display_name, b.bus_number, r.name, r.driver,
                (SELECT COUNT(*) FROM route_students rs WHERE rs.route_id=r.id)
                FROM app_users u LEFT JOIN buses b ON b.id=u.bus_id LEFT JOIN routes r ON r.id=u.route_id
                WHERE u.username=%s AND u.is_active=TRUE""", (username,))
            assignment = cursor.fetchone()
            if assignment:
                data.update({"monitor_name": assignment[0], "bus_number": assignment[1] or "Não atribuído", "route_name": assignment[2] or "Sem rota atribuída", "driver": assignment[3] or "Não informado", "expected_students": assignment[4] or 0})
            cursor.execute("""SELECT e.student_name, e.student_id, e.created_at, e.bus_id
                FROM student_events e JOIN app_users u ON u.username=%s
                LEFT JOIN buses b ON b.id=u.bus_id
                WHERE e.event_type='STUDENT_BOARDED' AND u.bus_id IS NOT NULL AND e.bus_id=b.bus_number
                ORDER BY e.created_at DESC LIMIT 8""", (username,))
            data["recent_boardings"] = [{"student_name": row[0], "student_id": row[1], "time": row[2].strftime("%H:%M"), "bus_id": row[3]} for row in cursor.fetchall()]
            if assignment and assignment[1]:
                cursor.execute("SELECT COUNT(DISTINCT student_id) FROM student_events WHERE event_type='STUDENT_BOARDED' AND bus_id=%s AND created_at::date=CURRENT_DATE", (assignment[1],))
                data["boarded_students"] = cursor.fetchone()[0]
                data["remaining_students"] = max(0, data["expected_students"] - data["boarded_students"])
                cursor.execute("SELECT message, created_at, status FROM notifications WHERE bus_id=%s ORDER BY created_at DESC LIMIT 30", (assignment[1],))
                data["notifications"] = [{"message": row[0], "time": row[1].strftime("%d/%m %H:%M"), "status": row[2]} for row in cursor.fetchall()]
    except psycopg2.Error:
        logger.warning("Não foi possível carregar a operação do monitor.")
    finally:
        if conn is not None:
            conn.close()
    return data


@app.get("/monitor", response_class=HTMLResponse)
async def monitor_home(request: Request):
    if not get_session_user(request):
        return RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)
    if get_session_role(request) != "MONITOR":
        return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(request, "monitor.html", {"title": "Operação do Monitor", "active_page": "monitor", **get_monitor_data(request)})


@app.get("/monitor/{section}", response_class=HTMLResponse)
async def monitor_section(request: Request, section: str):
    if not get_session_user(request):
        return RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)
    if get_session_role(request) != "MONITOR" or section not in {"embarques", "notificacoes", "perfil"}:
        return RedirectResponse(url="/monitor", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(request, "monitor_section.html", {"title": {"embarques": "Meus embarques", "notificacoes": "Notificações", "perfil": "Meu perfil"}[section], "section": section, **get_monitor_data(request)})


class ManualBoardingPayload(BaseModel):
    student_id: str = Field(pattern=r"^\d{5}$")
    bus_id: str = Field(min_length=1, max_length=50)
    boarding_location: str = Field(min_length=1, max_length=120)


class BoardingUpdatePayload(BaseModel):
    student_id: str = Field(min_length=1, max_length=50)
    bus_id: str = Field(min_length=1, max_length=50)
    boarding_at: str = Field(min_length=1)


@app.get("/scanner", response_class=HTMLResponse)
async def qr_scanner(request: Request):
    return templates.TemplateResponse(
        request,
        "scanner.html",
        {"qr_token_ttl_seconds": QR_TOKEN_TTL_SECONDS},
    )


@app.post("/api/v1/validar-qr")
async def validar_qr(payload: QRValidationPayload):
    try:
        aluno = validar_token_qr(payload.qr_code, QR_HMAC_SECRET)
    except QRTokenError as exc:
        return {
            "status": "erro",
            "dados_aluno": None,
            "mensagem": str(exc),
        }

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT status FROM students WHERE student_id = %s", (aluno["aluno_id"],))
            student_row = cursor.fetchone()
            if student_row is None or student_row[0] != "ATIVO":
                return {
                    "status": "inativo",
                    "dados_aluno": {"aluno_id": aluno["aluno_id"], "aluno_nome": aluno["aluno_nome"]},
                    "mensagem": "Este aluno está inativo para embarques.",
                }
            cursor.execute("SELECT used_at, invalidated_at, expires_at FROM qr_codes WHERE token = %s", (payload.qr_code.strip(),))
            qr_row = cursor.fetchone()
            if qr_row is not None:
                if qr_row[1] is not None:
                    return {"status": "invalido", "dados_aluno": None, "mensagem": "Este QR Code foi invalidado."}
                if qr_row[0] is not None:
                    return {"status": "duplicado", "dados_aluno": {"aluno_id": aluno["aluno_id"], "aluno_nome": aluno["aluno_nome"]}, "mensagem": "Este QR Code já foi utilizado."}
                if qr_row[2] <= datetime.now(qr_row[2].tzinfo):
                    return {"status": "expirado", "dados_aluno": None, "mensagem": "Este QR Code está expirado."}
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail="Cadastro de alunos indisponível.") from exc
    finally:
        if conn is not None:
            conn.close()

    try:
        if redis_client.sismember("blocked_students", aluno["aluno_id"]):
            return {
                "status": "bloqueado",
                "dados_aluno": {"aluno_id": aluno["aluno_id"], "aluno_nome": aluno["aluno_nome"]},
                "mensagem": "Este aluno está bloqueado para embarques.",
            }
    except redis.RedisError as exc:
        logger.error("Não foi possível consultar bloqueios de alunos: %s", exc)
        release_qr_scan(payload.qr_code)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Serviço de bloqueios indisponível.") from exc

    if not claim_qr_scan(payload.qr_code, aluno["exp"]):
        return {
            "status": "duplicado",
            "dados_aluno": {"aluno_id": aluno["aluno_id"], "aluno_nome": aluno["aluno_nome"]},
            "mensagem": "Este QR Code já foi utilizado.",
        }

    try:
        await publish_to_rabbitmq({
            "student_id": aluno["aluno_id"],
            "student_name": aluno["aluno_nome"],
            "bus_id": aluno["bus_id"],
            "monitor_id": "WEB-SCANNER",
            "boarding_location": aluno["boarding_location"],
            "timestamp": time.time(),
            "event_type": "STUDENT_BOARDED",
            "qr_code": payload.qr_code,
        })
    except HTTPException:
        release_qr_scan(payload.qr_code)
        raise

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE qr_codes SET used_at = CURRENT_TIMESTAMP WHERE token = %s AND used_at IS NULL", (payload.qr_code.strip(),))
        conn.commit()
    except psycopg2.Error:
        if conn is not None:
            conn.rollback()
        logger.warning("Não foi possível marcar o QR Code como utilizado.")
    finally:
        if conn is not None:
            conn.close()

    return {
        "status": "sucesso",
        "dados_aluno": {
            "aluno_id": aluno["aluno_id"],
            "aluno_nome": aluno["aluno_nome"],
            "bus_id": aluno["bus_id"],
            "boarding_location": aluno["boarding_location"],
        },
        "exp": aluno["exp"],
        "mensagem": "Embarque liberado. QR Code válido.",
    }


async def publish_boarding(student_id: str, student_name: str, bus_id: str, boarding_location: str, qr_code: str = "") -> None:
    event = {
        "student_id": student_id,
        "student_name": student_name,
        "bus_id": bus_id,
        "monitor_id": "WEB-SCANNER",
        "boarding_location": boarding_location,
        "timestamp": time.time(),
        "event_type": "STUDENT_BOARDED",
    }
    if qr_code:
        event["qr_code"] = qr_code
    await publish_to_rabbitmq(event)


@app.post("/api/v1/registrar-id")
async def register_by_id(payload: ManualBoardingPayload):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT student_name, status FROM students WHERE student_id = %s;", (payload.student_id,))
            student = cursor.fetchone()
        if student is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ID de aluno não encontrado.")
        if student[1] != "ATIVO":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aluno inativo para embarques.")
    except HTTPException:
        raise
    except psycopg2.Error as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Cadastro de alunos indisponível.") from exc
    finally:
        if conn is not None:
            conn.close()

    try:
        if redis_client.sismember("blocked_students", payload.student_id):
            return {"status": "bloqueado", "mensagem": "Este aluno está bloqueado para embarques."}
        await publish_boarding(payload.student_id, student[0], payload.bus_id, payload.boarding_location)
    except HTTPException:
        raise
    return {
        "status": "sucesso",
        "dados_aluno": {"aluno_id": payload.student_id, "aluno_nome": student[0], "bus_id": payload.bus_id},
        "mensagem": "Embarque registrado pelo ID.",
    }


@app.post("/admin/gerar-qr")
async def gerar_qr_admin(request: Request, payload: QRGenerationPayload):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT student_name, status FROM students WHERE student_id = %s;", (payload.aluno_id,))
            student = cursor.fetchone()
        if student is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cadastre o aluno antes de gerar o QR Code.")
        if student[1] != "ATIVO":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aluno inativo para geração de QR Code.")
        aluno_id = payload.aluno_id
        aluno_nome = student[0]
    except HTTPException:
        raise
    except psycopg2.Error as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Cadastro de alunos indisponível.") from exc
    finally:
        if conn is not None:
            conn.close()

    token = gerar_token_qr(
        aluno_id,
        aluno_nome,
        payload.bus_id,
        payload.boarding_location,
        QR_HMAC_SECRET,
        payload.validade_segundos,
    )
    try:
        qr_expiration = int(time.time()) + payload.validade_segundos
        redis_client.zadd("qr:valid", {token: qr_expiration})
    except redis.RedisError as exc:
        logger.warning("QR gerado sem registro da métrica de validade: %s", exc)
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""INSERT INTO qr_codes (token, student_id, bus_id, boarding_location, expires_at)
                VALUES (%s, %s, %s, %s, to_timestamp(%s))""",
                (token, aluno_id, payload.bus_id.strip(), payload.boarding_location.strip(), qr_expiration))
        conn.commit()
    except psycopg2.Error as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=503, detail="Não foi possível persistir o QR Code.") from exc
    finally:
        if conn is not None:
            conn.close()
    return {
        "aluno_id": aluno_id,
        "aluno_nome": aluno_nome,
        "bus_id": payload.bus_id.strip(),
        "boarding_location": payload.boarding_location.strip(),
        "validade_segundos": payload.validade_segundos,
        "token": token,
    }


@app.get("/api/v1/admin/qr-codes")
async def list_qr_codes(request: Request, status_filter: str = ""):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""SELECT q.id, q.student_id, s.student_name, q.token, q.bus_id,
                q.boarding_location, q.created_at, q.expires_at, q.used_at, q.invalidated_at
                FROM qr_codes q JOIN students s ON s.student_id=q.student_id
                WHERE (%s='' OR CASE WHEN q.invalidated_at IS NOT NULL THEN 'INVALIDO'
                    WHEN q.used_at IS NOT NULL THEN 'UTILIZADO'
                    WHEN q.expires_at < CURRENT_TIMESTAMP THEN 'EXPIRADO' ELSE 'VALIDO' END=%s)
                ORDER BY q.created_at DESC""", (status_filter, status_filter))
            result = []
            for row in cursor.fetchall():
                current = "INVALIDO" if row[9] else "UTILIZADO" if row[8] else "EXPIRADO" if row[7] < datetime.now(row[7].tzinfo) else "VALIDO"
                result.append({"id": row[0], "student_id": row[1], "student_name": row[2], "token": row[3], "bus_id": row[4], "boarding_location": row[5], "created_at": row[6].isoformat(), "expires_at": row[7].isoformat(), "used_at": row[8].isoformat() if row[8] else None, "status": current})
            return result
    finally:
        if conn is not None:
            conn.close()


@app.post("/api/v1/admin/qr-codes/{qr_id}/invalidate")
async def invalidate_qr_code(qr_id: int, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE qr_codes SET invalidated_at=CURRENT_TIMESTAMP WHERE id=%s AND used_at IS NULL RETURNING token", (qr_id,))
            row = cursor.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="QR Code não encontrado ou já utilizado.")
        conn.commit()
        try:
            redis_client.zrem("qr:valid", row[0])
        except redis.RedisError:
            pass
        return {"invalidated": True}
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/v1/admin/rotas")
async def list_routes(request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""SELECT r.id, r.name, r.status, r.driver, r.monitor, r.bus_id,
                b.bus_number, COUNT(rs.student_id), COUNT(DISTINCT st.id)
                FROM routes r LEFT JOIN buses b ON b.id=r.bus_id
                LEFT JOIN route_students rs ON rs.route_id=r.id LEFT JOIN route_stops st ON st.route_id=r.id
                GROUP BY r.id, b.bus_number ORDER BY r.name""")
            return [{"id": r[0], "name": r[1], "status": r[2], "driver": r[3] or "", "monitor": r[4] or "", "bus_id": r[5], "bus_number": r[6] or "", "student_count": r[7], "stop_count": r[8]} for r in cursor.fetchall()]
    finally:
        if conn is not None:
            conn.close()


@app.post("/api/v1/admin/rotas")
async def create_route(payload: RoutePayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO routes (name,bus_id,driver,monitor,status) VALUES (%s,%s,%s,%s,%s) RETURNING id", (payload.name.strip(), payload.bus_id, payload.driver.strip(), payload.monitor.strip(), payload.status))
            route_id = cursor.fetchone()[0]
        conn.commit()
        return {"id": route_id}
    finally:
        if conn is not None:
            conn.close()


@app.put("/api/v1/admin/rotas/{route_id}")
async def update_route(route_id: int, payload: RoutePayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE routes SET name=%s,bus_id=%s,driver=%s,monitor=%s,status=%s WHERE id=%s RETURNING id", (payload.name.strip(), payload.bus_id, payload.driver.strip(), payload.monitor.strip(), payload.status, route_id))
            if cursor.fetchone() is None:
                raise HTTPException(status_code=404, detail="Rota não encontrada.")
        conn.commit()
        return {"updated": True}
    finally:
        if conn is not None:
            conn.close()


@app.post("/api/v1/admin/rotas/{route_id}/pontos")
async def add_route_stop(route_id: int, payload: StopPayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO route_stops (route_id,name,address,stop_order) VALUES (%s,%s,%s,%s) RETURNING id", (route_id, payload.name.strip(), payload.address.strip(), payload.stop_order))
            stop_id = cursor.fetchone()[0]
        conn.commit()
        return {"id": stop_id}
    finally:
        if conn is not None:
            conn.close()


@app.put("/api/v1/admin/rotas/{route_id}/alunos/{student_id}")
async def assign_route_student(route_id: int, student_id: str, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO route_students (route_id,student_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", (route_id, student_id))
        conn.commit()
        return {"assigned": True}
    finally:
        if conn is not None:
            conn.close()


@app.delete("/api/v1/admin/rotas/{route_id}/alunos/{student_id}")
async def remove_route_student(route_id: int, student_id: str, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM route_students WHERE route_id=%s AND student_id=%s", (route_id, student_id))
        conn.commit()
        return {"removed": True}
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/v1/admin/embarques")
async def list_boardings(request: Request, start: str = "", end: str = "", search: str = "", student_id: str = "", bus_id: str = "", route_id: str = ""):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        clauses = ["e.event_type='STUDENT_BOARDED'"]
        params: list[object] = []
        if start:
            clauses.append("e.created_at >= %s::date")
            params.append(start)
        if end:
            clauses.append("e.created_at < (%s::date + INTERVAL '1 day')")
            params.append(end)
        if search:
            clauses.append("(e.student_name ILIKE %s OR e.student_id ILIKE %s OR e.bus_id ILIKE %s)")
            params.extend([f"%{search}%"] * 3)
        if student_id:
            clauses.append("e.student_id=%s")
            params.append(student_id)
        if bus_id:
            clauses.append("e.bus_id=%s")
            params.append(bus_id)
        if route_id:
            clauses.append("EXISTS (SELECT 1 FROM route_students rs WHERE rs.student_id=e.student_id AND rs.route_id=%s)")
            params.append(route_id)
        with conn.cursor() as cursor:
            cursor.execute(f"""SELECT e.id,e.student_id,e.student_name,e.bus_id,e.created_at,e.event_type,
                e.boarding_location,e.qr_token, r.name
                FROM student_events e LEFT JOIN route_students rs ON rs.student_id=e.student_id
                LEFT JOIN routes r ON r.id=rs.route_id WHERE {' AND '.join(clauses)}
                ORDER BY e.created_at DESC LIMIT 500""", params)
            return [{"id": r[0], "student_id": r[1], "student_name": r[2], "bus_id": r[3], "created_at": r[4].isoformat(), "status": "CONFIRMADO", "boarding_location": r[6], "qr_token": r[7], "route": r[8] or "Não vinculada"} for r in cursor.fetchall()]
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/v1/admin/relatorios")
async def boarding_report(request: Request, start: str = "", end: str = ""):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        date_filter = "e.event_type='STUDENT_BOARDED'"
        params: list[object] = []
        if start:
            date_filter += " AND e.created_at >= %s::date"
            params.append(start)
        if end:
            date_filter += " AND e.created_at < (%s::date + INTERVAL '1 day')"
            params.append(end)
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT student_id), COUNT(DISTINCT bus_id) FROM student_events e WHERE {date_filter}", params)
            totals = cursor.fetchone()
            cursor.execute(f"SELECT bus_id, COUNT(*) FROM student_events e WHERE {date_filter} GROUP BY bus_id ORDER BY COUNT(*) DESC", params)
            by_bus = [{"label": r[0], "value": r[1]} for r in cursor.fetchall()]
            cursor.execute(f"SELECT student_id, student_name, COUNT(*) FROM student_events e WHERE {date_filter} GROUP BY student_id, student_name ORDER BY COUNT(*) DESC LIMIT 100", params)
            by_student = [{"student_id": r[0], "label": r[1], "value": r[2]} for r in cursor.fetchall()]
        return {"total_boardings": totals[0], "students": totals[1], "buses": totals[2], "by_bus": by_bus, "by_student": by_student}
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/v1/admin/usuarios")
async def list_users(request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""SELECT u.id, u.username, u.display_name, u.email, u.role, u.is_active,
                u.created_at, u.bus_id, u.route_id, b.bus_number, r.name
                FROM app_users u LEFT JOIN buses b ON b.id=u.bus_id LEFT JOIN routes r ON r.id=u.route_id
                ORDER BY u.display_name""")
            return [{"id": r[0], "username": r[1], "display_name": r[2], "email": r[3] or "", "role": r[4], "is_active": r[5], "created_at": r[6].isoformat(), "bus_id": r[7], "route_id": r[8], "bus_number": r[9] or "", "route_name": r[10] or ""} for r in cursor.fetchall()]
    finally:
        if conn is not None:
            conn.close()


@app.post("/api/v1/admin/usuarios")
async def create_user(payload: UserPayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    password_hash = hashlib.sha256(payload.password.encode()).hexdigest() if payload.password else None
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO app_users (username,display_name,email,role,is_active,password_hash,bus_id,route_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id", (payload.username.strip(), payload.display_name.strip(), payload.email.strip(), payload.role, payload.is_active, password_hash, payload.bus_id if payload.role == "MONITOR" else None, payload.route_id if payload.role == "MONITOR" else None))
            user_id = cursor.fetchone()[0]
        conn.commit()
        return {"id": user_id}
    except psycopg2.IntegrityError as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=409, detail="Usuário já cadastrado.") from exc
    finally:
        if conn is not None:
            conn.close()


@app.put("/api/v1/admin/usuarios/{user_id}")
async def update_user(user_id: int, payload: UserPayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            if payload.password:
                cursor.execute("UPDATE app_users SET username=%s,display_name=%s,email=%s,role=%s,is_active=%s,password_hash=%s,bus_id=%s,route_id=%s WHERE id=%s RETURNING id", (payload.username.strip(), payload.display_name.strip(), payload.email.strip(), payload.role, payload.is_active, hashlib.sha256(payload.password.encode()).hexdigest(), payload.bus_id if payload.role == "MONITOR" else None, payload.route_id if payload.role == "MONITOR" else None, user_id))
            else:
                cursor.execute("UPDATE app_users SET username=%s,display_name=%s,email=%s,role=%s,is_active=%s,bus_id=%s,route_id=%s WHERE id=%s RETURNING id", (payload.username.strip(), payload.display_name.strip(), payload.email.strip(), payload.role, payload.is_active, payload.bus_id if payload.role == "MONITOR" else None, payload.route_id if payload.role == "MONITOR" else None, user_id))
            if cursor.fetchone() is None:
                raise HTTPException(status_code=404, detail="Usuário não encontrado.")
        conn.commit()
        return {"updated": True}
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/v1/admin/configuracoes")
async def get_settings(request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT key,value FROM app_settings ORDER BY key")
            return {r[0]: r[1] for r in cursor.fetchall()}
    finally:
        if conn is not None:
            conn.close()


@app.put("/api/v1/admin/configuracoes")
async def save_settings(payload: SettingPayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            for key, value in payload.settings.items():
                cursor.execute("INSERT INTO app_settings (key,value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value,updated_at=CURRENT_TIMESTAMP", (key, value))
        conn.commit()
        return {"saved": True}
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/v1/admin/alunos")
async def list_students(request: Request, search: str = "", status_filter: str = ""):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT student_id, student_name, class_name, guardian_name, guardian_phone,
                          status, created_at
                   FROM students
                   WHERE (%s = '' OR student_name ILIKE %s OR student_id ILIKE %s)
                     AND (%s = '' OR status = %s)
                   ORDER BY student_name;""",
                (search.strip(), f"%{search.strip()}%", f"%{search.strip()}%", status_filter, status_filter),
            )
            return [
                {
                    "student_id": row[0], "student_name": row[1], "class_name": row[2] or "",
                    "guardian_name": row[3] or "", "guardian_phone": row[4] or "",
                    "status": row[5], "created_at": row[6].isoformat(),
                }
                for row in cursor.fetchall()
            ]
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail="Cadastro de alunos indisponível.") from exc
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/v1/admin/alunos/{student_id}")
async def student_details(student_id: str, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT student_id, student_name, class_name, guardian_name, guardian_phone, status, created_at FROM students WHERE student_id = %s", (student_id,))
            student = cursor.fetchone()
            if student is None:
                raise HTTPException(status_code=404, detail="Aluno não encontrado.")
            cursor.execute("SELECT id, bus_id, created_at, boarding_location FROM student_events WHERE student_id = %s AND event_type = 'STUDENT_BOARDED' ORDER BY created_at DESC LIMIT 100", (student_id,))
            history = [{"id": row[0], "bus_id": row[1], "created_at": row[2].isoformat(), "boarding_location": row[3]} for row in cursor.fetchall()]
        return {"student_id": student[0], "student_name": student[1], "class_name": student[2] or "", "guardian_name": student[3] or "", "guardian_phone": student[4] or "", "status": student[5], "created_at": student[6].isoformat(), "boarding_history": history}
    finally:
        if conn is not None:
            conn.close()


@app.get("/admin/alunos", response_class=HTMLResponse)
async def admin_students_page(request: Request):
    redirect = require_admin(request)
    if redirect:
        return redirect
    data = get_dashboard_data()
    return templates.TemplateResponse(request, "page.html", {
        "title": "Alunos",
        "active_page": "alunos",
        **data,
    })


@app.post("/admin/alunos")
async def register_student(request: Request, payload: StudentRegistrationPayload):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT nextval('student_id_sequence');")
            numeric_id = cursor.fetchone()[0]
            if numeric_id > 99999:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Limite de IDs de alunos atingido.")
            student_id = f"{numeric_id:05d}"
            cursor.execute("""INSERT INTO students
                (student_id, student_name, class_name, guardian_name, guardian_phone, status)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING student_id;""",
                (student_id, payload.student_name.strip(), payload.class_name.strip(), payload.guardian_name.strip(), payload.guardian_phone.strip(), payload.status))
        conn.commit()
        return {"student_id": student_id, "student_name": payload.student_name.strip(), "status": payload.status}
    except HTTPException:
        if conn is not None:
            conn.rollback()
        raise
    except psycopg2.IntegrityError as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Não foi possível criar um ID único.") from exc
    except psycopg2.Error as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Cadastro de alunos indisponível.") from exc
    finally:
        if conn is not None:
            conn.close()


async def publish_to_rabbitmq(event_data: dict):
    """Publica o evento de embarque no RabbitMQ usando a conexão async persistente."""
    channel = await ensure_rabbitmq_channel()
    if channel is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Serviço de mensageria temporariamente indisponível.",
        )
    try:
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(event_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="student_events",
        )
    except Exception as e:
        logger.error("Falha ao publicar evento: %s", e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Serviço de mensageria temporariamente indisponível. Erro interno: {str(e)}"
        )


@app.post("/scan", status_code=status.HTTP_200_OK)
async def process_qr_scan(payload: ScanPayload):
    start_time = time.time()

    event_data = payload.model_dump()
    event_data["event_type"] = "STUDENT_BOARDED"

    await publish_to_rabbitmq(event_data)

    execution_time_ms = (time.time() - start_time) * 1000

    logger.info(
        "Embarque registrado: student_id=%s bus_id=%s latency_ms=%.2f",
        payload.student_id,
        payload.bus_id,
        round(execution_time_ms, 2),
    )

    return {
        "status": "success",
        "message": "Embarque registrado e enviado para processamento!",
        "latency_ms": round(execution_time_ms, 2)
    }


@app.get("/trip/{bus_id}/status")
async def get_trip_status(bus_id: str):
    cached_status = redis_client.get(f"bus:{bus_id}:students")

    if cached_status:
        return {
            "bus_id": bus_id,
            "source": "redis_cache",
            "students_boarded": json.loads(cached_status)
        }

    return {
        "bus_id": bus_id,
        "source": "redis_cache",
        "students_boarded": [],
        "message": "Nenhum aluno registrado para esta van até o momento."
    }


@app.get("/", include_in_schema=False)
async def application_root(request: Request):
    role = get_session_role(request)
    if get_session_user(request) and role == "ADMINISTRADOR":
        return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
    if get_session_user(request) and role == "MONITOR":
        return RedirectResponse(url="/monitor", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_form(request: Request):
    session_role = get_session_role(request)
    if get_session_user(request) and session_role:
        destination = "/admin" if session_role == "ADMINISTRADOR" else "/monitor"
        return RedirectResponse(url=destination, status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(request, "login.html", {"error": None})


@app.post("/admin/login")
async def admin_login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    authenticated = username == ADMIN_USER and password == ADMIN_PASSWORD
    authenticated_role = "ADMINISTRADOR" if authenticated else None
    user_claims = {}
    if not authenticated:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT username, password_hash, role, bus_id, route_id FROM app_users WHERE username = %s AND is_active = TRUE",
                    (username.strip(),),
                )
                user = cursor.fetchone()
            authenticated = bool(
                user
                and user[1]
                and hmac.compare_digest(
                    user[1], hashlib.sha256(password.encode("utf-8")).hexdigest()
                )
            )
            if authenticated:
                authenticated_role = user[2]
                user_claims = {"bus_id": user[3], "route_id": user[4]}
        except psycopg2.Error:
            logger.warning("Não foi possível consultar os usuários cadastrados no login.")
        finally:
            if conn is not None:
                conn.close()

    if authenticated:
        token = create_admin_session(username, authenticated_role or "ADMINISTRADOR", **user_claims)
        destination = "/admin" if authenticated_role == "ADMINISTRADOR" else "/monitor"
        response = RedirectResponse(url=destination, status_code=status.HTTP_303_SEE_OTHER)
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=token,
            max_age=SESSION_MAX_AGE_SECONDS,
            httponly=True,
            secure=SESSION_COOKIE_SECURE,
            samesite="lax",
        )
        return response

    return templates.TemplateResponse(
        request,
        "login.html",
        {"error": "Usuário ou senha inválidos."},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


@app.get("/admin/logout")
async def admin_logout():
    response = RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


def get_system_status() -> dict:
    event_count = 0
    postgres_ok = False
    for attempt in range(2):
        conn = None
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM student_events;")
                event_count = cursor.fetchone()[0]
            postgres_ok = True
            break
        except Exception as e:
            if attempt == 1:
                logger.warning("Postgres indisponível: %s", e)
        finally:
            if conn is not None:
                conn.close()

    active_buses = 0
    redis_ok = False
    try:
        active_buses = len(redis_client.keys("bus:*:students"))
        redis_ok = True
    except Exception as e:
        logger.warning("Redis indisponível: %s", e)

    return {
        "event_count": event_count,
        "active_buses": active_buses,
        "api_available": True,
        "postgres_ok": postgres_ok,
        "redis_ok": redis_ok,
    }


def get_dashboard_data() -> dict:
    metrics = {
        "total_students": 0,
        "today_boardings": 0,
        "active_buses": 0,
        "valid_qr_codes": 0,
        "recent_boardings": [],
        "daily_boardings": [],
        "monthly_boardings": [],
        "quarterly_boardings": [],
        "today_boardings_delta": 0,
        "students_by_bus": [],
        "operation_buses": [],
        "peak_hours": [],
        "route_usage": [],
        "students": [],
    }
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT student_id, student_name, created_at
                FROM students
                ORDER BY created_at DESC
                LIMIT 100;
                """
            )
            metrics["students"] = [
                {
                    "student_id": student_id,
                    "student_name": student_name,
                    "created_at": created_at.strftime("%d/%m/%Y %H:%M"),
                }
                for student_id, student_name, created_at in cursor.fetchall()
            ]

            cursor.execute(
                """
                          SELECT COUNT(DISTINCT student_id),
                              COUNT(*) FILTER (WHERE created_at::date = CURRENT_DATE),
                              COUNT(*) FILTER (WHERE created_at::date = CURRENT_DATE - INTERVAL '1 day')
                  FROM student_events
                  WHERE event_type = 'STUDENT_BOARDED';
                """
            )
            total_students, today_boardings, yesterday_boardings = cursor.fetchone()
            metrics["total_students"] = total_students
            metrics["today_boardings"] = today_boardings
            metrics["today_boardings_delta"] = today_boardings - yesterday_boardings

            cursor.execute(
                """
                SELECT TO_CHAR(day, 'DD/MM') AS label, COUNT(*) AS total
                FROM generate_series(CURRENT_DATE - INTERVAL '6 days', CURRENT_DATE, INTERVAL '1 day') AS day
                LEFT JOIN student_events ON student_events.created_at::date = day::date
                    AND student_events.event_type = 'STUDENT_BOARDED'
                GROUP BY day
                ORDER BY day;
                """
            )
            metrics["daily_boardings"] = [
                {"label": label, "value": total}
                for label, total in cursor.fetchall()
            ]

            for key, days in (("monthly_boardings", 29), ("quarterly_boardings", 89)):
                cursor.execute(
                    """
                    SELECT TO_CHAR(day, 'DD/MM') AS label, COUNT(e.id) AS total
                    FROM generate_series(CURRENT_DATE - %s * INTERVAL '1 day', CURRENT_DATE, INTERVAL '1 day') AS day
                    LEFT JOIN student_events e ON e.created_at::date = day::date
                        AND e.event_type = 'STUDENT_BOARDED'
                    GROUP BY day ORDER BY day;
                    """,
                    (days,),
                )
                metrics[key] = [{"label": label, "value": total} for label, total in cursor.fetchall()]

            cursor.execute(
                """
                SELECT bus_id, COUNT(*) AS total
                FROM student_events
                WHERE event_type = 'STUDENT_BOARDED'
                GROUP BY bus_id
                ORDER BY total DESC, bus_id
                LIMIT 8;
                """
            )
            metrics["students_by_bus"] = [
                {"label": bus_id, "value": total}
                for bus_id, total in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT TO_CHAR(created_at, 'HH24:00') AS label, COUNT(*) AS total
                FROM student_events
                WHERE event_type = 'STUDENT_BOARDED'
                GROUP BY EXTRACT(HOUR FROM created_at), TO_CHAR(created_at, 'HH24:00')
                ORDER BY EXTRACT(HOUR FROM created_at);
                """
            )
            metrics["peak_hours"] = [
                {"label": label, "value": total}
                for label, total in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT COALESCE(NULLIF(boarding_location, ''), 'Não informado') AS label,
                       COUNT(*) AS total
                FROM student_events
                WHERE event_type = 'STUDENT_BOARDED'
                GROUP BY 1
                ORDER BY total DESC, label
                LIMIT 8;
                """
            )
            metrics["route_usage"] = [
                {"label": label, "value": total}
                for label, total in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT id, student_id, student_name, created_at, bus_id, event_type
                FROM student_events
                                WHERE event_type = 'STUDENT_BOARDED'
                                    AND created_at::date = CURRENT_DATE
                ORDER BY created_at DESC
                LIMIT 10;
                """
            )
            metrics["recent_boardings"] = [
                {
                    "id": event_id,
                    "student_id": student_id,
                    "student_name": student_name,
                    "time": created_at.strftime("%d/%m/%Y %H:%M"),
                    "bus_id": bus_id,
                    "status": "Confirmado" if event_type == "STUDENT_BOARDED" else event_type,
                    "created_at": created_at.isoformat(),
                }
                for event_id, student_id, student_name, created_at, bus_id, event_type in cursor.fetchall()
            ]

            cursor.execute("SELECT bus_number, capacity, status FROM buses ORDER BY bus_number;")
            metrics["operation_buses"] = [
                {"bus_id": bus_number, "capacity": capacity, "status": status, "boarded": 0, "occupancy": 0}
                for bus_number, capacity, status in cursor.fetchall()
            ]
    except Exception as exc:
        logger.warning("Não foi possível carregar os dados do dashboard: %s", exc)
    finally:
        if conn is not None:
            conn.close()

    try:
        metrics["active_buses"] = len(redis_client.keys("bus:*:students"))
        active_bus_cache = {}
        for key in redis_client.keys("bus:*:students"):
            bus_id = key[len("bus:"):-len(":students")]
            active_bus_cache[bus_id] = len(json.loads(redis_client.get(key) or "[]"))
        known_buses = {item["bus_id"]: item for item in metrics["operation_buses"]}
        for bus_id, boarded in active_bus_cache.items():
            known_buses.setdefault(bus_id, {"bus_id": bus_id, "capacity": 0, "status": "ATIVO", "boarded": 0, "occupancy": 0})
            known_buses[bus_id]["boarded"] = boarded
        for bus in known_buses.values():
            capacity = bus["capacity"]
            bus["occupancy"] = round((bus["boarded"] / capacity) * 100) if capacity else 0
            if bus["status"] != "ATIVO":
                bus["operational_status"] = "inativo"
            elif bus["occupancy"] >= 90:
                bus["operational_status"] = "quase lotado"
            elif bus["boarded"]:
                bus["operational_status"] = "em operação"
            else:
                bus["operational_status"] = "aguardando"
        metrics["operation_buses"] = sorted(known_buses.values(), key=lambda item: item["bus_id"])
        redis_client.zremrangebyscore("qr:valid", "-inf", int(time.time()))
        metrics["valid_qr_codes"] = redis_client.zcard("qr:valid")
        blocked_ids = redis_client.smembers("blocked_students")
        for boarding in metrics["recent_boardings"]:
            if boarding["student_id"] in blocked_ids:
                boarding["status"] = "Bloqueado"
    except redis.RedisError as exc:
        logger.warning("Não foi possível carregar métricas do Redis: %s", exc)

    return metrics


@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request):
    redirect = require_admin(request)
    if redirect:
        return redirect

    status_data = get_system_status()
    dashboard_data = get_dashboard_data()
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "grafana_url": GRAFANA_URL,
            "event_count": status_data["event_count"],
            "active_buses": status_data["active_buses"],
            "system_healthy": status_data["api_available"],
            "postgres_ok": status_data["postgres_ok"],
            "redis_ok": status_data["redis_ok"],
            **dashboard_data,
        },
    )


@app.get("/api/v1/admin/onibus")
async def list_buses(request: Request, search: str = "", status_filter: str = ""):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""SELECT b.id, b.bus_number, b.plate, b.capacity, b.driver, b.monitor,
                b.status, COUNT(e.id) FILTER (WHERE e.event_type='STUDENT_BOARDED' AND e.created_at::date=CURRENT_DATE)
                FROM buses b LEFT JOIN student_events e ON e.bus_id = b.bus_number
                WHERE (%s='' OR b.bus_number ILIKE %s OR COALESCE(b.plate,'') ILIKE %s)
                  AND (%s='' OR b.status=%s)
                GROUP BY b.id ORDER BY b.bus_number""",
                (search.strip(), f"%{search.strip()}%", f"%{search.strip()}%", status_filter, status_filter))
            return [{"id": r[0], "bus_number": r[1], "plate": r[2] or "", "capacity": r[3], "driver": r[4] or "", "monitor": r[5] or "", "status": r[6], "boarded_today": r[7]} for r in cursor.fetchall()]
    finally:
        if conn is not None:
            conn.close()


@app.post("/api/v1/admin/onibus")
async def create_bus(payload: BusPayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""INSERT INTO buses (bus_number, plate, capacity, driver, monitor, status)
                VALUES (%s,%s,%s,%s,%s,%s) RETURNING id""", (payload.bus_number.strip(), payload.plate.strip() or None, payload.capacity, payload.driver.strip(), payload.monitor.strip(), payload.status))
            bus_id = cursor.fetchone()[0]
        conn.commit()
        return {"id": bus_id, "bus_number": payload.bus_number.strip()}
    except psycopg2.IntegrityError as exc:
        if conn is not None:
            conn.rollback()
        raise HTTPException(status_code=409, detail="Número ou placa já cadastrados.") from exc
    finally:
        if conn is not None:
            conn.close()


@app.put("/api/v1/admin/onibus/{bus_id}")
async def update_bus(bus_id: int, payload: BusPayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""UPDATE buses SET bus_number=%s, plate=%s, capacity=%s, driver=%s, monitor=%s, status=%s
                WHERE id=%s RETURNING id""", (payload.bus_number.strip(), payload.plate.strip() or None, payload.capacity, payload.driver.strip(), payload.monitor.strip(), payload.status, bus_id))
            if cursor.fetchone() is None:
                raise HTTPException(status_code=404, detail="Ônibus não encontrado.")
        conn.commit()
        return {"updated": True}
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/v1/admin/onibus/{bus_id}")
async def bus_details(bus_id: int, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, bus_number, plate, capacity, driver, monitor, status FROM buses WHERE id=%s", (bus_id,))
            bus = cursor.fetchone()
            if bus is None:
                raise HTTPException(status_code=404, detail="Ônibus não encontrado.")
            cursor.execute("SELECT id, student_id, student_name, created_at, boarding_location FROM student_events WHERE bus_id=%s AND event_type='STUDENT_BOARDED' ORDER BY created_at DESC LIMIT 100", (bus[1],))
            history = [{"id": r[0], "student_id": r[1], "student_name": r[2], "created_at": r[3].isoformat(), "boarding_location": r[4]} for r in cursor.fetchall()]
        return {"id": bus[0], "bus_number": bus[1], "plate": bus[2] or "", "capacity": bus[3], "driver": bus[4] or "", "monitor": bus[5] or "", "status": bus[6], "boarding_history": history}
    finally:
        if conn is not None:
            conn.close()


@app.get("/admin/alunos", response_class=HTMLResponse)
@app.get("/admin/onibus", response_class=HTMLResponse)
@app.get("/admin/rotas", response_class=HTMLResponse)
@app.get("/admin/qr-codes", response_class=HTMLResponse)
@app.get("/admin/embarques", response_class=HTMLResponse)
@app.get("/admin/relatorios", response_class=HTMLResponse)
@app.get("/admin/usuarios", response_class=HTMLResponse)
@app.get("/admin/simulacao", response_class=HTMLResponse)
@app.get("/admin/configuracoes", response_class=HTMLResponse)
async def admin_module_page(request: Request):
    redirect = require_admin(request)
    if redirect:
        return redirect

    page = request.url.path.removeprefix("/admin/")
    titles = {
        "alunos": "Alunos",
        "onibus": "Ônibus",
        "rotas": "Rotas",
        "qr-codes": "QR Codes",
        "embarques": "Embarques",
        "relatorios": "Relatórios",
        "usuarios": "Usuários",
        "simulacao": "Simulação",
        "configuracoes": "Configurações",
    }
    data = {"title": titles[page], "active_page": page}
    if page in {"alunos", "qr-codes", "embarques"}:
        dashboard_data = get_dashboard_data()
        data.update(dashboard_data)
    return templates.TemplateResponse(request, "page.html", data)


def get_notifications() -> list[dict]:
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT student_id, student_name, bus_id, event_type, boarding_at, message, status, created_at
                FROM notifications
                ORDER BY created_at DESC
                LIMIT 50;
                """
            )
            return [
                {
                    "student_id": student_id,
                    "student_name": student_name,
                    "bus_id": bus_id,
                    "event_type": event_type,
                    "boarding_at": boarding_at,
                    "message": message,
                    "status": notification_status,
                    "created_at": created_at.isoformat(),
                    "time": created_at.strftime("%d/%m/%Y %H:%M"),
                }
                for student_id, student_name, bus_id, event_type, boarding_at, message, notification_status, created_at
                in cursor.fetchall()
            ]
    except psycopg2.Error as exc:
        logger.error("Não foi possível consultar notificações: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Registro de notificações temporariamente indisponível.",
        ) from exc
    finally:
        if conn is not None:
            conn.close()


@app.get("/admin/notificacoes", response_class=HTMLResponse)
async def admin_notifications_page(request: Request):
    redirect = require_admin(request)
    if redirect:
        return redirect
    return templates.TemplateResponse(request, "notifications.html", {
        "title": "Notificações",
        "active_page": "notificacoes",
        "notifications": get_notifications(),
    })


@app.get("/api/v1/admin/notificacoes")
async def admin_notifications_api(request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")
    return get_notifications()


@app.get("/admin/alunos-bloqueados")
async def blocked_students(request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")
    try:
        return sorted(redis_client.smembers("blocked_students"))
    except redis.RedisError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Serviço de bloqueios indisponível.") from exc


@app.post("/admin/alunos-bloqueados")
async def block_student(payload: StudentBlockPayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")
    try:
        redis_client.sadd("blocked_students", payload.student_id.strip())
        return {"blocked": True, "student_id": payload.student_id.strip()}
    except redis.RedisError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Serviço de bloqueios indisponível.") from exc


@app.delete("/admin/alunos-bloqueados/{student_id}")
async def unblock_student(student_id: str, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")
    try:
        redis_client.srem("blocked_students", student_id.strip())
        return {"unblocked": True, "student_id": student_id.strip()}
    except redis.RedisError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Serviço de bloqueios indisponível.") from exc


@app.post("/admin/simulate")
async def admin_simulate_start(
    request: Request,
    min_count: int = Form(...),
    max_count: int = Form(...),
    confirm_password: str = Form(...),
):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")

    if confirm_password != ADMIN_SIMULATE_PASSWORD:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Senha de confirmação incorreta.")

    if active_job_id is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Já existe uma simulação em andamento.")

    min_count = max(1, min_count)
    max_count = min(max(max_count, min_count), SIMULATION_HARD_CAP)
    min_count = min(min_count, max_count)

    job_id = await start_simulation(min_count, max_count)
    return {"job_id": job_id}


@app.post("/admin/simulate/stop/{job_id}")
async def admin_simulate_stop(request: Request, job_id: str):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")

    if job_id not in simulation_jobs:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job não encontrado.")

    stop_simulation(job_id)
    return {"stopping": True}


@app.get("/admin/simulate/stream/{job_id}")
async def admin_simulate_stream(request: Request, job_id: str):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")

    if job_id not in simulation_jobs:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job não encontrado.")

    return StreamingResponse(
        event_stream(job_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.put("/admin/embarques/{event_id}")
async def update_boarding(event_id: int, payload: BoardingUpdatePayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")

    conn = None
    try:
        boarding_at = datetime.fromisoformat(payload.boarding_at.replace("Z", "+00:00"))
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE student_events
                SET student_id = %s, bus_id = %s, timestamp = %s, created_at = %s
                WHERE id = %s AND event_type = 'STUDENT_BOARDED'
                RETURNING id;
                """,
                (
                    payload.student_id.strip(),
                    payload.bus_id.strip(),
                    boarding_at.timestamp(),
                    boarding_at,
                    event_id,
                ),
            )
            if cursor.fetchone() is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Embarque não encontrado.")
        conn.commit()
        return {"updated": True}
    except HTTPException:
        if conn is not None:
            conn.rollback()
        raise
    except psycopg2.Error as exc:
        if conn is not None:
            conn.rollback()
        logger.error("Não foi possível editar o embarque %s: %s", event_id, exc)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Banco indisponível.") from exc
    finally:
        if conn is not None:
            conn.close()


@app.delete("/admin/embarques/{event_id}")
async def delete_boarding(event_id: int, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sessão inválida.")

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            connect_timeout=max(1, int(SERVICE_CONNECT_TIMEOUT_SECONDS)),
        )
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM student_events WHERE id = %s AND event_type = 'STUDENT_BOARDED' RETURNING id;",
                (event_id,),
            )
            if cursor.fetchone() is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Embarque não encontrado.")
        conn.commit()
        return {"deleted": True}
    except HTTPException:
        if conn is not None:
            conn.rollback()
        raise
    except psycopg2.Error as exc:
        if conn is not None:
            conn.rollback()
        logger.error("Não foi possível excluir o embarque %s: %s", event_id, exc)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Banco indisponível.") from exc
    finally:
        if conn is not None:
            conn.close()


@app.put("/admin/alunos/{student_id}")
async def update_student(student_id: str, payload: StudentUpdatePayload, request: Request):
    if not get_session_user(request):
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""UPDATE students SET student_name=%s, class_name=%s, guardian_name=%s,
                guardian_phone=%s, status=%s WHERE student_id=%s RETURNING student_id""",
                (payload.student_name.strip(), payload.class_name.strip(), payload.guardian_name.strip(), payload.guardian_phone.strip(), payload.status, student_id))
            if cursor.fetchone() is None:
                raise HTTPException(status_code=404, detail="Aluno não encontrado.")
        conn.commit()
        return {"updated": True, "student_id": student_id}
    except HTTPException:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()