import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Optional

import aio_pika
import psycopg2
import redis
from fastapi import FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from auth import create_admin_session, get_session_user, require_admin
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
    SIMULATION_HARD_CAP,
)
from simulation import active_job_id, event_stream, simulation_jobs, start_simulation, stop_simulation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_gateway")

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

rabbitmq_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
rabbitmq_channel: Optional[aio_pika.abc.AbstractChannel] = None


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
    await ensure_rabbitmq_channel()
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


class ScanPayload(BaseModel):
    student_id: str
    bus_id: str
    monitor_id: str
    timestamp: float = Field(default_factory=time.time)


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


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_form(request: Request):
    if get_session_user(request):
        return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(request, "login.html", {"error": None})


@app.post("/admin/login")
async def admin_login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    if username == ADMIN_USER and password == ADMIN_PASSWORD:
        token = create_admin_session(username)
        response = RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
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
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            connect_timeout=2,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM student_events;")
        event_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        postgres_ok = True
    except Exception as e:
        logger.warning("Postgres indisponível: %s", e)

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
        "healthy": postgres_ok and redis_ok,
    }


@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request):
    redirect = require_admin(request)
    if redirect:
        return redirect

    status_data = get_system_status()
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "grafana_url": GRAFANA_URL,
            "event_count": status_data["event_count"],
            "active_buses": status_data["active_buses"],
            "system_healthy": status_data["healthy"],
        },
    )


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