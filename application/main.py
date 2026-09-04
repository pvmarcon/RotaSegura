import json
import logging
import os
import time
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import pika
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_gateway")

app = FastAPI(
    title="Smart City Bus Tracking API",
    description="API Gateway com validação síncrona e despacho assíncrono para RabbitMQ",
    version="1.0.0"
)

# Configuração de conexões
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "127.0.0.1")
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")

# Conexão com o Redis
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

# Schema do Payload recebido no QR Code
class ScanPayload(BaseModel):
    student_id: str
    bus_id: str
    monitor_id: str
    timestamp: float = Field(default_factory=time.time)

def publish_to_rabbitmq(event_data: dict):
    """Publica o evento de embarque na fila do RabbitMQ de forma assíncrona."""
    try:
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=5672,
            blocked_connection_timeout=3
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue="student_events", durable=True)
        
        channel.basic_publish(
            exchange="",
            routing_key="student_events",
            body=json.dumps(event_data),
            properties=pika.BasicProperties(
                delivery_mode=2  # Mensagem persistente
            )
        )
        connection.close()
    except Exception as e:
        logger.error(f"Erro exato ao conectar no RabbitMQ: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Serviço de mensageria temporariamente indisponível. Erro interno: {str(e)}"
        )

# Rota do QR Code Scan
@app.post("/scan", status_code=status.HTTP_200_OK)
async def process_qr_scan(payload: ScanPayload):
    start_time = time.time()
    
    event_data = payload.model_dump()
    event_data["event_type"] = "STUDENT_BOARDED"
    
    publish_to_rabbitmq(event_data)
    
    execution_time_ms = (time.time() - start_time) * 1000
    
    logger.info(
        json.dumps({
            "event": "QR_SCAN_SUCCESS",
            "student_id": payload.student_id,
            "bus_id": payload.bus_id,
            "latency_ms": round(execution_time_ms, 2)
        })
    )
    
    return {
        "status": "success",
        "message": "Embarque registrado e enviado para processamento!",
        "latency_ms": round(execution_time_ms, 2)
    }

# Rota pra checar a situação da viagem
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