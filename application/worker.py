import json
import logging
import os
import time
import pika
import psycopg2
import redis

from local_store import claim_batch, init_local_store, mark_failed, mark_sent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("async_worker")

# Variáveis de ambiente
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "127.0.0.1")
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "smart_cities_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/offline_events.db")
OFFLINE_BATCH_SIZE = int(os.getenv("OFFLINE_BATCH_SIZE", "100"))
OFFLINE_SYNC_INTERVAL_SECONDS = float(os.getenv("OFFLINE_SYNC_INTERVAL_SECONDS", "5"))
OFFLINE_PROCESSING_TIMEOUT_SECONDS = float(os.getenv("OFFLINE_PROCESSING_TIMEOUT_SECONDS", "300"))
OFFLINE_RETRY_DELAY_SECONDS = float(os.getenv("OFFLINE_RETRY_DELAY_SECONDS", "30"))

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

def get_db_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )

def init_db():
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS student_events (
                id SERIAL PRIMARY KEY,
                student_id VARCHAR(50) NOT NULL,
                bus_id VARCHAR(50) NOT NULL,
                monitor_id VARCHAR(50),
                event_type VARCHAR(50) NOT NULL,
                timestamp DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_student_events_created_at ON student_events(created_at);
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(" [PostgreSQL] Tabela 'student_events' verificada/atualizada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inicializar tabela no PostgreSQL: {e}")


def publish_local_batch(channel) -> int:
    events = claim_batch(
        SQLITE_PATH,
        OFFLINE_BATCH_SIZE,
        OFFLINE_PROCESSING_TIMEOUT_SECONDS,
    )
    if not events:
        return 0

    sent_count = 0
    try:
        for event in events:
            channel.basic_publish(
                exchange="",
                routing_key="student_events",
                body=json.dumps(event).encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            mark_sent(SQLITE_PATH, [event["event_id"]])
            sent_count += 1
        logger.info("[SQLite] Lote enviado: %s eventos.", sent_count)
        return sent_count
    except Exception:
        remaining_ids = [event["event_id"] for event in events[sent_count:]]
        mark_failed(SQLITE_PATH, remaining_ids, OFFLINE_RETRY_DELAY_SECONDS)
        logger.exception("[SQLite] Falha ao enviar lote; eventos marcados para retry.")
        raise


def process_event(ch, method, properties, body):
    """Callback invocado sempre que uma mensagem chega no RabbitMQ."""
    event = json.loads(body.decode("utf-8"))
    logger.info(f" [Worker] Mensagem recebida da fila: {event}")
    
    student_id = event.get("student_id")
    bus_id = event.get("bus_id")
    monitor_id = event.get("monitor_id", "DESCONHECIDO")
    event_type = event.get("event_type", "STUDENT_BOARDED")
    timestamp = event.get("timestamp", time.time())

    try:
        # Gravação no PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO student_events (student_id, bus_id, monitor_id, event_type, timestamp)
        VALUES (%s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (student_id, bus_id, monitor_id, event_type, timestamp))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(" [PostgreSQL] Evento 'STUDENT_BOARDED' gravado com sucesso!")

        # Atualização do Cache no Redis
        redis_key = f"bus:{bus_id}:students"
        current_cache = redis_client.get(redis_key)
        students_list = json.loads(current_cache) if current_cache else []
        
        if student_id not in students_list:
            students_list.append(student_id)
            redis_client.set(redis_key, json.dumps(students_list))
            
        logger.info(" [Redis] Cache atualizado para o App dos Pais.")

        # Confirma o processamento para o RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Erro durante o processamento do evento: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_worker():
    init_local_store(SQLITE_PATH)
    init_db()

    while True:
        connection = None
        try:
            logger.info("[Worker] Verificando conexão de rede com RabbitMQ...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, connection_attempts=1)
            )
            channel = connection.channel()
            channel.queue_declare(queue="student_events", durable=True)
            channel.confirm_delivery()
            channel.basic_qos(prefetch_count=1)
            logger.info("[Worker] RabbitMQ conectado; sincronização offline ativa.")

            while connection.is_open:
                try:
                    publish_local_batch(channel)
                except (pika.exceptions.AMQPError, OSError):
                    break

                messages = channel.consume(
                    queue="student_events",
                    inactivity_timeout=OFFLINE_SYNC_INTERVAL_SECONDS,
                    auto_ack=False,
                )
                method, properties, body = next(messages)
                if method is not None:
                    process_event(channel, method, properties, body)
                channel.cancel()
        except (pika.exceptions.AMQPError, OSError) as exc:
            logger.warning("[Worker] RabbitMQ indisponível: %s", exc)
            time.sleep(OFFLINE_SYNC_INTERVAL_SECONDS)
        finally:
            if connection is not None and connection.is_open:
                connection.close()

if __name__ == "__main__":
    start_worker()