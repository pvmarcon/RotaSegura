import json
import logging
import os
import time
import pika
import psycopg2
import redis

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
    init_db()
    
    # Aguarda o RabbitMQ inicializar completamente
    time.sleep(2)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    
    channel.queue_declare(queue="student_events", durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="student_events", on_message_callback=process_event)

    logger.info(" [*] Worker ativo e aguardando eventos no RabbitMQ...")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()