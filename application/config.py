import os
from dotenv import load_dotenv

load_dotenv()


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Variável de ambiente obrigatória não configurada: {name}")
    return value


def get_default_host(service: str) -> str:
    return service if os.getenv("DOCKER_CONTAINER") == "1" else "127.0.0.1"


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", get_default_host("rabbitmq"))
REDIS_HOST = os.getenv("REDIS_HOST", get_default_host("redis"))

ADMIN_USER = get_required_env("ADMIN_USER")
ADMIN_PASSWORD = get_required_env("ADMIN_PASSWORD")
ADMIN_SIMULATE_PASSWORD = get_required_env("ADMIN_SIMULATE_PASSWORD")
SESSION_SECRET = get_required_env("SESSION_SECRET")
SESSION_MAX_AGE_SECONDS = int(os.getenv("SESSION_MAX_AGE_SECONDS", "14400"))
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8000")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://127.0.0.1:3000")

SIMULATION_MAX_CONCURRENCY = int(os.getenv("SIMULATION_MAX_CONCURRENCY", "25"))
SIMULATION_HARD_CAP = int(os.getenv("SIMULATION_HARD_CAP", "5000"))
SIMULATION_MIN_INTERVAL_S = float(os.getenv("SIMULATION_MIN_INTERVAL_S", "1"))
SIMULATION_MAX_INTERVAL_S = float(os.getenv("SIMULATION_MAX_INTERVAL_S", "3"))
BUCKET_WINDOW_SECONDS = 15

POSTGRES_HOST = os.getenv("POSTGRES_HOST", get_default_host("postgres"))
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "smart_cities_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

SESSION_COOKIE_NAME = "admin_session"
