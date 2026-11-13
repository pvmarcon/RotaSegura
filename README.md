# RotaSegura

## Persistência offline-first

Os eventos de embarque são gravados primeiro no SQLite local (`SQLITE_PATH`).
A API não depende do RabbitMQ para aceitar um embarque. O `worker.py` verifica a
conexão com o RabbitMQ e publica os eventos em lotes, respeitando os estados:

- `pending`: aguardando sincronização;
- `processing`: lote reservado por um worker;
- `sent`: confirmado pelo RabbitMQ;
- `failed`: falha temporária, aguardando nova tentativa.

Em Docker, o banco local fica no volume `offline_data`. Para execução local,
crie a pasta `application/data` ou defina `SQLITE_PATH` para um caminho
persistente compartilhado entre a API e o worker.

Variáveis opcionais:

- `OFFLINE_BATCH_SIZE` (padrão: `100`);
- `OFFLINE_SYNC_INTERVAL_SECONDS` (padrão: `5`);
- `OFFLINE_PROCESSING_TIMEOUT_SECONDS` (padrão: `300`);
- `OFFLINE_RETRY_DELAY_SECONDS` (padrão: `30`).