# ClickHouse — внешние интеграции

- **Credentials ClickHouse:** см. `config/users.xml`
- **Credentials MinIO:** см. `../minio/config/minio.env`
- **Credentials Redis:** см. `../redis/config/redis.conf`

Подключение к ClickHouse:

```bash
docker exec -it ch01 clickhouse-client --user admin --password '<CLICKHOUSE_ADMIN_PASSWORD>'
```

---

## Интеграции

| Интеграция | Документация | Описание |
| --- | --- | --- |
| MinIO (S3) | [minio.md](minio.md) | Чтение/запись CSV, Parquet, S3 engine, storage policies |
| Redis | [redis.md](redis.md) | Redis table engine, Redis Dictionary, JOIN |
| Kafka | [kafka.md](kafka.md) | Kafka engine, Materialized View, потоковый приём данных |
| Quotes Pipeline | [quotes.md](quotes.md) | Котировки: Kafka → тики → OHLC (1s, 1m, 1h), reload |
