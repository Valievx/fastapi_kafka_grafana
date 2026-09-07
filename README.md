```commandline
Swagger = http://127.0.0.1:8000/api/docs
Kafka = http://127.0.0.1:8090
Gragana = http://127.0.0.1:3000


Создание таблицы:
docker exec -i clickhouse clickhouse-client --database analytics < clickhouse/init.sql

Зайти в ClickHouse:
docker exec -it clickhouse clickhouse-client

Посмотреть таблицы:
SHOW TABLES;

Проверить количество записей:
docker exec clickhouse clickhouse-client --query "SELECT count() FROM analytics.game_events"
```




```commandline
Сделать mock отправки 1000000 событий 
Добавиьт batch в consumer


```
1. В consumer добавлен batch:
```python
class KafkaConsumer:

    def __init__(self, topic: str, consumer_id: int, group_id: str, handler: Callable):
        ...
        self.batch_size = 1000
        ...

    
    ...
    async def _consume(self):
        batch = []

        async for message in self.consumer:
            batch.append(message.value)

            if len(batch) >= self.batch_size:
                await self._process(batch)
                batch = []

    async def _process(self, batch: list[dict]):
        started = time.perf_counter()

        try:
            await self.handler(batch)

            elapsed = time.perf_counter() - started

            self.processed_messages += len(batch)
            self.total_time += elapsed
            self.last_time = elapsed
            self.max_time = max(self.max_time, elapsed)

        except Exception as e:
            logger.error(f"Consumer {self.consumer_id} failed: {type(e).__name__}: {e}")




```
2. Метод producer с send_and_wait изменен на send:
```python
class KafkaProducer:
    ...

    async def send(self, event: EventSchema, topic: str):
        await self.producer.send(
            topic=topic,
            value=event.model_dump_json().encode("utf-8")
        )
```

3. Клиент ClickHouse переписан на асинхронный:
```python
class ClickHouseClient:

    def __init__(self):
        self.client = None

    async def connect(self):
        self.client = await clickhouse_connect.get_async_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DB,
            connect_timeout=30,
            send_receive_timeout=30,
        )

    async def close(self):
        if self.client:
            self.client.close()

clickhouse_client = ClickHouseClient()

Первый результат обработки 1000000: 2h 38min 5.04s
Результат после рефакторинга: 14.24s
```


1. Метрику переделать чтобы consumer считал RPS и producer
2. Настроить grafana чтобы получать RPS consumer и RPS producer
3. Найти в clickhosue системные таблицы, успевает он индексировать или нет
4. Курс Clickhouse (Суммирование на лету через SummingMergeTree)
5. Самопрезентация
```
