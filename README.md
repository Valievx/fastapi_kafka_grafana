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

Метрику переделать чтобы consumer считал RPS и producer
Настроить grafana чтобы получать RPS consumer и RPS producer

Делать по одной задаче в день с leetcode

Найти в clickhosue системные таблицы, успевает он индексировать или нет
Курс Clickhouse (Суммирование на лету через SummingMergeTree)


Самопрезентация

Переделать резюме:
Свершенные действия + цифры % 
- Повысил покрытие проекта unit-тестами до 80% legacy с использованием Pytest
```
