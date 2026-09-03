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
Добавить volume для Kafka
Посмотреть как делается singletone


Game_event: id_event: UUID, event_type: LowCardinality(T), time: DateTime64, text: String

Сделать метод который будет отпарвлять событие в Kafka -> consumer (будет считать время и доступен по веб-интерфейсу) -
> handler -> clickhouse


сделать партиций столько сколько consumer


-----------------------------------------
Метод который отправляет что то на расчет, составить гороскоп, внутри ответ рандомное число, sleep несколько секунд
Сделать consumer который который читает id запроса и включает значение положил значение что он над ним работает, после сделано
чтобы фронтенд мог получить состояние таска



-----------------------------------------
Вопросы по Kafka:
зачем consumer
зачем topic
как организовать правильно партиции (в соответствии с counsumer) 
сколько хранятся данные
как почистить место на диске (volume)
как понять что все consumer успевают за producer
```
