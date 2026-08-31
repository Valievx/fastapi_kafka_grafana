import clickhouse_connect

from common.settings import settings


class ClickHouseClient:

    def __init__(self):
        self.client = None

    async def connect(self):
        self.client = clickhouse_connect.get_client(
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
