import os, json, asyncio, signal
import asyncpg
from aiokafka import AIOKafkaConsumer

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "telemetry-consumer")


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    await consumer.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        while not stop.is_set():
            msg = await consumer.getone()
            data = msg.value
            device_id = str(data["id"])
            value = float(data["value"])
            typ = str(data.get("type", "temperature"))
            async with pool.acquire() as con:
                await con.execute(
                    "INSERT INTO telemetry (device_id, value, type) VALUES ($1, $2, $3);",
                    device_id,
                    value,
                    typ
                )
    finally:
        await consumer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
