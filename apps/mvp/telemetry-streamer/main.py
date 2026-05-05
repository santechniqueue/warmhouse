import os, json, asyncio, random, signal
import asyncpg
from aiokafka import AIOKafkaProducer

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "10"))


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await producer.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        while not stop.is_set():
            async with pool.acquire() as con:
                rows = await con.fetch(
                    "SELECT id FROM devices ORDER BY id;"
                )
            print(f"{len(rows)} rows fetched")
            send_tasks = []
            for r in rows:
                device_id = r["id"]
                payload = {
                    "id": device_id,
                    "value": round(random.uniform(-20.0, 60.0), 2),
                    "type": "temperature",
                }
                key = f'Device_{str(device_id)}'
                send_tasks.append(producer.send_and_wait(KAFKA_TOPIC, payload, key=key.encode()))
                print(f"message sent", payload)

            if send_tasks:
                await asyncio.gather(*send_tasks)

            try:
                await asyncio.wait_for(stop.wait(), timeout=POLL_INTERVAL_SEC)
            except asyncio.TimeoutError:
                pass
    finally:
        await producer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
