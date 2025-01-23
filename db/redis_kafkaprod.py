import json
import time
from redis import Redis
from kafka import KafkaProducer

# Configuration
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}

KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topic": "orderbook_data"
}

def redis_key_filter(key):
    return key.startswith("orderbook:")

def main():
    redis_client = Redis(**REDIS_CONFIG)
    print(redis_client)
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print(kafka_producer)

    while True:
        try:
            # Get all matching keys from Redis
            keys = redis_client.keys("*")
            print(keys)
            orderbook_keys = [key.decode() for key in keys if redis_key_filter(key.decode())]

            for key in orderbook_keys:
                data = redis_client.get(key)
                if data:
                    orderbook_data = json.loads(data.decode())
                    orderbook_data["source"] = key.split(":")[1]  # Get exchange name
                    orderbook_data["market"] = key.split(":")[2]  # Get market pair
                    orderbook_data["timestamp"] = key.split(":")[3]  # Get timestamp

                    # Send to Kafka
                    kafka_producer.send(
                        KAFKA_CONFIG["topic"],
                        value=orderbook_data,
                        key=key.encode()
                    )

            # Sleep to avoid overwhelming the system
            time.sleep(1)

        except Exception as e:
            print(f"Error in producer: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
