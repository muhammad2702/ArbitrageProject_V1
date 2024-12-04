# redis_client.py
import redis
import json
import time  # Import time module for timestamping errors

class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0):
        try:
            self.client = redis.Redis(host=host, port=port, db=db)
            # Test the connection
            self.client.ping()
            print(f"Connected to Redis at {host}:{port}, DB: {db}")
        except redis.exceptions.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            exit(1)

    def store_order_book(self, exchange, symbol, data, terminal_callback=False):
        try:
            timestamp = int(data.get('timestamp'))
            key = f"orderbook:{exchange}:{symbol}:{timestamp}"
            # Convert the order book to a JSON string for storage
            value = json.dumps(data)
            self.client.set(key, value)
            if terminal_callback:
                print(f"Stored {exchange} order book for {symbol} at {timestamp}")
        except TypeError as e:
            print(f"[{exchange}] Error: {e}")
            self.log_error(exchange, symbol, str(e))
        except Exception as e:
            print(f"[{exchange}] Unexpected error: {e}")
            self.log_error(exchange, symbol, str(e))

    # Add the log_error method
    def log_error(self, exchange, symbol, message):
        error_key = f"errors:{exchange}:{symbol}"
        timestamp = int(time.time() * 1000000)  # Get current time in microseconds
        error_message = f"{timestamp}: {message}"
        self.client.rpush(error_key, error_message)
