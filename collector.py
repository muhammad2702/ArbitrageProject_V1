# collector.py
import yaml
import threading
import time
from redis_client import RedisClient
from exchanges import handle_binance, handle_bitstamp

def load_config(config_path='config.yaml'):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    config = load_config()

    # Initialize Redis client
    redis_conf = config['redis']
    redis_client = RedisClient(
        host=redis_conf.get('host', 'localhost'),
        port=redis_conf.get('port', 6379),
        db=redis_conf.get('db', 0)
    )

    # Define threads for each exchange
    threads = []

    universal_symbols = config['symbols']
    symbol_mappings = config.get('symbol_mappings', {})

    if 'binance' in config['exchanges']:
        binance_config = config['exchanges']['binance']
        binance_symbols = [
            symbol_mappings['binance'][sym] for sym in universal_symbols
            if sym in symbol_mappings['binance']
        ]
        thread_binance = threading.Thread(
            target=handle_binance,
            args=(redis_client, binance_symbols, binance_config)
        )
        threads.append(thread_binance)

    if 'bitstamp' in config['exchanges']:
        bitstamp_config = config['exchanges']['bitstamp']
        bitstamp_symbols = [
            symbol_mappings['bitstamp'][sym] for sym in universal_symbols
            if sym in symbol_mappings['bitstamp']
        ]
        thread_bitstamp = threading.Thread(
            target=handle_bitstamp,
            args=(redis_client, bitstamp_symbols, bitstamp_config)
        )
        threads.append(thread_bitstamp)

    # Start all threads
    for thread in threads:
        thread.start()

    # Run for the specified duration
    duration = config['collection'].get('duration_seconds', 604800)  # Default to 1 week
    print(f"Data collection started. Running for {duration} seconds.")
    end_time = time.time() + duration
    try:
        while time.time() < end_time:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Data collection interrupted by user.")
    finally:
        print("Data collection completed. Stopping threads.")
        # Threads will close when their WebSocket connections are closed

if __name__ == "__main__":
    main()
