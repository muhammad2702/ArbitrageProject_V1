import json
import time
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch
from decimal import Decimal

# Configuration
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topic": "orderbook_data"
}

POSTGRESQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "orderbook_db",
    "user": "elliot",
    "password": "elliot"
}

BATCH_SIZE = 100  # Number of records to process in one batch

def create_table(cursor):
    """Create the orderbook table if it doesn't exist."""
    table_query = """
    CREATE TABLE IF NOT EXISTS orderbook (
        id SERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        market TEXT NOT NULL,
        timestamp DECIMAL NOT NULL,
        bids JSONB,
        asks JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(table_query)

    # Create indexes for faster querying
    index_query = """
    CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp ON orderbook (timestamp);
    CREATE INDEX IF NOT EXISTS idx_orderbook_market ON orderbook (market);
    """
    cursor.execute(index_query)

def insert_batch(cursor, records):
    """Insert a batch of records into the orderbook table."""
    query = """
    INSERT INTO orderbook (
        source,
        market,
        timestamp,
        bids,
        asks
    ) VALUES (%s, %s, %s, %s, %s);
    """
    print(f"Executing query: {query}")
    #print(f"Records to insert: {records}")
    execute_batch(cursor, query, records)

def main():
    """Main function to consume Kafka messages and insert into PostgreSQL."""
    try:
        # Connect to PostgreSQL
        postgres_connection = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = postgres_connection.cursor()

        # Create table and indexes if they don't exist
        create_table(cursor)

        # Set up Kafka consumer
        kafka_consumer = KafkaConsumer(
            KAFKA_CONFIG["topic"],
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )

        batch = []

        for message in kafka_consumer:
            try:
                # Extract message values
                message_value = message.value
                source = message_value.get("source")
                market = message_value.get("market")
                timestamp = message_value.get("timestamp")
                bids = message_value.get("bids", [])
                asks = message_value.get("asks", [])

                # Validate required fields
                if not all([source, market, timestamp]):
                    print("Missing required fields in message")
                    continue

                # Convert bids and asks to JSON strings
                bids_json = json.dumps(bids)
                asks_json = json.dumps(asks)

                # Convert timestamp to Decimal
                
                timestamp_decimal = Decimal(timestamp)
             

                # Create record tuple
                record = (
                    source,
                    market,
                    timestamp_decimal,
                    bids_json,
                    asks_json
                )

                # Debug print to inspect the record
                print("Processing record")

                # Add to batch
                batch.append(record)

                # Process batch when it reaches the batch size
                if len(batch) >= BATCH_SIZE:
                    insert_batch(cursor, batch)
                    postgres_connection.commit()
                    batch = []
                    print(f"Inserted {BATCH_SIZE} records successfully")

            except Exception as e:
                print(f"Error processing message: {e}")
                continue

    except KeyboardInterrupt:
        print("Consumer stopped by user")
    except Exception as e:
        print(f"Critical error: {e}")
    finally:
        # Insert any remaining records in the batch
        if batch:
            insert_batch(cursor, batch)
            postgres_connection.commit()
        # Clean up connections
        cursor.close()
        postgres_connection.close()

if __name__ == "__main__":
    main()
