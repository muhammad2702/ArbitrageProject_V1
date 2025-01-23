import psycopg2

POSTGRESQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "orderbook_db",
    "user": "postgres",
    "password": "postgres"
}

def create_database():
    conn = psycopg2.connect(
        host=POSTGRESQL_CONFIG["host"],
        port=POSTGRESQL_CONFIG["port"],
        user=POSTGRESQL_CONFIG["user"],
        password=POSTGRESQL_CONFIG["password"]
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Create database
    cursor.execute(f"CREATE DATABASE {POSTGRESQL_CONFIG['database']};")
    print(f"Database {POSTGRESQL_CONFIG['database']} created successfully")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_database()
