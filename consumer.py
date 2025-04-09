from kafka import KafkaConsumer
import json
import psycopg2
import time
from collections import defaultdict

KAFKA_TOPIC = "BITCOIN_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_CONFIG = {
    "dbname": "stock_db",
    "user": "root",
    "password": "root",
    "host": "localhost",
    "port": "5432"
}

# Khởi tạo Kafka Consumer
print("Starting Kafka Consumer...")
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="bitcoin_consumer_group",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print("Kafka Consumer initialized successfully")
except Exception as e:
    print(f"Failed to initialize Kafka Consumer: {e}")
    exit(1)

# Kết nối PostgreSQL
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Connected to PostgreSQL")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bitcoin_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20),
            price DOUBLE PRECISION,
            trade_timestamp BIGINT,
            UNIQUE (symbol, trade_timestamp)
        )
    """)
    conn.commit()
    print("Table created or already exists")
except Exception as e:
    print(f"Failed to connect to PostgreSQL: {e}")
    exit(1)

INSERT_QUERY = """
    INSERT INTO bitcoin_prices (symbol, price, trade_timestamp)
    VALUES (%s, %s, %s)
    ON CONFLICT (symbol, trade_timestamp) DO NOTHING
"""

# Biến để đếm
message_count = defaultdict(int)  # Số message nhận được từ Kafka mỗi giây
insert_count = defaultdict(int)   # Số record chèn thành công vào DB mỗi giây
last_time = int(time.time())

print(f"Waiting for messages from Kafka on topic '{KAFKA_TOPIC}'...")
try:
    for message in consumer:
        price_data = message.value
        current_time = int(time.time())
        
        # Đếm số message nhận được
        message_count[current_time] += 1
        print(f"Received price from Kafka: {price_data}")
        
        # Chèn vào PostgreSQL và đếm số lần chèn thành công
        values = (price_data["symbol"], price_data["price"], price_data["timestamp"])
        try:
            cursor.execute(INSERT_QUERY, values)
            # Kiểm tra số hàng bị ảnh hưởng để xác định chèn thành công
            if cursor.rowcount > 0:
                insert_count[current_time] += 1
            conn.commit()
            print("Inserted price into PostgreSQL")
        except Exception as e:
            print(f"Error inserting into PostgreSQL: {e}")
            conn.rollback()
        
        # In kết quả khi chuyển sang giây mới
        if current_time > last_time:
            print(f"Time {last_time}: Messages received = {message_count[last_time]}, Records inserted = {insert_count[last_time]}")
            last_time = current_time

except KeyboardInterrupt:
    print("\nStopping Consumer manually...")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    # In số liệu cuối cùng trước khi dừng
    if message_count[last_time] > 0 or insert_count[last_time] > 0:
        print(f"Final count at {last_time}: Messages received = {message_count[last_time]}, Records inserted = {insert_count[last_time]}")
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals() and not conn.closed:
        conn.close()
    if 'consumer' in locals():
        consumer.close()
    print("Connections closed")