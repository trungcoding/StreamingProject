import websocket
import json
from kafka import KafkaProducer
import time

API_KEY = "ADD_YOUR_KEY"
WS_URL = f"wss://ws.finnhub.io?token={API_KEY}"
KAFKA_TOPIC = "BITCOIN_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

print("Initializing Kafka Producer...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=100000,
        buffer_memory=33554432,
        linger_ms=5
    )
    print("Kafka Producer initialized")
except Exception as e:
    print(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

def on_message(ws, message):
    data = json.loads(message)
    print(f"📨 Raw WebSocket data: {data}")
    if data.get("type") == "trade":
        for trade in data.get("data", []):
            price_data = {
                "symbol": trade.get("s"),
                "price": trade.get("p"),
                "timestamp": trade.get("t")
            }
            print(f"➡️ Sending to Kafka: {price_data}")
            try:
                producer.send(KAFKA_TOPIC, value=price_data)
                producer.flush()
            except Exception as e:
                print(f"Error sending to Kafka: {e}")
                producer.flush()

def on_open(ws):
    print("WebSocket opened")
    # ws.send(json.dumps({"type": "subscribe", "symbol": "AAPL"}))
    ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))
    time.sleep(0.2)

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} - {close_msg}")

def run_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_message=on_message,
                on_open=on_open,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever()
        except Exception as e:
            print(f"Error in WebSocket loop: {e}")
        print("Reconnecting in 5 seconds...")
        time.sleep(5)

if __name__ == "__main__":
    websocket.enableTrace(False)
    run_ws()
