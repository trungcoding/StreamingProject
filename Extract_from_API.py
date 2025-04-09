import websocket
import json

# Thay YOUR_API_KEY bằng API key của bạn từ Finnhub
API_KEY = "cvquj51r01qp88cnn6rgcvquj51r01qp88cnn6s0"
WS_URL = f"wss://ws.finnhub.io?token={API_KEY}"

def on_message(ws, message):
    data = json.loads(message)
    if data["type"] == "trade":  # Chỉ xử lý dữ liệu loại "trade"
        for trade in data["data"]:  # Lặp qua từng giao dịch trong danh sách
            symbol = trade["s"]
            price = trade["p"]
            timestamp = trade["t"]
            print(f"Symbol: {symbol}, Price: {price}, Timestamp: {timestamp}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed: {close_status_code} - {close_msg}")

def on_open(ws):
    print("WebSocket connection opened")
    ws.send(json.dumps({"type": "subscribe", "symbol": "AAPL"}))

if __name__ == "__main__":
    websocket.enableTrace(True)  # Bật trace để debug nếu cần
    ws = websocket.WebSocketApp(
        WS_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever()