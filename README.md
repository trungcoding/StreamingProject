# Real-time Bitcoin Price Monitoring System

## Overview
Hệ thống theo dõi giá Bitcoin thời gian thực, thu thập dữ liệu từ Finnhub API qua WebSocket, gửi message từ Producer tới Consumer bằng Kafka, transform bằng Python, lưu trữ trong PostgreSQL và trực quan hóa trên Grafana.

## Features
- Thu thập giá Bitcoin (`BINANCE:BTCUSDT`) từ Finnhub WebSocket.
- Pipeline dữ liệu qua Kafka với topic `BITCOIN_prices`.
- Xử lý dữ liệu (loại bỏ trùng lặp, chuẩn hóa dữ liệu)
- Lưu trữ dữ liệu vào PostgreSQL với 25 message và 6 record mỗi giây.
- Dashboard Grafana hiển thị giá và hiệu suất real-time.

## Tech Stack
- **Python**: Programming Language
- **Kafka**: Message queue
- **PostgreSQL**: Database
- **Grafana**: Visualization
- **Docker**: Containerization

## Setup
1. **Yêu cầu**:
   - Docker, Python 3.12, PostgreSQL, Grafana, Kafka
   - API Key từ [Finnhub](https://finnhub.io/)
2. **Hình ảnh thực tế**:
![Docker](images/Docker.jpg)
![Kafka](images/Kafka.jpg)
![PostgreSQL](images/Postgres.jpg)
![Grafana](Grafana/Kafka.jpg)
