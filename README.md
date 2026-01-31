# Multi-Exchange Orderbook Ingestor

Complete production-ready orderbook ingestion system with Docker support for 6 exchanges: **Binance, Bybit, OKX, Bitget, Huobi Global, Coinbase**.

Collects: **BTC, ETH, XRP, SOL, DOGE** pairs with **USDT & USDC** quotes.

## Quick Start (Docker - Recommended)

Everything runs in Docker. Just one command:

```bash
docker compose up
```

This starts:
- **QuestDB** (time-series database on port 9000 UI, 9009 ILP)
- **Orderbook ingestion** (connects to all 6 exchanges, publishes to ZeroMQ)

### View QuestDB UI
Open browser to: http://localhost:9000

### Subscribe to live orderbook updates (optional)

In another terminal inside the container:
```bash
docker compose exec orderbook python zmq_subscriber.py
```

Or from your host (if you have zmq installed locally):
```bash
python zmq_subscriber.py
```

---

## Local Setup (Without Docker)

If you prefer to run locally without Docker:

1) Install Python dependencies

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

2) Start QuestDB manually (or use Docker just for QuestDB)

```bash
docker compose up questdb -d
```

3) Run the ingestor

```bash
python orderbook.py config.json
```

4) (Optional) Run ZeroMQ subscriber in another terminal

```bash
python zmq_subscriber.py
```

---

## Configuration

Edit `config.json` to:
- Enable/disable exchanges
- Adjust recovery parameters
- Change QuestDB host/port
- Modify ZeroMQ endpoint

---

## Architecture

- **Exchanges**: Each exchange has a WebSocket ingestor that validates sequence numbers and recovers from gaps.
- **QuestDB**: Stores orderbook snapshots and updates as time-series data.
- **ZeroMQ**: Real-time PUB/SUB for downstream consumers.
- **Metrics**: Tracks latency, gaps, recovery success rate every 10 seconds.

---

## Future Expansion

To add spot/future/option across all exchange intersections:
- Extend `config.json` `symbols` section with product type
- Add per-exchange product mappings
- Expand symbol generation logic in `orderbook.py`

Ready when you are!
