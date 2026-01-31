<<<<<<< HEAD
# FBA-TD
crypto development and hw
=======
# FBA-TD — Multi-Exchange Orderbook Ingestor

Purpose: lightweight, containerized multi-exchange orderbook ingestion pipeline.

Quick facts
- Exchanges: Binance, Bybit, OKX, Bitget, Huobi, Coinbase
- Symbols: configured in `config.json` (BTC, ETH, XRP, SOL, DOGE with USDT/USDC)
- Data level: Level-2 snapshot + incremental updates (book bids/asks up to configured depth)
- Storage: QuestDB (ILP TCP writes) — table configured via `config.json.questdb.table`
- Realtime pub: ZeroMQ PUB (optional if `pyzmq` installed)

Install (local, Python)
- Create venv (optional): `python -m venv .venv` && `.venv\Scripts\activate`
## FBA-TD — Multi-Exchange Orderbook Ingestor

Purpose: containerized multi-exchange Level-2 orderbook ingestion.

Quick facts
- Exchanges: Binance, Bybit, OKX, Bitget, Huobi, Coinbase
- Symbols: BTC, ETH, XRP, SOL, DOGE × USDT, USDC (configured in `config.json` / `generated_symbols`)
- Data: Level-2 (price levels with quantities); snapshots + incremental updates
- Storage: QuestDB (ILP TCP writes). Table set in `config.json.questdb.table` (example: `timequestdb`)
- Realtime: ZeroMQ PUB (if `pyzmq` installed)

Install (local)
- `python -m venv .venv`
- `.venv\Scripts\activate`
- `pip install -r requirements.txt`

Run (Docker - recommended)
- `docker compose up --build -d`
- `docker compose logs -f orderbook`
- `docker compose down`

Core components
- `orderbook.py` — main ingest process, adapters, trackers, recovery, QuestDB writer
- `config.json` — exchanges, symbols, questdb, zeromq, pipeline parameters
- `zmq_subscriber.py` — example subscriber for ZeroMQ (optional)

Symbol mapping (normalized vs exchange)
- Internal normalized: `BASE/QUOTE` (e.g., `BTC/USDT`)
- Exchange formats (examples):
  - Binance/Bybit: `BTCUSDT`
  - OKX/Coinbase: `BTC-USDT`
  - Bitget: `BTCUSDT_SPBL`
  - Huobi: `btcusdt` (lowercase)

Behavior notes
- On receiving updates: validate sequence with exchange-specific `SequenceTracker`.
- On gap: `SymbolValidator` triggers `recover()` → calls adapter `fetch_snapshot()` and replays buffered updates.
- Writer truncates/book-slices bids/asks and writes ILP lines to QuestDB.

Troubleshooting
- Many gaps: verify `generated_symbols` accuracy; check adapter symbol format for each exchange.
- Increase `pipeline.max_recovery_attempts` temporarily to avoid immediate shutdown while debugging.
- Add logging inside adapter `fetch_snapshot()` to capture HTTP status and response body.
- QuestDB issues: verify ILP port (9009) and `config.json.questdb.host`.

Commands
```powershell
# build & run
docker compose up --build -d

# logs
docker compose logs -f orderbook

# create questdb table example
curl -sS "http://localhost:9000/exec?query=CREATE TABLE IF NOT EXISTS timequestdb(timestamp TIMESTAMP, seq LONG)"
```

Files
- `orderbook.py` (main)
- `config.json` (symbols + mappings)
- `requirements.txt`

Notes
- This README is intentionally concise and technical. For changes, edit `generated_symbols` in `config.json` to match exchange REST naming.
