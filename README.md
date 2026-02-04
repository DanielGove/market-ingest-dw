# Coinbase DW

Coinbase market data ingestion, order book reconstruction, and market making backtesting using Deepwater.

## Features

- **Real-time ingestion** of Coinbase L2 and trade data
- **Order book snapshots** built from L2 updates
- **Market making backtesting** framework with PnL tracking
- **Live monitoring** UI with latency stats
- **Microsecond precision** time-series storage using Deepwater

## Setup

```bash
cd ~/coinbase-dw
python -m venv venv
source venv/bin/activate
pip install -e /home/dan/Deepwater  # Adjust path as needed
```

## Quick Start

### 1. Collect Data

Start data ingestion (runs continuously):
```bash
python ingest.py --base-path data/coinbase-test --products BTC-USD ETH-USD
```

Build order book snapshots (in another terminal):
```bash
python snapshots.py --base-path data/coinbase-test --products BTC-USD ETH-USD --depth 500 --interval 1.0
```

### 2. Run Backtests

After collecting some data, run a market making backtest:
```bash
python backtest_mm.py \
  --base-path data/coinbase-test \
  --product BTC-USD \
  --start 10:00 \
  --end 12:00 \
  --spread-bps 5 \
  --size 0.01
```

## Commands

### Data Collection

**Start pipeline for a product:**
```bash
./run.sh BTC-USD              # Starts ingestion + snapshots for BTC-USD
./run.sh ETH-USD              # Add another product
./run.sh SOL-USD              # Add another product
```

**Check status:**
```bash
python ingest_client.py status    # Daemon status
python ingest_client.py metrics   # Performance metrics
python ingest_client.py list      # List all active products
```

**Stop pipeline:**
```bash
./stop.sh BTC-USD    # Stop just BTC-USD
./stop.sh            # Stop everything
```

**How it works:**
- Single daemon handles ingestion for all products
- Each product gets its own snapshot builder process (500 levels, 1s interval)
- Each product gets its own order book publisher (200 levels, 200ms interval)
- **Trading strategies should consume `OB200200-{PRODUCT}` feeds**

### Data Analysis

**Read trades (window or tail):**
```bash
# Time window
python read_trades.py --base-path data/coinbase-test --feed CB-TRADES-BTC-USD --start 10:00 --end 10:05

# Tail live
python read_trades.py --base-path data/coinbase-test --feed CB-TRADES-BTC-USD
```

**Monitor with UI:**
```bash
# Auto-detect feeds
python ui.py --base-path data/coinbase-test

# Specify feeds
python ui.py --base-path data/coinbase-test --feeds CB-TRADES-BTC-USD CB-L2-BTC-USD CB-L2SNAP-BTC-USD
```

### Backtesting

**Run market making backtest:**
```bash
python backtest_mm.py \
  --base-path data/coinbase-test \
  --product BTC-USD \
  --start 09:00 \
  --end 17:00 \
  --spread-bps 5 \
  --size 0.01 \
  --max-position 0.1
```

**Parameters:**
- `--spread-bps`: Spread in basis points (5 = 0.05%)
- `--size`: Quote size on each side
- `--max-position`: Maximum absolute position
- `--maker-fee-bps`: Maker fee in bps (negative = rebate)
- `--no-position-skew`: Disable position-based skewing

## Documentation

See [GUIDE.md](GUIDE.md) for:
- Complete Deepwater library overview
- Architecture explanation
- Strategy customization guide
- Performance characteristics
- Troubleshooting tips
