# Coinbase DW

Headless runners for ingesting Coinbase data into Deepwater and building orderbook snapshots.

## Setup
```bash
cd ~/coinbase-dw
python -m venv venv
source venv/bin/activate
pip install -e /home/dan/Deepwater
```

## Commands
- Ingest trades + L2:
  ```bash
  python ingest.py --base-path /data/coinbase-test --products BTC-USD ETH-USD
  ```
- Build orderbook snapshots:
  ```bash
  python snapshots.py --base-path /data/coinbase-test --products BTC-USD ETH-USD --depth 500 --interval 1.0
  ```
- Read trades (window or tail):
  ```bash
  python read_trades.py --base-path /data/coinbase-test --feed CB-TRADES-BTC-USD --start 10:00 --end 10:05
  # or tail live
  python read_trades.py --base-path /data/coinbase-test --feed CB-TRADES-BTC-USD
  ```
- UI (curses with latency/stats):
  ```bash
  python ui.py --base-path /data/coinbase-test
  # or specify feeds to show
  python ui.py --base-path /data/coinbase-test --feeds CB-TRADES-BTC-USD CB-L2-BTC-USD CB-L2SNAP-BTC-USD
  ```
