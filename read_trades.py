"""
Read trades over a time window or tail live.

Usage:
  python -m app.read_trades --base-path data/coinbase-test --feed CB-TRADES-BTC-USD --start 10:00 --end 10:05
"""
import argparse
import logging
from datetime import datetime, time as dtime, timezone
from pathlib import Path

from deepwater.platform import Platform
from deepwater.utils.timestamps import us_to_iso


def parse_hms(arg: str) -> int:
    parts = arg.split(":")
    if len(parts) not in (2, 3):
        raise ValueError("time must be HH:MM or HH:MM:SS")
    hh, mm = int(parts[0]), int(parts[1])
    ss = int(parts[2]) if len(parts) == 3 else 0
    today = datetime.now(timezone.utc).date()
    dt = datetime.combine(today, dtime(hour=hh, minute=mm, second=ss), tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def parse_args():
    ap = argparse.ArgumentParser(description="Read trades from Deepwater.")
    ap.add_argument("--base-path", default="/data/coinbase-test", help="Deepwater base path")
    ap.add_argument("--feed", required=True, help="Feed name (e.g., CB-TRADES-BTC-USD)")
    ap.add_argument("--start", help="HH:MM[:SS] UTC start (default: live tail)")
    ap.add_argument("--end", help="HH:MM[:SS] UTC end (optional)")
    ap.add_argument("--log-level", default="INFO", help="Logging level")
    return ap.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("dw.read_trades")
    platform = Platform(base_path=args.base_path)
    reader = platform.create_reader(args.feed)
    fields = [f["name"] for f in reader.record_format["fields"] if f.get("name") != "_"]

    start_ts = parse_hms(args.start) if args.start else None
    end_ts = parse_hms(args.end) if args.end else None

    if start_ts is None:
        stream = reader.stream_latest_records(playback=False)
    elif end_ts is None:
        stream = reader.stream_time_range(start_ts)
    else:
        stream = reader.stream_time_range(start_ts, end_ts)

    try:
        for rec in stream:
            rec_map = dict(zip(fields, rec))
            ts = rec_map.get("ev_us") or rec_map.get("proc_us") or rec_map.get("ts")
            side = rec_map.get("side")
            price = rec_map.get("price")
            size = rec_map.get("size")
            log.info("ts=%s side=%s price=%.8f size=%.8f",
                     us_to_iso(ts) if ts else "?",
                     side.decode() if isinstance(side, (bytes, bytearray)) else side,
                     price or 0.0, size or 0.0)
    except KeyboardInterrupt:
        log.info("Stopped.")
    finally:
        reader.close()
        platform.close()


if __name__ == "__main__":
    main()
