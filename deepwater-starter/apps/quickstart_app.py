#!/usr/bin/env python3
"""
Minimal Deepwater integration app.

Purpose:
- prove end-to-end wiring
- show writer + reader usage
- provide a copyable skeleton for real services
"""
from __future__ import annotations

import os
import time

from deepwater import Platform


def main() -> None:
    base_path = os.environ.get("DEEPWATER_BASE", "./data")
    p = Platform(base_path)

    # This app expects feeds created from ./configs.
    required = ("trades", "quotes")
    missing = [name for name in required if not p.feed_exists(name)]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(
            f"Missing feed(s): {names}. Run deepwater-create-feed --base-path {base_path} --config-dir ./configs"
        )

    now_us = int(time.time() * 1_000_000)

    tw = p.create_writer("trades")
    tw.write_values(now_us, 100.25, 1.50)
    tw.close()

    qw = p.create_writer("quotes")
    qw.write_values(now_us, 100.20, 100.30)
    qw.close()

    tr = p.create_reader("trades")
    qr = p.create_reader("quotes")

    trades = tr.latest(60)
    quotes = qr.latest(60)

    print(f"base_path={base_path}")
    print(f"trades_records={len(trades)} latest_trade={trades[-1] if trades else None}")
    print(f"quotes_records={len(quotes)} latest_quote={quotes[-1] if quotes else None}")

    tr.close()
    qr.close()
    p.close()


if __name__ == "__main__":
    main()
