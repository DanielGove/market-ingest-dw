"""
Prompt_toolkit dashboard for Deepwater feeds (read-only).
Polls feeds for latest records, computes lag and rough rps, and displays metadata.

Usage:
  python ui.py --base-path /data/coinbase-test
  python ui.py --base-path /data/coinbase-test --feeds CB-TRADES-BTC-USD CB-L2-BTC-USD CB-L2SNAP-BTC-USD
"""
import argparse
import time
import threading
from collections import deque
from typing import Dict, List, Optional, Tuple

from prompt_toolkit.application import Application
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import Layout, HSplit, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style
from prompt_toolkit.patch_stdout import patch_stdout

from deepwater.platform import Platform
from deepwater.utils.timestamps import us_to_iso


def parse_args():
    ap = argparse.ArgumentParser(description="Prompt_toolkit UI for Deepwater feeds (read-only).")
    ap.add_argument("--base-path", default="/data/coinbase-test", help="Deepwater base path")
    ap.add_argument("--feeds", nargs="+", help="Specific feeds to show (default: trades/L2/snap feeds)")
    ap.add_argument("--refresh-hz", type=float, default=2.0, help="UI refresh frequency")
    ap.add_argument("--poll-interval", type=float, default=0.5, help="Poll interval seconds")
    return ap.parse_args()


def _now_us() -> int:
    return time.time_ns() // 1_000


class FeedPoller:
    """Background poller that fetches latest records and computes lag/rps."""
    def __init__(self, base_path: str, feeds: List[str], poll_interval: float):
        self.platform = Platform(base_path=base_path)
        self.feeds = feeds
        self.poll_interval = poll_interval
        self.stats: Dict[str, dict] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="ui-poller", daemon=True)
        self._readers: Dict[str, Optional[object]] = {}

    def start(self):
        # preload metadata
        for f in self.feeds:
            try:
                meta = self.platform.describe_feed(f)
            except Exception as e:
                meta = {"err": str(e)}
            self.stats[f] = {
                "meta": meta,
                "err": None,
                "last_ts": None,
                "count": 0,
                "lag_ms": None,
                "last_update": None,
                "rps": 0.0,
                "_ts_window": deque(maxlen=64),
                "_last_seen_ts": None,
            }
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=1.0)
        self.platform.close()

    def snapshot(self) -> Dict[str, dict]:
        # shallow copy for UI
        snap = {}
        for k, v in self.stats.items():
            snap[k] = dict(v)
            snap[k].pop("_ts_window", None)
        return snap

    def _loop(self):
        while not self._stop.is_set():
            for f in self.feeds:
                st = self.stats.get(f) or {}
                r = self._readers.get(f)
                if r is None:
                    try:
                        r = self.platform.create_reader(f)
                        self._readers[f] = r
                        # refresh meta if creation succeeds
                        st["meta"] = self.platform.describe_feed(f)
                        st["err"] = None
                    except Exception as e:
                        st["err"] = str(e)
                        self._readers[f] = None
                        self.stats[f] = st
                        continue
                try:
                    rec = r.get_latest_record()
                    fields = [fld["name"] for fld in r.record_format["fields"] if fld.get("name") != "_"]
                    rec_map = dict(zip(fields, rec))
                    ts = rec_map.get("ev_us") or rec_map.get("proc_us") or rec_map.get("ts") or rec_map.get("snapshot_us")
                    prev_ts = st.get("_last_seen_ts")
                    st["_last_seen_ts"] = ts
                    if ts is not None:
                        st["last_ts"] = ts
                        st["last_update"] = time.time()
                        st["err"] = None
                        if ts != prev_ts:
                            st["count"] = st.get("count", 0) + 1
                            ts_win: deque = st.get("_ts_window") or deque(maxlen=64)
                            ts_win.append(st["last_update"])
                            st["_ts_window"] = ts_win
                            st["rps"] = self._compute_rps(ts_win)
                        st["lag_ms"] = max(0, (_now_us() - ts) / 1000.0)
                    self.stats[f] = st
                except Exception as e:
                    st["err"] = str(e)
                    # drop reader so we retry next loop
                    try:
                        r.close()
                    except Exception:
                        pass
                    self._readers[f] = None
                    self.stats[f] = st
            time.sleep(self.poll_interval)
        # cleanup
        for r in self._readers.values():
            if r:
                try:
                    r.close()
                except Exception:
                    pass

    @staticmethod
    def _compute_rps(win: deque) -> float:
        if len(win) < 2:
            return 0.0
        span = win[-1] - win[0]
        if span <= 0:
            return 0.0
        return len(win) / span


class UI:
    def __init__(self, base_path: str, feeds: List[str], refresh_hz: float, poll_interval: float):
        self.base_path = base_path
        self.feeds = feeds
        self.refresh = max(0.05, 1.0 / refresh_hz)
        self.poller = FeedPoller(base_path, feeds, poll_interval)
        self.header = FormattedTextControl(self._render_header)
        self.table = FormattedTextControl(self._render_table)
        kb = KeyBindings()
        @kb.add('c-c')
        @kb.add('q')
        def _(event):
            event.app.exit()

        self.style = Style.from_dict({
            "header": "bold #00ffff",
            "ok": "bold #00ff88",
            "err": "bold #ff4444",
            "meta": "#888888",
        })

        root = HSplit([
            Window(height=1, content=self.header, always_hide_cursor=True),
            Window(content=self.table, always_hide_cursor=True),
        ])

        self.app = Application(
            layout=Layout(root),
            key_bindings=kb,
            style=self.style,
            full_screen=True,
            refresh_interval=self.refresh,
        )

    def _render_header(self):
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        return [("", f"Deepwater UI  base={self.base_path}  feeds={len(self.feeds)}  {now}\n")]

    def _render_table(self):
        snap = self.poller.snapshot()
        lines: List[Tuple[str, str]] = []
        lines.append(("class:header", f"{'Feed':<32} {'Last TS (UTC)':<24} {'Lag(ms)':>8} {'RPS':>7} {'Persist':>8} {'Idx':>4} {'Chunk':>10} {'Status'}\n"))
        for name in self.feeds:
            st = snap.get(name, {})
            ts = st.get("last_ts")
            lag = st.get("lag_ms")
            rps = st.get("rps", 0.0)
            meta = st.get("meta") or {}
            persist = meta.get("lifecycle", {}).get("persist")
            idxflag = meta.get("lifecycle", {}).get("index_playback")
            csz = meta.get("lifecycle", {}).get("chunk_size_bytes")
            err = st.get("err")
            status_style = "class:ok" if not err else "class:err"
            ts_str = us_to_iso(ts) if ts else "—"
            lag_str = f"{lag:.2f}" if lag is not None else "—"
            line = f"{name:<32} {ts_str:<24} {lag_str:>8} {rps:>7.2f} {str(persist):>8} {str(idxflag):>4} {csz or '—':>10} "
            lines.append((status_style, line + (err or "OK") + "\n"))
        return lines

    def run(self):
        self.poller.start()
        with patch_stdout():
            try:
                self.app.run()
            finally:
                self.poller.stop()


def main():
    args = parse_args()
    plat = Platform(base_path=args.base_path)
    feeds = args.feeds
    if not feeds:
        feeds = [f for f in plat.list_feeds() if f.startswith(("CB-TRADES-", "CB-L2-", "CB-L2SNAP-"))]
    plat.close()
    ui = UI(args.base_path, feeds, args.refresh_hz, args.poll_interval)
    ui.run()


if __name__ == "__main__":
    main()
