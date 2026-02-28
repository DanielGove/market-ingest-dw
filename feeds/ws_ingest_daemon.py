#!/usr/bin/env python3
"""
Standalone WebSocket ingest daemon for OB200200 pipeline
Runs in background without UI, uses separate base_path
"""
import sys
import time
import signal
import logging
from pathlib import Path
import socket
import threading

# Make project root importable (so `feeds` package resolves)
sys.path.insert(0, str(Path(__file__).parent.parent))
# Optional local src override (keep legacy search)
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configure logging BEFORE importing anything else
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("ws_ingest_daemon")


def _default_base_path() -> str:
    if Path("/deepwater/data").exists():
        return "/deepwater/data/coinbase-advanced"
    return "data/coinbase-main"


class WebSocketIngestDaemon:
    """Daemon wrapper for MarketDataEngine with custom base_path"""
    
    def __init__(self, products: list[str], base_path: str = _default_base_path(),
                 control_sock: str | None = None):
        self.products = [p.upper() for p in products]
        self.base_path = base_path
        self.engine = None
        self.running = False
        self.control_sock = control_sock or "pids/ingest.sock"
        self._ctl_thread = None
    
    def start(self):
        """Start the ingest engine"""
        log.info(f"Starting WebSocket ingest daemon for {self.products}")
        log.info(f"Base path: {self.base_path}")
        
        # Import MarketDataEngine
        from feeds.websocket_client import MarketDataEngine
        from deepwater.platform import Platform
        
        # Create engine with modified initialization
        self.engine = MarketDataEngine()
        
        # CRITICAL: Close the default platform and clear state BEFORE modifying
        try:
            self.engine.platform.close()
        except Exception as e:
            log.warning(f"Error closing default platform: {e}")
        
        # Clear state
        self.engine.trade_writers.clear()
        self.engine.book_writers.clear()
        self.engine.product_ids.clear()
                
        # Replace with custom base_path  
        self.engine.platform = Platform(base_path=self.base_path)
        
        # Register products first while disconnected. The IO loop will issue
        # one consolidated subscribe for all products after connect.
        for product in self.products:
            log.info(f"Subscribing to {product}...")
            self.engine.subscribe(product)

        # Start engine IO loop after product registration to avoid
        # subscription burst rate-limits.
        self.engine._should_run = True
        self.engine.io_thread = __import__('threading').Thread(
            target=self.engine._io_loop, name="ws-io", daemon=True)
        self.engine.io_thread.start()
        
        self.running = True
        log.info("WebSocket engine started successfully")

        # Start control socket listener
        self._start_control()
        
        # Keep alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Received interrupt signal")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the engine"""
        if self.engine and self.running:
            log.info("Stopping WebSocket engine...")
            self.running = False
            self._stop_control()
            self.engine.stop()
            log.info("Engine stopped")

    # ---- control socket ----
    def _start_control(self):
        sock_path = Path(self.control_sock)
        sock_path.parent.mkdir(parents=True, exist_ok=True)
        if sock_path.exists():
            try: sock_path.unlink()
            except Exception: pass

        def loop():
            srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            srv.bind(str(sock_path))
            srv.listen(1)
            srv.settimeout(1.0)
            log.info("Control socket listening at %s", sock_path)
            while self.running:
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        log.warning("Control accept error: %s", e)
                    break
                with conn:
                    try:
                        data = conn.recv(1024).decode("ascii", "ignore").strip()
                        if not data:
                            continue
                        parts = data.split()
                        cmd = parts[0].upper()
                        if cmd == "SUB" and len(parts) == 2:
                            pid = parts[1].upper()
                            self.engine.subscribe(pid)
                            conn.sendall(b"OK\n")
                        elif cmd == "UNSUB" and len(parts) == 2:
                            pid = parts[1].upper()
                            self.engine.unsubscribe(pid)
                            conn.sendall(b"OK\n")
                        elif cmd == "LIST":
                            subs = sorted(self.engine.product_ids)
                            conn.sendall((", ".join(subs) + "\n").encode("ascii"))
                        elif cmd == "ROLL" and len(parts) in (1, 2):
                            target = parts[1].upper() if len(parts) == 2 and parts[1].upper() != "ALL" else None
                            result = self.engine.request_segment_roll(target, timeout_s=3.0)
                            if result.get("status") == "ok":
                                conn.sendall(
                                    (
                                        "OK target={target} trades_closed={trades_closed} l2_closed={l2_closed} "
                                        "missing={missing}\n"
                                    ).format(
                                        target=result.get("target", "ALL"),
                                        trades_closed=result.get("trades_closed", 0),
                                        l2_closed=result.get("l2_closed", 0),
                                        missing=",".join(result.get("missing", [])),
                                    ).encode("ascii", "ignore")
                                )
                            else:
                                conn.sendall(
                                    ("ERR status={status} target={target}\n").format(
                                        status=result.get("status", "error"),
                                        target=result.get("target", "ALL"),
                                    ).encode("ascii", "ignore")
                                )
                        else:
                            conn.sendall(b"ERR unknown\n")
                    except Exception as e:
                        log.warning("Control cmd error: %s", e, exc_info=True)
            try: srv.close()
            except Exception: pass
            try: sock_path.unlink()
            except Exception: pass

        self._ctl_thread = threading.Thread(target=loop, name="ingest-ctl", daemon=True)
        self._ctl_thread.start()

    def _stop_control(self):
        self.running = False
        if self._ctl_thread and self._ctl_thread.is_alive():
            self._ctl_thread.join(timeout=2.0)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="WebSocket ingest daemon")
    parser.add_argument("--products", type=str, required=True,
                       help="Comma-separated list of products (e.g., XRP-USD,BTC-USD)")
    parser.add_argument("--base-path", type=str, default=_default_base_path(),
                       help="Base path for data storage")
    parser.add_argument("--control-sock", type=str, default="pids/ingest.sock",
                       help="Unix domain socket path for control commands")
    args = parser.parse_args()
    
    products = [p.strip() for p in args.products.split(",")]
    daemon = WebSocketIngestDaemon(products, args.base_path, args.control_sock)
    
    # Handle signals
    def signal_handler(signum, frame):
        log.info(f"Received signal {signum}")
        daemon.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    daemon.start()

if __name__ == "__main__":
    main()
