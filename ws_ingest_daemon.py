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

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Use modified websocket client that doesn't reverse L2 updates
sys.path.insert(0, str(Path(__file__).parent))

# Configure logging BEFORE importing anything else
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("ws_ingest_daemon")

class WebSocketIngestDaemon:
    """Daemon wrapper for MarketDataEngine with custom base_path"""
    
    def __init__(self, products: list[str], base_path: str = "data/coinbase-main",
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
        from websocket_client import MarketDataEngine
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
        
        # Patch the _io_loop method to NOT reverse L2 updates
        # We need the updates in packet order, not reversed
        import types
        original_code = self.engine._io_loop.__code__
        
        # Replace with custom base_path  
        self.engine.platform = Platform(base_path=self.base_path)
        
        # Start engine IO loop BEFORE subscribing
        self.engine._should_run = True
        self.engine.io_thread = __import__('threading').Thread(
            target=self.engine._io_loop, name="ws-io", daemon=True)
        self.engine.io_thread.start()
        
        # Now subscribe to products
        for product in self.products:
            log.info(f"Subscribing to {product}...")
            self.engine.subscribe(product)
        
        self.running = True
        log.info("WebSocket engine started successfully")

        # Start control socket listener
        self._start_control()
        
        # Keep alive
        try:
            while self.running:
                time.sleep(1)
                
                # Log status every 30 seconds
                if int(time.time()) % 30 == 0:
                    status = self.engine.status_snapshot()
                    metrics = self.engine.metrics_snapshot()
                    log.info(f"Status: connected={status['connected']}, subs={status['subs']}")
                    if metrics:
                        log.info(f"Metrics: trades={metrics.get('trade', {})}, l2={metrics.get('l2', {})}")
                        
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
    parser.add_argument("--base-path", type=str, default="data/coinbase-main",
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
