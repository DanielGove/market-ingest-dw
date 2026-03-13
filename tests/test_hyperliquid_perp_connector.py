from __future__ import annotations

import unittest
from unittest.mock import Mock

from connectors.hyperliquid_perp.connector import HyperliquidPerpConnector
from connectors.loader import build_connector_for_venue, get_connector_profile_for_venue


class HyperliquidPerpConnectorTests(unittest.TestCase):
    def test_profile_and_loader_discover_hyperliquid_connector(self) -> None:
        profile = get_connector_profile_for_venue("hyperliquid")
        connector = build_connector_for_venue("hyperliquid", {})

        self.assertEqual(profile["key"], "hyperliquid")
        self.assertIn("trades", profile["families"])
        self.assertIn("l2", profile["families"])
        self.assertIn("perp_ctx", profile["families"])
        self.assertIn("funding", profile["families"])
        self.assertIn("open_interest", profile["families"])
        self.assertIsInstance(connector, HyperliquidPerpConnector)

    def test_feed_specs_include_all_families(self) -> None:
        connector = HyperliquidPerpConnector()

        specs = connector.feed_specs("BTC-USD")

        self.assertIn("trades", specs)
        self.assertIn("l2", specs)
        self.assertIn("perp_ctx", specs)
        self.assertIn("funding", specs)
        self.assertIn("open_interest", specs)
        self.assertEqual(specs["trades"]["feed_name"], "HL-TRADES-BTC-USD")
        self.assertEqual(specs["l2"]["feed_name"], "HL-L2-BTC-USD")
        self.assertEqual(specs["perp_ctx"]["feed_name"], "HL-PERP-CTX-BTC-USD")
        self.assertEqual(specs["funding"]["feed_name"], "HL-FUNDING-BTC-USD")
        self.assertEqual(specs["open_interest"]["feed_name"], "HL-OI-BTC-USD")

    def test_handle_raw_writes_trade(self) -> None:
        connector = HyperliquidPerpConnector()
        trade_writer = Mock()
        engine = Mock()
        engine.family_writers = {"trades": {"BTC-USD": trade_writer}}
        engine.log = Mock()

        raw = (
            b'{"channel":"trades","data":[{'
            b'"coin":"BTC","time":1690000000000,"tid":12345,'
            b'"side":"B","px":"30000.5","sz":"0.01"}]}'
        )

        connector.handle_raw(engine, raw, 1690000000100000, lambda: 1690000000200000)

        trade_writer.write_values.assert_called_once_with(
            1690000000000000,
            1690000000100000,
            1690000000200000,
            1690000000000000,
            12345,
            b"T",
            b"B",
            30000.5,
            0.01,
        )

    def test_handle_raw_writes_perp_ctx_funding_and_oi(self) -> None:
        connector = HyperliquidPerpConnector()
        ctx_writer = Mock()
        funding_writer = Mock()
        oi_writer = Mock()
        engine = Mock()
        engine.family_writers = {
            "perp_ctx": {"BTC-USD": ctx_writer},
            "funding": {"BTC-USD": funding_writer},
            "open_interest": {"BTC-USD": oi_writer},
        }
        engine.log = Mock()

        raw = (
            b'{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{'
            b'"funding":"0.0001","openInterest":"1234.56",'
            b'"markPx":"30000.0","oraclePx":"29999.0",'
            b'"midPx":"30000.5","premium":"0.00003"}}}'
        )

        recv_us = 1690000000100000
        proc_us = 1690000000200000

        connector.handle_raw(engine, raw, recv_us, lambda: proc_us)

        # perp_ctx should get the full snapshot
        ctx_writer.write_values.assert_called_once_with(
            recv_us,
            recv_us,
            proc_us,
            0.0001,
            1234.56,
            30000.0,
            29999.0,
            30000.5,
            0.00003,
            create_index=True,
        )

        # funding feed should get only the funding rate
        funding_writer.write_values.assert_called_once_with(
            recv_us,
            recv_us,
            proc_us,
            0.0001,
            create_index=True,
        )

        # open_interest feed should get only the OI
        oi_writer.write_values.assert_called_once_with(
            recv_us,
            recv_us,
            proc_us,
            1234.56,
            create_index=True,
        )

    def test_handle_raw_activeAssetCtx_missing_ctx_writers(self) -> None:
        """Connector should not raise if funding/oi writers are absent."""
        connector = HyperliquidPerpConnector()
        ctx_writer = Mock()
        engine = Mock()
        # funding and open_interest writers not provided
        engine.family_writers = {"perp_ctx": {"BTC-USD": ctx_writer}}
        engine.log = Mock()

        raw = (
            b'{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{'
            b'"funding":"0.0001","openInterest":"500.0",'
            b'"markPx":"30000.0","oraclePx":"29999.0",'
            b'"midPx":"30000.5","premium":"0.00003"}}}'
        )

        # Should not raise
        connector.handle_raw(engine, raw, 1690000000100000, lambda: 1690000000200000)
        ctx_writer.write_values.assert_called_once()

    def test_handle_raw_tradfi_perp_atprefix(self) -> None:
        """@-prefixed TradFi perps (e.g. @SPX) map to @SPX-USD product."""
        connector = HyperliquidPerpConnector()
        trade_writer = Mock()
        engine = Mock()
        engine.family_writers = {"trades": {"@SPX-USD": trade_writer}}
        engine.log = Mock()

        raw = (
            b'{"channel":"trades","data":[{'
            b'"coin":"@SPX","time":1690000000000,"tid":99,'
            b'"side":"S","px":"5000.0","sz":"1.0"}]}'
        )

        connector.handle_raw(engine, raw, 1690000000000000, lambda: 1690000000100000)

        trade_writer.write_values.assert_called_once()
        args = trade_writer.write_values.call_args[0]
        self.assertEqual(args[6], b"S")
        self.assertAlmostEqual(args[7], 5000.0)


if __name__ == "__main__":
    unittest.main()
