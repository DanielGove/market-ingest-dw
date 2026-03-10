from __future__ import annotations

import unittest
from unittest.mock import Mock

from connectors.hyperliquid_perp.connector import HyperliquidPerpConnector


class HyperliquidPerpConnectorTests(unittest.TestCase):
    def test_feed_specs_include_derived_context_families(self) -> None:
        connector = HyperliquidPerpConnector()

        specs = connector.feed_specs("BTC-USD")

        self.assertEqual(
            set(specs),
            {"trades", "l2", "perp_ctx", "funding", "open_interest"},
        )
        self.assertEqual(specs["funding"]["feed_name"], "HL-FUNDING-BTC-USD")
        self.assertEqual(specs["open_interest"]["feed_name"], "HL-OPEN-INTEREST-BTC-USD")

    def test_active_asset_ctx_writes_all_context_families(self) -> None:
        connector = HyperliquidPerpConnector()
        perp_ctx_writer = Mock()
        funding_writer = Mock()
        open_interest_writer = Mock()
        engine = Mock()
        engine.family_writers = {
            "perp_ctx": {"BTC-USD": perp_ctx_writer},
            "funding": {"BTC-USD": funding_writer},
            "open_interest": {"BTC-USD": open_interest_writer},
        }
        engine.log = Mock()

        raw = (
            b'{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"0.0001",'
            b'"openInterest":"12345.6","markPx":"100001.5","oraclePx":"100000.5",'
            b'"midPx":"100001.0","premium":"0.00001"}}}'
        )

        connector.handle_raw(engine, raw, 123456789, lambda: 123456999)

        perp_ctx_writer.write_values.assert_called_once_with(
            123456789,
            123456789,
            123456999,
            0.0001,
            12345.6,
            100001.5,
            100000.5,
            100001.0,
            0.00001,
            create_index=True,
        )
        funding_writer.write_values.assert_called_once_with(
            123456789,
            123456789,
            123456999,
            0.0001,
            0.00001,
            100001.5,
            100000.5,
            create_index=True,
        )
        open_interest_writer.write_values.assert_called_once_with(
            123456789,
            123456789,
            123456999,
            12345.6,
            100001.5,
            100000.5,
            100001.0,
            create_index=True,
        )


if __name__ == "__main__":
    unittest.main()
