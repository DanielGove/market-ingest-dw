from __future__ import annotations

import unittest
from unittest.mock import Mock

from connectors.binance_spot.connector import BinanceSpotConnector
from connectors.loader import build_connector_for_venue, get_connector_profile_for_venue


class BinanceSpotConnectorTests(unittest.TestCase):
    def test_profile_and_loader_discover_binance_connector(self) -> None:
        profile = get_connector_profile_for_venue("binance")
        connector = build_connector_for_venue("binance", {})

        self.assertEqual(profile["key"], "binance")
        self.assertEqual(profile["families"], ("trades", "l2"))
        self.assertIsInstance(connector, BinanceSpotConnector)

    def test_feed_specs_include_trade_and_l2(self) -> None:
        connector = BinanceSpotConnector()

        specs = connector.feed_specs("BTC-USDT")

        self.assertEqual(set(specs), {"trades", "l2"})
        self.assertEqual(specs["trades"]["feed_name"], "BN-TRADES-BTC-USDT")
        self.assertEqual(specs["l2"]["feed_name"], "BN-L2-BTC-USDT")

    def test_handle_raw_writes_agg_trade(self) -> None:
        connector = BinanceSpotConnector()
        trade_writer = Mock()
        engine = Mock()
        engine.family_writers = {"trades": {"BTC-USDT": trade_writer}}
        engine.log = Mock()

        raw = (
            b'{"e":"aggTrade","E":1672515782136,"s":"BTCUSDT","a":5933014,'
            b'"p":"42000.10","q":"0.25","T":1672515782134,"m":true}'
        )

        connector.handle_raw(engine, raw, 123456789, lambda: 123456999)

        trade_writer.write_values.assert_called_once_with(
            1672515782134000,
            123456789,
            123456999,
            1672515782136000,
            5933014,
            b"T",
            b"S",
            42000.10,
            0.25,
        )

    def test_handle_raw_writes_depth_updates(self) -> None:
        connector = BinanceSpotConnector()
        book_writer = Mock()
        engine = Mock()
        engine.family_writers = {"l2": {"BTC-USDT": book_writer}}
        engine.log = Mock()

        raw = (
            b'{"e":"depthUpdate","E":1672515783136,"T":1672515783135,"s":"BTCUSDT",'
            b'"b":[["42000.00","1.5"]],"a":[["42001.00","2.0"]]}'
        )

        connector.handle_raw(engine, raw, 223456789, lambda: 223456999)

        self.assertEqual(book_writer.write_values.call_count, 2)
        book_writer.write_values.assert_any_call(
            1672515783135000,
            223456789,
            223456999,
            1672515783136000,
            b"u",
            b"B",
            42000.00,
            1.5,
        )
        book_writer.write_values.assert_any_call(
            1672515783135000,
            223456789,
            223456999,
            1672515783136000,
            b"u",
            b"A",
            42001.00,
            2.0,
        )


if __name__ == "__main__":
    unittest.main()
