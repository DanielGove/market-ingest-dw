from __future__ import annotations

import unittest
from unittest.mock import Mock

from connectors.dydx_perp.connector import DydxPerpConnector
from connectors.loader import build_connector_for_venue, get_connector_profile_for_venue


class DydxPerpConnectorTests(unittest.TestCase):
    def test_profile_and_loader_discover_dydx_connector(self) -> None:
        profile = get_connector_profile_for_venue("dydx")
        connector = build_connector_for_venue("dydx", {})

        self.assertEqual(profile["key"], "dydx")
        self.assertEqual(profile["families"], ("trades", "l2", "perp_ctx"))
        self.assertIsInstance(connector, DydxPerpConnector)

    def test_feed_specs_include_trades_l2_perp_ctx(self) -> None:
        connector = DydxPerpConnector()

        specs = connector.feed_specs("BTC-USD")

        self.assertEqual(set(specs), {"trades", "l2", "perp_ctx"})
        self.assertEqual(specs["trades"]["feed_name"], "DX-TRADES-BTC-USD")
        self.assertEqual(specs["l2"]["feed_name"], "DX-L2-BTC-USD")
        self.assertEqual(specs["perp_ctx"]["feed_name"], "DX-PERP-CTX-BTC-USD")

    def test_handle_raw_writes_trade(self) -> None:
        connector = DydxPerpConnector()
        trade_writer = Mock()
        engine = Mock()
        engine.family_writers = {"trades": {"BTC-USD": trade_writer}}
        engine.log = Mock()

        raw = (
            b'{"type":"channel_data","channel":"v4_trades","id":"BTC-USD",'
            b'"contents":{"trades":[{"id":"abc123","side":"BUY",'
            b'"size":"0.001","price":"50000.0",'
            b'"createdAt":"2024-01-01T12:00:00.000Z"}]}}'
        )

        recv_us = 1704110400000000
        connector.handle_raw(engine, raw, recv_us, lambda: recv_us + 100)

        trade_writer.write_values.assert_called_once()
        args = trade_writer.write_values.call_args[0]
        self.assertEqual(args[5], b"T")
        self.assertEqual(args[6], b"B")
        self.assertAlmostEqual(args[7], 50000.0)
        self.assertAlmostEqual(args[8], 0.001)

    def test_handle_raw_writes_sell_trade(self) -> None:
        connector = DydxPerpConnector()
        trade_writer = Mock()
        engine = Mock()
        engine.family_writers = {"trades": {"ETH-USD": trade_writer}}
        engine.log = Mock()

        raw = (
            b'{"type":"channel_data","channel":"v4_trades","id":"ETH-USD",'
            b'"contents":{"trades":[{"id":"xyz","side":"SELL",'
            b'"size":"0.5","price":"3000.0",'
            b'"createdAt":"2024-01-01T12:00:01.000Z"}]}}'
        )

        connector.handle_raw(engine, raw, 1704110401000000, lambda: 1704110401100000)

        trade_writer.write_values.assert_called_once()
        args = trade_writer.write_values.call_args[0]
        self.assertEqual(args[6], b"S")

    def test_handle_raw_writes_orderbook_snapshot(self) -> None:
        connector = DydxPerpConnector()
        book_writer = Mock()
        engine = Mock()
        engine.family_writers = {"l2": {"BTC-USD": book_writer}}
        engine.log = Mock()

        raw = (
            b'{"type":"subscribed","channel":"v4_orderbook","id":"BTC-USD",'
            b'"contents":{'
            b'"bids":[{"price":"50000","size":"1.0"},{"price":"49999","size":"2.0"}],'
            b'"asks":[{"price":"50001","size":"0.5"}]}}'
        )

        connector.handle_raw(engine, raw, 1704110400000000, lambda: 1704110400100000)

        # 2 bids + 1 ask = 3 writes
        self.assertEqual(book_writer.write_values.call_count, 3)
        # First write (first bid) should have create_index=True
        first_call_kwargs = book_writer.write_values.call_args_list[0][1]
        self.assertTrue(first_call_kwargs.get("create_index"))
        # Second write should not have create_index=True
        second_call_kwargs = book_writer.write_values.call_args_list[1][1]
        self.assertFalse(second_call_kwargs.get("create_index", False))

    def test_handle_raw_writes_perp_ctx_from_markets(self) -> None:
        connector = DydxPerpConnector()
        ctx_writer = Mock()
        engine = Mock()
        engine.family_writers = {"perp_ctx": {"BTC-USD": ctx_writer}}
        engine.log = Mock()

        raw = (
            b'{"type":"subscribed","channel":"v4_markets","contents":{'
            b'"markets":{"BTC-USD":{'
            b'"nextFundingRate":"0.00005",'
            b'"openInterest":"500.0",'
            b'"oraclePrice":"50000.0",'
            b'"priceChange24H":"1000.0"}}}}'
        )

        recv_us = 1704110400000000
        connector.handle_raw(engine, raw, recv_us, lambda: recv_us + 100)

        ctx_writer.write_values.assert_called_once_with(
            recv_us,
            recv_us,
            recv_us + 100,
            0.00005,
            500.0,
            50000.0,
            1000.0,
            create_index=True,
        )

    def test_handle_raw_ignores_unknown_channel(self) -> None:
        connector = DydxPerpConnector()
        engine = Mock()
        engine.family_writers = {}
        engine.log = Mock()

        raw = b'{"type":"channel_data","channel":"v4_unknown","id":"BTC-USD","contents":{}}'
        connector.handle_raw(engine, raw, 1000, lambda: 1001)
        # No writes or errors expected
        engine.log.error.assert_not_called()

    def test_handle_raw_connected_updates_heartbeat(self) -> None:
        connector = DydxPerpConnector()
        engine = Mock()
        engine.family_writers = {}
        engine.log = Mock()

        raw = b'{"type":"connected","connection_id":"abc","message_id":0}'
        connector.handle_raw(engine, raw, 1000, lambda: 1001)
        # heartbeat should be refreshed; no trades or errors
        engine.log.error.assert_not_called()

    def test_send_subscribe_sets_markets_subscribed(self) -> None:
        connector = DydxPerpConnector()
        engine = Mock()
        engine._ws = Mock()

        connector.send_subscribe(engine, ["BTC-USD", "ETH-USD"])

        self.assertTrue(connector._markets_subscribed)
        # 2 products × 2 channels (v4_trades + v4_orderbook) + 1 v4_markets subscription = 5 sends
        self.assertEqual(engine._ws.send.call_count, 5)

    def test_send_subscribe_markets_only_once(self) -> None:
        connector = DydxPerpConnector()
        engine = Mock()
        engine._ws = Mock()

        connector.send_subscribe(engine, ["BTC-USD"])
        connector.send_subscribe(engine, ["ETH-USD"])

        # Second call should not re-subscribe to v4_markets
        calls = [str(c) for c in engine._ws.send.call_args_list]
        markets_count = sum(1 for c in calls if "v4_markets" in c)
        self.assertEqual(markets_count, 1)


if __name__ == "__main__":
    unittest.main()
