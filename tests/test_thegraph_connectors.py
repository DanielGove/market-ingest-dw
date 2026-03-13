"""Tests for TheGraph-based connectors (Uniswap V3, V2, PancakeSwap, stablecoin, bridge)."""

from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import orjson

from connectors.loader import build_connector_for_venue, get_connector_profile_for_venue
from connectors.thegraph_ws import TheGraphWsConnector
from connectors.univ3_dex.connector import UniswapV3Connector
from connectors.univ2_dex.connector import UniswapV2Connector
from connectors.pancake_dex.connector import PancakeSwapConnector
from connectors.stablecoin_monitor.connector import StablecoinMonitorConnector
from connectors.bridge_monitor.connector import BridgeMonitorConnector


# ---------------------------------------------------------------------------
# TheGraphWsConnector protocol tests
# ---------------------------------------------------------------------------

class ConcreteGraphConnector(TheGraphWsConnector):
    """Minimal concrete connector for protocol-level tests."""

    venue = "test_venue"

    def feed_specs(self, pid: str) -> dict:
        return {}

    def _build_subscription(self, product_ids):
        return "subscription { test }", {}

    def _handle_data(self, engine, data, recv_us, now_us):
        pass


class TheGraphWsProtocolTests(unittest.TestCase):
    def _make_engine(self):
        engine = Mock()
        engine._ws = Mock()
        engine._hb_last = 0.0
        engine.family_writers = {}
        engine.log = Mock()
        return engine

    def test_on_connect_sends_connection_init(self) -> None:
        conn = ConcreteGraphConnector(uri="wss://example.com")
        engine = self._make_engine()

        conn.on_connect(engine)

        engine._ws.send.assert_called_once_with(
            orjson.dumps({"type": "connection_init", "payload": {}})
        )
        self.assertFalse(conn._connected)

    def test_connection_ack_sets_connected(self) -> None:
        conn = ConcreteGraphConnector(uri="wss://example.com")
        engine = self._make_engine()
        conn.on_connect(engine)

        raw = orjson.dumps({"type": "connection_ack"})
        conn.handle_raw(engine, raw, 1000, lambda: 1001)

        self.assertTrue(conn._connected)

    def test_ping_triggers_pong_response(self) -> None:
        conn = ConcreteGraphConnector(uri="wss://example.com")
        engine = self._make_engine()
        conn.on_connect(engine)
        engine._ws.send.reset_mock()

        raw = orjson.dumps({"type": "ping"})
        conn.handle_raw(engine, raw, 1000, lambda: 1001)

        engine._ws.send.assert_called_once_with(orjson.dumps({"type": "pong"}))

    def test_error_message_is_logged(self) -> None:
        conn = ConcreteGraphConnector(uri="wss://example.com")
        engine = self._make_engine()
        conn.on_connect(engine)

        raw = orjson.dumps({"type": "error", "payload": [{"message": "bad query"}]})
        conn.handle_raw(engine, raw, 1000, lambda: 1001)

        engine.log.error.assert_called_once()

    def test_send_subscribe_sends_subscribe_message(self) -> None:
        conn = ConcreteGraphConnector(uri="wss://example.com")
        engine = self._make_engine()
        conn.on_connect(engine)
        engine._ws.send.reset_mock()

        conn.send_subscribe(engine, ["WETH-USDC"])

        self.assertEqual(engine._ws.send.call_count, 1)
        sent = orjson.loads(engine._ws.send.call_args[0][0])
        self.assertEqual(sent["type"], "subscribe")
        self.assertIn("query", sent["payload"])

    def test_send_unsubscribe_sends_complete_for_all_subs(self) -> None:
        conn = ConcreteGraphConnector(uri="wss://example.com")
        engine = self._make_engine()
        conn.on_connect(engine)
        conn.send_subscribe(engine, ["A"])
        conn.send_subscribe(engine, ["B"])
        engine._ws.send.reset_mock()

        conn.send_unsubscribe(engine, ["A", "B"])

        # Should have sent 2 "complete" messages
        self.assertEqual(engine._ws.send.call_count, 2)
        for call in engine._ws.send.call_args_list:
            msg = orjson.loads(call[0][0])
            self.assertEqual(msg["type"], "complete")

    def test_next_message_dispatches_to_handle_data(self) -> None:
        handled = []

        class _Connector(ConcreteGraphConnector):
            def _handle_data(self, engine, data, recv_us, now_us):
                handled.append(data)

        conn = _Connector(uri="wss://example.com")
        engine = self._make_engine()

        raw = orjson.dumps({
            "type": "next",
            "id": "1",
            "payload": {"data": {"swaps": [{"id": "abc"}]}},
        })
        conn.handle_raw(engine, raw, 1000, lambda: 1001)

        self.assertEqual(len(handled), 1)
        self.assertIn("swaps", handled[0])


# ---------------------------------------------------------------------------
# Uniswap V3 connector tests
# ---------------------------------------------------------------------------

class UniswapV3ConnectorTests(unittest.TestCase):
    def test_profile_and_loader_discover_univ3_connector(self) -> None:
        profile = get_connector_profile_for_venue("univ3")
        connector = build_connector_for_venue("univ3", {})

        self.assertEqual(profile["key"], "univ3")
        self.assertEqual(profile["families"], ("pool_swaps", "pool_liquidity"))
        self.assertIsInstance(connector, UniswapV3Connector)

    def test_feed_specs_include_pool_families(self) -> None:
        connector = UniswapV3Connector()

        specs = connector.feed_specs("WETH-USDC")

        self.assertEqual(set(specs), {"pool_swaps", "pool_liquidity"})
        self.assertEqual(specs["pool_swaps"]["feed_name"], "UV3-SWAPS-WETH-USDC")
        self.assertEqual(specs["pool_liquidity"]["feed_name"], "UV3-LIQ-WETH-USDC")

    def test_handle_data_writes_swap(self) -> None:
        connector = UniswapV3Connector()
        swap_writer = Mock()
        engine = Mock()
        engine.family_writers = {"pool_swaps": {"WETH-USDC": swap_writer}}
        engine.log = Mock()

        data = {
            "swaps": [
                {
                    "id": "swap1",
                    "timestamp": "1690000000",
                    "pool": {"id": "0xabc", "token0": {"symbol": "WETH"}, "token1": {"symbol": "USDC"}},
                    "amount0": "1.5",
                    "amount1": "-3000.0",
                    "amountUSD": "3000.0",
                    "sqrtPriceX96": "1234567890",
                    "tick": "200000",
                    "logIndex": "5",
                    "transaction": {"blockNumber": "18000001"},
                }
            ]
        }
        connector._handle_data(engine, data, 1690000000000000, lambda: 1690000000100000)

        swap_writer.write_values.assert_called_once()
        args = swap_writer.write_values.call_args[0]
        self.assertEqual(args[0], 1690000000 * 1_000_000)  # event_time
        self.assertEqual(args[3], 18000001)  # block_number
        self.assertEqual(args[5], b"B")  # side=buy (amount0 > 0)
        self.assertAlmostEqual(args[6], 1.5)  # amount0
        self.assertAlmostEqual(args[8], 3000.0)  # amount_usd

    def test_handle_data_deduplicates_by_block(self) -> None:
        connector = UniswapV3Connector()
        connector._last_swap_block = 18000001  # already seen this block

        swap_writer = Mock()
        engine = Mock()
        engine.family_writers = {"pool_swaps": {"WETH-USDC": swap_writer}}
        engine.log = Mock()

        data = {
            "swaps": [
                {
                    "id": "swap_old",
                    "timestamp": "1690000000",
                    "pool": {"id": "0xabc", "token0": {"symbol": "WETH"}, "token1": {"symbol": "USDC"}},
                    "amount0": "1.0",
                    "amount1": "-2000.0",
                    "amountUSD": "2000.0",
                    "sqrtPriceX96": "0",
                    "tick": "0",
                    "logIndex": "1",
                    "transaction": {"blockNumber": "18000001"},  # same block, skip
                }
            ]
        }
        connector._handle_data(engine, data, 1690000000000000, lambda: 1690000000100000)

        swap_writer.write_values.assert_not_called()

    def test_handle_data_writes_mint_event(self) -> None:
        connector = UniswapV3Connector()
        liq_writer = Mock()
        engine = Mock()
        engine.family_writers = {"pool_liquidity": {"WETH-USDC": liq_writer}}
        engine.log = Mock()

        data = {
            "mints": [
                {
                    "id": "mint1",
                    "timestamp": "1690000100",
                    "pool": {"id": "0xabc", "token0": {"symbol": "WETH"}, "token1": {"symbol": "USDC"}},
                    "amount": "1000000",
                    "amount0": "1.0",
                    "amount1": "2000.0",
                    "tickLower": "199000",
                    "tickUpper": "201000",
                    "logIndex": "3",
                    "transaction": {"blockNumber": "18000002"},
                }
            ]
        }
        connector._handle_data(engine, data, 1690000100000000, lambda: 1690000100100000)

        liq_writer.write_values.assert_called_once()
        args = liq_writer.write_values.call_args[0]
        self.assertEqual(args[5], b"M")  # mint

    def test_build_subscription_splits_token_pairs(self) -> None:
        connector = UniswapV3Connector()

        query, variables = connector._build_subscription(["WETH-USDC", "WBTC-WETH"])

        self.assertIn("subscription", query)
        self.assertIn("WETH", variables["token0s"])
        self.assertIn("WBTC", variables["token0s"])
        self.assertIn("USDC", variables["token1s"])


# ---------------------------------------------------------------------------
# Uniswap V2 connector tests
# ---------------------------------------------------------------------------

class UniswapV2ConnectorTests(unittest.TestCase):
    def test_profile_and_loader_discover_univ2_connector(self) -> None:
        profile = get_connector_profile_for_venue("univ2")
        connector = build_connector_for_venue("univ2", {})

        self.assertEqual(profile["key"], "univ2")
        self.assertEqual(profile["families"], ("pool_swaps", "pool_liquidity"))
        self.assertIsInstance(connector, UniswapV2Connector)

    def test_feed_specs_include_pool_families(self) -> None:
        connector = UniswapV2Connector()

        specs = connector.feed_specs("WETH-USDC")

        self.assertEqual(set(specs), {"pool_swaps", "pool_liquidity"})
        self.assertEqual(specs["pool_swaps"]["feed_name"], "UV2-SWAPS-WETH-USDC")
        self.assertEqual(specs["pool_liquidity"]["feed_name"], "UV2-LIQ-WETH-USDC")

    def test_handle_data_writes_v2_swap(self) -> None:
        connector = UniswapV2Connector()
        swap_writer = Mock()
        engine = Mock()
        engine.family_writers = {"pool_swaps": {"WETH-USDC": swap_writer}}
        engine.log = Mock()

        data = {
            "swaps": [
                {
                    "id": "swap1",
                    "timestamp": "1690000000",
                    "pair": {"id": "0xabc", "token0": {"symbol": "WETH"}, "token1": {"symbol": "USDC"}},
                    "amount0In": "0.0",
                    "amount1In": "2000.0",
                    "amount0Out": "1.0",
                    "amount1Out": "0.0",
                    "amountUSD": "2000.0",
                    "logIndex": "2",
                    "transaction": {"blockNumber": "18000001"},
                }
            ]
        }
        connector._handle_data(engine, data, 1690000000000000, lambda: 1690000000100000)

        swap_writer.write_values.assert_called_once()
        args = swap_writer.write_values.call_args[0]
        self.assertEqual(args[5], b"B")  # amount0Out > 0 means buy of token0
        self.assertAlmostEqual(args[6], 0.0)   # amount0In
        self.assertAlmostEqual(args[8], 1.0)   # amount0Out


# ---------------------------------------------------------------------------
# PancakeSwap connector tests
# ---------------------------------------------------------------------------

class PancakeSwapConnectorTests(unittest.TestCase):
    def test_profile_and_loader_discover_pancake_connector(self) -> None:
        profile = get_connector_profile_for_venue("pancake")
        connector = build_connector_for_venue("pancake", {})

        self.assertEqual(profile["key"], "pancake")
        self.assertEqual(profile["families"], ("pool_swaps", "pool_liquidity"))
        self.assertIsInstance(connector, PancakeSwapConnector)

    def test_feed_specs_include_pool_families(self) -> None:
        connector = PancakeSwapConnector()

        specs = connector.feed_specs("WBNB-USDT")

        self.assertEqual(set(specs), {"pool_swaps", "pool_liquidity"})
        self.assertEqual(specs["pool_swaps"]["feed_name"], "PS-SWAPS-WBNB-USDT")
        self.assertEqual(specs["pool_liquidity"]["feed_name"], "PS-LIQ-WBNB-USDT")

    def test_handle_data_writes_pancake_swap(self) -> None:
        connector = PancakeSwapConnector()
        swap_writer = Mock()
        engine = Mock()
        engine.family_writers = {"pool_swaps": {"WBNB-USDT": swap_writer}}
        engine.log = Mock()

        data = {
            "swaps": [
                {
                    "id": "ps1",
                    "timestamp": "1690000000",
                    "pool": {"id": "0xps", "token0": {"symbol": "WBNB"}, "token1": {"symbol": "USDT"}},
                    "amount0": "-1.0",
                    "amount1": "300.0",
                    "amountUSD": "300.0",
                    "sqrtPriceX96": "0",
                    "tick": "0",
                    "logIndex": "1",
                    "transaction": {"blockNumber": "35000001"},
                }
            ]
        }
        connector._handle_data(engine, data, 1690000000000000, lambda: 1690000000100000)

        swap_writer.write_values.assert_called_once()
        args = swap_writer.write_values.call_args[0]
        self.assertEqual(args[5], b"S")  # amount0 < 0 means sell token0


# ---------------------------------------------------------------------------
# Stablecoin monitor connector tests
# ---------------------------------------------------------------------------

class StablecoinMonitorConnectorTests(unittest.TestCase):
    def test_profile_and_loader_discover_stablecoin_connector(self) -> None:
        profile = get_connector_profile_for_venue("stablecoin")
        connector = build_connector_for_venue("stablecoin", {})

        self.assertEqual(profile["key"], "stablecoin")
        self.assertIn("stablecoin_transfers", profile["families"])
        self.assertIn("stablecoin_supply", profile["families"])
        self.assertIsInstance(connector, StablecoinMonitorConnector)

    def test_feed_specs_include_stablecoin_families(self) -> None:
        connector = StablecoinMonitorConnector()

        specs = connector.feed_specs("USDT")

        self.assertEqual(set(specs), {"stablecoin_transfers", "stablecoin_supply"})
        self.assertEqual(specs["stablecoin_transfers"]["feed_name"], "SC-XFER-USDT")
        self.assertEqual(specs["stablecoin_supply"]["feed_name"], "SC-SUPPLY-USDT")

    def test_handle_data_writes_transfer(self) -> None:
        connector = StablecoinMonitorConnector()
        xfer_writer = Mock()
        engine = Mock()
        engine.family_writers = {"stablecoin_transfers": {"USDT": xfer_writer}}
        engine.log = Mock()

        data = {
            "transfers": [
                {
                    "id": "xfer1",
                    "blockNumber": "18000001",
                    "timestamp": "1690000000",
                    "token": {"symbol": "USDT"},
                    "from": "0xSender",
                    "to": "0xReceiver",
                    "amount": "100000.0",
                    "amountUSD": "100000.0",
                    "logIndex": "7",
                }
            ]
        }
        connector._handle_data(engine, data, 1690000000000000, lambda: 1690000000100000)

        xfer_writer.write_values.assert_called_once()
        args = xfer_writer.write_values.call_args[0]
        self.assertEqual(args[5], b"T")  # regular transfer (not mint/burn)

    def test_handle_data_detects_mint(self) -> None:
        connector = StablecoinMonitorConnector()
        xfer_writer = Mock()
        engine = Mock()
        engine.family_writers = {"stablecoin_transfers": {"USDC": xfer_writer}}
        engine.log = Mock()

        data = {
            "transfers": [
                {
                    "id": "mint1",
                    "blockNumber": "18000002",
                    "timestamp": "1690000001",
                    "token": {"symbol": "USDC"},
                    "from": "0x0000000000000000000000000000000000000000",
                    "to": "0xRecipient",
                    "amount": "1000000.0",
                    "amountUSD": "1000000.0",
                    "logIndex": "1",
                }
            ]
        }
        connector._handle_data(engine, data, 1690000001000000, lambda: 1690000001100000)

        args = xfer_writer.write_values.call_args[0]
        self.assertEqual(args[5], b"M")  # mint from zero address

    def test_handle_data_writes_supply_snapshot(self) -> None:
        connector = StablecoinMonitorConnector()
        supply_writer = Mock()
        engine = Mock()
        engine.family_writers = {"stablecoin_supply": {"USDT": supply_writer}}
        engine.log = Mock()

        data = {
            "tokens": [
                {
                    "id": "0xusdt",
                    "symbol": "USDT",
                    "lastPriceBlockNumber": "18000001",
                    "totalSupply": "90000000000.0",
                    "circulatingSupply": "88000000000.0",
                }
            ]
        }
        connector._handle_data(engine, data, 1690000000000000, lambda: 1690000000100000)

        supply_writer.write_values.assert_called_once()
        args = supply_writer.write_values.call_args[0]
        self.assertAlmostEqual(args[4], 90000000000.0)  # total_supply
        self.assertAlmostEqual(args[5], 88000000000.0)  # circulating_supply


# ---------------------------------------------------------------------------
# Bridge monitor connector tests
# ---------------------------------------------------------------------------

class BridgeMonitorConnectorTests(unittest.TestCase):
    def test_profile_and_loader_discover_bridge_connector(self) -> None:
        profile = get_connector_profile_for_venue("bridge")
        connector = build_connector_for_venue("bridge", {})

        self.assertEqual(profile["key"], "bridge")
        self.assertEqual(profile["families"], ("bridge_transfers",))
        self.assertIsInstance(connector, BridgeMonitorConnector)

    def test_feed_specs_include_bridge_transfers(self) -> None:
        connector = BridgeMonitorConnector()

        specs = connector.feed_specs("USDC")

        self.assertEqual(set(specs), {"bridge_transfers"})
        self.assertEqual(specs["bridge_transfers"]["feed_name"], "BR-XFER-USDC")

    def test_handle_data_writes_deposit(self) -> None:
        connector = BridgeMonitorConnector()
        xfer_writer = Mock()
        engine = Mock()
        engine.family_writers = {"bridge_transfers": {"USDC": xfer_writer}}
        engine.log = Mock()

        data = {
            "bridgeEvents": [
                {
                    "id": "evt1",
                    "blockNumber": "18000001",
                    "blockTimestamp": "1690000000",
                    "token": {"symbol": "USDC"},
                    "amount": "50000.0",
                    "amountUSD": "50000.0",
                    "srcChainId": "1",
                    "dstChainId": "42161",
                    "direction": "DEPOSIT",
                    "logIndex": "2",
                }
            ]
        }
        connector._handle_data(engine, data, 1690000000000000, lambda: 1690000000100000)

        xfer_writer.write_values.assert_called_once()
        args = xfer_writer.write_values.call_args[0]
        self.assertEqual(args[5], b"D")  # deposit
        self.assertAlmostEqual(args[6], 50000.0)
        self.assertAlmostEqual(args[8], 1.0)   # srcChainId
        self.assertAlmostEqual(args[9], 42161.0)  # dstChainId

    def test_handle_data_writes_withdrawal(self) -> None:
        connector = BridgeMonitorConnector()
        xfer_writer = Mock()
        engine = Mock()
        engine.family_writers = {"bridge_transfers": {"ETH": xfer_writer}}
        engine.log = Mock()

        data = {
            "bridgeEvents": [
                {
                    "id": "evt2",
                    "blockNumber": "18000002",
                    "blockTimestamp": "1690000001",
                    "token": {"symbol": "ETH"},
                    "amount": "1.5",
                    "amountUSD": "4500.0",
                    "srcChainId": "42161",
                    "dstChainId": "1",
                    "direction": "WITHDRAWAL",
                    "logIndex": "3",
                }
            ]
        }
        connector._handle_data(engine, data, 1690000001000000, lambda: 1690000001100000)

        args = xfer_writer.write_values.call_args[0]
        self.assertEqual(args[5], b"W")  # withdrawal


# ---------------------------------------------------------------------------
# Connector discovery: all new venues discoverable
# ---------------------------------------------------------------------------

class ConnectorDiscoveryTests(unittest.TestCase):
    def test_all_new_venues_are_discoverable(self) -> None:
        from runtime.venue import known_venue_keys

        keys = known_venue_keys()
        self.assertIn("dydx", keys)
        self.assertIn("univ3", keys)
        self.assertIn("univ2", keys)
        self.assertIn("pancake", keys)
        self.assertIn("stablecoin", keys)
        self.assertIn("bridge", keys)
        # Original venues still present
        self.assertIn("hyperliquid", keys)
        self.assertIn("binance", keys)
        self.assertIn("coinbase", keys)
        self.assertIn("kraken", keys)


if __name__ == "__main__":
    unittest.main()
