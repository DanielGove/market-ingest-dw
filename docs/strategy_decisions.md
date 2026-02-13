# Strategy Decisions Feed (Deepwater-native backtesting)

## Goal
Capture strategy intents in a dedicated feed during live/“paper” runs, then backtest by replaying those decisions against recorded market feeds with any fill/latency model. No future peeking, minimal coupling.

## Live decision feed
- Name: `STRAT-ORDERS-<STRATNAME>` (e.g., `STRAT-ORDERS-PINGPONG`).
- Type: Deepwater feed, persisted like other feeds.

### Suggested schema (order-level, one record per decision)
- `event_time` (uint64): OB snapshot time that triggered the decision.
- `processed_time` (uint64): when the strategy produced the decision.
- `product_id` (char[16])
- `side` (char): `B` / `S`
- `type` (char): `L` (limit) / `M` (market) if used.
- `price` (float64)
- `size` (float64)
- `tif` (char): `G` (GTC), `I` (IOC), `F` (FOK), `P` (post-only/maker)
- `client_tag` (char[16]): optional user/tag for tracking

## Writing decisions live
- In your strategy loop, build a dict → tuple matching the schema and call a Deepwater writer on `STRAT-ORDERS-...`.
- Keep this independent of order routing; no fills simulated here.

## Backtest/eval flow (offline)
1) Read `STRAT-ORDERS-<...>` and the corresponding market feed (`OB<depth><period>-<product>`).
2) Use `processed_time` as the decision timestamp and add your latency model (time-to-action) to get an exchange arrival time for the order.
3) Apply fill model at that arrival time (cross or post), include fees/slippage as needed.
4) Include fees in PnL accounting (maker/taker as params).
5) Compute PnL, hit-rate, slippage, inventory, drawdowns.
6) You can run many fill models over the same decisions without rerunning the strategy.

## Order status simulation (planned)
- In production, order status (ACK/partial/fill/cancel) comes from the exchange.
- For backtests, we can synthesize a status feed per strategy (e.g., `STRAT-ORDERS-STATUS-<NAME>`) where the fill model emits status events (with the same latency model) back to the strategy.
- This lets live strategy code remain unchanged: it listens to status events the same way in backtest as in prod.

## Minimal writer hook (pseudocode)
```python
# assuming Platform already exists
writer = platform.create_writer("STRAT-ORDERS-PINGPONG")
record = (
    snapshot_ts,  # event_time
    proc_ts,      # processed_time
    product_id,
    side,
    order_type,
    price,
    size,
    tif,
    client_tag,
)
writer.write_values(*record)
```

## Why this helps
- Reproducible: decisions are immutable; fill assumptions can change later.
- Causal: timestamps derive from what was known then.
- Lightweight: no change to ingest/OB daemons; just add a writer.

## Next steps
- Add a tiny helper in `backtest/` to define/create the decision feed spec and return a writer.
- Add an offline evaluator that replays `STRAT-ORDERS-*` with a configurable fill/latency model.
