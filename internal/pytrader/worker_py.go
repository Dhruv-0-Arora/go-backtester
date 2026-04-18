package pytrader

// workerSource is the embedded Python worker program. The backtester
// materialises this to a temporary file at startup and runs it under the
// user's system Python interpreter. The worker reads newline-delimited
// JSON commands on stdin and writes newline-delimited JSON responses on
// stdout, with trader stdout captured per tick and returned in the response.
//
// Keeping the worker in-process in Python (one `load` followed by many
// `tick` commands) amortizes the import cost across ticks and matches the
// long-lived Py<PyAny> held by the Rust PyO3 implementation.
const workerSource = `from __future__ import annotations

import io
import importlib.util
import json
import os
import sys
from contextlib import redirect_stdout


# --- Embedded datamodel (identical in shape to IMC's reference datamodel) ---
class Listing:
    def __init__(self, symbol, product, denomination):
        self.symbol = symbol
        self.product = product
        self.denomination = denomination


class ConversionObservation:
    def __init__(self, bidPrice, askPrice, transportFees, exportTariff,
                 importTariff, sugarPrice, sunlightIndex):
        self.bidPrice = bidPrice
        self.askPrice = askPrice
        self.transportFees = transportFees
        self.exportTariff = exportTariff
        self.importTariff = importTariff
        self.sugarPrice = sugarPrice
        self.sunlightIndex = sunlightIndex


class Observation:
    def __init__(self, plainValueObservations=None, conversionObservations=None):
        self.plainValueObservations = plainValueObservations or {}
        self.conversionObservations = conversionObservations or {}


class Order:
    def __init__(self, symbol, price, quantity):
        self.symbol = symbol
        self.price = price
        self.quantity = quantity

    def __repr__(self):
        return f"Order({self.symbol}, {self.price}, {self.quantity})"


class OrderDepth:
    def __init__(self):
        self.buy_orders = {}
        self.sell_orders = {}


class Trade:
    def __init__(self, symbol, price, quantity, buyer=None, seller=None, timestamp=0):
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.buyer = buyer
        self.seller = seller
        self.timestamp = timestamp

    def __repr__(self):
        return (
            f"Trade({self.symbol}, {self.price}, {self.quantity}, "
            f"{self.buyer}, {self.seller}, {self.timestamp})"
        )


class TradingState:
    def __init__(self, traderData, timestamp, listings, order_depths,
                 own_trades, market_trades, position, observations):
        self.traderData = traderData
        self.timestamp = timestamp
        self.listings = listings
        self.order_depths = order_depths
        self.own_trades = own_trades
        self.market_trades = market_trades
        self.position = position
        self.observations = observations

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)


# Bind to sys.modules so user traders that write 'from datamodel import ...'
# find the correct symbols.
_datamodel_module = type(sys)("datamodel")
_datamodel_module.Listing = Listing
_datamodel_module.Order = Order
_datamodel_module.OrderDepth = OrderDepth
_datamodel_module.Trade = Trade
_datamodel_module.ConversionObservation = ConversionObservation
_datamodel_module.Observation = Observation
_datamodel_module.TradingState = TradingState
sys.modules["datamodel"] = _datamodel_module


def _load_trader(trader_file, workspace_root):
    if workspace_root and workspace_root not in sys.path:
        sys.path.insert(0, workspace_root)
    trader_dir = os.path.dirname(os.path.abspath(trader_file))
    if trader_dir and trader_dir not in sys.path:
        sys.path.insert(0, trader_dir)
    module_name = "user_trader_" + "".join(
        ch if ch.isalnum() or ch in "-_." else "_"
        for ch in os.path.splitext(os.path.basename(trader_file))[0]
    )
    spec = importlib.util.spec_from_file_location(module_name, trader_file)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load trader file: {trader_file}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    if not hasattr(module, "Trader"):
        raise RuntimeError("Trader file does not define a Trader class")
    return module.Trader()


def _build_order_depths(payload):
    depths = {}
    for symbol, rows in payload.items():
        buy_rows, sell_rows = rows
        depth = OrderDepth()
        depth.buy_orders = {int(p): int(v) for p, v in buy_rows}
        depth.sell_orders = {int(p): int(v) for p, v in sell_rows}
        depths[str(symbol)] = depth
    return depths


def _build_trade_dict(payload):
    out = {}
    for symbol, rows in payload.items():
        out[str(symbol)] = [
            Trade(str(r[0]), int(r[1]), int(r[2]), str(r[3]), str(r[4]), int(r[5]))
            for r in rows
        ]
    return out


def _build_conversion_obs(payload):
    return {
        str(product): ConversionObservation(
            float(values[0]), float(values[1]), float(values[2]),
            float(values[3]), float(values[4]), float(values[5]),
            float(values[6]),
        )
        for product, values in payload.items()
    }


def _build_state(msg):
    listings = {
        sym: Listing(sym, sym, "SEASHELLS") for sym in msg["listing_symbols"]
    }
    order_depths = _build_order_depths(msg["order_depths"])
    own_trades = _build_trade_dict(msg["own_trades"])
    market_trades = _build_trade_dict(msg["market_trades"])
    position = {str(k): int(v) for k, v in msg["position"]}
    plain_obs = {str(k): int(v) for k, v in msg["plain_obs"]}
    obs = Observation(
        plainValueObservations=plain_obs,
        conversionObservations=_build_conversion_obs(msg["conversion_obs"]),
    )
    return TradingState(
        str(msg["trader_data"]), int(msg["timestamp"]), listings,
        order_depths, own_trades, market_trades, position, obs,
    )


def _normalize_orders(raw_orders):
    if not isinstance(raw_orders, dict):
        raise RuntimeError(f"Trader.run returned non-dict orders: {type(raw_orders)}")
    normalized = {}
    for symbol, orders in raw_orders.items():
        if orders is None:
            continue
        if not isinstance(symbol, str):
            raise RuntimeError("Orders dictionary keys must be strings")
        if not isinstance(orders, list):
            raise RuntimeError(f"Orders for {symbol} are not a list")
        rows = []
        for order in orders:
            if isinstance(order, Order):
                rows.append([str(order.symbol), int(order.price), int(order.quantity)])
                continue
            if hasattr(order, "symbol") and hasattr(order, "price") and hasattr(order, "quantity"):
                rows.append([str(order.symbol), int(order.price), int(order.quantity)])
                continue
            if isinstance(order, (tuple, list)) and len(order) == 3:
                rows.append([str(order[0]), int(order[1]), int(order[2])])
                continue
            raise RuntimeError(f"Unrecognized order type in {symbol}: {order!r}")
        normalized[symbol] = rows
    return normalized


def _normalize_run_output(output):
    if isinstance(output, tuple):
        if len(output) == 3:
            orders, conversions, trader_data = output
        elif len(output) == 2:
            orders, trader_data = output
            conversions = 0
        elif len(output) == 1:
            orders = output[0]
            conversions = 0
            trader_data = ""
        else:
            raise RuntimeError("Trader.run returned tuple with unsupported length")
    else:
        orders = output
        conversions = 0
        trader_data = ""
    return _normalize_orders(orders), int(conversions), str(trader_data)


def _send(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":")))
    sys.stdout.write("\n")
    sys.stdout.flush()


def _error(exc):
    _send({"ok": False, "error": f"{type(exc).__name__}: {exc}"})


def main():
    trader = None
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            msg = json.loads(raw)
        except Exception as exc:  # noqa: BLE001
            _error(exc)
            continue
        cmd = msg.get("cmd")
        try:
            if cmd == "load":
                trader = _load_trader(msg["trader_file"], msg.get("workspace_root", ""))
                _send({"ok": True})
            elif cmd == "tick":
                if trader is None:
                    raise RuntimeError("load must be called before tick")
                state = _build_state(msg)
                buf = io.StringIO()
                with redirect_stdout(buf):
                    output = trader.run(state)
                orders, conversions, trader_data = _normalize_run_output(output)
                _send({
                    "ok": True,
                    "orders": orders,
                    "conversions": conversions,
                    "trader_data": trader_data,
                    "stdout": buf.getvalue(),
                })
            elif cmd == "ping":
                _send({"ok": True})
            elif cmd == "exit":
                _send({"ok": True})
                return
            else:
                raise RuntimeError(f"unknown command: {cmd!r}")
        except Exception as exc:  # noqa: BLE001
            _error(exc)


if __name__ == "__main__":
    main()
`
