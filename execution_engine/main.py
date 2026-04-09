"""
Execution Engine - Main entry point.

This engine subscribes to filtered signals and executes trades
through the broker client with risk management.
"""
import logging
import os
from pathlib import Path
from datetime import datetime, timezone

from shared.messaging import MessageBroker
from shared import db_client
from execution_engine.broker_client import BrokerClient


LOGGER = logging.getLogger("execution_engine")
MAX_ABS_ZSCORE = float(os.getenv("EXECUTION_MAX_ABS_ZSCORE", "6.0"))
MIN_BUY_ZSCORE = float(os.getenv("EXECUTION_MIN_BUY_ZSCORE", "2.0"))
DEFAULT_ORDER_QTY = int(os.getenv("EXECUTION_DEFAULT_ORDER_QTY", "1"))
SUPPORTED_SIGNAL_TYPE = "mean_reversion"
MIN_SELL_ZSCORE = float(os.getenv("EXECUTION_MIN_SELL_ZSCORE", "2.0"))
BUY_LIMIT_OFFSET_BPS = float(os.getenv("EXECUTION_BUY_LIMIT_OFFSET_BPS", "10"))
SELL_LIMIT_OFFSET_BPS = float(os.getenv("EXECUTION_SELL_LIMIT_OFFSET_BPS", "10"))
ORDER_BLOTTER_CSV = Path(os.getenv("EXECUTION_ORDER_BLOTTER_CSV", "/app/data/execution_orders.csv"))
MANUAL_LIMITS_CSV = Path(os.getenv("EXECUTION_MANUAL_LIMITS_CSV", "/app/data/manual_limit_orders.csv"))


def _ensure_blotter_file() -> None:
    ORDER_BLOTTER_CSV.parent.mkdir(parents=True, exist_ok=True)
    if ORDER_BLOTTER_CSV.exists():
        return

    ORDER_BLOTTER_CSV.write_text(
        "timestamp,ticker,side,qty,signal_type,z_score,reference_price,limit_price,status,reason,broker,order_id\n",
        encoding="utf-8",
    )


def _ensure_manual_limits_file() -> None:
    MANUAL_LIMITS_CSV.parent.mkdir(parents=True, exist_ok=True)
    if MANUAL_LIMITS_CSV.exists():
        return

    MANUAL_LIMITS_CSV.write_text(
        "timestamp,ticker,signal,qty,signal_type,z_score,reference_price,buy_limit_price,sell_limit_price,status,reason,broker,order_id\n",
        encoding="utf-8",
    )


def _append_blotter_row(
    *,
    timestamp: str,
    ticker: str,
    side: str,
    qty: int,
    signal_type: str,
    z_score: float | None,
    reference_price: float | None,
    limit_price: float | None,
    status: str,
    reason: str,
    broker: str,
    order_id: str,
) -> None:
    _ensure_blotter_file()
    row = [
        timestamp,
        ticker,
        side,
        str(qty),
        signal_type,
        "" if z_score is None else f"{z_score:.6f}",
        "" if reference_price is None else f"{reference_price:.6f}",
        "" if limit_price is None else f"{limit_price:.6f}",
        status,
        reason,
        broker,
        order_id,
    ]
    with ORDER_BLOTTER_CSV.open("a", encoding="utf-8") as file_handle:
        file_handle.write(",".join(value.replace(",", " ") for value in row) + "\n")


def _append_manual_limits_row(
    *,
    timestamp: str,
    ticker: str,
    signal: str,
    qty: int,
    signal_type: str,
    z_score: float | None,
    reference_price: float | None,
    buy_limit_price: float | None,
    sell_limit_price: float | None,
    status: str,
    reason: str,
    broker: str,
    order_id: str,
) -> None:
    _ensure_manual_limits_file()
    row = [
        timestamp,
        ticker,
        signal,
        str(qty),
        signal_type,
        "" if z_score is None else f"{z_score:.6f}",
        "" if reference_price is None else f"{reference_price:.6f}",
        "" if buy_limit_price is None else f"{buy_limit_price:.6f}",
        "" if sell_limit_price is None else f"{sell_limit_price:.6f}",
        status,
        reason,
        broker,
        order_id,
    ]
    with MANUAL_LIMITS_CSV.open("a", encoding="utf-8") as file_handle:
        file_handle.write(",".join(value.replace(",", " ") for value in row) + "\n")


def _derive_reference_price(data: dict, ticker: str) -> float | None:
    for key in ("limit_price", "reference_price", "last_price", "price"):
        if key not in data or data[key] is None:
            continue
        try:
            value = float(data[key])
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value

    latest_close = db_client.get_latest_close(ticker)
    if latest_close and latest_close > 0:
        return latest_close
    return None


def _compute_limit_price(side: str, reference_price: float) -> float:
    if side == "BUY":
        return round(reference_price * (1 - (BUY_LIMIT_OFFSET_BPS / 10_000.0)), 4)
    return round(reference_price * (1 + (SELL_LIMIT_OFFSET_BPS / 10_000.0)), 4)


def _compute_manual_limit_levels(reference_price: float) -> tuple[float, float]:
    buy_limit = round(reference_price * (1 - (BUY_LIMIT_OFFSET_BPS / 10_000.0)), 4)
    sell_limit = round(reference_price * (1 + (SELL_LIMIT_OFFSET_BPS / 10_000.0)), 4)
    return buy_limit, sell_limit


def _validate_signal(data: dict) -> tuple[bool, str]:
    """Validate payload shape and trading constraints."""
    ticker = data.get("ticker")
    signal = data.get("signal")
    signal_type = data.get("signal_type")
    z_score = data.get("z_score")

    if not ticker:
        return False, "missing ticker"

    if signal_type and signal_type != SUPPORTED_SIGNAL_TYPE:
        return False, f"unsupported signal_type={signal_type}"

    if signal not in {"BUY", "SELL", "HOLD"}:
        return False, f"unsupported signal={signal}"

    if signal == "HOLD":
        return False, "signal is HOLD"

    if z_score is None:
        return False, "missing z_score"

    try:
        z_value = float(z_score)
    except (TypeError, ValueError):
        return False, f"invalid z_score={z_score}"

    if abs(z_value) > MAX_ABS_ZSCORE:
        return False, f"z_score out of range ({z_value})"

    if signal == "BUY" and z_value > -abs(MIN_BUY_ZSCORE):
        return False, f"buy z_score not strong enough ({z_value})"

    if signal == "SELL" and z_value < abs(MIN_SELL_ZSCORE):
        return False, f"sell z_score not strong enough ({z_value})"

    return True, "ok"


def on_signal_received(data: dict, primary_broker: BrokerClient):
    """
    Callback function for processing received trading signals.
    
    Args:
        data: Dictionary containing signal data from News Engine.
        primary_broker: Broker adapter used for order routing.
    """
    LOGGER.info("Execution Engine received: %s", data)
    submitted_at = datetime.now(timezone.utc).isoformat()
    ticker = str(data.get("ticker", "")).upper()
    signal = str(data.get("signal", "")).upper()
    signal_type = str(data.get("signal_type") or SUPPORTED_SIGNAL_TYPE)

    z_score = None
    if data.get("z_score") is not None:
        try:
            z_score = float(data["z_score"])
        except (TypeError, ValueError):
            z_score = None

    valid, reason = _validate_signal(data)
    if not valid:
        LOGGER.info("Signal rejected by risk checks: %s", reason)
        return

    side = str(data["signal"]).upper()
    reference_price = _derive_reference_price(data, ticker)
    if reference_price is None:
        reason = "missing reference price"
        LOGGER.info("Signal rejected by pricing checks: %s", reason)
        return

    buy_limit_price, sell_limit_price = _compute_manual_limit_levels(reference_price)
    limit_price = _compute_limit_price(side, reference_price)

    if not primary_broker.is_connected:
        if not primary_broker.connect():
            LOGGER.error("Order rejected: broker connection failed")
            return

    order_response = primary_broker.place_limit_order(
        ticker=ticker,
        quantity=DEFAULT_ORDER_QTY,
        side=side,
        limit_price=limit_price,
    )

    order_status = str(order_response.get("status", "unknown"))
    order_id = str(order_response.get("order_id") or "")
    order_reason = str(order_response.get("reason") or "")

    if order_status.strip().lower() == "rejected":
        LOGGER.info("Order was rejected by broker, skipping CSV write")
        return

    _append_blotter_row(
        timestamp=submitted_at,
        ticker=ticker,
        side=side,
        qty=DEFAULT_ORDER_QTY,
        signal_type=signal_type,
        z_score=z_score,
        reference_price=reference_price,
        limit_price=limit_price,
        status=order_status,
        reason=order_reason,
        broker=primary_broker.broker_name,
        order_id=order_id,
    )
    _append_manual_limits_row(
        timestamp=submitted_at,
        ticker=ticker,
        signal=side,
        qty=DEFAULT_ORDER_QTY,
        signal_type=signal_type,
        z_score=z_score,
        reference_price=reference_price,
        buy_limit_price=buy_limit_price,
        sell_limit_price=sell_limit_price,
        status=order_status,
        reason=order_reason,
        broker=primary_broker.broker_name,
        order_id=order_id,
    )

    LOGGER.info(
        "Limit order routed ticker=%s side=%s qty=%s z=%.4f ref=%.4f limit=%.4f broker=%s response=%s at=%s",
        ticker,
        side,
        DEFAULT_ORDER_QTY,
        z_score if z_score is not None else 0.0,
        reference_price,
        limit_price,
        primary_broker.broker_name,
        order_response,
        submitted_at,
    )


def main():
    """Main entry point for the Execution Engine."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    LOGGER.info("Execution Engine starting")
    broker = MessageBroker()
    
    # Initialize broker clients (stubs for now)
    schwab_client = BrokerClient(broker_name="Charles Schwab")
    _ = BrokerClient(broker_name="Robinhood")
    schwab_client.connect()

    def _callback(data: dict):
        on_signal_received(data, schwab_client)
    
    LOGGER.info("Execution Engine listening for signals")
    broker.subscribe_to_execution_signals(_callback)


if __name__ == "__main__":
    main()