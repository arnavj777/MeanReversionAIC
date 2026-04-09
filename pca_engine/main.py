"""
PCA Engine - Main entry point.

This engine runs batch statistical analysis to detect anomalies
and publishes them to the Redis message broker.
"""
import argparse
import os
import time
from datetime import datetime
from pathlib import Path

from shared.messaging import MessageBroker
from shared import db_client
from shared.universe import get_universe_tickers
from pca_engine.analysis import PCASignalResult, run_pca_residual_signal
from pca_engine.data_loader import DataLoader


MANUAL_LIMITS_CSV = Path(os.getenv("EXECUTION_MANUAL_LIMITS_CSV", "/app/data/manual_limit_orders.csv"))
BUY_LIMIT_OFFSET_BPS = float(os.getenv("EXECUTION_BUY_LIMIT_OFFSET_BPS", "10"))
SELL_LIMIT_OFFSET_BPS = float(os.getenv("EXECUTION_SELL_LIMIT_OFFSET_BPS", "10"))
DEFAULT_ORDER_QTY = int(os.getenv("EXECUTION_DEFAULT_ORDER_QTY", "1"))
PLAN_VOL_WINDOW = int(os.getenv("PLAN_VOL_WINDOW", "120"))
PLAN_VOL_MULTIPLIER = float(os.getenv("PLAN_VOL_MULTIPLIER", "2.0"))
PLAN_MIN_SIDE_OFFSET_BPS = float(os.getenv("PLAN_MIN_SIDE_OFFSET_BPS", "25"))
PLAN_MAX_SIDE_OFFSET_BPS = float(os.getenv("PLAN_MAX_SIDE_OFFSET_BPS", "300"))
PLAN_MIN_EDGE_BPS = float(os.getenv("PLAN_MIN_EDGE_BPS", "80"))


def _ensure_manual_limits_file() -> None:
    MANUAL_LIMITS_CSV.parent.mkdir(parents=True, exist_ok=True)
    if MANUAL_LIMITS_CSV.exists():
        return

    MANUAL_LIMITS_CSV.write_text(
        "timestamp,ticker,signal,qty,signal_type,z_score,reference_price,buy_limit_price,sell_limit_price,status,reason,broker,order_id\n",
        encoding="utf-8",
    )


def _append_manual_limits_row(row_values: list[str]) -> None:
    _ensure_manual_limits_file()
    with MANUAL_LIMITS_CSV.open("a", encoding="utf-8") as file_handle:
        file_handle.write(",".join(value.replace(",", " ") for value in row_values) + "\n")


def _compute_manual_limit_levels(
    reference_price: float,
    buy_offset_bps: float,
    sell_offset_bps: float,
) -> tuple[float, float]:
    buy_limit = round(reference_price * (1 - (buy_offset_bps / 10_000.0)), 4)
    sell_limit = round(reference_price * (1 + (sell_offset_bps / 10_000.0)), 4)
    return buy_limit, sell_limit


def _compute_dynamic_offsets_bps(returns_df, ticker: str) -> tuple[float, float]:
    ticker_returns = returns_df.get(ticker)
    vol_bps = 0.0
    if ticker_returns is not None:
        sigma = float(ticker_returns.tail(PLAN_VOL_WINDOW).std(ddof=0))
        if sigma > 0:
            vol_bps = sigma * 10_000.0

    dynamic_side_bps = max(
        PLAN_MIN_SIDE_OFFSET_BPS,
        BUY_LIMIT_OFFSET_BPS,
        SELL_LIMIT_OFFSET_BPS,
        PLAN_VOL_MULTIPLIER * vol_bps,
    )
    dynamic_side_bps = min(dynamic_side_bps, PLAN_MAX_SIDE_OFFSET_BPS)
    return dynamic_side_bps, dynamic_side_bps


def _write_batch_manual_plan(signal_result: PCASignalResult, returns_df, zscore_threshold: float) -> int:
    """Write one batch plan row per actionable ticker (BUY/SELL only)."""
    written = 0
    now_iso = datetime.utcnow().isoformat()

    for ticker, z_score in signal_result.residual_zscores.items():
        signal = "HOLD"
        if z_score <= -abs(zscore_threshold):
            signal = "BUY"
        elif z_score >= abs(zscore_threshold):
            signal = "SELL"

        if signal == "HOLD":
            continue

        reference_price = db_client.get_latest_close(ticker)
        if reference_price is None or reference_price <= 0:
            continue

        buy_offset_bps, sell_offset_bps = _compute_dynamic_offsets_bps(returns_df, ticker)
        if (buy_offset_bps + sell_offset_bps) < PLAN_MIN_EDGE_BPS:
            continue

        buy_limit, sell_limit = _compute_manual_limit_levels(
            reference_price,
            buy_offset_bps=buy_offset_bps,
            sell_offset_bps=sell_offset_bps,
        )
        _append_manual_limits_row(
            [
                now_iso,
                ticker,
                signal,
                str(DEFAULT_ORDER_QTY),
                "mean_reversion_plan",
                f"{z_score:.6f}",
                f"{reference_price:.6f}",
                f"{buy_limit:.6f}",
                f"{sell_limit:.6f}",
                "plan",
                f"pca_batch_plan_edge_bps={buy_offset_bps + sell_offset_bps:.1f}",
                "PCA",
                "",
            ]
        )
        written += 1

    return written


def _get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _run_cycle(broker: MessageBroker, loader: DataLoader, tickers: list[str]) -> None:
    lookback_days = _get_env_int("PCA_LOOKBACK_DAYS", 30)
    min_observations = _get_env_int("PCA_MIN_OBSERVATIONS", 200)
    zscore_threshold = _get_env_float("PCA_ZSCORE_THRESHOLD", 2.0)
    residual_window = _get_env_int("PCA_RESIDUAL_Z_WINDOW", 120)
    min_explained_variance = _get_env_float("PCA_MIN_EXPLAINED_VARIANCE", 0.50)

    returns_df = loader.load_returns(
        tickers,
        lookback_days=lookback_days,
        min_observations=min_observations,
    )
    if returns_df.empty or len(returns_df) < min_observations:
        print(
            "PCA Engine: insufficient local return history, "
            f"have={len(returns_df)} need={min_observations}"
        )
        return

    try:
        signal_result = run_pca_residual_signal(
            returns_df=returns_df,
            zscore_threshold=zscore_threshold,
            residual_z_window=residual_window,
            min_explained_variance=min_explained_variance,
        )
    except ValueError as exc:
        print(f"PCA Engine: cycle skipped due to data/model constraints: {exc}")
        return

    plan_rows = _write_batch_manual_plan(
        signal_result,
        returns_df=returns_df,
        zscore_threshold=zscore_threshold,
    )
    print(f"PCA Engine: wrote {plan_rows} actionable rows to manual limit sheet")

    anomaly_data = {
        "ticker": signal_result.signal_ticker,
        "z_score": round(signal_result.signal_value, 4),
        "timestamp": datetime.utcnow().timestamp(),
        "signal": signal_result.signal,
        "signal_type": "mean_reversion_residual",
        "model": "pca_residual_v1",
        "explained_variance": round(signal_result.explained_variance_total, 4),
        "residual_z": signal_result.residual_zscores,
    }

    broker.publish_anomalies(anomaly_data)
    print(
        "PCA Engine: Published anomaly for "
        f"{signal_result.signal_ticker} z={signal_result.signal_value:.4f} "
        f"signal={signal_result.signal} "
        f"variance={signal_result.explained_variance_total:.4f}"
    )


def main():
    """Main entry point for one-shot or continuous PCA analysis."""
    parser = argparse.ArgumentParser(description="PCA anomaly engine")
    parser.add_argument(
        "--mode",
        choices=["once", "continuous"],
        default=os.getenv("PCA_MODE", "continuous"),
        help="Run mode: one cycle and exit, or continuous loop",
    )
    args = parser.parse_args()

    print("PCA Engine starting...")
    broker = MessageBroker()
    loader = DataLoader()
    tickers = get_universe_tickers(default_csv=os.getenv("PCA_TICKERS", "AAPL,MSFT,NVDA"))
    print(f"PCA Engine universe size={len(tickers)} sample={tickers[:10]}")

    if args.mode == "once":
        _run_cycle(broker, loader, tickers=tickers)
        return

    loop_seconds = _get_env_int("PCA_LOOP_SECONDS", 30)
    while True:
        _run_cycle(broker, loader, tickers=tickers)
        time.sleep(loop_seconds)


if __name__ == "__main__":
    main()