"""Delta ingestion loop that updates local OHLCV data in TimescaleDB."""

import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from data_ingestion.lseg_prices import LSEGPricesClient
from shared import db_client
from shared.universe import get_universe_tickers


LOGGER = logging.getLogger("data_ingestion")
CYCLE_SECONDS = 3 * 60 * 60
LOOKBACK_DAYS = int(os.getenv("INGESTION_LOOKBACK_DAYS", "30"))
USE_MOCK_INGESTION = os.getenv("INGESTION_USE_MOCK", "false").strip().lower() == "true"


def fetch_lseg_delta(ticker: str, last_time: Optional[datetime]) -> pd.DataFrame:
    """Generate mock 1-minute OHLCV bars for a ticker from last_time to now."""
    now = datetime.utcnow().replace(second=0, microsecond=0)
    start = (
        (last_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
        if last_time is not None
        else now - timedelta(days=30)
    )

    if start > now:
        return pd.DataFrame(columns=["time", "ticker", "open", "high", "low", "close", "volume"])

    times = pd.date_range(start=start, end=now, freq="1min")
    if times.empty:
        return pd.DataFrame(columns=["time", "ticker", "open", "high", "low", "close", "volume"])

    rows = []
    base_price = 100.0 + random.uniform(-10, 10)
    for ts in times:
        drift = random.uniform(-0.005, 0.005)
        open_px = base_price
        close_px = max(1.0, open_px * (1 + drift))
        high_px = max(open_px, close_px) * (1 + random.uniform(0.0001, 0.0025))
        low_px = min(open_px, close_px) * (1 - random.uniform(0.0001, 0.0025))
        volume = random.randint(1000, 100000)
        rows.append(
            {
                "time": ts.to_pydatetime(),
                "ticker": ticker,
                "open": round(open_px, 4),
                "high": round(high_px, 4),
                "low": round(low_px, 4),
                "close": round(close_px, 4),
                "volume": volume,
            }
        )
        base_price = close_px

    return pd.DataFrame(rows)


def run_cycle(prices_client: Optional[LSEGPricesClient], tickers: list[str]) -> None:
    """Run a single ingestion cycle across configured tickers."""
    for ticker in tickers:
        try:
            latest = db_client.get_latest_timestamp(ticker)
            if USE_MOCK_INGESTION:
                delta_df = fetch_lseg_delta(ticker, latest)
            else:
                if prices_client is None:
                    raise RuntimeError("LSEG price client is not initialized")
                delta_df = prices_client.fetch_ohlcv_delta(
                    ticker=ticker,
                    last_time=latest,
                    lookback_days=LOOKBACK_DAYS,
                )
            inserted = db_client.insert_dataframe(delta_df)
            LOGGER.info(
                "Ticker=%s latest=%s generated=%d inserted=%d source=%s",
                ticker,
                latest,
                len(delta_df),
                inserted,
                "mock" if USE_MOCK_INGESTION else "lseg",
            )
        except Exception:
            LOGGER.exception("Failed ingest cycle for ticker=%s", ticker)


def main() -> None:
    """Run ingestion continuously with fixed cadence."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    LOGGER.info("Data ingestion service starting")
    LOGGER.info("Ingestion source=%s", "mock" if USE_MOCK_INGESTION else "lseg")

    db_client.initialize_database()
    LOGGER.info("Database initialization complete")

    tickers = get_universe_tickers(default_csv="AAPL,MSFT,NVDA")
    LOGGER.info("Ingestion ticker universe size=%d sample=%s", len(tickers), tickers[:10])

    prices_client = None
    if not USE_MOCK_INGESTION:
        prices_client = LSEGPricesClient()

    shutting_down = {"value": False}

    def _shutdown_handler(signum, _frame):
        if shutting_down["value"]:
            return
        shutting_down["value"] = True
        LOGGER.info("Received shutdown signal %s. Closing ingestion resources...", signum)
        if prices_client is not None:
            prices_client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    while True:
        cycle_start = datetime.utcnow()
        LOGGER.info("Starting ingestion cycle at %s", cycle_start.isoformat())
        try:
            run_cycle(prices_client, tickers=tickers)
        except Exception:
            LOGGER.exception("Unexpected cycle-level failure")
        LOGGER.info("Cycle complete; sleeping for %d seconds", CYCLE_SECONDS)
        time.sleep(CYCLE_SECONDS)


if __name__ == "__main__":
    main()
