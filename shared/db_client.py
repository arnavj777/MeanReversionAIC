"""TimescaleDB client utilities for market OHLCV data access."""

import os
import math
from datetime import datetime, timedelta
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError


DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "market_data")
DB_USER = os.getenv("DB_USER", "arb_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "arb_pass")


def _get_engine() -> Engine:
    """Create a SQLAlchemy engine for TimescaleDB/PostgreSQL."""
    db_url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(db_url, pool_pre_ping=True)


def initialize_database() -> None:
    """Initialize the OHLCV table and convert it to a Timescale hypertable."""
    engine = _get_engine()
    with engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS ohlcv (
                    time TIMESTAMP NOT NULL,
                    ticker VARCHAR(16) NOT NULL,
                    open NUMERIC NOT NULL,
                    high NUMERIC NOT NULL,
                    low NUMERIC NOT NULL,
                    close NUMERIC NOT NULL,
                    volume NUMERIC NOT NULL
                );
                """
            )
        )
        connection.execute(
            text(
                "SELECT create_hypertable('ohlcv', 'time', if_not_exists => TRUE);"
            )
        )


def get_latest_timestamp(ticker: str) -> Optional[datetime]:
    """Return the latest timestamp stored for a ticker, or None if no rows exist."""
    engine = _get_engine()
    query = text("SELECT MAX(time) AS latest_time FROM ohlcv WHERE ticker = :ticker")
    with engine.connect() as connection:
        latest = connection.execute(query, {"ticker": ticker}).scalar()
    return latest


def get_latest_close(ticker: str) -> Optional[float]:
    """Return latest close price for ticker, or None when unavailable."""
    engine = _get_engine()
    query = text(
        """
        SELECT close
        FROM ohlcv
        WHERE ticker = :ticker
        ORDER BY time DESC
        LIMIT 1
        """
    )
    with engine.connect() as connection:
        value = connection.execute(query, {"ticker": ticker}).scalar()
    return float(value) if value is not None else None


def insert_dataframe(df: pd.DataFrame) -> int:
    """Insert OHLCV rows into the ohlcv table using pandas to_sql."""
    expected = ["time", "ticker", "open", "high", "low", "close", "volume"]
    if df.empty:
        print("db_client.insert_dataframe: received empty DataFrame, skipping insert")
        return 0

    missing = [col for col in expected if col not in df.columns]
    if missing:
        raise ValueError(f"insert_dataframe missing required columns: {missing}")

    insert_df = df[expected].copy()
    insert_df["time"] = pd.to_datetime(insert_df["time"], utc=False)

    engine = _get_engine()
    insert_df.to_sql(
        "ohlcv",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000,
    )
    return len(insert_df)


def get_historical_returns(
    tickers: Iterable[str],
    lookback_days: int = 30,
) -> pd.DataFrame:
    """Return a pivoted log-return matrix indexed by time with ticker columns."""
    tickers = list(tickers)
    if not tickers:
        return pd.DataFrame()

    start_time = datetime.utcnow() - timedelta(days=lookback_days)
    query = text(
        """
        SELECT time, ticker, close
        FROM ohlcv
        WHERE ticker = ANY(:tickers)
          AND time >= :start_time
        ORDER BY time ASC
        """
    )

    engine = _get_engine()
    try:
        with engine.connect() as connection:
            raw = pd.read_sql(
                query,
                connection,
                params={"tickers": tickers, "start_time": start_time},
            )
    except ProgrammingError as exc:
        # If PCA queries before ingestion initializes schema, self-heal once.
        if "relation \"ohlcv\" does not exist" not in str(exc):
            raise
        initialize_database()
        with engine.connect() as connection:
            raw = pd.read_sql(
                query,
                connection,
                params={"tickers": tickers, "start_time": start_time},
            )

    if raw.empty:
        return pd.DataFrame()

    raw["time"] = pd.to_datetime(raw["time"])
    prices = raw.pivot(index="time", columns="ticker", values="close").sort_index()
    prices = prices.astype(float)

    # Log return: ln(P_t / P_{t-1})
    ratio = prices / prices.shift(1)
    log_returns = ratio.apply(lambda col: col.map(lambda x: math.log(x) if x > 0 else float("nan")))
    return log_returns
