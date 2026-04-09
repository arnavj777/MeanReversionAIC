"""Ticker universe helpers shared across ingestion and PCA engines."""

from __future__ import annotations

import logging
import os

import pandas as pd
import requests


LOGGER = logging.getLogger("shared.universe")
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def _get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _normalize_ticker(symbol: str) -> str:
    # Keep symbols broadly compatible with upstream providers and loaders.
    return str(symbol).strip().upper().replace(".", "-")


def _fetch_sp500_tickers() -> list[str]:
    response = requests.get(
        SP500_WIKI_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        },
        timeout=20,
    )
    response.raise_for_status()

    tables = pd.read_html(response.text)
    if not tables:
        raise ValueError("No tables found in S&P 500 source")

    for table in tables:
        columns = [str(col).strip().lower() for col in table.columns]
        if "symbol" not in columns:
            continue

        symbol_col = table.columns[columns.index("symbol")]
        symbols = [_normalize_ticker(item) for item in table[symbol_col].dropna().tolist()]
        # Preserve order while removing duplicates.
        seen = set()
        deduped = []
        for symbol in symbols:
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            deduped.append(symbol)
        return deduped

    raise ValueError("S&P 500 symbol column not found")


def get_universe_tickers(default_csv: str) -> list[str]:
    """Resolve ticker universe from env config.

    Env knobs:
    - UNIVERSE_MODE: "custom" (default) or "sp500"
    - UNIVERSE_TICKERS: CSV symbols when mode=custom
    - UNIVERSE_MAX_TICKERS: cap applied to resolved list
    """
    mode = os.getenv("UNIVERSE_MODE", "custom").strip().lower()
    max_tickers = _get_env_int("UNIVERSE_MAX_TICKERS", 500)

    if mode == "sp500":
        try:
            tickers = _fetch_sp500_tickers()
            if max_tickers > 0:
                tickers = tickers[:max_tickers]
            LOGGER.info("Universe mode=sp500 resolved tickers=%d", len(tickers))
            return tickers
        except Exception:
            LOGGER.exception("Failed to resolve S&P 500 universe; falling back to custom list")

    raw_csv = os.getenv("UNIVERSE_TICKERS", default_csv)
    tickers = [_normalize_ticker(ticker) for ticker in raw_csv.split(",") if ticker.strip()]
    if max_tickers > 0:
        tickers = tickers[:max_tickers]
    LOGGER.info("Universe mode=custom resolved tickers=%d", len(tickers))
    return tickers
