"""Local TimescaleDB-backed data loader for PCA engine."""

from typing import Iterable

import pandas as pd

from shared import db_client


class DataLoader:
    """Loads and cleans historical return data for PCA processing."""

    def load_returns(
        self,
        tickers: Iterable[str],
        lookback_days: int = 30,
        min_observations: int = 200,
    ) -> pd.DataFrame:
        """Fetch return matrix and keep a sufficiently populated sub-universe.

        For broad universes (e.g., S&P 500), requiring a perfectly dense matrix across
        all symbols can lead to an empty result while ingestion is still filling history.
        This method keeps only symbols with adequate sample count, then enforces a
        NaN-free rectangular matrix for PCA fit.
        """
        data = db_client.get_historical_returns(tickers=tickers, lookback_days=lookback_days)
        if data.empty:
            return data

        ordered_tickers = list(tickers)
        cleaned = data.reindex(columns=ordered_tickers).sort_index().ffill()
        valid_columns = cleaned.count() >= max(5, int(min_observations))
        cleaned = cleaned.loc[:, valid_columns]
        cleaned = cleaned.dropna()
        return cleaned
