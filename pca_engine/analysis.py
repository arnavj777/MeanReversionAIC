"""Core PCA analysis utilities for residual-based mean reversion signals."""

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd
from sklearn.decomposition import PCA


@dataclass
class PCASignalResult:
    """Container for PCA output and selected trade candidate."""

    signal_ticker: str
    signal_value: float
    signal: str
    explained_variance_ratio: List[float]
    explained_variance_total: float
    residual_zscores: Dict[str, float]
    residual_latest: Dict[str, float]


def prepare_return_matrix(returns_df: pd.DataFrame) -> pd.DataFrame:
    """Return a clean, finite matrix for PCA fitting."""
    if returns_df.empty:
        return returns_df

    matrix = returns_df.copy().ffill().dropna()
    if matrix.empty:
        return matrix

    matrix = matrix.loc[:, matrix.std(ddof=0) > 0]
    return matrix


def _standardize(returns_df: pd.DataFrame) -> pd.DataFrame:
    """Standardize returns per ticker using z-normalization."""
    centered = returns_df - returns_df.mean()
    std = returns_df.std(ddof=0)
    std = std.where(std != 0)
    standardized = centered.divide(std, axis=1)
    return standardized.dropna(axis=1, how="all")


def run_pca_residual_signal(
    returns_df: pd.DataFrame,
    zscore_threshold: float,
    residual_z_window: int,
    min_explained_variance: float,
) -> PCASignalResult:
    """Fit PCA, compute residuals, and return strongest mean-reversion candidate."""
    matrix = prepare_return_matrix(returns_df)
    if matrix.empty or len(matrix) < 5 or matrix.shape[1] < 2:
        raise ValueError("Not enough data to run PCA")

    standardized = _standardize(matrix)
    if standardized.empty or standardized.shape[1] < 2:
        raise ValueError("Not enough variable tickers to run PCA")

    n_components = min(2, standardized.shape[1] - 1)
    if n_components < 1:
        raise ValueError("No valid PCA component count")

    pca = PCA(n_components=n_components)
    scores = pca.fit_transform(standardized.values)
    reconstructed = pca.inverse_transform(scores)
    reconstructed_df = pd.DataFrame(
        reconstructed,
        index=standardized.index,
        columns=standardized.columns,
    )

    residuals = standardized - reconstructed_df

    window = min(residual_z_window, len(residuals))
    rolling_mean = residuals.rolling(window=window, min_periods=window).mean()
    rolling_std = residuals.rolling(window=window, min_periods=window).std(ddof=0)

    latest_residual = residuals.iloc[-1]
    latest_mean = rolling_mean.iloc[-1]
    latest_std = rolling_std.iloc[-1].where(rolling_std.iloc[-1] != 0)

    zscores = ((latest_residual - latest_mean) / latest_std).dropna()
    if zscores.empty:
        raise ValueError("Residual z-scores are empty")

    explained_ratio = pca.explained_variance_ratio_.tolist()
    explained_total = float(sum(explained_ratio))
    if explained_total < min_explained_variance:
        return PCASignalResult(
            signal_ticker=str(zscores.idxmin()),
            signal_value=float(zscores.min()),
            signal="HOLD",
            explained_variance_ratio=[float(x) for x in explained_ratio],
            explained_variance_total=explained_total,
            residual_zscores={k: float(v) for k, v in zscores.to_dict().items()},
            residual_latest={k: float(v) for k, v in latest_residual.to_dict().items()},
        )

    ticker = str(zscores.idxmin())
    z_value = float(zscores.loc[ticker])
    signal = "BUY" if z_value <= -abs(zscore_threshold) else "HOLD"

    return PCASignalResult(
        signal_ticker=ticker,
        signal_value=z_value,
        signal=signal,
        explained_variance_ratio=[float(x) for x in explained_ratio],
        explained_variance_total=explained_total,
        residual_zscores={k: float(v) for k, v in zscores.to_dict().items()},
        residual_latest={k: float(v) for k, v in latest_residual.to_dict().items()},
    )
