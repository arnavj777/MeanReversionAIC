"""PCA Engine - Batch statistical analysis for anomaly detection."""

from .analysis import PCASignalResult, run_pca_residual_signal
from .data_loader import DataLoader

__all__ = ["DataLoader", "PCASignalResult", "run_pca_residual_signal"]