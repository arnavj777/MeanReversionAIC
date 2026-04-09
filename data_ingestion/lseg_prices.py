"""LSEG-backed price ingestion client for OHLCV delta retrieval."""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
import refinitiv.data as rd
from refinitiv.data.session import platform


LOGGER = logging.getLogger("data_ingestion.lseg")


class LSEGPricesClient:
    """Fetches OHLCV bars from LSEG using a platform session."""

    def __init__(self):
        load_dotenv()
        self.app_key = os.getenv("LSEG_APP_KEY")
        self.username = os.getenv("LSEG_USERNAME")
        self.password = os.getenv("LSEG_PASSWORD")
        self.session = None
        self._session_open = False

        try:
            if not self.app_key or not self.username or not self.password:
                raise ValueError("LSEG credentials are incomplete")

            definition = platform.Definition(
                name="default",
                app_key=self.app_key,
                grant=platform.GrantPassword(
                    username=self.username,
                    password=self.password,
                ),
                signon_control=True,
            )
            self.session = definition.get_session()
            rd.session.set_default(self.session)
            self.session.open()
            self._session_open = True
            LOGGER.info("LSEG price session opened")
        except Exception:
            LOGGER.exception("Failed to open LSEG price session")

    def _format_ric(self, ticker: str) -> str:
        normalized = str(ticker).strip().upper()
        if "." in normalized:
            return normalized
        return f"{normalized}.O"

    def _candidate_rics(self, ticker: str) -> list[str]:
        """Generate candidate RICs for US equities across common exchanges."""
        normalized = str(ticker).strip().upper().replace("-", ".")
        candidates: list[str] = []

        if "." in normalized:
            candidates.append(normalized)
        else:
            candidates.extend([f"{normalized}.O", f"{normalized}.N", normalized])

        # Preserve order while removing duplicates.
        seen = set()
        deduped = []
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped

    def _get_history(self, ric: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
        """Try primary and fallback get_history invocation styles for a RIC."""
        try:
            history = rd.get_history(
                universe=[ric],
                fields=["OPEN_PRC", "HIGH_1", "LOW_1", "TRDPRC_1", "ACVOL_UNS"],
                interval="minute",
                start=start,
                end=end,
            )
            if history is not None and not history.empty:
                return history
        except Exception:
            LOGGER.debug("Primary get_history failed for %s", ric, exc_info=True)

        try:
            history = rd.get_history(
                universe=ric,
                fields=["OPEN_PRC", "HIGH_1", "LOW_1", "TRDPRC_1", "ACVOL_UNS"],
                interval="minute",
                start=start.isoformat(),
                end=end.isoformat(),
            )
            if history is not None and not history.empty:
                return history
        except Exception:
            LOGGER.debug("Fallback get_history failed for %s", ric, exc_info=True)

        return None

    def _to_utc_aware(self, value: datetime) -> datetime:
        """Normalize timestamp to UTC-aware datetime for safe comparisons."""
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _normalize_history(self, ticker: str, history: pd.DataFrame) -> pd.DataFrame:
        if history is None or history.empty:
            return pd.DataFrame(columns=["time", "ticker", "open", "high", "low", "close", "volume"])

        frame = history.copy()
        if isinstance(frame.index, pd.DatetimeIndex):
            frame = frame.reset_index().rename(columns={frame.index.name or "index": "time"})
        elif "time" not in frame.columns:
            frame = frame.reset_index()
            if "index" in frame.columns:
                frame = frame.rename(columns={"index": "time"})

        upper_cols = {str(col).upper(): col for col in frame.columns}

        def _find_col(candidates):
            for cand in candidates:
                for key, original in upper_cols.items():
                    if cand in key:
                        return original
            return None

        open_col = _find_col(["OPEN", "OPEN_PRC"])
        high_col = _find_col(["HIGH", "HIGH_1"])
        low_col = _find_col(["LOW", "LOW_1"])
        close_col = _find_col(["CLOSE", "TRDPRC_1", "CLOSE_PRC"])
        volume_col = _find_col(["VOLUME", "ACVOL", "TRNOVR", "TRDPRC_1_VOLUME"])

        if not all([open_col, high_col, low_col, close_col, volume_col]):
            LOGGER.warning("LSEG history columns did not map cleanly: %s", list(frame.columns))
            return pd.DataFrame(columns=["time", "ticker", "open", "high", "low", "close", "volume"])

        normalized = pd.DataFrame(
            {
                "time": pd.to_datetime(frame["time"], utc=False, errors="coerce"),
                "ticker": ticker,
                "open": pd.to_numeric(frame[open_col], errors="coerce"),
                "high": pd.to_numeric(frame[high_col], errors="coerce"),
                "low": pd.to_numeric(frame[low_col], errors="coerce"),
                "close": pd.to_numeric(frame[close_col], errors="coerce"),
                "volume": pd.to_numeric(frame[volume_col], errors="coerce"),
            }
        )

        normalized = normalized.dropna()
        normalized = normalized.drop_duplicates(subset=["time", "ticker"]).sort_values("time")
        return normalized

    def fetch_ohlcv_delta(
        self,
        ticker: str,
        last_time: Optional[datetime],
        lookback_days: int = 30,
    ) -> pd.DataFrame:
        """Fetch delta OHLCV rows from LSEG for [last_time+1m, now]."""
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        normalized_last = self._to_utc_aware(last_time) if last_time is not None else None
        start = (
            (normalized_last + timedelta(minutes=1)).replace(second=0, microsecond=0)
            if normalized_last is not None
            else now - timedelta(days=lookback_days)
        )

        if start > now:
            return pd.DataFrame(columns=["time", "ticker", "open", "high", "low", "close", "volume"])

        history = None
        resolved_ric = None
        for ric in self._candidate_rics(ticker):
            history = self._get_history(ric=ric, start=start, end=now)
            if history is not None and not history.empty:
                resolved_ric = ric
                break

        if resolved_ric is None:
            LOGGER.warning("No intraday history returned for ticker=%s across tested RICs", ticker)
            return pd.DataFrame(columns=["time", "ticker", "open", "high", "low", "close", "volume"])

        LOGGER.debug("Resolved ticker=%s to ric=%s", ticker, resolved_ric)
        return self._normalize_history(ticker=ticker, history=history)

    def close(self) -> None:
        try:
            if self.session is not None:
                self.session.close()
            rd.close_session()
            if self._session_open:
                LOGGER.info("LSEG price session closed")
        except Exception:
            LOGGER.exception("Error while closing LSEG price session")
        finally:
            self._session_open = False
