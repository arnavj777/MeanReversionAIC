"""Production LSEG news client for volume-based fundamental-shock filtering."""

import logging
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
import refinitiv.data as rd
from refinitiv.data.session import platform


LOGGER = logging.getLogger("news_engine.lseg")
FAIL_SAFE_NEWS_VOLUME = 999


class LSEGNewsClient:
    """Client wrapper around LSEG Refinitiv Data headlines API."""

    def __init__(self):
        """Open LSEG session using credentials sourced from environment variables."""
        load_dotenv()
        self.app_key = os.getenv("LSEG_APP_KEY")
        self.username = os.getenv("LSEG_USERNAME")
        self.password = os.getenv("LSEG_PASSWORD")
        self.session = None

        if not self.app_key:
            LOGGER.warning("LSEG_APP_KEY is not set; session open may fail")
        if not self.username or not self.password:
            LOGGER.warning("LSEG username/password are not fully configured")

        self._session_open = False
        try:
            if not self.app_key or not self.username or not self.password:
                raise ValueError("LSEG credentials are incomplete for platform session")

            # Explicitly register a platform (RDP) session so Docker does not fall back
            # to desktop proxy discovery on localhost.
            session_definition = platform.Definition(
                name="default",
                app_key=self.app_key,
                grant=platform.GrantPassword(
                    username=self.username,
                    password=self.password,
                ),
                signon_control=True,
            )

            self.session = session_definition.get_session()
            rd.session.set_default(self.session)
            self.session.open()
            self._session_open = True
            LOGGER.info("LSEG session opened")
        except Exception:
            LOGGER.exception("Failed to open LSEG session")

    def _format_ric(self, ticker: str) -> str:
        """Format ticker for LSEG query; assume US equity RIC if no suffix provided."""
        normalized = str(ticker).strip().upper()
        if "." in normalized:
            return normalized
        return f"{normalized}.O"

    def get_recent_news_volume(self, ticker: str, lookback_hours: int = 24) -> int:
        """Return number of news headlines for ticker over lookback, or fail-safe value."""
        if not ticker:
            return FAIL_SAFE_NEWS_VOLUME

        start_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        end_time = datetime.now(timezone.utc)
        ric = self._format_ric(ticker)

        try:
            headlines = rd.news.get_headlines(
                query=ric,
                start=start_time,
                end=end_time,
                count=100,
            )
            if headlines is None:
                return 0
            return int(len(headlines.index))
        except Exception:
            LOGGER.exception(
                "LSEG news query failed for ticker=%s ric=%s; returning fail-safe volume",
                ticker,
                ric,
            )
            return FAIL_SAFE_NEWS_VOLUME

    def close(self) -> None:
        """Close LSEG session gracefully."""
        try:
            if self.session is not None:
                self.session.close()
            rd.close_session()
            if self._session_open:
                LOGGER.info("LSEG session closed")
        except Exception:
            LOGGER.exception("Error while closing LSEG session")
        finally:
            self._session_open = False
