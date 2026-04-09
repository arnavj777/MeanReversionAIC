"""News provider clients for Finnhub primary with yfinance fallback."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
import requests
import yfinance as yf


LOGGER = logging.getLogger("news_engine.providers")


@dataclass
class NewsHeadline:
    """Normalized headline item used by the news engine policy layer."""

    title: str
    summary: str
    source: str
    url: str
    published_at: datetime


class FinnhubNewsClient:
    """Fetch company headlines from Finnhub."""

    def __init__(self) -> None:
        load_dotenv()
        self.api_key = os.getenv("FINNHUB_API_KEY", "").strip()
        self.timeout_seconds = float(os.getenv("NEWS_HTTP_TIMEOUT_SECONDS", "10"))
        if not self.api_key:
            LOGGER.warning("FINNHUB_API_KEY is empty; Finnhub calls will fail")

    def get_recent_headlines(self, ticker: str, lookback_hours: int = 24) -> list[NewsHeadline]:
        if not ticker:
            return []

        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(hours=lookback_hours)

        params = {
            "symbol": str(ticker).upper(),
            "from": start_utc.date().isoformat(),
            "to": now_utc.date().isoformat(),
            "token": self.api_key,
        }

        response = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("Finnhub response is not a list")

        headlines: list[NewsHeadline] = []
        for item in payload:
            if not isinstance(item, dict):
                continue

            published_epoch = item.get("datetime")
            if not published_epoch:
                continue

            published_at = datetime.fromtimestamp(int(published_epoch), tz=timezone.utc)
            if published_at < start_utc:
                continue

            title = str(item.get("headline") or "").strip()
            summary = str(item.get("summary") or "").strip()
            if not title and not summary:
                continue

            headlines.append(
                NewsHeadline(
                    title=title,
                    summary=summary,
                    source=str(item.get("source") or "finnhub"),
                    url=str(item.get("url") or ""),
                    published_at=published_at,
                )
            )

        return headlines

    def close(self) -> None:
        """No-op close method for interface compatibility."""
        return


class YFinanceNewsClient:
    """Fetch ticker headlines via yfinance as fallback."""

    def get_recent_headlines(self, ticker: str, lookback_hours: int = 24) -> list[NewsHeadline]:
        if not ticker:
            return []

        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(hours=lookback_hours)
        payload = yf.Ticker(str(ticker).upper()).news or []

        headlines: list[NewsHeadline] = []
        for item in payload:
            if not isinstance(item, dict):
                continue

            content = item.get("content") if isinstance(item.get("content"), dict) else {}
            title = str(content.get("title") or item.get("title") or "").strip()
            summary = str(content.get("summary") or item.get("summary") or "").strip()
            if not title and not summary:
                continue

            published_epoch = (
                content.get("pubDate")
                or item.get("providerPublishTime")
                or item.get("published_at")
            )
            published_at = _parse_yf_timestamp(published_epoch) or now_utc
            if published_at < start_utc:
                continue

            provider = content.get("provider") if isinstance(content.get("provider"), dict) else {}
            source = str(provider.get("displayName") or item.get("publisher") or "yfinance")
            url = str(content.get("canonicalUrl", {}).get("url") or item.get("link") or "")

            headlines.append(
                NewsHeadline(
                    title=title,
                    summary=summary,
                    source=source,
                    url=url,
                    published_at=published_at,
                )
            )

        return headlines

    def close(self) -> None:
        """No-op close method for interface compatibility."""
        return


class FallbackNewsClient:
    """Try Finnhub first, then yfinance fallback when needed."""

    def __init__(
        self,
        primary: FinnhubNewsClient,
        fallback: YFinanceNewsClient,
        fallback_on_empty: bool,
    ) -> None:
        self.primary = primary
        self.fallback = fallback
        self.fallback_on_empty = fallback_on_empty

    def get_recent_headlines(
        self,
        ticker: str,
        lookback_hours: int = 24,
    ) -> tuple[list[NewsHeadline], str]:
        try:
            primary_headlines = self.primary.get_recent_headlines(
                ticker,
                lookback_hours=lookback_hours,
            )
            if primary_headlines:
                return primary_headlines, "finnhub"
            if not self.fallback_on_empty:
                return [], "finnhub"
        except Exception:
            LOGGER.exception("Finnhub fetch failed for %s; attempting yfinance fallback", ticker)

        try:
            fallback_headlines = self.fallback.get_recent_headlines(
                ticker,
                lookback_hours=lookback_hours,
            )
            return fallback_headlines, "yfinance"
        except Exception:
            LOGGER.exception("yfinance fallback failed for %s", ticker)
            return [], "none"

    def close(self) -> None:
        self.primary.close()
        self.fallback.close()


def _parse_yf_timestamp(value: Optional[object]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value), tz=timezone.utc)

    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        # yfinance may return ISO timestamps ending with Z.
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    return None
