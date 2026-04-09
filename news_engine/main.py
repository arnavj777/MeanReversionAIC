"""
News Engine - Main entry point.

This engine subscribes to PCA anomalies and filters them
based on news sentiment analysis.
"""
import logging
import os
import signal
import sys
from datetime import datetime

from shared.messaging import MessageBroker
from news_engine.providers import (
    FallbackNewsClient,
    FinnhubNewsClient,
    NewsHeadline,
    YFinanceNewsClient,
)


LOGGER = logging.getLogger("news_engine")
LOOKBACK_HOURS = int(os.getenv("NEWS_LOOKBACK_HOURS", "24"))
NEGATIVE_SENTIMENT_THRESHOLD = float(os.getenv("NEWS_SENTIMENT_BLOCK_THRESHOLD", "-0.15"))
FALLBACK_ON_EMPTY = os.getenv("NEWS_FALLBACK_ON_EMPTY", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

POSITIVE_WORDS = {
    "beat",
    "beats",
    "upgrade",
    "upgrades",
    "strong",
    "growth",
    "bullish",
    "record",
    "profit",
    "profits",
    "outperform",
    "surge",
    "rally",
}

NEGATIVE_WORDS = {
    "miss",
    "misses",
    "downgrade",
    "downgrades",
    "weak",
    "lawsuit",
    "probe",
    "fraud",
    "loss",
    "losses",
    "warning",
    "cut",
    "cuts",
    "bearish",
    "plunge",
    "drop",
}


def _score_text(text: str) -> float:
    tokens = [part.strip(".,:;!?()[]{}\"'").lower() for part in text.split() if part]
    if not tokens:
        return 0.0

    positive_hits = sum(1 for token in tokens if token in POSITIVE_WORDS)
    negative_hits = sum(1 for token in tokens if token in NEGATIVE_WORDS)
    raw_score = positive_hits - negative_hits
    return raw_score / max(len(tokens), 1)


def _aggregate_sentiment(headlines: list[NewsHeadline]) -> float:
    if not headlines:
        return 0.0

    scores = [_score_text(f"{item.title} {item.summary}") for item in headlines]
    return sum(scores) / len(scores)


def process_anomaly(
    message_data: dict,
    broker: MessageBroker,
    news_client: FallbackNewsClient,
):
    """
    Callback function for processing received anomalies.
    
    Args:
        message_data: Dictionary containing anomaly data from PCA Engine.
        broker: Shared Redis message broker.
        news_client: News client with provider fallback.
    """
    ticker = message_data.get("ticker")
    z_score = message_data.get("z_score")

    if not ticker:
        LOGGER.warning("Dropped malformed message without ticker: %s", message_data)
        return

    headlines, provider_used = news_client.get_recent_headlines(
        ticker,
        lookback_hours=LOOKBACK_HOURS,
    )
    news_volume = len(headlines)
    sentiment_score = _aggregate_sentiment(headlines)
    LOGGER.info(
        "Anomaly received ticker=%s z_score=%s news_volume=%s sentiment=%.4f provider=%s lookback_hours=%s",
        ticker,
        z_score,
        news_volume,
        sentiment_score,
        provider_used,
        LOOKBACK_HOURS,
    )

    if sentiment_score > NEGATIVE_SENTIMENT_THRESHOLD:
        payload = {
            **message_data,
            "signal_type": "mean_reversion",
            "news_volume": news_volume,
            "news_provider": provider_used,
            "news_sentiment": round(sentiment_score, 6),
            "news_filter_passed": True,
            "news_checked_at": datetime.utcnow().isoformat(),
        }
        subscribers = broker.publish_execution_signal(payload)
        LOGGER.info(
            "Trade approved for %s: liquidity shock candidate (subscribers=%s)",
            ticker,
            subscribers,
        )
        return

    LOGGER.info(
        "Trade rejected for %s: sentiment %.4f <= threshold %.4f (provider=%s).",
        ticker,
        sentiment_score,
        NEGATIVE_SENTIMENT_THRESHOLD,
        provider_used,
    )


def main():
    """Main entry point for the News Engine."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    LOGGER.info("News Engine starting")
    broker = MessageBroker()
    news_client = FallbackNewsClient(
        primary=FinnhubNewsClient(),
        fallback=YFinanceNewsClient(),
        fallback_on_empty=FALLBACK_ON_EMPTY,
    )

    shutting_down = {"value": False}

    def _shutdown_handler(signum, _frame):
        if shutting_down["value"]:
            return
        shutting_down["value"] = True
        LOGGER.info("Received shutdown signal %s. Closing news clients...", signum)
        news_client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    def _callback(message_data: dict):
        process_anomaly(message_data, broker, news_client)
    
    LOGGER.info("News Engine listening for PCA anomalies")
    broker.subscribe_to_anomalies(_callback)


if __name__ == "__main__":
    main()