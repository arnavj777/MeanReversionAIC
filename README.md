# stat_arb_bot

## Strategy Overview

**stat_arb_bot** is a residual-based mean reversion statistical arbitrage system that combines PCA factor modeling with sentiment analysis and intelligent risk management.

### Core Strategy Logic

The bot identifies stocks that have deviated significantly from their expected returns given broader market factors, then trades on the assumption they'll revert:

1. **Factor extraction** (PCA Engine): Uses Principal Component Analysis to extract 2 main market factors from your stock universe (AAPL, MSFT, NVDA by default). These factors represent systematic drivers of returns across all stocks.

2. **Anomaly detection**: Calculates residuals—how much each stock's actual returns deviate from what the PCA model predicts. Converts residuals into Z-scores over a rolling 120-period window.

3. **Signal generation**:
   - **BUY signal**: Z-score ≤ -2.0 (stock severely underperforming vs. factors → expected to bounce back up)
   - **SELL signal**: Z-score ≥ +2.0 (stock severely outperforming vs. factors → expected to pull back down)
   - **HOLD**: All other cases

4. **Sentiment filtering** (News Engine): Before executing, checks whether the stock has negative news (earnings miss, downgrade, "fraud", "lawsuit", etc.). If sentiment score ≤ -0.15, blocks the trade to avoid trading on fundamental bad news.

5. **Risk-managed execution**: Places limit orders at 10 basis points better than current price, with volatility-adjusted spreads (wider spreads for volatile stocks).

### Why Mean Reversion?

The strategy assumes that **temporary shocks cause stocks to deviate from their fundamental relationships with market factors**, but these deviations are mean-reverting. The two PCA components capture what should drive returns; anything beyond that is treated as a temporary anomaly ready to snap back.

### Data Flow

```
LSEG API/Mock Data
      ↓
Data Ingestion Engine (3-hour cycles)
      ↓
TimescaleDB (OHLCV storage)
      ↓
┌─────────────────────────────────────┐
│ PCA Engine (30-second cycles)       │ → generates residual Z-scores
└─────────────────────────────────────┘
      ↓ (Redis: pca_anomalies)
┌─────────────────────────────────────┐
│ News Engine                         │ → sentiment filtering
└─────────────────────────────────────┘
      ↓ (Redis: execution_signals)
┌─────────────────────────────────────┐
│ Execution Engine                    │ → order placement & risk mgmt
└─────────────────────────────────────┘
      ↓
CSV Logs & Broker API
```

### Key Components

| Component | Role | Input | Output | Frequency |
|-----------|------|-------|--------|-----------|
| **Data Ingestion** | Fetch market prices | LSEG/Mock | TimescaleDB OHLCV | Every 3 hours |
| **PCA Engine** | Signal generation via residual analysis | TimescaleDB returns | Redis anomalies (Z-scores) | Every 30 seconds |
| **News Engine** | Filter signals on negative sentiment | Redis anomalies + Finnhub API | Redis execution signals | On anomaly |
| **Execution Engine** | Order management & risk controls | Redis signals + DB prices | CSV logs + Broker API | On filtered signal |

### Risk Controls

- Only trades when Z-score crosses ±2.0 (statistical significance)
- Rejects trades if explained variance < 50% (model quality gate)
- Blocks trades during negative sentiment periods (score ≤ -0.15)
- Caps position sizing at 300 basis points maximum spread
- Guards against extreme outliers (Z-score > ±6.0 rejected)
- Volatility-adjusted limit orders (10 basis points standard, up to 300 basis points for high-vol stocks)

### Environment Configuration

Key environment variables control strategy behavior:

- `PCA_TICKERS`: Stock universe (default: AAPL,MSFT,NVDA)
- `PCA_LOOKBACK_DAYS`: Historical window for returns (default: 30)
- `PCA_ZSCORE_THRESHOLD`: Signal threshold (default: 2.0)
- `PCA_MIN_EXPLAINED_VARIANCE`: Model quality gate (default: 0.50)
- `NEWS_SENTIMENT_BLOCK_THRESHOLD`: Sentiment block threshold (default: -0.15)
- `EXECUTION_MAX_ABS_ZSCORE`: Risk limit for extreme signals (default: 6.0)

---

## Run Modes

The PCA service can run in two modes without invoking any LSEG API calls:

- `continuous`: default service mode that runs every `PCA_LOOP_SECONDS`.
- `once`: run one PCA cycle and exit.

PCA uses TimescaleDB local returns only. LSEG is used by `news_engine` only.

## PCA-Only Start (No LSEG calls)

Start only the required services:

```bash
docker compose up -d timescaledb redis data_ingestion pca_engine
```

Tail PCA logs:

```bash
docker compose logs -f pca_engine
```

## One-Shot PCA Cycle

Run one cycle against local DB and exit:

```bash
docker compose run --rm pca_engine python -m pca_engine.main --mode once
```

## Optional PCA Environment Variables

These variables can be set in `.env`:

- `PCA_TICKERS=AAPL,MSFT,NVDA`
- `PCA_LOOKBACK_DAYS=30`
- `PCA_LOOP_SECONDS=30`
- `PCA_MIN_OBSERVATIONS=200`
- `PCA_RESIDUAL_Z_WINDOW=120`
- `PCA_ZSCORE_THRESHOLD=2.0`
- `PCA_MIN_EXPLAINED_VARIANCE=0.50`
- `PCA_MODE=continuous`

## Full Stack (includes LSEG-dependent news engine)

```bash
docker compose up -d --build
```
