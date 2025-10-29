 #!/usr/bin/env python3
"""
ibkr_news_bot_m3.py

- Connects to IB Gateway / TWS using ib_insync
- Qualifies contracts for configured tickers
- Requests historical news headlines and (optionally) article bodies
- Prints results to console and optionally posts to Telegram

Requirements:
    pip install ib_insync python-dotenv aiohttp

Notes:
 - You must run TWS or IB Gateway locally or remotely and enable API connections.
 - Configure connection and behavior via environment variables or edit CONFIG below.
 - IB historical news delivery depends on provider availability for a given contract.
 - See IB TWS API and ib_insync docs for details.
   (ib_insync docs: https://ib-insync.readthedocs.io) 
   (IB API news: https://interactivebrokers.github.io/tws-api/news.html)
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from ib_insync import IB, Stock, util, Contract
from dotenv import load_dotenv

# Optional Telegram posting
import aiohttp

# Load .env if present
load_dotenv()

# ======= CONFIG (change environment variables or edit here) =======
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", os.getenv("IB_PORT", "4002")))  # 4001/4002 depending on TWS/Gateway mode
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", os.getenv("IB_CLIENT_ID", "1234")))

# Tickers to monitor (comma-separated env var or default)
TICKERS = [t.strip() for t in os.getenv("IB_TICKERS", "AAPL,TSLA,SPY").split(",") if t.strip()]

# How often to poll (seconds). Default 600s = 10 minutes
POLL_SECONDS = int(os.getenv("IB_POLL_SECONDS", "600"))

# How many historical news items to request per ticker each run (max)
HIST_NEWS_LIMIT = int(os.getenv("IB_HIST_NEWS_LIMIT", "20"))

# Whether to fetch the full article body for each news item (slower)
FETCH_ARTICLE_BODY = os.getenv("IB_FETCH_ARTICLE_BODY", "true").lower() in ("1", "true", "yes")

# Telegram config (optional)
TELEGRAM_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TG_CHAT_ID")  # numeric or string id

# Simple local memory of seen article IDs to avoid duplicates across runs
SEEN_ARTICLE_IDS_FILE = os.getenv("SEEN_IDS_FILE", "seen_article_ids.txt")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ibkr_news_bot_m3")

# ==============================================================

def load_seen_ids(path: str) -> set:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def save_seen_ids(path: str, ids: set):
    with open(path, "w", encoding="utf-8") as f:
        for _id in sorted(ids):
            f.write(f"{_id}\n")

async def post_to_telegram(session: aiohttp.ClientSession, text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured; skipping post.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            if resp.status != 200:
                txt = await resp.text()
                logger.warning("Telegram post failed %s: %s", resp.status, txt)
    except Exception:
        logger.exception("Exception while posting to Telegram")

def short_time_ago(ts: Optional[int]) -> str:
    if ts is None or ts == 0:
        return "unknown time"
    # IB sends timestamps as epoch seconds (usually)
    dt = datetime.fromtimestamp(ts)
    diff = datetime.now() - dt
    s = int(diff.total_seconds())
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s//60}m ago"
    if s < 86400:
        return f"{s//3600}h ago"
    return dt.isoformat(" ", "seconds")

async def fetch_and_publish_news(ib: IB, contracts: Dict[str, Contract], seen_ids: set, session: Optional[aiohttp.ClientSession]):
    """
    For each contract, request historical news headlines and optionally article bodies.
    Uses ib.reqHistoricalNewsAsync under the hood (ib_insync wrapper).
    """
    # We'll use an increasing reqId
    req_id = 1000
    for symbol, contract in contracts.items():
        try:
            logger.info("Requesting news for %s (conId=%s)", symbol, getattr(contract, "conId", "N/A"))
            # If we don't have a conId, try to qualify the contract:
            if not getattr(contract, "conId", None):
                logger.debug("No conId for %s; trying to qualify contract", symbol)
                details = ib.qualifyContracts(contract)
                if not details:
                    logger.warning("Could not qualify contract for %s; skipping", symbol)
                    continue
                contract = details[0].contract

            conId = contract.conId

            # providerCodes: empty string "" requests all known providers (may behave differently per IB config)
            provider_codes = ""

            # Request historical news; using async variant so we can await it without registering explicit callbacks.
            # Signature in ib_insync: reqHistoricalNewsAsync(reqId, conId, providerCodes, startDateTime, endDateTime, totalResults, options=None)
            # Use timeframe: last 7 days (ISO formatted times) to reduce results; IB may ignore depending on provider.
            end_dt = datetime.utcnow()
            start_dt = end_dt - timedelta(days=7)
            start_str = start_dt.strftime("%Y%m%d %H:%M:%S")
            end_str = end_dt.strftime("%Y%m%d %H:%M:%S")

            # Call async method
            news_ticks = await ib.reqHistoricalNewsAsync(req_id, conId, provider_codes, start_str, end_str, HIST_NEWS_LIMIT)
            # news_ticks is a list of NewsTick-like objects with fields: providerCode, articleId, headline, timeStamp, extraData
            logger.debug("Received %d news ticks for %s", len(news_ticks) if news_ticks else 0, symbol)

            if not news_ticks:
                logger.info("No news for %s", symbol)
                req_id += 1
                continue

            for nt in news_ticks:
                # Construct a stable article key to deduplicate. Some exchanges/providers may include providerCode + articleId
                article_key = f"{getattr(nt, 'providerCode', '')}:{getattr(nt, 'articleId', '')}"
                if article_key in seen_ids:
                    logger.debug("Already seen %s, skipping", article_key)
                    continue

                ts = getattr(nt, "timeStamp", None)
                headline = getattr(nt, "headline", "") or ""
                extra = getattr(nt, "extraData", "") or ""
                provider = getattr(nt, "providerCode", "")

                # Format and print
                out_lines = [
                    f"=== {symbol} | {provider} | {short_time_ago(ts)} ===",
                    f"Headline: {headline}",
                ]
                if extra:
                    out_lines.append(f"Extra: {extra}")

                article_body = None
                if FETCH_ARTICLE_BODY:
                    # request the article body via reqNewsArticle (sync wrapper in ib_insync):
                    try:
                        # note: IB's reqNewsArticle uses provider and articleId fields
                        art = ib.reqNewsArticle(getattr(nt, "providerCode", ""), getattr(nt, "articleId", ""))
                        # art will typically be a tuple or object — ib_insync returns a string body if available (check docs)
                        article_body = art if isinstance(art, str) else str(art)
                        if article_body:
                            out_lines.append("--- Article body (truncated) ---")
                            out_lines.append(article_body[:2000])  # truncate to keep output manageable
                    except Exception:
                        logger.exception("Failed to fetch article body for %s", article_key)

                # join and publish to console
                text = "\n".join(out_lines)
                print(text)
                print()

                # Optionally post to Telegram
                if session and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                    try:
                        await post_to_telegram(session, f"{symbol} — {headline}\n{provider}\n{short_time_ago(ts)}")
                    except Exception:
                        logger.exception("Telegram post failure")

                # mark seen
                seen_ids.add(article_key)

            req_id += 1

        except Exception:
            logger.exception("Exception while fetching news for %s", symbol)


async def main_loop():
    # load seen ids
    seen_ids = load_seen_ids(SEEN_ARTICLE_IDS_FILE)
    logger.info("Loaded %d seen article ids", len(seen_ids))

    ib = IB()
    util.useQt(False)  # ensure non-Qt loop (works in headless environments)
    try:
        logger.info("Connecting to IB at %s:%s (clientId=%s)...", IB_HOST, IB_PORT, IB_CLIENT_ID)
        ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)
        if not ib.isConnected():
            logger.error("Could not connect to IB (is TWS/IB Gateway running and API enabled?). Exiting.")
            return

        # Build Contract objects
        contracts = {}
        for t in TICKERS:
            # default to US stock on SMART exchange
            c = Stock(symbol=t, exchange="SMART", currency="USD")
            contracts[t] = c

        # Optionally create aiohttp session if Telegram configured
        session = None
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            session = aiohttp.ClientSession()

        # Loop forever, polling every POLL_SECONDS
        while True:
            try:
                # Ensure contracts are qualified (get conId)
                logger.debug("Qualifying contracts...")
                for sym, contr in list(contracts.items()):
                    try:
                        details = ib.qualifyContracts(contr)
                        if details:
                            # details[0] is ContractDetails; extract contract
                            contracts[sym] = details[0].contract
                            logger.debug("Qualified %s -> conId=%s", sym, details[0].contract.conId)
                        else:
                            logger.warning("Could not qualify %s", sym)
                    except Exception:
                        logger.exception("Error qualifying %s", sym)

                # Fetch and publish news
                await fetch_and_publish_news(ib, contracts, seen_ids, session)

                # persist seen ids to disk periodically (after each loop)
                save_seen_ids(SEEN_ARTICLE_IDS_FILE, seen_ids)
                logger.debug("Saved %d seen ids", len(seen_ids))

                logger.info("Sleeping for %s seconds...", POLL_SECONDS)
                await asyncio.sleep(POLL_SECONDS)

            except Exception:
                logger.exception("Unhandled exception in main loop; will continue after short delay.")
                await asyncio.sleep(10)

    finally:
        try:
            if session:
                await session.close()
        except Exception:
            pass
        try:
            ib.disconnect()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("User requested exit. Goodbye.")
