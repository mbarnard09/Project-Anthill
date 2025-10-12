import os
import time
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from contextlib import contextmanager

import requests
from requests_oauthlib import OAuth1
import psycopg
from psycopg.rows import dict_row


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_env_var(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Environment variable {name} not set.")
    return value


@contextmanager
def get_db_connection(db_url: str):
    conn = None
    try:
        conn = psycopg.connect(db_url, row_factory=dict_row)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Database transaction failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()


def compute_next_9am_et(now_utc: datetime) -> datetime:
    try:
        now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        # Fallback to UTC if TZ not available
        now_et = now_utc
    target_et = now_et.replace(hour=9, minute=0, second=0, microsecond=0)
    if now_et >= target_et:
        target_et = target_et + timedelta(days=1)
    # Convert back to UTC for sleeping calculations
    try:
        target_utc = target_et.astimezone(timezone.utc)
    except Exception:
        target_utc = target_et
    return target_utc


def format_tweet(rows: list[dict]) -> str:
    header = (
        "We use GPT-5 to determine the sentiment of online comments. "
        "Here are the most bullishly mentioned stocks across Reddit, Stocktwits and 4Chan this morning."
    )
    lines = [header, ""]
    for idx, r in enumerate(rows[:3], start=1):
        ticker = r.get("ticker") or "?"
        mentions = int(r.get("mention_count") or 0)
        pct = float(r.get("bullish_pct") or 0.0)
        lines.append(f"{idx}. ${ticker} - {mentions} mentions || {pct:.1f}% Bullish")
    tweet = "\n".join(lines)
    # Trim if somehow exceeds 280 chars (unlikely but safe)
    if len(tweet) > 280:
        tweet = tweet[:279] + "…"
    return tweet


def post_tweet(text: str, cfg: dict) -> bool:
    try:
        if not cfg.get("twitter_enabled", False):
            logging.info("Twitter posting disabled. Preview only:\n" + text)
            return False
        ck = cfg.get('twitter_consumer_key')
        cs = cfg.get('twitter_consumer_secret')
        at = cfg.get('twitter_access_token')
        ats = cfg.get('twitter_access_secret')
        if not all([ck, cs, at, ats]):
            logging.warning("Twitter posting skipped: missing credentials.")
            return False
        url = "https://api.twitter.com/2/tweets"
        auth = OAuth1(ck, cs, at, ats)
        payload = {"text": text}
        resp = requests.post(url, json=payload, auth=auth, timeout=10)
        if 200 <= resp.status_code < 300:
            logging.info("Daily tweet posted successfully.")
            return True
        logging.error(f"Failed to post daily tweet. Status={resp.status_code} Body={resp.text}")
        return False
    except Exception as e:
        logging.error(f"Error while posting daily tweet: {e}", exc_info=True)
        return False


def fetch_top_bullish_stocks(conn, since_utc: datetime, min_mentions: int = 100) -> list[dict]:
    sql = """
        WITH window AS (
            SELECT c.asset_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN c.sentiment = 1 THEN 1 ELSE 0 END) AS pos,
                   SUM(CASE WHEN c.sentiment = -1 THEN 1 ELSE 0 END) AS neg
            FROM comments c
            JOIN assets a ON a.id = c.asset_id
            JOIN sources s ON s.id = c.source_id
            WHERE a.universe = 'stock'
              AND c.commented_at >= %s
              AND (
                s.name LIKE '/r/%' OR s.name IN ('4Chan', 'StockTwits')
              )
            GROUP BY c.asset_id
            HAVING COUNT(*) > %s
        ), ranked AS (
            SELECT w.asset_id,
                   w.total,
                   w.pos,
                   w.neg,
                   CASE WHEN (w.pos + w.neg) > 0
                        THEN 100.0 * w.pos::float / (w.pos + w.neg)
                        ELSE NULL END AS bullish_pct
            FROM window w
        )
        SELECT a.id,
               a.ticker,
               a.name,
               r.total AS mention_count,
               r.pos,
               r.neg,
               r.bullish_pct
        FROM ranked r
        JOIN assets a ON a.id = r.asset_id
        ORDER BY r.bullish_pct DESC NULLS LAST, r.total DESC
        LIMIT 3;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (since_utc, min_mentions))
        rows = cur.fetchall() or []
        return rows


def run_once(cfg: dict):
    since_utc = datetime.now(timezone.utc) - timedelta(hours=12)
    logging.info("Querying top bullish stocks for last 12 hours (min 100 mentions)...")
    with get_db_connection(cfg['database_url']) as conn:
        rows = fetch_top_bullish_stocks(conn, since_utc, cfg['min_mentions'])
    if not rows:
        logging.info("No qualifying stocks found for the daily tweet window. Skipping post.")
        return
    tweet = format_tweet(rows)
    post_tweet(tweet, cfg)


def main():
    logging.info("Starting daily tweet service...")
    try:
        cfg = {
            "database_url": get_env_var("POSTGRES_URL"),
            "min_mentions": int(os.getenv("DAILY_TWEET_MIN_MENTIONS", "100")),
            "twitter_enabled": os.getenv("TWITTER_ENABLED", "false").lower() in ("1", "true", "yes"),
            "twitter_consumer_key": os.getenv("TWITTER_CONSUMER_KEY"),
            "twitter_consumer_secret": os.getenv("TWITTER_CONSUMER_SECRET"),
            "twitter_access_token": os.getenv("TWITTER_ACCESS_TOKEN"),
            "twitter_access_secret": os.getenv("TWITTER_ACCESS_SECRET"),
        }
    except (ValueError, TypeError) as e:
        logging.error(f"Configuration error: {e}")
        return

    while True:
        now_utc = datetime.now(timezone.utc)
        next_run_utc = compute_next_9am_et(now_utc)
        sleep_seconds = max(0.0, (next_run_utc - now_utc).total_seconds())
        logging.info(f"Next daily tweet scheduled at {next_run_utc.isoformat()}. Sleeping {sleep_seconds:.1f}s...")
        time.sleep(sleep_seconds)
        try:
            run_once(cfg)
        except Exception as e:
            logging.error(f"Error running daily tweet: {e}", exc_info=True)


if __name__ == "__main__":
    main()


