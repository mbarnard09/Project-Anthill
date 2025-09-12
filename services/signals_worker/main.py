import os
import time
import logging
from datetime import datetime, timezone, timedelta
import random
from contextlib import contextmanager
from zoneinfo import ZoneInfo
import requests
import math
from requests_oauthlib import OAuth1
try:
    # Prefer precise NYSE holiday calendar if available
    from holidays.financial import NYSE as _HolidayProvider  # type: ignore
    _HOLIDAY_PROVIDER = _HolidayProvider()
except Exception:
    try:
        import holidays as _holidays_mod  # type: ignore
        _HOLIDAY_PROVIDER = _holidays_mod.UnitedStates()
    except Exception:
        _HOLIDAY_PROVIDER = None

import psycopg
from psycopg.rows import dict_row
import numpy as np
from psycopg.types.json import Jsonb
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from openai import OpenAI
import math

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_env_var(name, default=None):
    """Gets an environment variable or returns a default."""
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Environment variable {name} not set.")
    return value

@contextmanager
def get_db_connection(db_url: str):
    """Provides a database connection."""
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

@contextmanager
def advisory_lock(conn, lock_name):
    """Acquires a Postgres advisory lock for the duration of the context."""
    lock_acquired = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s));", (lock_name,))
            result = cur.fetchone()
            lock_acquired = result['pg_try_advisory_lock'] if result else False
            if lock_acquired:
                logging.info(f"Acquired lock: {lock_name}")
                yield True
            else:
                logging.warning(f"Could not acquire lock: {lock_name}. Another worker may be running.")
                yield False
    finally:
        if lock_acquired:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(hashtext(%s));", (lock_name,))
                logging.info(f"Released lock: {lock_name}")

def setup_heartbeat_table(conn):
    """Ensures the job_heartbeats table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS job_heartbeats (
                job_name VARCHAR(255) PRIMARY KEY,
                last_ok TIMESTAMPTZ,
                last_err TEXT
            );
        """)
    logging.info("job_heartbeats table checked/created.")

def update_heartbeat(conn, job_name: str, status: str, error_message: str | None = None):
    """Updates the heartbeat for a job."""
    with conn.cursor() as cur:
        if status == "ok":
            cur.execute("""
                INSERT INTO job_heartbeats (job_name, last_ok, last_err)
                VALUES (%s, NOW(), NULL)
                ON CONFLICT (job_name) DO UPDATE SET last_ok = NOW(), last_err = NULL;
            """, (job_name,))
        elif status == "error":
            cur.execute("""
                INSERT INTO job_heartbeats (job_name, last_err)
                VALUES (%s, %s)
                ON CONFLICT (job_name) DO UPDATE SET last_err = %s;
            """, (job_name, error_message, error_message))
    logging.info(f"Heartbeat updated for {job_name} with status: {status}")

def is_us_market_holiday(now_utc: datetime) -> bool:
    """Returns True if today is a US stock market holiday (best-effort).

    Uses NYSE calendar when available, otherwise falls back to US federal holidays.
    """
    if _HOLIDAY_PROVIDER is None:
        return False
    try:
        now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
        d = now_et.date()
        return d in _HOLIDAY_PROVIDER
    except Exception:
        return False

def is_us_equity_market_hours(now_utc: datetime) -> bool:
    """Approximate US equity regular hours: 09:30–16:00 ET on weekdays (no holiday calendar)."""
    try:
        now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        logging.info("Market-hours check: timezone conversion failed; treating as closed")
        return False
    if now_et.weekday() >= 5:
        logging.info(f"Market-hours check: weekend (weekday={now_et.weekday()}) -> closed")
        return False
    if is_us_market_holiday(now_utc):
        logging.info("Market-hours check: holiday -> closed")
        return False
    start = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    is_open = start <= now_et <= end
    logging.info(f"Market-hours check: now_et={now_et.isoformat()} in regular hours -> {is_open}")
    return is_open

def fetch_tiingo_intraday_snapshot(ticker: str, token: str) -> dict | None:
    """Fetches intraday snapshot from Tiingo IEX endpoint for a single ticker.

    Returns a dict with keys 'last' and 'prevClose' if available.
    """
    url = "https://api.tiingo.com/iex"
    params = {"tickers": ticker, "token": token}
    try:
        logging.info(f"Tiingo intraday request for {ticker}")
        resp = requests.get(url, params=params, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            row = data[0]
            result = {
                "last": row.get("last"),
                "prevClose": row.get("prevClose"),
                "yearHigh": row.get("yearHigh"),
                "yearLow": row.get("yearLow"),
            }
            logging.info(f"Tiingo intraday snapshot {ticker}: last={result['last']} prevClose={result['prevClose']} yearHigh={result['yearHigh']} yearLow={result['yearLow']}")
            return result
    except Exception as e:
        logging.warning(f"Tiingo intraday snapshot failed for {ticker}: {e}")
    return None

def fetch_tiingo_eod_prices(ticker: str, start_date: str, end_date: str, token: str) -> list[dict]:
    """Fetch Tiingo EOD daily prices for [start_date, end_date]. Returns list of bars sorted by date."""
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {"startDate": start_date, "endDate": end_date, "token": token}
    try:
        logging.info(f"Tiingo EOD request for {ticker}: {start_date} -> {end_date}")
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            logging.info(f"Tiingo EOD received {len(data)} bars for {ticker}")
            return data
    except Exception as e:
        logging.warning(f"Tiingo EOD fetch failed for {ticker}: {e}")
    return []

def compute_pct_change_1d_equity(ticker: str, now_utc: datetime, token: str) -> float | None:
    """Computes 1-day % change for equities using intraday when appropriate else EOD closes.

    Rules:
    - If market hours and intraday delta indicates a real move, use (last - prevClose)/prevClose*100
    - Otherwise, compute using last two adjusted EOD closes
    Returns a rounded float (2 decimals) or None if unavailable.
    """
    try:
        if is_us_equity_market_hours(now_utc):
            snap = fetch_tiingo_intraday_snapshot(ticker, token)
            if snap and snap.get("last") is not None and snap.get("prevClose") not in (None, 0):
                last = float(snap["last"])  # most recent trade price
                prev_close = float(snap["prevClose"])  # previous trading day's close
                # "real move" check: ignore if essentially unchanged
                delta = last - prev_close
                logging.info(f"1d calc (intraday) {ticker}: last={last} prevClose={prev_close} delta={delta}")
                if abs(delta) >= max(0.01, 0.0005 * prev_close):
                    pct = (last - prev_close) / prev_close * 100.0
                    logging.info(f"1d calc (intraday) {ticker}: pct={pct}")
                    return round(pct, 2)

        # Fallback to EOD closes (also used outside market hours)
        end_dt = now_utc.date()
        start_dt = end_dt - timedelta(days=10)
        bars = fetch_tiingo_eod_prices(ticker, start_dt.isoformat(), end_dt.isoformat(), token)
        # Use adjusted closes from the last two trading days
        closes = [b.get("adjClose") for b in bars if b.get("adjClose") is not None]
        if len(closes) >= 2:
            prev, last = closes[-2], closes[-1]
            if prev not in (None, 0):
                pct = (last - prev) / prev * 100.0
                logging.info(f"1d calc (EOD) {ticker}: prev={prev} last={last} pct={pct}")
                return round(pct, 2)
    except Exception as e:
        logging.warning(f"Failed 1d change calc for {ticker}: {e}")
    return None

def compute_pct_change_7d_equity(ticker: str, now_utc: datetime, token: str) -> float | None:
    """Computes 7-day % change using adjusted EOD closes, choosing the closest earlier trading day ~7 days prior.

    Returns a rounded float (2 decimals) or None if unavailable.
    """
    try:
        end_dt = now_utc.date()
        start_dt = end_dt - timedelta(days=14)
        target_dt = end_dt - timedelta(days=7)
        bars = fetch_tiingo_eod_prices(ticker, start_dt.isoformat(), end_dt.isoformat(), token)
        if not bars:
            return None
        # Ensure sorted by date ascending
        try:
            bars.sort(key=lambda b: b.get("date", ""))
        except Exception:
            pass
        # Map date->adjClose
        eod = []
        for b in bars:
            adj = b.get("adjClose")
            d = b.get("date")
            if adj is None or d is None:
                continue
            # Tiingo dates like 2024-08-09T00:00:00.000Z -> take date part
            d_str = d[:10]
            eod.append((d_str, float(adj)))
        if not eod:
            return None
        # latest close
        last_date, last_close = eod[-1]
        # find closest earlier trading day to target_dt
        target_str = target_dt.isoformat()
        candidate = None
        for d_str, c in reversed(eod):
            if d_str <= target_str:
                candidate = (d_str, c)
                break
        if candidate is None:
            # no earlier trading day; take earliest available
            candidate = eod[0]
        prev_date, prev_close = candidate
        if prev_close in (None, 0):
            return None
        pct = (last_close - prev_close) / prev_close * 100.0
        logging.info(f"7d calc {ticker}: last=({last_date},{last_close}) prev=({prev_date},{prev_close}) pct={pct}")
        return round(pct, 2)
    except Exception as e:
        logging.warning(f"Failed 7d change calc for {ticker}: {e}")
    return None

def get_current_price_equity(ticker: str, now_utc: datetime, token: str) -> float | None:
    """Gets the current price: use intraday last when available, otherwise last EOD adjClose."""
    try:
        snap = fetch_tiingo_intraday_snapshot(ticker, token)
        if snap and snap.get("last") is not None:
            price = float(snap["last"])
            logging.info(f"Current price (intraday) {ticker}: {price}")
            return round(price, 2)
        end_dt = now_utc.date()
        start_dt = end_dt - timedelta(days=10)
        bars = fetch_tiingo_eod_prices(ticker, start_dt.isoformat(), end_dt.isoformat(), token)
        closes = [b.get("adjClose") for b in bars if b.get("adjClose") is not None]
        if closes:
            price = float(closes[-1])
            logging.info(f"Current price (EOD) {ticker}: {price}")
            return round(price, 2)
    except Exception as e:
        logging.warning(f"Failed current price for {ticker}: {e}")
    return None

def fetch_52wk_high_low_equity(ticker: str, now_utc: datetime, token: str) -> tuple[float | None, float | None]:
    """Returns (yearHigh, yearLow) if available; falls back to computing from ~370 days EOD adjClose."""
    try:
        snap = fetch_tiingo_intraday_snapshot(ticker, token)
        yh = snap.get("yearHigh") if snap else None
        yl = snap.get("yearLow") if snap else None
        if yh is not None and yl is not None:
            try:
                yh_f = round(float(yh), 2)
                yl_f = round(float(yl), 2)
                logging.info(f"52w from snapshot {ticker}: high={yh_f} low={yl_f}")
                return yh_f, yl_f
            except Exception:
                pass
    except Exception as e:
        logging.info(f"Snapshot lacked 52w range for {ticker}: {e}")

    try:
        end_dt = now_utc.date()
        start_dt = end_dt - timedelta(days=370)
        bars = fetch_tiingo_eod_prices(ticker, start_dt.isoformat(), end_dt.isoformat(), token)
        closes = [float(b.get("adjClose")) for b in bars if b.get("adjClose") is not None]
        if closes:
            yh_f = round(max(closes), 2)
            yl_f = round(min(closes), 2)
            logging.info(f"52w from EOD {ticker}: high={yh_f} low={yl_f}")
            return yh_f, yl_f
    except Exception as e:
        logging.warning(f"Failed 52w calc for {ticker}: {e}")
    return None, None

def calculate_signals(conn, as_of: datetime, baseline_lookback_windows: int):
    """
    Computes 2-hour Share-of-Voice (SoV) signals.
    """
    window_start = as_of - timedelta(hours=2)
    
    logging.info(f"Calculating signals for window: [{window_start.isoformat()}, {as_of.isoformat()})")

    with conn.cursor() as cur:
        # 1. Get comment counts per asset in the window (stock universe only).
        cur.execute("""
            SELECT c.asset_id, COUNT(*) AS x_asset_2h
            FROM comments c
            JOIN assets a ON a.id = c.asset_id
            WHERE c.commented_at >= %s AND c.commented_at < %s
              AND a.universe = 'stock'
            GROUP BY c.asset_id;
        """, (window_start, as_of))
        asset_counts = cur.fetchall()
        logging.info(f"Found mentions for {len(asset_counts)} assets in the window.")

        if not asset_counts:
            logging.info("No comments found in the current window.")
            return []

        # 2. Calculate total mentions in the window.
        x_total_2h = sum(row['x_asset_2h'] for row in asset_counts)
        logging.info(f"Total mentions across all assets (x_total_2h): {x_total_2h}")
        if x_total_2h == 0:
            logging.info("Total mentions are zero for the current window.")
            return []

        signals_to_insert = []
        asset_ids_with_activity = [row['asset_id'] for row in asset_counts]

        # 3. Fetch historical SoV for baseline calculation
        cur.execute("""
            SELECT asset_id, sov_now FROM asset_signal_windows
            WHERE asset_id = ANY(%s) AND window_hours = 2 AND as_of < %s
            ORDER BY as_of DESC
            LIMIT %s;
        """, (asset_ids_with_activity, as_of, baseline_lookback_windows * len(asset_ids_with_activity)))
        
        history = {}
        logging.info("Fetching historical SoV data for baseline calculation...")
        for row in cur.fetchall():
            if row['asset_id'] not in history:
                history[row['asset_id']] = []
            history[row['asset_id']].append(float(row['sov_now']))
        logging.info(f"Fetched historical data for {len(history)} assets.")

        for i, row in enumerate(asset_counts):
            asset_id = row['asset_id']
            x_asset_2h = row['x_asset_2h']
            
            # 4. Calculate current SoV
            sov_now = x_asset_2h / x_total_2h if x_total_2h > 0 else 0

            # 5. Calculate baseline and Z-score
            mu_sov, sigma_sov, z_sov = 0.0, 0.0, None
            asset_history = history.get(asset_id, [])
            
            if len(asset_history) >= 1: # Need at least one previous window for a baseline
                mu_sov = np.mean(asset_history)
                sigma_sov = np.std(asset_history) # Population stddev
                
                if sigma_sov > 0:
                    z_sov = (sov_now - mu_sov) / sigma_sov
            
            if i < 3: # Log first 3 calculations for inspection
                logging.info(f"  -> Asset {asset_id}: x_asset_2h={x_asset_2h}, sov_now={sov_now:.4f}, mu_sov={mu_sov:.4f}, sigma_sov={sigma_sov:.4f}, z_sov={z_sov if z_sov is None else f'{z_sov:.2f}'} (from {len(asset_history)} prior windows)")

            signals_to_insert.append({
                'asset_id': asset_id,
                'as_of': as_of,
                'window_hours': 2,
                'x_asset_2h': x_asset_2h,
                'x_total_2h': x_total_2h,
                'sov_now': str(sov_now),
                'mu_sov': str(mu_sov),
                'sigma_sov': str(sigma_sov),
                'z_sov': str(z_sov) if z_sov is not None else None
            })

        # 6. Upsert results into asset_signal_windows
        if signals_to_insert:
            upsert_sql = """
                INSERT INTO asset_signal_windows (
                    asset_id, as_of, window_hours, x_asset_2h, x_total_2h, 
                    sov_now, mu_sov, sigma_sov, z_sov
                )
                VALUES (
                    %(asset_id)s, %(as_of)s, %(window_hours)s, %(x_asset_2h)s, %(x_total_2h)s,
                    %(sov_now)s, %(mu_sov)s, %(sigma_sov)s, %(z_sov)s
                )
                ON CONFLICT (asset_id, as_of, window_hours) DO UPDATE SET
                    x_asset_2h = EXCLUDED.x_asset_2h,
                    x_total_2h = EXCLUDED.x_total_2h,
                    sov_now = EXCLUDED.sov_now,
                    mu_sov = EXCLUDED.mu_sov,
                    sigma_sov = EXCLUDED.sigma_sov,
                    z_sov = EXCLUDED.z_sov;
            """
            cur.executemany(upsert_sql, signals_to_insert)
            logging.info(f"Upserted {len(signals_to_insert)} signal rows.")

    # Return the newly computed signals for the alerts step
    return signals_to_insert

def get_asset_details(conn, asset_ids):
    """Fetches asset tickers, names, and universe for a list of asset IDs."""
    if not asset_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, ticker, name, universe FROM assets WHERE id = ANY(%s);", (asset_ids,))
        return {row['id']: {'ticker': row['ticker'], 'name': row['name'], 'universe': row['universe']} for row in cur.fetchall()}

def fetch_comments_for_summary(conn, asset_id: int, start_time: datetime, end_time: datetime, limit: int = 50):
    """Fetches the most recent comment bodies for a given asset in a time window."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT body FROM comments
            WHERE asset_id = %s AND commented_at >= %s AND commented_at < %s
            ORDER BY commented_at DESC
            LIMIT %s;
        """, (asset_id, start_time, end_time, limit))
        return [row['body'] for row in cur.fetchall() if row['body']]

def generate_summary(client: OpenAI, model: str, comments: list[str], ticker: str) -> str:
    """Generates a summary of comments using an LLM."""
    if not comments:
        return "No comments available for summary."

    # Simple concatenation, but truncate to avoid excessive token usage.
    # A more advanced implementation might be more selective.
    context = "\n".join(comments)
    max_context_len = 8000 # Rough character limit to keep tokens reasonable
    if len(context) > max_context_len:
        context = context[:max_context_len]

    prompt = f"""
        The following are recent comments from social media about the stock ${ticker}.
        Please provide a neutral summary that highlights:
        - Key themes and overall sentiment
        - Potential catalysts for the mention spike, with special attention to any NEWS items or headlines referenced
        - Notable disagreements or uncertainty if present
        Keep the response concise but longer if needed to capture material details.

        Comments:
        ---
        {context}
        ---
        Summary:
    """
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=350,
        )
        summary = response.choices[0].message.content
        return summary.strip() if summary else "Could not generate a summary."
    except Exception as e:
        logging.error(f"Failed to generate summary for ${ticker}: {e}", exc_info=True)
        return "Summary generation failed due to an error."

def _safe_percent_str(value: float | None) -> str:
    try:
        if value is None:
            return "N/A"
        return f"{value:.2f}%"
    except Exception:
        return "N/A"

def _safe_money_str(value: float | None) -> str:
    try:
        if value is None:
            return "N/A"
        return f"${value:.2f}"
    except Exception:
        return "N/A"

def _safe_percent_signed(value: float | None) -> str:
    try:
        if value is None:
            return "N/A"
        sign = "+" if value > 0 else ""
        return f"{sign}{value:.2f}%"
    except Exception:
        return "N/A"

def build_tweet_text_full(ticker: str,
                          company_name: str,
                          summary: str,
                          current_price: float | None,
                          pct_change_1d: float | None,
                          pct_change_7d: float | None,
                          year_high: float | None,
                          year_low: float | None) -> str:
    """Builds a full-length tweet (no 280-char truncation)."""
    header = f"🚨 Mention Spike Alert: ${ticker}"
    subheader = "We monitor Reddit, 4Chan, StockTwits and Twitter for unusual surges in mentions and sentiment using GPT5."
    current_price_line = f"Current Price: {_safe_money_str(current_price)}"
    daily_change_line = f"Daily Change: {_safe_percent_signed(pct_change_1d)}"
    if year_high is not None and year_low is not None:
        wk52_line = f"52 Week H/L: {_safe_money_str(year_high)} - {_safe_money_str(year_low)}"
    else:
        wk52_line = "52 Week H/L: N/A"
    summary_header = "AI Summary Of Mentions:"
    clean_summary = (summary or "").strip()
    return (
        f"{header}\n\n"
        f"{subheader}\n\n"
        f"{current_price_line}\n"
        f"{daily_change_line}\n"
        f"{wk52_line}\n\n"
        f"{summary_header}\n"
        f"{clean_summary}"
    )

def build_tweet_text(ticker: str,
                     company_name: str,
                     summary: str,
                     current_price: float | None,
                     pct_change_1d: float | None,
                     pct_change_7d: float | None,
                     year_high: float | None,
                     year_low: float | None) -> str:
    """Builds a tweet within 280 characters based on the email alert content."""
    header = f"🚨 Mention Spike Alert: ${ticker}"
    subheader = "We monitor Reddit, 4Chan, StockTwits and Twitter for unusual surges in mentions and sentiment using GPT5."
    current_price_line = f"Current Price: {_safe_money_str(current_price)}"
    daily_change_line = f"Daily Change: {_safe_percent_signed(pct_change_1d)}"
    if year_high is not None and year_low is not None:
        wk52_line = f"52 Week H/L: {_safe_money_str(year_high)} - {_safe_money_str(year_low)}"
    else:
        wk52_line = "52 Week H/L: N/A"

    # Base without summary
    base = (
        f"{header}\n\n"
        f"{subheader}\n\n"
        f"{current_price_line}\n"
        f"{daily_change_line}\n"
        f"{wk52_line}\n\n"
        f"AI Summary Of Mentions:\n"
    )

    # compute available chars for summary
    max_len = 280
    available = max_len - len(base)
    clean_summary = summary.replace("\n", " ").strip()

    if available <= 0:
        # try to condense price_line by removing 52w then 7d if needed
        base = (
            f"{header}\n{current_price_line}\n{daily_change_line}\n\nAI Summary Of Mentions:\n"
        )
        available = max_len - len(base)

    # final fallback: ensure at least some space for summary
    if available < 20:
        # minimal format
        base = f"{header}\nAI Summary Of Mentions:\n"
        available = max_len - len(base)

    if available <= 0:
        available = 0

    if len(clean_summary) > available:
        if available <= 1:
            clean_summary = "…"
        else:
            clean_summary = clean_summary[:available - 1].rstrip() + "…"

    tweet = f"{base}{clean_summary}"
    # Safety clamp
    if len(tweet) > 280:
        tweet = tweet[:279] + "…"
    return tweet

def post_tweet(text: str, config: dict) -> bool:
    """Posts a tweet using Twitter API v2 and OAuth 1.0a credentials."""
    try:
        ck = config.get('twitter_consumer_key')
        cs = config.get('twitter_consumer_secret')
        at = config.get('twitter_access_token')
        ats = config.get('twitter_access_secret')
        if not all([ck, cs, at, ats]):
            logging.warning("Twitter posting skipped: missing credentials.")
            return False

        url = "https://api.twitter.com/2/tweets"
        auth = OAuth1(ck, cs, at, ats)
        payload = {"text": text}
        resp = requests.post(url, json=payload, auth=auth, timeout=10)
        if 200 <= resp.status_code < 300:
            logging.info("Tweet posted successfully.")
            return True
        logging.error(f"Failed to post tweet. Status={resp.status_code} Body={resp.text}")
        return False
    except Exception as e:
        logging.error(f"Error while posting tweet: {e}", exc_info=True)
        return False

def process_alerts(conn, signal_rows, config, openai_client):
    """
    Processes signals to generate alerts based on thresholds and cooldowns.
    """
    if not signal_rows:
        return

    MAX_ALERTS_PER_RUN = 5  # Temporary cap to prevent email floods
    alerts_sent_this_run = 0

    asset_ids = [row['asset_id'] for row in signal_rows]
    asset_details = get_asset_details(conn, asset_ids)

    for row in signal_rows:
        try:
            z_sov = float(row['z_sov']) if row['z_sov'] is not None else -1.0
            x_total_2h = int(row['x_total_2h'])
            x_asset_2h = int(row['x_asset_2h'])
            as_of_dt = row['as_of']
            
            # 1. Apply floors
            if not (
                x_total_2h >= config['alert_min_total_2h'] and
                x_asset_2h >= config['alert_min_asset_2h'] and
                row['z_sov'] is not None and z_sov >= config['alert_z_sov_threshold']
            ):
                continue
            
            # 2. Check cooldown/dedupe
            as_of_epoch = int(as_of_dt.timestamp())
            cooldown_bucket = math.floor(as_of_epoch / (config['alert_cooldown_hours'] * 3600))
            dedupe_key = f"{row['asset_id']}|2|{cooldown_bucket}"
            
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM alerts_emitted WHERE dedupe_key = %s;", (dedupe_key,))
                if cur.fetchone():
                    logging.info(f"Skipping alert for asset {row['asset_id']} due to cooldown (key: {dedupe_key}).")
                    continue
            
            # Temporary cap to avoid email floods during testing
            if alerts_sent_this_run >= MAX_ALERTS_PER_RUN:
                logging.warning(f"Max alerts per run ({MAX_ALERTS_PER_RUN}) reached. Skipping remaining alerts for this cycle.")
                break

            # 3. Send email
            asset = asset_details.get(row['asset_id'], {})
            ticker = asset.get('ticker', f"ID:{row['asset_id']}")
            
            # Fetch comments and generate summary
            comments = fetch_comments_for_summary(conn, row['asset_id'], as_of_dt - timedelta(hours=2), as_of_dt)
            summary = generate_summary(openai_client, config['openai_model'], comments, ticker)

            subject = f"Mention Spike Alert: ${ticker}"
            
            asset_url = f"{config['frontend_base_url']}/dashboard/symbol/{ticker}?universe={asset.get('universe', 'stock')}"

            # Price performance (equities)
            pct_change_1d = None
            pct_change_7d = None
            current_price = None
            year_high = None
            year_low = None
            try:
                if asset.get('universe', 'stock') == 'stock' and config.get('tiingo_api_key'):
                    pct_change_1d = compute_pct_change_1d_equity(ticker, as_of_dt if isinstance(as_of_dt, datetime) else datetime.now(timezone.utc), config['tiingo_api_key'])
                    pct_change_7d = compute_pct_change_7d_equity(ticker, as_of_dt if isinstance(as_of_dt, datetime) else datetime.now(timezone.utc), config['tiingo_api_key'])
                    current_price = get_current_price_equity(ticker, as_of_dt if isinstance(as_of_dt, datetime) else datetime.now(timezone.utc), config['tiingo_api_key'])
                    yh, yl = fetch_52wk_high_low_equity(ticker, as_of_dt if isinstance(as_of_dt, datetime) else datetime.now(timezone.utc), config['tiingo_api_key'])
                    year_high, year_low = yh, yl
            except Exception as e:
                logging.warning(f"Price change computation failed for {ticker}: {e}")

            body = f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; border: 1px solid #ddd; border-radius: 8px; padding: 20px;">
                <h2 style="color: #1a1a1a; border-bottom: 2px solid #eee; padding-bottom: 10px;">Mention Spike Alert: ${ticker}</h2>
                <p style="font-size: 16px;">A significant increase in social media mentions has been detected for <strong>{asset.get('name', ticker)} (${ticker})</strong>.</p>
                
                <div style="background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0;">
                    <h4 style="margin-top: 0; color: #333;">ChatGPT Summary of Recent Mentions:</h4>
                    <p style="font-size: 14px; color: #555;"><em>"{summary}"</em></p>
                </div>

                <h4 style="color: #333;">Signal Details (as of {as_of_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC):</h4>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Z-Score</td><td style="padding: 8px; text-align: right;"><strong>{float(row['z_sov']):.2f}</strong> (Threshold: {config['alert_z_sov_threshold']})</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Asset Mentions (2h)</td><td style="padding: 8px; text-align: right;">{x_asset_2h}</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Total Mentions (2h)</td><td style="padding: 8px; text-align: right;">{x_total_2h}</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Share of Voice</td><td style="padding: 8px; text-align: right;">{float(row['sov_now']) * 100:.2f}%</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Baseline Mean SoV</td><td style="padding: 8px; text-align: right;">{float(row['mu_sov']) * 100:.2f}%</td></tr>
                    <tr><td style="padding: 8px;">Baseline Std Dev</td><td style="padding: 8px; text-align: right;">{float(row['sigma_sov']) * 100:.2f}%</td></tr>
                </table>

                <h4 style="color: #333; margin-top: 20px;">Price Performance</h4>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Current Price</td><td style="padding: 8px; text-align: right;">{(f"${current_price:.2f}" if current_price is not None else "N/A")}</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">1d Change</td><td style="padding: 8px; text-align: right;">{(f"{pct_change_1d:.2f}%" if pct_change_1d is not None else "N/A")}</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">7d Change</td><td style="padding: 8px; text-align: right;">{(f"{pct_change_7d:.2f}%" if pct_change_7d is not None else "N/A")}</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">52-Week High</td><td style="padding: 8px; text-align: right;">{(f"${year_high:.2f}" if year_high is not None else "N/A")}</td></tr>
                    <tr><td style="padding: 8px;">52-Week Low</td><td style="padding: 8px; text-align: right;">{(f"${year_low:.2f}" if year_low is not None else "N/A")}</td></tr>
                </table>

                <div style="text-align: center; margin-top: 25px;">
                    <a href="{asset_url}" style="background-color: #007bff; color: white; padding: 12px 25px; text-decoration: none; border-radius: 5px; font-size: 16px;">View Asset Dashboard</a>
                </div>
            </div>
            """

            message = Mail(
                from_email=('alerts@monkeemath.com', 'Project Ape Alerts'), # Matching test-send.js
                to_emails=config['alerts_to_email'],
                subject=subject,
                html_content=body
            )
            
            try:
                sg = SendGridAPIClient(config['sendgrid_api_key'])
                response = sg.send(message)
                logging.info(f"Sent alert email for asset {row['asset_id']}. Status: {response.status_code}")
                alerts_sent_this_run += 1
            except Exception as e:
                logging.error(f"Failed to send email for asset {row['asset_id']}: {e}", exc_info=True)
                continue # Do not record if email failed

            # 3b. Optional: Tweet the alert
            try:
                if config.get('twitter_enabled'):
                    full_tweet = build_tweet_text_full(
                        ticker=ticker,
                        company_name=f"{asset.get('name', ticker)} (${ticker})",
                        summary=summary,
                        current_price=current_price,
                        pct_change_1d=pct_change_1d,
                        pct_change_7d=pct_change_7d,
                        year_high=year_high,
                        year_low=year_low
                    )
                    posted = post_tweet(full_tweet, config)
                    if not posted:
                        # Fallback to truncated variant
                        logging.warning(f"Full-length tweet failed, attempting truncated tweet for {ticker}.")
                        short_tweet = build_tweet_text(
                            ticker=ticker,
                            company_name=f"{asset.get('name', ticker)} (${ticker})",
                            summary=summary,
                            current_price=current_price,
                            pct_change_1d=pct_change_1d,
                            pct_change_7d=pct_change_7d,
                            year_high=year_high,
                            year_low=year_low
                        )
                        post_tweet(short_tweet, config)
            except Exception as e:
                logging.error(f"Failed tweeting alert for {ticker}: {e}", exc_info=True)

            # 4. Record the alert
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alerts_emitted (asset_id, window_hours, as_of, z_reported, dedupe_key)
                    VALUES (%s, %s, %s, %s, %s);
                """, (row['asset_id'], 2, as_of_dt, str(z_sov), dedupe_key))
            
            logging.info(f"Alert for asset {row['asset_id']} recorded in alerts_emitted.")
            
        except (ValueError, TypeError) as e:
            logging.error(f"Error processing signal row for alert: {row}. Error: {e}", exc_info=True)
            continue
    logging.info(f"Finished processing alerts. Total emails sent: {alerts_sent_this_run}")
            
def main():
    """Main function for the signals worker."""
    logging.info("Starting signals worker...")

    # Load configuration from environment variables
    try:
        config = {
            "database_url": get_env_var("POSTGRES_URL"),
            "sendgrid_api_key": get_env_var("SENDGRID_API_KEY"),
            "alerts_to_email": get_env_var("ALERTS_TO_EMAIL"),
            "frontend_base_url": get_env_var("FRONTEND_BASE_URL", "https://project-ape-3u1s.vercel.app"),
            "openai_api_key": get_env_var("OPENAI_API_KEY"),
            "openai_model": get_env_var("OPENAI_MODEL", "gpt-4-turbo"),
            "alert_min_total_2h": int(get_env_var("ALERT_MIN_TOTAL_2H", "50")),
            "alert_min_asset_2h": int(get_env_var("ALERT_MIN_ASSET_2H", "5")),
            "alert_z_sov_threshold": float(get_env_var("ALERT_Z_SOV_THRESHOLD", "2.5")),
            "alert_cooldown_hours": int(get_env_var("ALERT_COOLDOWN_HOURS", "6")),
            "baseline_min_windows": int(get_env_var("BASELINE_MIN_WINDOWS", "12")),
            "baseline_lookback_windows": int(get_env_var("BASELINE_LOOKBACK_WINDOWS", "48")),
            "tiingo_api_key": os.getenv("TIINGO_API_KEY"),
            # Twitter
            "twitter_enabled": get_env_var("TWITTER_ENABLED", "false").lower() in ("1", "true", "yes"),
            "twitter_consumer_key": os.getenv("TWITTER_CONSUMER_KEY"),
            "twitter_consumer_secret": os.getenv("TWITTER_CONSUMER_SECRET"),
            "twitter_access_token": os.getenv("TWITTER_ACCESS_TOKEN"),
            "twitter_access_secret": os.getenv("TWITTER_ACCESS_SECRET"),
        }
    except (ValueError, TypeError) as e:
        logging.error(f"Configuration error: {e}")
        return

    openai_client = OpenAI(api_key=config['openai_api_key'])

    # One-time setup: ensure heartbeat table exists
    try:
        with get_db_connection(config['database_url']) as conn:
            setup_heartbeat_table(conn)
    except Exception as e:
        logging.error(f"Failed to set up heartbeat table: {e}", exc_info=True)
        return

    while True:
        now_utc = datetime.now(timezone.utc)
        # Align to the top of the next hour
        next_hour = (now_utc + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        sleep_seconds = (next_hour - now_utc).total_seconds() + random.uniform(-1, 1) # Add jitter
        logging.info(f"Next run at {next_hour.isoformat()}. Sleeping for {sleep_seconds:.2f} seconds.")
        time.sleep(sleep_seconds)
        as_of = next_hour
        logging.info(f"Worker tick. Processing for as_of={as_of.isoformat()}")

        try:
            with get_db_connection(config['database_url']) as conn:
                with advisory_lock(conn, 'sov_signals_2h') as locked:
                    if not locked:
                        continue # Skip cycle if lock not acquired

                    # 1. Signals step
                    logging.info("Running signals step...")
                    signal_rows = calculate_signals(conn, as_of, config['baseline_lookback_windows'])

                    # 2. Alerts step
                    logging.info("Running alerts step...")
                    process_alerts(conn, signal_rows, config, openai_client)

                    # 3. Heartbeat
                    logging.info("Updating heartbeat...")
                    update_heartbeat(conn, "sov_signals_2h", "ok")

                    logging.info("Cycle completed successfully.")

        except Exception as e:
            logging.error(f"Error in worker cycle: {e}", exc_info=True)
            try:
                with get_db_connection(config['database_url']) as conn:
                    update_heartbeat(conn, "sov_signals_2h", "error", str(e))
            except Exception as he:
                logging.error(f"Failed to write error heartbeat: {he}", exc_info=True)

        # continue loop normally

if __name__ == "__main__":
    main()
