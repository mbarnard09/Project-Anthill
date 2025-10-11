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
from core.deepseek import deepseek_chat

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_env_var(name, default=None):
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

@contextmanager
def advisory_lock(conn, lock_name):
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
    if _HOLIDAY_PROVIDER is None:
        return False
    try:
        now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
        d = now_et.date()
        return d in _HOLIDAY_PROVIDER
    except Exception:
        return False

def is_us_equity_market_hours(now_utc: datetime) -> bool:
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
            return result
    except Exception as e:
        logging.warning(f"Tiingo intraday snapshot failed for {ticker}: {e}")
    return None

def fetch_tiingo_eod_prices(ticker: str, start_date: str, end_date: str, token: str) -> list[dict]:
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {"startDate": start_date, "endDate": end_date, "token": token}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
    except Exception as e:
        logging.warning(f"Tiingo EOD fetch failed for {ticker}: {e}")
    return []

def compute_pct_change_1d_equity(ticker: str, now_utc: datetime, token: str) -> float | None:
    try:
        if is_us_equity_market_hours(now_utc):
            snap = fetch_tiingo_intraday_snapshot(ticker, token)
            if snap and snap.get("last") is not None and snap.get("prevClose") not in (None, 0):
                last = float(snap["last"])  # most recent trade price
                prev_close = float(snap["prevClose"])  # previous trading day's close
                delta = last - prev_close
                if abs(delta) >= max(0.01, 0.0005 * prev_close):
                    pct = (last - prev_close) / prev_close * 100.0
                    return round(pct, 2)
        end_dt = now_utc.date()
        start_dt = end_dt - timedelta(days=10)
        bars = fetch_tiingo_eod_prices(ticker, start_dt.isoformat(), end_dt.isoformat(), token)
        closes = [b.get("adjClose") for b in bars if b.get("adjClose") is not None]
        if len(closes) >= 2:
            prev, last = closes[-2], closes[-1]
            if prev not in (None, 0):
                pct = (last - prev) / prev * 100.0
                return round(pct, 2)
    except Exception as e:
        logging.warning(f"Failed 1d change calc for {ticker}: {e}")
    return None

def compute_pct_change_7d_equity(ticker: str, now_utc: datetime, token: str) -> float | None:
    try:
        end_dt = now_utc.date()
        start_dt = end_dt - timedelta(days=14)
        target_dt = end_dt - timedelta(days=7)
        bars = fetch_tiingo_eod_prices(ticker, start_dt.isoformat(), end_dt.isoformat(), token)
        if not bars:
            return None
        try:
            bars.sort(key=lambda b: b.get("date", ""))
        except Exception:
            pass
        eod = []
        for b in bars:
            adj = b.get("adjClose")
            d = b.get("date")
            if adj is None or d is None:
                continue
            d_str = d[:10]
            eod.append((d_str, float(adj)))
        if not eod:
            return None
        last_date, last_close = eod[-1]
        target_str = target_dt.isoformat()
        candidate = None
        for d_str, c in reversed(eod):
            if d_str <= target_str:
                candidate = (d_str, c)
                break
        if candidate is None:
            candidate = eod[0]
        prev_date, prev_close = candidate
        if prev_close in (None, 0):
            return None
        pct = (last_close - prev_close) / prev_close * 100.0
        return round(pct, 2)
    except Exception as e:
        logging.warning(f"Failed 7d change calc for {ticker}: {e}")
    return None

def get_current_price_equity(ticker: str, now_utc: datetime, token: str) -> float | None:
    try:
        snap = fetch_tiingo_intraday_snapshot(ticker, token)
        if snap and snap.get("last") is not None:
            price = float(snap["last"])
            return round(price, 2)
        end_dt = now_utc.date()
        start_dt = end_dt - timedelta(days=10)
        bars = fetch_tiingo_eod_prices(ticker, start_dt.isoformat(), end_dt.isoformat(), token)
        closes = [b.get("adjClose") for b in bars if b.get("adjClose") is not None]
        if closes:
            price = float(closes[-1])
            return round(price, 2)
    except Exception as e:
        logging.warning(f"Failed current price for {ticker}: {e}")
    return None

def fetch_tiingo_crypto_price(ticker: str, token: str) -> float | None:
    try:
        pair = f"{ticker.lower()}usd"
        url = "https://api.tiingo.com/tiingo/crypto/prices"
        params = {"tickers": pair, "token": token}
        resp = requests.get(url, params=params, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            price_data = data[0].get('priceData') or []
            if price_data:
                last = price_data[-1].get('last') or price_data[-1].get('close')
                if last is not None:
                    return round(float(last), 6)
    except Exception as e:
        logging.info(f"Tiingo crypto price fetch failed for {ticker}: {e}")
    return None

def fetch_comments_for_summary(conn, asset_id: int, start_time: datetime, end_time: datetime, limit: int = 50):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sentiment, body FROM comments
            WHERE asset_id = %s AND commented_at >= %s AND commented_at < %s AND body IS NOT NULL
            ORDER BY commented_at DESC
            LIMIT %s;
        """, (asset_id, start_time, end_time, limit))
        rows = cur.fetchall()
        return [
            {"sentiment": int(r.get('sentiment') or 0), "body": str(r.get('body') or '')}
            for r in rows if r.get('body')
        ]

def generate_summary(deepseek_api_key: str, comments: list[dict], ticker: str, universe: str, hours: int, mention_count: int) -> str:
    try:
        system_content = "\n".join([
            'You are a sharp, conversational market analyst. You receive recent social comments about one stock or crypto.',
            '',
            'Your job is to capture the FLAVOR of the chatter (short, readable, trader-friendly):',
            '1) Summarize the mood in 2–3 tight paragraphs — buzz, momentum shifts, uncertainty.',
            '2) Surface catalysts: confirmed news, speculative rumors, and repeated themes (bullish + bearish).',
            '3) Highlight 2–4 especially interesting/representative comments (PARAPHRASED).',
            '4) If a comment is vague, infer plausible context to make it useful, but LABEL it "(inferred)".',
            '5) Call out risks/contradictions if they stand out.',
            '',
            'Output in Markdown with EXACT sections and spacing:',
            '',
            '### Summary',
            '[2–3 concise paragraphs, focus on mood and buzz]',
            '',
            '### Notable Themes & Catalysts',
            '- Single-line bullets: news, speculation, repeated points; why traders care',
            '',
            '### Interesting Comments',
            '- **Tagline** — Paraphrase with any helpful inference/context (2–4 items total)',
            '',
            '### Risks & Contradictions',
            '- Short bullets with key uncertainties or disagreements',
            '',
            'Style rules:',
            '- Prefer colorful, concrete paraphrase over generic phrasing.',
            '- Avoid dry stats unless they clarify the chatter.',
            '- Do NOT quote usernames or paste comments verbatim.',
        ])

        import re as _re
        def sanitize(s: str) -> str:
            return _re.sub(r"[\*`_>#]", "", _re.sub(r"\s+", " ", s)).trim() if hasattr(str, 'trim') else _re.sub(r"[\*`_>#]", "", _re.sub(r"\s+", " ", s)).strip()
        def truncate(s: str, n: int) -> str:
            return s[: n - 1] + '…' if len(s) > n else s

        formatted_comments = []
        for c in (comments or [])[:50]:
            try:
                s = int(c.get('sentiment') if isinstance(c, dict) else 0)
            except Exception:
                s = 0
            label = 'pos' if s > 0 else ('neg' if s < 0 else 'neu')
            body = c.get('body') if isinstance(c, dict) else str(c)
            formatted_comments.append(f"- [{label}] {truncate(sanitize(body or ''), 240)}")
        formatted_block = "\n".join(formatted_comments)

        user_content = (
            f"Context (use exactly the formatting below):\n"
            f"- Asset: {ticker}\n"
            f"- Universe: {universe}\n"
            f"- Timeframe: last {hours} hours\n"
            f"- Comments ingested: {mention_count}\n\n"
            + (f"Recent comments (most recent first, do not quote verbatim — paraphrase for clarity):\n{formatted_block}\n\n" if formatted_block else "")
            + "Task: Produce Markdown per the structure. Include 2–4 items in the \"Interesting Comments\" section (paraphrased, no direct quotes). Use clear bullets (\"- \") and include blank lines between sections so it renders correctly. Be specific and avoid filler."
        )

        summary = deepseek_chat(
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content},
            ],
            api_key=deepseek_api_key,
            temperature=0.3,
            max_tokens=1500,
        )
        return (summary or "Could not generate a summary.").strip()
    except Exception as e:
        logging.error(f"Failed to generate summary for ${ticker}: {e}", exc_info=True)
        return "Summary generation failed due to an error."

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
                          year_low: float | None,
                          window_label: str = "24h") -> str:
    header = f"🚨 Mention Spike Alert: ${ticker}"
    subheader = "We monitor Reddit, 4Chan, StockTwits and Twitter for unusual surges in mentions and sentiment using GPT5."
    current_price_line = f"Current Price: {_safe_money_str(current_price)}"
    daily_change_line = f"Daily Change: {_safe_percent_signed(pct_change_1d)}"
    wk52_line = f"52 Week H/L: {_safe_money_str(year_high)} - {_safe_money_str(year_low)}" if year_high is not None and year_low is not None else "52 Week H/L: N/A"
    summary_header = "AI Summary Of Mentions:"
    clean_summary = (summary or "").strip()
    return (
        f"{header}\n\n"
        f"{subheader}\n\n"
        f"Window: {window_label}\n"
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
                     year_low: float | None,
                     window_label: str = "24h") -> str:
    header = f"🚨 Mention Spike Alert: ${ticker}"
    subheader = "We monitor Reddit, 4Chan, StockTwits and Twitter for unusual surges in mentions and sentiment using GPT5."
    current_price_line = f"Current Price: {_safe_money_str(current_price)}"
    daily_change_line = f"Daily Change: {_safe_percent_signed(pct_change_1d)}"
    wk52_line = f"52 Week H/L: {_safe_money_str(year_high)} - {_safe_money_str(year_low)}" if year_high is not None and year_low is not None else "52 Week H/L: N/A"
    base = (
        f"{header}\n\n"
        f"{subheader}\n\n"
        f"Window: {window_label}\n"
        f"{current_price_line}\n"
        f"{daily_change_line}\n"
        f"{wk52_line}\n\n"
        f"AI Summary Of Mentions:\n"
    )
    max_len = 280
    available = max_len - len(base)
    clean_summary = summary.replace("\n", " ").strip()
    if available <= 0:
        base = f"{header}\nAI Summary Of Mentions:\n"
        available = max_len - len(base)
    if available <= 0:
        available = 0
    if len(clean_summary) > available:
        clean_summary = clean_summary[:max(0, available - 1)].rstrip() + ("…" if available > 0 else "")
    tweet = f"{base}{clean_summary}"
    if len(tweet) > 280:
        tweet = tweet[:279] + "…"
    return tweet

def calculate_signals(conn, as_of: datetime, baseline_lookback_windows: int, window_hours: int, universe: str):
    window_start = as_of - timedelta(hours=window_hours)
    logging.info(f"Calculating signals for window: [{window_start.isoformat()}, {as_of.isoformat()})")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.asset_id, COUNT(*) AS x_asset_2h
            FROM comments c
            JOIN assets a ON a.id = c.asset_id
            WHERE a.universe = %s AND c.commented_at >= %s AND c.commented_at < %s
            GROUP BY c.asset_id;
        """, (universe, window_start, as_of))
        asset_counts = cur.fetchall()
        if not asset_counts:
            logging.info("No comments found in the current window.")
            return []
        x_total_2h = sum(row['x_asset_2h'] for row in asset_counts)
        if x_total_2h == 0:
            logging.info("Total mentions are zero for the current window.")
            return []
        signals_to_insert = []
        asset_ids_with_activity = [row['asset_id'] for row in asset_counts]
        cur.execute("""
            SELECT asset_id, sov_now FROM asset_signal_windows
            WHERE asset_id = ANY(%s) AND window_hours = %s AND as_of < %s
            ORDER BY as_of DESC
            LIMIT %s;
        """, (asset_ids_with_activity, window_hours, as_of, baseline_lookback_windows * len(asset_ids_with_activity)))
        history = {}
        for row in cur.fetchall():
            if row['asset_id'] not in history:
                history[row['asset_id']] = []
            history[row['asset_id']].append(float(row['sov_now']))
        for row in asset_counts:
            asset_id = row['asset_id']
            x_asset_2h = row['x_asset_2h']
            sov_now = x_asset_2h / x_total_2h if x_total_2h > 0 else 0
            mu_sov, sigma_sov, z_sov = 0.0, 0.0, None
            asset_history = history.get(asset_id, [])
            if len(asset_history) >= 1:
                mu_sov = np.mean(asset_history)
                sigma_sov = np.std(asset_history)
                if sigma_sov > 0:
                    z_sov = (sov_now - mu_sov) / sigma_sov
            signals_to_insert.append({
                'asset_id': asset_id,
                'as_of': as_of,
                'window_hours': window_hours,
                'x_asset_2h': x_asset_2h,
                'x_total_2h': x_total_2h,
                'sov_now': str(sov_now),
                'mu_sov': str(mu_sov),
                'sigma_sov': str(sigma_sov),
                'z_sov': str(z_sov) if z_sov is not None else None
            })
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
            with conn.cursor() as cur2:
                cur2.executemany(upsert_sql, signals_to_insert)
        return signals_to_insert

def get_asset_details(conn, asset_ids):
    if not asset_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, ticker, name, universe FROM assets WHERE id = ANY(%s);", (asset_ids,))
        return {row['id']: {'ticker': row['ticker'], 'name': row['name'], 'universe': row['universe']} for row in cur.fetchall()}

def fetch_alert_recipient_emails(conn, universe: str) -> list[str]:
    if universe not in ("stock", "crypto"):
        return []
    column = 'enable_stock_surge' if universe == 'stock' else 'enable_crypto_surge'
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT u.email
            FROM user_alert_prefs p
            JOIN users u ON u.id = p.user_id
            WHERE u.email IS NOT NULL
              AND u.deleted_at IS NULL
              AND p.{column} = TRUE
            """
        )
        rows = cur.fetchall()
        emails = [r['email'] for r in rows if r.get('email')]
        return sorted(list({e.strip().lower() for e in emails if isinstance(e, str)}))

def process_alerts(conn, signal_rows, config, deepseek_api_key: str, window_hours: int, universe: str):
    if not signal_rows:
        return
    MAX_ALERTS_PER_RUN = 5
    alerts_sent_this_run = 0
    asset_ids = [row['asset_id'] for row in signal_rows]
    asset_details = get_asset_details(conn, asset_ids)
    for row in signal_rows:
        try:
            z_sov = float(row['z_sov']) if row['z_sov'] is not None else -1.0
            x_total_2h = int(row['x_total_2h'])
            x_asset_2h = int(row['x_asset_2h'])
            as_of_dt = row['as_of']
            min_total = int(os.getenv('ALERT_MIN_TOTAL_24H') or config.get('alert_min_total_2h') or 50)
            min_asset = int(os.getenv('ALERT_MIN_ASSET_24H') or config.get('alert_min_asset_2h') or 125)
            if not (
                x_total_2h >= min_total and
                x_asset_2h >= min_asset and
                row['z_sov'] is not None and z_sov >= config['alert_z_sov_threshold']
            ):
                continue
            as_of_epoch = int(as_of_dt.timestamp())
            cooldown_bucket = math.floor(as_of_epoch / (config['alert_cooldown_hours'] * 3600))
            dedupe_key = f"{row['asset_id']}|{window_hours}|{cooldown_bucket}"
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM alerts_emitted WHERE dedupe_key = %s;", (dedupe_key,))
                if cur.fetchone():
                    logging.info(f"Skipping alert for asset {row['asset_id']} due to cooldown (key: {dedupe_key}).")
                    continue
            if alerts_sent_this_run >= MAX_ALERTS_PER_RUN:
                logging.warning(f"Max alerts per run ({MAX_ALERTS_PER_RUN}) reached. Skipping remaining alerts for this cycle.")
                break
            asset = asset_details.get(row['asset_id'], {})
            ticker = asset.get('ticker', f"ID:{row['asset_id']}")
            comments = fetch_comments_for_summary(conn, row['asset_id'], as_of_dt - timedelta(hours=window_hours), as_of_dt)
            summary = generate_summary(deepseek_api_key, comments, ticker, asset.get('universe', 'stock'), window_hours, x_asset_2h)
            subject = f"Mention Spike Alert ({window_hours}h): ${ticker}"
            asset_url = f"{config['frontend_base_url']}/dashboard/symbol/{ticker}?universe={asset.get('universe', 'stock')}"
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
                    yh, yl = None, None
                    year_high, year_low = yh, yl
                elif asset.get('universe') == 'crypto' and config.get('tiingo_api_key'):
                    current_price = fetch_tiingo_crypto_price(ticker, config['tiingo_api_key'])
            except Exception as e:
                logging.warning(f"Price change computation failed for {ticker}: {e}")
            body = f"""
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; border: 1px solid #ddd; border-radius: 8px; padding: 20px;">
                <h2 style="color: #1a1a1a; border-bottom: 2px solid #eee; padding-bottom: 10px;">Mention Spike Alert ({window_hours}h): ${ticker}</h2>
                <p style="font-size: 16px;">A significant increase in social media mentions has been detected for <strong>{asset.get('name', ticker)} (${ticker})</strong>.</p>
                
                <div style="background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0;">
                    <h4 style="margin-top: 0; color: #333;">DeepSeek Summary of Recent Mentions:</h4>
                    <p style="font-size: 14px; color: #555;"><em>"{summary}"</em></p>
                </div>

                <h4 style="color: #333;">Signal Details ({window_hours}h window as of {as_of_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC):</h4>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Z-Score</td><td style="padding: 8px; text-align: right;"><strong>{float(row['z_sov']):.2f}</strong> (Threshold: {config['alert_z_sov_threshold']})</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Asset Mentions ({window_hours}h)</td><td style="padding: 8px; text-align: right;">{x_asset_2h}</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Total Mentions ({window_hours}h)</td><td style="padding: 8px; text-align: right;">{x_total_2h}</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Share of Voice</td><td style="padding: 8px; text-align: right;">{float(row['sov_now']) * 100:.2f}%</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Baseline Mean SoV</td><td style="padding: 8px; text-align: right;">{float(row['mu_sov']) * 100:.2f}%</td></tr>
                    <tr><td style="padding: 8px;">Baseline Std Dev</td><td style="padding: 8px; text-align: right;">{float(row['sigma_sov']) * 100:.2f}%</td></tr>
                </table>

                <h4 style="color: #333; margin-top: 20px;">Price Performance</h4>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">Current Price</td><td style="padding: 8px; text-align: right;">{(_safe_money_str(current_price))}</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px;">1d Change</td><td style="padding: 8px; text-align: right;">{(_safe_percent_signed(pct_change_1d))}</td></tr>
                </table>

                <div style="text-align: center; margin-top: 25px;">
                    <a href="{asset_url}" style="background-color: #007bff; color: white; padding: 12px 25px; text-decoration: none; border-radius: 5px; font-size: 16px;">View Asset Dashboard</a>
                </div>
            </div>
            """
            try:
                recipients = fetch_alert_recipient_emails(conn, universe)
            except Exception as e:
                logging.error(f"Failed recipient lookup: {e}", exc_info=True)
                recipients = []
            if not recipients:
                logging.info(f"No subscribers for alert: universe={asset.get('universe')} ticker={ticker}. Skipping send.")
                continue
            message = Mail(
                from_email=('alerts@monkeemath.com', 'Monkeemath Alerts'),
                to_emails=recipients,
                subject=subject,
                html_content=body,
                is_multiple=True,
            )
            try:
                sg = SendGridAPIClient(config['sendgrid_api_key'])
                response = sg.send(message)
                logging.info(f"Sent alert email for asset {row['asset_id']}. Status: {response.status_code}")
                alerts_sent_this_run += 1
            except Exception as e:
                logging.error(f"Failed to send email for asset {row['asset_id']}: {e}", exc_info=True)
                continue
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
                        year_low=year_low,
                        window_label=f"{window_hours}h"
                    )
                    posted = post_tweet(full_tweet, config)
                    if not posted:
                        short_tweet = build_tweet_text(
                            ticker=ticker,
                            company_name=f"{asset.get('name', ticker)} (${ticker})",
                            summary=summary,
                            current_price=current_price,
                            pct_change_1d=pct_change_1d,
                            pct_change_7d=pct_change_7d,
                            year_high=year_high,
                            year_low=year_low,
                            window_label=f"{window_hours}h"
                        )
                        post_tweet(short_tweet, config)
            except Exception as e:
                logging.error(f"Failed tweeting alert for {ticker}: {e}", exc_info=True)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alerts_emitted (asset_id, window_hours, as_of, z_reported, dedupe_key)
                    VALUES (%s, %s, %s, %s, %s);
                """, (row['asset_id'], window_hours, as_of_dt, str(z_sov), dedupe_key))
        except (ValueError, TypeError) as e:
            logging.error(f"Error processing signal row for alert: {row}. Error: {e}", exc_info=True)
            continue
    logging.info(f"Finished processing alerts. Total emails sent: {alerts_sent_this_run}")

def post_tweet(text: str, config: dict) -> bool:
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

def main():
    logging.info("Starting signals worker 24h...")
    try:
        config = {
            "database_url": get_env_var("POSTGRES_URL"),
            "sendgrid_api_key": get_env_var("SENDGRID_API_KEY"),
            "frontend_base_url": get_env_var("FRONTEND_BASE_URL", "https://project-ape-3u1s.vercel.app"),
            "deepseek_api_key": get_env_var("DEEPSEEK_API_KEY"),
            "alert_min_total_2h": int(get_env_var("ALERT_MIN_TOTAL_2H", "50")),
            "alert_min_asset_2h": int(get_env_var("ALERT_MIN_ASSET_2H", "125")),
            "alert_z_sov_threshold": float(get_env_var("ALERT_Z_SOV_THRESHOLD", "2.5")),
            "alert_cooldown_hours": int(get_env_var("ALERT_COOLDOWN_HOURS", "6")),
            "baseline_min_windows": int(get_env_var("BASELINE_MIN_WINDOWS", "12")),
            "baseline_lookback_windows": int(get_env_var("BASELINE_LOOKBACK_WINDOWS", "48")),
            "tiingo_api_key": os.getenv("TIINGO_API_KEY"),
            "twitter_enabled": get_env_var("TWITTER_ENABLED", "false").lower() in ("1", "true", "yes"),
            "twitter_consumer_key": os.getenv("TWITTER_CONSUMER_KEY"),
            "twitter_consumer_secret": os.getenv("TWITTER_CONSUMER_SECRET"),
            "twitter_access_token": os.getenv("TWITTER_ACCESS_TOKEN"),
            "twitter_access_secret": os.getenv("TWITTER_ACCESS_SECRET"),
        }
    except (ValueError, TypeError) as e:
        logging.error(f"Configuration error: {e}")
        return

    deepseek_api_key = config['deepseek_api_key']

    try:
        with get_db_connection(config['database_url']) as conn:
            setup_heartbeat_table(conn)
    except Exception as e:
        logging.error(f"Failed to set up heartbeat table: {e}", exc_info=True)
        return

    # Universe selection (required)
    universe = os.getenv("UNIVERSE", "stock").strip().lower()
    if universe not in ("stock", "crypto"):
        logging.error("UNIVERSE must be 'stock' or 'crypto'")
        return

    # Backfill is intentionally excluded here. Use the one-off script in services/backfill_24h/.

    while True:
        now_utc = datetime.now(timezone.utc)
        next_hour = (now_utc + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        # run only on even hours (2h cadence)
        if next_hour.hour % 2 != 0:
            # sleep to next even hour
            next_hour = next_hour + timedelta(hours=1)
        sleep_seconds = (next_hour - now_utc).total_seconds() + random.uniform(-1, 1)
        logging.info(f"Next run at {next_hour.isoformat()} (24h). Sleeping for {sleep_seconds:.2f} seconds.")
        time.sleep(sleep_seconds)
        as_of = next_hour
        logging.info(f"Worker tick 24h. Processing for as_of={as_of.isoformat()}")
        try:
            with get_db_connection(config['database_url']) as conn:
                with advisory_lock(conn, f'sov_signals_24h_{universe}') as locked:
                    if not locked:
                        continue
                    logging.info("Running signals step (24h)...")
                    signal_rows = calculate_signals(conn, as_of, config['baseline_lookback_windows'], 24, universe)
                    logging.info("Running alerts step (24h)...")
                    process_alerts(conn, signal_rows, config, deepseek_api_key, 24, universe)
                    logging.info("Updating heartbeat 24h...")
                    update_heartbeat(conn, f"sov_signals_24h_{universe}", "ok")
                    logging.info("Cycle completed successfully (24h).")
        except Exception as e:
            logging.error(f"Error in worker cycle 24h: {e}", exc_info=True)
            try:
                with get_db_connection(config['database_url']) as conn:
                    update_heartbeat(conn, f"sov_signals_24h_{universe}", "error", str(e))
            except Exception as he:
                logging.error(f"Failed to write error heartbeat: {he}", exc_info=True)

if __name__ == "__main__":
    main()


