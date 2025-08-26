import os
import time
import logging
from datetime import datetime, timezone, timedelta
import random
from contextlib import contextmanager

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

def calculate_signals(conn, as_of: datetime, baseline_lookback_windows: int):
    """
    Computes 2-hour Share-of-Voice (SoV) signals.
    """
    window_start = as_of - timedelta(hours=2)
    
    logging.info(f"Calculating signals for window: [{window_start.isoformat()}, {as_of.isoformat()})")

    with conn.cursor() as cur:
        # 1. Get comment counts per asset in the window.
        cur.execute("""
            SELECT asset_id, COUNT(*) AS x_asset_2h
            FROM comments
            WHERE commented_at >= %s AND commented_at < %s
            GROUP BY asset_id;
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
        Please provide a brief, neutral summary of the key themes, overall sentiment, and what might be driving the mention spike.

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
            max_tokens=200,
        )
        summary = response.choices[0].message.content
        return summary.strip() if summary else "Could not generate a summary."
    except Exception as e:
        logging.error(f"Failed to generate summary for ${ticker}: {e}", exc_info=True)
        return "Summary generation failed due to an error."

def process_alerts(conn, signal_rows, config, openai_client):
    """
    Processes signals to generate alerts based on thresholds and cooldowns.
    """
    if not signal_rows:
        return

    MAX_ALERTS_PER_RUN = 5  # Temporary cap for testing
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

if __name__ == "__main__":
    main()
