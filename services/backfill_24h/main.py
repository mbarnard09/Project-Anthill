import logging
from datetime import datetime, timezone, timedelta
import psycopg
from psycopg.rows import dict_row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================
# Hardcoded configuration (edit)
# ==============================
DATABASE_URL = "postgres://neondb_owner:npg_AIhtB1in3HpD@ep-fancy-dawn-ae2jxh14-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require"
UNIVERSE = "crypto"  # 'stock' or 'crypto'
DAYS = 10
BASELINE_LOOKBACK_WINDOWS = 48

def run_backfill(database_url: str, universe: str, days: int, baseline_lookback_windows: int):
    if universe not in ("stock", "crypto"):
        raise RuntimeError("UNIVERSE must be 'stock' or 'crypto'")
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            now_utc = datetime.now(timezone.utc)
            start = now_utc - timedelta(days=days)
            start = start.replace(minute=0, second=0, microsecond=0)
            if start.hour % 2 != 0:
                start = start + timedelta(hours=1)

            as_of = start
            processed = 0
            while as_of < now_utc:
                window_hours = 24
                window_start = as_of - timedelta(hours=window_hours)
                logging.info(f"Backfill {universe} 24h window [{window_start.isoformat()}, {as_of.isoformat()})")

                # Count per asset in universe
                cur.execute(
                    """
                    SELECT c.asset_id, COUNT(*) AS x_asset
                    FROM comments c
                    JOIN assets a ON a.id = c.asset_id
                    WHERE a.universe = %s AND c.commented_at >= %s AND c.commented_at < %s
                    GROUP BY c.asset_id;
                    """,
                    (universe, window_start, as_of),
                )
                asset_counts = cur.fetchall()
                if not asset_counts:
                    as_of = as_of + timedelta(hours=2)
                    continue
                x_total = sum(int(r['x_asset']) for r in asset_counts)
                if x_total == 0:
                    as_of = as_of + timedelta(hours=2)
                    continue

                asset_ids = [r['asset_id'] for r in asset_counts]
                cur.execute(
                    """
                    SELECT asset_id, sov_now
                    FROM asset_signal_windows
                    WHERE asset_id = ANY(%s) AND window_hours = %s AND as_of < %s
                    ORDER BY as_of DESC
                    LIMIT %s
                    """,
                    (asset_ids, window_hours, as_of, baseline_lookback_windows * max(1, len(asset_ids))),
                )
                history = {}
                for row in cur.fetchall():
                    history.setdefault(row['asset_id'], []).append(float(row['sov_now']))

                rows = []
                for r in asset_counts:
                    asset_id = r['asset_id']
                    x_asset = int(r['x_asset'])
                    sov_now = x_asset / x_total if x_total > 0 else 0.0
                    h = history.get(asset_id, [])
                    if h:
                        import numpy as np
                        mu = float(np.mean(h))
                        sd = float(np.std(h))
                    else:
                        mu = 0.0
                        sd = 0.0
                    z = None
                    if sd > 0:
                        z = (sov_now - mu) / sd
                    rows.append({
                        'asset_id': asset_id,
                        'as_of': as_of,
                        'window_hours': window_hours,
                        'x_asset_2h': x_asset,
                        'x_total_2h': x_total,
                        'sov_now': str(sov_now),
                        'mu_sov': str(mu),
                        'sigma_sov': str(sd),
                        'z_sov': str(z) if z is not None else None,
                    })

                if rows:
                    cur.executemany(
                        """
                        INSERT INTO asset_signal_windows (
                            asset_id, as_of, window_hours, x_asset_2h, x_total_2h,
                            sov_now, mu_sov, sigma_sov, z_sov
                        ) VALUES (
                            %(asset_id)s, %(as_of)s, %(window_hours)s, %(x_asset_2h)s, %(x_total_2h)s,
                            %(sov_now)s, %(mu_sov)s, %(sigma_sov)s, %(z_sov)s
                        )
                        ON CONFLICT (asset_id, as_of, window_hours) DO UPDATE SET
                            x_asset_2h = EXCLUDED.x_asset_2h,
                            x_total_2h = EXCLUDED.x_total_2h,
                            sov_now = EXCLUDED.sov_now,
                            mu_sov = EXCLUDED.mu_sov,
                            sigma_sov = EXCLUDED.sigma_sov,
                            z_sov = EXCLUDED.z_sov
                        """,
                        rows,
                    )

                conn.commit()
                processed += 1
                as_of = as_of + timedelta(hours=2)

            logging.info(f"Backfill complete for {universe}. Windows processed: {processed}")

def main():
    run_backfill(DATABASE_URL, UNIVERSE.lower(), DAYS, BASELINE_LOOKBACK_WINDOWS)

if __name__ == '__main__':
    main()


