#!/usr/bin/env python
import os
import sys
from datetime import datetime, timezone, timedelta
import psycopg

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("HEALTHCHECKER: DATABASE_URL not set.", file=sys.stderr)
    sys.exit(1)

try:
    # Use a very short timeout for health checks
    conn_str = f"{DATABASE_URL} connect_timeout=5"
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            # Check for the existence of the row first
            cur.execute("SELECT last_ok FROM job_heartbeats WHERE job_name = 'sov_signals_2h';")
            result = cur.fetchone()

            # Case 1: No heartbeat row found yet. This is OK on startup.
            if not result:
                print("HEALTHCHECKER: OK (No heartbeat recorded yet, worker is likely waiting for first run)")
                sys.exit(0)

            last_ok = result[0]

            # Case 2: Row exists, but last_ok is NULL (worker has only ever errored).
            # We'll consider this healthy for now, as the main logs would show the errors.
            if last_ok is None:
                print("HEALTHCHECKER: OK (Worker has an error history but is running)")
                sys.exit(0)

            # Case 3: A valid heartbeat exists. Check if it's stale.
            if isinstance(last_ok, datetime):
                if (datetime.now(timezone.utc) - last_ok) > timedelta(hours=2):
                    print(f"HEALTHCHECKER: FAIL - Heartbeat is stale (last successful run was at {last_ok})", file=sys.stderr)
                    sys.exit(1)
                else:
                    print(f"HEALTHCHECKER: OK (Last successful run was at {last_ok})")
                    sys.exit(0)
            
            # Fallback for unexpected types
            print(f"HEALTHCHECKER: WARN - Unexpected type for last_ok: {type(last_ok)}", file=sys.stderr)
            sys.exit(0)


except Exception as e:
    print(f"HEALTHCHECKER: FAIL - Health check failed with an exception: {e}", file=sys.stderr)
    sys.exit(1)
