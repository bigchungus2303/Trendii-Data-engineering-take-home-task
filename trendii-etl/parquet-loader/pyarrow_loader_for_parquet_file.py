import os
import sys
import glob
import json
import psycopg2
import pyarrow.parquet as pq

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5434")
PGUSER = os.getenv("PGUSER", "trendii")
PGPASSWORD = os.getenv("PGPASSWORD", "trendii")
PGDATABASE = os.getenv("PGDATABASE", "trendii")

DATA_DIR = os.getenv("DATA_DIR", "./data/raw/events")

def loader():
    conn = None
    cur = None
    try:
        # Connect
        try:
            conn = psycopg2.connect(
                host=PGHOST, port=PGPORT, user=PGUSER,
                password=PGPASSWORD, dbname=PGDATABASE
            )
            cur = conn.cursor()
        except psycopg2.Error as e:
            print(f"[FATAL] DB connection failed: {e}", file=sys.stderr)
            return

        # Init DDL
        try:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                DROP TABLE IF EXISTS raw.events_jsonl;
                CREATE TABLE IF NOT EXISTS raw.events_jsonl (line jsonb);
            """)
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            print(f"[FATAL] Failed to prepare raw.events_jsonl: {e}", file=sys.stderr)
            return

        # Files
        files = sorted(glob.glob(os.path.join(DATA_DIR, "*.parquet")))
        print(f"Found {len(files)} parquet files.")
        if not files:
            return

        # Load
        for f in files:
            print(f"Loading {f}...")
            try:
                table = pq.read_table(f)
            except Exception as e:
                print(f"[WARN] Skipping file (read error) {f}: {e}", file=sys.stderr)
                continue

            try:
                batches = table.to_batches()
                total_rows = 0
                for batch in batches:
                    pylist = batch.to_pylist()
                    if not pylist:
                        continue
                    rows = [json.dumps(r, default=str) for r in pylist]
                    # mogrify may raise; protect and rollback per batch
                    try:
                        args_str = ",".join(cur.mogrify("(%s)", (r,)).decode("utf-8") for r in rows)
                        cur.execute(f"INSERT INTO raw.events_jsonl(line) VALUES {args_str}")
                        total_rows += len(rows)
                    except psycopg2.Error as e:
                        conn.rollback()
                        print(f"[ERROR] Batch insert failed for {f}: {e}", file=sys.stderr)
                        # continue with next batch/file
                        continue
                conn.commit()
                print(f"Committed {total_rows} rows from {f}.")
            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Failed processing {f}: {e}", file=sys.stderr)
                # keep going to next file
                continue

    finally:
        # Cleanup
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

if __name__ == "__main__":
    loader()
# To run this script, ensure you have the required packages installed: