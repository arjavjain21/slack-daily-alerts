import os
import psycopg

DSN = os.getenv("SUPABASE_DB_URL")
if not DSN:
    raise SystemExit("Missing SUPABASE_DB_URL env variable")

INFO_SQL = """
select
  current_database() as db,
  current_user as db_user,
  inet_server_addr()::text as server_ip,
  inet_server_port() as server_port,
  version() as pg_version
"""

SCHEMA_SQL = """
select column_name, data_type
from information_schema.columns
where table_schema = 'public' and table_name = 'campaign_reporting'
order by ordinal_position
"""

YDAY_SAMPLE_SQL = """
select *
from public.campaign_reporting
where start_date = current_date - interval '1 day'
limit 1
"""

ANY_SAMPLE_SQL = "select * from public.campaign_reporting limit 1"

def main():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(INFO_SQL)
            info = dict(zip([d.name for d in cur.description], cur.fetchone()))
            print("== connection info ==")
            for k, v in info.items():
                print(f"{k}: {v}")

            cur.execute(SCHEMA_SQL)
            cols = cur.fetchall()
            print("\n== campaign_reporting schema seen by this runner ==")
            for name, dtype in cols:
                print(f"{name} : {dtype}")
            print(f"Total columns: {len(cols)}")

            # Try yesterday
            cur.execute(YDAY_SAMPLE_SQL)
            rows = cur.fetchall()
            if not rows:
                print("\nNo row for yesterday. Showing any 1 row instead.")
                cur.execute(ANY_SAMPLE_SQL)
                rows = cur.fetchall()

            if rows:
                colnames = [d.name for d in cur.description]
                print("\n== sample row ==")
                print(dict(zip(colnames, rows[0])))
            else:
                print("\nTable has zero rows or is unreadable.")

if __name__ == "__main__":
    main()
