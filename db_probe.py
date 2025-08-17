import os
import psycopg

DSN = os.getenv("SUPABASE_DB_URL")
if not DSN:
    raise SystemExit("Missing SUPABASE_DB_URL env variable")

def print_schema(cur):
    cur.execute("""
        select column_name, data_type
        from information_schema.columns
        where table_schema = 'public' and table_name = 'campaign_reporting'
        order by ordinal_position
    """)
    cols = cur.fetchall()
    print("== campaign_reporting schema that THIS runner sees ==")
    for name, dtype in cols:
        print(f"{name} : {dtype}")
    print(f"Total columns: {len(cols)}")

def print_sample(cur):
    # Yesterday sample by start_date if possible, falls back to any row
    cur.execute("""
        select *
        from public.campaign_reporting
        where start_date = current_date - interval '1 day'
        limit 1
    """)
    rows = cur.fetchall()
    if not rows:
        cur.execute("select * from public.campaign_reporting limit 1")
        rows = cur.fetchall()
        print("No row for yesterday. Showing any 1 row instead.")
    if rows:
        colnames = [d.name for d in cur.description]
        print("== sample row ==")
        print(dict(zip(colnames, rows[0])))
    else:
        print("Table has zero rows.")

def main():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            print_schema(cur)
            print_sample(cur)

if __name__ == "__main__":
    main()
