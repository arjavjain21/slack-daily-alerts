import os
import psycopg
from decimal import Decimal

DSN = os.getenv("SUPABASE_DB_URL")
if not DSN:
    raise SystemExit("Missing SUPABASE_DB_URL env variable")

# We define "yesterday" relative to IST, not UTC
# start_date is a DATE column, so we derive the IST date explicitly
SQL = """
WITH ist_yesterday AS (
  SELECT ((now() AT TIME ZONE 'Asia/Kolkata')::date - 1) AS d
),
base AS (
  SELECT
    cr.client_name,
    cr.total_sent,
    cr.new_leads_reached,
    cr.replies_count,
    cr.positive_reply,
    cr.bounce_count
  FROM public.campaign_reporting cr
  JOIN ist_yesterday y ON cr.start_date = y.d
)
SELECT
  client_name,
  COALESCE(SUM(total_sent), 0)               AS sent,
  COALESCE(SUM(new_leads_reached), 0)        AS leads,
  COALESCE(SUM(replies_count), 0)            AS replies,
  COALESCE(SUM(positive_reply), 0)           AS positives,
  COALESCE(SUM(bounce_count), 0)             AS bounces,
  CASE WHEN COALESCE(SUM(total_sent), 0) = 0 THEN 0
       ELSE ROUND(SUM(positive_reply)::numeric / NULLIF(SUM(total_sent),0), 4)
  END                                         AS reply_rate,
  CASE WHEN COALESCE(SUM(total_sent), 0) = 0 THEN 0
       ELSE ROUND(SUM(bounce_count)::numeric / NULLIF(SUM(total_sent),0), 4)
  END                                         AS bounce_rate
FROM base
GROUP BY client_name
ORDER BY client_name;
"""

def main():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]

    print("Columns:", cols)
    print(f"Row count: {len(rows)}")

    idx = {name: i for i, name in enumerate(cols)}

    total_sent = sum(r[idx["sent"]] for r in rows)
    total_leads = sum(r[idx["leads"]] for r in rows)
    total_replies = sum(r[idx["replies"]] for r in rows)
    total_pos = sum(r[idx["positives"]] for r in rows)
    total_bounces = sum(r[idx["bounces"]] for r in rows)

    print("Totals -> sent:", total_sent, "leads:", total_leads, "replies:", total_replies, "positives:", total_pos, "bounces:", total_bounces)

    # Show first 10 rows
    for r in rows[:10]:
        print(dict(zip(cols, r)))

if __name__ == "__main__":
    main()
