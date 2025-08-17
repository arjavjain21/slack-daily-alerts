import os
import psycopg

# Supabase Postgres DSN comes from GitHub Actions secret
DSN = os.getenv("SUPABASE_DB_URL")
if not DSN:
    raise SystemExit("Missing SUPABASE_DB_URL env variable")

SQL = """
SELECT
  client_name,
  SUM(total_email_sent)       AS sent,
  SUM(new_leads_reached)      AS leads,
  SUM(replies_count)          AS replies,
  SUM(positive_reply)         AS positives,
  SUM(bounce_count)           AS bounces,
  CASE WHEN SUM(total_email_sent)=0 THEN 0
       ELSE SUM(positive_reply)::float / SUM(total_email_sent)
  END AS reply_rate,
  CASE WHEN SUM(total_email_sent)=0 THEN 0
       ELSE SUM(bounce_count)::float / SUM(total_email_sent)
  END AS bounce_rate
FROM public.campaign_reporting
WHERE start_date::date = CURRENT_DATE - INTERVAL '1 day'
GROUP BY client_name
HAVING SUM(total_email_sent) > 0
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

    # Totals for a quick gut check
    idx = {name: i for i, name in enumerate(cols)}
    total_sent = sum(r[idx["sent"]] for r in rows)
    total_leads = sum(r[idx["leads"]] for r in rows)
    total_replies = sum(r[idx["replies"]] for r in rows)
    total_pos = sum(r[idx["positives"]] for r in rows)
    total_bounces = sum(r[idx["bounces"]] for r in rows)

    print("Totals -> sent:", total_sent, "leads:", total_leads, "replies:", total_replies, "positives:", total_pos, "bounces:", total_bounces)

    # Show first 10 rows as dict for readability
    for r in rows[:10]:
        print(dict(zip(cols, r)))

if __name__ == "__main__":
    main()
