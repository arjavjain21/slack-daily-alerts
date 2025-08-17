import os
import json
import psycopg
import requests

DSN = os.getenv("SUPABASE_DB_URL")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")  # set this as a secret too

if not (DSN and SLACK_TOKEN and SLACK_CHANNEL):
    raise SystemExit("Missing env vars: SUPABASE_DB_URL / SLACK_BOT_TOKEN / SLACK_CHANNEL_ID")

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
  SUM(total_sent)       AS sent,
  SUM(new_leads_reached) AS leads,
  SUM(replies_count)     AS replies,
  SUM(positive_reply)    AS positives,
  SUM(bounce_count)      AS bounces
FROM base
GROUP BY client_name
ORDER BY client_name;
"""

def fetch_data():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

def build_blocks(rows):
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "📊 Daily Campaign Report (IST Yesterday)"}}
    ]
    for r in rows[:10]:  # send first 10 for test
        txt = f"*{r['client_name']}* — Sent: {r['sent']}, Leads: {r['leads']}, Replies: {r['replies']}, Positives: {r['positives']}, Bounces: {r['bounces']}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": txt}})
    return blocks

def post_to_slack(blocks):
    url = "https://slack.com/api/chat.postMessage"
    payload = {
        "channel": SLACK_CHANNEL,
        "blocks": blocks,
        "text": "Daily report"
    }
    resp = requests.post(url, headers={"Authorization": f"Bearer {SLACK_TOKEN}"}, json=payload)
    print("Slack response:", resp.status_code, resp.text)

def main():
    rows = fetch_data()
    blocks = build_blocks(rows)
    post_to_slack(blocks)

if __name__ == "__main__":
    main()
