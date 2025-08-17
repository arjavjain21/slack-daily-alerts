import os
import psycopg
import requests
from decimal import Decimal, InvalidOperation

DSN = os.getenv("SUPABASE_DB_URL")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")

# Tunables via env with sane defaults
LOW_LEADS_THRESHOLD = int(os.getenv("LOW_LEADS_THRESHOLD", "250"))
LOW_REPLY_RATE_THRESHOLD = Decimal(os.getenv("LOW_REPLY_RATE_THRESHOLD", "0.01"))  # 1% as 0.01

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
    cr.bounce_count,
    (now() AT TIME ZONE 'Asia/Kolkata')::date - 1 AS ist_yday
  FROM public.campaign_reporting cr
  JOIN ist_yesterday y ON cr.start_date = y.d
)
SELECT
  ist_yday,
  client_name,
  SUM(total_sent)        AS sent,
  SUM(new_leads_reached) AS leads,
  SUM(replies_count)     AS replies,
  SUM(positive_reply)    AS positives,
  SUM(bounce_count)      AS bounces
FROM base
GROUP BY ist_yday, client_name
ORDER BY client_name;
"""

def fetch_rows():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    return cols, [dict(zip(cols, r)) for r in rows]

def d2pct(x: Decimal) -> str:
    # Format Decimal fraction as percent with one decimal place, e.g., 0.0123 -> 1.2%
    try:
        return f"{(x * 100).quantize(Decimal('0.1'))}%"
    except Exception:
        return "0.0%"

def safe_div(num: int, den: int) -> Decimal:
    if not den:
        return Decimal(0)
    return Decimal(num) / Decimal(den)

def build_message(rows):
    if not rows:
        return [
            {"type": "header", "text": {"type": "plain_text", "text": "Daily Campaign Alerts"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "No data for IST yesterday."}}
        ]

    ist_date = rows[0]["ist_yday"]  # date object

    # Global totals
    total_leads = sum(r["leads"] for r in rows)
    total_replies = sum(r["replies"] for r in rows)
    total_positives = sum(r["positives"] for r in rows)

    overall_reply_rate = safe_div(total_replies, total_leads)

    # Lists
    low_leads = []
    low_reply = []
    zero_positive = []

    for r in rows:
        leads = r["leads"]
        replies = r["replies"]
        positives = r["positives"]
        rr = safe_div(replies, leads)

        if leads < LOW_LEADS_THRESHOLD:
            low_leads.append((r["client_name"], leads))

        if rr < LOW_REPLY_RATE_THRESHOLD:
            # Include 0 leads accounts with 0 percent, to mirror your example
            low_reply.append((r["client_name"], replies, leads, rr))

        if positives == 0:
            zero_positive.append((r["client_name"], positives))

    # Sort for readability
    low_leads.sort(key=lambda x: (x[1], x[0]))  # by leads asc
    low_reply.sort(key=lambda x: (x[3], x[0]))  # by reply rate asc
    zero_positive.sort(key=lambda x: x[0])      # by name

    # Build sections
    header = {"type": "header", "text": {"type": "plain_text", "text": f"Daily Campaign Alerts : {ist_date}"}}

    summary_lines = [
        ":bar_chart: Yesterday’s Summary",
        f"• New Leads: {total_leads}",
        f"• Replies: {total_replies}",
        f"• Positive Replies: {total_positives}",
        f"• Overall Reply Rate: {d2pct(overall_reply_rate)}",
    ]
    summary_block = {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(summary_lines)}}

    def bulletify_low_leads(items):
        if not items:
            return "• None"
        return "\n".join([f"• {name}: {leads} Leads" for name, leads in items])

    def bulletify_low_reply(items):
        if not items:
            return "• None"
        out = []
        for name, replies, leads, rr in items:
            pct = d2pct(rr)
            out.append(f"• {name}: {replies} Replies / {leads} Leads ({pct})")
        return "\n".join(out)

    def bulletify_zero_pos(items):
        if not items:
            return "• None"
        return "\n".join([f"• {name}: 0 Positive Replies" for name, _ in items])

    low_leads_block = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": ":chart_with_downwards_trend: Accounts with < "
                 f"{LOW_LEADS_THRESHOLD} New Leads Contacted Yesterday\n" + bulletify_low_leads(low_leads)}
    }

    low_reply_block = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": ":turtle: Accounts with Reply Rate < "
                 f"{d2pct(LOW_REPLY_RATE_THRESHOLD)} for Yesterday\n" + bulletify_low_reply(low_reply)}
    }

    zero_pos_block = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": ":rotating_light: Accounts with 0 Positive Replies from Yesterday\n"
                 + bulletify_zero_pos(zero_positive)}
    }

    return [header, summary_block, low_leads_block, low_reply_block, zero_pos_block]

def post_to_slack(blocks):
    url = "https://slack.com/api/chat.postMessage"
    payload = {"channel": SLACK_CHANNEL, "blocks": blocks, "text": "Daily Campaign Alerts"}
    resp = requests.post(url, headers={"Authorization": f"Bearer {SLACK_TOKEN}"}, json=payload, timeout=30)
    print("Slack response:", resp.status_code, resp.text)
    resp.raise_for_status()

def main():
    cols, rows = fetch_rows()
    blocks = build_message(rows)
    post_to_slack(blocks)

if __name__ == "__main__":
    main()
