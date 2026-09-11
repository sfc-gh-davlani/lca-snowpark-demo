"""
LCA Rule Rate Alert — Snowpark Orchestration Demo
==================================================
Simulates the LCA daily rule-rate alert workflow:
  1. Freshness check: MAX(submission_date) from source table
  2. Data freshness guard: verify data is current
  3. N=2..8 day loop: compute aggregated alert metrics per Report_Date
  4. Write results to ALERT_RESULTS table

This script runs inside Snowflake via EXECUTE IMMEDIATE FROM a Git Repository stage.
The Snowpark session is provided by the runtime — no credentials needed.
"""
import datetime


def main(session):
    schema = "DEMO_DB.LCA_DEMO"

    # ── Step 1: Freshness Query ──────────────────────────────────
    max_date_row = session.sql(
        f"SELECT MAX(submission_date) AS max_sub_date FROM {schema}.LCA_TRANSACTIONS_ALL"
    ).collect()

    if not max_date_row or max_date_row[0]["MAX_SUB_DATE"] is None:
        raise ValueError("Freshness query returned no data — source table may be empty.")

    max_sub_date = max_date_row[0]["MAX_SUB_DATE"]
    if hasattr(max_sub_date, "date"):
        max_sub_date = max_sub_date.date()

    today = datetime.date.today()

    # lst_dt_t: if max_sub_date == today, use yesterday; otherwise use max_sub_date
    lst_dt_t = max_sub_date - datetime.timedelta(days=1) if max_sub_date == today else max_sub_date

    # ── Step 2: Data Freshness Guard ─────────────────────────────
    # Verify data is within expected range (not more than 10 days stale)
    staleness = (today - lst_dt_t).days
    if staleness > 10:
        raise ValueError(
            f"Data is {staleness} days stale (lst_dt_t={lst_dt_t}). "
            "Aborting — source data may not be loading."
        )

    # ── Step 3: N=2..8 Loop ──────────────────────────────────────
    results_inserted = 0

    for n in range(2, 9):
        report_date = today - datetime.timedelta(days=n)

        # Data freshness guard for this N value
        expected_lst = today - datetime.timedelta(days=n)
        if lst_dt_t < expected_lst:
            continue  # skip — data not fresh enough for this N

        # Alert query: aggregate rule rates for this report_date
        alert_df = session.sql(f"""
            SELECT
                submission_date AS report_date,
                rule_name,
                AVG(rule_rate) AS avg_rule_rate,
                SUM(loan_count) AS total_loans
            FROM {schema}.LCA_TRANSACTIONS_ALL
            WHERE submission_date = '{report_date}'
            GROUP BY submission_date, rule_name
        """)

        rows = alert_df.collect()
        if not rows:
            continue  # no data for this date

        # Insert results into output table
        for row in rows:
            session.sql(f"""
                INSERT INTO {schema}.ALERT_RESULTS
                    (report_date, n_value, rule_name, avg_rule_rate, total_loans)
                VALUES
                    ('{row['REPORT_DATE']}', {n}, '{row['RULE_NAME']}',
                     {row['AVG_RULE_RATE']}, {row['TOTAL_LOANS']})
            """).collect()
            results_inserted += 1

    return f"LCA Demo completed: lst_dt_t={lst_dt_t}, {results_inserted} rows inserted into ALERT_RESULTS"


# ── Entry point for EXECUTE IMMEDIATE ────────────────────────────
# When Snowflake runs this via EXECUTE IMMEDIATE, it calls main()
# with the active session. The return value is printed to the task log.
from snowflake.snowpark.context import get_active_session
session = get_active_session()
result = main(session)
print(result)
