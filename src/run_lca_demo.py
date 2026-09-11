"""
LCA Rule Rate Alert — Snowpark Orchestration Demo
"""
import datetime


def main(session) -> str:
    """Handler for Snowflake stored procedure. Called with active session."""
    schema = "DEMO_DB.LCA_DEMO"

    # Step 1: Freshness Query
    max_date_row = session.sql(
        f"SELECT MAX(submission_date) AS max_sub_date FROM {schema}.LCA_TRANSACTIONS_ALL"
    ).collect()

    if not max_date_row or max_date_row[0]["MAX_SUB_DATE"] is None:
        return "ERROR: Freshness query returned no data — source table may be empty."

    max_sub_date = max_date_row[0]["MAX_SUB_DATE"]
    if hasattr(max_sub_date, "date"):
        max_sub_date = max_sub_date.date()

    today = datetime.date.today()
    lst_dt_t = max_sub_date - datetime.timedelta(days=1) if max_sub_date == today else max_sub_date

    # Step 2: Data Freshness Guard
    staleness = (today - lst_dt_t).days
    if staleness > 10:
        return f"ERROR: Data is {staleness} days stale (lst_dt_t={lst_dt_t}). Aborting."

    # Step 3: N=2..8 Loop
    results_inserted = 0

    for n in range(2, 9):
        report_date = today - datetime.timedelta(days=n)
        expected_lst = today - datetime.timedelta(days=n)
        if lst_dt_t < expected_lst:
            continue

        rows = session.sql(f"""
            SELECT
                submission_date AS report_date,
                rule_name,
                AVG(rule_rate) AS avg_rule_rate,
                SUM(loan_count) AS total_loans
            FROM {schema}.LCA_TRANSACTIONS_ALL
            WHERE submission_date = '{report_date}'
            GROUP BY submission_date, rule_name
        """).collect()

        if not rows:
            continue

        for row in rows:
            session.sql(f"""
                INSERT INTO {schema}.ALERT_RESULTS
                    (report_date, n_value, rule_name, avg_rule_rate, total_loans)
                VALUES
                    ('{row['REPORT_DATE']}', {n}, '{row['RULE_NAME']}',
                     {row['AVG_RULE_RATE']}, {row['TOTAL_LOANS']})
            """).collect()
            results_inserted += 1

    return f"SUCCESS: lst_dt_t={lst_dt_t}, {results_inserted} rows inserted into ALERT_RESULTS"
