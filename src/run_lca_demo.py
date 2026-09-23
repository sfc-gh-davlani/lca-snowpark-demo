"""
LCA Rule Rate Alert — Snowpark Orchestration Demo
VERSION: 1.1.0 — Added alert threshold detection and improved logging
"""
import datetime

__version__ = "1.1.0"
ALERT_THRESHOLD = 0.10


def main(session) -> str:
    """Handler for Snowflake stored procedure. Called with active session."""
    schema = "DEMO_DB.LCA_DEMO"

    print(f"[LCA] START version={__version__} threshold={ALERT_THRESHOLD}")

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

    print(f"[LCA] FRESH max_sub_date={max_sub_date} lst_dt_t={lst_dt_t}")

    # Step 2: Data Freshness Guard
    staleness = (today - lst_dt_t).days
    if staleness > 10:
        return f"ERROR: Data is {staleness} days stale (lst_dt_t={lst_dt_t}). Aborting."

    # Step 3: N=2..8 Loop
    results_inserted = 0
    alerts_fired = 0

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
            avg_rate = float(row['AVG_RULE_RATE'])
            alert = avg_rate > ALERT_THRESHOLD
            if alert:
                alerts_fired += 1
                print(f"[LCA] ALERT N={n} rule={row['RULE_NAME']} rate={avg_rate:.4f} > {ALERT_THRESHOLD}")

            session.sql(f"""
                INSERT INTO {schema}.ALERT_RESULTS
                    (report_date, n_value, rule_name, avg_rule_rate, total_loans)
                VALUES
                    ('{row['REPORT_DATE']}', {n}, '{row['RULE_NAME']}',
                     {row['AVG_RULE_RATE']}, {row['TOTAL_LOANS']})
            """).collect()
            results_inserted += 1

        print(f"[LCA] LOOP N={n} report_date={report_date} rules={len(rows)}")

    print(f"[LCA] DONE {results_inserted} rows inserted, {alerts_fired} alerts fired")
    return f"SUCCESS v{__version__}: lst_dt_t={lst_dt_t}, {results_inserted} rows inserted, {alerts_fired} alerts fired"
