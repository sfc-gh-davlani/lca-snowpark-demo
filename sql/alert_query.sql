-- Alert Query: aggregate rule rates by rule_name for a given report_date
-- Parameters: ${report_date} (YYYY-MM-DD), ${schema} (fully qualified schema)
SELECT
    submission_date AS report_date,
    rule_name,
    AVG(rule_rate)  AS avg_rule_rate,
    SUM(loan_count) AS total_loans
FROM ${schema}.LCA_TRANSACTIONS_ALL
WHERE submission_date = '${report_date}'
GROUP BY submission_date, rule_name
ORDER BY rule_name;
