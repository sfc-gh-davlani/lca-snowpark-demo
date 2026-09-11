-- Freshness Query: returns the most recent submission_date from the source table
-- Used to determine lst_dt_t (the latest date with complete data)
SELECT MAX(submission_date) AS max_sub_date
FROM ${schema}.LCA_TRANSACTIONS_ALL;
