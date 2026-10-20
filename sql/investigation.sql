-- Example analyst/agent discovery queries after loading Parquet into Iceberg.
-- Replace 'financial_demo' with target database. Do NOT expose ground truth.
SELECT CAST(transaction_ts AS DATE) AS txn_day,
       COUNT(*) AS attempts, SUM(CASE WHEN approved THEN 1 ELSE 0 END) AS approved_count,
       AVG(CASE WHEN approved THEN 1.0 ELSE 0.0 END) AS approval_rate
FROM financial_demo.payment_transactions
GROUP BY CAST(transaction_ts AS DATE)
ORDER BY txn_day;

SELECT processor_id, card_network, channel, merchant_category_code, cross_border,
       COUNT(*) AS attempts,
       AVG(CASE WHEN approved THEN 1.0 ELSE 0.0 END) AS approval_rate
FROM financial_demo.payment_transactions
WHERE transaction_ts >= '2026-10-09'
GROUP BY processor_id, card_network, channel, merchant_category_code, cross_border
HAVING COUNT(*) >= 100
ORDER BY approval_rate ASC
LIMIT 50;
