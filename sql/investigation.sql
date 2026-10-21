-- Read-only queries. Replace RUN_ID with the selected generation_run_id.
SELECT CAST(transaction_ts AS DATE) AS txn_day,
       COUNT(*) AS attempts, SUM(CASE WHEN approved THEN 1 ELSE 0 END) AS approved_count,
       AVG(CASE WHEN approved THEN 1.0 ELSE 0.0 END) AS approval_rate
FROM meridian_financial.payment_transactions
WHERE generation_run_id = 'RUN_ID'
GROUP BY CAST(transaction_ts AS DATE)
ORDER BY txn_day;

SELECT t.processor_id, t.card_network, t.channel, m.merchant_category_code, t.cross_border,
       COUNT(*) AS attempts,
       AVG(CASE WHEN t.approved THEN 1.0 ELSE 0.0 END) AS approval_rate
FROM meridian_financial.payment_transactions t
JOIN meridian_financial.merchants m
  ON t.merchant_id = m.merchant_id AND t.generation_run_id = m.generation_run_id
WHERE t.generation_run_id = 'RUN_ID' AND t.transaction_ts >= '2026-10-09'
GROUP BY t.processor_id, t.card_network, t.channel, m.merchant_category_code, t.cross_border
HAVING COUNT(*) >= 100
ORDER BY approval_rate ASC
LIMIT 50;
