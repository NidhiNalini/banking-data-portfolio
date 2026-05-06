-- Query 1: Segment summary
SELECT segment, COUNT(*) AS customers,
       ROUND(AVG(rfm_total),2) AS avg_rfm,
       ROUND(AVG(monetary_avg),2) AS avg_spend
FROM rfm_scores GROUP BY segment ORDER BY avg_rfm DESC;

-- Query 2: High-value customers with full profile
SELECT r.account_id, c.full_name, c.province, c.account_type,
       r.recency_days, r.frequency, ROUND(r.monetary_avg,2) AS avg_spend, r.rfm_total
FROM rfm_scores r JOIN customers c ON r.account_id = c.account_id
WHERE r.segment = 'HIGH_VALUE' ORDER BY r.rfm_total DESC;

-- Query 3: At-risk customers still recently active (priority for re-engagement)
SELECT r.account_id, c.full_name, c.province, r.recency_days, r.frequency, r.rfm_total
FROM rfm_scores r JOIN customers c ON r.account_id = c.account_id
WHERE r.segment = 'AT_RISK' AND r.recency_days <= 60
ORDER BY r.recency_days ASC;

-- Query 4: Geographic breakdown of segments
SELECT c.province, r.segment, COUNT(*) AS count
FROM rfm_scores r JOIN customers c ON r.account_id = c.account_id
GROUP BY c.province, r.segment ORDER BY c.province, count DESC;
