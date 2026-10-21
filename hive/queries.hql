USE etherlens;

-- Q1 daily activity
SELECT dt, COUNT(*) AS tx_count, COUNT(DISTINCT from_address) AS active_senders,
       ROUND(SUM(value_eth), 2) AS eth_moved
FROM silver_tx GROUP BY dt ORDER BY dt;

-- Q2 top 20 senders
SELECT from_address, COUNT(*) AS n
FROM silver_tx GROUP BY from_address ORDER BY n DESC LIMIT 20;

-- Q3 median gas price by hour of day (UTC)
SELECT hour, percentile_approx(gas_price_gwei, 0.5) AS median_gwei
FROM silver_tx GROUP BY hour ORDER BY hour;

-- Q4 share of plain 21,000-gas transfers per day
SELECT dt, AVG(CASE WHEN is_plain_transfer THEN 1.0 ELSE 0.0 END) AS plain_share
FROM silver_tx GROUP BY dt ORDER BY dt;

-- Q5 busiest hour per day
SELECT dt, hour, n FROM (
  SELECT dt, hour, n, ROW_NUMBER() OVER (PARTITION BY dt ORDER BY n DESC) AS rk
  FROM (SELECT dt, hour, COUNT(*) AS n FROM silver_tx GROUP BY dt, hour) c
) t WHERE rk = 1 ORDER BY dt;
