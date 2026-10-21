SELECT DATE(block_timestamp) AS day, COUNT(*) AS tx
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE block_timestamp >= TIMESTAMP('__START_DATE__')
  AND block_timestamp <  TIMESTAMP('__END_DATE__')
GROUP BY day
ORDER BY day;
