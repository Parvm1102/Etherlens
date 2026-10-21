EXPORT DATA OPTIONS (
  uri = 'gs://__BUCKET__/bronze/tx/*.parquet',
  format = 'PARQUET',
  overwrite = true
) AS
SELECT
  `hash`, block_number, block_timestamp, from_address, to_address,
  CAST(value AS STRING) AS value,
  gas, gas_price, nonce, receipt_gas_used, receipt_effective_gas_price,
  receipt_status, transaction_type
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE block_timestamp >= TIMESTAMP('__START_DATE__')
  AND block_timestamp <  TIMESTAMP('__END_DATE__');
