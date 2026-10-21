CREATE DATABASE IF NOT EXISTS etherlens;
USE etherlens;

DROP TABLE IF EXISTS silver_tx;
CREATE EXTERNAL TABLE silver_tx (
  hash STRING, block_number BIGINT, ts TIMESTAMP, hour INT, dow INT,
  from_address STRING, to_address STRING, is_contract_creation BOOLEAN,
  value_eth DOUBLE, gas_limit BIGINT, gas_used BIGINT, gas_price_gwei DOUBLE,
  fee_eth DOUBLE, nonce BIGINT, tx_type BIGINT, is_plain_transfer BOOLEAN
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '__SILVER_PATH__';

MSCK REPAIR TABLE silver_tx;

DROP TABLE IF EXISTS daily_stats;
CREATE EXTERNAL TABLE daily_stats (
  dt STRING, tx_count BIGINT, active_senders BIGINT, active_addresses BIGINT,
  total_eth DOUBLE, avg_gas_gwei DOUBLE, median_gas_gwei DOUBLE,
  total_fee_eth DOUBLE, plain_transfer_share DOUBLE, contract_creations BIGINT
)
STORED AS PARQUET
LOCATION '__GOLD_PATH__/daily_stats';
