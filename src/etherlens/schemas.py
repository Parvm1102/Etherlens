"""Spark schemas for bronze, silver, and stream records."""
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_SCHEMA = StructType([
    StructField("hash", StringType(), True),
    StructField("block_number", LongType(), True),
    StructField("block_timestamp", TimestampType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("value", StringType(), True),
    StructField("gas", LongType(), True),
    StructField("gas_price", LongType(), True),
    StructField("nonce", LongType(), True),
    StructField("receipt_gas_used", LongType(), True),
    StructField("receipt_effective_gas_price", LongType(), True),
    StructField("receipt_status", LongType(), True),
    StructField("transaction_type", LongType(), True),
])

SILVER_SCHEMA = StructType([
    StructField("hash", StringType(), False),
    StructField("block_number", LongType(), False),
    StructField("ts", TimestampType(), False),
    StructField("dt", StringType(), False),
    StructField("hour", IntegerType(), False),
    StructField("dow", IntegerType(), False),
    StructField("from_address", StringType(), False),
    StructField("to_address", StringType(), True),
    StructField("is_contract_creation", BooleanType(), False),
    StructField("value_eth", DoubleType(), False),
    StructField("gas_limit", LongType(), False),
    StructField("gas_used", LongType(), False),
    StructField("gas_price_gwei", DoubleType(), False),
    StructField("fee_eth", DoubleType(), False),
    StructField("nonce", LongType(), False),
    StructField("tx_type", LongType(), False),
    StructField("is_plain_transfer", BooleanType(), False),
])

STREAM_SCHEMA = StructType([
    StructField("hash", StringType(), True),
    StructField("block_number", LongType(), True),
    StructField("block_timestamp", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("value_wei", StringType(), True),
    StructField("gas_limit", LongType(), True),
    StructField("gas_price_wei", LongType(), True),
    StructField("nonce", LongType(), True),
    StructField("tx_type", IntegerType(), True),
])
