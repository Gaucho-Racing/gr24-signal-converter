CREATE OR REPLACE PIPELINE signals_pipeline
AS LOAD DATA S3 'gaucho-racing/public/gr24_dump/signals_output/*.parquet'
CONFIG '{"region": "us-west-2"}'
CREDENTIALS '{"aws_access_key_id": "-------", "aws_secret_access_key": "-------"}'
INTO TABLE `signal`
FORMAT PARQUET
(
    `timestamp` <- `timestamp`,
    `vehicle_id` <- `vehicle_id`,
    `name` <- `name`,
    `value` <- `value`,
    `raw_value` <- `raw_value`,
    @`produced_at` <- `produced_at`,
    @`created_at` <- `created_at`
)
SET 
    created_at = FROM_UNIXTIME(@created_at/1000000),
    produced_at = FROM_UNIXTIME(@produced_at/1000000);

TEST PIPELINE signals_pipeline;