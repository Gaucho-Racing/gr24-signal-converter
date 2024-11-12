CREATE OR REPLACE PIPELINE signals_pipeline
AS LOAD DATA S3 'gaucho-racing/public/gr24_dump/signals_output/*.parquet'
CONFIG '{"region": "us-west-2"}'
CREDENTIALS '{"aws_access_key_id": "-------", "aws_secret_access_key": "-------"}'
INTO TABLE signals
FORMAT PARQUET
(
    signal_id <- signal_id,
    @created_at <- created_at,
    scaled_value <- scaled_value,
    node <- node,
    millis <- millis
)
SET created_at = FROM_UNIXTIME(@created_at/1000000);

TEST PIPELINE signals_pipeline;