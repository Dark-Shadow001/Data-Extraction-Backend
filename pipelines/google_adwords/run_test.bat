
set SNOWFLAKE_ACCOUNT=td15766.ap-southeast-1
set SNOWFLAKE_USERNAME=Demo
set SNOWFLAKE_PASSWORD=Demo@12345
set SNOWFLAKE_DATABASE=TEST
set SNOWFLAKE_SCHEMA=PUBLIC
set SNOWFLAKE_WAREHOUSE=COMPUTE_WH
set SNOWFLAKE_ROLE=SYSADMIN

:: To run the pipeline for a single customer id
python pipeline.py -p google_ads.yaml -c 1392977511
