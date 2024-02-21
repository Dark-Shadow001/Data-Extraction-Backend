***********
gs_pipeline
***********

pipeline to fetch records from google-sheet and load to postgres/snowflake warehouse

prerequisite:
***************

    * create a project in the `Google API Console <https://console.developers.google.com/apis/dashboard>`_
    * enable Google Sheets API under library tab
    * create Service Account Credentials. save json file.
    * Copy the email address from json file, go to a Google Sheet document, next click on the Share button and give editor permissions.

How to run script:
********************

    * set all the required environment variables

    ::


        export PYTHONPATH=...\gs_pipeline
        export SERVICE_ACCOUNT_PATH=\atidiv-c1-0667b61f69b9.json
        export FILE_NAME=CX Category Mapping
        export SHEET_NAME=Sheet1
        export HEADER_ROW_NUM=1
        export TABLE_NAME=cx_category_mapping
        export COLUMNS='tag,category' # pulls only the required columns in a sheet

        # snowflake target
        export SNOWFLAKE_ACCOUNT=xg21200.us-central1.gcp
        export SNOWFLAKE_USERNAME=abhishek
        export SNOWFLAKE_PASSWORD=xxxx
        export SNOWFLAKE_DATABASE=RAW
        export SNOWFLAKE_SCHEMA=TEST_SCHEMA
        export SNOWFLAKE_WAREHOUSE=ATIDIV_INGEST
        export TARGET=snowflake

        # postgres target
        export POSTGRES_USER=abhishek_atidiv
        export POSTGRES_PASSWORD=X0maPwqxxx
        export POSTGRES_HOST=xxx-pg11-8.cjdgs51ighej.us-west-2.rds.amazonaws.com
        export POSTGRES_DATABASE=dev_analytics
        export POSTGRES_SCHEMA=dev
        export TARGET=postgres



:code:`python pipeline.py`