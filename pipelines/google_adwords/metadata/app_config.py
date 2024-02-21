import os
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = os.environ['GOOGLE_CREDENTIAL_FILE_PATH']
VIEW_ID = os.environ['VIEW_ID']

snowflake_account = os.environ['SNOWFLAKE_ACCOUNT']
snowflake_username = os.environ['SNOWFLAKE_USERNAME']
snowflake_role = os.environ['SNOWFLAKE_ROLE']
snowflake_password = os.environ['SNOWFLAKE_PASSWORD']
snowflake_database = os.environ['SNOWFLAKE_DATABASE']
snowflake_schema = os.environ['SNOWFLAKE_SCHEMA']
snowflake_warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
