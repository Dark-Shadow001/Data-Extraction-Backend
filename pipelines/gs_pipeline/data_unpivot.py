"""

set all the required environment variables before running gs_pipeline.py. This loads google sheet records to the
target data warehouse.

        *** required ***
        export FILE_NAME=CX Category Mapping
        export SHEET_NAME=Sheet1
        export HEADER_ROW_NUM=1
        export TABLE_NAME=cx_category_mapping
        export COLUMNS='tag,category'  # pulls only the required columns in a sheet

        *** depends on target ***
        # snowflake target
        export SNOWFLAKE_ACCOUNT=xg21200.us-central1.gcp
        export SNOWFLAKE_USERNAME=abhishek
        export SNOWFLAKE_PASSWORD=xxxx
        export SNOWFLAKE_DATABASE=RAW
        export SNOWFLAKE_SCHEMA=TEST_SCHEMA
        export SNOWFLAKE_WAREHOUSE=ATIDIV_INGEST
        export TARGET=snowflake
        *** or ***
        # postgres target
        export POSTGRES_USER=abhishek_atidiv
        export POSTGRES_PASSWORD=X0maPwqxxx
        export POSTGRES_HOST=xxx-pg11-8.cjdgs51ighej.us-west-2.rds.amazonaws.com
        export POSTGRES_DATABASE=dev_analytics
        export POSTGRES_SCHEMA=dev
        export TARGET=postgres

"""

from datetime import datetime, timedelta
import os
from helpers import pretty, create_ddl
from tap import google_sheet
from target.postgres_connector import PostgresConnector
from target.snowflake_connector import SnowflakeConnector
import sf

def push_snowflake(frame, table_name):
    if os.environ['SHEET_NAME'] == "Inventory Target":
        fixed_columns = ['macro', 'sub', 'fabric', 'color', 'sku', 'safety_days']
    
    if os.environ['SHEET_NAME'] == "Forecast":
        fixed_columns = ['macro', 'sub', 'color', 'sku']

    columns = [i for i in fixed_columns]

    df_columns = list(frame.columns)

    date_columns = [i for i in df_columns if i not in fixed_columns]
    for i in date_columns:
        if (datetime.now() - timedelta(days=int( os.environ['INTERVAL'] ))) <= (datetime.strptime(i, "%m/%d/%Y")):
            columns.append(i)

    frame = frame[columns]
    frame = frame.melt(id_vars = fixed_columns, var_name = 'Week', value_name = 'Quantity')



    frame.columns = map(str.upper, frame.columns)
    frame['LOAD_TIME'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    credentials = {
        "account" : os.environ['SNOWFLAKE_ACCOUNT'],
        "username" : os.environ['SNOWFLAKE_USERNAME'],
        "password" : os.environ['SNOWFLAKE_PASSWORD'],
        "warehouse" : os.environ['SNOWFLAKE_WAREHOUSE'],
        "database" : os.environ['SNOWFLAKE_DATABASE'],
        "schema" : os.environ['SNOWFLAKE_SCHEMA'],
        "role" : "PIPELINE_ROLE"
    }

    sf_client = sf.snowflake(credentials=credentials)
    
    res = sf_client.snapshot_insert_rows_from_dataframe(frame,
                                                        table_name,
                                                        os.environ['SNOWFLAKE_SCHEMA'],
                                                        os.environ['SNOWFLAKE_DATABASE'],
                                                        create_if_not_exist = True,
                                                        expand=True,
                                                        insert_mode=sf.snowflake_insertmode.snapshot)



    # ddl = create_ddl(frame, table_name=table_name, target='snowflake')

    # sf_account = os.environ['SNOWFLAKE_ACCOUNT']
    # sf_username = os.environ['SNOWFLAKE_USERNAME']
    # sf_password = os.environ['SNOWFLAKE_PASSWORD']
    # sf_database = os.environ['SNOWFLAKE_DATABASE']
    # sf_schema = os.environ['SNOWFLAKE_SCHEMA']
    # sf_warehouse = os.environ['SNOWFLAKE_WAREHOUSE']

    # # connects to snowflake
    # cnx = SnowflakeConnector(sf_username, sf_password, sf_account, sf_warehouse, sf_database, sf_schema)
    # pretty(f'connected to snowflake')
    # pretty(f'executing ddl statements...')
    # # pretty(ddl)
    # cnx.execute(ddl)
    # frame.columns = map(str.upper, frame.columns)
    # frame['LOAD_TIME'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # success, nchunks, nrows = cnx.pandas_writer(df=frame, table_name=table_name.upper())
    # pretty(f'loaded {nrows} records to {table_name}')


def push_postgres(frame, table_name):
    ddl = create_ddl(frame, table_name=table_name, target='postgres')

    db_user = os.environ['POSTGRES_USER']
    db_password = os.environ['POSTGRES_PASSWORD']
    host = os.environ['POSTGRES_HOST']
    database = os.environ['POSTGRES_DATABASE']
    schema = os.environ['POSTGRES_SCHEMA']

    cnx = PostgresConnector(db_user, db_password, host, database, schema)
    pretty(f'connected to postgres')
    pretty(f'executing ddl statements...')
    # pretty(ddl)
    cnx.execute(ddl)
    cnx.truncate_table(table_name)
    num_chunks, num_rows = cnx.insert_writer(frame, table_name)
    pretty(f'loaded {num_rows} records to {table_name}')


def main():
    saf_path = os.environ['SERVICE_ACCOUNT_PATH']
    file_name = os.environ['FILE_NAME']
    sheet_name = os.environ['SHEET_NAME']
    header_row_num = os.environ['HEADER_ROW_NUM']
    cols = os.environ.get('COLUMNS', None)  # 'tag,category'
    target = os.environ['TARGET']
    table_name = os.environ['TABLE_NAME']

    frame = google_sheet(
        service_account_file=saf_path,
        file_name=file_name,
        sheet_name=sheet_name,
        header_row_num=header_row_num,
        columns=cols,
    )

    if target == 'snowflake':
        push_snowflake(frame, table_name)

    elif target == 'postgres':
        push_postgres(frame, table_name)

    else:
        pretty(f'target {target} does not exists')


if __name__ == '__main__':
    st = datetime.now()
    pretty(f'ETL started: {st}')
    main()
    et = datetime.now()
    pretty(f'ETL Ended: {et}')
    pretty(f'Execution time: {et - st}')
