import argparse
import os
import sys
from operator import attrgetter
import pandas as pd
import pendulum
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from metadata import query_attributes
import metadata.utils as ut
from utilities.snowflake_connector import SnowflakeConnector
from datetime import datetime, timedelta


def insert(df, cnx, schema, start_date, table_name=None):
    is_exists = cnx.is_table_exists(table_name)
    if len(df) == 0:
        if not is_exists:
            raise Exception(f'no records found for the config. Hence, {table_name} table is not created')
        print(f'records inserted: 0 : {table_name}')
        return
    if not is_exists:
        ddl = ut.create_table(schema, table_name, df)
        cnx.execute(ddl)
    # get cols from table and df to compare
    col_table = cnx.get_all_columns(table_name)
    df.columns = map(str.upper, df.columns)
    col_df = df.columns
    cols_to_add = list(set(col_df) - set(col_table))

    if len(cols_to_add) > 0:
        print(f'columns to add : {cols_to_add}')
    for col in cols_to_add:
        sql = f"""ALTER TABLE {schema}.{table_name} ADD {col} text;"""
        cnx.execute(sql)
    cnx.execute(f"""delete from {table_name} where SEGMENTS_DATE = '{start_date}'""")
    try:
        df['METRICS_INTERACTION_EVENT_TYPES'] = df['METRICS_INTERACTION_EVENT_TYPES'].astype(str)

    except Exception as e:
        print(e)
    status, num_chunks, num_rows = cnx.pandas_writer(df, table_name)
    print(f'start_date: {start_date} num_chunks:{num_chunks} records inserted: {num_rows} :{table_name}')


def load_data(dataframe, load_date, table_name):
    cnx = SnowflakeConnector(snowflake_username, snowflake_password, snowflake_account,
                             snowflake_warehouse, snowflake_database, snowflake_schema)
    insert(dataframe, cnx, snowflake_schema, load_date.strftime('%Y-%m-%d'), table_name=table_name)


def get_metrics(client, customer_id, level, fields, segment_date):
    ga_service = client.get_service("GoogleAdsService", version="v11")
    query = f"""SELECT {fields} FROM {level} where segments.date = '{segment_date}'"""
    # Issues a search request using streaming.
    response = ga_service.search_stream(customer_id=customer_id, query=query)
    try:
        for batch in response:
            for row in batch.results:
                field_list = fields.split(', ')
                resp = {}
                for f in field_list:
                    resp[f] = attrgetter(f)(row)
                    # print(resp)
                # resp.update({'segments.date': row.segments.date})
                yield resp
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)


def main(ga_client, customer_id, level, start, end, table_name):
    fields = ''
    if level == 'campaign':
        fields = query_attributes.campaign
    elif level == 'ad_group':
        fields = query_attributes.ad_group
    elif level == 'ad_group_ad':
        fields = query_attributes.ad_group_ad
    else:
        raise Exception('fields are not passed')

    while start < end:
        resp = get_metrics(ga_client, customer_id, level, fields, start.strftime('%Y-%m-%d'))
        df = pd.DataFrame()
        for r in resp:
            _df = pd.DataFrame([r])
            df = pd.concat([df, _df])
            df['LOAD_DATE'] = pendulum.now().strftime('%Y-%m-%d')
        df.columns = df.columns.str.replace('.', '_', regex=False)
        load_data(df, start, table_name.upper())
        start = start+timedelta(days=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="google adwords data."
    )
    parser.add_argument("-p", "--path", type=str, required=True, help="The Google Ads YAML file path.", )
    parser.add_argument("-c", "--customer_id", type=str, required=True, help="The Google Ads customer ID.", )
    # parser.add_argument("-l", "--level", type=str, required=True, help="level: campaign, ad group, ad",
    #                     default='campaign')
    # parser.add_argument("-t", "--table", type=str, required=True, help="table name")
    parser.add_argument("-st", "--start_date", type=str, help="start date.",
                        default=pendulum.now().subtract(days=15).strftime('%Y-%m-%d'))
    parser.add_argument("-et", "--end_date", type=str, help="end date.",
                        default=pendulum.now().subtract().strftime('%Y-%m-%d'))

    args = parser.parse_args()

    since = datetime.strptime(args.start_date, '%Y-%m-%d')
    until = datetime.strptime(args.end_date, '%Y-%m-%d')
    cus_id = args.customer_id
    google_ads_client = GoogleAdsClient.load_from_storage(args.path)

    snowflake_account = os.environ['SNOWFLAKE_ACCOUNT']
    snowflake_username = os.environ['SNOWFLAKE_USERNAME']
    snowflake_password = os.environ['SNOWFLAKE_PASSWORD']
    snowflake_database = os.environ['SNOWFLAKE_DATABASE']
    snowflake_schema = os.environ['SNOWFLAKE_SCHEMA']
    snowflake_warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
    # if args.level not in ('campaign', 'ad_group', 'ad_group_ad'):
    #     raise Exception(f'{args.level} level not found')
    # if not args.table:
    #     table = args.level
    # else:
    #     table = args.table
    for level in ['campaign', 'ad_group', 'ad_group_ad']:
        main(google_ads_client, cus_id, level, since, until, table_name=level)
