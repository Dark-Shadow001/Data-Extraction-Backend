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
from utilities.mysql_connector import MySqlConnector
from datetime import datetime, timedelta

ACTIVE = "Active"
EMPTY_CELL = ""
GSHEET_URL = "https://docs.google.com/spreadsheets/d/1pNmp9_do5VMdXDw8U2M3a84zHoQD0KmFJ44WOpTTkfM/edit#gid=0"

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
    print(f"""delete from {table_name} where SEGMENTS_DATE = '{start_date}'""")
    try:
        df['METRICS_INTERACTION_EVENT_TYPES'] = df['METRICS_INTERACTION_EVENT_TYPES'].astype(str)

    except Exception as e:
        print(e)
    status, num_chunks, num_rows = cnx.pandas_writer(df, table_name)
    print(f'start_date: {start_date} num_chunks:{num_chunks} records inserted: {num_rows} :{table_name}')


def load_data(dataframe, load_date, table_name):
    cnx = MySqlConnector(mysql_username, mysql_password, mysql_host,
                            mysql_database)
    insert(dataframe, cnx, mysql_database, load_date.strftime('%Y-%m-%d'), table_name=table_name)


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


def fetch_customers_details():
    # via google sheet link: 
    df = pd.read_excel(GSHEET_URL)
    df1 = (df[df['status'] == ACTIVE])[['customer_id', 'start_date', 'end_date']]
    active_customers = []
    for _, row in df1.iterrows():
        active_customers.append((row['customer_id'], row['start_date'], row['end_date']))
    return active_customers


def main(ga_client, level, table_name):
    fields = ''
    if level == 'campaign':
        fields = query_attributes.campaign
    elif level == 'ad_group':
        fields = query_attributes.ad_group
    elif level == 'ad_group_ad':
        fields = query_attributes.ad_group_ad
    else:
        raise Exception('fields are not passed')

    for customer in iter(fetch_customers_details()):
    # [(123, start_date1, end_date1), (234, start_date2, ''), (345, '', ''), ...]
        customer_id = customer[0]
        # if start_date and end_date in sheet are available
        if customer[1] != EMPTY_CELL and customer[2] != EMPTY_CELL:
            start_date = customer[1]
            end_date = customer[2]
        # if start_date is present but end_date is empty
        elif customer[1] != EMPTY_CELL and customer[2] == EMPTY_CELL:
            start_date = customer[1]
            end_date = datetime.today().strftime('%Y-%m-%d')
        # for last 15 days
        else:
            start_date = (datetime.today() - timedelta(days=15)).strftime("%Y-%m-%d")
            end_date = datetime.today().strftime('%Y-%m-%d')
        while start_date <= end_date:
            resp = get_metrics(ga_client, customer_id, level, fields, start_date)
            df = pd.DataFrame()
            for r in resp:
                _df = pd.DataFrame([r])
                df = pd.concat([df, _df])
                df['LOAD_DATE'] = pendulum.now().strftime('%Y-%m-%d')
            df.columns = df.columns.str.replace('.', '_', regex=False)
            load_data(df, start_date, table_name.upper())
            start_date = start_date+timedelta(days=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="google adwords data."
    )
    parser.add_argument("-p", "--path", type=str, required=True, help="The Google Ads YAML file path.", )
    # parser.add_argument("-c", "--customer_id", type=str, required=False, help="The Google Ads customer ID", )
    # parser.add_argument("-l", "--level", type=str, required=True, help="level: campaign, ad group, ad",
    #                     default='campaign')
    # parser.add_argument("-t", "--table", type=str, required=True, help="table name")
    parser.add_argument("-st", "--start_date", type=str, help="start date.",
                        default=pendulum.now().subtract(days=2).strftime('%Y-%m-%d'))
    parser.add_argument("-et", "--end_date", type=str, help="end date.",
                        default=pendulum.now().subtract().strftime('%Y-%m-%d'))

    args = parser.parse_args()

    # since = datetime.strptime(args.start_date, '%Y-%m-%d')
    # until = datetime.strptime(args.end_date, '%Y-%m-%d')
    # cus_id = args.customer_id
    google_ads_client = GoogleAdsClient.load_from_storage(args.path)

    mysql_host = os.environ['MYSQL_HOST']
    mysql_username = os.environ['MYSQL_USERNAME']
    mysql_password = os.environ['MYSQL_PASSWORD']
    mysql_database = os.environ['MYSQL_DATABASE']
    # if args.level not in ('campaign', 'ad_group', 'ad_group_ad'):
    #     raise Exception(f'{args.level} level not found')
    # if not args.table:
    #     table = args.level
    # else:
    #     table = args.table
    for level in ['campaign', 'ad_group', 'ad_group_ad']:
        main(google_ads_client, level, table_name=level)
