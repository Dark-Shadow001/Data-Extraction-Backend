import argparse
import os
import pandas as pd
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
import google_adwords.metadata.utils as ut
from google_adwords.utilities.mysql_connector import MySqlConnector

ACTIVE = "Active"
EMPTY_CELL = ""
GSHEET_URL = "https://docs.google.com/spreadsheets/d/1pNmp9_do5VMdXDw8U2M3a84zHoQD0KmFJ44WOpTTkfM/edit#gid=0"


def load_gsheet_data(table_name):
    cnx = MySqlConnector(mysql_username, mysql_password, mysql_host,mysql_database)
    dataframe = pd.read_excel(GSHEET_URL)
    cnx.upsert_into_table(mysql_database, table_name, dataframe)


if __name__ == "__main__":

    mysql_host = os.environ['MYSQL_HOST']
    mysql_username = os.environ['MYSQL_USERNAME']
    mysql_password = os.environ['MYSQL_PASSWORD']
    mysql_database = os.environ['MYSQL_DATABASE']
    mysql_table_name = "customers"

    today_date_str = datetime.today().strftime('%Y-%m-%d')
    load_gsheet_data(mysql_table_name)
