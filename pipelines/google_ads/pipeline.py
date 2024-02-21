import os
import json
import gspread
import argparse
import pendulum
import pandas as pd
from datetime import datetime


ACTIVE = "Active"
EMPTY_CELL = ""
SERVICE_ACCOUNT = "service_account.json"
GSHEET_URL = "https://docs.google.com/spreadsheets/d/1pNmp9_do5VMdXDw8U2M3a84zHoQD0KmFJ44WOpTTkfM/edit#gid=0"
WORKSHEET_NAME = "ads_account"


loginCustomerId = "6475271286"
oauth_client_id = "889536869374-eqruuc0bd1pssirbcu2vk5lrkr3sjfc3.apps.googleusercontent.com"
oauth_client_secret = "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"
refresh_token = "1//04deMq9JLHpPBCgYIARAAGAQSNwF-L9IrzCbvLQ3ZX029z6noW0rUep8Rd569daxXOF5ft-GMciHAHbsR91JbyGgE_3qHGxguqbo"
developer_token = "mT-y0a7eg1v3-JA5mXcZyA"


postgres_host = "campaignkart-edw.chesmplfieye.ap-south-1.rds.amazonaws.com"
postgres_port = 5432
postgres_user = "siteuser"
postgres_password = "0a7AVSvctgavf-AVFtvt7s-gby8v"
postgres_dbname = "website"
postgres_schema = "google_ads"
batch_size_rows = 10000
data_flattening_max_level = 5


def customers_data():
    gc = gspread.service_account(filename = SERVICE_ACCOUNT)
    sh = gc.open_by_url(GSHEET_URL)
    worksheet = sh.worksheet(WORKSHEET_NAME)
    data =  worksheet.get_all_values()
    header = data.pop( 0 )
    df = pd.DataFrame(data, columns = header)
    df1 = (df[df['status'] == ACTIVE])[['customer_id', 'start_date', 'end_date']]
    active_customers = []
    for _, row in df1.iterrows():
        active_customers.append( ( row['customer_id'], row['start_date'], row['end_date'] ) )
    return active_customers
    

def main():
    start = datetime.now()
    parser = argparse.ArgumentParser()
    # tz = pendulum.timezone('UTC')
    # parser.add_argument("-s", "--start_date", default= tz.convert(pendulum.now().subtract(days=30)).isoformat(timespec= "seconds").replace("+00:00", "Z") )
    # parser.add_argument("-e", "--end_date", default= tz.convert(pendulum.now().subtract(days=1)).isoformat(timespec= "seconds").replace("+00:00", "Z") )
    parser.add_argument("-s", "--start_date", default= pendulum.now().subtract(days = 30 ).to_date_string() )
    parser.add_argument("-e", "--end_date", default= pendulum.now().subtract(days = 1 ).to_date_string() )
    parser.add_argument("-c", "--customer_id" )
    args = parser.parse_args()

    tap = "tap-google-ads"
    target = "target-postgres"
    tap_config = f"config_{tap}.json"
    target_config = f"config_{tap}_{target}.json"
    
    if args.customer_id:
        customer_id_list = [ (args.customer_id, "", "") ]
    else:
        customer_id_list = customers_data()

    for row in customer_id_list:
        customer_id, start_date, end_date = row
        
        if args.start_date:
            start_date = f"{args.start_date}T00:00:00Z"
        else:
            start_date = f"{start_date}T00:00:00Z"

        if args.end_date:
            end_date = f"{args.end_date}T00:00:00Z"
        else:
            end_date = f"{end_date}T00:00:00Z"

        with open(tap_config, 'w') as f:
            json.dump({
                "start_date": start_date,
                "end_date": end_date,
                "login_customer_ids": [{"customerId": customer_id, "loginCustomerId": loginCustomerId}],
                "oauth_client_id": oauth_client_id,
                "oauth_client_secret": oauth_client_secret,
                "refresh_token": refresh_token,
                "developer_token": developer_token,
            }, f)

        with open(target_config, 'w') as f:
            json.dump({
                "host": postgres_host,
                "port": postgres_port,
                "user": postgres_user,
                "password": postgres_password,
                "dbname": postgres_dbname,
                "default_target_schema": postgres_schema,
                "batch_size_rows": batch_size_rows,
                "data_flattening_max_level": data_flattening_max_level,
            }, f)

        for catalog in ['campaign', 'ad_group', 'ad']:
            if os.name == "posix":
                cmd = f"{tap}/.venv/bin/{tap} --config {tap_config} --catalog {catalog}_performance_report.json | {target}/.venv/bin/{target} --config {target_config}"
            elif os.name == "nt":
                cmd = f"{tap}\.venv\Scripts\\{tap} --config {tap_config} --catalog {catalog}_performance_report.json | {target}\.venv\Scripts\\{target} --config {target_config}"

            try:
                print(cmd)
                r = os.system(cmd)
            except Exception as e:
                print(e)
        
        os.remove(tap_config) if os.path.exists(tap_config) else None
        os.remove(target_config) if os.path.exists(target_config) else None
    
    end = datetime.now()
    print(f"Execution Time: {end - start}")


if __name__ == '__main__':
    main()
