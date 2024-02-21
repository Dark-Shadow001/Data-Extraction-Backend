import re
from datetime import date
from pprint import pprint

import pandas as pd


def format_fields(field_lst, field_name='expression'):
    formatted_fields = []
    for field in field_lst:
        # formatted_fields.append({f'{field_name}': f'ga:{field}'})
        formatted_fields.append({f'{field_name}': f'{field}'})
    return formatted_fields


def get_headers(response):
    column_headers = response['reports'][0]['columnHeader']
    dimension_headers = column_headers.get('dimensions', [])
    metricHeaders = [n['name'] for n in column_headers['metricHeader']['metricHeaderEntries']]
    headers = dimension_headers + metricHeaders
    headers = [re.compile(r"ga:").sub("", m) for m in headers]
    # print(f'headers : {headers}')
    return headers


def get_df(response):
    df = pd.DataFrame(columns=get_headers(response))
    for row in (response['reports'][0].get('data', '').get('rows', '')):
        dim = row.get('dimensions', [])
        met = row['metrics'][0]['values']
        # print(dim + met)
        df.loc[len(df)] = dim + met
    return df


def create_table(schema, table_name, df):
    type_dict = df.columns.to_series().groupby(df.dtypes).groups
    _text = []
    _int = []
    _bool = []
    _float = []
    _date = []
    for col_type, col_name in type_dict.items():
        if col_type == object:
            # _text = [f' {x} VARIANT ' if ('[' or ']') in str(df[x].values[0]) else f' {x} TEXT ' for x in tuple(col_name)]
            _text = [f' {x} TEXT ' for x in tuple(col_name)]
        elif col_type == 'int64':
            _int = [f' {x} BIGINT ' for x in tuple(col_name)]
        elif col_type == 'float64':
            _float = [f' {x} NUMERIC(32,3) ' for x in tuple(col_name)]
        elif col_type == 'bool':
            _bool = [f' {x} boolean ' for x in tuple(col_name)]
        elif col_type == 'datetime64[ns]':
            _date = [f' {x} TIMESTAMPLTZ ' for x in tuple(col_name)]

    ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{table_name} (' + ','.join(_text + _int + _float + _bool + _date) + ');' \
        # """load_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP );"""
    # print(ddl)
    return ddl
