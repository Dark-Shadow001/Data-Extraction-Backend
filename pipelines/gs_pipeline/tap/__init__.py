import gspread
import pandas as pd
import numpy as np


def google_sheet(**kwargs):
    """
    :param kwargs:  file_name (google sheet file name),
                    sheet_id or sheet_name, service_account_file,
                    header_row_num (default=0),
                    columns (default all) - list of cols to return
    :return: data-frame
    """

    sheet_id = kwargs.get('sheet_id', None)
    service_account_file = kwargs.get('service_account_file', None)
    gs_file_name = kwargs.get('file_name', None)
    gs_name = kwargs.get('sheet_name', None)
    gs_header = int(kwargs.get('header_row_num', 0))
    gs_cols = kwargs.get('columns', None)

    if gs_header > 0:
        gs_header = gs_header - 1

    gc = gspread.service_account(filename=service_account_file)
    if gs_file_name:
        sh = gc.open(gs_file_name)
    elif sheet_id:
        sh = gc.open_by_key(sheet_id)
    else:
        raise Exception('sheet_id or sheet_name is required')

    worksheet = sh.worksheet(gs_name)

    # ### retrieve data ###
    data = worksheet.get_all_values()
    headers = data.pop(gs_header)  # find headers
    df = pd.DataFrame(data, columns=headers)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    if gs_cols:
        gs_cols = gs_cols.split(',')
        gs_cols = [s.strip().replace(' ', '_') for s in gs_cols]
        df = df[gs_cols]
    else:
        df = df[[col for col in list(df.columns) if col != '']]

    df = df.replace('', np.nan)
    df.dropna(how='all', inplace=True)
    df = df.replace({np.nan: None})
    return df
