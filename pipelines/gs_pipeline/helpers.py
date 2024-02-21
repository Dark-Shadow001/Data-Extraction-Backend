from datetime import datetime

import numpy as np
import pandas as pd


def pretty(*ag):
    d = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{d} - {ag[0]}')


def normalise(jsn, main, **kwargs):
    """returns flattened list of dfs based on input params"""
    nsd_tmp = []
    main_df = pd.json_normalize(jsn[main], sep='_')
    nsd_tmp.append(main_df)

    for k, v in kwargs.items():
        record_path = v[0]
        try:
            meta = v[1]
            inter_df = pd.json_normalize(jsn[main], record_path, meta, sep='_')
        except IndexError:
            inter_df = pd.json_normalize(jsn[main], record_path, sep='_')
        nsd_tmp.append(inter_df)

    # remove list in json
    normalised = []
    for f in nsd_tmp:
        if len(f) > 0:
            line_col = [k for k, v in f.items() if type(v.values[0]) != list]
            f = f[line_col]
            normalised.append(f)
        else:
            normalised.append(pd.DataFrame())

    return normalised


def create_ddl(frame, pk=None, table_name=None, target='snowflake'):
    ddl = ''
    if table_name:
        ddl += table_name + '(\n'
    else:
        ddl += 'table_name(\n'
    for c, r in frame.items():
        if type(r.values[0]) == str:
            ddl += "\"" + c.upper() + "\"" + ' text,\n'
        elif type(r.values[0]) in (int, np.int64):
            ddl += "\"" + c.upper() + "\"" + ' BIGINT,\n'
        elif type(r.values[0]) in (float, np.float64):
            ddl += "\"" + c.upper() + "\"" + ' NUMERIC(38,7),\n'
        elif type(r.values[0]) in (bool, np.bool_):
            ddl += "\"" + c.upper() + "\"" + ' BOOLEAN,\n'
        elif type(r.values[0]) == type(None):
            ddl += "\"" + c.upper() + "\"" + ' text,\n'
        if 'date' in c or 'Date' in c or 'created_at' in c or 'updated_at' in c:
            ddl = ddl.replace(c + ' text,', c + ' TIMESTAMP_LTZ(9),')

    if target == 'snowflake':
        ddl = 'CREATE OR REPLACE TABLE ' + ddl + 'LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()\n'
    elif target == 'postgres':
        ddl = 'CREATE TABLE IF NOT EXISTS ' + ddl + 'load_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP\n'

    if pk is None:
        ddl = ddl[:-1]
        ddl += ');'
    else:
        ddl += f'PRIMARY KEY ({pk}) );'

    return ddl
