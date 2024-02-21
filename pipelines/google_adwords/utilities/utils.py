import pandas as pd
import numpy as np


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


def create_ddl(frame, pk=None, schema=None, table_name=None):
    if schema:
        ddl = 'CREATE TABLE IF NOT EXISTS ' + schema + '.'
    else:
        ddl = 'CREATE TABLE IF NOT EXISTS schema.'
    if table_name:
        ddl += table_name + '(\n'
    else:
        ddl += 'table_name(\n'
    for c, r in frame.items():
        if type(r.values[0]) == str:
            ddl += c + ' text,\n'
        elif type(r.values[0]) in (int, np.int64):
            ddl += c + ' BIGINT,\n'
        elif type(r.values[0]) in (float, np.float64):
            ddl += c + ' NUMERIC(38,7),\n'
        elif type(r.values[0]) in (bool, np.bool_):
            ddl += c + ' BOOLEAN,\n'
        elif type(r.values[0]) == type(None):
            ddl += c + ' text,\n'
        if 'date' in c or 'Date' in c or 'created_at' in c or 'updated_at' in c:
            ddl = ddl.replace(c + ' text,', c + ' TIMESTAMPTZ,')

    ddl += 'load_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,\n'
    if pk is None:
        ddl = ddl[:-1]
        ddl += ');'
    else:
        ddl += f'PRIMARY KEY ({pk}) );'
    return ddl
