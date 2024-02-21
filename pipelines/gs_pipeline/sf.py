import pandas
import numpy
import snowflake.connector as sc
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import io
import gc
import logging
import string
import random
import json
from typing import Union, List, Dict, Optional
try:
    from .helper import Column
except:
    from helper import Column
from enum import Enum
logger = logging.getLogger(__name__)

class snowflake_insertmode(Enum):
    snapshot = 0
    update = 1
    insert = 2
class snowflake():

    def __init__(self,credentials : dict):
        self.client = sc.connect(
            user=credentials['username'],
            password=credentials['password'],
            account=credentials['account'],
            warehouse=credentials['warehouse'],
            database=credentials.get('database',None),
            schema=credentials.get('schema',None)
            )
        self.close = self.client.close
        if credentials.get('role',None) is not None:
            self.query(f'use role {credentials["role"]}')
        self.query(f'use warehouse {credentials["warehouse"]}')
        if credentials.get('database'):
            self.query(f'use database {credentials["database"]}')
        if credentials.get('schema'):
            self.query(f'use schema {credentials["schema"]}')
        
    @property
    def type(self):
        return "snowflake"
    @property
    def warehouse(self):
        return self.client.warehouse
    @property
    def role(self):
        return self.client.role
    @property
    def account(self):
        return self.client.account
    @property
    def database(self):
        return self.client.database
    @property
    def schema(self):
        return self.client.schema
    @property
    def region(self):
        return self.client.region

    def exists(self,
               database : str,
               schema : Optional[str] = None,
               table_or_view : Optional[str] = None
               ) -> bool : 
        exitst_result = {
            'database': False,
            'schema': False,
            'table': False,
            'view': False
            }
        query_database = "SHOW DATABASES"
        query_schemata = f"SHOW SCHEMAS in {database}"
        query_table = f"SHOW TABLES in {database}.{schema}"
        query_view = f"SHOW VIEWS in {database}.{schema}"

        result = self.query(query_database)
        if database.upper() in result['name'].values.tolist():
            exitst_result['database'] = True
            del [[result]]
            if schema:
                result = self.query(query_schemata)
                if schema.upper() in result['name'].values.tolist():
                    del [[result]]
                    exitst_result['schema'] = True

                    if table_or_view:
                        result = self.query(query_table)
                        if table_or_view.upper() in result['name'].values.tolist():
                            exitst_result['table'] = True
                        else:
                            del [[result]]
                            result = self.query(query_view)
                            if table_or_view.upper() in result.values.tolist():
                                exitst_result['view'] = True

                            del [[result]]
        gc.collect()
        return exitst_result
    def exists_database(self,
                        database : str
                        ) -> bool:
        return self.exists(database)['database']
    def exists_schema(self,
                      database : str,
                      schema : str
                      ) -> bool:
        return self.exists(database,schema)['schema']
    def exists_table(self,
                  database : str,
                  schema : str,
                  table : str,
                  ) -> bool:
        return self.exists(database,schema,table)['table']
    def exists_view(self,
                  database : str,
                  schema : str,
                  table : str,
                  ) -> bool:
        return self.exists(database,schema,table)['view']
    def create_table(
            self,
            database: str ,
            schema : str,
            table : str,
            table_schema : List[Column],
            overwrite : bool = False,
            is_temporary : bool = False,
            is_transient : bool = False,
            clusering_keys : list = None,
            normalize_name : bool = True
            ) -> bool :
        if normalize_name:
            database = database.upper()
            schema = schema.upper()
            table = table.upper()
            
        name = f'"{database}"."{schema}"."{table}"'
        if overwrite == False and self.exists_table(database,schema,table):
            raise Exception(f'{name} already exists '
                            f'pass "overwrite" = True or choose another '
                            f'destination')

        query = 'create or replace '
        if is_temporary:
            query = query + f'temporary table {name} \n(\n'
        elif is_transient:
            query = query + f'transient table {name} \n(\n'
        else:
            query = query + f'table {name}\n(\n'
        
        query+= ',\n'.join('"'+x['name'].upper()+ '" '+ x['column_type'] +
                         (f' {x["constraints"]}' if x.get('constraints',None) is not None else '') +
                         (f' default {x["default"]}' if x.get('default',None) is not None else '')  for x in table_schema)
        query+="\n)"
        if clusering_keys is not None:
            query += f'\n cluster cluster by ({",".join(clusering_keys)})'
        

        result = self.client.cursor().execute(query).fetchall()
        
        return result == [(f'Table {table} successfully created.',)]
    def create_schema(self,
                      database: str,
                      schema : str,
                      overwrite : bool = False,
                      normalize_name : bool = True
                      ) -> bool :
        if normalize_name:
            database = database.upper()
            schema = schema.upper()

        name = f'"{database}"."{schema}"'
        if overwrite == False and self.exists(database,schema,None)['schema']:
            raise Exception(f'{name} already exists '
                            f'pass "overwrite" = True or choose another '
                            f'destination')
        query = f'create or replace schema {name};'
        result = self.client.cursor().execute(query).fetchall()
        return result == [(f'Schema {schema} successfully created.',)]

    def create_database(self,
                      database: str,
                      overwrite : bool = False,
                      normalize_name : bool = True
                      ) -> bool :
        if normalize_name:
            database = database.upper()

        name = f'"{database}"'
        if overwrite == False and self.exists(database,None,None)['database']:
            raise Exception(f'{name} already exists '
                            f'pass "overwrite" = True or choose another '
                            f'destination')
        query = f'create or replace database {name};'
        result = self.client.cursor().execute(query).fetchall()
        return result == [(f'Database {database} successfully created.',)]

    
        
    def get_table_schema(self,
                         database: str,
                         schema : str,
                         table : str
                         ) -> List[Dict[str,str]] :
        def to_dict(x):
            col = Column(x['name'],
                          x['type'],
                          'not null' if x['null?']=='N' else None,
                          x['default']
                          )
            return col
        
        query = f'desc table "{database.upper()}"."{schema.upper()}"."{table.upper()}"'
        result = self.client.cursor().execute(query)
        columns = [x[0] for x in result.description]
        result = pandas.DataFrame(result.fetchall(),columns = columns)

        result = result.apply(lambda x: to_dict(x),axis=1).values.tolist()
        return result

    def drop_database(self,
                      database : str
                      ) -> bool :
        database = database.upper()
        name = f'{database}'
        output = self.client.cursor().execute(f'drop database if exists {name};')
        output = output.fetchall()
        
        return (output[0][0] == f'{database} successfully dropped.' or
                output[0][0] == (f"Drop statement executed successfully "
                                 "({database} already dropped).")
                )
    
    def drop_schema(self,
                    database: str,
                    schema : str
                    ) -> bool :
        name = f'{database}.{schema}'
        output = self.client.cursor().execute(f'drop schema if exists {name};')
        output = output.fetchall()
        
        return (output[0][0] == f'{schema} successfully dropped.' or
                output[0][0] == (f"Drop statement executed successfully "
                                 "({schema} already dropped).")
                )

    def drop_table(self,
                   database: str,
                   schema : str,
                   table : str
                   ) -> bool :

        name = f'{database}.{schema}.{table}'
        output = self.client.cursor().execute(f'drop table if exists {name};')
        output = output.fetchall()
        
        return (output[0][0] == f'{table} successfully dropped.' or
                output[0][0] == (f"Drop statement executed successfully "
                                 "({table} already dropped).")
                )

    def add_columns(self,
                    database: str,
                    schema : str,
                    table : str,
                    columns :  List[Column]
                    ) -> bool :
        table_schema = self.get_table_schema(database,schema,table)
        names = [x['name'] for x in table_schema]
        t_name = f'"{database.upper()}"."{schema.upper()}"."{table.upper()}"'
        query_strings = []
        for column in columns:
            if column['name'] in names:
                pass
            else:
                query_string = f'{column["name"]} {column["column_type"]} '
                if column.get('default',None) is not None:
                    query_string += f'default {column["default"]} '
                if column.get('constraints',None) is not None:
                    query_string += f'{column["constraints"]} '
                query_strings.append(query_string)
        if len(query_strings) >0 :
            query_string = f'alter table {t_name} add column '
            query_string+= ','.join(query_strings)

            
            result = self.query(query_string).values.tolist()
            return result[0][0] == 'Statement executed successfully.'
        else: return True

    def drop_columns(self, database: str ,schema : str, table : str, columns :  list):
        table_schema = self.get_table_schema(database,schema,table)
        names = [x['name'] for x in table_schema]
        t_name = f'"{database.upper()}"."{schema.upper()}"."{table.upper()}"'
        query_string = ''
        for column in columns:
            if column['name'] in names:
                pass
            else:
                query_string += f'{column["name"]} {column["column_type"]} '
                if column.get('default',None) is not None:
                    query_string += f'default {column["default"]} '
                if column.get('constraints',None) is not None:
                    query_string += f'{column["constraints"]} '
        if query_string != '':
            query_string = f'alter table {t_name} add' + query_string
        result = self.query(query_string).values.tolist()
        return result[0][0] == 'Statement executed successfully.'
            
    
    def query(self,query,output = 'df'):
        
        logger.log(25,f'executing {query[0:30]}..')
        result = self.client.cursor().execute(query)
        if output == 'df':
            # try:
                # df = result.fetch_pandas_all()
            # except:
            values = result.fetchall()
            columns = [col[0] for col in result.description] 
            df = pandas.DataFrame(values,columns=columns)
            return df
        else:
            data = result.fetchall()
            columns = [col[0] for col in result.description]
            return columns,data
    @staticmethod
    def make_str(x):
        if x is None or isinstance(x,(str,datetime)):
            return x
        elif type(x) == float and \
             (x == float('NaN') or numpy.isnan(x)):
            return None
        elif isinstance(x ,(list,dict)):
            return json.dumps(x)
        else:
            return str(x)

    def insert_rows_from_dataframe(
            self,
            df : pandas.DataFrame,
            table : str,
            schema : str,
            database : str,
            create_if_not_exist : bool = False,
            truncate : bool = False,
            expand : bool = False,
            **kwargs
            ):
        if kwargs.get('normalize_name',True) == True:
            table = table.upper()
            schema = schema.upper()
            database = database.upper()
        df_new = df.copy()
        df_new.columns = map(str.upper, df_new.columns)
        df_new = df_new.applymap(snowflake.make_str)
        table_exists = self.exists_table(database,schema,table)
        if table_exists == False and create_if_not_exist == False:
            raise Exception(f"Table {database}.{schema}.{table} "
                            f"does not exist.")
        elif table_exists == False and create_if_not_exist == True:
            
            columns = [Column(col,"VARIANT") for col in df_new.columns]
            result = self.create_table(database,schema,table,columns)
            if len(df) == 0: return 0
        elif table_exists == True:
            if len(df) == 0: return 0

            result = self.get_table_schema(database,
                                           schema,
                                           table)
            cols_in_table = [r['name'] for r in  result]
            cols_in_df = df_new.columns
            cols_to_add_df = list(set(cols_in_table)-set(cols_in_df))
            for col in cols_to_add_df:
                df_new[col] = None

            if expand:
                cols_to_add_table = list(set(cols_in_df)-set(cols_in_table))
                columns = [Column(col,"VARIANT") for col in cols_to_add_table]
                if len(columns) > 0:
                    result = self.add_columns(database,
                                         schema,
                                         table,
                                         columns
                                         )
            else:
                cols_to_drop = list(set(cols_in_df)-set(cols_in_table))
                df_new.drop(columns = cols_to_drop,inplace=True)
            
            if truncate:
                self.query(f'TRUNCATE TABLE "{database}"."{schema}"."{table}"')

        status, num_chunks, num_rows, _ = write_pandas(
            self.client,
            df_new,
            table,
            database = database,
            schema = schema,
            chunk_size=kwargs.get('chunk_size',16000))
        return num_rows        

    
    def snapshot_insert_rows_from_dataframe(
            self,
            df : pandas.DataFrame,
            table : str,
            schema : str,
            database : str,
            primary_keys :  List[str] = [],
            target_columns :  List[str] = [],
            exclude_columns :  List[str] = [],
            create_if_not_exist : bool = False,
            truncate : bool = False,
            expand : bool = False,
            insert_mode : snowflake_insertmode = snowflake_insertmode.snapshot,
            **kwargs
            ):
        stage_table = ''.join(random.choices(
            string.ascii_uppercase, k=6))
        stage_table = stage_table + '_' + table
        if kwargs.get('normalize_name',True) == True:
                table = table.upper()
                stage_table = stage_table.upper()
                schema = schema.upper()
                database = database.upper()
        self.query(f'USE DATABASE {database}')
        self.query(f'USE SCHEMA {schema}')
        exclude_columns = list(map(str.upper,exclude_columns))
        primary_keys = list(map(str.upper,primary_keys))
        df_new = df.copy()
        df_new.columns = map(str.upper, df_new.columns)
        df_new = df_new.applymap(snowflake.make_str)
        table_exists = self.exists_table(database,schema,table)
        if table_exists == False and create_if_not_exist == False:
            raise Exception(f"Table {database}.{schema}.{table} "
                            f"does not exist.")
        elif table_exists == False and create_if_not_exist == True:
            columns = [Column(col,"VARIANT") for col in df_new.columns]
            result = self.create_table(database,schema,table,columns)
            if len(df) == 0: return {'number of rows inserted': 0}
            status, num_chunks, num_rows, _ = write_pandas(
            self.client,
            df_new,
            table,
            database = database,
            schema = schema,
            chunk_size=kwargs.get('chunk_size',16000))
            return {'number of rows inserted': num_rows}
        elif table_exists == True:
            if len(df) == 0: return {'number of rows inserted': 0}
            result = self.get_table_schema(database,
                                           schema,
                                           table)
            cols_in_table = [r['name'] for r in  result]
            cols_in_df = df_new.columns
            cols_to_add_df = list(set(cols_in_table)-set(cols_in_df))
            for col in cols_to_add_df:
                df_new[col] = None

            if expand:
                cols_to_add_table = list(set(cols_in_df)-set(cols_in_table))
                columns = [Column(col,"VARIANT") for col in cols_to_add_table]
                if len(columns) > 0:
                    result = self.add_columns(database,
                                         schema,
                                         table,
                                         columns
                                         )
            else:
                cols_to_drop = list(set(cols_in_df)-set(cols_in_table))
                df_new.drop(columns = cols_to_drop,inplace=True)

            self.query(f'create temporary table '
                       f'"{database}"."{schema}"."{stage_table}" like '
                       f'"{database}"."{schema}"."{table}"')
            status, num_chunks, num_rows, _ = write_pandas(
                self.client,
                df_new,
                stage_table,
                database = database,
                schema = schema,
                chunk_size=kwargs.get('chunk_size',16000))
            
            _columns = df_new.columns
            _columns =  list(map(str.upper,_columns))
            _primary_keys = list(map(str.upper,primary_keys))
            _target_columns = list(map(str.upper,target_columns))
            _exclude_columns = list(map(str.upper,exclude_columns))
            if len(_primary_keys) == 0:
                _primary_keys = list(_columns)
            
                
            _target_columns = list(set(_target_columns) - set(_exclude_columns))
            _primary_keys = list(set(_primary_keys) - set(_exclude_columns))
            _primary_keys = list(set(_primary_keys) - set(_target_columns))

            insert_expr = 'insert (' + ','.join([f'"{c}"'
                                                 for c in _columns]) +')'
            value_expr = 'values (' + ','.join([f's."{c}"'
                                                for c in _columns]) +')'

            target_full_name = f'"{database}"."{schema}"."{table}"'
            stage_full_name = f'"{database}"."{schema}"."{stage_table}"'

            if insert_mode == snowflake_insertmode.snapshot or \
               insert_mode == snowflake_insertmode.snapshot.name or \
               insert_mode == snowflake_insertmode.snapshot.value:
                join_expr = 'and'.join([f' equal_null(f."{c}",s."{c}") '
                                    for c in set(_primary_keys +
                                                 _target_columns)])
                merge_query = (f'MERGE INTO {target_full_name} f '
                               f'USING {stage_full_name} s '
                               f"ON {join_expr} "
                               f"when not matched then {insert_expr} "
                               f"{value_expr}"
                               )
            elif insert_mode == snowflake_insertmode.update or \
               insert_mode == snowflake_insertmode.update.name or \
               insert_mode == snowflake_insertmode.update.value:
                join_expr = 'and'.join([f' equal_null(f."{c}",s."{c}") '
                                    for c in set(_primary_keys)])
                if len(_target_columns) == 0:
                    update_columns = list(set(_columns) - set(_primary_keys))
                    _target_columns = update_columns
                else:
                    update_columns = _target_columns + _exclude_columns
                update_columns = list(set(update_columns))
                set_expr = 'update set ' + ','.join([f'f."{c}"=s."{c}"'
                                                   for c in update_columns])
                # print(set_expr)
                match_expr = ' or '.join([f'equal_null(f."{c}",s."{c}") = FALSE'
                                                  for c in _target_columns])
                # print(match_expr)
                if len(match_expr) > 0:
                    match_expr = ' and ('+ match_expr +')'

                # print(match_expr)
                                                 
                merge_query = (f'MERGE INTO {target_full_name} f '
                               f'USING {stage_full_name} s '
                               f"ON {join_expr} "
                               f"when not matched then {insert_expr} "
                               f"{value_expr} "
                               f"when matched  {match_expr} then {set_expr} "
                               )
            elif insert_mode == snowflake_insertmode.insert or \
               insert_mode == snowflake_insertmode.insert.name or \
               insert_mode == snowflake_insertmode.insert.value:
                merge_query = (f'insert into {target_full_name} '
                               f'select * from {stage_full_name}')
                
            # print(merge_query)
            df = self.query(merge_query)
            return df.to_dict('records')[0]