import math
import concurrent.futures
import numpy as np
import pandas as pd
import mysqlx.connection

class MySqlConnector:
    chunk_size = 16000

    def __init__(self, user, password, host, database):
        """
        :param user:
        :param password:
        :param database:
        :param schema:
        """

        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.connection = self._connection()

    def _connection(self):
        try:
            self.connection = MySQLdb.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          database=self.database,
                                           )
            return self.connection
        except (Exception, MySQLdb.Error) as error:
            print(f"Error while connecting to MySQL: {error}")
            raise error

    def execute(self, sql):
        """ runs all selector queries"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                self.connection.commit()
                try:
                    count = cursor.rowcount
                    res = cursor.fetchall()
                except MySQLdb.Error:
                    res = count
            return res
        except MySQLdb.Error as e:
            # print(sql)
            raise e

    def split_df(self, df, chunk_size):
        """
        :param df:
        :param chunk_size:
        :return: num_chunks, list_of_df
        """
        list_of_df = list()
        df = df.drop_duplicates()
        num_chunks = math.ceil(len(df) / self.chunk_size)
        for i in range(num_chunks):
            list_of_df.append(df[i * chunk_size:(i + 1) * chunk_size])
        return num_chunks, list_of_df   

    def get_columns(self, table_name):
        """
        :param table_name:
        :return: list of columns
        """
        query_col = """SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '""" + table_name + """'
                    AND TABLE_SCHEMA = '""" + self.schema + """'
                    ORDER BY ORDINAL_POSITION"""

        # print(f'finding columns : {table_name} ')
        res = self.execute(query_col)
        columns = [i[0] for i in res]
        columns = columns[:-1]
        return columns

    def get_all_columns(self, table_name):
        """
        :param table_name:
        :return: list of columns
        """
        query_col = """SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '""" + table_name.upper() + """'
                    AND TABLE_SCHEMA = '""" + self.schema + """'
                    ORDER BY ORDINAL_POSITION"""

        # print(f'finding columns : {table_name} ')
        res = self.execute(query_col)
        columns = [i[0] for i in res]
        return columns

    @staticmethod
    def add_missing_columns(df, columns):
        """Formats the DF and adds missing columns"""
        print(f'total records in df :{len(df)}')
        # adding missing columns
        df_columns = list(df.columns.values)
        table_columns = columns

        col_not_in_df = set(table_columns) - set(df_columns)
        print(f'missing columns from df : {col_not_in_df}')
        for col in col_not_in_df:
            df[col] = ''
        df = df[table_columns]
        print(f'added missing columns to df')
        print(f'final df col length : {len(df.columns)}')
        return df

    def is_table_exists(self, table):
        sql = f"""SELECT EXISTS (
                   SELECT * FROM information_schema.tables 
                       WHERE  table_schema = '{self.schema}'
                       AND    table_name   = '{table.upper()}'
               );"""
        is_exists = self.execute(sql)[0][0]
        return is_exists

    def concurrent_insert(self, frame, table_name, columns):
        # remove nan, null, quotes from df
        frame.fillna(value=np.nan, inplace=True)
        frame.fillna('', inplace=True)
        frame.replace({"'": '`'}, regex=True, inplace=True)
        frame_list = frame.values.tolist()

        template_query = 'INSERT INTO ' + self.schema + '.' + table_name + \
                         ' (' + ','.join(columns) + ') VALUES '

        for row in frame_list:
            # format list of lines from frame_list
            sub_query = str(tuple(row)).replace("\'\'", 'NULL') + ','
            template_query += sub_query

        query = template_query[:-1]
        # print(query)
        result = self.execute(query)
        num_rows = result[0][0]
        return num_rows

    def cursor_writer(self, data, table_name, columns=[]):
        """
        :param data: data-frame
        :param table_name:
        :param columns: optional
        :return: num of records inserted
        """
        data.columns = map(str.upper, data.columns)

        if len(data) == 0:
            return 0, 0

        if not columns:
            columns = self.get_columns(table_name)
        try:
            num_chunks, list_of_df = self.split_df(data[columns], chunk_size=self.chunk_size)
        except KeyError:
            data = self.add_missing_columns(data, columns)
            num_chunks, list_of_df = self.split_df(data[columns], chunk_size=self.chunk_size)

        num_rows = 0
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Start the load operations and mark each future with its df
            future_to_db = \
                {executor.submit(self.concurrent_insert, frame, table_name, columns): frame for frame in list_of_df}

            for future in concurrent.futures.as_completed(future_to_db):
                frame = future_to_db[future]
                try:
                    res = future.result()
                    num_rows = num_rows + res
                    # print(f'printing data : {res}')
                except Exception as exc:
                    print('%r generated an exception: %s' % (frame, exc))

        return num_chunks, num_rows

    def insert_into_table(self, schema, table_name, data, columns=[]):
        """
        :param schema:
        :param table_name:
        :param data:
        :param columns:
        :return: num of records inserted
        """
        if self.connection.schema != schema:
            self.execute('USE SCHEMA ' + schema)

        if type(data) == type(pd.DataFrame()):
            data_list = data.values.tolist()
        else:
            data_list = data

        if type(data_list) != type(list()):
            raise Exception('data parameter is incompatible. Expected pandas.DataFrame or list')

        if not columns:
            query_col = self.get_columns(table_name)
            print(f'get columns from {table_name}. Query used : {query_col} ')
            res = self.execute(query_col)
            columns = [i[0] for i in res]
            columns = columns[:-1]

        if not columns:
            template_query = 'INSERT INTO ' + schema + '.' + table_name + ' VALUES '
        else:
            template_query = 'INSERT INTO ' + schema + '.' + table_name + ' (' + ','.join(columns) + ') VALUES '
        query = template_query

        if len(data_list) == 0:
            return 0

        for row in data_list:
            sub_query = '(' + ','.join([("'" + str(i).strip().replace("'", "`") + "'") if (
                    (str(i).strip() != '') and (str(i).strip() != 'nan') and
                    (str(i).strip().lower() != 'none')) else 'null' for i in row.values()]) + '),'
            # print(f'subquery {sub_query}')

            query += sub_query

        query = query[:-1] + ';'

        result = self.execute(query)
        result = result[0][0]
        return result

    def upsert_into_table(self, schema, table_name, data, columns=[]):
            """
            :param schema:
            :param table_name:
            :param data:
            :param columns:
            :return: num of records upserted
            """
            if self.connection.schema != schema:
                self.execute('USE SCHEMA ' + schema)

            if type(data) == type(pd.DataFrame()):
                data_list = data.values.tolist()
            else:
                data_list = data

            if type(data_list) != type(list()):
                raise Exception('data parameter is incompatible. Expected pandas.DataFrame or list')

            if not columns:
                query_col = self.get_columns(table_name)
                print(f'get columns from {table_name}. Query used : {query_col} ')
                res = self.execute(query_col)
                columns = [i[0] for i in res]
                columns = columns[:-1]

            # https://dev.mysql.com/doc/refman/5.7/en/replace.html
            if not columns:
                template_query = 'REPLACE INTO ' + schema + '.' + table_name + ' VALUES '
            else:
                template_query = 'REPLACE INTO ' + schema + '.' + table_name + ' (' + ','.join(columns) + ') VALUES '
            query = template_query

            if len(data_list) == 0:
                return 0

            for row in data_list:
                sub_query_1 = '(' + ','.join([("'" + str(i).strip().replace("'", "`") + "'") if (
                        (str(i).strip() != '') and (str(i).strip() != 'nan') and
                        (str(i).strip().lower() != 'none')) else 'null' for i in row.values()]) + ')'

                query += sub_query_1

            query = query[:-1] + ';'

            result = self.execute(query)
            result = result[0][0]
            return result

    def close(self):
        # close connection
        self.connection.close()
