from vertica_python import connect
from Experiment.Constants import VERTICA_CONNECTION
from utilities import get_column_names_from_sql_query
from pandas import DataFrame
import sqlite3
import os


class DatabaseManager():
    def __init__(self, filename, cache=True, connection_details=VERTICA_CONNECTION):
        self.filename = filename
        if not cache and os.path.isfile(self.datafile):
            os.remove(self.datafile)
        self.db = sqlite3.connect(filename)
        self.init_db()
        self.connection_details = connection_details

    @staticmethod
    def get_feature_table_name(data_set, feature_name):
        return data_set+'_feature_'+feature_name

    def init_db(self):
        ddl = "CREATE TABLE IF NOT EXISTS metadata(type BLOB, subtype BLOB, value BLOB);"
        self.db.cursor().execute(ddl)
        self.db.commit()

    def set_meta_data(self, type_name, sub_type_name, value):
        self.delete_meta_data(type_name, sub_type_name)
        dml = "INSERT INTO metadata('type', 'subtype', 'value') VALUES(?,?,?);"
        self.db.execute(dml, (type_name, sub_type_name, value))
        self.db.commit()

    def get_meta_data(self, type_name, sub_type_name):
        dml = "SELECT value FROM metadata WHERE type='{type_name}' AND subtype='{sub_type_name}'".format(type_name=type_name, sub_type_name=sub_type_name)
        value = self.db.execute(dml).fetchone()
        return None if value is None else value[0]

    def delete_meta_data(self, type_name, sub_type_name):
        dml1 = "DELETE FROM metadata WHERE type='{type_name}' AND subtype='{sub_type_name}'".format(type_name=type_name, sub_type_name=sub_type_name)
        self.db.execute(dml1)
        self.db.commit()

    def data_set_has_feature(self, data_set, key):
        features_table = self.get_feature_table_name(data_set, key)
        table_names = [name[0] for name in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        return features_table in table_names

    def get_data_set_feature(self, data_set, feature_name, limit=None, offset=None):
        if self.data_set_has_feature(data_set, feature_name):
            features_table_name = self.get_feature_table_name(data_set, feature_name)
            sql = "SELECT auto_id, IFNULL(value, 'NULL') FROM \"{name}\" ORDER BY 'auto_id'".format(name=features_table_name)
            if isinstance(limit, int):
                sql += ' LIMIT ' + str(limit)
            if isinstance(offset, int):
                sql += ' OFFSET ' + str(offset)

            values = [[item[0], item[1]] for item in self.db.execute(sql).fetchall()]
            return DataFrame(values, columns=['auto_id', feature_name])
        else:
            return None

    def set_data_set_feature(self, data_set, feature_name, df):
        if isinstance(df, DataFrame):
            feature_table_name = self.get_feature_table_name(data_set, feature_name)
            tmp_table_name = 'tmp_' + feature_table_name
            self.db.execute("DROP TABLE IF EXISTS \"{name}\"".format(name=tmp_table_name))
            self.db.execute("CREATE TABLE \"{name}\"(auto_id INTEGER, value BLOB)".format(name=tmp_table_name))
            values = [(value[0], value[1],) for value in df[['auto_id', feature_name]].values]
            self.insert_rows_to_table(tmp_table_name, ['auto_id', 'value'], values)
            self.db.execute("DROP TABLE IF EXISTS \"{name}\"".format(name=feature_table_name))
            self.db.execute("ALTER TABLE \"{tmp_name}\" RENAME TO \"{name}\"".format(tmp_name=tmp_table_name, name=feature_table_name))
            self.db.commit()

    def delete_data_set_feature(self, data_set, feature_name):
        feature_table_name = self.get_feature_table_name(data_set, feature_name)
        self.db.execute("DROP TABLE IF EXISTS \"{name}\"".format(name=feature_table_name))
        self.db.commit()

    def get_data_set_list(self):
        return [name[0] for name in self.db.execute("SELECT type FROM metadata WHERE subtype='query'").fetchall()]

    def create_table(self, table_name, columns):
        columns_with_types = ','.join(['auto_id INTEGER PRIMARY KEY AUTOINCREMENT'] + [column + " BLOB" for column in columns])
        ddl1 = "DROP TABLE IF EXISTS \"{name}\"".format(name=table_name)
        ddl2 = "CREATE TABLE \"{name}\"({columns_with_types})".format(name=table_name, columns_with_types=columns_with_types)
        self.db.cursor().execute(ddl1)
        self.db.cursor().execute(ddl2)
        self.db.commit()

    def insert_rows_to_table(self, table_name, columns, rows):
        values = "VALUES (" + ','.join(len(columns) * ['?']) + ")"
        self.db.cursor().executemany("INSERT INTO \"{name}\"({columns}) {values}".format(name=table_name, columns=','.join(columns), values=values), rows)

    def get_number_of_rows_in_table(self, table_name):
        c = self.db.cursor()
        c.execute("SELECT count(*) FROM \"{name}\"".format(name=table_name))
        num = c.fetchone()[0]
        return num

    def fetch_from_vertica_to_df(self, data_set, query, block_size=100000):
        data_set_query = self.get_meta_data(data_set, 'query')
        if data_set_query != query:
            connection = connect(self.connection_details)
            cursor_remote = connection.cursor()
            print 'Executing ', data_set, 'Query...'
            print query
            columns = get_column_names_from_sql_query(query)
            self.create_table(data_set, columns)
            cursor_remote.execute(query)

            while True:
                rows = cursor_remote.fetchmany(block_size)
                rows = [tuple([str(ele) for ele in row]) for row in rows]
                self.insert_rows_to_table(data_set, columns, rows)
                if len(rows) < block_size:
                    break

            self.db.commit()
            cursor_remote.close()
            if self.get_number_of_rows_in_table(data_set) == 0:
                raise (ValueError('SQL result in empty fetch!!'))
            else:
                self.split_table_into_features(data_set)
                self.set_meta_data(data_set, 'query', query)
                self.set_meta_data(data_set, 'columns', ','.join(columns))
                self.set_meta_data(data_set, 'built_features', '')

    def split_table_into_features(self, table_name):
        columns = list(map(lambda x: x[0], self.db.execute('SELECT * FROM ' + table_name + ' LIMIT 10;').description))
        for column in columns:
            feature_table_name = self.get_feature_table_name(table_name, column)
            self.db.execute("DROP TABLE IF EXISTS " + feature_table_name)
            self.db.execute("CREATE TABLE \"{name}\"(auto_id INTEGER, value BLOB)".format(name=feature_table_name))
            self.db.execute("INSERT INTO \"{name}\" SELECT auto_id, {col_name} FROM \"{base_name}\"".format(name=feature_table_name, col_name=column, base_name=table_name))
            self.db.commit()
        self.db.execute("DROP TABLE IF EXISTS " + table_name)
        self.db.commit()

if __name__ == "__main__":
    dm = DatabaseManager("test.db")
    query_n = "SELECT client_id, visited_domains FROM train.base_training_data WHERE log_time_hour = '2015-06-01 00:00:00'"
    dm.fetch_from_vertica_to_df("test", query_n)
    result = dm.get_data_set_feature("test", "visited_domains")