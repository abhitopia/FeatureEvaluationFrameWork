from vertica_python import connect
from Experiment.Constants import VERTICA_CONNECTION
from Experiment.MetadataDataFrame import MetadataDataFrame
from utilities import get_column_names_from_sql_query
from shove import Shove
import gc

class DataManager():
    def __init__(self, filename, connection_details=VERTICA_CONNECTION):
        self.filename = filename
        self.store = Shove('file://'+self.filename, 'memory://', optimize=False)
        self.connection_details = connection_details

    @staticmethod
    def get_attribute_name(data_set, attribute):
        return '_' + data_set + '_' + attribute

    def get_data_set_attribute(self, data_set, key):
        attrib_name = self.get_attribute_name(data_set, key)
        if self.has_attribute(data_set, key):
            return self.store[attrib_name]
        else:
            return None
        self.store.close()
        gc.collect()
        self.store = Shove('file://'+self.filename, 'memory://', optimize=False)

    def set_data_set_attribute(self, data_set, key, value):
        attrib_name = self.get_attribute_name(data_set, key)
        self.store.update({attrib_name: value})
        self.store.sync()
        self.store.close()
        gc.collect()
        self.store = Shove('file://'+self.filename, 'memory://', optimize=False)

    def has_attribute(self, data_set, key):
        attrib_name = self.get_attribute_name(data_set, key)
        if attrib_name in self.store.keys():
            return True
        else:
            return False

    def remove_attribute(self, data_set, key):
        attrib_name = self.get_attribute_name(data_set, key)
        if attrib_name in self.store:
            del self.store[attrib_name]
            self.store.sync()

    def get_data_set_list(self):
        data_sets = [key.split('_')[1] for key in self.store.keys() if '_query' in key]
        return data_sets

    def fetch_from_vertica_to_df(self, data_set, query):
        data_set_query = self.get_data_set_attribute(data_set, 'query')
        if data_set_query != query:
            connection = connect(self.connection_details)
            cursor = connection.cursor()
            print 'Executing ', data_set, 'Query...'
            print query
            columns = get_column_names_from_sql_query(query)
            cursor.execute(query)

            data = []
            while True:
                rows = cursor.fetchmany(10000)
                data.extend([[str(ele) for ele in row] for row in rows])
                if len(rows) <= 1:
                    break

            df = MetadataDataFrame(data=data, columns=columns, meta_info={'query': query, 'built_features': [], 'aggregate_values': {},
                                                               'columns': columns})

            cursor.close()
            if len(df) == 0:
                raise(ValueError('SQL result in empty fetch!!'))
            else:
                self.set_data_set_attribute(data_set, 'data', df)
                self.set_data_set_attribute(data_set, 'query', query)
                self.set_data_set_attribute(data_set, 'columns', columns)
                self.set_data_set_attribute(data_set, 'built_features', [])