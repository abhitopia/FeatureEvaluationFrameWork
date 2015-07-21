import re
import copy

class DataConfig():
    def __init__(self, data_config):
        self.config = data_config

    @staticmethod
    def format_sql_query(query_template, tags_dict):
        query = copy.deepcopy(query_template)
        tags_to_fill = re.findall(r'{(\w+)}', query)
        for tag in tags_to_fill:
            query = query.replace('{' + tag + '}', tags_dict[tag])
        return query

    def can_format_data_config(self):
        tags_to_fill = set(re.findall(r'{(\w+)}', self.config['select_sql']))
        return all(len(tags_to_fill - set(value.keys())) == 0 for data_set, value in self.config if data_set != 'select_sql')

    def has_valid_select_sql(self):
        if isinstance(self.config, dict) and 'select_sql' in self.config:
            if isinstance(self.config['select_sql'], str):
                return True

    def has_valid_data_sets(self):
        if isinstance(self.config, dict):
            return all(isinstance(value, dict) for data_set, value in self.config.items() if data_set != 'select_sql')

    def has_valid_data_config(self):

        if self.has_valid_data_sets() and self.has_valid_select_sql():

                return True
        return False

    def parse_data_config(self):
        if self.has_valid_data_config():
            select_sql = self.config['select_sql']
            del self.config['select_sql']
            for data_set, tags in self.config.items():
                self.config[data_set] = self.format_sql_query(select_sql, tags)
        else:
            raise (ValueError('Experimenter: data_config parameter is ill defined'))