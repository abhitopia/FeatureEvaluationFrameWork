import itertools
from pandas import DataFrame
import random
import gc


class FeatureBuilder():
    def __init__(self, data_manager, feature_definitions, aggregate_value_definitions, delimiter=';',
                 null_words=['N/A', 'NULL', 'nan'], cache=True):
        self.data_manager = data_manager
        self.delimiter = delimiter
        self.feature_definitions = feature_definitions
        self.aggregate_value_definitions = aggregate_value_definitions
        self.null_words = null_words
        self.cache = cache
        self.data_set = None

    def split_data_into_features(self):
        data_sets = self.data_manager.get_data_set_list()
        for data_set in data_sets:
            self.data_set = data_set
            data_frame = self.data_manager.get_data_set_attribute(data_set, 'data')
            if data_frame is not None:
                for feature_name in data_frame.keys():
                    if not self.is_feature_built(feature_name):
                        feature = data_frame[feature_name]
                        feature.name = feature_name
                        self.data_manager.set_data_set_attribute(data_set, feature_name, feature)

    def load_feature(self, feature_name):
        if self.is_aggregate_feature(feature_name):
            return self.data_manager.get_meta_data('aggregate_feature', feature_name)
        else:
            print 'Loading feature:', feature_name
            return self.data_manager.get_data_set_feature(self.data_set, feature_name).sort(['auto_id'])

    def dump_feature(self, feature_name, feature_value):
        if self.is_aggregate_feature(feature_name):
            self.data_manager.set_meta_data('aggregate_feature', feature_name, feature_value)
        else:
            self.data_manager.set_data_set_feature(self.data_set, feature_name, feature_value)
            built_features = self.data_manager.get_meta_data(self.data_set, 'built_features').split(',')
            built_features.append(feature_name)
            self.data_manager.set_meta_data(self.data_set, 'built_features', ','.join(built_features))

    def get_features_data_frame(self, data_set, feature_names):
        data = {}
        for part in feature_names:
            data[part] = self.data_manager.get_data_set_feature(data_set, part).sort(['auto_id'])[part]

        data['auto_id'] = self.data_manager.get_data_set_feature(data_set, feature_names[0]).sort(['auto_id'])['auto_id']
        return DataFrame(data=data)

    def get_feature_parts(self, feature_name):
        return feature_name.split(self.delimiter)

    def is_numerical_feature(self, feature_name, feature_value, num_samples=100):
        num_samples = min(len(feature_value[feature_name]), num_samples)
        indices = random.sample(range(0, len(feature_value[feature_name])), num_samples)
        sample = feature_value[feature_name][indices][feature_value[feature_name][indices] != '']
        if sample.convert_objects(convert_numeric=True, convert_dates=False)._is_numeric_mixed_type:
            idx = sample.str.contains('^[-]*[0-9]+\.[0-9]+$', regex=True, na=False)
            return idx[idx].count() > 0 and sample.count() > 0

    def post_process(self, feature_name, feature_value):
        if not self.is_aggregate_feature(feature_name):
            feature_value[feature_name] = feature_value[feature_name].str.replace(
                r'|'.join(self.null_words), '')
            if self.is_numerical_feature(feature_name, feature_value):
                feature_value[feature_name] = feature_value[feature_name].convert_objects(
                    convert_numeric=True)
        return feature_value

    def get_dependency_features(self, feature_name):
        if self.is_interaction_feature(feature_name):
            return self.get_feature_parts(feature_name)
        elif self.is_custom_feature(feature_name) or self.is_aggregate_feature(feature_name):
            custom_feature_builder = self.feature_definitions[feature_name] if self.is_custom_feature(feature_name) else \
                self.aggregate_value_definitions[feature_name]
            num_args, name_args = custom_feature_builder.func_code.co_argcount, \
                                  custom_feature_builder.func_code.co_varnames
            return [col for col in name_args[0:num_args]]
        else:
            return []

    def build_dependency_features(self, feature_name):
        for feature_name in self.get_dependency_features(feature_name):
            self.build_feature(feature_name)

    def is_interaction_feature(self, feature_name):
        return len(self.get_feature_parts(feature_name)) > 1

    def can_make_interaction_feature(self, feature_name):
        if not self.is_interaction_feature(feature_name):
            return False
        return all(self.exists(part) for part in self.get_feature_parts(feature_name))

    def build_interaction_feature(self, feature_name):
        print 'Building Feature/Value:', feature_name

        def interaction_feature_join(row):
            if set(['']) & set(row) == 1:
                return ''
            else:
                return ' '.join(
                    [';'.join(elem) for elem in list(itertools.product(*[str(col).split() for col in row]))])

        if self.can_make_interaction_feature(feature_name):
            feature_parts = self.get_feature_parts(feature_name)
            df = self.get_features_data_frame(self.data_set, feature_parts)
            feature_value = df[feature_parts].apply(interaction_feature_join, axis=1)
            return DataFrame(data={'auto_id': df['auto_id'], feature_name: feature_value})

    def is_aggregate_feature(self, feature_name):
        if feature_name in self.aggregate_value_definitions:
            custom_feature_builder = self.aggregate_value_definitions[feature_name]
            if not (self.is_interaction_feature(feature_name) or self.is_custom_feature(
                    feature_name)):
                if custom_feature_builder is not None and callable(custom_feature_builder):
                    return True
        return False

    def can_make_aggregate_feature(self, feature_name):
        if self.is_aggregate_feature(feature_name):
            custom_feature_builder = self.aggregate_value_definitions[feature_name]
            num_args = custom_feature_builder.func_code.co_argcount
            name_args = custom_feature_builder.func_code.co_varnames
            return True if all(self.exists(feature_name=arg) for arg in name_args[0:num_args]) else False
        else:
            return False

    def build_aggregate_feature(self, feature_name):
        print 'Building Feature/Value:', feature_name
        if self.can_make_aggregate_feature(feature_name):
            cached_columns = self.get_dependency_features(feature_name)
            df = self.get_features_data_frame(self.data_set, cached_columns)

            def custom_func():
                arg_list = dict([(col_name, df[col_name]) for col_name in cached_columns])
                return self.aggregate_value_definitions[feature_name](**arg_list)

            feature_value = custom_func()
            return feature_value

    def is_custom_feature(self, feature_name):
        if feature_name in self.feature_definitions:
            custom_feature_builder = self.feature_definitions[feature_name]
            if not self.is_interaction_feature(feature_name) and not self.exists(feature_name):
                if custom_feature_builder is not None and callable(custom_feature_builder):
                    return True
        return False

    def can_make_custom_feature(self, feature_name):
        if self.is_custom_feature(feature_name):
            custom_feature_builder = self.feature_definitions[feature_name]
            num_args = custom_feature_builder.func_code.co_argcount
            name_args = custom_feature_builder.func_code.co_varnames
            return True if all(self.is_feature_built(arg) for arg in name_args[0:num_args]) else False
        else:
            return False

    def build_custom_feature(self, feature_name):
        print 'Building Feature/Value:', feature_name
        if self.can_make_custom_feature(feature_name):
            cached_columns = self.get_dependency_features(feature_name)
            aggregate_feature_names = list(set(cached_columns) & set(self.aggregate_value_definitions.keys()))
            cached_columns = list(set(cached_columns) - set(aggregate_feature_names))
            aggregate_feature_values = {}
            for agg_feature in aggregate_feature_names:
                aggregate_feature_values[agg_feature] = self.data_manager.get_meta_data('aggregate_feature', agg_feature)

            def custom_func(row):
                arg_list = dict([(col_name, row[cached_columns.index(col_name)]) for col_name in cached_columns])
                arg_list.update(dict(
                    [(agg_val, aggregate_feature_values[agg_val]) for agg_val in
                    aggregate_feature_names]))
                return str(self.feature_definitions[feature_name](**arg_list))

            df = self.get_features_data_frame(self.data_set, cached_columns)
            feature_value = df[cached_columns].apply(custom_func, axis=1)
            return DataFrame(data={'auto_id': df['auto_id'], feature_name: feature_value})

    def exists(self, feature_name):
        if self.is_aggregate_feature(feature_name):
            return self.data_manager.get_meta_data('aggregate_feature', feature_name) is not None
        else:
            return self.data_manager.data_set_has_feature(self.data_set, feature_name)

    def is_feature_built(self, feature_name):
        if self.is_aggregate_feature(feature_name):
            return self.exists(feature_name)
        else:
            built_features = self.data_manager.get_meta_data(self.data_set, 'built_features').split(',')
            return feature_name in built_features

    def remove_feature(self, feature_name):
        self.data_manager.remove_attribute(self.data_set, feature_name)
        if self.is_aggregate_feature(feature_name):
            self.data_manager.remove_attribute('aggregate_feature', feature_name)
        else:
            built_features = self.data_manager.get_data_set_attribute(self.data_set, 'built_features')
            if feature_name in built_features:
                built_features.remove(feature_name)
            self.data_manager.set_data_set_attribute(self.data_set, 'built_features', feature_name)

    def build_feature(self, feature_name):
        if not self.is_feature_built(feature_name):
            self.build_dependency_features(feature_name)
            if self.can_make_interaction_feature(feature_name):
                feature_value = self.build_interaction_feature(feature_name)
            elif self.can_make_custom_feature(feature_name):
                feature_value = self.build_custom_feature(feature_name)
            elif self.can_make_aggregate_feature(feature_name):
                feature_value = self.build_aggregate_feature(feature_name)
            else:
                feature_value = self.load_feature(feature_name)
                print 'Building Feature/Value:', feature_name
            feature_value = self.post_process(feature_name, feature_value)
            self.dump_feature(feature_name, feature_value)
            gc.collect()
            if not self.is_feature_built(feature_name):
                raise (ValueError('Could not make feature/value:', feature_name))
        else:
            print 'Using cache:', feature_name

    def get_feature(self, feature_name):
        return self.data_frame[feature_name] if self.exists(feature_name) else None

    def build_features(self):

        data_sets = self.data_manager.get_data_set_list()
        if 'train' in data_sets:
            data_sets.remove('train')
            data_sets = ['train'] + data_sets

        for data_set in data_sets:
            self.data_set = data_set
            print 'Building/Loading Cached features for data set:', data_set
            features = dict(self.feature_definitions, **self.aggregate_value_definitions)
            for feature_name in features:
                if not self.cache and feature_name not in self.data_manager.get_meta_data(self.data_set, 'columns').split(','):
                    self.data_manager.delete_data_set_feature(self.data_set, feature_name)

            for feature_name in features:
                self.build_feature(feature_name)