import numpy as np
from pandas import DataFrame
from collections import defaultdict


class VW_Formatter():
    def __init__(self):
        pass

    @staticmethod
    def get_feature_types(data_frame, feature_names):
        feature_types = {}
        for feature_name in feature_names:
            if data_frame[feature_name].dtype == np.float64:
                feature_types[feature_name] = 'numerical'
            else:
                feature_types[feature_name] = 'categorical'
        return feature_types

    @staticmethod
    def can_vw_format(data_frame, feature_names):
        return all(feature in data_frame.keys() for feature in feature_names + ['label'])

    @staticmethod
    def to_vw_format_data_frame(data_frame, features):
        if isinstance(data_frame, DataFrame):
            feature_types = VW_Formatter.get_feature_types(data_frame, features)
            data = defaultdict(list)

            def vw_format(value):
                prepend = column + '_value:' if column != 'label' and feature_types[column] == 'numerical' else ''
                data[column].append({
                    'label': str(value) + ' '
                }.get(column, str(column) + ' ' + prepend + str(value) + ' '))

            if VW_Formatter.can_vw_format(data_frame, features):
                for column in ['label'] + features:
                    data_frame[column].apply(vw_format)
                return DataFrame(data=data, dtype=object)
            else:
                raise ValueError('Either feature or label not cached in data_frame')
        else:
            raise TypeError('Argument data_frame must be instance of Pandas DataFrame')

    @staticmethod
    def to_vw_format_file(data_frame, features, output_path,  mode):
        VW_Formatter.to_vw_format_data_frame(data_frame, features)[['label'] + features].to_csv(output_path, header=False, sep='|', index=False, mode=mode)

