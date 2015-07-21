from collections import defaultdict
import numpy as np
import pandas
import matplotlib.pylab as plt


class FeatureAnalyzer:
    def __init__(self, experiment_name=''):
        self.experiment_name = experiment_name

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
    def convert_into_one_hot_representation(data_frame, feature_name):
        unique_feature_categories = defaultdict(int)
        data = []

        def find_unique_feature_categories(val):
            row_list = str(val[0]).split(' ')
            data.append(row_list if val[0] != '' else [])
            for part in row_list:
                unique_feature_categories[part] += 1

        data_frame[[feature_name]].apply(find_unique_feature_categories, axis=1)
        one_hot_categories = dict([(key, []) for key in unique_feature_categories.keys()])

        for row in data:
            for category in one_hot_categories:
                if category in row:
                    one_hot_categories[category].append(1)
                else:
                    one_hot_categories[category].append(0)

        df_us = pandas.DataFrame(data=one_hot_categories)

        data = data_frame[['label']].join(df_us)
        return data

    def analyze_categorical_feature(self, data_frame, feature_name):
        data = FeatureAnalyzer.convert_into_one_hot_representation(data_frame, feature_name)
        data.is_copy = False
        data['label'] = data['label'].astype(int).apply(lambda x: 0 if x == -1 else 1)
        result_dict = {'category': [], 'correlation': [], 'abs_correlation': [], 'fraction': [], 'rate': []}
        for key in data.keys():
            if key != 'label':
                result_dict['category'].append(key)
                key_data = data[['label', key]][data[key] == 1]
                num_engagements = len(key_data[key_data['label'] == 1])
                if len(key_data) > 0:
                    result_dict['correlation'].append(data[[key, 'label']].corr().values[0, 1])
                    result_dict['abs_correlation'].append(abs(result_dict['correlation'][-1]))
                    result_dict['fraction'].append(len(key_data)/float(len(data)))
                    result_dict['rate'].append(num_engagements/float(len(key_data)))
                else:
                    result_dict['correlation'].append(0.0)
                    result_dict['abs_correlation'].append(0.0)
                    result_dict['fraction'].append(0.0)
                    result_dict['rate'].append(0.0)

        plt.figure()
        result = pandas.DataFrame(data=result_dict).sort(columns='abs_correlation', ascending=False)
        fig = result.plot(x='category', y=['correlation', 'fraction', 'rate'], kind='bar', stacked=True, figsize=(len(data)/200, 10)).get_figure()
        fig.set_tight_layout(True)
        png_path = 'Data/' + self.experiment_name + '_' + feature_name + '.png'
        fig.savefig(png_path, dpi=fig.dpi)

    def analyze_numerical_feature(self, data_frame, feature_name):
        data = data_frame[['label', feature_name]]
        data.is_copy = False
        data['label'] = data['label'].astype(int).apply(lambda x: 0 if x == -1 else 1)
        pos_data,  neg_data = data[data['label'] == 1][feature_name], data[data['label'] != 1][feature_name]
        fig = plt.figure()
        fig.add_subplot()
        pos_data.hist(alpha=0.5, bins=100, color='g', normed=True, label='positives'), neg_data.hist(alpha=0.5, bins=100, color='r', normed=True, label='negatives')

        fig.set_tight_layout(True)
        png_path = 'Data/' + self.experiment_name + '_' + feature_name + '.png'
        fig.savefig(png_path, dpi=fig.dpi)

    def analyze(self, data_frame, feature_to_analyze):
        feature_type = FeatureAnalyzer.get_feature_types(data_frame, feature_to_analyze)
        for feature in feature_type:
            if feature_type[feature] == 'categorical':
                self.analyze_categorical_feature(data_frame, feature)
                pass
            else:
                self.analyze_numerical_feature(data_frame, feature)
