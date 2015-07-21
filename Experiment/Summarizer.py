import hashlib
import os
import operator
from Experiment.MetricEvaluation import MetricEvaluation
from Experiment.Constants import COLOR_MAP
from Experiment.MetadataDataFrame import MetadataDataFrame
from matplotlib.patches import Rectangle
import matplotlib.pyplot as plt
from collections import defaultdict
import itertools
import numpy as np
import cPickle as Pickle


class Summarizer:
    def __init__(self, datafile, cache=True, model_config=None):
        self.datafile = datafile
        if not cache and os.path.isfile(self.datafile):
            os.remove(self.datafile)
        self.variable_index = 0
        self.meta_info = None
        self.result = None
        self.parse_configs(model_config)
        self.load_results()
        self.fill_result_with_new_features()

    def parse_configs(self, model_config):
        self.meta_info = None
        if model_config:
            feature_names = list(set([feature_name for model in model_config.values() for feature_name in
                                model.get('features', {}).keys()]))
            greedy_feature_names = list(set([feature_name for model in model_config.values() for feature_name in
                                    model.get('greedy features', {}).keys()]))

            learner_names = list(set([learner_name for model in model_config.values() for learner_name in model['learners'].keys()]))
            learner_params_rows = [param_list for model in model_config.values() for param_list in model['learners'].values()]
            learner_params_rows = list(itertools.chain.from_iterable(learner_params_rows))
            learner_parameters = list(set([param_name for row in learner_params_rows for param_name in row.keys()]))

            self.meta_info = {
                'metrics': MetricEvaluation.current_metrics(),
                'model_names': model_config.keys(),
                'learner_names': learner_names,
                'learner_parameters': learner_parameters,
                'features': feature_names + greedy_feature_names
            }
        else:
            print 'Summary Mode Active'

    def load_results(self):
        if os.path.isfile(self.datafile):
            self.result = Pickle.load(open(self.datafile, 'rb'))
        elif self.meta_info is not None:
            columns = ['id', 'model', 'learner'] + list(set(self.meta_info['features'])) + self.meta_info['metrics'] + self.meta_info[
                'learner_parameters']
            self.result = MetadataDataFrame(columns=columns, meta_info=self.meta_info)
        else:
            raise (RuntimeError('No results exist to Summarize!!'))

    def fill_result_with_new_features(self):
        meta_info = self.result.meta_info
        columns = ['model', 'learner'] + list(set(meta_info['features'])) + meta_info['metrics'] + meta_info[
            'learner_parameters']
        if set(self.result.keys()) < set(columns):
            for column in set(columns).difference(set(self.result.keys())):
                self.result[column] = 'absent'

    @staticmethod
    def get_experiment_id(model_name, learner_name, features, params):
        h_str = ''.join(sorted([model_name, learner_name] + features + [':'.join([str(k), str(v)]) for k, v in params.items()]))
        return int(int(hashlib.sha1(h_str).hexdigest(), 16) % (10 ** 8))

    def get_row_dict(self, model_name, learner_name, features, params, metrics):
        row = None
        if isinstance(model_name, str) and isinstance(learner_name, str):
            row = {'model': model_name, 'learner': learner_name}
            if isinstance(features, list) and all(isinstance(value, str) for value in features):
                for feature in self.result.meta_info['features']:
                    row[feature] = 'present' if feature in features else 'absent'

                if isinstance(params, dict) and len(params) > 0:
                    for param in self.result.meta_info['learner_parameters']:
                        row[param] = params[param] if param in params else ''

                    if isinstance(metrics, dict) and len(metrics) > 0:
                        for metric in self.result.meta_info['metrics']:
                            row[metric] = metrics[metric]
                        row['id'] = self.get_experiment_id(model_name, learner_name, features, params)
                        return row
        if row is None:
            raise(ValueError("Can't add to results, inconsistent arguments!!"))
        else:
            return row

    def check_if_exists(self, model_name, learner_name, features, params):
        experiment_id = self.get_experiment_id(model_name, learner_name, features, params)
        result = self.result[self.result['id'] == experiment_id]
        return len(result) > 0

    def get_metrics(self, model_name, learner_name, features, params):
        experiment_id = self.get_experiment_id(model_name, learner_name, features, params)
        result = self.result[self.result.meta_info['metrics']][self.result['id'] == experiment_id]
        return dict(zip(self.result.meta_info['metrics'], result.values[0]))

    def add_metrics(self, model_name, learner_name, features, params_list, metrics_list):
        rows = defaultdict(list)
        for params, metrics in zip(params_list, metrics_list):
            if not self.check_if_exists(model_name, learner_name, features, params):
                row = self.get_row_dict(model_name, learner_name, features, params, metrics)
                [rows[key].append(value) for key, value in row.items()]

        self.result = self.result.append(MetadataDataFrame(data=rows), ignore_index=True)
        Pickle.dump(self.result, open(self.datafile, 'wb'))

    def plot_results(self):
        group_by_keys = ['model', 'learner'] + list(set(self.result.meta_info['features']))
        result = MetadataDataFrame(columns=group_by_keys)
        for grouped_by, data_frame in self.result.groupby(group_by_keys):
            result = result.append(data_frame[group_by_keys+['AUC']][data_frame['AUC'] == data_frame['AUC'].max()], ignore_index=True)

        result = result.sort(['AUC'], ascending=[1]).reset_index(drop=True)

        result_list = []
        for model in result['model'].unique():
            result_model = result[result['model'] == model]
            rows_to_keep = [True]
            index = result_model.index.tolist()
            if len(result_model) > 1:
                base = result_model['AUC'][index[0]]
                for i in range(1, len(result_model)):
                    if result_model['AUC'][index[i]] > base + 0.0001:
                        rows_to_keep.append(True)
                        base = result_model['AUC'][index[i]]
                    else:
                        rows_to_keep.append(False)
            result_list.append(result_model[rows_to_keep])

        result = result_list[0]
        if len(result_list) > 1:
            for i in range(1, len(result_list)):
                result = result.append(result_list[i], ignore_index=True)

        result = result.sort(['AUC'], ascending=[1]).reset_index(drop=True)

        if len(result) == 1:
            return

        key_freq = defaultdict(int)
        for key in result.keys():
            distinct_values = self.result[key].unique()
            if len(distinct_values) == 1:
                del result[key]
            else:
                for val in distinct_values:
                    key_freq[key] = max(key_freq[key], len(self.result[key][self.result[key] == val]))

        sorted_keys = [key[0] for key in sorted(key_freq.items(), key=operator.itemgetter(1))]
        group_by_keys = [key for key in sorted_keys if key in group_by_keys]
        default_annotation = {'weight': 'bold', 'ha': 'center', 'va': 'center'}

        fig = plt.figure(figsize=(len(result.keys())/2, len(result)/2))
        plt.subplots_adjust(left=0.1, right=1.0, bottom=0.0, top=1.0, wspace=0.2, hspace=0.0)

        index = 0
        for key in group_by_keys:
            sorted_values = result[key].tolist()
            color_map, color_map_r = COLOR_MAP.next()
            width = 3 if key in ['model', 'learner'] else 1
            for i in range(len(result)):
                if result[key][i] == 'present':
                    plt.gca().add_patch(Rectangle(xy=(index, i), facecolor='green', width=width, height=1, alpha=0.5))

                elif result[key][i] == 'absent':
                    plt.gca().add_patch(Rectangle(xy=(index, i), facecolor='red', width=width, height=1, alpha=0.5))
                else:
                    color_value = color_map(sorted_values.index(result[key][i]) / float(len(result) - 1))
                    color_value_r = color_map_r(sorted_values.index(result[key][i]) / float(len(result) - 1))
                    default_annotation.update({'rotation': '00', 'fontsize': 8})
                    plt.gca().add_patch(Rectangle(xy=(index, i), facecolor=color_value, width=width, height=1))
                    plt.gca().annotate(sorted_values[i], (index++float(width)/2, i+0.5), color=color_value_r, **default_annotation)

            default_annotation.update({'rotation': '90', 'fontsize': 9})
            plt.gca().add_patch(Rectangle(xy=(index, len(result)), facecolor=color_map(1), width=width, height=15))
            plt.gca().annotate(key, (index+float(width)/2, len(result) + 7.5), color='black', **default_annotation)
            index += width

        plt.ylim([0, len(result)+15])
        plt.xlim([0, index])
        ticks = np.linspace(start=0.5, stop=len(result)-0.5, endpoint=len(result)-0.5, num=len(result))
        labels = [round(value, 4) for value in result['AUC'].tolist()]
        plt.gca().get_yaxis().set_ticks(ticks)
        plt.ylabel('AUC')
        plt.gca().get_yaxis().set_ticklabels(labels)

        png_path = '.'.join(self.datafile.split('.')[0:-1]) + '.png'
        fig.savefig(png_path, dpi=fig.dpi)
        print 'Summary saved as', png_path

    def summarize(self):
        self.plot_results()
