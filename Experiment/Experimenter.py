import hashlib
import os
from Experiment.FeatureBuilder import FeatureBuilder
from Experiment.DatabaseManager import DatabaseManager
from Experiment.Learner import Learner
from Experiment.Metric import Metric
from Experiment.Summarizer import Summarizer
from Experiment.Constants import VERTICA_CONNECTION
from Experiment.DataConfig import DataConfig
from Experiment.ModelConfig import ModelConfig
from Experiment.MetricEvaluation import MetricEvaluation
from Experiment.FeatureAnalyzer import FeatureAnalyzer
from Experiment.utilities import make_path
import time
from shutil import rmtree


class Experimenter():
    def __init__(self, name, data_config, model_config=None,
                 connection_details=VERTICA_CONNECTION, greedy_criterion='AUC', greed_level=1, threads=1,
                 work_dir=None, cache_data=True, cache_results=True, cache_features=True):
        self.experiment_name = name
        self.work_dir = work_dir if work_dir is not None else os.path.join('/tmp', self.experiment_name)
        rmtree(self.work_dir, ignore_errors=True), make_path(self.work_dir)
        self.data_config = DataConfig(data_config)
        self.data_config.parse_data_config()
        self.threads = threads if isinstance(threads, int) and threads >= 1 else 1
        self.greed_level = greed_level if isinstance(greed_level, int) and greed_level >= 1 else 1

        self.datafile_name = './Data/' + self.experiment_name + '.db'
        self.result_file = './Data/' + self.experiment_name + '_result.pkl'
        make_path('./Data')

        self.data_manager = DatabaseManager(self.datafile_name, cache=cache_data, connection_details=connection_details)
        self.model_config = None
        self.train_test_files_generated = False
        self.feature_analyser = FeatureAnalyzer(experiment_name=name)
        if model_config:
            self.model_config = ModelConfig(model_config)
            self.model_config.parse_model_config()
            self.feature_builder = FeatureBuilder(self.data_manager, feature_definitions=self.model_config.feature_definitions,
                                                  aggregate_value_definitions=self.model_config.aggregate_value_definitions,
                                                  cache=cache_features)
        else:
            print 'Only Fetching Data'
            return

        if 'train' in self.data_config.config and 'test' in self.data_config.config:
            self.greed_criterion = greedy_criterion
            self.metric_evaluator = MetricEvaluation
            self.learner = Learner(self.work_dir, self.data_manager)
            self.summarizer = Summarizer(model_config=model_config, datafile=self.result_file, cache=cache_results)
        else:
            print 'Fetching Data and Constructing Features..'
            return

    def manage_data_files(self, model_name, features_used, learner_name, option='generate'):
        h_str = ''.join(sorted([model_name, learner_name] + features_used))
        file_id = int(int(hashlib.sha1(h_str).hexdigest(), 16) % (10 ** 8))
        data_file_paths = {}
        if 'train' in self.data_config.config.keys() and 'test' in self.data_config.config.keys():
            for data_set in ['train', 'test']:
                data_file_paths[data_set] = os.path.join(self.work_dir, str(file_id) + '_' + data_set)
                file_path = data_file_paths[data_set]
                if not os.path.isfile(file_path) and option == 'generate':
                    self.learner.get_formatted_data_set(learner_name, data_set, features_used, file_path)
                    print 'Generated:', file_path
                elif option == 'remove' and os.path.isfile(file_path):
                    os.remove(file_path)
                    os.remove(file_path + '.cache')
                    print 'Removed:', file_path + '(.cache)'
        return data_file_paths

    def run_model(self, model_name, features_used, learner_name, learner_params):
        min_metric = Metric(metric_name=self.greed_criterion)
        num_params = len(learner_params)
        labels = self.data_manager.get_data_set_feature('test', 'label')['label'].values.astype(int)
        for thread_num in range(0, num_params, self.threads):
            start, stop = thread_num, min(thread_num + self.threads, num_params)
            print 'Processing', start, 'to', stop, 'of', num_params
            cached_results, parameters_not_cached_list = [], []
            for parameters in learner_params[start:stop]:
                if not self.summarizer.check_if_exists(model_name, learner_name, features_used, parameters):
                    parameters_not_cached_list.append(parameters)
                else:
                    cached_results.append((parameters, self.summarizer.get_metrics(model_name
                                           , learner_name, features_used, parameters)))
            start_time = time.time()
            new_results = []
            if len(parameters_not_cached_list) > 0:
                data_file_paths = self.manage_data_files(model_name, features_used, learner_name, 'generate')
                result = self.learner.do_train_and_test(learner_name, parameters_not_cached_list, data_file_paths)
                print 'Time Taken:', (time.time() - start_time) / len(result)
                for parameters, predictions in result:
                    new_results.append((parameters, self.metric_evaluator.get_metrics(labels, predictions)))
                param_list = [arg[0] for arg in new_results]
                metric_list = [arg[1] for arg in new_results]
                self.summarizer.add_metrics(model_name, learner_name, features_used, param_list, metric_list)
            for parameters, metrics in cached_results + new_results:
                greedy_metric_now = Metric(self.greed_criterion, metrics[self.greed_criterion])
                if min_metric.is_worse_than(greedy_metric_now):
                    min_metric = greedy_metric_now
                print '\nModel:', model_name
                print 'Features:', features_used
                print 'Learner:', learner_name
                print 'Hyper-parameters:', parameters
                print 'Metrics:', metrics

        self.manage_data_files(model_name, features_used, learner_name, option='remove')
        return min_metric

    def run_experiment(self):
        # Phase I : Build data sets
        if self.data_config.config:
            print 'Downloading/Loading Cached data...'
            for data_set in self.data_config.config:
                self.data_manager.fetch_from_vertica_to_df(data_set, self.data_config.config[data_set])

        # Phase II : Build Features
        if self.model_config:
            self.feature_builder.build_features()

        # Phase III: Feature Analyses
        for model_name, model in self.model_config.config.items():
            if 'analyze' in self.data_config.config and len(model['analyze']) > 0:
                print 'Analysing features:', model['analyze']
                self.feature_analyser.analyze(self.data_manager.get_data_set('analyze'), model['analyze'])

        # Phase IV:  Running Experiments
        print 'Starting/Resuming experiments...'
        for model_name, model in self.model_config.config.items():
            if 'train' in self.data_config.config and 'test' in self.data_config.config:
                features = model['features'].keys()
                greedy_features = model['greedy features'].keys()
                learner_config = model['learners']
                print '\n\nModel:', model_name
                print 'Features:', features
                print 'Greedy Features:', greedy_features
                for learner_name, learner_params in learner_config.items():
                    print '\nGreedy Features Used:', []
                    self.run_model(model_name, features, learner_name, learner_params)
                    # Greedy Feature Evaluation
                    for greedy_iter in range(min(self.greed_level, len(greedy_features))):
                        min_greedy_metric, greedy_feature_added = Metric(self.greed_criterion), None
                        for greedy_feature in greedy_features:
                            features_used = features + [greedy_feature]
                            print '\nGreedy Features Used:', list(set(features_used) & set(greedy_features))
                            min_metric_now = self.run_model(model_name, features_used, learner_name,
                                                                     learner_params)
                            if min_greedy_metric.is_worse_than(min_metric_now):
                                min_greedy_metric, greedy_feature_added = min_metric_now, greedy_feature

                        print 'Greedy Feature Added:', greedy_feature_added
                        features.append(greedy_feature_added)
                        greedy_features.remove(greedy_feature_added)
