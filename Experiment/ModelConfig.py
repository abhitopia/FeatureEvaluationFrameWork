import itertools
from Experiment.utilities import should_only_have
import copy


class ModelConfig():
    def __init__(self, model_config):
        self.config = model_config
        self.fill_optional_attributes()
        self.feature_definitions = {}
        self.aggregate_value_definitions = {}
        self.feature_used_names = []
        self.learner_names = []

    def fill_optional_attributes(self):
        for model_name in self.config:
            self.config[model_name]['ignore'] = self.config[model_name].get('ignore', [])
            self.config[model_name]['inherit'] = self.config[model_name].get('inherit', [])
            self.config[model_name]['greedy features'] = self.config[model_name].get('greedy features', {})
            self.config[model_name]['features'] = self.config[model_name].get('features', {})
            self.config[model_name]['aggregate values'] = self.config[model_name].get('aggregate values', {})
            self.config[model_name]['learners'] = self.config[model_name].get('learners', {})
            self.config[model_name]['analyze'] = self.config[model_name].get('analyze', [])

    def is_valid_model(self, model):

        def is_valid_learner(param):
            return isinstance(param, dict) and all(isinstance(param[key], list) for key in param)

        key_value_types = {
            'inherit': lambda x: isinstance(x, list) and all(
                m_name in self.config.keys() for m_name in x),
            'features': lambda x: isinstance(x, dict) and all(
                value is None or callable(value) for value in x.values()),
            'greedy features': lambda x: isinstance(x, dict) and all(
                value is None or callable(value) for value in x.values()),
            'ignore': lambda x: isinstance(x, list),
            'aggregate values': lambda x: isinstance(x, dict) and all(
                value is None or callable(value) for value in x.values()),
            'learners': lambda x: isinstance(x, dict) and all(
                is_valid_learner(value) for value in x.values()),
            'analyze': lambda x: isinstance(x, list) and all(isinstance(y, str) for y in x)
        }

        if isinstance(model, dict) and should_only_have(model, key_value_types.keys()):
            if all(key_value_types[key](value) for key, value in model.items()):
                return True
        return False

    def has_valid_model_config(self):
        return isinstance(self.config, dict) and all(
            self.is_valid_model(model) for model in self.config.values())

    def inherit_model_attribute(self, this_model_name, attribute_name, models_parsed=None):
        models_parsed = [] if models_parsed is None else models_parsed
        empty_attribute_value_mapping = {
            'features': {},
            'greedy features': {},
            'ignore': [],
            'aggregate values': {}
        }
        if this_model_name in models_parsed:
            return empty_attribute_value_mapping[attribute_name]
        else:
            models_parsed.append(this_model_name)
            this_model = self.config[this_model_name]
            this_model_attribute = copy.deepcopy(this_model[attribute_name])
            for base_model_name in this_model['inherit']:
                b_features = self.inherit_model_attribute(base_model_name, attribute_name, models_parsed)
                if isinstance(this_model_attribute, dict):
                    this_model_attribute.update(b_features)
                elif isinstance(this_model_attribute, list):
                    this_model_attribute = list(set(b_features + this_model_attribute))
            return this_model_attribute

    def remove_ignores_from_models(self):
        for model_name in self.config:
            for ignored in self.config[model_name]['ignore']:
                if ignored in self.config[model_name]['features']:
                    del self.config[model_name]['features'][ignored]
                if ignored in self.config[model_name]['greedy features']:
                    del self.config[model_name]['greedy features'][ignored]

    def set_all_feature_definitions(self):
        for model_name in self.config:
            self.feature_definitions.update(self.config[model_name]['features'])
            self.feature_definitions.update(self.config[model_name]['greedy features'])

    def set_all_aggregate_value_definitions(self):
        for model_name in self.config:
            self.aggregate_value_definitions.update(self.config[model_name]['aggregate values'])

    def set_all_learner_names(self):
        self.learner_names = list(
            set([learner_name for model in self.config.values() for learner_name in model['learners'].keys()]))

    def set_all_features_used_names(self):
        self.feature_used_names = list(set([learner_name for model in self.config.values() for learner_name in
                                            model['features'].keys() + model['greedy features'].keys()]))

    def build_learner_config(self):
        for model_name in self.config:
            for learner, hyper_parameters in self.config[model_name]['learners'].items():
                learner_config = [dict(zip(hyper_parameters.keys(), x)) for x in
                                  list(itertools.product(*hyper_parameters.values()))]
                self.config[model_name]['learners'][learner] = learner_config

    def parse_model_config(self):
        if self.has_valid_model_config():
            self.build_learner_config()
            for model_name in self.config:
                self.config[model_name]['features'] = self.inherit_model_attribute(model_name, 'features')
                self.config[model_name]['aggregate values'] = self.inherit_model_attribute(model_name,
                                                                                           'aggregate values')
                self.config[model_name]['greedy features'] = self.inherit_model_attribute(model_name, 'greedy features')
                self.config[model_name]['ignore'] = self.inherit_model_attribute(model_name, 'ignore')

            self.set_all_feature_definitions()
            self.remove_ignores_from_models()
            self.set_all_aggregate_value_definitions()
            self.set_all_learner_names()
            self.set_all_features_used_names()
        else:
            raise (ValueError('Experimenter: model_config parameter is ill defined'))