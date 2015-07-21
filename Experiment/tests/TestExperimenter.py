import unittest

from Experiment import Experimenter


class TestExperimenter(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_can_format_data_config(self):
        experimenter = Experimenter(
            data_config={'train': {'tag1': '2344'}, 'test': {'tag1': '2344'}, 'select_sql': 'SELECT * FROM {tag2}'})
        self.assertFalse(experimenter.can_format_data_config())
        experimenter = Experimenter(
            data_config={'train': {'tag1': '2344'}, 'test': {'tag1': '2344'}, 'select_sql': 'SELECT * FROM {tag1}'})
        self.assertTrue(experimenter.can_format_data_config())

    def test_if_has_valid_data_config(self):
        experimenter = Experimenter(data_config=['a', 'b'])
        self.assertFalse(experimenter.has_valid_data_config())
        experimenter.data_config = {'A': 'a', 'B': 'b'}
        self.assertFalse(experimenter.has_valid_data_config())
        experimenter.data_config = {'Train': 'a', 'B': 'b'}
        self.assertFalse(experimenter.has_valid_data_config())
        experimenter.data_config = {'Train': 'a', 'Test': 'b'}
        self.assertFalse(experimenter.has_valid_data_config())
        experimenter.data_config = {'train': {'a': 1}, 'test': {'b': 2}}
        self.assertFalse(experimenter.has_valid_data_config())
        experimenter.data_config = {'train': {'a': 1}, 'test': {'b': 2}, 'select_sql': """SELECT hello FROM crap"""}
        self.assertTrue(experimenter.has_valid_data_config())
        experimenter.data_config = {'train': {'tag1': '2344'}, 'test': {'tag1': '2344'},
                                    'select_sql': 'SELECT * FROM {tag2}'}
        self.assertFalse(experimenter.has_valid_data_config())
        experimenter.data_config = {'train': {'tag1': '2344'}, 'test': {'tag1': '2344'},
                                    'select_sql': 'SELECT * FROM {tag1}'}
        self.assertTrue(experimenter.has_valid_data_config())

    def test_build_data_config(self):
        experimenter = Experimenter(data_config={'train': {'table': 'train_data'}, 'test': {'table': 'test_data'},
                                                 'select_sql': 'SELECT * FROM {table}'})
        experimenter.build_data_config()
        self.assertEqual(experimenter.data_config['train'], 'SELECT * FROM train_data')
        self.assertEqual(experimenter.data_config['test'], 'SELECT * FROM test_data')

    def test_if_has_valid_learner_config(self):
        experimenter = Experimenter(learner_config={'vw': ['param']})
        self.assertFalse(experimenter.has_valid_learner_config())
        experimenter.learner_config = {'vw': {'param': 'value'}}
        self.assertFalse(experimenter.has_valid_learner_config())
        experimenter.learner_config = {'vw': {'param': ['value'], 'param2': 'value2'}}
        self.assertFalse(experimenter.has_valid_learner_config())
        experimenter.learner_config = {'vw': {'param': ['value'], 'param2': ['value2', 'value22']}}
        self.assertTrue(experimenter.has_valid_learner_config())

    def test_build_learner_config(self):
        experimenter = Experimenter(learner_config={'vw': {'param': ['1', '2']}})
        experimenter.build_learner_config()
        self.assertEquals(experimenter.learner_config, {'vw': [{'param': '1'}, {'param': '2'}]})
        experimenter = Experimenter(
            learner_config={'vw': {'param1': ['1', '2'], 'param2': [3, 4]}})
        experimenter.build_learner_config()
        print 'learner_config', experimenter.learner_config
        self.assertEquals(experimenter.learner_config,
                          {'vw': [{'param1': '1', 'param2': 3}, {'param1': '2', 'param2': 3},
                                  {'param1': '1', 'param2': 4}, {'param1': '2', 'param2': 4}]})
        experimenter = Experimenter(
            learner_config={'vw': {'param1': ['1', '2'], 'param2': [3, 4]}, 'fm': {'param3': ['5']}})
        experimenter.build_learner_config()
        print 'learner_config', experimenter.learner_config
        self.assertEquals(experimenter.learner_config,
                          {'vw': [{'param1': '1', 'param2': 3}, {'param1': '2', 'param2': 3},
                                  {'param1': '1', 'param2': 4}, {'param1': '2', 'param2': 4}],
                           'fm': [{'param3': '5'}]})

    def test_is_valid_model(self):
        experimenter = Experimenter()
        model = {'ignore': {}, 'features': {}, 'inherit': []}
        self.assertFalse(experimenter.is_valid_model(model))
        model = {'ignore': [], 'features': {}}
        self.assertFalse(experimenter.is_valid_model(model))
        model = {'ignore': [], 'features': {}, 'inherit': []}
        self.assertTrue(experimenter.is_valid_model(model))
        model = {'ignore': [], 'features': {'feat1': 2}, 'inherit': []}
        self.assertFalse(experimenter.is_valid_model(model))
        model = {'ignore': [], 'features': {'feat1': None}, 'inherit': []}
        self.assertTrue(experimenter.is_valid_model(model))
        model = {'ignore': [], 'features': {'feat1': lambda x: x}, 'inherit': []}
        self.assertTrue(experimenter.is_valid_model(model))

    def test_has_valid_model_config(self):
        experimenter = Experimenter()
        experimenter.model_config = [{'ignore': [], 'features': {'feat1': lambda x: x}, 'inherit': []}]
        self.assertFalse(experimenter.has_valid_model_config())
        experimenter.model_config = {'base_model': {'ignore': [], 'features': {'feat1': lambda x: x}, 'inherit': []}}
        self.assertTrue(experimenter.has_valid_model_config())
        experimenter.model_config = {
            'derived_model': {'ignore': [], 'features': {'feat1': lambda x: x}, 'inherit': ['base_model']}}
        self.assertFalse(experimenter.has_valid_model_config())
        experimenter.model_config = {'base_model': {'ignore': [], 'features': {'feat1': lambda x: x}, 'inherit': []},
                                     'derived_model': {'ignore': [], 'features': {'feat1': lambda x: x},
                                                       'inherit': ['base_model']}}
        self.assertTrue(experimenter.has_valid_model_config())

    def test_build_model_config(self):
        experimenter = Experimenter()
        func = lambda x: x
        experimenter.model_config = {'base_model': {'ignore': [], 'features': {'feat1': func}, 'inherit': []}}
        experimenter.build_model_config()
        self.assertEquals(experimenter.model_config, {'base_model': {'feat1': func}})
        experimenter.model_config = {'base_model': {'ignore': [], 'features': {'feat1': func}, 'inherit': []},
                                     'derived_model': {'ignore': [], 'features': {}, 'inherit': ['base_model']}}
        experimenter.build_model_config()
        self.assertEquals(experimenter.model_config, {'base_model': {'feat1': func}, 'derived_model': {'feat1': func}})
        experimenter.model_config = {'base_model': {'ignore': [], 'features': {'feat1': func}, 'inherit': []},
                                     'derived_model': {'ignore': ['feat1'], 'features': {'feat2': None},
                                                       'inherit': ['base_model']}}
        experimenter.build_model_config()
        self.assertEquals(experimenter.model_config, {'base_model': {'feat1': func}, 'derived_model': {'feat2': None}})
        experimenter.model_config = {
            'base_model': {'ignore': [], 'features': {'feat1': func}, 'inherit': ['derived_model']},
            'derived_model': {'ignore': ['feat1'], 'features': {'feat2': None}, 'inherit': ['base_model']}}
        experimenter.build_model_config()
        self.assertEquals(experimenter.model_config, {'base_model': {'feat2': None}, 'derived_model': {'feat2': None}})
