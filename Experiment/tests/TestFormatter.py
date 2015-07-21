import unittest

import nose
from pandas import DataFrame, Series
from pandas.util.testing import assert_series_equal

from Experiment import VW_Formatter


class TestFormatter(unittest.TestCase):
    def setUp(self):
        df = DataFrame(data={'label': ['1', '-1', '1'], 'client_id': ['1234', '', '5678'],
                             'visited_domains': ['43534 345345 345345', '345435', '34532'],
                             'position_id': ['4567', '324', ''],
                             'client_id;position_id': ['1234;4567', '', ''], 'numerical_feature': ['4.2', '', '']})
        self.formatter = VW_Formatter(data_frame=df)

    def test_check_feature_type(self):
        self.assertEquals(self.formatter.get_feature_type('label'), 'categorical')
        self.assertEquals(self.formatter.get_feature_type('visited_domains'), 'categorical')
        self.assertEquals(self.formatter.get_feature_type('position_id'), 'categorical')
        self.assertEquals(self.formatter.get_feature_type('client_id'), 'categorical')
        self.assertEquals(self.formatter.get_feature_type('client_id;position_id'), 'categorical')
        self.assertEquals(self.formatter.get_feature_type('numerical_feature'), 'numerical')

    def test_can_vw_format(self):
        self.assertTrue(self.formatter.can_vw_format(['client_id', 'client_id;position_id', 'numerical_feature']))
        self.assertFalse(self.formatter.can_vw_format(['my_random_id', 'client_id;position_id', 'numerical_feature']))
        del self.formatter.data_frame['label']
        self.assertFalse(self.formatter.can_vw_format(['client_id', 'client_id;position_id', 'numerical_feature']))

    @nose.tools.raises(ValueError)
    def test_conversion_to_vw_format_when_cannot_vw_format(self):
        self.formatter.to_vw_format(['my_random_id', 'client_id;position_id', 'numerical_feature'])

    def test_conversion_to_vw_format(self):
        result = Series(data=['1 |client_id 1234 ', '-1 |client_id ', '1 |client_id 5678 '])
        assert_series_equal(self.formatter.to_vw_format(['client_id']), result)
        result[0], result[1], result[2] = result[0] + '|visited_domains 43534 345345 345345 ', \
            result[1] + '|visited_domains 345435 ', result[2] + '|visited_domains 34532 '

        assert_series_equal(self.formatter.to_vw_format(['client_id', 'visited_domains']), result)
        result[0], result[1], result[2] = result[0] + '|position_id 4567 ', \
            result[1] + '|position_id 324 ', result[2] + '|position_id '
        assert_series_equal(self.formatter.to_vw_format(['client_id', 'visited_domains', 'position_id']), result)

        result[0], result[1], result[2] = result[0] + '|client_id;position_id 1234;4567 ', \
            result[1] + '|client_id;position_id ', result[2] + '|client_id;position_id '
        assert_series_equal(
            self.formatter.to_vw_format(['client_id', 'visited_domains', 'position_id', 'client_id;position_id']),
            result)

        result[0], result[1], result[2] = result[0] + '|numerical_feature numerical_feature_value:4.2 ', \
            result[1] + '|numerical_feature ', result[2] + '|numerical_feature '
        assert_series_equal(
            self.formatter.to_vw_format(
                ['client_id', 'visited_domains', 'position_id', 'client_id;position_id', 'numerical_feature']),
            result)

