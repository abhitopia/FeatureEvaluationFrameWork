import unittest

from pandas.util.testing import assert_series_equal
from pandas import DataFrame, Series

from Experiment.Feature import Feature


class TestFeature(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame(data={'client_id': ['1234', '5678', ''],
                                  'visited_domains': ['43534 345345 345345', '345435', '23234'],
                                  'position_id': ['4567', 'N/A', '2234']})
        self.simple_feat = Feature(feature_name='client_id', data_frame=self.df)
        self.int_feat_2 = Feature(feature_name='client_id;position_id', data_frame=self.df)
        self.int_feat_22 = Feature(feature_name='client_id|position_id', data_frame=self.df, delimiter='|')
        self.int_feat_3 = Feature(feature_name='client_id;position_id;visited_domains', data_frame=self.df)
        self.custom_feature = Feature(feature_name='my_custom_feature', data_frame=self.df)

    def tearDown(self):
        pass

    def test_conversion_to_feature_parts(self):
        self.assertEquals(self.simple_feat.get_feature_parts(), ['client_id'])
        self.assertEquals(self.int_feat_2.get_feature_parts(), ['client_id', 'position_id'])
        self.assertEquals(self.int_feat_22.get_feature_parts(), ['client_id', 'position_id'])
        self.assertEquals(self.int_feat_3.get_feature_parts(), ['client_id', 'position_id', 'visited_domains'])

    def test_check_if_feature_already_exists(self):
        self.assertTrue(self.simple_feat.exists())
        self.assertFalse(self.int_feat_2.exists())
        self.assertFalse(self.custom_feature.exists())

    def test_check_if_feature_is_simple_feature(self):
        self.assertTrue(self.simple_feat.is_simple_feature())
        self.assertFalse(self.int_feat_2.is_simple_feature())
        self.assertFalse(self.custom_feature.is_simple_feature())

    def test_check_if_feature_is_interaction_feature(self):
        self.assertFalse(self.simple_feat.is_interaction_feature())
        self.assertTrue(self.int_feat_2.is_interaction_feature())
        self.assertFalse(self.custom_feature.is_interaction_feature())

    def test_check_if_feature_is_custom_feature(self):
        self.assertFalse(self.simple_feat.is_custom_feature())
        self.assertFalse(self.int_feat_2.is_custom_feature())
        self.assertTrue(self.custom_feature.is_custom_feature())

    def test_can_make_simple_feature(self):
        self.assertTrue(self.simple_feat.can_make_simple_feature())
        self.assertFalse(self.int_feat_2.can_make_simple_feature())
        self.assertFalse(self.int_feat_3.can_make_simple_feature())
        self.assertFalse(self.custom_feature.can_make_simple_feature())

    def test_can_make_interaction_feature(self):
        self.assertFalse(self.simple_feat.can_make_interaction_feature())
        self.assertTrue(self.int_feat_2.can_make_interaction_feature())
        self.assertTrue(self.int_feat_3.can_make_interaction_feature())
        self.assertFalse(self.custom_feature.can_make_interaction_feature())

    def test_can_make_custom_feature(self):
        self.assertFalse(self.simple_feat.can_make_custom_feature())
        self.assertFalse(self.int_feat_2.can_make_custom_feature())
        self.assertFalse(self.int_feat_3.can_make_custom_feature())
        self.assertFalse(self.custom_feature.can_make_custom_feature())
        self.custom_feature.custom_feature_builder = lambda client_id, position_id: client_id + position_id
        self.assertTrue(self.custom_feature.can_make_custom_feature())
        self.custom_feature.custom_feature_builder = None

    def test_build_simple_feature(self):
        if self.simple_feat.build_simple_feature() and \
                not self.int_feat_2.build_simple_feature() and \
                not self.int_feat_3.build_simple_feature() and not \
                self.custom_feature.build_simple_feature():
            assert_series_equal(self.simple_feat.get_feature(), Series(data=['1234', '5678', '']))
        else:
            self.assertFalse(True, 'Feature building failed')

    def test_build_interaction_feature(self):
        if self.int_feat_2.build_interaction_feature() and self.int_feat_3.build_interaction_feature() and not \
                self.custom_feature.build_interaction_feature():
            assert_series_equal(self.int_feat_2.get_feature(),
                                Series(data=['1234;4567', '', '']))
            assert_series_equal(self.int_feat_3.get_feature(),
                                Series(data=['1234;4567;43534 1234;4567;345345 1234;4567;345345', '', '']))
        else:
            self.assertFalse(True, 'Feature building failed')

    def test_build_custom_feature(self):
        self.custom_feature.custom_feature_builder = lambda client_id, position_id: int(client_id) + int(position_id)
        if not self.int_feat_2.build_custom_feature() and not self.int_feat_3.build_custom_feature() and \
                self.custom_feature.build_custom_feature():
            assert_series_equal(self.custom_feature.get_feature(), Series(data=['5801', '', '']))
        else:
            self.assertFalse(True, 'Feature building failed')
        self.custom_feature.custom_feature_builder = None

    def test_build_any_feature(self):
        self.custom_feature.custom_feature_builder = lambda client_id, position_id: int(client_id) + int(position_id)
        if self.simple_feat.build_feature() and self.int_feat_2.build_feature() and self.int_feat_3.build_feature() \
                and self.custom_feature.build_feature():
            assert_series_equal(self.simple_feat.get_feature(), Series(data=['1234', '5678', '']))
            assert_series_equal(self.int_feat_2.get_feature(),
                                Series(data=['1234;4567', '', '']))
            assert_series_equal(self.int_feat_3.get_feature(),
                                Series(data=['1234;4567;43534 1234;4567;345345 1234;4567;345345', '', '']))
            assert_series_equal(self.custom_feature.get_feature(), Series(data=['5801', '', '']))
        else:
            self.assertFalse(True, 'Feature building failed')