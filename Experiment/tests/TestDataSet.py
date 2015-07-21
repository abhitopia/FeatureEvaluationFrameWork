import unittest
import pandas
import numpy as np
from pandas.util.testing import assert_frame_equal
from Experiment.DataManager import DataManager


class TestDataSet(unittest.TestCase):
    def setUp(self):
        self.dataSet = DataManager()

    def tearDown(self):
        pass

    def test_read_and_write_data_frame(self):
        # TODO: Better way to test this
        print 'Read and Write Data Frame to HDF5 file'
        hdf5_file = '/tmp/test.h5'
        node_name = 'SomeNodeName'
        df = pandas.DataFrame(np.random.randn(100, 4), columns=list('ABCD'))
        self.dataSet.write_df_to_store(node_name, df, hdf5_file)
        self.assertTrue(self.dataSet.check_if_node_already_exists(node_name, hdf5_file))
        read_df = self.dataSet.read_df_from_store(node_name, hdf5_file)
        assert_frame_equal(df.sort(axis=1), read_df.sort(axis=1), check_names=True)

    def test_fetch_data_from_vertica_to_df(self):
        # TODO: Better way to test this
        print "Get data from Vertica and save it to Data Frame"
        query = """SELECT
                        client_id,
                        position_id,
                        visited_domains
                    FROM
                        train.base_training_data
                    WHERE
                        log_time_hour = '2015-04-14 00:00:00' LIMIT 100"""
        df = self.dataSet.fetch_from_vertica_to_df(query)
        self.assertEquals(len(df), 100)