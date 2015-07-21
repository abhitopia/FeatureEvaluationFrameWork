import csv
import os
from subprocess import CalledProcessError, call
import numpy as np


class VW_Wrapper():

    def __init__(self, vw_path='/usr/local/bin/vw', verbose=True):
        self.vw_path = vw_path
        self.vw_options = {}
        self.verbose = verbose

    @staticmethod
    def execute_command(cmd):
        try:
            call(cmd.split())
        except CalledProcessError as e:
            print 'Error calling VW. Error:', e

    def set_vw_options(self, vw_options, mode):
        if isinstance(vw_options, dict) and 'data' in vw_options:
            output_dir = {
                'train': os.path.dirname(vw_options.get('data', './vw_output/train.set')),
                'test': os.path.dirname(vw_options.get('data', './vw_output/test.set'))
            }[mode]

            main_vw_options = {
                'train': {
                    'data': vw_options['data'],
                    'final_regressor': vw_options.get('final_regressor', os.path.join(output_dir, 'trained.model')),
                    'cache_file': vw_options.get('cache_file', vw_options['data'] + '.cache'),
                    'bit_precision': vw_options.get('bit_precision', 29),
                    'hash': vw_options.get('hash', 'strings')
                },
                'test': {
                    'data': vw_options['data'],
                    'initial_regressor': vw_options.get('initial_regressor', self.vw_options.get('final_regressor', os.path.join(output_dir, 'trained.model'))),
                    'predictions': vw_options.get('predictions', os.path.join(output_dir, 'predictions')),
                    'cache_file': vw_options.get('cache_file', vw_options['data'] + '.cache'),
                    'bit_precision': vw_options.get('bit_precision', 29),
                    'hash': vw_options.get('hash', 'strings'),
                    'testonly': ''
                }
            }[mode]

            if not os.path.isfile(main_vw_options['cache_file']):
                vw_options['kill_cache'] = ''

            auxiliary_options = dict([(key, value) for key, value in vw_options.items() if key not in main_vw_options])

            self.vw_options.update(main_vw_options)
            self.vw_options.update(auxiliary_options)

            if mode == 'test':
                del self.vw_options['final_regressor']
                self.vw_options['passes'] = 1

        else:
            raise(AttributeError('Train: you must specify "data" path'))

    def train(self, vw_options):
        self.set_vw_options(vw_options, 'train')
        cmd = self.vw_path + ' ' + ' '.join(['--' + str(option) + ' ' + str(value) for option, value in self.vw_options.items()])
        if self.verbose:
            print 'Train:', cmd
        self.execute_command(cmd)

    def test(self, vw_options):
        self.set_vw_options(vw_options, 'test')
        cmd = self.vw_path + ' ' + ' '.join(['--' + str(option) + ' ' + str(value) for option, value in self.vw_options.items()])
        if self.verbose:
            print 'Test:', cmd
        self.execute_command(cmd)
        return self.collect_predictions()

    def collect_class_labels(self):
        with open(self.vw_options['data'], 'r') as data_file:
            reader = csv.reader(data_file, delimiter=' ')
            real_class_values = np.asarray([int(x[0]) for x in reader])
        return real_class_values

    def collect_predictions(self):
        def sigmoid(x):
            if isinstance(x, list):
                x = np.asarray(x)
            return 1. / (1 + np.exp(-x))
        return sigmoid(np.loadtxt(self.vw_options['predictions'], delimiter='\t'))