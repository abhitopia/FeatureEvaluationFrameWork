import os
import signal
from Experiment.utilities import id_generator
from Experiment.VW_Formatter import VW_Formatter
from Experiment.VW_Wrapper import VW_Wrapper
from pathos.multiprocessing import Pool as Pool
import sys
from pandas import DataFrame


class Learner:
    def __init__(self, work_dir, data_manager):
        self.work_dir = work_dir
        self.data_manager = data_manager
        self.block_size = 1000000

    def get_formatted_data_set(self, learner, data_set, features, output_path):
        columns = ['label'] + features
        num_rows = len(self.data_manager.get_data_set_feature(data_set, 'label'))
        for offset in range(0, num_rows, self.block_size):
            data = {}
            for column in columns:
                data[column] = self.data_manager.get_data_set_feature(data_set, column, offset=offset, limit=self.block_size)[column]
            {
                'VW': VW_Formatter.to_vw_format_file
            }.get(learner, self.learner_not_found)(DataFrame(data), features, output_path, 'a')

    def do_train_and_test(self, learner, parameters, data_file_paths):
        return {
            'VW': self.vw_train_and_test
        }.get(learner, self.learner_not_found)(parameters, data_file_paths)

    def learner_not_found(self, *args):
        raise (ValueError(self.learner + ' not recognised'))

    def vw_train_and_test(self, options_list, data_file_paths):

        def init_worker():
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        def run_learner(options):
            train_options = {
                'final_regressor': os.path.join(self.work_dir, id_generator()),
                'data': data_file_paths['train'],
                'cache_file': data_file_paths['train'] + '.cache'
            }
            train_options.update(options)
            test_options = {
                'data': data_file_paths['test'],
                'predictions': os.path.join(self.work_dir, id_generator()),
                'cache_file': data_file_paths['test'] + '.cache'
            }
            test_options.update(options)

            # TO DO: remove below if.
            if 'kill_cache' in options:
                del train_options['kill_cache']
                del test_options['kill_cache']

            vw_wrapper = VW_Wrapper(verbose=False)
            vw_wrapper.train(train_options)
            predictions = vw_wrapper.test(test_options)
            os.remove(train_options['final_regressor'])
            os.remove(test_options['predictions'])
            return options, predictions

        if len(options_list) > 1:
            try:
                if not os.path.isfile(data_file_paths['test'] + '.cache') or not os.path.isfile(
                                        data_file_paths['train'] +'.cache'):
                    run_learner(options_list[0])
                pool = Pool(len(options_list), init_worker)
                result_list = pool.map_async(run_learner, options_list).get(99999999)
                pool.close()
                pool.join()
                return result_list
            except KeyboardInterrupt:
                print '  Keyboard Interrupt, exiting...) '
                pool.terminate()
                pool.join()
                sys.exit(0)

        elif len(options_list) == 1:
            return [run_learner(options_list[0])]
        else:
            return []



