EXPERIMENT_NAME = 'temporal_feature_binned'

# This variables contains configuration to get train and test data from Vertica
DATA_CONFIG = {
    # It was important to specify a general select statement to fetch arbitrarily complex features or raw features.
    # Also notice the tags like {DATA_FROM_HOUR}. These are replaced for different datasets namely 'train' and 'test'.
    'select_sql': """SELECT
                    log_time_hour :: VARCHAR AS log_time_hour,
                    feature_converter('country_dsp_code', c.country_dsp_code) AS country_dsp_code,
                    feature_converter('client_id', CASE WHEN td.client_id = -1 THEN 'N/A' :: VARCHAR ELSE td.client_id :: VARCHAR END ) AS client_id,
                    feature_converter('placement_id', CASE WHEN td.placement_id = -1 THEN 'N/A' :: VARCHAR ELSE td.placement_id :: VARCHAR END ) AS placement_id,
                    feature_converter('position_id', CASE WHEN td.position_id = -1 THEN 'N/A' :: VARCHAR ELSE td.position_id :: VARCHAR END ) AS position_id,
                    feature_converter('size', IFNULL(td.size::VARCHAR,'N/A') ) AS size,
                    feature_converter('screen_size_id', CASE WHEN td.screen_size_id = -1 THEN 'N/A' :: VARCHAR ELSE td.screen_size_id :: VARCHAR END ) AS screen_size_id,
                    feature_converter('browser_id', CASE WHEN td.browser_id = -1 THEN 'N/A' :: VARCHAR ELSE td.browser_id :: VARCHAR END ) AS browser_id,
                    feature_converter('os_id', CASE WHEN td.os_id = -1 THEN 'N/A' :: VARCHAR ELSE td.os_id :: VARCHAR END ) AS os_id,
                    feature_converter('clicker', IFNULL(td.clicker::VARCHAR,'N/A') ) AS clicker,
                    feature_converter('visited_domains', IFNULL(td.visited_domains::VARCHAR,'N/A') ) AS visited_domains,
                    feature_converter('visited_logpoints', IFNULL(td.visited_logpoints::VARCHAR,'N/A') ) AS visited_logpoints,
                    feature_converter('user_agent', IFNULL(SPLIT_USERAGENT(td.user_agent)::VARCHAR,'N/A') ) AS user_agent,
                    feature_converter('url', CASE WHEN td.rtb_url = '' THEN td.website_id :: VARCHAR ELSE SPLIT_URL(td.rtb_url) :: VARCHAR END ) AS url,
                    feature_converter('inventory_source_id', CASE WHEN td.inventory_source_id = -1 THEN 'N/A' :: VARCHAR ELSE td.inventory_source_id :: VARCHAR END ) AS inventory_source_id,
                    feature_converter('label', CASE WHEN click_count = 0 THEN '-1'  ELSE '1' END) AS label
                FROM
                    train.base_training_data td
                JOIN
                    temp.countries c
                ON
                    td.user_country_id = c.country_id
                WHERE
                    ( click_count > 0 OR (events!='' AND random() < 0.01 ) OR events = '' )
                    AND log_time_hour >= '{DATA_FROM_HOUR}'
                    AND log_time_hour <  '{DATA_TO_HOUR}'
                    AND c.country_dsp_code = '{COUNTRY}'""",
    # Specify the tags used by train data set
    'train':
        {
            'DATA_TO_HOUR': '2015-05-01 01:00:00',
            'DATA_FROM_HOUR': '2015-05-01 00:00:00',
            'COUNTRY': 'DK'
        },
    # Specify the tags used test data set
    'test':
        {
            'DATA_TO_HOUR': '2015-05-01 02:00:00',
            'DATA_FROM_HOUR': '2015-05-01 01:00:00',
            'COUNTRY': 'DK'
        }
}


def time_of_day(hour_of_day, bin_size=1):
    hour = int(hour_of_day)
    for bin_num in range(0, 24, bin_size):
        if hour >= bin_num:
            return str(bin_num)
        

from Experiment.Constants import PRODUCTION_CPC_MODEL_TUNED

MODEL_CONFIG = {
    # The keys are model names.
    'base_model': PRODUCTION_CPC_MODEL_TUNED,
    'hour_of_day': {
        # all the features specified in "inherit" becomes features for this model
        'inherit': ['base_model'],  # Thus, 'client_id, 'visited_domains', etc are part this model
        'greedy features': {
            'hour_of_day': lambda log_time_hour: log_time_hour.split(' ')[1].split(':')[0],
            'client_id;hour_of_day': None,
            'screen_size_id;hour_of_day': None,
            'browser_id;hour_of_day': None,
            'position_id;hour_of_day': None,
            'size;hour_of_day': None,
            'url;hour_of_day': None,
            'browser_id;url;hour_of_day': None,
            'screen_size_id;browser_id;hour_of_day': None,
            'client_id;browser_id;hour_of_day': None,
            'client_id;browser_id;hour_of_day;screen_size_id': None,
            'inventory_source_id;hour_of_day': None,
            'inventory_source_id;position_id;size;url;hour_of_day': None,

            'time_of_day_2': lambda hour_of_day: time_of_day(hour_of_day, 2),
            'client_id;time_of_day_2': None,
            'screen_size_id;time_of_day_2': None,
            'browser_id;time_of_day_2': None,
            'position_id;time_of_day_2': None,
            'size;time_of_day_2': None,
            'url;time_of_day_2': None,
            'browser_id;url;time_of_day_2': None,
            'screen_size_id;browser_id;time_of_day_2': None,
            'client_id;browser_id;time_of_day_2': None,
            'client_id;browser_id;time_of_day_2;screen_size_id': None,
            'inventory_source_id;time_of_day_2': None,
            'inventory_source_id;position_id;size;url;time_of_day_2': None,

            'time_of_day_3': lambda hour_of_day: time_of_day(hour_of_day, 3),
            'client_id;time_of_day_3': None,
            'screen_size_id;time_of_day_3': None,
            'browser_id;time_of_day_3': None,
            'position_id;time_of_day_3': None,
            'size;time_of_day_3': None,
            'url;time_of_day_3': None,
            'browser_id;url;time_of_day_3': None,
            'screen_size_id;browser_id;time_of_day_3': None,
            'client_id;browser_id;time_of_day_3': None,
            'client_id;browser_id;time_of_day_3;screen_size_id': None,
            'inventory_source_id;time_of_day_3': None,
            'inventory_source_id;position_id;size;url;time_of_day_3': None,

            'time_of_day_4': lambda hour_of_day: time_of_day(hour_of_day, 4),
            'client_id;time_of_day_4': None,
            'screen_size_id;time_of_day_4': None,
            'browser_id;time_of_day_4': None,
            'position_id;time_of_day_4': None,
            'size;time_of_day_4': None,
            'url;time_of_day_4': None,
            'browser_id;url;time_of_day_4': None,
            'screen_size_id;browser_id;time_of_day_4': None,
            'client_id;browser_id;time_of_day_4': None,
            'client_id;browser_id;time_of_day_4;screen_size_id': None,
            'inventory_source_id;time_of_day_4': None,
            'inventory_source_id;position_id;size;url;time_of_day_4': None,

            'time_of_day_6': lambda hour_of_day: time_of_day(hour_of_day, 6),
            'client_id;time_of_day_6': None,
            'screen_size_id;time_of_day_6': None,
            'browser_id;time_of_day_6': None,
            'position_id;time_of_day_6': None,
            'size;time_of_day_6': None,
            'url;time_of_day_6': None,
            'browser_id;url;time_of_day_6': None,
            'screen_size_id;browser_id;time_of_day_6': None,
            'client_id;browser_id;time_of_day_6': None,
            'client_id;browser_id;time_of_day_6;screen_size_id': None,
            'inventory_source_id;time_of_day_6': None,
            'inventory_source_id;position_id;size;url;time_of_day_6': None,

            'time_of_day_8': lambda hour_of_day: time_of_day(hour_of_day, 8),
            'client_id;time_of_day_8': None,
            'screen_size_id;time_of_day_8': None,
            'browser_id;time_of_day_8': None,
            'position_id;time_of_day_8': None,
            'size;time_of_day_8': None,
            'url;time_of_day_8': None,
            'browser_id;url;time_of_day_8': None,
            'screen_size_id;browser_id;time_of_day_8': None,
            'client_id;browser_id;time_of_day_8': None,
            'client_id;browser_id;time_of_day_8;screen_size_id': None,
            'inventory_source_id;time_of_day_8': None,
            'inventory_source_id;position_id;size;url;time_of_day_8': None

        },
        'learners': {
            # The key is the learner name. Currently only VW is supported
            'VW': {
                'l1': [14],  # for example both l1=7.0 and 8.0 are evaluated here
                'passes': [7],
                'ftrl_alpha': [0.05623413251903491],
                'ftrl_beta': [0.0],
                'sort_features': [''],
                'hash': ["strings"],
                'bit_precision': [29],
                'ftrl': [''],
                'loss_function': ["logistic"],
                'l2': [0.0],
                'quiet': [''],
                'sort_features': ['']
            }
        }

    }
}

from Experiment import Experimenter
experiment = Experimenter(name=EXPERIMENT_NAME, data_config=DATA_CONFIG, model_config=MODEL_CONFIG, threads=1,
                          greed_level=4)
experiment.run_experiment()
