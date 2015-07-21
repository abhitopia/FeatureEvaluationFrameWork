EXPERIMENT_NAME = 'engagement_model'

# This variables contains configuration to get train and test data from Vertica
DATA_CONFIG = {
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
                    feature_converter('label', CASE WHEN engagement_count = 0 THEN '-1'  ELSE '1' END) AS label
                FROM
                    train.base_training_data td
                JOIN
                    temp.countries c
                ON
                    td.user_country_id = c.country_id
                WHERE
                    ( ( engagement_count > 0 AND random() < 0.1) OR (events!='' AND random() < 0.4 ) OR ( events = '') )
                    AND log_time_hour >= '{DATA_FROM_HOUR}'
                    AND log_time_hour <  '{DATA_TO_HOUR}'
                    AND c.country_dsp_code = '{COUNTRY}'""",
    # Specify the tags used by train data set
    'train':
        {
            'DATA_TO_HOUR': '2015-05-01 17:00:00',
            'DATA_FROM_HOUR': '2015-05-01 09:00:00',
            'COUNTRY': 'DK'
        },
    # Specify the tags used test data set
    'test':
        {
            'DATA_TO_HOUR': '2015-05-01 19:00:00',
            'DATA_FROM_HOUR': '2015-05-01 18:00:00',
            'COUNTRY': 'DK'
        }
}

MODEL_CONFIG = {
    # The keys are model names.
    'base_model': {
        'greedy features': {
            # Request Features
            'user_agent': None,
            'client_id;user_agent': None,
            'inventory_source_id;url': None,
            'inventory_source_id;position_id;size;url': None,
            'client_id': None,
            'position_id': None,
            'size': None,
            'client_id;placement_id': None,
            'client_id;position_id': None,
            'client_id;size': None,
            # Global cookie features
            'screen_size_id': None,
            'visited_domains': None,
            'clicker': None,
            'visited_logpoints': None,
            # Client cookie features
            'client_id;visited_domains': None,
            'client_id;screen_size_id': None,
            'client_id;visited_logpoints': None
        },
        'learners': {
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
                          greed_level=10)
experiment.run_experiment()
