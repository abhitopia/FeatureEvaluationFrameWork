EXPERIMENT_NAME = 'geographical_location_12hour'
DATA_CONFIG = {
    'train':
        {
            'DATA_TO_HOUR': '2015-05-08 17:00:00',
            'DATA_FROM_HOUR': '2015-05-08 05:00:00',
            'COUNTRY': 'DK'
        },
    'test':
        {
            'DATA_TO_HOUR': '2015-05-08 18:00:00',
            'DATA_FROM_HOUR': '2015-05-08 17:00:00',
            'COUNTRY': 'DK'
        },
    'select_sql': """SELECT
                        feature_converter('country_dsp_code', c.country_dsp_code) AS country_dsp_code,
                        feature_converter('client_id', CASE WHEN td.client_id = -1 THEN 'N/A' :: VARCHAR ELSE td.client_id :: VARCHAR END ) AS client_id,
                        feature_converter('placement_id', CASE WHEN td.placement_id = -1 THEN 'N/A' :: VARCHAR ELSE td.placement_id :: VARCHAR END ) AS placement_id,
                        feature_converter('position_id', CASE WHEN td.position_id = -1 THEN 'N/A' :: VARCHAR ELSE td.position_id :: VARCHAR END ) AS position_id,
                        feature_converter('size', IFNULL(td.size::VARCHAR,'N/A') ) AS size,
                        feature_converter('screen_size_id', CASE WHEN td.screen_size_id = -1 THEN 'N/A' :: VARCHAR ELSE td.screen_size_id :: VARCHAR END ) AS screen_size_id,
                        feature_converter('browser_id', CASE WHEN td.browser_id = -1 THEN 'N/A' :: VARCHAR ELSE td.browser_id :: VARCHAR END ) AS browser_id,
                        feature_converter('os_id', CASE WHEN td.os_id = -1 THEN 'N/A' :: VARCHAR ELSE td.os_id :: VARCHAR END ) AS os_id,
                        feature_converter('device_type_id', CASE WHEN td.device_type_id = -1 THEN 'N/A' :: VARCHAR ELSE td.device_type_id :: VARCHAR END ) AS device_type_id,
                        feature_converter('clicker', IFNULL(td.clicker::VARCHAR,'N/A') ) AS clicker,
                        feature_converter('visited_domains', IFNULL(td.visited_domains::VARCHAR,'N/A') ) AS visited_domains,
                        feature_converter('visited_logpoints', IFNULL(td.visited_logpoints::VARCHAR,'N/A') ) AS visited_logpoints,
                        feature_converter('user_agent', IFNULL(SPLIT_USERAGENT(td.user_agent)::VARCHAR,'N/A') ) AS user_agent,
                        feature_converter('url', CASE WHEN td.rtb_url = '' THEN td.website_id :: VARCHAR ELSE SPLIT_URL(td.rtb_url) :: VARCHAR END ) AS url,
                        feature_converter('inventory_source_id', CASE WHEN td.inventory_source_id = -1 THEN 'N/A' :: VARCHAR ELSE td.inventory_source_id :: VARCHAR END ) AS inventory_source_id,
                        feature_converter('user_city_id', CASE WHEN rtbi.user_city_id = -1 THEN 'N/A' :: VARCHAR ELSE rtbi.user_city_id :: VARCHAR END ) AS city_id,
                        feature_converter('user_zip_code', CASE WHEN rtbi.user_zip_code = '' THEN 'N/A' :: VARCHAR ELSE rtbi.user_zip_code :: VARCHAR END ) AS zip_code,
                        feature_converter('label', CASE WHEN td.click_count = 0 THEN '-1'  ELSE '1' END) AS label
                    FROM
                        train.base_training_data td
                    JOIN
                        temp.countries c
                    ON
                        td.user_country_id = c.country_id
                    JOIN fact.rtb_impressions AS rtbi
                    ON
                        rtbi.transaction_id = td.transaction_id
                        AND rtbi.log_time_hour = td.log_time_hour
                    WHERE
                        ( td.click_count > 0 OR (td.events!='' AND random() < 0.01 ) OR td.events = '' )
                        AND td.log_time_hour >= '{DATA_FROM_HOUR}'
                        AND td.log_time_hour <  '{DATA_TO_HOUR}'
                        AND c.country_dsp_code = '{COUNTRY}'"""
}

from Experiment.Constants import PRODUCTION_CPC_MODEL_TUNED
MODEL_CONFIG = {
    'base_model': PRODUCTION_CPC_MODEL_TUNED,
    'with_city_id': {
        'inherit': ['base_model'],
        'features': {
            'city_id': None,
            'client_id;city_id': None,
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
    },
    'with_zip_code': {
        'inherit': ['base_model'],
        'features': {
            'zip_code': None,
            'client_id;zip_code': None,
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
    },
    'with_city_and_zip': {
        'inherit': ['with_city_id', 'with_zip_code'],
        'features': {
            'zip_code': None,
            'client_id;zip_code': None,
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
                          greed_level=1)
experiment.run_experiment()
