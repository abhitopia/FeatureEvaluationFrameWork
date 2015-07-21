EXPERIMENT_NAME = 'user_agent_replacement'
DATA_CONFIG = {
    'train':
        {
            'DATA_TO_HOUR': '2015-04-23 00:00:00',
            'DATA_FROM_HOUR': '2015-04-16 00:00:00',
            'COUNTRY': 'DK'
        },
    'test':
        {
            'DATA_TO_HOUR': '2015-04-24 00:00:00',
            'DATA_FROM_HOUR': '2015-04-23 00:00:00',
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
                    AND c.country_dsp_code = '{COUNTRY}'"""
}
LEARNER_CONFIG = {
    'VW': {
        'l1': [7.0],
        'passes': [1],
        'ftrl_alpha': [0.1],
        'ftrl_beta': [0.0],
        'kill_cache': [True],
        'sort_features': [True],
        'hash': ["strings"],
        'bit_precision': [29],
        'ftrl': [True],
        'loss_function': ["logistic"],
        'l2': [0.0],
    }
}

MODEL_CONFIG = {
    'no_user_agent': {
        'inherit': [],
        'features': {
            'client_id': None,
            'client_id;placement_id': None,
            'position_id': None,
            'size': None,
            'screen_size_id': None,
            'visited_domains': None,
            'client_id;position_id': None,
            'client_id;size': None,
            'client_id;screen_size_id': None,
            'client_id;visited_domains': None,
            'client_id;visited_logpoints': None,
            'inventory_source_id;url': None,
            'inventory_source_id;position_id;size;url': None,
            'client_id;inventory_source_id;url': None,
            'client_id;inventory_source_id;position_id;size;url': None,
            'clicker': None,
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
    'with_user_agent': {
        'inherit': ['no_user_agent'],
        'features': {
            'user_agent': None,
            'client_id;user_agent': None,
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
    'with_os_id': {
        'inherit': ['no_user_agent'],
        'features': {
            'os_id': None,
            'client_id;os_id': None
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
    'with_browser_id': {
        'inherit': ['no_user_agent'],
        'features': {
            'browser_id': None,
            'client_id;browser_id': None
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
    'with_device_id': {
        'inherit': ['no_user_agent'],
        'features': {
            'device_type_id': None,
            'client_id;device_type_id': None
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
    'with_browser and os and device_type ids': {
        'inherit': ['with_os_id', 'with_browser_id', 'with_device_id'],
        'features': {},
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
experiment = Experimenter(datafile=EXPERIMENT_NAME + '.h5', data_config=DATA_CONFIG, model_config=MODEL_CONFIG,
                          learner_config=LEARNER_CONFIG)
experiment.run_experiment()
