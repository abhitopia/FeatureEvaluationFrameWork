EXPERIMENT_NAME = 'VD_with_recency_1week'
# This becomes the name of one single HDF5 file that contains the data and results. It will be stored in the same
# folder

from pandas import Series
from numpy import nan

# This variables contains configuration to get train and test data from Vertica
DATA_CONFIG = {
    # It was important to specify a general select statement to fetch arbitrarily complex features or raw features.
    # Also notice the tags like {DATA_FROM_HOUR}. These are replaced for different datasets namely 'train' and 'test'.
    'select_sql': """SELECT
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
                    feature_converter('visited_domains', IFNULL(td.visited_domains_with_recency,'N/A') ) AS vd_with_recency,
                    feature_converter('visited_logpoints', IFNULL(td.visited_logpoints::VARCHAR,'N/A') ) AS visited_logpoints,
                    feature_converter('user_agent', IFNULL(SPLIT_USERAGENT(td.user_agent)::VARCHAR,'N/A') ) AS user_agent,
                    feature_converter('url', CASE WHEN td.rtb_url = '' THEN td.website_id :: VARCHAR ELSE SPLIT_URL(td.rtb_url) :: VARCHAR END ) AS url,
                    feature_converter('inventory_source_id', CASE WHEN td.inventory_source_id = -1 THEN 'N/A' :: VARCHAR ELSE td.inventory_source_id :: VARCHAR END ) AS inventory_source_id,
                    feature_converter('label', CASE WHEN click_count = 0 THEN '-1'  ELSE '1' END) AS label
                FROM
                    temp.td_abhi_2 AS td
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
            'DATA_TO_HOUR': '2015-06-07 00:00:00',
            'DATA_FROM_HOUR': '2015-06-01 00:00:00',
            'COUNTRY': 'DK'
        },
    # Specify the tags used test data set
    'test':
        {
            'DATA_TO_HOUR': '2015-06-08 00:00:00',
            'DATA_FROM_HOUR': '2015-06-07 00:00:00',
            'COUNTRY': 'DK'
        }
}

from Experiment.Constants import PRODUCTION_CPC_MODEL_TUNED


def get_vd(vd_with_recency, max_hours_passed):
    result = []
    for part in vd_with_recency.split(' '):
        parts_split = part.split(':')
        if len(parts_split) > 1:
            feature, hours = parts_split[0], int(parts_split[1])
            if hours > max_hours_passed:
                continue
            else:
                result.append(feature)
    return ' '.join(result)

MODEL_CONFIG = {
    'production_model': PRODUCTION_CPC_MODEL_TUNED,
    'no_VD': {
        'inherit': ['production_model'],
        'features': {
        },
        'ignore': [ 'visited_domains',
                    'client_id;visited_domains'],
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
    'VD_with_recency': {
        'inherit': ['no_VD'],
        'features': {
            'vd_with_recency': None
        },
        'greedy features': {
            'vd_4_hours': lambda vd_with_recency: get_vd(vd_with_recency, 4),
            'vd_8_hours': lambda vd_with_recency: get_vd(vd_with_recency, 8),
            'vd_12_hours': lambda vd_with_recency: get_vd(vd_with_recency, 12),
            'vd_18_hours': lambda vd_with_recency: get_vd(vd_with_recency, 18),
            'vd_1_day': lambda vd_with_recency: get_vd(vd_with_recency, 24),
            'vd_2_days': lambda vd_with_recency: get_vd(vd_with_recency, 24*2),
            'vd_3_days': lambda vd_with_recency: get_vd(vd_with_recency, 24*3),
            'vd_4_days': lambda vd_with_recency: get_vd(vd_with_recency, 24*4),
            'vd_1_week': lambda vd_with_recency: get_vd(vd_with_recency, 24*7),
            'vd_2_weeks': lambda vd_with_recency: get_vd(vd_with_recency, 24*7*2),
            'vd_3_weeks': lambda vd_with_recency: get_vd(vd_with_recency, 24*7*3),
            'vd_4_weeks': lambda vd_with_recency: get_vd(vd_with_recency, 24*28),
            
            'client_id;vd_4_hours': None,
            'client_id;vd_8_hours': None,
            'client_id;vd_12_hours': None,
            'client_id;vd_18_hours': None,
            'client_id;vd_1_day':  None,
            'client_id;vd_2_days': None,
            'client_id;vd_3_days': None,
            'client_id;vd_4_days': None,
            'client_id;vd_1_week': None,
            'client_id;vd_2_weeks': None,
            'client_id;vd_3_weeks': None,
            'client_id;vd_4_weeks': None
        },
        'ignore': ['parse_vd_info',
                    'vd_with_recency'],
        'learners': {
            # The key is the learner name. Currently only VW is supported
            'VW': {
                'l1': [11, 12, 14, 16, 17],  # for example both l1=7.0 and 8.0 are evaluated here
                'passes': [7],
                'ftrl_alpha': [0.025, 0.05623413251903491, 0.075, 0.1],
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
experiment = Experimenter(name=EXPERIMENT_NAME,
                          data_config=DATA_CONFIG,
                          model_config=MODEL_CONFIG,
                          threads=2,
                          greed_level=4,
                          cache_data=True,
                          cache_results=True
                          )
experiment.run_experiment()