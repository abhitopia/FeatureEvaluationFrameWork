EXPERIMENT_NAME = 'Category_interest_change_no_normalization'
# This variables contains configuration to get train and test data from Vertica
DATA_CONFIG = {
    # It was important to specify a general select statement to fetch arbitrarily complex features or raw features.
    # Also notice the tags like {DATA_FROM_HOUR}. These are replaced for different datasets namely 'train' and 'test'.
    'select_sql': """
                SELECT
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
                        category_ids AS category_ids,
                        cookie_categories AS cookie_categories,
                        client_categories AS client_categories,
                        feature_converter('label', CASE WHEN click_count = 0 THEN '-1'  ELSE '1' END) AS label
                FROM
                    experimental.td_with_categories_2 td
                JOIN
                    temp.countries c
                ON
                    td.user_country_id = c.country_id
                WHERE
                    ( click_count > 0 OR (events!='' AND random() < 0.01 ) OR events = '' )
                    AND log_time_hour >= '{DATA_FROM_HOUR}'
                    AND log_time_hour <  '{DATA_TO_HOUR}'
                    AND c.country_dsp_code = '{COUNTRY}'""",
    'train':
        {
            'DATA_TO_HOUR': '2015-06-27 00:00:00',
            'DATA_FROM_HOUR': '2015-06-20 00:00:00',
            'COUNTRY': 'DK'
        },
    'test':
        {
            'DATA_TO_HOUR': '2015-06-28 00:00:00',
            'DATA_FROM_HOUR': '2015-06-27 00:00:00',
            'COUNTRY': 'DK'
        }
}

from collections import defaultdict
import math
from Experiment.Constants import PRODUCTION_CPC_MODEL_TUNED


def categories2dict(category_str):
    result = defaultdict(list)
    if category_str != 'N/A' and len(category_str) != 0 and category_str is not None and category_str != 'None':
        for cat_hour_pair in category_str.split(','):
            category = cat_hour_pair.split(';')[0]
            hour = int(cat_hour_pair.split(';')[1])
            result[category].append(hour)
    return result


def get_binned_categories(feature, period):
    feat_dict = categories2dict(feature)
    result = {}
    for feature, recency_list in feat_dict.items():
        result[feature] = {}
        recency_list = sorted(recency_list)
        last_period = int(math.ceil(float(recency_list[-1])/period) * period + 1)
        for edge in range(period, last_period, period):
            result[feature][edge] = 0
            for hour in recency_list:
                if edge-period <= hour < edge:
                    result[feature][edge] += 1
    return result


def get_cookie_interest_change(feature, period, kind, normalize=True):
    binned_cats = get_binned_categories(feature, period)
    result = {}
    for feature, counts in binned_cats.items():
        p_1 = counts[period]
        p_2 = counts[2*period] if 2*period in counts else 0
        p_hist = sum(value for value in counts.values())/len(counts)
        value = {
            'short_term_rel': (p_1 + 0.01)/(p_2 + 0.01),
            'long_term_rel': (p_1 + 0.01)/(p_hist + 0.01),
            'long_term_abs': 2*p_1 - p_hist,
            'short_term_abs': p_1 - p_2
        }[kind]
        if value != 0:
            result[feature] = float(value)

    if len(result) > 20:
        keys = sorted(result, key=result.get, reverse=True)[0:20]
        result = dict([(key, result[key]) for key in keys])

    if normalize:
        l2_norm = math.sqrt(sum(val * val for val in result.values()))
        if l2_norm > 0:
            result = dict([(key, val/l2_norm) for key, val in result.items()])
        result = dict([(key, val/l2_norm) for key, val in result.items()])
    result = [key + ':' + str(val) for key, val in result.items()]
    return ' '.join(result)


def get_client_feature(feature, normalize=True):
    result = {}
    if feature == 'N/A' or feature == '':
        return ''
    else:
        for pair in feature.split(','):
            category = pair.split(';')[0]
            freq = float(pair.split(';')[1])
            result[category] = freq

        if len(result) > 20:
            keys = sorted(result, key=result.get, reverse=True)[0:20]
            result = dict([(key, result[key]) for key in keys])

        if normalize:
            l2_norm = math.sqrt(sum(val * val for val in result.values()))
            if l2_norm > 0:
                result = dict([(key, val/l2_norm) for key, val in result.items()])
        result = [key + ':' + str(val) for key, val in result.items()]

        return ' '.join(result)


def get_cookie_feature(feature, period=7*24, normalize=True):
    feat_dict = categories2dict(feature)
    result = {}
    for feature, recency_list in feat_dict.items():
        recency_list = sorted(recency_list)
        frequency = 0
        for hours in recency_list:
            if hours < period:
                frequency += 1
            else:
                break
        if frequency > 0:
            result[feature] = float(frequency)

    if len(result) > 20:
        keys = sorted(result, key=result.get, reverse=True)[0:20]
        result = dict([(key, result[key]) for key in keys])

    if normalize:
        l2_norm = math.sqrt(sum(val * val for val in result.values()))
        if l2_norm > 0:
            result = dict([(key, val/l2_norm) for key, val in result.items()])
    result = [key + ':' + str(val) for key, val in result.items()]
    return ' '.join(result)


def get_cookie_client_intersection(cookie_feature, client_feature, normalize=True):
    if cookie_feature != '' and client_feature != '':
        cookie_features = {}
        client_features = {}
        result = {}
        for pair in cookie_feature.split(' '):
            cookie_features[pair.split(':')[0]] = float(pair.split(':')[1])

        for pair in client_feature.split(' '):
            client_features[pair.split(':')[0]] = float(pair.split(':')[1])

        for common in list(set(cookie_features.keys()) & set(client_features.keys())):
            result[common] = cookie_features[common] * client_features[common]

        if normalize:
            l2_norm = math.sqrt(sum(val * val for val in result.values()))
            if l2_norm > 0:
                result = dict([(key, val/l2_norm) for key, val in result.items()])

        result = [key + ':' + str(val) for key, val in result.items()]
        return ' '.join(result)
    else:
        return ''


def process_domain_category_ids(cat_str):
    return ' '.join(cat_str.split(';'))


def domain_intersection(domain_feature, other_feature, normalize=True):
    if domain_feature != '' and other_feature != '':
        domain_features = domain_feature.split(' ')
        other_features = {}
        result = {}
        for pair in other_feature.split(' '):
            other_features[pair.split(':')[0]] = float(pair.split(':')[1])

        for common in list(set(domain_features) & set(other_features.keys())):
            result[common] = other_features[common]

        if normalize:
            l2_norm = math.sqrt(sum(val * val for val in result.values()))
            if l2_norm > 0:
                result = dict([(key, val/l2_norm) for key, val in result.items()])

        result = [key + ':' + str(val) for key, val in result.items()]
        return ' '.join(result)
    else:
        return ''


MODEL_CONFIG = {
    'production_model': PRODUCTION_CPC_MODEL_TUNED,
    'category_model': {
        'inherit': ['production_model'],
        'greedy features': {
            # cookie interest change normalized
            'cookie_short_term_rel': lambda cookie_categories: get_cookie_interest_change(cookie_categories, 7*24, 'short_term_rel', normalize=False),
            'cookie_long_term_rel': lambda cookie_categories: get_cookie_interest_change(cookie_categories, 7*24, 'long_term_rel', normalize=False),
            'cookie_short_term_abs': lambda cookie_categories: get_cookie_interest_change(cookie_categories, 7*24, 'short_term_abs', normalize=False),
            'cookie_long_term_abs': lambda cookie_categories: get_cookie_interest_change(cookie_categories, 7*24, 'long_term_abs', normalize=False),

            # cookie interest change normalized
            'screen_size_id;cookie_short_term_rel': None,
            'screen_size_id;cookie_long_term_rel': None,
            'screen_size_id;cookie_short_term_abs': None,
            'screen_size_id;cookie_long_term_abs': None,

            # cookie interest change normalized
            'client_id;cookie_short_term_rel': None,
            'client_id;cookie_long_term_rel': None,
            'client_id;cookie_short_term_abs': None,
            'client_id;cookie_long_term_abs': None,

            # cookie interest change normalized
            'browser_id;cookie_short_term_rel': None,
            'browser_id;cookie_long_term_rel': None,
            'browser_id;cookie_short_term_abs': None,
            'browser_id;cookie_long_term_abs': None,

            # cookie interest change normalized
            'os_id;cookie_short_term_rel': None,
            'os_id;cookie_long_term_rel': None,
            'os_id;cookie_short_term_abs': None,
            'os_id;cookie_long_term_abs': None,

            # cookie interest change normalized
            'position_id;cookie_short_term_rel': None,
            'position_id;cookie_long_term_rel': None,
            'position_id;cookie_short_term_abs': None,
            'position_id;cookie_long_term_abs': None,

            # cookie interest change normalized
            'size;cookie_short_term_rel': None,
            'size;cookie_long_term_rel': None,
            'size;cookie_short_term_abs': None,
            'size;cookie_long_term_abs': None,

            # cookie interest change normalized
            'inventory_source_id;cookie_short_term_rel': None,
            'inventory_source_id;cookie_long_term_rel': None,
            'inventory_source_id;cookie_short_term_abs': None,
            'inventory_source_id;cookie_long_term_abs': None,

            # cookie interest change normalized
            'inventory_source_id;position_id;size;url;cookie_short_term_rel': None,
            'inventory_source_id;position_id;size;url;cookie_long_term_rel': None,
            'inventory_source_id;position_id;size;url;cookie_short_term_abs': None,
            'inventory_source_id;position_id;size;url;cookie_long_term_abs': None,

            # cookie interest change normalized
            'screen_size_id;browser_id;cookie_short_term_rel': None,
            'screen_size_id;browser_id;cookie_long_term_rel': None,
            'screen_size_id;browser_id;cookie_short_term_abs': None,
            'screen_size_id;browser_id;cookie_long_term_abs': None,

            # cookie interest change normalized
            'browser_id;url;cookie_short_term_rel': None,
            'browser_id;url;cookie_long_term_rel': None,
            'browser_id;url;cookie_short_term_abs': None,
            'browser_id;url;cookie_long_term_abs': None
        },
        'learners': {
            'VW': {
                'l1': [14],
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
experiment = Experimenter(name=EXPERIMENT_NAME,
                          data_config=DATA_CONFIG,
                          model_config=MODEL_CONFIG,
                          threads=1,
                          greed_level=4,
                          cache_data=True,
                          cache_features=True,
                          cache_results=True)
experiment.run_experiment()

