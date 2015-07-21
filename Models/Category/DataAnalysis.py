import math

EXPERIMENT_NAME = 'Category_Analysis'

# This variables contains configuration to get train and test data from Vertica
DATA_CONFIG = {
    # It was important to specify a general select statement to fetch arbitrarily complex features or raw features.
    # Also notice the tags like {DATA_FROM_HOUR}. These are replaced for different datasets namely 'train' and 'test'.
    'select_sql': """
                SELECT
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
    'analyze':
        {
            'DATA_TO_HOUR': '2015-06-20 01:00:00',
            'DATA_FROM_HOUR': '2015-06-20 00:00:00',
            'COUNTRY': 'DK'
        }
}

from collections import defaultdict


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
    if normalize:
        l2_norm = math.sqrt(sum(val * val for val in result.values()))
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

        if normalize:
            l2_norm = math.sqrt(sum(val * val for val in result.values()))
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
        if frequency > 0:
            result[feature] = float(frequency)
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


MODEL_CONFIG = {
    'feature_creation': {
        'features': {
            #domain categories
            'domain_categories': lambda category_ids: process_domain_category_ids(category_ids),
            'client_id;domain_categories': lambda category_ids: process_domain_category_ids(category_ids),

            #client categories
            'client_categories_freq': lambda client_categories: get_client_feature(client_categories, normalize=True),

            #cookie categories
            'cookie_categories_12_hours': lambda cookie_categories: get_cookie_feature(cookie_categories, period=12, normalize=True),
            'cookie_categories_1_day': lambda cookie_categories: get_cookie_feature(cookie_categories, period=1*24, normalize=True),
            'cookie_categories_7_days': lambda cookie_categories: get_cookie_feature(cookie_categories, period=7*24, normalize=True),
            'cookie_categories_28_days': lambda cookie_categories: get_cookie_feature(cookie_categories, period=28*24, normalize=True),

            #cookie interest change normalized
            'cookie_short_term_rel': lambda cookie_categories: get_cookie_interest_change(cookie_categories, 7*24, 'short_term_rel', normalize=True),
            'cookie_long_term_rel': lambda cookie_categories: get_cookie_interest_change(cookie_categories, 7*24, 'long_term_rel', normalize=True),
            'cookie_short_term_abs': lambda cookie_categories: get_cookie_interest_change(cookie_categories, 7*24, 'short_term_abs', normalize=True),
            'cookie_long_term_abs': lambda cookie_categories: get_cookie_interest_change(cookie_categories, 7*24, 'long_term_abs', normalize=True),

            #cookie client category intersections
            'client_cookie_category_12_hours': lambda client_categories_freq, cookie_categories_12_hours: get_cookie_client_intersection(cookie_categories_12_hours, client_categories_freq, normalize=True),
            'client_cookie_category_1_day': lambda client_categories_freq, cookie_categories_1_day: get_cookie_client_intersection(cookie_categories_1_day, client_categories_freq, normalize=True),
            'client_cookie_category_7_days': lambda client_categories_freq, cookie_categories_7_days: get_cookie_client_intersection(cookie_categories_7_days, client_categories_freq, normalize=True),
            'client_cookie_category_28_days': lambda client_categories_freq, cookie_categories_28_days: get_cookie_client_intersection(cookie_categories_28_days, client_categories_freq, normalize=True),

            'client_cookie_short_term_rel': lambda client_categories_freq, cookie_short_term_rel: get_cookie_client_intersection(cookie_short_term_rel, client_categories_freq, normalize=True),
            'client_cookie_long_term_rel': lambda client_categories_freq, cookie_long_term_rel: get_cookie_client_intersection(cookie_long_term_rel, client_categories_freq, normalize=True),
            'client_cookie_short_term_abs': lambda client_categories_freq, cookie_short_term_abs: get_cookie_client_intersection(cookie_short_term_abs, client_categories_freq, normalize=True),
            'client_cookie_long_term_rel': lambda client_categories_freq, cookie_long_term_rel: get_cookie_client_intersection(cookie_long_term_rel, client_categories_freq, normalize=True)
        },
        'analyze': [
            'client_categories_freq'
        ]
    }
}

from Experiment import Experimenter
experiment = Experimenter(name=EXPERIMENT_NAME,
                          data_config=DATA_CONFIG,
                          model_config=MODEL_CONFIG,
                          threads=1,
                          greed_level=4,
                          cache_data=True,
                          cache_results=False,
                          cache_features=False)
experiment.run_experiment()

