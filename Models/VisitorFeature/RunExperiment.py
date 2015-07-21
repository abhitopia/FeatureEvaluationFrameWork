EXPERIMENT_NAME = 'VisitorFeature'
# This becomes the name of one single HDF5 file that contains the data and results. It will be stored in the same
# folder

from pandas import Series

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
                    feature_converter('visited_domains', IFNULL(td.vd_hour_coeffs,'N/A') ) AS visitor_info,
                    feature_converter('visited_logpoints', IFNULL(td.visited_logpoints::VARCHAR,'N/A') ) AS visited_logpoints,
                    feature_converter('user_agent', IFNULL(SPLIT_USERAGENT(td.user_agent)::VARCHAR,'N/A') ) AS user_agent,
                    feature_converter('url', CASE WHEN td.rtb_url = '' THEN td.website_id :: VARCHAR ELSE SPLIT_URL(td.rtb_url) :: VARCHAR END ) AS url,
                    feature_converter('inventory_source_id', CASE WHEN td.inventory_source_id = -1 THEN 'N/A' :: VARCHAR ELSE td.inventory_source_id :: VARCHAR END ) AS inventory_source_id,
                    feature_converter('label', CASE WHEN click_count = 0 THEN '-1'  ELSE '1' END) AS label
                FROM
                    temp.td_abhi AS td
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
            'DATA_TO_HOUR': '2015-05-07 00:00:00',
            'DATA_FROM_HOUR': '2015-05-01 00:00:00',
            'COUNTRY': 'DK'
        },
    # Specify the tags used test data set
    'test':
        {
            'DATA_TO_HOUR': '2015-05-08 00:00:00',
            'DATA_FROM_HOUR': '2015-05-07 00:00:00',
            'COUNTRY': 'DK'
        }
}


def num_vds(visitor_info, which):
    if visitor_info == '':
        return 0.0
    else:
        values = list(set(visitor_info.split(' ')))
        weights = Series(data=[float(ele.split('=')[1]) for ele in values])
        result = {
            'good': float(len(weights[weights > 0])),
            'bad': float(len(weights[weights < 0]))
        }[which]
        return result


def sum_weights(visitor_info, which):
    if visitor_info == '':
        return 0.0
    else:
        values = list(set(visitor_info.split(' ')))
        weights = Series(data=[float(ele.split('=')[1]) for ele in values])
        return {
            'good': weights[weights > 0].sum(),
            'bad': weights[weights < 0].sum()
        }[which]


def time_proximity(visitor_info, which):
    if visitor_info == '':
        return 0.0
    else:
        values = list(set(visitor_info.split(' ')))
        hours = Series(data=[float(ele.split('=')[0]) for ele in values])
        weights = Series(data=[float(ele.split('=')[1]) for ele in values])
        return {
            'good': (1.0/(hours[weights > 0])).sum(),
            'bad': (1.0/(hours[weights < 0])).sum()
        }[which]


def visit_score(visitor_info, which):
    if visitor_info == '':
        return 0.0
    else:
        values = list(set(visitor_info.split(' ')))
        hours = Series(data=[float(ele.split('=')[0]) for ele in values])
        weights = Series(data=[float(ele.split('=')[1]) for ele in values])
        return {
            'good': (weights[weights > 0]/(hours[weights > 0])).sum(),
            'bad': (weights[weights < 0]/(hours[weights < 0])).sum()
        }[which]


def ratio(num, den, mean_num, mean_den):
    num_value = num if num !=0 else mean_num
    den_value = den if den !=0 else mean_den
    return float(num_value)/float(den_value)


from Experiment.Constants import PRODUCTION_CPC_MODEL_TUNED
MODEL_CONFIG = {
    'production_model': PRODUCTION_CPC_MODEL_TUNED,
    'visitor_features': {
        # all the features specified in "inherit" becomes features for this model
        'inherit': ['production_model'],  # Thus, 'client_id, 'visited_domains', etc are part this model
        # This features are added in greedy fashion. This allow for Greedy Feature Analysis Experiment.
        'greedy features': {
            # visited domain counts
            'good_count': lambda visitor_info: num_vds(visitor_info, 'good'),
            'bad_count': lambda visitor_info: num_vds(visitor_info, 'bad'),
            'effective_count': lambda good_count, bad_count: good_count - bad_count,
            'ratio_count': lambda good_count, bad_count, mean_good_count, mean_bad_count: ratio(good_count, bad_count, mean_good_count, mean_bad_count),
            'total_count': lambda good_count, bad_count: good_count + bad_count,

            # visited domain weights
            'sum_good_weights': lambda visitor_info: sum_weights(visitor_info, 'good'),
            'sum_bad_weights': lambda visitor_info: sum_weights(visitor_info, 'bad'),
            'sum_weights': lambda sum_good_weights, sum_bad_weights: sum_good_weights + sum_bad_weights,
            'effective_weight': lambda sum_good_weights, sum_bad_weights: sum_good_weights - sum_bad_weights,
            'ratio_weights': lambda sum_good_weights, sum_bad_weights, mean_good_weights, mean_bad_weights: ratio(sum_good_weights, sum_bad_weights, mean_good_weights, mean_bad_weights),

            # # time proximity with visited domains
            'good_time_proximity': lambda visitor_info: time_proximity(visitor_info, 'good'),
            'bad_time_proximity': lambda visitor_info: time_proximity(visitor_info, 'bad'),
            'sum_time_proximity': lambda good_time_proximity, bad_time_proximity: good_time_proximity + bad_time_proximity,
            'effective_time_proximity': lambda good_time_proximity, bad_time_proximity: float(good_time_proximity) - float(bad_time_proximity),
            'ratio_time_proximity': lambda good_time_proximity, bad_time_proximity, mean_good_tp, mean_bad_tp: ratio(good_time_proximity, bad_time_proximity, mean_good_tp, mean_bad_tp),

            # visited goodness score that combines weights and recency:avg
            'good_visit_score': lambda visitor_info: visit_score(visitor_info, 'good'),
            'bad_visit_score': lambda visitor_info: visit_score(visitor_info, 'bad'),
            'sum_visit_score': lambda good_visit_score, bad_visit_score: good_visit_score + bad_visit_score,
            'effective_visit_score': lambda good_visit_score, bad_visit_score: good_visit_score - bad_visit_score,
            'ratio_visit_score': lambda good_visit_score, bad_visit_score, mean_good_vs, mean_bad_vs: ratio(good_visit_score, bad_visit_score, mean_good_vs, mean_bad_vs),

            # avg visited goodness score that combines weights and recency
            'good_visit_score_avg': lambda good_visit_score, good_time_proximity, mean_good_vs, mean_good_tp: ratio(good_visit_score, good_time_proximity, mean_good_vs, mean_good_tp),
            'bad_visit_score_avg': lambda bad_visit_score, bad_time_proximity, mean_bad_vs, mean_bad_tp: ratio(bad_visit_score, bad_time_proximity, mean_bad_vs, mean_bad_tp),
            'sum_visit_score_avg': lambda good_visit_score_avg, bad_visit_score_avg: good_visit_score_avg + bad_visit_score_avg,
            'effective_visit_score_avg': lambda good_visit_score_avg, bad_visit_score_avg: good_visit_score_avg - bad_visit_score_avg,
            'ratio_visit_score_avg': lambda good_visit_score_avg, bad_visit_score_avg, mean_good_vsa, mean_bad_vsa: ratio(good_visit_score_avg, bad_visit_score_avg, mean_good_vsa, mean_bad_vsa)

            # # avg time proximity with visited domains
            # 'good_time_proximity_avg': lambda good_time_proximity, good_count: good_time_proximity/good_count if good_count != 0 else '',
            # 'bad_time_proximity_avg': lambda bad_time_proximity, bad_count: bad_time_proximity/bad_count if bad_count != 0 else '',
            # 'sum_time_proximity_avg': lambda good_time_proximity_avg, bad_time_proximity_avg: good_time_proximity_avg+bad_time_proximity_avg,
            # 'ratio_time_proximity_avg': lambda good_time_proximity_avg, bad_time_proximity_avg: good_time_proximity_avg/bad_time_proximity_avg if bad_time_proximity_avg != 0 else '',
            # 'effective_time_proximity_avg': lambda good_time_proximity_avg, bad_time_proximity_avg: good_time_proximity_avg - bad_time_proximity_avg,
        },
        'aggregate values': {
            'mean_bad_count': lambda bad_count: bad_count.mean(),
            'mean_good_count': lambda good_count: good_count.mean(),
            'mean_good_weights': lambda sum_good_weights: sum_good_weights.mean(),
            'mean_bad_weights': lambda sum_bad_weights: sum_bad_weights.mean(),
            'mean_good_tp': lambda good_time_proximity: good_time_proximity.mean(),
            'mean_bad_tp': lambda bad_time_proximity: bad_time_proximity.mean(),
            'mean_good_vs': lambda good_visit_score: good_visit_score.mean(),
            'mean_bad_vs': lambda bad_visit_score: bad_visit_score.mean(),
            'mean_good_vsa': lambda good_visit_score_avg: good_visit_score_avg.mean(),
            'mean_bad_vsa': lambda bad_visit_score_avg: bad_visit_score_avg.mean()
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