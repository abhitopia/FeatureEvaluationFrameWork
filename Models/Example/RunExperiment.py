EXPERIMENT_NAME = 'ExampleExperiment'
# This becomes the name of one single HDF5 file that contains the data and results. It will be stored in the same
# folder


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
            'DATA_TO_HOUR': '2015-04-20 10:00:00',
            'DATA_FROM_HOUR': '2015-04-20 09:00:00',
            'COUNTRY': 'DK'
        },
    # Specify the tags used test data set
    'test':
        {
            'DATA_TO_HOUR': '2015-04-20 11:00:00',
            'DATA_FROM_HOUR': '2015-04-20 10:00:00',
            'COUNTRY': 'DK'
        }
}


# Following dictionary specifies the models to be trained and tested on.
# Note that it inherently, does MODEL EVALUATION on different models
# Also, for experiments like clicker. One can specify arbitrarily complex CUSTOM FEATURE definitions here.
MODEL_CONFIG = {
    # The keys are model names.
    'base_model': {
        'features': {
            # Specify the features used by this model.
            # The value as None means either the feature is a column that was fetched in SELECT statement or
            # it is a interaction feature.
            # Also, note that there is no need to Specify is the feature is CATEGORICAL or NUMERICAL.
            # Features which have fractional values automatically used as NUMERICAL.
            # NOTE: only features which have decimal values become NUMERICAL features, as such make sure your custom
            # numerical feature return float(value). Nan are automatically replaced by mean values.
            # Also, numerical features are automatically normalized. Test dataset uses params from Train dataset.
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
            'clicker': None
        }
    },
    'number of visited domains': {
        # all the features specified in "inherit" becomes features for this model
        'inherit': ['base_model'],  # Thus, 'client_id, 'visited_domains', etc are part this model
        'features': {
            # specify features of this model not already part of inherited models.
            'num_visited_domains': lambda visited_domains: float(len(visited_domains.split(' '))),  # custom feature
            # If the value specified is not None, then it must be a function with arguments as simple features.
            # Here for example, num_visited_domains calculated the number of visited domains.
            # NOTE: float value is returned to make sure this is treated as Numerical feature
            'nvd_normalized': lambda num_visited_domains, min_nvd, max_nvd: (num_visited_domains - min_nvd)/(max_nvd - min_nvd)
        },
        # This features are added in greedy fashion. This allow for Greedy Feature Analysis Experiment.
        'greedy features': {
            'placement_id': None,
            'inventory_source_id': None
        },
        # contains the features to not be part of this model.
        'ignore': ['visited_domains',
                   'num_visited_domains'],  # Here the visited_domains feature is inherited from 'base_model' but will be
        # ignored and not used for this model.

        # The following are applied at the end of dependent features are built
        # Note: these are calculated only for train data set and passed over to test etc.
        'aggregate values': {
            'min_nvd': lambda num_visited_domains: num_visited_domains.min(),
            'max_nvd': lambda num_visited_domains: num_visited_domains.max(),
            'mean_nvd': lambda num_visited_domains: num_visited_domains.mean()
        },
        'learners': {
            # The key is the learner name. Currently only VW is supported
            'VW': {
                'l1': [11, 12, 13, 14],  # for example both l1=7.0 and 8.0 are evaluated here
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

# Additionally, adding new functionality is easier now as all the code is written in modular OOP fashion.
# results of the experiment are saved in the same data file.
# To run this experiment,

# python Experiment
from Experiment import Experimenter
experiment = Experimenter(name=EXPERIMENT_NAME,
                          data_config=DATA_CONFIG,
                          model_config=MODEL_CONFIG,
                          threads=4,
                          greed_level=2,
                          cache_data=True,
                          cache_results=True,
                          cache_features=True)

experiment.run_experiment()
