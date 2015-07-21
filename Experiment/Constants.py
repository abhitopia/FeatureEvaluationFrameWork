from itertools import cycle
import matplotlib.pyplot as plt

VERTICA_CONNECTION = {'host': '10.1.24.75',
                    'port': 5433,
                    'user': 'dbadmin',
                    'password': 'MUEKGZ4edpSw2zGFXt2eht',
                    'database': 'dspr'}


# As on 20th May, 2015
PRODUCTION_CPC_MODEL = {
    'features': {
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
            'l1': [7],  # for example both l1=7.0 and 8.0 are evaluated here
            'passes': [1],
            'ftrl_alpha': [0.1],
            'ftrl_beta': [0],
            'kill_cache': [''],
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


# Following gives AUC 0.8277
PRODUCTION_CPC_MODEL_TUNED = {
    'features': {
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


COLOR_MAP = cycle([#(plt.cm.binary, plt.cm.binary_r),
                   (plt.cm.YlOrRd, plt.cm.YlOrRd_r), 
                   (plt.cm.PuBuGn, plt.cm.PuBuGn_r), 
                   (plt.cm.BuGn, plt.cm.BuGn_r), 
                   (plt.cm.BuPu, plt.cm.BuPu_r), 
                   (plt.cm.OrRd, plt.cm.OrRd_r),
                   (plt.cm.gist_yarg, plt.cm.gist_yarg_r), 
                   (plt.cm.GnBu, plt.cm.GnBu_r), 
                   (plt.cm.RdPu, plt.cm.RdPu_r), 
                   (plt.cm.Greens, plt.cm.Greens_r), 
                   (plt.cm.Greys, plt.cm.Greys_r), 
                   (plt.cm.Blues, plt.cm.Blues_r),
                   (plt.cm.Oranges, plt.cm.Oranges_r), 
                   (plt.cm.PuBu, plt.cm.PuBu_r), 
                   (plt.cm.PuRd, plt.cm.PuRd_r), 
                   (plt.cm.YlGnBu, plt.cm.YlGnBu_r), 
                   (plt.cm.Purples, plt.cm.Purples_r), 
                   (plt.cm.YlGn, plt.cm.YlGn_r),
                   (plt.cm.Reds, plt.cm.Reds_r), 
                   (plt.cm.YlOrBr, plt.cm.YlOrBr_r)])