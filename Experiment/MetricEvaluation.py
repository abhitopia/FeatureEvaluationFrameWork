import numpy as np
from sklearn.metrics import precision_recall_curve, auc, roc_auc_score


class MetricEvaluation():
    def __init__(self):
        pass

    @staticmethod
    def current_metrics():
        return ['AUC', 'Expected CTR', 'Real CTR', 'Precision Recall AUC', 'Log Likelihood', 'Mean Squared Error',
                'Relative Information Gain']

    # The current implementation, which is taken directly from the Sklearn website, uses
    # linear interpolation to calculate the area under the curve. This is not the correct 
    # procedure for a precision-recall curve, and so may given inaccurate results when the 
    # precision-recall pairs are well separated. 
    # See "The Relationship Between Precision-Recall and Roc Curves" by Jesse Davis and Mark 
    # Goadrich for more details.
    @staticmethod
    def calculate_precision_recall_auc(labels, probabilities):
        precision, recall, thresholds = precision_recall_curve(labels, probabilities)
        return auc(recall, precision)

    # Calculate the average log-likelihood of the examples in the data set.
    @staticmethod
    def calculate_log_like(labels, probabilities):
        epsilon = 1e-10
        probabilities[probabilities == 1] = 1 - epsilon
        probabilities[probabilities == 0] = epsilon
        # 0.5*(labels+1) changes labels from {-1,1} format to {0,1} format.
        return np.mean(0.5 * (labels + 1) * np.log(probabilities) + 0.5 * (1 - labels) * np.log(1 - probabilities))

    @staticmethod
    def calculate_mean_square_error(labels, probabilities):
        # Calculate the mean square error.
        # 0.5*(labels+1) changes labels from {-1,1} format to {0,1} format.
        return np.mean((0.5 * (labels + 1) - probabilities) ** 2)

    # This function calculates the entropy of a given probability distribution over
    # a finite set of events.
    @staticmethod
    def calculate_entropy(prob_dist):
        return -np.dot(prob_dist, np.log(prob_dist))

    # This function calculates the empirical distribution over a finite set of events.
    # obs_vec - vector of independent observations from a finite observation space.
    # obs_space - Finite set of possible observations. If not given, then this is taken to be
    # the set of unique events in obs_vec.
    @staticmethod
    def calculate_discrete_distribution(obs_vec, obs_space=None):

        # If necessary, calculate the observation space.
        if obs_space is None:
            obs_space = np.unique(obs_vec)

        obs_space_size = obs_space.shape[0]
        p_obs = np.zeros(obs_space_size)

        for i in np.arange(obs_space_size):
            p_obs[i] = np.sum(obs_vec == obs_space[i])

        return p_obs / np.float(np.sum(p_obs))

    @staticmethod
    def calculate_class_label_entropy(labels):
        # Calculate the empirical distribution of the class labels.
        p_class_labels = MetricEvaluation.calculate_discrete_distribution(labels)
        return MetricEvaluation.calculate_entropy(p_class_labels)

    @staticmethod
    def calculate_relative_information_gain(labels, probabilities, probabilities_base=None):
        # Calculate the log-likelihood of the new model
        log_like_new = MetricEvaluation.calculate_log_like(labels, probabilities)

        if probabilities_base is None:
            # Calculate entropy of class labels
            class_label_ent = MetricEvaluation.calculate_class_label_entropy(labels)
            # Calculate the relative information gain of the new model
            rig = (class_label_ent + log_like_new) / class_label_ent
        else:
            # Calculate the log-likelihood of the base model
            log_like_base = MetricEvaluation.calculate_log_like(labels, probabilities_base)
            # Calculate the relative information gain of the new model
            rig = (log_like_base - log_like_new) / log_like_base

        return rig

    @staticmethod
    def calculate_expected_positive_label_rate(probabilities):
        return np.mean(probabilities)

    @staticmethod
    def calculate_real_positive_label_rate(labels):
        return float(np.sum(labels == 1)) / labels.shape[0]

    @staticmethod
    def get_metrics(labels, probabilities):
        metrics = {
            'AUC': roc_auc_score(labels, probabilities),
            'Expected CTR': MetricEvaluation.calculate_expected_positive_label_rate(probabilities),
            'Real CTR': MetricEvaluation.calculate_real_positive_label_rate(labels),
            'Precision Recall AUC': MetricEvaluation.calculate_precision_recall_auc(labels, probabilities),
            'Log Likelihood': MetricEvaluation.calculate_log_like(labels, probabilities),
            'Mean Squared Error': MetricEvaluation.calculate_mean_square_error(labels, probabilities),
            'Relative Information Gain': MetricEvaluation.calculate_relative_information_gain(labels, probabilities)
        }
        return metrics