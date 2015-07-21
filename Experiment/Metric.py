class Metric:
    SMALL_NUMBER = -10000000
    LARGE_NUMBER = 10000000
    metric_worst_value = {
        'AUC': SMALL_NUMBER,
        'Precision Recall AUC': SMALL_NUMBER,
        'Log Likelihood': LARGE_NUMBER,
        'Mean Squared Error': LARGE_NUMBER,
        'Relative Information Gain': SMALL_NUMBER
    }

    def __init__(self, metric_name, metric_value=None):
        self.metric_name = metric_name
        if metric_value is None:
            self.metric_value = self.metric_worst_value[metric_name]
        else:
            self.metric_value = metric_value

    def is_better_than(self, another_metric):
        assert isinstance(another_metric, Metric)
        valid_metrics = {
            'AUC': self.metric_value > another_metric.metric_value,
            'Precision Recall AUC': self.metric_value > another_metric.metric_value,
            'Log Likelihood': self.metric_value < another_metric.metric_value,
            'Mean Squared Error': self.metric_value < another_metric.metric_value,
            'Relative Information Gain': self.metric_value > another_metric.metric_value
        }
        if self.metric_name == another_metric.metric_name:
            return valid_metrics[self.metric_name]

    def is_worse_than(self, another_metric):
        return not self.is_better_than(another_metric)