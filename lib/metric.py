import logging
log = logging.getLogger(__name__)

class Metric():
    def __init__(self, metric_dict):
        if self.valid(metric_dict):
            self.metric = metric_dict
        else:
            raise ValueError("Invalid Metric Structure")

    def valid(self, metric):
        for param in ['metric', 'value', 'timestamp']:
            if param not in metric:
                return False
        else:
            return True

    def enqueue(self, queue):
        queue.put((self.metric['metric'], (self.metric['timestamp'], self.metric['value'])))
