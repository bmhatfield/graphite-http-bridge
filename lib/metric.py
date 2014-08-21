import Queue

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

    def discard(self):
        log.warning("Queue full: dropping metric '%s' at %s (%s)" % (self.metric['metric'], self.metric['timestamp'], self.metric['value']))

    def enqueue(self, queue):
        if not queue.full():
            try:
                queue.put((self.metric['metric'], (self.metric['timestamp'], self.metric['value'])), block=True, timeout=1)
            except Queue.Full:
                self.discard()
        else:
            self.discard()
