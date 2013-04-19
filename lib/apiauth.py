import logging
log = logging.getLogger(__name__)

class APIAuthenticator():
    def __init__(self):
        self.valid_keys = set()

    def valid(self, key):
        return key in self.valid_keys

    def add(self, key):
        log.debug("Adding API key: %s" % (key))
        self.valid_keys.add(key)

    def remove(self, key):
        log.debug("Removing API key: %s" % (key))
        self.valid_keys.discard(key)
