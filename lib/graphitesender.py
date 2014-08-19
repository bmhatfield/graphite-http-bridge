import time
import struct
import pickle
import socket
import threading

import logging
log = logging.getLogger(__name__)

class GraphiteSender(threading.Thread):
    def __init__(self, metric_queue, host, port, udp=False):
        threading.Thread.__init__(self)
        self.daemon = True

        # Todo: Clean up parameters
        self.queue = metric_queue
        self.host = host
        self.port = port
        self.udp = udp
        self.batch = 250
        self.last_sent = time.time()
        self.send_every = 30
        self.timeout = 5.0
        self.enabled = True
        self.sendnow = False

        self.connect()

    def run(self):
        while self.enabled:
            if self.queue.qsize() >= self.batch or (self.send_overdue() and not self.queue.empty()):
                batch = []
                while len(batch) <= self.batch and not self.queue.empty():
                    batch.append(self.queue.get())

                if len(batch) > 0:
                    log.debug("Found metric batch with %s metrics" % (len(batch)))

                    try:
                        self.send(self.pickled(batch))
                        self.last_sent = time.time()
                    except Exception as e:
                        log.error("Exception Sending: %s" % e)
                        log.warning("Failed to send: re-enqueueing %s metrics" % (len(batch)))
                        for metric in batch:
                            self.queue.put(metric)
                        log.debug("Finished re-enqueueing messages")
                else:
                    self.sendnow = False
            else:
                log.debug("Nothing to send: sleeping %s" % self.send_every)
                time.sleep(self.send_every)

    def send_overdue(self):
        return time.time() - self.last_sent > self.send_every

    def pickled(self, tuples):
        payload = pickle.dumps(tuples)
        header = struct.pack("!L", len(payload))
        return header + payload

    def connect(self):
        if self.udp:
            adapter = socket.SOCK_DGRAM
        else:
            adapter = socket.SOCK_STREAM

        self.socket = socket.socket(socket.AF_INET, adapter)
        self.socket.settimeout(self.timeout)

        if self.socket is not None:
            self.socket.connect((self.host, self.port))
        else:
            raise IOError("Unable to create socket")

    def close(self):
        if self.socket is not None:
            self.socket.close()
        self.socket = None

    def send(self, data):
        try:
            if self.socket is None:
                self.connect()

            if self.socket is not None:
                log.debug("Sending pickled data...")
                self.socket.sendall(data)
                log.debug("Sent.")
            else:
                log.error("Socket unavailable.")
        except Exception as e:
            log.error("Send failed", e)
            self.close()
            raise e
