import time
import struct
import pickle
import socket
import threading

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
            if self.queue.qsize() > self.batch or (self.send_overdue() and not self.queue.empty()):
                batch = []
                while not self.queue.empty():
                    batch.append(self.queue.get())

                if len(batch) > 0:
                    print "Found metric batch with %s metrics" % (len(batch))

                    try:
                        self.send(self.pickled(batch))
                        self.last_sent = time.time()
                    except:
                        print "Failed to send: re-enqueueing %s metrics" % (len(batch))
                        for metric in batch:
                            self.queue.put(metric)
                        time.sleep(self.send_every)
                else:
                    self.sendnow = False
            else:
                time.sleep(1)

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
                print "Sending pickled data..."
                self.socket.sendall(data)
                print "Sent."
            else:
                print "Socket unavailable."
        except Exception as e:
            print "Send failed", e
            self.close()
            raise e