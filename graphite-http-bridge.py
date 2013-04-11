import sys
import json
import time
import struct
import pickle
import socket
import threading
import Queue

# Requires bottle 0.11+
import bottle
app = bottle.Bottle()

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
        self.last_sent = 0
        self.send_every = 60
        self.enabled = True
        self.sendnow = False

        self.socket = self.connect()

    def run(self):
        while self.enabled:
            if self.queue.qsize() > self.batch or self.sendnow:
                batch = []
                while not self.queue.empty():
                    batch.append(self.queue.get())

                if len(batch) > 0:
                    self.send(self.pickled(batch))
                    self.last_sent = time.time()
                    self.sendnow = False
            else:
                if time.time() - self.last_sent > 60:
                    self.sendnow = True
                else:
                    time.sleep(1)

    def pickled(self, tuples):
        payload = pickle.dumps(tuples)
        header = struct.pack("!L", len(payload))
        return header + payload

    def connect(self):
        # TODO: Smoother connection handling
        if self.udp:
            adapter = socket.SOCK_DGRAM
        else:
            adapter = socket.SOCK_STREAM

        sock = socket.socket(socket.AF_INET, adapter)

        if sock is not None:
            sock.connect((self.host, self.port))
            return sock
        else:
            raise IOError("Unable to connect to Graphite")

    def send(self, data):
        # TODO: Handle connection errors!
        if self.socket is None:
            self.socket = self.connect()
            
        self.socket.sendall(data)


@app.post('/publish/:api_key')
def publish(api_key):
    if api_key in valid_api_keys:
        try:
            body = bottle.request.body.read()
            params = json.loads(body)
        except:
            bottle.abort(400, "Unable to successfully parse JSON")

        for param in ['metric', 'value', 'timestamp']:
            if param not in params:
                bottle.abort(400, "JSON Missing Value: %s" % (param))                

        try:
            q.put((params['metric'], (params['timestamp'], params['value'])))
        except Exception as e:
            print e
            bottle.abort(500, "Unable to store metric!")
    else:
        bottle.abort(403, "API Key not valid")


if __name__ == "__main__":
    # TODO: Implement API Key Handling
    valid_api_keys = ['af092bfcc9302']

    # TODO: Parameters / optparse
    q = Queue.Queue(maxsize=250000)
    sender_thread = GraphiteSender(q, '127.0.0.1', 2004)
    sender_thread.start()
    app.run(host='0.0.0.0', port=8080, debug=True)