import sys
sys.path.append('lib')

import json
import Queue

# Requires bottle 0.11+
import bottle
app = bottle.Bottle()

from metric import Metric
from graphitesender import GraphiteSender

@app.post('/publish/:api_key')
def publish(api_key):
    if api_key in valid_api_keys:
        try:
            body = bottle.request.body.read()
            metrics = json.loads(body)
        except:
            print body
            bottle.abort(400, "Unable to successfully parse JSON")

        if type(metrics) == list:
            for metric in metrics:
                try:
                    m = Metric(metric)
                    m.enqueue(q)
                except ValueError:
                    bottle.abort(400, "Invalid Metric Structure: %s" % (str(metric)))
                except Exception:
                    bottle.abort(500, "Unable to store metric!")
        else:
            bottle.abort(400, "Metric structure must be <list> containing n <dict> items")
    else:
        bottle.abort(403, "API Key not valid")


if __name__ == "__main__":
    # TODO: Implement API Key Handling
    valid_api_keys = ['af092bfcc9302']

    # TODO: Parameters / optparse
    try:
        q = Queue.Queue(maxsize=250000)
        sender_thread = GraphiteSender(q, '127.0.0.1', 2004)
        sender_thread.start()
        app.run(host='0.0.0.0', port=8080, debug=True)
    except Exception as e:
        print "Startup failed..."
        print e