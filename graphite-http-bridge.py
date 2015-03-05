#!/usr/bin/env python

# Standard Python Libraries
import os
import sys
import json
import Queue
import logging
import optparse

# Third Party Libraries
# http://pyyaml.org/
import yaml

# https://pypi.python.org/pypi/python-daemon/
import daemon

# http://bottlepy.org/docs/dev/
# Built against bottle 0.12-dev
import bottle

# https://github.com/bmhatfield/python-pidfile
import pidfile

# Application Libraries
sys.path.append('lib')
from metric import Metric
from apiauth import APIAuthenticator
from graphitesender import GraphiteSender

parser = optparse.OptionParser()
parser.add_option("--graphite-host", dest="graphite_host", default="127.0.0.1", help="Host that Graphite is running on")
parser.add_option("--pickle-port", dest="graphite_port", default=2004, help="Port that Graphite is running Carbon's pickle listener on")
parser.add_option("--local-port", dest="local_port", default=8080, help="Port that Graphite is running on")
parser.add_option("--max-queue-depth", dest="max_depth", default=250000, help="Maximum size of metric queue before metrics are rejected")
parser.add_option("--api-conf", dest="api_conf", default="api_keys.yml", help="Yaml file containing 'keys: [valid, key]'")
parser.add_option("--run-dir", dest="run_directory", default=".", help="Directory for where pidfiles should be placed")
parser.add_option("--log-dir", dest="log_directory", default="/var/log", help="Directory for where logs should be placed")
parser.add_option("--foreground", dest="foreground", action='store_true', default=False, help="Don't daemonize")
parser.add_option("--reloader", dest="reloader", action='store_true', default=False, help="Reload on module changes")
parser.add_option("--debug", dest="debug", action='store_true', default=False, help="Increase logger verbosity")
(options, args) = parser.parse_args()

# Load Valid API Key YAML File
if os.path.isfile(options.api_conf):
    with open(options.api_conf) as api_config:
        api_conf = yaml.safe_load(api_config.read())
else:
    sys.exit("Unable to load Valid API keys.")

# Configure logging module
log_path = os.path.join(options.log_directory, 'bridge.log')

log = logging.getLogger()
if options.debug:
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.INFO)

if options.foreground:
    log_output = logging.StreamHandler()
else:
    log_output = logging.FileHandler(log_path)

log_output.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
log.addHandler(log_output)

# Create Bottle Application
app = bottle.Bottle()

# Create API Authenticator (Keys added later)
authenticator = APIAuthenticator()

# Create Threadsafe Queue
q = Queue.Queue(maxsize=options.max_depth)

# Configure the main route: the API POST route
@app.post('/publish/:api_key')
def publish(api_key):
    if authenticator.valid(api_key):
        try:
            body = bottle.request.body.read()
            metrics = json.loads(body)
        except:
            log.error("Request unparsable: %s" % (body))
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

@app.post('/metrics/lines/:api_key')
def publish(api_key):
    if authenticator.valid(api_key):
        body = bottle.request.body.read()
        metrics = body.readlines()

        if type(metrics) == list:
            for line in metrics:
                try:
                    parts = line.split()

                    if len(parts) != 3:
                        continue
                    else:
                        metric = {
                            'metric': parts[0],
                            'value': parts[1],
                            'timestamp': parts[2]
                        }

                    m = Metric(metric)
                    m.enqueue(q)
                except ValueError:
                    bottle.abort(400, "Invalid Metric Structure: %s" % (str(metric)))
                except Exception:
                    bottle.abort(500, "Unable to store metric!")
        else:
            bottle.abort(400, "Metric structure must be lines of 'metric.path value timestamp'")
    else:
        bottle.abort(403, "API Key not valid")


def main():
    try:
        if 'keys' in api_conf:
            for key in api_conf['keys']:
                authenticator.add(key)
        else:
            log.error("'keys' not in dict: %s" % (str(api_conf)))

        sender_thread = GraphiteSender(q, options.graphite_host, options.graphite_port)
        sender_thread.start()

        app.run(server='paste', host='0.0.0.0', port=options.local_port,
                debug=options.debug, reloader=options.reloader)
    except Exception as e:
        log.error("STARTUP FAILED")
        log.error(str(e))


if __name__ == "__main__":
    pidpath = os.path.join(options.run_directory, 'bridge.pid')

    if len(args) == 0 or 'start' in args:
        if options.foreground:
            main()
        else:
            try:
                with daemon.DaemonContext(working_directory=options.run_directory,
                                          pidfile=pidfile.PidFile(pidpath), files_preserve=[log_output.stream]):
                    main()
            except (Exception, SystemExit) as e:
                with open(log_path, 'a+') as fh:
                    fh.write(str(e) + "\n")
    elif 'stop' in args:
        try:
            with open(pidpath) as ph:
                pid = ph.read()
                os.kill(int(pid), 15)
        except IOError as e:
            if e.errno == 2:
                raise SystemExit("Pidfile not found - is the process running?")
