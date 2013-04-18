## {{{ http://code.activestate.com/recipes/577911/ (r2)
import fcntl
import os

import logging
log = logging.getLogger(__name__)

class PidFile(object):
    """Context manager that locks a pid file.  Implemented as class
    not generator because daemon.py is calling .__exit__() with no parameters
    instead of the None, None, None specified by PEP-343."""
    # pylint: disable=R0903

    def __init__(self, path):
        self.path = path
        self.pidfile = None

    def __enter__(self):
        try:
            self.pidfile = open(self.path, "a+")
            fcntl.flock(self.pidfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            raise SystemExit("Unable to lock pidfile: %s\n%s" % (self.path, e))
        self.pidfile.seek(0)
        self.pidfile.truncate()
        self.pidfile.write(str(os.getpid()))
        self.pidfile.flush()
        self.pidfile.seek(0)
        return self.pidfile

    def __exit__(self, exc_type=None, exc_value=None, exc_tb=None):
        try:
            self.pidfile.close()
        except IOError as err:
            # ok if file was just closed elsewhere
            if err.errno != 9:
                raise
        os.remove(self.path)
