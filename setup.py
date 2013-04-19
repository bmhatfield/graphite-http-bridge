#!/usr/bin/env python
from distutils.core import setup

version = "0.1.1"

setup(name="graphite-http-bridge",
      version=version,
      description="Bottle-based HTTP bridge to Graphite",
      author="Brian Hatfield",
      author_email="bmhatfield@gmail.com",
      url="https://github.com/bmhatfield/graphite-http-bridge",
      package_dir={'': 'lib'},
      py_modules=['apiauth', 'graphitesender', 'metric'],
      data_files=[('/etc/init/', ["init/ubuntu/graphite-http-bridge.conf"])],
      scripts=["graphite-http-bridge.py"]
    )
