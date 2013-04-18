graphite-http-bridge
====================

Bridges HTTP POST requests in batches to a Carbon socket.

Usage
=====

Install the application, and then run `graphite-http-bridge.py --foreground` to test.

Sending Data
============

Send JSON:
`[{"metric": 'some.metric.path', "value": 42, "timestamp": 1234567890}]`

Configuration
=============

All configuration is command line, with the exception of the api\_keys.yml file. Copy from api\_keys.yml.example.
