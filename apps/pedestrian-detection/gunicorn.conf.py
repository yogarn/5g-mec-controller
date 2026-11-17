"""Overlap HTTP I/O while keeping one model process and serialized inference."""
import os

bind = '0.0.0.0:5000'
# Multiple processes would duplicate the model and contend for the health port.
workers = 1
threads = int(os.getenv('HTTP_THREADS', '5'))
if threads < 1:
    raise ValueError('HTTP_THREADS must be a positive integer')
worker_class = 'sync' if threads == 1 else 'gthread'
timeout = 60
