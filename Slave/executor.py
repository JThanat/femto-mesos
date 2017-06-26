import threading
import time
import logging
from random import random

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )


def execute_with_cache(cache_data):
    """thread worker function with cache"""
    t = threading.currentThread()
    pause = random() * 10
    logging.debug('working with', str(cache_data))
    time.sleep(pause)
    logging.debug('finish')


def fetch_and_execute(cache, key: str):
    """thread worker function for worker without cache"""
    t = threading.currentThread()
    fetching_time = random() * 5
    time.sleep(fetching_time)
    logging.debug('fetching %s', key)
    cache[key] = key
    execution_time = random()
    time.sleep(execution_time)
    logging.debug('executing %s', cache['key'])
    logging.debug('finish')
