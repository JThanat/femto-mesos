import threading
import time
import logging
from random import random


logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )


def execute_with_cache():
    """thread worker function with cache"""
    t = threading.currentThread()
    pause = random()
    logging.debug('working %s', pause)
    time.sleep(pause)
    logging.debug('finish')

def fetch_and_execute():
    """thread worker function for worker without cache"""
    t = threading.currentThread()
    fetching_time = random()*5
    time.sleep(fetching_time)
    logging.debug('fetching %s', fetching_time)

    execution_time = random()
    time.sleep(execution_time)
    logging.debug('executing %s', execution_time)
    logging.debug('finish')









