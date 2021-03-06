import logging
import threading
import time
from random import *

from zookeeper.job import Jobstate

logging.basicConfig(filename="running.log",
                    level=logging.DEBUG,
                    format="(%(threadName)-10s) %(message)s",
                    )


class Executor(threading.Thread):
    def __init__(self, job_path, parent=None, **kwargs):
        self.parent = parent
        self.job_path = job_path
        super(Executor, self).__init__(**kwargs)

    def run(self):
        self.parent.allocate()
        super(Executor, self).run()
        self.parent.update_state(Jobstate.SUCCESSFUL)
        self.parent.release()


logging.basicConfig(level=logging.DEBUG,
                    format="(%(threadName)-10s) %(message)s",
                    )


def execute_with_cache(cache_data):
    """thread worker function with cache"""
    t = threading.currentThread()
    pause = randint(1, 3)
    logging.debug("working with %s", str(cache_data))
    time.sleep(pause)
    logging.debug("finish")


def fetch_and_execute(cache, key):
    """thread worker function for worker without cache"""
    t = threading.currentThread()
    fetching_time = randint(2, 4)
    time.sleep(fetching_time)
    logging.debug("fetching %s", key)
    cache[key] = key
    execution_time = randint(1, 2)
    time.sleep(execution_time)
    logging.debug("executing %s", cache[key])
    logging.debug("finish")
    print "finish executing {cache}".format(cache=cache[key])
