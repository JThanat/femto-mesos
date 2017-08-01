import logging
import threading
import time

from Queue import Queue
from dispatcher_exec import *

logging.basicConfig(filename="test.log",
                    level=logging.DEBUG,
                    format="(%(threadName)-10s) %(message)s",
                    )

class Dispatcher(threading.Thread):
    __default_executor_number = 4

    def __init__(self, client, available_thread=4, **kwargs):
        self.node_name = "dispatcher"
        self.work_queue = Queue()
        self.available_thread = available_thread
        self.client = client
        super(Dispatcher, self).__init__(**kwargs)

    def run(self):
        while True:
            if not self.available():
                time.sleep(1)
                logging.debug("waiting 100 ms for available thread")
                continue
            elif self.available() and not self.work_queue.empty():
                task = self.work_queue.get()
                if task:
                    self.run_task(dataset=task.get('dataset'), groupid=task.get('groupid'))
                else:
                    continue
            else:
                continue

    def run_task(self, dataset, groupid):
        worker_name = self.node_name + "-" + "worker"
        t = Dispatch_Executor(parent=self, name=worker_name, target=put_job, daemon=True, args=[self.client, dataset, groupid])
        t.start()

    def allocate(self):
        self.available_thread -= 1

    def release(self):
        self.available_thread += 1

    def available(self):
        return self.available_thread > 0
