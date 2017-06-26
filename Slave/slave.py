import threading
import time
from Slave.executor import *


class Slave(threading.Thread):
    # This class define the compute node of the system
    __default_executor_number = 4

    def __init__(self, slave_id, task_pool, executor_number=0, *args):
        # the args is the default constructor from superclass (Thread)
        self.cache = {}
        self.executor_number = executor_number
        self.worker_id_count = 0
        self.node_name = "Slave-" + str(slave_id)
        self.task_pool = task_pool
        if self.executor_number == 0:
            # Initial the executor here, the default would be 4 executors
            self.available_executor = self.__default_executor_number
        else:
            self.available_executor = executor_number

        super(Slave, self).__init__(*args)

    def run(self):
        while True:
            if not self.available():
                # wait for an average task time and then poll again
                # suppose that it will be 200 ms -> but 2s is used here for human time scale
                time.sleep(2)
                logging.debug('waiting 200 ms for available executor')
                continue
            elif self.available() and not self.task_pool.empty():
                # get the task from the queue if there exist an available executor and the queue is not empty
                task = self.task_pool.get()
                self.run_task(task.dataset, task.groupid)
            else:
                # available but no task to do simply pass
                continue

    def run_task(self, dataset, groupid, slots_needed=1):
        # allocate the resource
        self.allocate()
        # let the thread run
        worker_name = self.node_name + str(self.executor_number - self.available_executor)
        key = str(dataset) + "-" + str(groupid)
        cache_data = self.cache.get(key)
        if not cache_data:
            t = threading.Thread(name=worker_name, target=fetch_and_execute(), daemon=True, args=[self.cache, key])
        else:
            t = threading.Thread(name=worker_name, target=execute_with_cache(), daemon=True, args=[cache_data])

        # return the resource
        self.release()
        # Notify master after release the resource
        self.notify_master()

    def allocate(self, slots_allocated=1):
        # To make it simple we will run only one executor per allocation fist, the slots_allocated variable
        # will soon be used when multiple executor need to be allocated at a time
        self.available_executor -= slots_allocated

    def release(self, slots_released=1):
        # To make it simple we will run only one executor per allocation fist, the slots_allocated variable
        # will soon be used when multiple executor need to be allocated at a time
        self.available_executor += slots_released

    def notify_master(self):
        # Some Notifying Method
        pass

    def available(self):
        return self.available_executor > 0

    def __available_resources(self):
        return self.executor_number
