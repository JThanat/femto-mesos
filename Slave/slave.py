from queue import Queue

from Slave.executor import *


class Slave(threading.Thread):
    # This class define the compute node of the system
    __default_executor_number = 4

    def __init__(self, slave_id, task_pool, executor_number=2, **kwargs):
        # the args is the default constructor from superclass (Thread)
        self.cache = {}
        self.executor_number = executor_number
        self.executor_queue = Queue()
        self.worker_id_count = 0
        self.node_name = "Slave" + str(slave_id)
        self.task_pool = task_pool
        self.available_executor = self.executor_number

        super(Slave, self).__init__(**kwargs)

    def run(self):
        while True:
            if not self.available():
                # wait for an average task time and then poll again
                # suppose that average execution time will be 400 ms -> but 4s is used here for human time scale
                time.sleep(4)
                logging.debug("waiting 400 ms for available executor")
                continue
            elif self.available() and not self.task_pool.empty():
                # get the task from the queue if there exist an available executor and the queue is not empty
                task = self.task_pool.get()
                self.run_task(task["dataset"], task["groupid"])
            else:
                # available but no task to do simply pass
                continue

    def run_task(self, dataset, groupid, slots_needed=1):
        # let the thread run
        worker_name = self.node_name + "-" + "worker"
        key = str(dataset) + "-" + str(groupid)
        cache_data = self.cache.get(key)
        if not cache_data:
            t = Executor(parent=self, name=worker_name, target=fetch_and_execute, daemon=True, args=[self.cache, key])
        else:
            t = Executor(parent=self, name=worker_name, target=execute_with_cache, daemon=True, args=[cache_data])
        t.start()

    def allocate(self, slots_allocated=1):
        # To make it simple we will run only one executor per allocation fist, the slots_allocated variable
        # will soon be used when multiple executor need to be allocated at a time
        self.available_executor -= slots_allocated
        print("Available Executor: " + str(self.__available_resources()))

    def release(self, slots_released=1):
        # To make it simple we will run only one executor per allocation fist, the slots_allocated variable
        # will soon be used when multiple executor need to be allocated at a time
        self.available_executor += slots_released
        print("Available Executor: " + str(self.__available_resources()))

    def notify_master(self):
        # Some Notifying Method
        if self.task_pool.qsize == 0:
            print("Notifying master")

    def available(self):
        return self.available_executor > 0

    def __available_resources(self):
        return self.available_executor
