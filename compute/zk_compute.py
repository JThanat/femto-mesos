import uuid
import json

from executor import *
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.retry import ForceRetryError


class Slave(threading.Thread):
    # This class define the compute node of the system.
    # This will represents a cluster of compute node using thread for each worker
    __default_executor_number = 4

    def __init__(self, client, path, slave_id=None, executor_number=1, **kwargs):

        """
       :param client: zookeeper client
       :param path: the path to zookeeper work directory
       :param slave_id: a unique string used to identify slave
       :param executor_number: number of limited executor of this Slave
       :param kwargs: the default constructor from superclass (Thread)
       """
        self.cache = {}
        self.client = client
        self.executor_number = executor_number
        self.worker_id_count = 0
        self.available_executor = self.executor_number
        self.slave_id = uuid.uuid4() if not slave_id else slave_id
        self.wait_count = 1
        self.default_wait = self.wait_count
        self.unowned_job = []

        self.path = path
        self.unowned_path = self.path + "/unowned_path"
        self.owned_path = self.path + "/owned_path"
        self.structured_paths = (self.path, self.unowned_path, self.owned_path)
        self.ensured_path = False

        self._ensure_paths()

        super(Slave, self).__init__(**kwargs)

    def _ensure_paths(self):
        if not self.ensured_path:
            for path in self.structured_paths:
                self.client.ensure_path(path)
            self.ensured_path = True

    def get_job_from_list(self):
        """
        get an appropriate job to execute
        :return: job_id or None
        """
        for job in self.unowned_job:
            job_id = job.split("-")[-1]
            if self.cache.get(job_id):
                return self.get_job(job)
        return None

    def get_job(self, entry):
        path = self.unowned_path + "/" + str(entry)
        return self.client.retry(self._inner_get, (path,))

    def _inner_get(self, path):
        try:
            data, stat = self.client.get(path)
        except NoNodeError:
            # the first node has vanished in the meantime, try to
            # get another one
            raise ForceRetryError()
        try:
            self.client.delete(path)
        except NoNodeError:
            # we were able to get the data but someone else has removed
            # the node in the meantime. consider the item as processed
            # by the other process
            raise ForceRetryError()
        del self.unowned_job[:]
        return data

    def run(self):
        while True:
            if not self.available():
                # wait for an average task time and then poll again
                # suppose that average execution time will be 400 ms -> but 4s is used here for human time scale
                time.sleep(4)
                logging.debug("waiting 400 ms for available executor")
                continue
            else:
                # the name of each ZNode will be in the form of
                # entry-created_order-dataset:groupid
                # job_id should be identified by dataset:groupid
                self.unowned_job = self.client.get_children('/unowned')
                self.unowned_job.sort()

                job = self.get_job_from_list()

                if job:
                    self.execute_job(job)
                elif self.wait_for_work():
                    time.sleep(1)
                    logging.debug("waiting for a new job that satisfy")
                else:
                    # get the oldest job
                    self.unowned_job.sort()
                    job = self.get_job(self.unowned_job[0])
                    self.execute_job(job)

    def wait_for_work(self):
        if self.wait_count == 0:
            self.wait_count = self.default_wait
            return False
        else:
            self.wait_count -= 1
            return True

    def execute_job(self, job):
        job_object = json.loads(job)
        self.run_task(dataset=job_object.get('dataset'), groupid=job_object.get('groupid'), slots_needed=1)

    def run_task(self, dataset, groupid, slots_needed=1):
        # let the thread run
        worker_name = "worker"
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
