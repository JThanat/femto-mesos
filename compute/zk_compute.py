import json
import uuid

from kazoo.exceptions import NoNodeError
from kazoo.retry import ForceRetryError

from executor import *
from zookeeper.job import Jobstate


class Slave(threading.Thread):
    # This class define the compute node of the system.
    # This will represents a cluster of compute node using thread for each worker
    __default_executor_number = 4
    prefix = "entry-"

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
        self.unowned_path = self.path + "/unowned"
        self.owned_path = self.path + "/owned"
        self.running_job_path = ""
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
        return self.client.retry(self._inner_get, path)

    def get_job_from_path(self, path):
        return self.client.retry(self._inner_get_for_update, path)

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

    def _inner_get_for_update(self, path):
        try:
            data, stat = self.client.get(path)
        except NoNodeError:
            raise ForceRetryError()

        return data

    def own_job(self, value, priority=100):
        # move job to owned part
        self._check_put_arguments(value, priority)
        self._ensure_paths()
        value_dict = json.loads(value)
        value_dict["state"] = Jobstate.RUNNING
        value_dict["worker"] = str(self.slave_id)
        path = '{path}/{prefix}{priority:03d}-{dataset}:{groupid}-'.format(
            path=self.owned_path, prefix=self.prefix, priority=priority,
            dataset=value_dict.get("dataset"),
            groupid=value_dict.get("groupid")
        )
        final_val = json.dumps(value_dict)
        self.running_job_path = self.client.create(path, final_val, sequence=True)

    def update_state(self, state):
        # update state in owned job
        job = self.get_job_from_path(self.running_job_path)
        job_object = json.loads(job)
        job_object["state"] = state
        job_updated = json.dumps(job_object)
        self.client.retry(self.client.set, self.running_job_path, job_updated)
        # if state == Jobstate.SUCCESSFUL:
        #     self.clear_running_path()

    def _check_put_arguments(self, value, priority=100):
        if not isinstance(value, bytes):
            raise TypeError("value must be a byte string")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        elif priority < 0 or priority > 999:
            raise ValueError("priority must be between 0 and 999")

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
                # entry-priority-dataset:groupid-created_order
                # job_id should be identified by dataset:groupid
                # entry[-1] is created_order entry[1] is priority
                self.unowned_job = self.client.get_children('/unowned')

                if len(self.unowned_job) == 0:
                    # no more task to do
                    continue

                # -int(entry.split("-")[1]) changes the sort order to descending order
                self.unowned_job = sorted(self.unowned_job, key=lambda entry: (-int(entry.split("-")[1]), entry.split("-")[-1]))

                job = self.get_job_from_list()

                if job:
                    self.own_job(job)
                    self.execute_job(job)
                elif self.wait_for_work():
                    time.sleep(1)
                    logging.debug("waiting for a new job that satisfy")
                else:
                    # get the oldest job
                    sorted(self.unowned_job, key=lambda entry: (-int(entry.split("-")[1]), entry.split("-")[-1]))
                    job = self.get_job(self.unowned_job[0])
                    self.own_job(job)
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
        self.run_task(job_path=self.running_job_path, dataset=job_object.get('dataset'),
                      groupid=job_object.get('groupid'), slots_needed=1)

    def run_task(self, job_path, dataset, groupid, slots_needed=1):
        # let the thread run
        worker_name = "worker"
        key = str(dataset) + ":" + str(groupid)
        cache_data = self.cache.get(key)
        if not cache_data:
            t = Executor(job_path=job_path, parent=self, name=worker_name, target=fetch_and_execute, args=[self.cache, key])
        else:
            t = Executor(job_path=job_path, parent=self, name=worker_name, target=execute_with_cache, args=[cache_data])
        t.daemon = True
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

    def clear_running_path(self):
        self.running_job_path = ""

    def available(self):
        return self.available_executor > 0

    def __available_resources(self):
        return self.available_executor
