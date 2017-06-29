import logging
import threading
import time
import uuid
from random import randint

from framework.task import Task

TOTAL_TASKS = 5
TASK_CPUS = 1
TASK_MEM = 128

logging.basicConfig(level=logging.DEBUG,
                    format="(%(threadName)-10s) %(message)s",
                    )


class Dispatcher():
    # This class defines the framework of Mesos or Job Dispatcher of the system
    def __init__(self, implicitAcknowledgements, executor):
        self.executor = executor
        self.task_data = {}
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0

    def registered(self, driver, framework_id, master_info):
        logging.debug("Registered with framework ID %s" % framework_id.value)

    def resource_offers(self, driver, offers):
        # the resourceOffers method, where we can process the incoming offers from Mesos and potentially use them
        # to launch tasks.
        for offer in offers:
            tasks = []
            offer_cpus = 0
            offer_mem = 0
            for resource in offer.resources:
                if resource.cpu == "cpus":
                    offer_cpus += offer_cpus
                elif resource.name == "mem":
                    offer_mem += offer_mem

            logging.debug(
                "Received offer {0} with cpus: {1} and mem: {2}".format(offer.id.value, offer_cpus, offer_mem))

            remaining_cpus = offer_cpus
            remaining_mem = offer_mem

            while self.tasksLaunched < TOTAL_TASKS and remaining_cpus >= TASK_CPUS and remaining_mem >= TASK_MEM:
                tid = self.tasksLaunched
                self.tasksLaunched += 1

                # really launch some task and keep track of the value
                logging.debug("Launching task {0} using offer {1}".format(tid, offer.id.value))
                task = self.new_task(offer)
                tasks.append(task)

                remaining_cpus -= TASK_CPUS
                remaining_mem -= TASK_MEM

                self.launch_tasks(offer.id, tasks)

                # Mesos
                # operation = mesos_pb2.Offer.Operation()
                # operation.type = mesos_pb2.Offer.Operation.LAUNCH
                # operation.launch.task_infos.extend(tasks)
                # driver.acceptOffers([offer.id], [operation])
                # or
                # driver.launchTasks(offer.id, tasks)

    def new_task(self, offer):
        # For Mesos
        # task = mesos_pb2.TaskInfo()
        # task.task_id.value = str(tid)
        # task.slave_id.value = offer.slave_id.value
        # task.name = "task %d" % tid
        # task.executor.MergeFrom(self.executor)
        #
        # cpus = task.resources.add()
        # cpus.name = "cpus"
        # cpus.type = mesos_pb2.Value.SCALAR
        # cpus.scalar.value = TASK_CPUS
        #
        # mem = task.resources.add()
        # mem.name = "mem"
        # mem.type = mesos_pb2.Value.SCALAR
        # mem.scalar.value = TASK_MEM

        tid = uuid.uuid4()
        task = Task(tid, offer, self.executor)
        return task

    def offerRescinded(self, driver, offerId):
        # stop the task if the offer is no longer valid
        pass

    def statusUpdate(self, driver, update):
        # For Mesos
        # logging.debug("Task {0} is in state {1}".format(update.task_id.value, mesos_pb2.TaskState.Name(update.state)))
        logging.debug("Task {0} is in state {1}".format(update.task_id, update.state))

    def launch_task(self, offer_id, tasks):
        # Should be deleted soon
        for task in tasks:
            t = threading.Thread(target=run_task, name="OfferId {}".format(str(offer_id)),
                                 args=[offer_id, task.task_id], daemon=True)


def run_task(offer_id, task_id):
    logging.debug("Task {0} is being executed by offer_id {1}").format(task_id, offer_id)
    execution_time = randint(2, 5)
    time.sleep(execution_time)
    logging.debug("finish")
