import logging
import sys
import threading

from mesos.interface import mesos_pb2
from job import *
import mesos.native

TOTAL_TASKS = 5

TASK_CPUS = 1
TASK_MEM = 128

logging.basicConfig(filename="running.log",
                    level=logging.DEBUG,
                    format="(%(threadName)-10s) %(message)s",
                    )

class Dispatcher(mesos.interface.Scheduler):
    def __init__(self, implicitAcknowledgements, executor):
        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor
        self.taskData = {}
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0

    def registered(self, driver, frameworkId, masterInfo):
        # Some DB Connection could go be instantiated here
        print "Registered with framework ID %s" % frameworkId.value

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            print "Received offer %s with cpus: %s and mem: %s" % (offer.id.value, offerCpus, offerMem)

            remainingCpus = offerCpus
            remainingMem = offerMem

            while self.tasksLaunched < TOTAL_TASKS and \
                            remainingCpus >= TASK_CPUS and \
                            remainingMem >= TASK_MEM:
                tid = self.tasksLaunched
                self.tasksLaunched += 1

                print "Launching task %d using offer %s" % (tid, offer.id.value)

                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid
                task.executor.MergeFrom(self.executor)

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = TASK_CPUS

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = TASK_MEM

                tasks.append(task)
                self.taskData[task.task_id.value] = (
                    offer.slave_id, task.executor.executor_id)

                remainingCpus -= TASK_CPUS
                remainingMem -= TASK_MEM

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        print "Task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        # Ensure the binary data came through.
        if update.data != "data with a \0 byte":
            print "The update data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(update.data))
            sys.exit(1)

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1
            if self.tasksFinished == TOTAL_TASKS:
                print "All tasks done, waiting for final framework message"

            slave_id, executor_id = self.taskData[update.task_id.value]

            self.messagesSent += 1
            driver.sendFrameworkMessage(
                executor_id,
                slave_id,
                'data with a \0 byte')

        if update.state == mesos_pb2.TASK_LOST or \
                        update.state == mesos_pb2.TASK_KILLED or \
                        update.state == mesos_pb2.TASK_FAILED:
            print "Aborting because task %s is in unexpected state %s with message '%s'" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
            driver.abort()

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1

        # The message bounced back as expected.
        if message != "data with a \0 byte":
            print "The returned message data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(message))
            sys.exit(1)
        print "Received message:", repr(str(message))

        if self.messagesReceived == TOTAL_TASKS:
            if self.messagesReceived != self.messagesSent:
                print "Sent", self.messagesSent,
                print "but received", self.messagesReceived
                sys.exit(1)
            print "All tasks done, and all messages received, exiting"
            driver.stop()

    def new_task(self, offer):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(tid)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %d" % tid
        task.executor.MergeFrom(self.executor)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM

        return task

import uuid
import time

class HelloWorldScheduler(mesos.interface.Scheduler):
    def __init__(self, implicitAcknowledgements, executor, jobs):
        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor
        self.jobs = jobs

    def registered(self, driver, frameworkId, masterInfo):
        # Some DB Connection could go be instantiated here
        print "Registered with framework ID %s" % frameworkId.value

    _submitted = False
    lock = threading.Lock()

    def resourceOffers(self, driver, offers):
        with self.lock:
            print "Received resource offers: {}".format([offer.id.value for offer in offers])

            pending_jobs = []
            tasks = []
            for job in self.jobs:
                if not job.submitted:
                    pending_jobs.append(job)

            for offer in offers:
                if len(pending_jobs) == 0:
                    driver.declineOffer(offer.id)
                    print "decline resource offers: {}".format([offer.id.value for offer in offers])
                    break

                job = pending_jobs.pop(0)
                task = job.new_task(offer)
                task.command.value = job.command

                print "Launching task {task} using offer {offer}".format(task=task.task_id.value, offer=offer.id.value)

                tasks.append(task)

                operation = mesos_pb2.Offer.Operation()
                operation.type = mesos_pb2.Offer.Operation.LAUNCH
                operation.launch.task_infos.extend(tasks)

                driver.acceptOffers([offer.id], [operation])

                job.submitted = True

    def statusUpdate(self, driver, update):
        with self.lock:
            # TODO - Change jobs to map instead of List
            for job in self.jobs:
                if str(job.id) == update.task_id.value:
                    if update.state == mesos_pb2.TASK_RUNNING:
                        job.started()
                        break
                    elif update.state == mesos_pb2.TASK_FINISHED:
                        job.succeed()
                        break
                    elif update.state == mesos_pb2.TASK_FAILED:
                        pass
                    elif update.state == mesos_pb2.TASK_KILLED:
                        pass
                    elif update.state == mesos_pb2.TASK_LOST:
                        pass
                    elif update.state == mesos_pb2.TASK_ERROR:
                        job.fail()
                        break
                    else:
                        break

    def reconcileTasks(self, tasks):
        """
          Allows the framework to query the status for non-terminal tasks.
          This causes the master to send back the latest task status for
          each task in 'statuses', if possible. Tasks that are no longer
          known will result in a TASK_LOST update. If statuses is empty,
          then the master will send the latest status for each task
          currently known.
        """
        print "Reconcile Task"
        print type(tasks)

