import uuid
import json
from mesos.interface import mesos_pb2
import mesos.native

class Jobstate(object):
    PENDING = 1
    STAGING = 2
    RUNNING = 3
    SUCCESSFUL = 4
    FAILED = 5

class Job(object):
    def __init__(self, cpus = 1.0, mem = 128.0, command = "", retries = 3):
        self.submitted = False
        self.cpus = cpus
        self.mem = mem
        self.command = command
        self.retries = retries
        self.id = uuid.uuid4()
        self.status = Jobstate.PENDING

    def new_task(self, offer):
        task = mesos_pb2.TaskInfo()
        id = uuid.uuid4()
        task.task_id.value = str(id)
        task.slave_id.value = offer.slave_id.value
        task.name = "task {0}".format(str(id))

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = 1

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = 1

        return task

    def launch(self):
        self.status = Jobstate.STAGING

    def started(self):
        self.status = Jobstate.RUNNING

    def succeed(self):
        self.status = Jobstate.SUCCESSFUL

    def fail(self):
        if self.retries == 0:
            self.status = Jobstate.FAILED
        else:
            self.retries -= 1
            self.status = Jobstate.PENDING


    @classmethod
    def fromJSON(self, json_data):
        cpus = json_data.get("cpus")
        mem = json_data.get("mem")
        command = json_data.get("command")
        return self(cpus,mem,command)



def do_fit_first(offer, jobs):
    to_launch = []
    launched = []
    offer_cpus = 0.0
    offer_mem = 0.0

    for resource in offer.resources:
        if resource.name == "cpus":
            offer_cpus += resource.scalar.value
        elif resource.name == "mem":
            offer_mem += resource.scalar.value

    print "Received offer {offer} with cpus: {cpu} and mem: {mem}".format(offer=offer.id.value, cpu=offer_cpus, mem=offer_mem)

    for job in jobs:
        job_cpus = job.cpus
        job_mem = job.mem

        if offer_cpus >= job_cpus and offer_mem >= job_mem:
            offer_cpus -= job_cpus
            offer_mem -= offer_mem
            to_launch.append(job.new_task(offer))
            job.submitted = True
            launched.append(job)

    for job in launched:
        job.launch()

    jobs = [x for x in jobs if x not in launched]
    return to_launch







