import uuid
import json
from mesos.interface import mesos_pb2
import mesos.native

class Job(object):
    def __init__(self, cpus = 1.0, mem = 128.0, command = ""):
        self.submitted = False
        self.cpus = cpus
        self.mem = mem
        self.command = command

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

    @classmethod
    def fromJSON(self, json_data):
        cpus = json_data.get("cpus")
        mem = json_data.get("mem")
        command = json_data.get("command")
        return self(cpus,mem,command)







