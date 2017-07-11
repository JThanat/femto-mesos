#!/usr/bin/env python
# This main is adapted from APACHE Mesos original test_framework.py
import json
import os
import sys

import mesos.native
from mesos.interface import mesos_pb2

from framework.framework import HelloWorldScheduler
from framework.job import Job

if __name__ == "__main__":

    # Read JSON File
    json_file = sys.argv[2]
    jobs = []
    with open(json_file) as json_data:
        json_object = json.load(json_data)
        jobs_array = json_object["jobs"]
        print jobs_array

    # Prepare job object Array
    for j in jobs_array:
        jobs.append(Job.fromJSON(j))

    # Framework Info, Executor and Driver
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "hello-world"
    framework.checkpoint = True
    implicitAcknowledgements = 1

    if len(sys.argv) != 3:
        print
        "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.command.value = os.path.abspath("./test-executor")
    executor.name = "Test Executor (Python)"
    executor.source = "python_test"
    # STEP1: Get Framework Info
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"
    framework.checkpoint = True

    driver = mesos.native.MesosSchedulerDriver(
        HelloWorldScheduler(implicitAcknowledgements, executor, jobs),
        framework,
        sys.argv[1],
        implicitAcknowledgements)

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop()

    sys.exit(status)
