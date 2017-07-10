#!/usr/bin/env python
# This main is adapted from APACHE Mesos original test_framework.py
import os
import sys

import mesos.native
from mesos.interface import mesos_pb2

from framework.framework import HelloWorldScheduler

if __name__ == "__main__":
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "hello-world"
    framework.checkpoint = True
    implicitAcknowledgements = 1

    if len(sys.argv) != 2:
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
        HelloWorldScheduler(implicitAcknowledgements, executor),
        framework,
        sys.argv[1],
        implicitAcknowledgements)

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop()

    sys.exit(status)
