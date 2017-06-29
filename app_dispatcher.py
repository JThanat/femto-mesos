#!/usr/bin/env python
# This main is adapted from APACHE Mesos original test_framework.py
import os
import sys

from framework.framework import Dispatcher

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print
        "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.command.value = os.path.abspath("./test-executor")
    executor.name = "Test Executor (Python)"
    executor.source = "python_test"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"
    framework.checkpoint = True

    implicitAcknowledgements = 1
    if os.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS"):
        print
        "Enabling explicit status update acknowledgements"
        implicitAcknowledgements = 0

    if os.getenv("MESOS_AUTHENTICATE_FRAMEWORKS"):
        print
        "Enabling authentication for the framework"

        if not os.getenv("DEFAULT_PRINCIPAL"):
            print
            "Expecting authentication principal in the environment"
            sys.exit(1);

        credential = mesos_pb2.Credential()
        credential.principal = os.getenv("DEFAULT_PRINCIPAL")

        if os.getenv("DEFAULT_SECRET"):
            credential.secret = os.getenv("DEFAULT_SECRET")

        framework.principal = os.getenv("DEFAULT_PRINCIPAL")

        driver = mesos.native.MesosSchedulerDriver(
            Dispatcher(implicitAcknowledgements, executor),
            framework,
            sys.argv[1],
            implicitAcknowledgements,
            credential)
    else:
        framework.principal = "test-framework-python"

        driver = mesos.native.MesosSchedulerDriver(
            Dispatcher(implicitAcknowledgements, executor),
            framework,
            sys.argv[1],
            implicitAcknowledgements)

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop()

    sys.exit(status)
