TOTAL_TASKS = 5

TASK_CPUS = 1
TASK_MEM = 128


class dispathcer():
    # This class defines the framework of Mesos or Job Dispatcher of the system
    def __init__(self, implicitAcknowledgements, executor):
        self.executor = executor
        self.task_data = {}
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0

    def registered(self, driver, frameworkId, masterInfo):
        print("Registered with framework ID %s" % frameworkId.value)

    def resource_offers(self, driver, offers):
        for offer in offers:
            tasks = []
            offer_cpus = 0
            offer_mem = 0
            for resource in offer.resources:
                if resource.cpu == "cpus":
                    offer_cpus += offer_cpus
                elif resource.name == "mem":
                    offer_mem += offer_mem

            print("Received offer {0} with cpus: {1} and mem: {2}".format(offer.id.value, offer_cpus, offer_mem))

            remaining_cpus = offer_cpus
            remaining_mem = offer_mem

        while self.tasksLaunched < TOTAL_TASKS and remaining_cpus >= TASK_CPUS and remaining_mem >= TASK_MEM:
            tid = self.tasksLaunched
            self.tasksLaunched += 1

            # really launch some task and keep track of the value
            print("Launching task {0} using offer {1}".format(tid, offer.id.value))
            task = {}
            tasks.append(task)

            remaining_cpus -= TASK_CPUS
            remaining_mem -= TASK_MEM

            # Probably Something More

    def statusUpdate(self,driver, update):
        pass

    def frameworkMessage(self):
        pass