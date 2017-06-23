import threading


class Slave(threading.Thread):
    # This class define the compute node of the system
    _default_executor_number = 4

    def __init__(self, executor_number=0, *args):
        # the args is the default constructor from superclass (Thread)
        self.cache = {}
        self.executor_number = executor_number
        if self.executor_number == 0:
            # Initial the executor here, the default would be 4 executors
            self.available_executor = self._default_executor_number
        else:
            self.available_executor = executor_number
            
        super(Slave, self).__init__(*args)

    def run_task(self, slots_needed=1):
        # allocate the resource
        self.allocate()
        # let the thread run

        # return the resource
        self.release()

    def allocate(self, slots_allocated=1):
        # To make it simple we will run only one executor per allocation fist, the slots_allocated variable
        # will soon be used when multiple executor need to be allocated at a time
        self.available_executor -= slots_allocated

    def release(self, slots_released=1):
        # To make it simple we will run only one executor per allocation fist, the slots_allocated variable
        # will soon be used when multiple executor need to be allocated at a time
        self.available_executor += slots_released

        # Notify master after release the resource
        self.notify_master(self.available_executor)

    def notify_master(self, available_executor):
        # Some Notifying Method
        return
