class Task(object):
    def __init__(self, tid, offer, executor):
        self.task_id = str(tid)
        self.slave_id = str(offer.slave_id)
        self.name = "task {}".format(str(tid))
        self.executor = executor
