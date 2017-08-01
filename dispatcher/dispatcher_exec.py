import json
import threading
import logging
from zookeeper.job import Jobstate

class Dispatch_Executor(threading.Thread):
    def __init__(self, parent=None, **kwargs):
        self.parent = parent
        super(Dispatch_Executor, self).__init__(**kwargs)

    def run(self):
        self.parent.allocate()
        super(Dispatch_Executor, self).run()
        self.parent.release()

def put_job(client, dataset, groupid, priority=100):
    t = threading.currentThread()
    unowned_path = "/unowned"
    prefix = "entry-"
    logging.debug("putting job {dataset}:{groupid} into zookeeper".format(dataset=dataset, groupid=groupid))

    value_dict = {}
    value_dict["dataset"] = dataset
    value_dict["groupid"] = groupid
    value_dict["state"] = Jobstate.PENDING

    path = "{path}/{prefix}{priority:03d}-{dataset}:{groupid}-".format(
        path=unowned_path,
        prefix=prefix,
        priority=priority,
        dataset=dataset,
        groupid=groupid
    )
    final_val = json.dumps(value_dict)
    client.create(path, final_val, sequence=True)


def poll_job(dataset):
    # This should keep track of the job
    pass
