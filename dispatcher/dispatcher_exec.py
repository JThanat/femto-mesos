import json
import threading
import logging
from zookeeper.job import Jobstate
from kazoo.exceptions import NoNodeError
from kazoo.retry import ForceRetryError
from pymongo import MongoClient
from kazoo.client import KazooClient

logging.basicConfig(filename="test.log",
                    level=logging.DEBUG,
                    format="(%(threadName)-10s) %(message)s",
                    )

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


def poll_job(client):
    # This should keep track of the job
    t = threading.currentThread()
    job = get(client)

    if not job:
        logging.debug("No data to return")
        return None

    job_object = json.loads(job)

    #TODO - Move Mongo somewhere make sense
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client["result_database"]
    results = db.results

    job_mongo = results.find_one({"dataset":job_object.get("dataset"),
                      "groupid":job_object.get("groupid")
                      })

    logging.debug("return data {dataset}:{groupid} objectid:{objectid}".format(dataset=job_mongo.get("dataset"),
                                                                               groupid=job_mongo.get("groupid"),
    return job_object

def get(client):
    t = threading.currentThread()
    children = client.get_children('/done')
    if len(children) == 0:
        return None
    children = sorted(children, key=lambda entry: (-int(entry.split("-")[1]), entry.split("-")[-1]))
    entry_path = children[0]
    done_path = '/done'
    path = "{path}/{entry_path}".format(
        path=done_path,
        entry_path=entry_path
    )
    return client.retry(_inner_get, client, path)

def _inner_get(client, path):
    try:
        data, stat = client.get(path)
    except NoNodeError:
        # the first node has vanished in the meantime, try to
        # get another one
        raise ForceRetryError()
    try:
        client.delete(path)
    except NoNodeError:
        # we were able to get the data but someone else has removed
        # the node in the meantime. consider the item as processed
        # by the other process
        raise ForceRetryError()
    return data