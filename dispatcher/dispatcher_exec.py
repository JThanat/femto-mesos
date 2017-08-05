import json
import logging
import threading
import time

from kazoo.exceptions import NoNodeError
from kazoo.retry import ForceRetryError, KazooRetry, RetryFailedError

from storage.db import MongoInitializer
from zookeeper.job import Jobstate

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


def execute_job(mongodb, client, dataset, groupid, priority=100):
    db_result = mongodb.get_from_key(dataset, groupid)
    if not db_result:
        owned_jobs = check_zk(client, dataset, groupid)
        if len(owned_jobs) == 0:
            put_job(client, dataset, groupid, priority=priority)
        else:
            # TODO wait and get the result
            db_result = wait_job_and_return(mongodb, client, dataset, groupid)
            logging.debug("return executed {dataset}:{groupid} objectid:{objectid} after waiting".format(
                dataset=db_result.get("dataset"),
                groupid=db_result.get("groupid"),
                objectid=str(db_result.get("_id"))))

    else:
        logging.debug("return already executed data {dataset}:{groupid} objectid:{objectid}".format(
            dataset=db_result.get("dataset"),
            groupid=db_result.get("groupid"),
            objectid=str(db_result.get("_id"))))
        return


def wait_job_and_return(mongodb, client, dataset, groupid):
    db_result = mongodb.get_from_key(dataset, groupid)
    if not db_result:
        logging.debug("waiting 100ms for {dataset}:{groupid} to be executed".format(
            dataset=dataset,
            groupid=groupid
        ))
        time.sleep(1)
        return wait_job_and_return(mongodb, client, dataset, groupid)
    else:
        return db_result


def check_zk(client, dataset, groupid):
    t = threading.currentThread()
    owned_path = "/owned"
    unowned_path = "/unowned"
    finding_key = "{dataset}:{groupid}".format(dataset=dataset, groupid=groupid)
    owned_job = client.get_children(owned_path)
    unowned_job = client.get_children(unowned_path)
    unowned_job = filter(lambda j: j.split("-")[2] == finding_key, unowned_job)
    owned_job = filter(lambda j: j.split("-")[2] == finding_key, owned_job)
    return owned_job + unowned_job


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
    return


def poll_job(client):
    # This should keep track of the job
    t = threading.currentThread()
    job = get(client)

    if not job:
        logging.debug("No data to return")
        return None

    job_object = json.loads(job)

    # TODO - Move Mongo somewhere make sense
    mongodb = MongoInitializer()
    db = mongodb.db
    results = mongodb.collection
    job_mongo = mongodb.get_from_key(job_object.get("dataset"), job_object.get("groupid"))
    logging.debug("return data {dataset}:{groupid} objectid:{objectid}".format(dataset=job_mongo.get("dataset"),
                                                                               groupid=job_mongo.get("groupid"),
                                                                               objectid=str(job_mongo.get("_id"))))
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
    kr = KazooRetry(max_tries=3, ignore_expire=False)
    try:
        result = kr(_inner_get, client, path)
    except RetryFailedError:
        return None

    return result


def _inner_get(client, path):
    max_retries = 3
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
