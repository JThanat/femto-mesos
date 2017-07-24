# This has been modified from
# https://github.com/python-zk/kazoo/blob/a4ef6234706a84c3d8c15c67dc6477b580490842/kazoo/recipe/queue.py
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.retry import ForceRetryError
from kazoo.protocol.states import EventType


class BaseQueue(object):
    def __init__(self, client, path):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path to use in ZooKeeper.
        """
        self.client = client
        self.path = path
        self._entries_path = path
        self.structure_paths = (self.path, )
        self.ensured_path = False

    def _check_put_arguments(self, value, priority=100):
        if not isinstance(value, bytes):
            raise TypeError("value must be a byte string")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        elif priority < 0 or priority > 999:
            raise ValueError("priority must be between 0 and 999")

    def _ensure_paths(self):
        if not self.ensured_path:
            # make sure our parent / internal structure nodes exists
            for path in self.structure_paths:
                self.client.ensure_path(path)
            self.ensured_path = True

    def __len__(self):
        self._ensure_paths()
        _, stat = self.client.retry(self.client.get, self._entries_path)
        return stat.children_count


class Queue(BaseQueue):
    """A distributed queue with optional priority support.
    This queue does not offer reliable consumption. An entry is removed
    from the queue prior to being processed. So if an error occurs, the
    consumer has to re-queue the item or it will be lost.
    """

    prefix = "entry-"

    def __init__(self, client, path):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path to use in ZooKeeper.
        """
        super(Queue, self).__init__(client, path)
        self._children = []

    def __len__(self):
        """Return queue size."""
        return super(Queue, self).__len__()

    def get(self):
        self._ensure_paths()
        return self.client.retry(self._inner_get)

    def _inner_get(self):
        if not self._children:
            self._children = self.client.retry(
                self.client.get_children, self.path)
            self._children = sorted(self._children)
        if not self._children:
            return None
        name = self._children[0]
        try:
            data, stat = self.client.get(self.path + "/" + name)
        except NoNodeError:  # pragma: nocover
            # the first node has vanished in the meantime, try to
            # get another one
            raise ForceRetryError()
        try:
            self.client.delete(self.path + "/" + name)
        except NoNodeError:  # pragma: nocover
            # we were able to get the data but someone else has removed
            # the node in the meantime. consider the item as processed
            # by the other process
            raise ForceRetryError()
        self._children.pop(0)
        return data

    def put(self, value, priority=100):
        self._check_put_arguments(value, priority)
        self._ensure_paths()
        path = '{path}/{prefix}{priority:03d}-'.format(
            path=self.path, prefix=self.prefix, priority=priority)
        self.client.create(path, value, sequence=True)